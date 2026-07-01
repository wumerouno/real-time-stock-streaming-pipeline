from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNOWFLAKE_DIR = PROJECT_ROOT / "snowflake"


def load_dotenv() -> None:
    try:
        from dotenv import load_dotenv as load_dotenv_file
    except ImportError:
        return

    load_dotenv_file(PROJECT_ROOT / ".env")


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def require_env(name: str) -> str:
    value = env(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def read_private_key(path: str) -> bytes:
    try:
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
            load_pem_private_key,
        )
    except ImportError as exc:
        raise RuntimeError("Private-key auth requires cryptography from snowflake-connector-python") from exc

    passphrase = env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    password = passphrase.encode("utf-8") if passphrase else None
    key_data = Path(path).read_bytes()
    private_key = load_pem_private_key(key_data, password=password)
    return private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def connection_config() -> dict[str, Any]:
    config: dict[str, Any] = {
        "account": require_env("SNOWFLAKE_ACCOUNT"),
        "user": require_env("SNOWFLAKE_USER"),
        "role": env("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": env("SNOWFLAKE_WAREHOUSE", "STREAMING_WH"),
        "database": env("SNOWFLAKE_DATABASE", "STOCK_STREAMING"),
        "schema": env("SNOWFLAKE_SCHEMA", "MARKET"),
    }

    private_key_path = env("SNOWFLAKE_PRIVATE_KEY_PATH")
    password = env("SNOWFLAKE_PASSWORD")
    if private_key_path:
        config["private_key"] = read_private_key(private_key_path)
    elif password:
        config["password"] = password
    else:
        raise RuntimeError("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH for authentication")

    return config


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False

    index = 0
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""

        if char == "'" and in_single_quote and next_char == "'":
            buffer.append(char)
            buffer.append(next_char)
            index += 2
            continue

        if char == "'":
            in_single_quote = not in_single_quote

        if char == ";" and not in_single_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
        else:
            buffer.append(char)

        index += 1

    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)

    return statements


def execute_sql_file(cursor: Any, path: Path) -> None:
    for statement in split_sql_statements(path.read_text(encoding="utf-8")):
        cursor.execute(statement)


def validation_events(batch_id: str) -> list[dict[str, Any]]:
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    return [
        {
            "trade_id": f"{batch_id}-aapl-001",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 125,
            "price": 211.25,
            "event_time": now.isoformat(),
            "ingest_time": (now + timedelta(seconds=1)).isoformat(),
            "exchange": "NASDAQ",
            "trader_id": "validation-0001",
            "sequence": 1,
            "schema_version": "1.0",
        },
        {
            "trade_id": f"{batch_id}-msft-001",
            "symbol": "MSFT",
            "side": "SELL",
            "quantity": 80,
            "price": 452.75,
            "event_time": now.isoformat(),
            "ingest_time": (now + timedelta(seconds=2)).isoformat(),
            "exchange": "NYSE",
            "trader_id": "validation-0002",
            "sequence": 2,
            "schema_version": "1.0",
        },
    ]


def insert_bronze_events(cursor: Any, events: list[dict[str, Any]]) -> None:
    for offset, event in enumerate(events):
        metadata = {
            "topic": "stock-trades",
            "partition": 0,
            "offset": offset,
            "validation": True,
        }
        cursor.execute(
            """
            INSERT INTO BRONZE_KAFKA_TRADES (RECORD_CONTENT, RECORD_METADATA)
            SELECT PARSE_JSON(%s), PARSE_JSON(%s)
            """,
            (json.dumps(event), json.dumps(metadata)),
        )


def fetch_count(cursor: Any, query: str, params: tuple[Any, ...]) -> int:
    cursor.execute(query, params)
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def wait_for_count(
    cursor: Any,
    label: str,
    query: str,
    params: tuple[Any, ...],
    expected_count: int,
    timeout_seconds: int,
) -> int:
    deadline = time.monotonic() + timeout_seconds
    last_count = 0
    while time.monotonic() < deadline:
        last_count = fetch_count(cursor, query, params)
        if last_count >= expected_count:
            return last_count
        time.sleep(5)

    raise RuntimeError(f"Timed out waiting for {label}: expected {expected_count}, found {last_count}")


def suspend_tasks(cursor: Any) -> None:
    for task_name in ("TASK_SILVER_TO_GOLD_1M", "TASK_BRONZE_TO_SILVER"):
        try:
            cursor.execute(f"ALTER TASK {task_name} SUSPEND")
        except Exception:
            pass


def run_validation(timeout_seconds: int, keep_tasks_running: bool) -> dict[str, Any]:
    config = connection_config()
    safe_config = {key: value for key, value in config.items() if key not in {"password", "private_key"}}

    try:
        import snowflake.connector
    except ImportError as exc:
        raise RuntimeError('Install Snowflake dependencies with: python -m pip install -e ".[snowflake]"') from exc

    batch_id = f"sfval-{uuid.uuid4().hex[:10]}"
    events = validation_events(batch_id)

    connection = snowflake.connector.connect(**config)
    try:
        cursor = connection.cursor()
        try:
            execute_sql_file(cursor, SNOWFLAKE_DIR / "01_setup.sql")
            execute_sql_file(cursor, SNOWFLAKE_DIR / "02_streams_tasks.sql")
            insert_bronze_events(cursor, events)

            cursor.execute("EXECUTE TASK TASK_BRONZE_TO_SILVER")

            silver_count = wait_for_count(
                cursor,
                "silver validation trades",
                "SELECT COUNT(*) FROM SILVER_VALID_TRADES WHERE TRADE_ID LIKE %s",
                (f"{batch_id}%",),
                len(events),
                timeout_seconds,
            )

            gold_count = wait_for_count(
                cursor,
                "gold one-minute windows",
                """
                SELECT COUNT(*)
                FROM GOLD_SYMBOL_WINDOWS_1M
                WHERE WINDOW_START >= DATEADD('MINUTE', -5, CURRENT_TIMESTAMP())
                  AND SYMBOL IN ('AAPL', 'MSFT')
                """,
                (),
                2,
                timeout_seconds,
            )

            bronze_count = fetch_count(
                cursor,
                "SELECT COUNT(*) FROM BRONZE_KAFKA_TRADES WHERE RECORD_CONTENT:trade_id::STRING LIKE %s",
                (f"{batch_id}%",),
            )

            return {
                "status": "passed",
                "batch_id": batch_id,
                "connection": safe_config,
                "bronze_rows": bronze_count,
                "silver_rows": silver_count,
                "gold_window_rows": gold_count,
            }
        finally:
            if not keep_tasks_running:
                suspend_tasks(cursor)
            cursor.close()
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Snowflake setup, tasks, and transforms.")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--keep-tasks-running", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    try:
        result = run_validation(args.timeout_seconds, args.keep_tasks_running)
    except RuntimeError as exc:
        print(f"Snowflake validation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
