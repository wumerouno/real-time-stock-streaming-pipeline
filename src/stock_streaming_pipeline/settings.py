from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else float(raw)


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trade_topic: str = _env("KAFKA_TRADE_TOPIC", "stock-trades")
    dlq_topic: str = _env("KAFKA_DLQ_TOPIC", "stock-trades-dlq")
    anomaly_topic: str = _env("KAFKA_ANOMALY_TOPIC", "stock-anomalies")
    aggregate_topic: str = _env("KAFKA_AGGREGATE_TOPIC", "stock-aggregates")
    consumer_group: str = _env("KAFKA_CONSUMER_GROUP", "stock-streaming-pipeline")


@dataclass(frozen=True)
class StreamSettings:
    event_rate_per_second: int = _env_int("EVENT_RATE_PER_SECOND", 50)
    out_of_order_rate: float = _env_float("OUT_OF_ORDER_RATE", 0.12)
    invalid_event_rate: float = _env_float("INVALID_EVENT_RATE", 0.01)
    anomaly_rate: float = _env_float("ANOMALY_RATE", 0.015)
    watermark_delay: str = _env("WATERMARK_DELAY", "2 minutes")
    max_offsets_per_trigger: int = _env_int("MAX_OFFSETS_PER_TRIGGER", 5000)
    checkpoint_root: Path = Path(_env("CHECKPOINT_ROOT", "data/checkpoints"))
    delta_root: Path = Path(_env("DELTA_ROOT", "data/delta"))
    metrics_root: Path = Path(_env("METRICS_ROOT", "data/metrics"))


@dataclass(frozen=True)
class SnowflakeSettings:
    enabled: bool = _env("SNOWFLAKE_ENABLED", "false").lower() == "true"
    account: str = _env("SNOWFLAKE_ACCOUNT", "")
    user: str = _env("SNOWFLAKE_USER", "")
    password: str = _env("SNOWFLAKE_PASSWORD", "")
    role: str = _env("SNOWFLAKE_ROLE", "SYSADMIN")
    warehouse: str = _env("SNOWFLAKE_WAREHOUSE", "STREAMING_WH")
    database: str = _env("SNOWFLAKE_DATABASE", "STOCK_STREAMING")
    schema: str = _env("SNOWFLAKE_SCHEMA", "MARKET")


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings = KafkaSettings()
    stream: StreamSettings = StreamSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()


def load_settings() -> AppSettings:
    return AppSettings()

