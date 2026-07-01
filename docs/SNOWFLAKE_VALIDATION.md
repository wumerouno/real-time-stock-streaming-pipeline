# Snowflake Validation

The local Docker demo uses Delta Lake for runnable storage. The Snowflake path is validated with `scripts/validate_snowflake.py`, which executes the Snowflake setup and transformation assets against a live Snowflake account.

## Required Credentials

Create `.env` from `.env.example` and set:

```powershell
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<user>
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=STREAMING_WH
SNOWFLAKE_DATABASE=STOCK_STREAMING
SNOWFLAKE_SCHEMA=MARKET
```

Use one authentication method:

```powershell
SNOWFLAKE_PASSWORD=<password>
```

or:

```powershell
SNOWFLAKE_PRIVATE_KEY_PATH=C:\path\to\rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<optional_passphrase>
```

Do not commit `.env`, private keys, or connector property files containing secrets.

## Run Validation

```powershell
python -m pip install -e ".[snowflake]"
python scripts/validate_snowflake.py
```

To leave the task graph running after validation:

```powershell
python scripts/validate_snowflake.py --keep-tasks-running
```

By default, the script suspends the Snowflake tasks after validation to avoid unnecessary warehouse usage.

## What The Script Verifies

The validator performs these checks against Snowflake:

1. Executes `snowflake/01_setup.sql`.
2. Executes `snowflake/02_streams_tasks.sql`.
3. Inserts a deterministic validation batch into `BRONZE_KAFKA_TRADES`.
4. Executes `TASK_BRONZE_TO_SILVER`.
5. Waits for validation trades to appear in `SILVER_VALID_TRADES`.
6. Waits for one-minute symbol windows to appear in `GOLD_SYMBOL_WINDOWS_1M`.

Expected output shape:

```json
{
  "batch_id": "sfval-...",
  "bronze_rows": 2,
  "gold_window_rows": 2,
  "silver_rows": 2,
  "status": "passed"
}
```

If credentials are missing, the script exits before connecting and names the missing `SNOWFLAKE_*` variable.

