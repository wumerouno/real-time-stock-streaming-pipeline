from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.getenv("STOCK_PIPELINE_HOME", "/opt/stock-pipeline")

with DAG(
    dag_id="stock_streaming_quality_and_backfill",
    description="Batch reconciliation companion for the stock streaming pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "stocks", "data-quality"],
) as dag:
    backfill_daily_gold = BashOperator(
        task_id="backfill_daily_gold_delta",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "spark/batch_backfill_job.py"
        ),
    )

    validate_delta_outputs = BashOperator(
        task_id="validate_delta_outputs",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "test -d data/delta/silver/valid_trades && "
            "test -d data/delta/gold/daily_symbol_summary"
        ),
    )

    backfill_daily_gold >> validate_delta_outputs

