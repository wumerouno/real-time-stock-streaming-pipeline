from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_trunc, sum as spark_sum


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("stock-market-batch-backfill")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    delta_root = Path(env("DELTA_ROOT", "data/delta"))
    source = delta_root / "silver" / "valid_trades"
    target = delta_root / "gold" / "daily_symbol_summary"

    trades = spark.read.format("delta").load(str(source))
    daily = (
        trades.withColumn("trade_day", date_trunc("day", col("event_ts")))
        .groupBy("trade_day", "symbol")
        .agg(
            count("*").alias("trade_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("notional").alias("total_notional"),
        )
    )
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(target))
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
