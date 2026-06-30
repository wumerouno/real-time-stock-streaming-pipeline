from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    concat_ws,
    count,
    create_map,
    current_timestamp,
    expr,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    session_window,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def trade_schema() -> StructType:
    return StructType(
        [
            StructField("trade_id", StringType()),
            StructField("symbol", StringType()),
            StructField("side", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DoubleType()),
            StructField("event_time", StringType()),
            StructField("ingest_time", StringType()),
            StructField("exchange", StringType()),
            StructField("trader_id", StringType()),
            StructField("sequence", IntegerType()),
            StructField("schema_version", StringType()),
        ]
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("stock-market-realtime-pipeline")
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "12"))
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def kafka_source(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", env("KAFKA_TRADE_TOPIC", "stock-trades"))
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "5000"))
        .load()
    )


def prepare_stream(raw: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    bronze = raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_payload"),
        current_timestamp().alias("bronze_ingested_at"),
    )

    parsed = bronze.withColumn("payload", from_json(col("raw_payload"), trade_schema()))
    enriched = (
        parsed.select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "message_key",
            "raw_payload",
            "bronze_ingested_at",
            col("payload.*"),
        )
        .withColumn("event_ts", to_timestamp("event_time"))
        .withColumn("ingest_ts", to_timestamp("ingest_time"))
        .withColumn(
            "validation_error",
            when(col("trade_id").isNull(), lit("MISSING_TRADE_ID"))
            .when(col("symbol").isNull(), lit("MISSING_SYMBOL"))
            .when(~col("side").isin("BUY", "SELL"), lit("INVALID_SIDE"))
            .when(col("quantity").isNull() | (col("quantity") <= 0), lit("INVALID_QUANTITY"))
            .when(col("price").isNull() | (col("price") <= 0), lit("INVALID_PRICE"))
            .when(col("event_ts").isNull(), lit("INVALID_EVENT_TIME"))
            .otherwise(lit(None)),
        )
    )

    invalid = enriched.filter(col("validation_error").isNotNull())
    valid = (
        enriched.filter(col("validation_error").isNull())
        .withColumn("symbol", expr("upper(symbol)"))
        .withColumn("notional", col("quantity") * col("price"))
        .withWatermark("event_ts", env("WATERMARK_DELAY", "2 minutes"))
        .dropDuplicates(["trade_id"])
    )
    return bronze, valid, invalid


def with_anomaly_flags(valid: DataFrame) -> DataFrame:
    reference_prices = {
        "AAPL": 210.0,
        "MSFT": 450.0,
        "NVDA": 126.0,
        "TSLA": 185.0,
        "AMZN": 190.0,
        "META": 505.0,
        "GOOGL": 176.0,
        "JPM": 205.0,
    }
    mapping_args = []
    for symbol, price in reference_prices.items():
        mapping_args.extend([lit(symbol), lit(price)])

    ref_price = create_map(*mapping_args).getItem(col("symbol"))
    return (
        valid.withColumn("reference_price", ref_price)
        .withColumn(
            "price_move_pct",
            when(col("reference_price").isNotNull(), spark_abs(col("price") - col("reference_price")) / col("reference_price")).otherwise(lit(None)),
        )
        .withColumn(
            "anomaly_reason",
            when(col("quantity") >= 25_000, lit("HIGH_QUANTITY"))
            .when(col("notional") >= 1_000_000, lit("HIGH_NOTIONAL"))
            .when(col("price_move_pct") >= 0.25, lit("PRICE_DISLOCATION"))
            .otherwise(lit("NORMAL")),
        )
        .withColumn(
            "severity",
            when(col("anomaly_reason").isin("HIGH_QUANTITY", "HIGH_NOTIONAL"), lit("high"))
            .when(col("anomaly_reason") == "PRICE_DISLOCATION", lit("medium"))
            .otherwise(lit("none")),
        )
    )


def build_aggregates(valid: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    tumbling_1m = (
        valid.groupBy(window("event_ts", "1 minute"), "symbol")
        .agg(
            count("*").alias("trade_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("notional").alias("total_notional"),
            spark_sum(when(col("side") == "BUY", col("quantity")).otherwise(lit(0))).alias("buy_quantity"),
            spark_sum(when(col("side") == "SELL", col("quantity")).otherwise(lit(0))).alias("sell_quantity"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            (spark_sum("notional") / spark_sum("quantity")).alias("vwap"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    sliding_5m = (
        valid.groupBy(window("event_ts", "5 minutes", "1 minute"), "symbol")
        .agg(
            count("*").alias("trade_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("notional").alias("total_notional"),
            (spark_sum("notional") / spark_sum("quantity")).alias("vwap"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    sessions = (
        valid.groupBy(session_window("event_ts", "3 minutes"), "symbol")
        .agg(
            count("*").alias("trade_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("notional").alias("total_notional"),
        )
        .withColumn("session_start", col("session_window.start"))
        .withColumn("session_end", col("session_window.end"))
        .drop("session_window")
    )

    return tumbling_1m, sliding_5m, sessions


def start_delta_query(df: DataFrame, name: str, output_path: Path, checkpoint_root: Path, output_mode: str = "append"):
    return (
        df.writeStream.format("delta")
        .queryName(name)
        .outputMode(output_mode)
        .option("checkpointLocation", str(checkpoint_root / name))
        .trigger(processingTime=env("TRIGGER_INTERVAL", "10 seconds"))
        .start(str(output_path))
    )


def start_kafka_query(df: DataFrame, name: str, topic: str, checkpoint_root: Path):
    return (
        df.writeStream.format("kafka")
        .queryName(name)
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("topic", topic)
        .option("checkpointLocation", str(checkpoint_root / name))
        .trigger(processingTime=env("TRIGGER_INTERVAL", "10 seconds"))
        .start()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))

    delta_root = Path(env("DELTA_ROOT", "data/delta"))
    checkpoint_root = Path(env("CHECKPOINT_ROOT", "data/checkpoints"))
    delta_root.mkdir(parents=True, exist_ok=True)
    checkpoint_root.mkdir(parents=True, exist_ok=True)

    bronze, valid, invalid = prepare_stream(kafka_source(spark))
    anomalies = with_anomaly_flags(valid).filter(col("anomaly_reason") != "NORMAL")
    tumbling_1m, sliding_5m, sessions = build_aggregates(valid)

    dlq_payload = invalid.select(
        col("message_key").alias("key"),
        to_json(
            struct(
                col("validation_error"),
                col("raw_payload"),
                col("topic").alias("source_topic"),
                col("partition").alias("source_partition"),
                col("offset").alias("source_offset"),
                col("bronze_ingested_at"),
            )
        ).alias("value"),
    )

    anomaly_payload = anomalies.select(
        col("symbol").alias("key"),
        to_json(
            struct(
                "trade_id",
                "symbol",
                "side",
                "quantity",
                "price",
                "notional",
                "event_ts",
                "anomaly_reason",
                "severity",
                "reference_price",
                "price_move_pct",
            )
        ).alias("value"),
    )

    aggregate_payload = tumbling_1m.select(
        concat_ws("|", col("symbol"), col("window_start").cast("string")).alias("key"),
        to_json(
            struct(
                "symbol",
                "window_start",
                "window_end",
                "trade_count",
                "total_quantity",
                "total_notional",
                "buy_quantity",
                "sell_quantity",
                "min_price",
                "max_price",
                "vwap",
            )
        ).alias("value"),
    )

    queries = [
        start_delta_query(bronze, "bronze_raw_trades", delta_root / "bronze" / "raw_trades", checkpoint_root),
        start_delta_query(valid, "silver_valid_trades", delta_root / "silver" / "valid_trades", checkpoint_root),
        start_delta_query(anomalies, "gold_anomalies", delta_root / "gold" / "anomalies", checkpoint_root),
        start_delta_query(tumbling_1m, "gold_tumbling_1m", delta_root / "gold" / "tumbling_1m", checkpoint_root),
        start_delta_query(sliding_5m, "gold_sliding_5m", delta_root / "gold" / "sliding_5m", checkpoint_root),
        start_delta_query(sessions, "gold_sessions", delta_root / "gold" / "sessions", checkpoint_root),
        start_kafka_query(dlq_payload, "dlq_invalid_trades", env("KAFKA_DLQ_TOPIC", "stock-trades-dlq"), checkpoint_root),
        start_kafka_query(anomaly_payload, "kafka_anomalies", env("KAFKA_ANOMALY_TOPIC", "stock-anomalies"), checkpoint_root),
        start_kafka_query(aggregate_payload, "kafka_aggregates", env("KAFKA_AGGREGATE_TOPIC", "stock-aggregates"), checkpoint_root),
    ]

    print("started queries=" + ",".join(query.name for query in queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
