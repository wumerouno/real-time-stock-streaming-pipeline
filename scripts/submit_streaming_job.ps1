param(
    [string]$Master = "local[*]"
)

$ErrorActionPreference = "Stop"

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0"

spark-submit `
  --master $Master `
  --packages $packages `
  spark/stock_streaming_job.py

