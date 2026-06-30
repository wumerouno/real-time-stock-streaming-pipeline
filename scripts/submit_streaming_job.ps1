param(
    [string]$Master = "local[*]",
    [string]$IvyCache = ""
)

$ErrorActionPreference = "Stop"

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0"

if ([string]::IsNullOrWhiteSpace($IvyCache)) {
    if ($IsWindows) {
        $IvyCache = Join-Path $env:TEMP "spark-ivy"
    } else {
        $IvyCache = "/tmp/.ivy2"
    }
}

New-Item -ItemType Directory -Force -Path $IvyCache | Out-Null

spark-submit `
  --master $Master `
  --conf "spark.jars.ivy=$IvyCache" `
  --packages $packages `
  spark/stock_streaming_job.py
