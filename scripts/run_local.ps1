$ErrorActionPreference = "Stop"

docker compose up -d zookeeper kafka kafka-ui spark-master spark-worker

docker compose run --rm topic-init

Write-Host "Kafka UI: http://localhost:8080"
Write-Host "Start the producer in another terminal:"
Write-Host "  docker compose --profile runtime up producer"
Write-Host "Start Spark streaming:"
Write-Host "  docker compose exec spark-master spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 /opt/stock-pipeline/spark/stock_streaming_job.py"
