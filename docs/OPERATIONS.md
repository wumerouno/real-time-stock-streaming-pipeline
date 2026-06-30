# Operations Runbook

## Local Startup

```powershell
docker compose up -d zookeeper kafka kafka-ui spark-master spark-worker
docker compose run --rm topic-init
docker compose --profile runtime up producer
docker compose exec spark-master spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 `
  /opt/stock-pipeline/spark/stock_streaming_job.py
streamlit run dashboard/streamlit_app.py
```

## Observability

- Kafka UI: `http://localhost:8080`
- Spark UI: `http://localhost:4040`
- Delta outputs: `data/delta`
- Checkpoints: `data/checkpoints`
- DLQ topic: `stock-trades-dlq`

Check topic lag for Kafka consumers that commit offsets:

```powershell
python -m stock_streaming_pipeline.lag_monitor --topics stock-trades --json
```

Spark Structured Streaming stores source offsets in checkpoint files. Use Spark UI for micro-batch duration, input rows per second, processed rows per second, state rows, and watermark progress.

## Backpressure Playbook

Symptoms:

- Kafka input topic grows faster than Spark processed rows.
- Spark micro-batch duration exceeds `TRIGGER_INTERVAL`.
- State store rows grow continuously.

Actions:

- Lower `EVENT_RATE_PER_SECOND`.
- Lower `MAX_OFFSETS_PER_TRIGGER` to stabilize each batch, or raise it if Spark has unused capacity.
- Increase Spark executor cores and memory.
- Increase Kafka partitions and match Spark parallelism.
- Increase watermark delay only if late data is being dropped and state size is acceptable.

## Failure Handling

- Invalid events go to `stock-trades-dlq` with validation reason and raw payload.
- Keep checkpoints when restarting Spark; deleting checkpoints reprocesses from configured Kafka offsets.
- Reprocess DLQ records only after fixing the schema or producer defect that caused the failure.
