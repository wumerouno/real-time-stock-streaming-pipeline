from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition

from stock_streaming_pipeline.settings import load_settings


@dataclass(frozen=True)
class PartitionLag:
    topic: str
    partition: int
    committed_offset: int | None
    high_watermark: int
    lag: int | None


def collect_lag(bootstrap_servers: str, group_id: str, topics: list[str]) -> list[PartitionLag]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
            "default.topic.config": {"auto.offset.reset": "earliest"},
        }
    )
    try:
        metadata = consumer.list_topics(timeout=10)
        partitions: list[TopicPartition] = []
        for topic in topics:
            if topic not in metadata.topics:
                raise KafkaException(f"topic does not exist: {topic}")
            partitions.extend(
                TopicPartition(topic, partition)
                for partition in sorted(metadata.topics[topic].partitions)
            )

        committed = consumer.committed(partitions, timeout=10)
        lag_rows: list[PartitionLag] = []
        for topic_partition in committed:
            low, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
            committed_offset = topic_partition.offset if topic_partition.offset >= 0 else None
            lag = None if committed_offset is None else max(high - committed_offset, 0)
            lag_rows.append(
                PartitionLag(
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                    committed_offset=committed_offset,
                    high_watermark=high,
                    lag=lag,
                )
            )
        return lag_rows
    finally:
        consumer.close()


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Report Kafka consumer lag for configured topics.")
    parser.add_argument("--bootstrap-servers", default=settings.kafka.bootstrap_servers)
    parser.add_argument("--group-id", default=settings.kafka.consumer_group)
    parser.add_argument(
        "--topics",
        default=settings.kafka.trade_topic,
        help="Comma-separated topic list. Spark stores offsets in checkpoints, so this is most useful for Kafka consumer groups that commit offsets.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rows = collect_lag(args.bootstrap_servers, args.group_id, args.topics.split(","))
    if args.json:
        print(json.dumps([asdict(row) for row in rows], indent=2))
        return

    print("topic partition committed high_watermark lag")
    for row in rows:
        committed = "-" if row.committed_offset is None else str(row.committed_offset)
        lag = "-" if row.lag is None else str(row.lag)
        print(f"{row.topic} {row.partition} {committed} {row.high_watermark} {lag}")


if __name__ == "__main__":
    main()

