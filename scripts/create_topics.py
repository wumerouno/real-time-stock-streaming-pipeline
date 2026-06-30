from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

from stock_streaming_pipeline.settings import load_settings


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Create Kafka topics for the streaming project.")
    parser.add_argument("--bootstrap-servers", default=settings.kafka.bootstrap_servers)
    parser.add_argument("--config", default="config/topics.yml")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    topics = []
    for name, spec in config["topics"].items():
        topics.append(
            NewTopic(
                topic=name,
                num_partitions=int(spec["partitions"]),
                replication_factor=int(spec["replication_factor"]),
                config=spec.get("config", {}),
            )
        )

    futures = admin.create_topics(topics, operation_timeout=30)
    for name, future in futures.items():
        try:
            future.result()
            print(f"created topic={name}")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                print(f"exists topic={name}")
            else:
                raise


if __name__ == "__main__":
    main()
