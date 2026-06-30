from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from typing import Any

from confluent_kafka import KafkaException, Producer

from stock_streaming_pipeline.events import StockEventSimulator
from stock_streaming_pipeline.settings import load_settings


def _delivery_report(error: Any, message: Any) -> None:
    if error is not None:
        print(f"delivery_failed topic={message.topic()} partition={message.partition()} error={error}")


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Publish simulated stock trades to Kafka.")
    parser.add_argument("--bootstrap-servers", default=settings.kafka.bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka.trade_topic)
    parser.add_argument("--rate", type=int, default=settings.stream.event_rate_per_second)
    parser.add_argument("--max-events", type=int, default=0, help="0 means run until interrupted.")
    parser.add_argument("--out-of-order-rate", type=float, default=settings.stream.out_of_order_rate)
    parser.add_argument("--invalid-event-rate", type=float, default=settings.stream.invalid_event_rate)
    parser.add_argument("--anomaly-rate", type=float, default=settings.stream.anomaly_rate)
    parser.add_argument("--seed", type=int, default=None)
    return parser


def run(args: argparse.Namespace) -> int:
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "stock-event-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.num.messages": 10000,
        }
    )
    simulator = StockEventSimulator(
        out_of_order_rate=args.out_of_order_rate,
        invalid_event_rate=args.invalid_event_rate,
        anomaly_rate=args.anomaly_rate,
        seed=args.seed,
    )
    running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    interval = 1 / max(1, args.rate)
    published = 0
    started = time.time()

    try:
        while running and (args.max_events <= 0 or published < args.max_events):
            payload = simulator.next_event()
            symbol = str(payload.get("symbol", "UNKNOWN"))
            producer.produce(
                args.topic,
                key=symbol.encode("utf-8"),
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                headers={"schema_version": str(payload.get("schema_version", "unknown"))},
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            published += 1

            if published % max(args.rate, 1) == 0:
                elapsed = max(time.time() - started, 0.001)
                print(f"published={published} rate={published / elapsed:.1f}/s topic={args.topic}")

            time.sleep(interval)
    except BufferError:
        print("producer buffer is full; backpressure reached the producer", file=sys.stderr)
        return 2
    except KafkaException as exc:
        print(f"kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.flush(30)

    print(f"finished published={published}")
    return 0


def main() -> None:
    raise SystemExit(run(build_parser().parse_args()))


if __name__ == "__main__":
    main()

