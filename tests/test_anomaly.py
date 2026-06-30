from __future__ import annotations

import unittest
from datetime import UTC, datetime

from stock_streaming_pipeline.anomaly import detect_trade_anomaly
from stock_streaming_pipeline.events import TradeEvent


def make_event(**overrides) -> TradeEvent:
    payload = {
        "trade_id": "trade-123456",
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": 100,
        "price": 210.0,
        "event_time": datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
        "ingest_time": datetime(2026, 6, 30, 12, 0, 1, tzinfo=UTC),
        "exchange": "NASDAQ",
        "trader_id": "trader-0001",
        "sequence": 1,
    }
    payload.update(overrides)
    return TradeEvent.from_payload(payload)


class AnomalyTests(unittest.TestCase):
    def test_high_quantity_anomaly(self) -> None:
        decision = detect_trade_anomaly(make_event(quantity=30_000))

        self.assertTrue(decision.is_anomaly)
        self.assertEqual(decision.reason, "HIGH_QUANTITY")
        self.assertEqual(decision.severity, "high")

    def test_price_dislocation_anomaly(self) -> None:
        decision = detect_trade_anomaly(make_event(price=350.0))

        self.assertTrue(decision.is_anomaly)
        self.assertEqual(decision.reason, "PRICE_DISLOCATION")

    def test_normal_trade_is_not_anomaly(self) -> None:
        decision = detect_trade_anomaly(make_event())

        self.assertFalse(decision.is_anomaly)
        self.assertEqual(decision.reason, "NORMAL")


if __name__ == "__main__":
    unittest.main()
