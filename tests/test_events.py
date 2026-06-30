from __future__ import annotations

import unittest
from datetime import UTC, datetime

from stock_streaming_pipeline.events import StockEventSimulator, validate_trade_event


def valid_payload() -> dict:
    return {
        "trade_id": "trade-123456",
        "symbol": "aapl",
        "side": "BUY",
        "quantity": 100,
        "price": 210.50,
        "event_time": datetime(2026, 6, 30, 12, 0, tzinfo=UTC).isoformat(),
        "ingest_time": datetime(2026, 6, 30, 12, 0, 1, tzinfo=UTC).isoformat(),
        "exchange": "NASDAQ",
        "trader_id": "trader-0001",
        "sequence": 1,
        "schema_version": "1.0",
    }


class EventValidationTests(unittest.TestCase):
    def test_valid_event_normalizes_symbol_and_computes_notional(self) -> None:
        result = validate_trade_event(valid_payload())

        self.assertTrue(result.valid)
        self.assertIsNotNone(result.event)
        assert result.event is not None
        self.assertEqual(result.event.symbol, "AAPL")
        self.assertEqual(result.event.notional, 21_050)

    def test_negative_quantity_is_invalid(self) -> None:
        payload = valid_payload()
        payload["quantity"] = -1

        result = validate_trade_event(payload)

        self.assertFalse(result.valid)
        self.assertTrue(any("quantity" in error for error in result.errors))

    def test_simulator_can_emit_out_of_order_events(self) -> None:
        simulator = StockEventSimulator(out_of_order_rate=1.0, invalid_event_rate=0, anomaly_rate=0, seed=7)

        payload = simulator.next_event()

        event_time = datetime.fromisoformat(payload["event_time"])
        ingest_time = datetime.fromisoformat(payload["ingest_time"])
        self.assertLess(event_time, ingest_time)


if __name__ == "__main__":
    unittest.main()
