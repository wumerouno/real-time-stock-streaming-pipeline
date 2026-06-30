from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

SYMBOL_BASE_PRICES: dict[str, float] = {
    "AAPL": 210.0,
    "MSFT": 450.0,
    "NVDA": 126.0,
    "TSLA": 185.0,
    "AMZN": 190.0,
    "META": 505.0,
    "GOOGL": 176.0,
    "JPM": 205.0,
}

EXCHANGES = ("NYSE", "NASDAQ", "IEX", "CBOE")


@dataclass(frozen=True)
class TradeEvent:
    trade_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: int
    price: float
    event_time: datetime
    ingest_time: datetime
    exchange: str
    trader_id: str
    sequence: int
    schema_version: str = "1.0"

    @property
    def notional(self) -> float:
        return self.quantity * self.price

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TradeEvent":
        result = validate_trade_event(payload)
        if result.event is None:
            raise ValueError("; ".join(result.errors))
        return result.event


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    event: TradeEvent | None
    errors: list[str]


def validate_trade_event(payload: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    trade_id = _required_str(payload, "trade_id", errors, min_length=8)
    symbol = _required_str(payload, "symbol", errors, min_length=1, max_length=8)
    side = _required_str(payload, "side", errors, min_length=3, max_length=4)
    quantity = _required_int(payload, "quantity", errors, minimum=1, maximum=1_000_000)
    price = _required_float(payload, "price", errors, minimum=0.000001)
    event_time = _required_datetime(payload, "event_time", errors)
    ingest_time = _required_datetime(payload, "ingest_time", errors)
    exchange = _required_str(payload, "exchange", errors, min_length=2, max_length=16)
    trader_id = _required_str(payload, "trader_id", errors, min_length=3, max_length=32)
    sequence = _required_int(payload, "sequence", errors, minimum=0)
    schema_version = str(payload.get("schema_version", "1.0"))

    if side is not None and side not in {"BUY", "SELL"}:
        errors.append("side: must be BUY or SELL")

    if errors:
        return ValidationResult(valid=False, event=None, errors=errors)

    event = TradeEvent(
        trade_id=trade_id or "",
        symbol=(symbol or "").upper(),
        side=side,  # type: ignore[arg-type]
        quantity=quantity or 0,
        price=price or 0.0,
        event_time=event_time or datetime.now(tz=UTC),
        ingest_time=ingest_time or datetime.now(tz=UTC),
        exchange=exchange or "",
        trader_id=trader_id or "",
        sequence=sequence or 0,
        schema_version=schema_version,
    )
    return ValidationResult(valid=True, event=event, errors=[])


def _required_str(
    payload: dict[str, Any],
    field_name: str,
    errors: list[str],
    min_length: int,
    max_length: int | None = None,
) -> str | None:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{field_name}: required string")
        return None
    normalized = value.strip()
    if len(normalized) < min_length:
        errors.append(f"{field_name}: shorter than {min_length}")
    if max_length is not None and len(normalized) > max_length:
        errors.append(f"{field_name}: longer than {max_length}")
    return normalized


def _required_int(
    payload: dict[str, Any],
    field_name: str,
    errors: list[str],
    minimum: int,
    maximum: int | None = None,
) -> int | None:
    try:
        value = int(payload[field_name])
    except (KeyError, TypeError, ValueError):
        errors.append(f"{field_name}: required integer")
        return None

    if value < minimum:
        errors.append(f"{field_name}: must be >= {minimum}")
    if maximum is not None and value > maximum:
        errors.append(f"{field_name}: must be <= {maximum}")
    return value


def _required_float(
    payload: dict[str, Any],
    field_name: str,
    errors: list[str],
    minimum: float,
) -> float | None:
    try:
        value = float(payload[field_name])
    except (KeyError, TypeError, ValueError):
        errors.append(f"{field_name}: required number")
        return None

    if value < minimum:
        errors.append(f"{field_name}: must be >= {minimum}")
    return value


def _required_datetime(
    payload: dict[str, Any],
    field_name: str,
    errors: list[str],
) -> datetime | None:
    value = payload.get(field_name)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"{field_name}: invalid datetime")
            return None
    else:
        errors.append(f"{field_name}: required datetime")
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class StockEventSimulator:
    """Generates realistic trade events with controlled disorder and bad records."""

    def __init__(
        self,
        out_of_order_rate: float = 0.12,
        invalid_event_rate: float = 0.01,
        anomaly_rate: float = 0.015,
        seed: int | None = None,
    ) -> None:
        self.out_of_order_rate = out_of_order_rate
        self.invalid_event_rate = invalid_event_rate
        self.anomaly_rate = anomaly_rate
        self._random = random.Random(seed)
        self._sequence = 0

    def next_event(self) -> dict[str, Any]:
        self._sequence += 1
        symbol = self._random.choice(list(SYMBOL_BASE_PRICES))
        base_price = SYMBOL_BASE_PRICES[symbol]
        event_time = datetime.now(tz=UTC)

        if self._random.random() < self.out_of_order_rate:
            event_time -= timedelta(seconds=self._random.randint(5, 150))

        price = max(0.01, self._random.gauss(base_price, base_price * 0.012))
        quantity = max(1, int(self._random.expovariate(1 / 250)))

        if self._random.random() < self.anomaly_rate:
            if self._random.random() < 0.55:
                quantity *= self._random.randint(30, 120)
            else:
                price *= self._random.choice([0.55, 1.55, 2.2])

        payload: dict[str, Any] = {
            "trade_id": str(uuid.uuid4()),
            "symbol": symbol,
            "side": self._random.choice(["BUY", "SELL"]),
            "quantity": quantity,
            "price": round(price, 4),
            "event_time": event_time.isoformat(),
            "ingest_time": datetime.now(tz=UTC).isoformat(),
            "exchange": self._random.choice(EXCHANGES),
            "trader_id": f"trader-{self._random.randint(1, 500):04d}",
            "sequence": self._sequence,
            "schema_version": "1.0",
        }

        if self._random.random() < self.invalid_event_rate:
            return self._damage_payload(payload)

        return payload

    def _damage_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        damaged = dict(payload)
        fault = self._random.choice(["negative_quantity", "bad_side", "missing_symbol", "bad_price"])
        if fault == "negative_quantity":
            damaged["quantity"] = -abs(int(damaged["quantity"]))
        elif fault == "bad_side":
            damaged["side"] = "HOLD"
        elif fault == "missing_symbol":
            damaged.pop("symbol", None)
        elif fault == "bad_price":
            damaged["price"] = 0
        damaged["fault_injected"] = fault
        return damaged
