from __future__ import annotations

from dataclasses import dataclass

from stock_streaming_pipeline.events import SYMBOL_BASE_PRICES, TradeEvent


@dataclass(frozen=True)
class AnomalyDecision:
    is_anomaly: bool
    reason: str
    severity: str


def detect_trade_anomaly(
    event: TradeEvent,
    high_quantity_threshold: int = 25_000,
    high_notional_threshold: float = 1_000_000,
    price_move_threshold: float = 0.25,
) -> AnomalyDecision:
    base_price = SYMBOL_BASE_PRICES.get(event.symbol)

    if event.quantity >= high_quantity_threshold:
        return AnomalyDecision(True, "HIGH_QUANTITY", "high")

    if event.notional >= high_notional_threshold:
        return AnomalyDecision(True, "HIGH_NOTIONAL", "high")

    if base_price is not None:
        relative_move = abs(event.price - base_price) / base_price
        if relative_move >= price_move_threshold:
            return AnomalyDecision(True, "PRICE_DISLOCATION", "medium")

    return AnomalyDecision(False, "NORMAL", "none")

