from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Candle:
    start: datetime
    low: float
    high: float
    open: float
    close: float
    volume: float


@dataclass
class ProductInfo:
    product_id: str
    price: float
    base_increment: float
    quote_increment: float
    base_min_size: float
    quote_min_size: float
    trading_disabled: bool = False


@dataclass
class Position:
    side: str
    quantity: float
    entry_price: float
    position_size: float
    stop_loss: float
    take_profit: float
    trailing_stop: float
    highest_price: float
    opened_at: str
    mode: str
    lowest_price: float | None = None
    entry_order_id: str | None = None
    entry_fees_paid: float = 0.0
    entry_reason: str = ""
    market_state: str = "UNKNOWN"
    entry_indicators: dict[str, Any] = field(default_factory=dict)
    entry_quality_score: int = 0


@dataclass
class ClosedTrade:
    opened_at: str
    closed_at: str
    side: str
    quantity: float
    position_size: float
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    reason: str
    reason_tag: str
    result: str
    trade_duration_seconds: float
    fees_paid: float
    mode: str
    entry_order_id: str | None = None
    exit_order_id: str | None = None
    entry_reason: str = ""
    market_state: str = "UNKNOWN"
    entry_indicators: dict[str, Any] = field(default_factory=dict)
    entry_quality_score: int = 0

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BotState:
    cash: float
    peak_equity: float
    day_start_equity: float
    day_marker: str
    position: Position | None = None
    last_loss_at: str | None = None
    last_signal_at: str | None = None
    entry_timestamps: list[str] = field(default_factory=list)
    consecutive_losses: int = 0
    max_drawdown_pct_seen: float = 0.0
    trading_disabled: bool = False
    trading_disabled_reason: str | None = None
    trading_paused_until: str | None = None
    closed_trades: list[ClosedTrade] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "BotState":
        position_payload = payload.get("position")
        if position_payload:
            legacy_position = dict(position_payload)
            legacy_position.setdefault("side", "long")
            legacy_position.setdefault("lowest_price", legacy_position.get("entry_price"))
            legacy_position.setdefault(
                "position_size",
                float(legacy_position.get("entry_price", 0.0)) * float(legacy_position.get("quantity", 0.0)),
            )
            legacy_position.setdefault("entry_reason", "")
            legacy_position.setdefault("market_state", "UNKNOWN")
            legacy_position.setdefault("entry_indicators", {})
            legacy_position.setdefault("entry_quality_score", 0)
            position = Position(**legacy_position)
        else:
            position = None

        closed_trades = []
        for trade in payload.get("closed_trades", []):
            legacy_trade = dict(trade)
            legacy_trade.setdefault("side", "long")
            legacy_trade.setdefault(
                "position_size",
                float(legacy_trade.get("entry_price", 0.0)) * float(legacy_trade.get("quantity", 0.0)),
            )
            legacy_trade.setdefault("reason_tag", legacy_trade.get("reason", "unknown"))
            pnl_value = float(legacy_trade.get("pnl", 0.0))
            legacy_trade.setdefault("result", "WIN" if pnl_value > 0 else "LOSS" if pnl_value < 0 else "FLAT")
            legacy_trade.setdefault("entry_reason", "")
            legacy_trade.setdefault("market_state", "UNKNOWN")
            legacy_trade.setdefault("entry_indicators", {})
            legacy_trade.setdefault("entry_quality_score", 0)
            opened_at = legacy_trade.get("opened_at")
            closed_at = legacy_trade.get("closed_at")
            duration_seconds = 0.0
            if opened_at and closed_at:
                try:
                    duration_seconds = (
                        datetime.fromisoformat(str(closed_at)) - datetime.fromisoformat(str(opened_at))
                    ).total_seconds()
                except ValueError:
                    duration_seconds = 0.0
            legacy_trade.setdefault("trade_duration_seconds", duration_seconds)
            closed_trades.append(ClosedTrade(**legacy_trade))
        return cls(
            cash=float(payload["cash"]),
            peak_equity=float(payload["peak_equity"]),
            day_start_equity=float(payload["day_start_equity"]),
            day_marker=str(payload["day_marker"]),
            position=position,
            last_loss_at=payload.get("last_loss_at"),
            last_signal_at=payload.get("last_signal_at"),
            entry_timestamps=[str(item) for item in payload.get("entry_timestamps", [])],
            consecutive_losses=int(payload.get("consecutive_losses", 0)),
            max_drawdown_pct_seen=float(payload.get("max_drawdown_pct_seen", 0.0)),
            trading_disabled=bool(payload.get("trading_disabled", False)),
            trading_disabled_reason=payload.get("trading_disabled_reason"),
            trading_paused_until=payload.get("trading_paused_until"),
            closed_trades=closed_trades,
        )


@dataclass
class TradeResult:
    price: float
    quantity: float
    fees_paid: float
    order_id: str | None = None


@dataclass
class StrategyDecision:
    action: str
    reason: str
    stop_loss: float | None = None
    take_profit: float | None = None
    trailing_stop: float | None = None
    market_state: str = "UNKNOWN"
    indicators: dict[str, Any] = field(default_factory=dict)
