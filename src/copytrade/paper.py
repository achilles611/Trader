from __future__ import annotations

import copy
import random
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable

from .config import CopyTradeConfig, RiskConfig, SizingConfig
from .models import CopySignal, ExecutionAttempt, ExecutionFill, PositionEvent, PositionEventType, VirtualTargetPosition, as_utc, stable_id
from .storage import CopyTradeStore


@dataclass(frozen=True)
class SizingDecision:
    allocation_fraction: float
    size_ratio: float | None
    target_size_fraction: float | None
    bucket: str


class TargetSizeClassifier:
    """Prior-only target sizing; invalid/stale equity never trains the model."""

    def __init__(self, config: SizingConfig) -> None:
        self.config = config
        self._history: dict[str, list[float]] = {}

    def classify(
        self, target_wallet: str, initial_notional: float, target_equity: float | None,
        equity_source: str = "exact", equity_age_seconds: float | None = None,
    ) -> SizingDecision:
        valid = (
            target_equity is not None and target_equity > 0 and initial_notional > 0
            and equity_source in self.config.accepted_equity_sources
            and (equity_age_seconds is None or equity_age_seconds <= self.config.max_equity_age_seconds)
        )
        if not valid:
            return SizingDecision(self.config.fallback_fraction, None, None, "fallback")
        fraction = abs(initial_notional) / target_equity
        history = self._history.setdefault(target_wallet.lower(), [])
        if len(history) < self.config.min_history:
            decision = SizingDecision(self.config.fallback_fraction, None, fraction, "fallback")
        elif self.config.mode == "quantile":
            ordered = sorted(history)
            low, high = _quantile(ordered, self.config.quantiles[0]), _quantile(ordered, self.config.quantiles[1])
            if fraction < low:
                decision = SizingDecision(self.config.small_fraction, fraction / max(low, 1e-12), fraction, "small")
            elif fraction > high:
                decision = SizingDecision(self.config.large_fraction, fraction / max(high, 1e-12), fraction, "large")
            else:
                decision = SizingDecision(self.config.medium_fraction, 1.0, fraction, "medium")
        else:
            ordered = sorted(history)
            mid = len(ordered) // 2
            baseline = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2
            ratio = fraction / max(baseline, 1e-12)
            if ratio < self.config.small_ratio_max:
                decision = SizingDecision(self.config.small_fraction, ratio, fraction, "small")
            elif ratio > self.config.large_ratio_min:
                decision = SizingDecision(self.config.large_fraction, ratio, fraction, "large")
            else:
                decision = SizingDecision(self.config.medium_fraction, ratio, fraction, "medium")
        history.append(fraction)
        return decision

    def seed(self, target_wallet: str, fractions: Iterable[float]) -> None:
        self._history[target_wallet.lower()] = [float(value) for value in fractions if value > 0]


def _quantile(values: list[float], fraction: float) -> float:
    point = (len(values) - 1) * fraction
    low = int(point)
    high = min(len(values) - 1, low + 1)
    return values[low] + (values[high] - values[low]) * (point - low)


class SignalFactory:
    def __init__(self, sizing: TargetSizeClassifier, config: CopyTradeConfig) -> None:
        self.sizing, self.config = sizing, config

    def from_position_event(self, event: PositionEvent, available_capital: float) -> list[CopySignal]:
        if event.event_type is PositionEventType.OPEN:
            return self._entry(event, available_capital, "open") if self.config.sizing.copy_initial_entries else []
        if event.event_type is PositionEventType.ADD:
            return self._entry(event, available_capital, "add") if self.config.sizing.copy_target_adds else []
        if event.event_type is PositionEventType.REDUCE:
            return [self._exit(event, "reduce")]
        if event.event_type is PositionEventType.CLOSE:
            return [self._exit(event, "close")]
        # Retained for compatibility with external callers creating legacy FLIP events.
        signals = [self._exit(event, "close", "short" if event.direction == "long" else "long")]
        return signals + (self._entry(event, available_capital, "open") if self.config.sizing.copy_initial_entries else [])

    def _entry(self, event: PositionEvent, available_capital: float, action: str) -> list[CopySignal]:
        initial_notional = event.initial_delta_notional or event.notional
        decision = self.sizing.classify(event.target_wallet, initial_notional, event.target_equity, event.equity_source, event.equity_age_seconds)
        capital = max(0.0, available_capital) * decision.allocation_fraction
        return [CopySignal(
            signal_id=stable_id("signal", event.event_id, action), target_wallet=event.target_wallet,
            campaign_id=event.campaign_id, source_event_id=event.event_id, symbol=event.symbol, action=action,
            direction=event.direction, target_price=event.price, target_quantity=abs(event.delta_quantity),
            target_notional=event.notional, allocation_fraction=decision.allocation_fraction, requested_capital=capital,
            created_at=event.event_timestamp, source_event_timestamp=event.event_timestamp, size_ratio=decision.size_ratio,
            reason=f"size_{decision.bucket}", target_position_before=event.before_quantity,
            target_equity=event.target_equity, equity_source=event.equity_source, equity_age_seconds=event.equity_age_seconds,
        )]

    @staticmethod
    def _exit(event: PositionEvent, action: str, direction: str | None = None) -> CopySignal:
        return CopySignal(
            signal_id=stable_id("signal", event.event_id, action, direction or event.direction), target_wallet=event.target_wallet,
            campaign_id=event.campaign_id, source_event_id=event.event_id, symbol=event.symbol, action=action,
            direction=direction or event.direction, target_price=event.price, target_quantity=abs(event.delta_quantity),
            target_notional=event.notional, allocation_fraction=0.0, requested_capital=0.0, created_at=event.event_timestamp,
            source_event_timestamp=event.event_timestamp, reason=f"target_{action}", target_position_before=event.before_quantity,
            target_equity=event.target_equity, equity_source=event.equity_source, equity_age_seconds=event.equity_age_seconds,
        )


@dataclass
class PaperPortfolio:
    initial_capital: float
    cash: float | None = None
    sleeves: dict[str, VirtualTargetPosition] = field(default_factory=dict)
    peak_equity: float | None = None
    max_drawdown_fraction: float = 0.0
    closed_results: list[tuple[str, float, object]] = field(default_factory=list)
    realized_results: list[tuple[str, float, object]] = field(default_factory=list)
    start_of_day_equity: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.cash is None:
            self.cash = self.initial_capital
        if self.peak_equity is None:
            self.peak_equity = self.equity

    @property
    def committed_capital(self) -> float:
        return sum(s.remaining_capital for s in self.sleeves.values() if s.is_open)

    @property
    def unrealized_pnl(self) -> float:
        return sum(s.unrealized_pnl for s in self.sleeves.values() if s.is_open)

    @property
    def equity(self) -> float:
        # cash excludes reserved principal; adding it once plus current marked
        # P&L is the paper accounting identity.
        return float(self.cash or 0.0) + self.committed_capital + self.unrealized_pnl

    @property
    def drawdown_fraction(self) -> float:
        peak = max(float(self.peak_equity or self.initial_capital), 1e-12)
        return max(0.0, peak - self.equity) / peak

    def update_peak(self, timestamp: object | None = None) -> None:
        self.peak_equity = max(float(self.peak_equity or 0.0), self.equity)
        self.max_drawdown_fraction = max(self.max_drawdown_fraction, self.drawdown_fraction)
        if timestamp is not None:
            self.start_of_day_equity.setdefault(str(as_utc(timestamp).date()), self.equity)

    def mark(self, symbol: str, price: float, timestamp: object) -> None:
        for sleeve in self.sleeves.values():
            if sleeve.is_open and sleeve.symbol == symbol:
                sleeve.current_mark = price
                sign = 1.0 if sleeve.direction == "long" else -1.0
                sleeve.unrealized_pnl = (price - sleeve.entry_price) * sleeve.quantity * sign
                sleeve.max_drawdown = max(sleeve.max_drawdown, max(0.0, -sleeve.unrealized_pnl - sleeve.realized_pnl))
                sleeve.updated_at = as_utc(timestamp)
        self.update_peak(timestamp)

    def cap_base(self, setting: str, timestamp: object) -> float:
        if setting == "initial_capital":
            return self.initial_capital
        if setting == "start_of_day_equity":
            return self.start_of_day_equity.get(str(as_utc(timestamp).date()), self.equity)
        return self.equity

    def target_committed(self, wallet: str) -> float:
        return sum(s.remaining_capital for s in self.sleeves.values() if s.is_open and s.target_wallet == wallet.lower())

    def symbol_committed(self, symbol: str) -> float:
        return sum(s.remaining_capital for s in self.sleeves.values() if s.is_open and s.symbol == symbol)

    def target_realized(self, wallet: str) -> float:
        return sum(value for target, value, _ in self.realized_results if target == wallet.lower())

    def daily_realized(self, timestamp: object) -> float:
        day = as_utc(timestamp).date()
        return sum(value for _, value, at in self.realized_results if as_utc(at).date() == day)

    @property
    def consecutive_losses(self) -> int:
        count = 0
        for _, value, _ in reversed(self.closed_results):
            if value >= 0:
                break
            count += 1
        return count


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    capital: float
    reason: str


class CopyRiskEngine:
    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate_entry(self, signal: CopySignal, portfolio: PaperPortfolio, received_at: object, execution_price: float) -> RiskDecision:
        now = as_utc(received_at)
        if self.config.kill_switch_path.exists(): return RiskDecision(False, 0.0, "kill_switch")
        if (now - signal.source_event_timestamp).total_seconds() > self.config.max_signal_age_seconds: return RiskDecision(False, 0.0, "stale_signal")
        if self.config.symbol_allowlist and signal.symbol not in self.config.symbol_allowlist: return RiskDecision(False, 0.0, "symbol_not_allowlisted")
        if signal.symbol in self.config.symbol_blocklist: return RiskDecision(False, 0.0, "symbol_blocklisted")
        if self.config.max_leverage is not None and signal.target_leverage is not None and signal.target_leverage > self.config.max_leverage: return RiskDecision(False, 0.0, "leverage_limit")
        if portfolio.drawdown_fraction >= self.config.max_copy_drawdown_fraction: return RiskDecision(False, 0.0, "max_drawdown")
        if portfolio.consecutive_losses >= self.config.max_consecutive_losses: return RiskDecision(False, 0.0, "consecutive_losses")
        cap_base = portfolio.cap_base(self.config.risk_cap_base, now)
        if portfolio.target_realized(signal.target_wallet) <= -cap_base * self.config.target_loss_stop_fraction: return RiskDecision(False, 0.0, "target_loss_stop")
        if portfolio.daily_realized(now) <= -cap_base * self.config.daily_loss_stop_fraction: return RiskDecision(False, 0.0, "daily_loss_stop")
        if abs(execution_price - signal.target_price) / max(signal.target_price, 1e-12) * 10_000 > self.config.max_price_deviation_bps: return RiskDecision(False, 0.0, "price_deviation")
        if sum(s.is_open for s in portfolio.sleeves.values()) >= self.config.max_simultaneous_virtual_campaigns: return RiskDecision(False, 0.0, "max_simultaneous_campaigns")
        max_total = cap_base * self.config.max_total_committed_fraction
        max_target = cap_base * self.config.max_capital_per_target_fraction
        max_symbol = cap_base * self.config.max_capital_per_symbol_fraction
        capital = min(signal.requested_capital, max(0.0, portfolio.cash or 0.0),
                      max(0.0, max_total - portfolio.committed_capital),
                      max(0.0, max_target - portfolio.target_committed(signal.target_wallet)),
                      max(0.0, max_symbol - portfolio.symbol_committed(signal.symbol)))
        if capital + 1e-12 < signal.requested_capital and self.config.insufficient_capital_action == "skip": return RiskDecision(False, 0.0, "insufficient_capital")
        return RiskDecision(capital > 0, capital, "approved" if capital >= signal.requested_capital else "scaled_capital" if capital > 0 else "insufficient_capital")


class PaperExecutionEngine:
    """Deterministic paper-only sleeve execution with transaction-backed replay safety."""

    def __init__(self, config: CopyTradeConfig, store: CopyTradeStore | None = None) -> None:
        self.config, self.store = config, store
        self.portfolio = PaperPortfolio(config.capital.initial_capital)
        self.equity_history = [self.portfolio.equity]
        self.risk, self.rng = CopyRiskEngine(config.risk), random.Random(config.paper_execution.random_seed)
        self._pending_fills: list[ExecutionFill] = []

    def restore(
        self, sleeves: Iterable[VirtualTargetPosition], snapshot: dict[str, Any] | None = None,
        realized_results: Iterable[tuple[str, float, object]] | None = None,
    ) -> None:
        restored = list(sleeves)
        self.portfolio.sleeves = {s.sleeve_id: s for s in restored}
        self.portfolio.cash = self.portfolio.initial_capital - sum(s.remaining_capital for s in restored if s.is_open) - sum(s.entry_fee for s in restored) + sum(s.realized_pnl for s in restored)
        self.portfolio.closed_results = [(s.target_wallet, s.realized_pnl - s.entry_fee, s.closed_at or s.updated_at) for s in restored if not s.is_open]
        self.portfolio.realized_results = list(realized_results) if realized_results is not None else [
            (s.target_wallet, s.realized_pnl, s.updated_at) for s in restored if s.realized_pnl
        ]
        if snapshot:
            self.portfolio.cash = float(snapshot["cash"])
            self.portfolio.peak_equity = float(snapshot.get("peak_equity") or snapshot["equity"])
            self.portfolio.max_drawdown_fraction = float(snapshot.get("max_drawdown_fraction") or snapshot["drawdown_fraction"])
        else:
            self.portfolio.peak_equity = max(self.portfolio.initial_capital, self.portfolio.equity)
        self.equity_history = [self.portfolio.equity]

    def mark_to_market(self, symbol: str, market_price: float, timestamp: object) -> None:
        if market_price > 0:
            self.portfolio.mark(symbol, market_price, timestamp)
            self.equity_history.append(self.portfolio.equity)

    def process_signal(
        self, signal: CopySignal, *, received_at: object | None = None, market_price: float | None = None,
        market_metadata: dict[str, object] | None = None, fault_hook: Any = None,
    ) -> ExecutionAttempt:
        existing = getattr(self.store, "get_execution_attempt", lambda _id: None)(signal.signal_id) if self.store else None
        if existing:
            return existing
        original = copy.deepcopy(self.portfolio)
        self._pending_fills = []
        self._market_metadata = dict(market_metadata or {})
        received = as_utc(received_at or signal.source_event_timestamp + timedelta(milliseconds=self.config.paper_execution.detection_latency_ms))
        order_time = received + timedelta(milliseconds=self.config.paper_execution.order_latency_ms)
        price = market_price if market_price and market_price > 0 else signal.target_price
        self.mark_to_market(signal.symbol, price, received)
        attempt = self._exit(signal, price, received, order_time) if signal.action in {"reduce", "close"} else self._entry(signal, price, received, order_time)
        snapshot = self._snapshot() if attempt.status == "filled" else None
        if self.store:
            try:
                committed = self.store.commit_execution(signal, attempt, self.portfolio.sleeves.values(), self._pending_fills, snapshot=snapshot, fault_hook=fault_hook)  # type: ignore[attr-defined]
                if not committed:
                    self.portfolio = original
                    return self.store.get_execution_attempt(signal.signal_id)  # type: ignore[attr-defined]
            except Exception:
                self.portfolio = original
                raise
        self.equity_history.append(self.portfolio.equity)
        return attempt

    def _entry(self, signal: CopySignal, price: float, received: object, order_time: object) -> ExecutionAttempt:
        execution_price = self._execution_price(price, signal.direction, opening=True)
        decision = self.risk.evaluate_entry(signal, self.portfolio, received, execution_price)
        attempt_id = stable_id("attempt", signal.signal_id, received, "entry")
        if not decision.allowed: return self._attempt(attempt_id, signal, decision.reason, "skipped", received)
        if self.rng.random() < self.config.paper_execution.missed_trade_rate: return self._attempt(attempt_id, signal, "simulated_missed_trade", "missed", received)
        quantity = self._quantize_quantity(decision.capital / execution_price)
        notional, fee = quantity * execution_price, quantity * execution_price * self.config.paper_execution.fee_rate
        if notional < self.config.paper_execution.min_order_notional or quantity <= 0: return self._attempt(attempt_id, signal, "minimum_order_notional", "skipped", received)
        if (self.portfolio.cash or 0.0) + 1e-12 < decision.capital + fee: return self._attempt(attempt_id, signal, "insufficient_cash_after_fee", "skipped", received)
        sleeve_id = stable_id("sleeve", signal.target_wallet, signal.campaign_id or signal.source_event_id, signal.symbol, signal.direction)
        sleeve = VirtualTargetPosition(sleeve_id=sleeve_id, target_wallet=signal.target_wallet.lower(), campaign_id=signal.campaign_id,
            symbol=signal.symbol, direction=signal.direction, quantity=quantity, entry_price=execution_price,
            allocated_capital=decision.capital, remaining_capital=decision.capital, entry_fee=fee, opened_at=as_utc(order_time),
            updated_at=as_utc(order_time), target_entry_price=signal.target_price, current_mark=price)
        self.portfolio.cash = (self.portfolio.cash or 0.0) - decision.capital - fee
        self.portfolio.sleeves[sleeve_id] = sleeve
        self.portfolio.update_peak(order_time)
        entry_deterioration = (execution_price - signal.target_price) / max(signal.target_price, 1e-12) * 10_000 * (1 if signal.direction == "long" else -1)
        self._pending_fills.append(ExecutionFill(stable_id("execfill", attempt_id), attempt_id, sleeve_id, execution_price, quantity, notional, fee,
            self.config.paper_execution.slippage_bps, as_utc(order_time), {"target_price": signal.target_price, "allocation_reason": decision.reason,
            "market_price": price, "entry_deterioration_bps": entry_deterioration, **getattr(self, "_market_metadata", {})}))
        return self._attempt(attempt_id, signal, decision.reason, "filled", received, order_time)

    def _exit(self, signal: CopySignal, price: float, received: object, order_time: object) -> ExecutionAttempt:
        attempt_id = stable_id("attempt", signal.signal_id, received, "exit")
        candidates = [s for s in self.portfolio.sleeves.values() if s.is_open and s.target_wallet == signal.target_wallet.lower() and s.symbol == signal.symbol]
        if not candidates: return self._attempt(attempt_id, signal, "no_virtual_sleeve", "skipped", received)
        fraction = 1.0 if signal.action == "close" else min(1.0, signal.target_quantity / max(abs(signal.target_position_before), 1e-12))
        for sleeve in candidates:
            exit_price = self._execution_price(price, sleeve.direction, opening=False)
            closing_quantity = self._quantize_quantity(sleeve.quantity * fraction)
            if closing_quantity <= 0: continue
            actual_fraction = min(1.0, closing_quantity / max(sleeve.quantity, 1e-12))
            released = sleeve.remaining_capital * actual_fraction
            gross = (exit_price - sleeve.entry_price) * closing_quantity * (1 if sleeve.direction == "long" else -1)
            fee = closing_quantity * exit_price * self.config.paper_execution.fee_rate
            net = gross - fee
            self.portfolio.cash = (self.portfolio.cash or 0.0) + released + net
            sleeve.quantity, sleeve.remaining_capital = max(0.0, sleeve.quantity - closing_quantity), max(0.0, sleeve.remaining_capital - released)
            sleeve.realized_pnl += net
            sleeve.exit_fee += fee
            sleeve.current_mark, sleeve.unrealized_pnl, sleeve.updated_at = price, (price - sleeve.entry_price) * sleeve.quantity * (1 if sleeve.direction == "long" else -1), as_utc(order_time)
            self.portfolio.realized_results.append((sleeve.target_wallet, net, as_utc(order_time)))
            if sleeve.quantity <= 1e-12:
                sleeve.quantity = sleeve.remaining_capital = sleeve.unrealized_pnl = 0.0
                sleeve.closed_at = as_utc(order_time)
                self.portfolio.closed_results.append((sleeve.target_wallet, sleeve.realized_pnl - sleeve.entry_fee, as_utc(order_time)))
            exit_deterioration = (exit_price - signal.target_price) / max(signal.target_price, 1e-12) * 10_000 * (-1 if sleeve.direction == "long" else 1)
            self._pending_fills.append(ExecutionFill(stable_id("execfill", attempt_id, sleeve.sleeve_id), attempt_id, sleeve.sleeve_id,
                exit_price, closing_quantity, closing_quantity * exit_price, fee, self.config.paper_execution.slippage_bps,
                as_utc(order_time), {"target_price": signal.target_price, "fraction": actual_fraction, "market_price": price,
                                     "realized_pnl": net, "target_wallet": sleeve.target_wallet,
                                     "exit_deterioration_bps": exit_deterioration, **getattr(self, "_market_metadata", {})}))
        if not self._pending_fills: return self._attempt(attempt_id, signal, "quantity_rounds_to_zero", "skipped", received)
        self.portfolio.update_peak(order_time)
        return self._attempt(attempt_id, signal, "filled", "filled", received, order_time)

    def _snapshot(self) -> dict[str, float]:
        return {"cash": float(self.portfolio.cash or 0.0), "equity": self.portfolio.equity,
                "committed_capital": self.portfolio.committed_capital, "drawdown_fraction": self.portfolio.drawdown_fraction,
                "peak_equity": float(self.portfolio.peak_equity or self.portfolio.equity), "max_drawdown_fraction": self.portfolio.max_drawdown_fraction}

    def _attempt(self, attempt_id: str, signal: CopySignal, reason: str, status: str, received: object, order_time: object | None = None) -> ExecutionAttempt:
        received_at, order_at = as_utc(received), as_utc(order_time) if order_time else None
        return ExecutionAttempt(attempt_id, signal.signal_id, signal.target_wallet, signal.symbol, signal.action, signal.requested_capital,
            status, reason, signal.source_event_timestamp, received_at, received_at, order_at, order_at,
            max(0.0, (received_at - signal.source_event_timestamp).total_seconds() * 1000), 0.0)

    def _execution_price(self, price: float, direction: str, *, opening: bool) -> float:
        bps = self.config.paper_execution.slippage_bps / 10_000
        return price * (1 + bps if (direction == "long") == opening else 1 - bps)

    def _quantize_quantity(self, quantity: float) -> float:
        return float(Decimal(str(quantity)).quantize(Decimal("1").scaleb(-self.config.paper_execution.quantity_precision), rounding=ROUND_DOWN))
