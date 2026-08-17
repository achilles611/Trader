"""Short-horizon simulation/shadow decision gate above wallet observations.

No object in this module submits an order.  A raw wallet event is merely
provenance; entry requires validated indicators, a versioned model, positive
net edge, effective confidence, and independent risk limits.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Mapping
from uuid import uuid4

from .confidence import ConfidenceSnapshot
from .science_repository import ScientificRepository


class DecisionType(StrEnum):
    ENTER = "ENTER"
    HOLD = "HOLD"
    EXIT = "EXIT"
    SKIP = "SKIP"


@dataclass(frozen=True)
class DecisionRiskPolicy:
    account_risk_budget: float
    available_equity: float
    max_leverage: float
    max_notional: float
    adverse_move_floor: float = 0.002
    entry_min_effective_confidence: float = 0.60
    exit_effective_confidence: float = 0.52
    max_position_age_seconds: float = 600.0

    def __post_init__(self) -> None:
        if self.account_risk_budget <= 0 or self.available_equity <= 0 or self.max_leverage <= 0 or self.max_notional <= 0:
            raise ValueError("Risk policy values must be positive.")
        if not 0 <= self.exit_effective_confidence < self.entry_min_effective_confidence <= 1:
            raise ValueError("Entry confidence must exceed exit confidence for hysteresis.")
        if not 0 < self.max_position_age_seconds <= 600:
            raise ValueError("Short-horizon positions may not exceed 600 seconds.")


@dataclass(frozen=True)
class DecisionInput:
    timestamp: str
    symbol: str
    side: str
    model_id: str
    model_version: int
    model_state: str
    active_indicator_versions: tuple[tuple[str, int], ...]
    indicator_values: Mapping[str, float]
    confidence: ConfidenceSnapshot
    expected_gross_edge: float
    estimated_fees: float
    estimated_spread: float
    estimated_slippage: float
    estimated_market_impact: float
    estimated_latency_cost: float
    predicted_adverse_move_quantile: float
    predicted_mfe: float | None
    signal_age_seconds: float
    alpha_survival: float
    market_regime: str
    position_open: bool = False
    position_age_seconds: float = 0.0
    hard_risk_exit: bool = False
    regime_invalidated: bool = False
    source_wallet_action: Mapping[str, Any] | None = None
    source_wallet_leverage: float | None = None
    source_exit_observed: bool = False
    extra_reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def expected_net_edge(self) -> float:
        return self.expected_gross_edge - self.estimated_fees - self.estimated_spread - self.estimated_slippage - self.estimated_market_impact - self.estimated_latency_cost


@dataclass(frozen=True)
class DecisionRecord:
    decision_id: str
    decision: DecisionType
    reasons: tuple[str, ...]
    risk_budget: float
    suggested_notional: float
    derived_leverage: float
    payload: Mapping[str, Any]


class ScientificDecisionEngine:
    def __init__(self, repository: ScientificRepository | None = None, *, execution_mode: str = "SIMULATION_SHADOW") -> None:
        if execution_mode != "SIMULATION_SHADOW":
            raise ValueError("Scientific decisions are simulation/shadow only.")
        self.repository = repository
        self.execution_mode = execution_mode

    def decide(self, item: DecisionInput, policy: DecisionRiskPolicy, *, decision_id: str | None = None) -> DecisionRecord:
        if item.side not in {"long", "short"}:
            return self._record(decision_id, item, DecisionType.SKIP, ("invalid_model_side",), policy, 0.0, 0.0)
        if item.position_open:
            return self._exit_or_hold(decision_id, item, policy)
        reasons = list(item.extra_reasons)
        if item.model_state not in {"ACTIVE_SIMULATION", "SHADOW"}:
            reasons.append("model_not_execution_eligible")
        if not item.active_indicator_versions:
            reasons.append("no_validated_indicators")
        if item.expected_net_edge <= 0:
            reasons.append("non_positive_expected_net_edge")
        if item.confidence.effective_confidence < policy.entry_min_effective_confidence:
            reasons.append("effective_confidence_below_entry_threshold")
        if item.regime_invalidated:
            reasons.append("regime_invalidated")
        if item.signal_age_seconds < 0 or item.alpha_survival < 0:
            reasons.append("invalid_signal_evidence")
        if reasons:
            return self._record(decision_id, item, DecisionType.SKIP, tuple(reasons), policy, 0.0, 0.0)
        risk_budget = policy.account_risk_budget * item.confidence.model_confidence * item.confidence.effective_confidence
        adverse = max(item.predicted_adverse_move_quantile, policy.adverse_move_floor)
        unbounded_notional = risk_budget / adverse
        max_by_leverage = policy.available_equity * policy.max_leverage
        notional = min(unbounded_notional, policy.max_notional, max_by_leverage)
        leverage = notional / policy.available_equity
        if notional <= 0:
            return self._record(decision_id, item, DecisionType.SKIP, ("risk_budget_zero",), policy, risk_budget, 0.0)
        return self._record(decision_id, item, DecisionType.ENTER, ("validated_indicator_model_edge_risk_gate_passed",), policy, risk_budget, notional, leverage)

    def _exit_or_hold(self, decision_id: str | None, item: DecisionInput, policy: DecisionRiskPolicy) -> DecisionRecord:
        reasons = list(item.extra_reasons)
        if item.hard_risk_exit:
            reasons.append("hard_risk_exit")
        elif item.position_age_seconds >= policy.max_position_age_seconds:
            reasons.append("maximum_position_age_reached")
        elif item.expected_net_edge <= 0:
            reasons.append("expected_net_edge_non_positive")
        elif item.confidence.effective_confidence < policy.exit_effective_confidence:
            reasons.append("effective_confidence_below_exit_threshold")
        elif item.regime_invalidated:
            reasons.append("regime_invalidated")
        if item.source_exit_observed:
            reasons.append("source_exit_observed_as_evidence")
        decision = DecisionType.EXIT if any(reason != "source_exit_observed_as_evidence" for reason in reasons) else DecisionType.HOLD
        if decision is DecisionType.HOLD:
            reasons.append("position_still_meets_hysteresis_and_edge_requirements")
        return self._record(decision_id, item, decision, tuple(reasons), policy, 0.0, 0.0)

    def _record(self, decision_id: str | None, item: DecisionInput, decision: DecisionType, reasons: tuple[str, ...], policy: DecisionRiskPolicy, risk_budget: float, notional: float, leverage: float = 0.0) -> DecisionRecord:
        identifier = decision_id or uuid4().hex
        payload = {
            "timestamp": item.timestamp, "symbol": item.symbol, "side": item.side,
            "model_version": {"model_id": item.model_id, "version": item.model_version, "state": item.model_state},
            "active_indicator_versions": [{"indicator_id": identifier, "version": version} for identifier, version in item.active_indicator_versions],
            "indicator_values": dict(item.indicator_values), "model_confidence": item.confidence.model_confidence,
            "trade_confidence": item.confidence.trade_confidence, "effective_confidence": item.confidence.effective_confidence,
            "confidence_explanation": item.confidence.explanation, "expected_gross_edge": item.expected_gross_edge,
            "estimated_costs": {"fees": item.estimated_fees, "spread": item.estimated_spread, "slippage": item.estimated_slippage, "market_impact": item.estimated_market_impact, "latency": item.estimated_latency_cost},
            "expected_net_edge": item.expected_net_edge, "signal_age_seconds": item.signal_age_seconds,
            "alpha_survival": item.alpha_survival, "risk_budget": risk_budget, "suggested_notional": notional,
            "derived_leverage": leverage, "predicted_mae": item.predicted_adverse_move_quantile, "predicted_mfe": item.predicted_mfe,
            "maximum_holding_seconds": policy.max_position_age_seconds, "reasons": list(reasons),
            "provenance": {"source_wallet_action": dict(item.source_wallet_action or {}), "source_wallet_leverage_ignored": item.source_wallet_leverage is not None, "execution_mode": self.execution_mode},
        }
        record = DecisionRecord(identifier, decision, reasons, risk_budget, notional, leverage, payload)
        if self.repository:
            self.repository.record_decision(identifier, created_at=item.timestamp, symbol=item.symbol, decision=decision.value, payload=payload)
        return record
