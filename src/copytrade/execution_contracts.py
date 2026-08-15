"""Versioned, venue-neutral contracts for the Phase-D execution boundary.

This module intentionally has no transport, credential, or Hyperliquid
dependency.  It describes what Phase C decided and the evidence Phase D must
retain before any future venue adapter can be called.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .contracts import PHASE_D_EXECUTION_CONTRACT_VERSION
from .models import CopySignal, as_utc, stable_id, utc_now


class ExposureEffect(str, Enum):
    INCREASE = "INCREASE"
    REDUCE = "REDUCE"
    FLATTEN = "FLATTEN"
    NEUTRAL = "NEUTRAL"


class ExecutionState(str, Enum):
    CREATED = "CREATED"
    VALIDATING = "VALIDATING"
    BLOCKED = "BLOCKED"
    READY = "READY"
    SUBMITTING = "SUBMITTING"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED_BY_VENUE = "REJECTED_BY_VENUE"
    EXPIRED = "EXPIRED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    TERMINAL_ERROR = "TERMINAL_ERROR"


class ReconciliationState(str, Enum):
    MATCHED = "MATCHED"
    RECONCILING = "RECONCILING"
    MISMATCH = "MISMATCH"
    UNKNOWN_ORDER = "UNKNOWN_ORDER"
    UNKNOWN_POSITION = "UNKNOWN_POSITION"
    INCOMPLETE = "INCOMPLETE"
    VERIFIED_FLAT = "VERIFIED_FLAT"


class VenueOrderStatus(str, Enum):
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


TERMINAL_EXECUTION_STATES = frozenset({
    ExecutionState.BLOCKED,
    ExecutionState.FILLED,
    ExecutionState.CANCELLED,
    ExecutionState.REJECTED_BY_VENUE,
    ExecutionState.EXPIRED,
    ExecutionState.TERMINAL_ERROR,
})


# These are intentionally explicit instead of being inferred from a numeric
# status rank.  In particular, a fill that races a cancellation is allowed to
# advance CANCELLED to FILLED, while an old acknowledgement cannot move a
# filled order backwards.
LEGAL_EXECUTION_TRANSITIONS: dict[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.CREATED: frozenset({ExecutionState.VALIDATING, ExecutionState.BLOCKED, ExecutionState.TERMINAL_ERROR}),
    ExecutionState.VALIDATING: frozenset({ExecutionState.READY, ExecutionState.BLOCKED, ExecutionState.TERMINAL_ERROR}),
    ExecutionState.BLOCKED: frozenset(),
    # A readiness decision can become unsafe before the adapter boundary when
    # newer account authority or immutable integrity evidence arrives.
    ExecutionState.READY: frozenset({
        ExecutionState.BLOCKED, ExecutionState.SUBMITTING, ExecutionState.CANCELLED, ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.SUBMITTING: frozenset({
        ExecutionState.SUBMISSION_UNKNOWN, ExecutionState.ACKNOWLEDGED, ExecutionState.PARTIALLY_FILLED,
        ExecutionState.FILLED, ExecutionState.REJECTED_BY_VENUE, ExecutionState.RECONCILIATION_REQUIRED,
        ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.SUBMISSION_UNKNOWN: frozenset({
        ExecutionState.ACKNOWLEDGED, ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED,
        ExecutionState.REJECTED_BY_VENUE, ExecutionState.RECONCILIATION_REQUIRED, ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.ACKNOWLEDGED: frozenset({
        ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED, ExecutionState.CANCEL_PENDING,
        ExecutionState.CANCELLED, ExecutionState.EXPIRED, ExecutionState.RECONCILIATION_REQUIRED,
        ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.PARTIALLY_FILLED: frozenset({
        ExecutionState.FILLED, ExecutionState.CANCEL_PENDING, ExecutionState.CANCELLED,
        ExecutionState.RECONCILIATION_REQUIRED, ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.CANCEL_PENDING: frozenset({
        ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED, ExecutionState.CANCELLED,
        ExecutionState.RECONCILIATION_REQUIRED, ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.CANCELLED: frozenset({ExecutionState.FILLED, ExecutionState.RECONCILIATION_REQUIRED}),
    ExecutionState.REJECTED_BY_VENUE: frozenset({ExecutionState.RECONCILIATION_REQUIRED}),
    ExecutionState.EXPIRED: frozenset({ExecutionState.RECONCILIATION_REQUIRED}),
    # Contradictory immutable venue evidence discovered after a fill must
    # surface as an alarm instead of being hidden by terminal state.
    ExecutionState.FILLED: frozenset({ExecutionState.RECONCILIATION_REQUIRED}),
    ExecutionState.RECONCILIATION_REQUIRED: frozenset({
        ExecutionState.ACKNOWLEDGED, ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED,
        ExecutionState.CANCELLED, ExecutionState.REJECTED_BY_VENUE, ExecutionState.EXPIRED,
        ExecutionState.TERMINAL_ERROR,
    }),
    ExecutionState.TERMINAL_ERROR: frozenset(),
}


def validate_execution_transition(previous: ExecutionState, next_state: ExecutionState) -> None:
    """Raise for an illegal state change; callers may separately ignore stale evidence."""
    if previous == next_state:
        return
    if next_state not in LEGAL_EXECUTION_TRANSITIONS[previous]:
        raise ValueError(f"Illegal Phase-D execution transition: {previous.value} -> {next_state.value}")


def exposure_effect_for_action(action: str) -> ExposureEffect:
    normalized = action.lower()
    if normalized in {"open", "add"}:
        return ExposureEffect.INCREASE
    if normalized == "reduce":
        return ExposureEffect.REDUCE
    if normalized == "close":
        return ExposureEffect.FLATTEN
    raise ValueError(f"Unsupported Phase-C action for Phase-D execution: {action}")


def order_side(effect: ExposureEffect, direction: str) -> str:
    """Return the normalized venue side without treating a close as trusted."""
    normalized_direction = direction.lower()
    if normalized_direction not in {"long", "short"}:
        raise ValueError(f"Unsupported execution direction: {direction}")
    opening = effect is ExposureEffect.INCREASE
    return "BUY" if (normalized_direction == "long") == opening else "SELL"


@dataclass(frozen=True)
class ExecutionIntent:
    """Immutable, durable meaning of one Phase-C copy decision."""

    intent_id: str
    signal_id: str
    source_event_id: str
    target_wallet: str
    campaign_id: str | None
    symbol: str
    action: str
    direction: str
    requested_quantity: float
    requested_capital: float
    source_event_timestamp: object
    accepted_at: object
    contract_version: int = PHASE_D_EXECUTION_CONTRACT_VERSION
    provenance: dict[str, Any] = field(default_factory=dict)
    exposure_effect: ExposureEffect = ExposureEffect.NEUTRAL
    supersedes_intent_id: str | None = None
    execution_domain: str = "SIMULATOR"
    execution_account_id: str = "SIMULATOR:default"
    state: ExecutionState = ExecutionState.CREATED
    updated_at: object | None = None

    @classmethod
    def from_copy_signal(
        cls, signal: CopySignal, *, accepted_at: object | None = None, provenance: dict[str, Any] | None = None,
        supersedes_intent_id: str | None = None, execution_domain: str = "SIMULATOR",
        execution_account_id: str = "SIMULATOR:default",
    ) -> "ExecutionIntent":
        effect = exposure_effect_for_action(signal.action)
        if signal.target_quantity <= 0:
            raise ValueError("Phase-D execution intent requires a positive requested quantity.")
        accepted = as_utc(accepted_at or utc_now())
        semantic_provenance = {
            "phase_c_signal": {
                "signal_id": signal.signal_id,
                "source_event_id": signal.source_event_id,
                "reason": signal.reason,
                "target_price": signal.target_price,
                "target_notional": signal.target_notional,
                "allocation_fraction": signal.allocation_fraction,
                "target_position_before": signal.target_position_before,
                "target_leverage": signal.target_leverage,
                "target_equity": signal.target_equity,
                "equity_source": signal.equity_source,
                "equity_age_seconds": signal.equity_age_seconds,
            },
            **dict(provenance or {}),
        }
        return cls(
            intent_id=stable_id("phase_d_execution_intent_v1", PHASE_D_EXECUTION_CONTRACT_VERSION, signal.signal_id),
            signal_id=signal.signal_id, source_event_id=signal.source_event_id,
            target_wallet=signal.target_wallet.lower(), campaign_id=signal.campaign_id, symbol=signal.symbol,
            action=signal.action.lower(), direction=signal.direction.lower(), requested_quantity=abs(signal.target_quantity),
            requested_capital=signal.requested_capital, source_event_timestamp=signal.source_event_timestamp,
            accepted_at=accepted, provenance=semantic_provenance, exposure_effect=effect,
            supersedes_intent_id=supersedes_intent_id, execution_domain=execution_domain,
            execution_account_id=execution_account_id, updated_at=accepted,
        )


@dataclass(frozen=True)
class ExecutionRiskDecision:
    decision_id: str
    intent_id: str
    allowed: bool
    reason: str
    evaluated_at: object
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionSubmission:
    submission_id: str
    intent_id: str
    client_order_id: str
    requested_quantity: float
    side: str
    created_at: object
    updated_at: object
    state: str = "PREPARED"
    venue_order_id: str | None = None
    filled_quantity: float = 0.0
    raw_evidence: dict[str, Any] = field(default_factory=dict)
    execution_domain: str = "SIMULATOR"
    execution_account_id: str = "SIMULATOR:default"


@dataclass(frozen=True)
class SubmissionRequest:
    intent_id: str
    submission_id: str
    client_order_id: str
    symbol: str
    side: str
    quantity: float
    exposure_effect: ExposureEffect
    reduce_only: bool


@dataclass(frozen=True)
class VenueOrder:
    client_order_id: str
    status: VenueOrderStatus
    requested_quantity: float
    filled_quantity: float
    venue_order_id: str | None = None
    reason: str = ""
    venue_timestamp: object | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VenueFill:
    venue_fill_id: str
    client_order_id: str
    quantity: float
    price: float
    fee: float
    venue_timestamp: object
    side: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VenuePosition:
    symbol: str
    signed_quantity: float
    observed_at: object
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionSafetyContext:
    """Risk/control inputs owned outside venue mechanics.

    ``hard_transport_stop`` intentionally has different semantics from
    ``entry_inhibited``.  The former stops all adapter writes; the latter only
    blocks exposure increases and leaves safe reductions eligible.
    """

    entry_inhibited: bool = False
    entry_inhibit_reason: str = ""
    hard_transport_stop: bool = False
    source_recovery_continuous: bool = True
    market_evidence_current: bool = True
    reconciliation_healthy: bool = True
    verified_positions: dict[str, float] = field(default_factory=dict)
    verified_positions_current: bool = False
    verified_positions_authoritative: bool = False
