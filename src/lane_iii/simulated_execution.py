"""Lane III Phase E deterministic MNQ simulated execution.

This module is deliberately a closed simulation boundary.  It consumes the
frozen L3-D ``SignalDecision`` contract and frozen L3-B market-quality enums,
but contains no provider adapter, account, transport, credential, broker, or
copier dependency.  All time is supplied replay/event time.

The model is intentionally modest: marketable orders are filled only against
the supplied top of book after configured latency.  It does *not* claim queue
position or exchange matching-engine realism.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Iterable

from .contracts import LaneIIIInstrument, LaneIIIRefused, OperatorCommand, canonical_hash, normalized_utc
from .market_data import DataQuality, MNQContract
from .trader_v0 import (
    SignalDecision,
    SignalDecisionType,
    TRADER_V0_ARTIFACT_HASH,
    TRADER_V0_STRATEGY,
)


L3E_SCHEMA = "lane-iii-phase-e-simulated-execution-v1"
L3E_VERSION = "lane-iii-phase-e-v1"
SIMULATOR_ID = "l3e-deterministic-mnq-simulator"
MNQ_TICK_SIZE = Decimal("0.25")
MNQ_TICK_VALUE = Decimal("0.50")


class SimulationRefused(LaneIIIRefused):
    """An execution-simulation input or state transition is unsafe."""


class SimulationRecoveryRefused(SimulationRefused):
    """Persisted simulation evidence cannot safely reconstruct state."""


class SimulationAdmissionStatus(StrEnum):
    ADMITTED = "ADMITTED"
    REJECTED = "REJECTED"
    DUPLICATE = "DUPLICATE"
    IGNORED = "IGNORED"


class SimulationRejectionReason(StrEnum):
    UNKNOWN_STRATEGY = "UNKNOWN_STRATEGY"
    ARTIFACT_MISMATCH = "ARTIFACT_MISMATCH"
    MALFORMED_DECISION = "MALFORMED_DECISION"
    EXPIRED_SIGNAL = "EXPIRED_SIGNAL"
    DUPLICATE_SIGNAL = "DUPLICATE_SIGNAL"
    WRONG_CONTRACT = "WRONG_CONTRACT"
    MARKET_QUALITY_DEGRADED = "MARKET_QUALITY_DEGRADED"
    STALE_MARKET_STATE = "STALE_MARKET_STATE"
    SIMULATOR_DISARMED = "SIMULATOR_DISARMED"
    NEW_ENTRIES_PAUSED = "NEW_ENTRIES_PAUSED"
    LOSS_CEILING = "LOSS_CEILING"
    POSITION_LIMIT = "POSITION_LIMIT"
    OPEN_ORDER_LIMIT = "OPEN_ORDER_LIMIT"
    EXPOSURE_EXISTS = "EXPOSURE_EXISTS"
    OPPOSING_EXPOSURE = "OPPOSING_EXPOSURE"
    IMPOSSIBLE_LIFECYCLE = "IMPOSSIBLE_LIFECYCLE"


class SimulatedOrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class SimulatedOrderKind(StrEnum):
    MARKET_ENTRY = "MARKET_ENTRY"
    MARKET_EXIT = "MARKET_EXIT"
    PROTECTIVE_STOP = "PROTECTIVE_STOP"
    FLATTEN = "FLATTEN"


class SimulatedOrderState(StrEnum):
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"


class SimulatedPositionSide(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class SimulationHealth(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNRESOLVED = "UNRESOLVED"


class SimulationLedgerEventType(StrEnum):
    SIGNAL_ADMITTED = "SIGNAL_ADMITTED"
    SIGNAL_REJECTED = "SIGNAL_REJECTED"
    SIGNAL_DUPLICATE = "SIGNAL_DUPLICATE"
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_PARTIALLY_FILLED = "ORDER_PARTIALLY_FILLED"
    ORDER_FILLED = "ORDER_FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_CONFIRMED = "CANCEL_CONFIRMED"
    STRATEGY_EXIT_REQUESTED = "STRATEGY_EXIT_REQUESTED"
    STOP_TRIGGERED = "STOP_TRIGGERED"
    FLATTEN_REQUESTED = "FLATTEN_REQUESTED"
    POSITION_CHANGED = "POSITION_CHANGED"
    RISK_STATE_CHANGED = "RISK_STATE_CHANGED"
    OPERATOR_STATE_CHANGED = "OPERATOR_STATE_CHANGED"
    MARKET_DEGRADED = "MARKET_DEGRADED"
    MARKET_RECOVERED = "MARKET_RECOVERED"


def _decimal(value: object, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        qualifier = "positive" if positive else "non-negative" if nonnegative else "finite"
        raise ValueError(f"{field} must be {qualifier}.")
    return result


def _time(value: object, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _time_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _enum(value: object, enum_type: type[StrEnum], field: str) -> StrEnum:
    try:
        return enum_type(str(value))
    except ValueError as exc:
        raise SimulationRecoveryRefused(f"Persisted {field} is invalid.") from exc


@dataclass(frozen=True)
class SimulationLatency:
    """Event-time-only delays.  No wall-clock or random component exists."""

    signal_processing_ms: int = 25
    order_submission_ms: int = 50
    venue_fill_ms: int = 25
    cancellation_ms: int = 25

    def __post_init__(self) -> None:
        for field, value in asdict(self).items():
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a non-negative integer.")

    @property
    def total(self) -> timedelta:
        return timedelta(milliseconds=self.signal_processing_ms + self.order_submission_ms + self.venue_fill_ms)

    def payload(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class SimulationConfig:
    """Fixed simulation-owned guardrails; signals never own sizing or stops."""

    contract: MNQContract
    configured_quantity: int = 1
    maximum_position_quantity: int = 1
    maximum_open_orders: int = 4
    maximum_working_order_age_seconds: int = 30
    maximum_ledger_events: int = 100_000
    maximum_market_event_history: int = 100_000
    maximum_session_loss: Decimal = Decimal("100.00")
    protective_stop_ticks: int = 20
    entry_slippage_ticks: int = 1
    exit_slippage_ticks: int = 1
    commission_per_contract: Decimal = Decimal("0.00")
    latency: SimulationLatency = SimulationLatency()
    version: str = L3E_VERSION

    def __post_init__(self) -> None:
        if type(self.contract) is not MNQContract or self.contract.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise ValueError("L3-E requires one exact MNQContract.")
        for field, value in (
            ("Configured quantity", self.configured_quantity),
            ("Maximum position quantity", self.maximum_position_quantity),
            ("Maximum open orders", self.maximum_open_orders),
            ("Maximum working-order age", self.maximum_working_order_age_seconds),
            ("Maximum ledger events", self.maximum_ledger_events),
            ("Maximum market event history", self.maximum_market_event_history),
            ("Protective stop ticks", self.protective_stop_ticks),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{field} must be a positive integer.")
        if self.maximum_position_quantity < self.configured_quantity:
            raise ValueError("Maximum position quantity must permit the configured quantity.")
        for field, value in (
            ("Maximum session loss", self.maximum_session_loss),
            ("Commission per contract", self.commission_per_contract),
        ):
            _decimal(value, field, nonnegative=True)
        for field, value in (("Entry slippage ticks", self.entry_slippage_ticks), ("Exit slippage ticks", self.exit_slippage_ticks)):
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a non-negative integer.")
        if type(self.latency) is not SimulationLatency:
            raise ValueError("L3-E latency must be an explicit immutable model.")
        if not isinstance(self.version, str) or not self.version:
            raise ValueError("Simulation configuration version is required.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": L3E_SCHEMA,
            "version": self.version,
            "contract": self.contract.payload(),
            "configured_quantity": self.configured_quantity,
            "maximum_position_quantity": self.maximum_position_quantity,
            "maximum_open_orders": self.maximum_open_orders,
            "maximum_working_order_age_seconds": self.maximum_working_order_age_seconds,
            "maximum_ledger_events": self.maximum_ledger_events,
            "maximum_market_event_history": self.maximum_market_event_history,
            "maximum_session_loss": str(self.maximum_session_loss),
            "protective_stop_ticks": self.protective_stop_ticks,
            "entry_slippage_ticks": self.entry_slippage_ticks,
            "exit_slippage_ticks": self.exit_slippage_ticks,
            "commission_per_contract": str(self.commission_per_contract),
            "latency": self.latency.payload(),
            "matching_claim": "TOP_OF_BOOK_AFTER_EVENT_TIME_LATENCY_NO_QUEUE_POSITION",
        }

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


COMMISSIONED_SIMULATION_CONFIG = SimulationConfig(MNQContract("MNQU6", "2026-09"))


@dataclass(frozen=True)
class SimulatedMarketState:
    """One replay market observation usable by the modest top-of-book model."""

    market_event_id: str
    event_time: str
    contract: MNQContract
    bid_price: Decimal | None
    ask_price: Decimal | None
    bid_quantity: int | None
    ask_quantity: int | None
    quality: DataQuality

    def __post_init__(self) -> None:
        if not isinstance(self.market_event_id, str) or not self.market_event_id:
            raise ValueError("Simulated market event identity is required.")
        normalized_utc(self.event_time, "Simulated market event time")
        if type(self.contract) is not MNQContract or self.contract.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise ValueError("Simulated market state requires a concrete MNQ contract.")
        if type(self.quality) is not DataQuality:
            raise ValueError("Simulated market quality must be explicit.")
        values = (self.bid_price, self.ask_price, self.bid_quantity, self.ask_quantity)
        if self.quality is DataQuality.HEALTHY:
            if any(value is None for value in values):
                raise ValueError("Healthy market state requires a complete top of book.")
            bid, ask = _decimal(self.bid_price, "Best bid", positive=True), _decimal(self.ask_price, "Best ask", positive=True)
            if bid > ask:
                raise ValueError("Best bid may not exceed best ask.")
            for field, value in (("Bid quantity", self.bid_quantity), ("Ask quantity", self.ask_quantity)):
                if type(value) is not int or value < 0:
                    raise ValueError(f"{field} must be a non-negative integer.")
        elif any(value is not None for value in values):
            raise ValueError("Degraded market state must not carry a fillable stale book.")

    def payload(self) -> dict[str, object]:
        return {
            "market_event_id": self.market_event_id,
            "event_time": normalized_utc(self.event_time, "Simulated market event time"),
            "contract": self.contract.payload(),
            "bid_price": None if self.bid_price is None else str(self.bid_price),
            "ask_price": None if self.ask_price is None else str(self.ask_price),
            "bid_quantity": self.bid_quantity,
            "ask_quantity": self.ask_quantity,
            "quality": self.quality.value,
        }


@dataclass(frozen=True)
class SignalAdmission:
    decision_id: str
    status: SimulationAdmissionStatus
    reason: str | None
    admitted_at: str

    def payload(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "status": self.status.value,
            "reason": self.reason,
            "admitted_at": normalized_utc(self.admitted_at, "Signal admission time"),
        }


@dataclass(frozen=True)
class SimulatedOrder:
    order_id: str
    signal_decision_id: str | None
    side: SimulatedOrderSide
    kind: SimulatedOrderKind
    requested_quantity: int
    filled_quantity: int
    average_fill_price: Decimal | None
    state: SimulatedOrderState
    created_at: str
    eligible_at: str
    stop_price: Decimal | None = None
    cancel_requested_at: str | None = None
    cancel_eligible_at: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.order_id, str) or not self.order_id.startswith("l3e-o-"):
            raise ValueError("Simulated order requires an L3-E deterministic identity.")
        if self.signal_decision_id is not None and not self.signal_decision_id.startswith("l3d-d-"):
            raise ValueError("Simulated order source must be an L3-D decision identity.")
        if type(self.side) is not SimulatedOrderSide or type(self.kind) is not SimulatedOrderKind or type(self.state) is not SimulatedOrderState:
            raise ValueError("Simulated order enums must be explicit.")
        if type(self.requested_quantity) is not int or self.requested_quantity <= 0:
            raise ValueError("Simulated requested quantity must be positive.")
        if type(self.filled_quantity) is not int or not 0 <= self.filled_quantity <= self.requested_quantity:
            raise ValueError("Simulated filled quantity is inconsistent.")
        if self.filled_quantity == 0 and self.average_fill_price is not None:
            raise ValueError("An unfilled order cannot assert an average fill price.")
        if self.filled_quantity and self.average_fill_price is None:
            raise ValueError("A filled order requires an average fill price.")
        if self.average_fill_price is not None:
            _decimal(self.average_fill_price, "Average fill price", positive=True)
        created, eligible = _time(self.created_at, "Order creation time"), _time(self.eligible_at, "Order eligible time")
        if eligible < created:
            raise ValueError("Order eligibility cannot predate order creation.")
        if self.kind is SimulatedOrderKind.PROTECTIVE_STOP:
            if self.stop_price is None:
                raise ValueError("Protective stop requires a trigger price.")
            _decimal(self.stop_price, "Protective stop price", positive=True)
        elif self.stop_price is not None:
            raise ValueError("Only a protective stop has a trigger price.")
        if self.cancel_requested_at is None:
            if self.cancel_eligible_at is not None:
                raise ValueError("Cancel eligibility requires a cancellation request.")
        else:
            requested = _time(self.cancel_requested_at, "Cancel request time")
            eligible_cancel = _time(self.cancel_eligible_at, "Cancel eligibility time")
            if eligible_cancel < requested:
                raise ValueError("Cancel confirmation cannot predate its request.")
        if self.state is SimulatedOrderState.FILLED and self.filled_quantity != self.requested_quantity:
            raise ValueError("Filled order must have no remaining quantity.")
        if self.state is SimulatedOrderState.PARTIALLY_FILLED and not 0 < self.filled_quantity < self.requested_quantity:
            raise ValueError("Partial order must have partial quantity.")

    @property
    def remaining_quantity(self) -> int:
        return self.requested_quantity - self.filled_quantity

    @property
    def working(self) -> bool:
        return self.state in {SimulatedOrderState.WORKING, SimulatedOrderState.PARTIALLY_FILLED, SimulatedOrderState.CANCEL_REQUESTED}

    def payload(self) -> dict[str, object]:
        return {
            "order_id": self.order_id,
            "signal_decision_id": self.signal_decision_id,
            "side": self.side.value,
            "kind": self.kind.value,
            "requested_quantity": self.requested_quantity,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "average_fill_price": None if self.average_fill_price is None else str(self.average_fill_price),
            "state": self.state.value,
            "created_at": normalized_utc(self.created_at, "Order creation time"),
            "eligible_at": normalized_utc(self.eligible_at, "Order eligible time"),
            "stop_price": None if self.stop_price is None else str(self.stop_price),
            "cancel_requested_at": self.cancel_requested_at and normalized_utc(self.cancel_requested_at, "Cancel request time"),
            "cancel_eligible_at": self.cancel_eligible_at and normalized_utc(self.cancel_eligible_at, "Cancel eligibility time"),
        }


@dataclass(frozen=True)
class SimulatedFill:
    fill_id: str
    order_id: str
    market_event_id: str
    filled_at: str
    quantity: int
    price: Decimal
    slippage_ticks: int
    commission: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.fill_id, str) or not self.fill_id.startswith("l3e-f-"):
            raise ValueError("Simulated fill requires an L3-E deterministic identity.")
        if not isinstance(self.order_id, str) or not self.order_id.startswith("l3e-o-"):
            raise ValueError("Simulated fill requires an L3-E order reference.")
        if not isinstance(self.market_event_id, str) or not self.market_event_id:
            raise ValueError("Simulated fill requires a market event reference.")
        normalized_utc(self.filled_at, "Fill time")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("Simulated fill quantity must be positive.")
        _decimal(self.price, "Simulated fill price", positive=True)
        if type(self.slippage_ticks) is not int or self.slippage_ticks < 0:
            raise ValueError("Simulated fill slippage must be non-negative whole ticks.")
        _decimal(self.commission, "Simulated fill commission", nonnegative=True)

    def payload(self) -> dict[str, object]:
        return {
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "market_event_id": self.market_event_id,
            "filled_at": normalized_utc(self.filled_at, "Fill time"),
            "quantity": self.quantity,
            "price": str(self.price),
            "slippage_ticks": self.slippage_ticks,
            "commission": str(self.commission),
        }


@dataclass(frozen=True)
class SimulatedPosition:
    side: SimulatedPositionSide = SimulatedPositionSide.FLAT
    quantity: int = 0
    average_entry_price: Decimal | None = None
    realized_pnl: Decimal = Decimal("0.00")
    fees_paid: Decimal = Decimal("0.00")

    def __post_init__(self) -> None:
        if type(self.side) is not SimulatedPositionSide:
            raise ValueError("Simulated position side must be explicit.")
        if type(self.quantity) is not int or self.quantity < 0:
            raise ValueError("Simulated position quantity must be non-negative.")
        if self.side is SimulatedPositionSide.FLAT and (self.quantity != 0 or self.average_entry_price is not None):
            raise ValueError("Flat simulation position cannot retain exposure.")
        if self.side in {SimulatedPositionSide.LONG, SimulatedPositionSide.SHORT} and (self.quantity <= 0 or self.average_entry_price is None):
            raise ValueError("Directional simulation position requires exact filled exposure.")
        if self.side is SimulatedPositionSide.UNKNOWN and self.quantity != 0:
            raise ValueError("Unknown position must not fabricate a quantity.")
        if self.average_entry_price is not None:
            _decimal(self.average_entry_price, "Average entry price", positive=True)
        _decimal(self.realized_pnl, "Realized P&L")
        _decimal(self.fees_paid, "Fees paid", nonnegative=True)

    def payload(self) -> dict[str, object]:
        return {
            "side": self.side.value,
            "quantity": self.quantity,
            "average_entry_price": None if self.average_entry_price is None else str(self.average_entry_price),
            "realized_pnl": str(self.realized_pnl),
            "fees_paid": str(self.fees_paid),
        }


@dataclass(frozen=True)
class SimulationOperatorState:
    armed: bool = False
    paused_new_entries: bool = True
    flatten_latched: bool = False

    def payload(self) -> dict[str, bool]:
        return asdict(self)


@dataclass(frozen=True)
class SimulationLedgerEvent:
    sequence: int
    event_type: SimulationLedgerEventType
    event_time: str
    payload: dict[str, object]

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError("Ledger sequence must be positive.")
        if type(self.event_type) is not SimulationLedgerEventType:
            raise ValueError("Ledger event type must be explicit.")
        normalized_utc(self.event_time, "Ledger event time")

    def canonical_payload(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "event_type": self.event_type.value,
            "event_time": normalized_utc(self.event_time, "Ledger event time"),
            "payload": self.payload,
        }

    @property
    def event_id(self) -> str:
        return "l3e-e-" + canonical_hash(self.canonical_payload())[:32]

    def payload_with_id(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.canonical_payload()}


@dataclass(frozen=True)
class SimulationMetrics:
    signals_admitted: int
    signals_rejected: int
    orders_created: int
    orders_rejected: int
    partial_fills: int
    full_fills: int
    cancel_requests: int
    strategy_exits: int
    stop_exits: int
    flatten_events: int
    average_slippage_ticks: Decimal
    total_simulated_latency_ms: int
    realized_pnl: Decimal
    maximum_adverse_excursion: Decimal
    maximum_favorable_excursion: Decimal
    market_events_processed: int
    ledger_events: int


@dataclass(frozen=True)
class SimulationReplayReport:
    ledger_hash: str
    state_hash: str
    metrics: SimulationMetrics
    final_position: SimulatedPosition


class DeterministicMNQSimulator:
    """Small deterministic simulator with explicit order/position truth.

    Public transition methods are intentionally named for simulation state,
    never broker action.  The object is synchronous so replay ordering is
    entirely supplied by the caller.
    """

    def __init__(self, config: SimulationConfig = COMMISSIONED_SIMULATION_CONFIG, *, run_id: str = "l3e-default-run") -> None:
        if type(config) is not SimulationConfig:
            raise SimulationRefused("L3-E requires an exact immutable SimulationConfig.")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("Simulation run identity is required.")
        self.config = config
        self.run_id = run_id
        self.position = SimulatedPosition()
        self.operator_state = SimulationOperatorState()
        self.health = SimulationHealth.HEALTHY
        self.orders: dict[str, SimulatedOrder] = {}
        self.fills: list[SimulatedFill] = []
        self.ledger: list[SimulationLedgerEvent] = []
        self.admissions: dict[str, SignalAdmission] = {}
        self._market_event_ids: set[str] = set()
        self._last_market: SimulatedMarketState | None = None
        self._last_event_time: datetime | None = None
        self._loss_ceiling_breached = False
        self._metrics: dict[str, int | Decimal] = {
            "signals_admitted": 0, "signals_rejected": 0, "orders_created": 0, "orders_rejected": 0,
            "partial_fills": 0, "full_fills": 0, "cancel_requests": 0, "strategy_exits": 0,
            "stop_exits": 0, "flatten_events": 0, "slippage_ticks_total": 0, "fill_count": 0,
            "market_events_processed": 0, "maximum_adverse_excursion": Decimal("0.00"),
            "maximum_favorable_excursion": Decimal("0.00"),
        }

    @property
    def simulator_identity(self) -> str:
        return SIMULATOR_ID

    @property
    def ledger_hash(self) -> str:
        return canonical_hash([event.payload_with_id() for event in self.ledger])

    @property
    def state_hash(self) -> str:
        return canonical_hash(self.state_payload())

    @property
    def loss_ceiling_breached(self) -> bool:
        return self._loss_ceiling_breached

    @property
    def working_orders(self) -> tuple[SimulatedOrder, ...]:
        return tuple(order for order in sorted(self.orders.values(), key=lambda value: value.order_id) if order.working)

    def _clock(self, value: str, field: str) -> datetime:
        moment = _time(value, field)
        if self._last_event_time is not None and moment < self._last_event_time:
            raise SimulationRefused("Simulation event time cannot move backward.")
        self._last_event_time = moment
        return moment

    def _emit(self, event_type: SimulationLedgerEventType, at: str, payload: dict[str, object]) -> SimulationLedgerEvent:
        if len(self.ledger) >= self.config.maximum_ledger_events:
            raise SimulationRefused("Simulation ledger retention limit reached; replay must checkpoint and rotate safely.")
        normalized = normalized_utc(at, "Simulation ledger time")
        event = SimulationLedgerEvent(len(self.ledger) + 1, event_type, normalized, payload)
        self.ledger.append(event)
        return event

    def _admission(self, decision_id: str, status: SimulationAdmissionStatus, reason: str | None, at: str) -> SignalAdmission:
        admission = SignalAdmission(decision_id, status, reason, at)
        self.admissions[decision_id] = admission
        if status is SimulationAdmissionStatus.ADMITTED:
            self._metrics["signals_admitted"] = int(self._metrics["signals_admitted"]) + 1
            kind = SimulationLedgerEventType.SIGNAL_ADMITTED
        elif status is SimulationAdmissionStatus.REJECTED:
            self._metrics["signals_rejected"] = int(self._metrics["signals_rejected"]) + 1
            kind = SimulationLedgerEventType.SIGNAL_REJECTED
        elif status is SimulationAdmissionStatus.DUPLICATE:
            kind = SimulationLedgerEventType.SIGNAL_DUPLICATE
        else:
            kind = SimulationLedgerEventType.SIGNAL_ADMITTED
        self._emit(kind, at, admission.payload())
        return admission

    def _reject_signal(self, decision_id: str, reason: SimulationRejectionReason, at: str) -> SignalAdmission:
        return self._admission(decision_id, SimulationAdmissionStatus.REJECTED, reason.value, at)

    def _assert_signal(self, signal: object) -> SignalDecision:
        if type(signal) is not SignalDecision:
            raise SimulationRefused("Only exact immutable L3-D SignalDecision values may reach L3-E.")
        try:
            malformed = (
                not isinstance(signal.decision_id, str)
                or type(signal.decision) is not SignalDecisionType
                or not isinstance(signal.created_at, str)
                or not isinstance(signal.expires_at, str)
                or signal.decision_id != "l3d-d-" + canonical_hash(signal.payload_without_identity())[:32]
            )
            normalized_utc(signal.created_at, "Signal creation time")
            normalized_utc(signal.expires_at, "Signal expiry time")
        except (AttributeError, TypeError, ValueError):
            raise SimulationRefused(SimulationRejectionReason.MALFORMED_DECISION.value) from None
        if malformed:
            raise SimulationRefused(SimulationRejectionReason.MALFORMED_DECISION.value)
        if signal.strategy_identity != TRADER_V0_STRATEGY.strategy_identity:
            raise SimulationRefused(SimulationRejectionReason.UNKNOWN_STRATEGY.value)
        if signal.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH:
            raise SimulationRefused(SimulationRejectionReason.ARTIFACT_MISMATCH.value)
        return signal

    def _entry_block_reason(self) -> SimulationRejectionReason | None:
        if self.health is not SimulationHealth.HEALTHY:
            return SimulationRejectionReason.MARKET_QUALITY_DEGRADED
        if self._last_market is None:
            return SimulationRejectionReason.STALE_MARKET_STATE
        if not self.operator_state.armed:
            return SimulationRejectionReason.SIMULATOR_DISARMED
        if self.operator_state.paused_new_entries or self.operator_state.flatten_latched:
            return SimulationRejectionReason.NEW_ENTRIES_PAUSED
        if self._loss_ceiling_breached:
            return SimulationRejectionReason.LOSS_CEILING
        if self.position.side is not SimulatedPositionSide.FLAT or any(order.kind is SimulatedOrderKind.MARKET_ENTRY for order in self.working_orders):
            return SimulationRejectionReason.EXPOSURE_EXISTS
        if len(self.working_orders) >= self.config.maximum_open_orders:
            return SimulationRejectionReason.OPEN_ORDER_LIMIT
        return None

    def admit_signal(self, signal: object, *, admitted_at: str | None = None) -> SignalAdmission:
        """Validate one frozen signal and create only a simulated action.

        A malformed foreign object is rejected by exception because it has no
        safe deterministic decision identity.  Valid L3-D values are recorded
        visibly as admitted, rejected, ignored, or duplicate ledger facts.
        """
        if type(signal) is not SignalDecision:
            raw_id = getattr(signal, "decision_id", None)
            decision_id = raw_id if isinstance(raw_id, str) and raw_id else "l3e-malformed-" + canonical_hash({"type": type(signal).__name__})[:24]
            fallback = _time_text(self._last_event_time or datetime(1970, 1, 1, tzinfo=timezone.utc))
            at = normalized_utc(admitted_at or fallback, "Signal admission time")
            self._clock(at, "Signal admission time")
            return self._reject_signal(decision_id, SimulationRejectionReason.MALFORMED_DECISION, at)
        at = normalized_utc(admitted_at or signal.created_at, "Signal admission time")
        self._clock(at, "Signal admission time")
        try:
            checked = self._assert_signal(signal)
        except SimulationRefused as exc:
            reason = {
                SimulationRejectionReason.UNKNOWN_STRATEGY.value: SimulationRejectionReason.UNKNOWN_STRATEGY,
                SimulationRejectionReason.ARTIFACT_MISMATCH.value: SimulationRejectionReason.ARTIFACT_MISMATCH,
                SimulationRejectionReason.MALFORMED_DECISION.value: SimulationRejectionReason.MALFORMED_DECISION,
            }.get(str(exc), SimulationRejectionReason.MALFORMED_DECISION)
            return self._reject_signal(signal.decision_id, reason, at)
        prior = self.admissions.get(checked.decision_id)
        if prior is not None:
            return self._admission(checked.decision_id, SimulationAdmissionStatus.DUPLICATE, SimulationRejectionReason.DUPLICATE_SIGNAL.value, at)
        if _time(at, "Signal admission time") > _time(checked.expires_at, "Signal expiry time"):
            return self._reject_signal(checked.decision_id, SimulationRejectionReason.EXPIRED_SIGNAL, at)
        if checked.decision is SignalDecisionType.NO_TRADE:
            return self._admission(checked.decision_id, SimulationAdmissionStatus.IGNORED, "NO_TRADE", at)
        if checked.decision in {SignalDecisionType.LONG, SignalDecisionType.SHORT}:
            reason = self._entry_block_reason()
            if reason is not None:
                return self._reject_signal(checked.decision_id, reason, at)
            side = SimulatedOrderSide.BUY if checked.decision is SignalDecisionType.LONG else SimulatedOrderSide.SELL
            admission = self._admission(checked.decision_id, SimulationAdmissionStatus.ADMITTED, None, at)
            self._create_order(checked.decision_id, side, SimulatedOrderKind.MARKET_ENTRY, self.config.configured_quantity, at)
            if self._last_market is not None and _time(self._last_market.event_time, "Latest market event time") >= _time(at, "Signal admission time"):
                self._process_market(self._last_market)
            return admission
        if checked.decision is SignalDecisionType.EXIT:
            admission = self._admission(checked.decision_id, SimulationAdmissionStatus.ADMITTED, None, at)
            self._metrics["strategy_exits"] = int(self._metrics["strategy_exits"]) + 1
            self._emit(SimulationLedgerEventType.STRATEGY_EXIT_REQUESTED, at, {"decision_id": checked.decision_id, "position": self.position.payload()})
            for order in self.working_orders:
                if order.kind is SimulatedOrderKind.MARKET_ENTRY:
                    self.request_cancel(order.order_id, requested_at=at)
            if self.position.side in {SimulatedPositionSide.LONG, SimulatedPositionSide.SHORT}:
                side = SimulatedOrderSide.SELL if self.position.side is SimulatedPositionSide.LONG else SimulatedOrderSide.BUY
                self._create_order(checked.decision_id, side, SimulatedOrderKind.MARKET_EXIT, self.position.quantity, at)
            if self._last_market is not None and _time(self._last_market.event_time, "Latest market event time") >= _time(at, "Signal admission time"):
                self._process_market(self._last_market)
            return admission
        return self._reject_signal(checked.decision_id, SimulationRejectionReason.IMPOSSIBLE_LIFECYCLE, at)

    def _create_order(self, decision_id: str | None, side: SimulatedOrderSide, kind: SimulatedOrderKind, quantity: int, at: str, *, stop_price: Decimal | None = None) -> SimulatedOrder:
        event_time = _time(at, "Simulated order time")
        payload = {
            "run_id": self.run_id, "decision_id": decision_id, "side": side.value, "kind": kind.value,
            "quantity": quantity, "at": normalized_utc(at, "Simulated order time"), "stop_price": None if stop_price is None else str(stop_price),
        }
        order = SimulatedOrder(
            "l3e-o-" + canonical_hash(payload)[:32], decision_id, side, kind, quantity, 0, None,
            SimulatedOrderState.WORKING, at, _time_text(event_time + self.config.latency.total), stop_price,
        )
        if order.order_id in self.orders:
            return self.orders[order.order_id]
        self.orders[order.order_id] = order
        self._metrics["orders_created"] = int(self._metrics["orders_created"]) + 1
        self._emit(SimulationLedgerEventType.ORDER_CREATED, at, order.payload())
        return order

    def request_cancel(self, order_id: str, *, requested_at: str) -> SimulatedOrder:
        if order_id not in self.orders:
            raise SimulationRefused("Unknown simulated order cannot be cancelled.")
        at = normalized_utc(requested_at, "Cancellation request time")
        self._clock(at, "Cancellation request time")
        order = self.orders[order_id]
        if not order.working:
            return order
        if order.state is SimulatedOrderState.CANCEL_REQUESTED:
            return order
        eligible = _time_text(_time(at, "Cancellation request time") + timedelta(milliseconds=self.config.latency.cancellation_ms))
        updated = replace(order, state=SimulatedOrderState.CANCEL_REQUESTED, cancel_requested_at=at, cancel_eligible_at=eligible)
        self.orders[order_id] = updated
        self._metrics["cancel_requests"] = int(self._metrics["cancel_requests"]) + 1
        self._emit(SimulationLedgerEventType.CANCEL_REQUESTED, at, {"order_id": order_id, "remaining_quantity": updated.remaining_quantity, "cancel_eligible_at": eligible})
        return updated

    def apply_operator_command(self, command: OperatorCommand, *, requested_at: str) -> SimulationOperatorState:
        if type(command) is not OperatorCommand:
            raise ValueError("Simulation operator command must be explicit.")
        at = normalized_utc(requested_at, "Operator command time")
        self._clock(at, "Operator command time")
        prior = self.operator_state
        if command is OperatorCommand.ARM:
            if self.operator_state.flatten_latched:
                raise SimulationRefused("A flatten latch cannot be armed before simulated exposure resolves.")
            self.operator_state = replace(prior, armed=True)
        elif command is OperatorCommand.DISARM:
            self.operator_state = SimulationOperatorState(False, True, prior.flatten_latched)
        elif command is OperatorCommand.PAUSE_NEW_ENTRIES:
            self.operator_state = replace(prior, paused_new_entries=True)
        elif command is OperatorCommand.RESUME_NEW_ENTRIES:
            if prior.flatten_latched:
                raise SimulationRefused("A flatten latch blocks new simulated entries.")
            self.operator_state = replace(prior, paused_new_entries=False)
        elif command is OperatorCommand.FLATTEN:
            self.operator_state = SimulationOperatorState(False, True, True)
            self._metrics["flatten_events"] = int(self._metrics["flatten_events"]) + 1
            self._emit(SimulationLedgerEventType.FLATTEN_REQUESTED, at, {"position": self.position.payload()})
            for order in self.working_orders:
                if order.kind is not SimulatedOrderKind.PROTECTIVE_STOP:
                    self.request_cancel(order.order_id, requested_at=at)
            if self.position.side in {SimulatedPositionSide.LONG, SimulatedPositionSide.SHORT}:
                side = SimulatedOrderSide.SELL if self.position.side is SimulatedPositionSide.LONG else SimulatedOrderSide.BUY
                self._create_order(None, side, SimulatedOrderKind.FLATTEN, self.position.quantity, at)
        self._emit(SimulationLedgerEventType.OPERATOR_STATE_CHANGED, at, {"command": command.value, "state": self.operator_state.payload()})
        if self._last_market is not None and _time(self._last_market.event_time, "Latest market event time") >= _time(at, "Operator command time"):
            self._process_market(self._last_market)
        return self.operator_state

    def on_market(self, state: SimulatedMarketState) -> None:
        if type(state) is not SimulatedMarketState:
            raise SimulationRefused("L3-E accepts only an explicit simulated market state.")
        if state.contract != self.config.contract:
            raise SimulationRefused("Simulated market state contract does not match the commissioned MNQ expiry.")
        if state.market_event_id in self._market_event_ids:
            return
        if len(self._market_event_ids) >= self.config.maximum_market_event_history:
            raise SimulationRefused("Simulation market-event retention limit reached; replay must checkpoint and rotate safely.")
        moment = self._clock(state.event_time, "Simulated market event time")
        self._market_event_ids.add(state.market_event_id)
        self._last_market = state
        self._metrics["market_events_processed"] = int(self._metrics["market_events_processed"]) + 1
        if state.quality is not DataQuality.HEALTHY:
            old_health = self.health
            self.health = SimulationHealth.UNRESOLVED if state.quality in {DataQuality.GAPPED, DataQuality.INVALID} else SimulationHealth.DEGRADED
            if old_health is not self.health:
                self._emit(SimulationLedgerEventType.MARKET_DEGRADED, state.event_time, {"quality": state.quality.value, "health": self.health.value, "position": self.position.payload()})
            return
        if self.health is not SimulationHealth.HEALTHY:
            self.health = SimulationHealth.HEALTHY
            self._emit(SimulationLedgerEventType.MARKET_RECOVERED, state.event_time, {"market_event_id": state.market_event_id})
        self._process_market(state)
        self._update_excursions(state)
        self._check_loss_ceiling(state.event_time)
        # retain explicit clock use so this method cannot accidentally become wall-clock based.
        assert moment == _time(state.event_time, "Simulated market event time")

    def _process_market(self, market: SimulatedMarketState) -> None:
        if market.quality is not DataQuality.HEALTHY or self.health is not SimulationHealth.HEALTHY:
            return
        now = _time(market.event_time, "Simulated market event time")
        # Confirmation is separate from request and needs a valid later/same replay observation.
        for order in sorted(self.working_orders, key=lambda item: item.order_id):
            if order.state is SimulatedOrderState.CANCEL_REQUESTED and _time(order.cancel_eligible_at or order.created_at, "Cancellation eligibility") <= now:
                self.orders[order.order_id] = replace(order, state=SimulatedOrderState.CANCELLED)
                self._emit(SimulationLedgerEventType.CANCEL_CONFIRMED, market.event_time, {"order_id": order.order_id, "filled_quantity": order.filled_quantity, "remaining_quantity": order.remaining_quantity})
        bid_available, ask_available = int(market.bid_quantity or 0), int(market.ask_quantity or 0)
        priority = {SimulatedOrderKind.FLATTEN: 0, SimulatedOrderKind.PROTECTIVE_STOP: 1, SimulatedOrderKind.MARKET_EXIT: 2, SimulatedOrderKind.MARKET_ENTRY: 3}
        candidates = sorted(self.working_orders, key=lambda item: (priority[item.kind], item.order_id))
        for original in candidates:
            order = self.orders[original.order_id]
            if not order.working or _time(order.eligible_at, "Order eligibility") > now:
                continue
            if order.kind is SimulatedOrderKind.PROTECTIVE_STOP:
                assert order.stop_price is not None
                triggered = (order.side is SimulatedOrderSide.SELL and Decimal(market.bid_price or "0") <= order.stop_price) or (order.side is SimulatedOrderSide.BUY and Decimal(market.ask_price or "0") >= order.stop_price)
                if not triggered:
                    continue
                self._emit(SimulationLedgerEventType.STOP_TRIGGERED, market.event_time, {"order_id": order.order_id, "trigger_price": str(order.stop_price), "bid": str(market.bid_price), "ask": str(market.ask_price)})
                self._metrics["stop_exits"] = int(self._metrics["stop_exits"]) + 1
            available = ask_available if order.side is SimulatedOrderSide.BUY else bid_available
            fill_quantity = min(order.remaining_quantity, available)
            if fill_quantity <= 0:
                continue
            if order.side is SimulatedOrderSide.BUY:
                ask_available -= fill_quantity
                reference = Decimal(market.ask_price or "0")
            else:
                bid_available -= fill_quantity
                reference = Decimal(market.bid_price or "0")
            slippage = self.config.entry_slippage_ticks if order.kind is SimulatedOrderKind.MARKET_ENTRY else self.config.exit_slippage_ticks
            price = reference + MNQ_TICK_SIZE * slippage if order.side is SimulatedOrderSide.BUY else reference - MNQ_TICK_SIZE * slippage
            self._fill(order, market, fill_quantity, price, slippage)
        self._cancel_expired_orders(market.event_time)

    def _fill(self, order: SimulatedOrder, market: SimulatedMarketState, quantity: int, price: Decimal, slippage_ticks: int) -> None:
        commission = self.config.commission_per_contract * quantity
        fill_payload = {"order_id": order.order_id, "market_event_id": market.market_event_id, "at": market.event_time, "quantity": quantity, "price": str(price), "prior_filled_quantity": order.filled_quantity}
        fill = SimulatedFill("l3e-f-" + canonical_hash(fill_payload)[:32], order.order_id, market.market_event_id, market.event_time, quantity, price, slippage_ticks, commission)
        if any(value.fill_id == fill.fill_id for value in self.fills):
            return
        total = order.filled_quantity + quantity
        average = price if order.average_fill_price is None else ((order.average_fill_price * order.filled_quantity) + (price * quantity)) / total
        state = SimulatedOrderState.FILLED if total == order.requested_quantity else SimulatedOrderState.PARTIALLY_FILLED
        updated = replace(order, filled_quantity=total, average_fill_price=average, state=state)
        self.orders[order.order_id] = updated
        self.fills.append(fill)
        self._metrics["fill_count"] = int(self._metrics["fill_count"]) + 1
        self._metrics["slippage_ticks_total"] = int(self._metrics["slippage_ticks_total"]) + slippage_ticks * quantity
        self._metrics["full_fills" if state is SimulatedOrderState.FILLED else "partial_fills"] = int(self._metrics["full_fills" if state is SimulatedOrderState.FILLED else "partial_fills"]) + 1
        event_type = SimulationLedgerEventType.ORDER_FILLED if state is SimulatedOrderState.FILLED else SimulationLedgerEventType.ORDER_PARTIALLY_FILLED
        self._emit(event_type, market.event_time, {"order": updated.payload(), "fill": fill.payload()})
        self._apply_position_fill(updated, fill)
        if self.position.side in {SimulatedPositionSide.LONG, SimulatedPositionSide.SHORT}:
            self._ensure_protective_stop(market.event_time)
        else:
            self._retire_protective_stops(market.event_time)

    def _apply_position_fill(self, order: SimulatedOrder, fill: SimulatedFill) -> None:
        prior = self.position
        is_entry = order.kind is SimulatedOrderKind.MARKET_ENTRY
        signed_entry = 1 if order.side is SimulatedOrderSide.BUY else -1
        fee_total = prior.fees_paid + fill.commission
        realized = prior.realized_pnl - fill.commission
        if is_entry:
            target_side = SimulatedPositionSide.LONG if signed_entry > 0 else SimulatedPositionSide.SHORT
            if prior.side is SimulatedPositionSide.FLAT:
                self.position = SimulatedPosition(target_side, fill.quantity, fill.price, realized, fee_total)
            elif prior.side is target_side:
                assert prior.average_entry_price is not None
                quantity = prior.quantity + fill.quantity
                average = ((prior.average_entry_price * prior.quantity) + (fill.price * fill.quantity)) / quantity
                self.position = SimulatedPosition(target_side, quantity, average, realized, fee_total)
            else:
                raise SimulationRefused("L3-E refuses a simulated reversal while exposure remains.")
        else:
            expected = SimulatedPositionSide.LONG if order.side is SimulatedOrderSide.SELL else SimulatedPositionSide.SHORT
            if prior.side is SimulatedPositionSide.FLAT:
                raise SimulationRefused("An exit fill cannot create exposure from a flat simulated position.")
            if prior.side is not expected or fill.quantity > prior.quantity:
                raise SimulationRefused("Exit quantity is incompatible with simulated exposure.")
            assert prior.average_entry_price is not None
            direction = Decimal("1") if prior.side is SimulatedPositionSide.LONG else Decimal("-1")
            gross = ((fill.price - prior.average_entry_price) / MNQ_TICK_SIZE) * MNQ_TICK_VALUE * fill.quantity * direction
            remaining = prior.quantity - fill.quantity
            self.position = SimulatedPosition(
                SimulatedPositionSide.FLAT if remaining == 0 else prior.side,
                remaining, None if remaining == 0 else prior.average_entry_price,
                realized + gross, fee_total,
            )
        self._emit(SimulationLedgerEventType.POSITION_CHANGED, fill.filled_at, {"prior": prior.payload(), "current": self.position.payload(), "fill_id": fill.fill_id})

    def _ensure_protective_stop(self, at: str) -> None:
        if self.position.side not in {SimulatedPositionSide.LONG, SimulatedPositionSide.SHORT}:
            return
        assert self.position.average_entry_price is not None
        current = next((order for order in self.working_orders if order.kind is SimulatedOrderKind.PROTECTIVE_STOP), None)
        if current is not None:
            # The original trigger remains immutable; added partial exposure is
            # protected at that same-or-tighter level, never widened by thesis.
            # Requested quantity includes any stop fills already applied.  The
            # remaining stop coverage is exactly the remaining filled position.
            desired_quantity = current.filled_quantity + self.position.quantity
            if current.requested_quantity != desired_quantity:
                self.orders[current.order_id] = replace(current, requested_quantity=desired_quantity)
            return
        side = SimulatedOrderSide.SELL if self.position.side is SimulatedPositionSide.LONG else SimulatedOrderSide.BUY
        offset = MNQ_TICK_SIZE * self.config.protective_stop_ticks
        stop = self.position.average_entry_price - offset if side is SimulatedOrderSide.SELL else self.position.average_entry_price + offset
        self._create_order(None, side, SimulatedOrderKind.PROTECTIVE_STOP, self.position.quantity, at, stop_price=stop)

    def _retire_protective_stops(self, at: str) -> None:
        for order in self.working_orders:
            if order.kind is SimulatedOrderKind.PROTECTIVE_STOP:
                self.request_cancel(order.order_id, requested_at=at)

    def _cancel_expired_orders(self, at: str) -> None:
        now = _time(at, "Working-order age time")
        for order in self.working_orders:
            if order.kind is SimulatedOrderKind.PROTECTIVE_STOP or order.state is SimulatedOrderState.CANCEL_REQUESTED:
                continue
            if now - _time(order.created_at, "Order creation time") > timedelta(seconds=self.config.maximum_working_order_age_seconds):
                self.request_cancel(order.order_id, requested_at=at)

    def _update_excursions(self, market: SimulatedMarketState) -> None:
        if self.position.side is SimulatedPositionSide.FLAT or self.position.average_entry_price is None:
            return
        mark = Decimal(market.bid_price or "0") if self.position.side is SimulatedPositionSide.LONG else Decimal(market.ask_price or "0")
        direction = Decimal("1") if self.position.side is SimulatedPositionSide.LONG else Decimal("-1")
        pnl = ((mark - self.position.average_entry_price) / MNQ_TICK_SIZE) * MNQ_TICK_VALUE * self.position.quantity * direction
        if pnl < 0:
            self._metrics["maximum_adverse_excursion"] = max(Decimal(self._metrics["maximum_adverse_excursion"]), -pnl)
        else:
            self._metrics["maximum_favorable_excursion"] = max(Decimal(self._metrics["maximum_favorable_excursion"]), pnl)

    def _check_loss_ceiling(self, at: str) -> None:
        if self._loss_ceiling_breached or self.position.realized_pnl > -self.config.maximum_session_loss:
            return
        self._loss_ceiling_breached = True
        self._emit(SimulationLedgerEventType.RISK_STATE_CHANGED, at, {"loss_ceiling_breached": True, "realized_pnl": str(self.position.realized_pnl), "maximum_session_loss": str(self.config.maximum_session_loss)})

    def metrics(self) -> SimulationMetrics:
        fill_count = int(self._metrics["fill_count"])
        average = Decimal("0.00") if fill_count == 0 else Decimal(int(self._metrics["slippage_ticks_total"])) / fill_count
        return SimulationMetrics(
            signals_admitted=int(self._metrics["signals_admitted"]), signals_rejected=int(self._metrics["signals_rejected"]),
            orders_created=int(self._metrics["orders_created"]), orders_rejected=int(self._metrics["orders_rejected"]),
            partial_fills=int(self._metrics["partial_fills"]), full_fills=int(self._metrics["full_fills"]),
            cancel_requests=int(self._metrics["cancel_requests"]), strategy_exits=int(self._metrics["strategy_exits"]),
            stop_exits=int(self._metrics["stop_exits"]), flatten_events=int(self._metrics["flatten_events"]),
            average_slippage_ticks=average, total_simulated_latency_ms=int(self.config.latency.total.total_seconds() * 1000),
            realized_pnl=self.position.realized_pnl, maximum_adverse_excursion=Decimal(self._metrics["maximum_adverse_excursion"]),
            maximum_favorable_excursion=Decimal(self._metrics["maximum_favorable_excursion"]),
            market_events_processed=int(self._metrics["market_events_processed"]), ledger_events=len(self.ledger),
        )

    def report(self) -> SimulationReplayReport:
        return SimulationReplayReport(self.ledger_hash, self.state_hash, self.metrics(), self.position)

    def state_payload(self) -> dict[str, object]:
        return {
            "schema": L3E_SCHEMA,
            "simulator_identity": self.simulator_identity,
            "run_id": self.run_id,
            "configuration_hash": self.config.configuration_hash,
            "position": self.position.payload(),
            "operator_state": self.operator_state.payload(),
            "health": self.health.value,
            "loss_ceiling_breached": self._loss_ceiling_breached,
            "orders": [order.payload() for order in sorted(self.orders.values(), key=lambda value: value.order_id)],
            "fills": [fill.payload() for fill in self.fills],
            "admissions": [admission.payload() for _, admission in sorted(self.admissions.items())],
            "last_market": None if self._last_market is None else self._last_market.payload(),
            "market_event_ids": sorted(self._market_event_ids),
            "last_event_time": None if self._last_event_time is None else _time_text(self._last_event_time),
            "metrics": {key: str(value) if isinstance(value, Decimal) else value for key, value in sorted(self._metrics.items())},
            "ledger": [event.payload_with_id() for event in self.ledger],
            "ledger_hash": self.ledger_hash,
        }

    def snapshot(self) -> dict[str, object]:
        payload = self.state_payload()
        return {"state": payload, "state_hash": canonical_hash(payload)}

    @classmethod
    def from_snapshot(cls, config: SimulationConfig, snapshot: dict[str, object]) -> "DeterministicMNQSimulator":
        if type(config) is not SimulationConfig or not isinstance(snapshot, dict):
            raise SimulationRecoveryRefused("Simulation recovery requires exact config and snapshot mapping.")
        state = snapshot.get("state")
        if not isinstance(state, dict) or snapshot.get("state_hash") != canonical_hash(state):
            raise SimulationRecoveryRefused("Simulation persistence hash is corrupt or inconsistent.")
        if state.get("schema") != L3E_SCHEMA or state.get("simulator_identity") != SIMULATOR_ID:
            raise SimulationRecoveryRefused("Simulation persistence has the wrong schema or authority.")
        if state.get("configuration_hash") != config.configuration_hash:
            raise SimulationRecoveryRefused("Simulation persistence configuration does not match the active simulator.")
        instance = cls(config, run_id=str(state.get("run_id")))
        try:
            instance.position = _position_from_payload(state["position"])
            operator = state["operator_state"]
            instance.operator_state = SimulationOperatorState(bool(operator["armed"]), bool(operator["paused_new_entries"]), bool(operator["flatten_latched"]))
            instance.health = _enum(state["health"], SimulationHealth, "health")  # type: ignore[assignment]
            instance._loss_ceiling_breached = bool(state["loss_ceiling_breached"])
            instance.orders = {order.order_id: order for order in (_order_from_payload(value) for value in state["orders"])}
            instance.fills = [_fill_from_payload(value) for value in state["fills"]]
            instance.admissions = {admission.decision_id: admission for admission in (_admission_from_payload(value) for value in state["admissions"])}
            instance._last_market = None if state["last_market"] is None else _market_from_payload(state["last_market"])
            instance._market_event_ids = set(str(value) for value in state["market_event_ids"])
            instance._last_event_time = None if state["last_event_time"] is None else _time(state["last_event_time"], "Persisted event time")
            metrics = state["metrics"]
            instance._metrics = {key: Decimal(value) if key in {"maximum_adverse_excursion", "maximum_favorable_excursion"} else int(value) for key, value in metrics.items()}
            instance.ledger = [_ledger_from_payload(value) for value in state["ledger"]]
        except (KeyError, TypeError, ValueError, SimulationRecoveryRefused) as exc:
            raise SimulationRecoveryRefused("Simulation persistence is malformed; recovery fails closed.") from exc
        if [event.sequence for event in instance.ledger] != list(range(1, len(instance.ledger) + 1)):
            raise SimulationRecoveryRefused("Simulation ledger ordering is inconsistent.")
        if state.get("ledger_hash") != instance.ledger_hash:
            raise SimulationRecoveryRefused("Simulation ledger hash is inconsistent.")
        if instance.state_payload() != state:
            raise SimulationRecoveryRefused("Simulation state cannot be reconstructed exactly from persistence.")
        return instance


def _contract_from_payload(payload: object) -> MNQContract:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted contract is malformed.")
    return MNQContract(str(payload["contract_symbol"]), str(payload["contract_expiry"]), str(payload["exchange"]), LaneIIIInstrument(str(payload["strategy_instrument"])))


def _market_from_payload(payload: object) -> SimulatedMarketState:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted market state is malformed.")
    return SimulatedMarketState(str(payload["market_event_id"]), str(payload["event_time"]), _contract_from_payload(payload["contract"]), None if payload["bid_price"] is None else Decimal(str(payload["bid_price"])), None if payload["ask_price"] is None else Decimal(str(payload["ask_price"])), payload["bid_quantity"], payload["ask_quantity"], _enum(payload["quality"], DataQuality, "market quality"))  # type: ignore[arg-type]


def _position_from_payload(payload: object) -> SimulatedPosition:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted position is malformed.")
    return SimulatedPosition(_enum(payload["side"], SimulatedPositionSide, "position side"), int(payload["quantity"]), None if payload["average_entry_price"] is None else Decimal(str(payload["average_entry_price"])), Decimal(str(payload["realized_pnl"])), Decimal(str(payload["fees_paid"])))  # type: ignore[arg-type]


def _order_from_payload(payload: object) -> SimulatedOrder:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted order is malformed.")
    return SimulatedOrder(str(payload["order_id"]), payload["signal_decision_id"], _enum(payload["side"], SimulatedOrderSide, "order side"), _enum(payload["kind"], SimulatedOrderKind, "order kind"), int(payload["requested_quantity"]), int(payload["filled_quantity"]), None if payload["average_fill_price"] is None else Decimal(str(payload["average_fill_price"])), _enum(payload["state"], SimulatedOrderState, "order state"), str(payload["created_at"]), str(payload["eligible_at"]), None if payload["stop_price"] is None else Decimal(str(payload["stop_price"])), payload["cancel_requested_at"], payload["cancel_eligible_at"])  # type: ignore[arg-type]


def _fill_from_payload(payload: object) -> SimulatedFill:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted fill is malformed.")
    return SimulatedFill(str(payload["fill_id"]), str(payload["order_id"]), str(payload["market_event_id"]), str(payload["filled_at"]), int(payload["quantity"]), Decimal(str(payload["price"])), int(payload["slippage_ticks"]), Decimal(str(payload["commission"])))


def _admission_from_payload(payload: object) -> SignalAdmission:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted admission is malformed.")
    return SignalAdmission(str(payload["decision_id"]), _enum(payload["status"], SimulationAdmissionStatus, "admission status"), payload["reason"], str(payload["admitted_at"]))  # type: ignore[arg-type]


def _ledger_from_payload(payload: object) -> SimulationLedgerEvent:
    if not isinstance(payload, dict):
        raise SimulationRecoveryRefused("Persisted ledger event is malformed.")
    event = SimulationLedgerEvent(int(payload["sequence"]), _enum(payload["event_type"], SimulationLedgerEventType, "ledger event type"), str(payload["event_time"]), payload["payload"])  # type: ignore[arg-type]
    if payload.get("event_id") != event.event_id:
        raise SimulationRecoveryRefused("Persisted ledger event identity is inconsistent.")
    return event


class DeterministicExecutionReplay:
    """Apply an already ordered mixed replay without introducing wall-clock time."""

    def __init__(self, simulator: DeterministicMNQSimulator) -> None:
        if type(simulator) is not DeterministicMNQSimulator:
            raise ValueError("Replay requires an exact L3-E simulator.")
        self.simulator = simulator

    def replay(self, events: Iterable[SignalDecision | SimulatedMarketState | tuple[OperatorCommand, str]]) -> SimulationReplayReport:
        for event in events:
            if type(event) is SignalDecision:
                self.simulator.admit_signal(event)
            elif type(event) is SimulatedMarketState:
                self.simulator.on_market(event)
            elif isinstance(event, tuple) and len(event) == 2 and type(event[0]) is OperatorCommand and isinstance(event[1], str):
                self.simulator.apply_operator_command(event[0], requested_at=event[1])
            else:
                raise SimulationRefused("Replay event is not a frozen signal, market state, or explicit operator command.")
        return self.simulator.report()
