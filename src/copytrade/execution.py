"""Phase-D.0 execution foundation.

The only concrete adapter in this module is a deterministic in-memory
simulator.  There is intentionally no HTTP client, signing code, credential
handling, or live venue adapter here.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any, Iterable, Protocol

from .execution_contracts import (
    ExecutionIntent,
    ExecutionRiskDecision,
    ExecutionSafetyContext,
    ExecutionState,
    ExecutionSubmission,
    ExposureEffect,
    ReconciliationState,
    SubmissionRequest,
    VenueFill,
    VenueOrder,
    VenueOrderStatus,
    VenuePosition,
    order_side,
)
from .models import CopySignal, ExecutionAttempt, ExecutionFill, as_utc, stable_id, utc_now
from .storage import CopyTradeDatabase


EPSILON = 1e-12


class AmbiguousSubmissionError(RuntimeError):
    """The adapter may have transmitted; the engine must reconcile, not retry."""


class InjectedExecutionFault(RuntimeError):
    """Deliberate deterministic process-loss analogue used only by D.3 tests."""


class _StaleVenueObservation(RuntimeError):
    """Internal signal that an observation is not strong enough to clear risk."""


@dataclass
class DeterministicFaultInjector:
    """Raise once at named durable/external checkpoints without sleeping.

    This is intentionally a small callable so the same object can drive the
    paper transaction and the D execution engine.  It records every observed
    checkpoint, which makes a crash test's placement auditable.
    """

    checkpoints: Iterable[str]
    observed: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._remaining = set(self.checkpoints)

    def __call__(self, checkpoint: str) -> None:
        self.observed.append(checkpoint)
        if checkpoint in self._remaining:
            self._remaining.remove(checkpoint)
            raise InjectedExecutionFault(f"injected_execution_fault:{checkpoint}")


class ExecutionAdapter(Protocol):
    """Normalized boundary future venue adapters must satisfy.

    Implementations receive only an approved SubmissionRequest.  They do not
    decide risk, mutate local execution state, or receive signing credentials
    from the ledger.
    """

    adapter_name: str
    adapter_mode: str

    def submit(self, request: SubmissionRequest) -> VenueOrder: ...
    def cancel(self, client_order_id: str) -> VenueOrder: ...
    def get_order(self, client_order_id: str) -> VenueOrder | None: ...
    def list_fills(self, client_order_id: str) -> list[VenueFill]: ...
    def list_open_orders(self) -> list[VenueOrder]: ...
    def get_positions(self) -> list[VenuePosition]: ...
    def get_balances(self) -> dict[str, Any]: ...
    def get_instrument_metadata(self, symbol: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SimulatorPlan:
    """A single submit fault-injection plan for DeterministicExecutionSimulator."""

    mode: str = "immediate_fill"
    fill_quantities: tuple[float, ...] = ()
    price: float = 100.0
    fee_rate: float = 0.0
    reason: str = ""
    hide_order_reads: bool = False


@dataclass
class SimulatedClock:
    """Injectable monotonic clock so scenario replay never depends on sleeps."""

    current: object = "2026-01-01T00:00:00+00:00"

    def now(self) -> object:
        return as_utc(self.current)

    def advance(self, milliseconds: int = 1) -> object:
        self.current = as_utc(self.current) + timedelta(milliseconds=milliseconds)
        return self.now()


@dataclass(frozen=True)
class SimulatorStep:
    """One deterministic venue-side event, reusable across scenario replays."""

    action: str
    quantity: float = 0.0
    price: float = 100.0
    fee: float = 0.0
    artifact_id: str | None = None
    milliseconds: int = 1


@dataclass(frozen=True)
class SimulatorScenario:
    """Explicit ordered script; no randomness or wall-clock timing is implied."""

    name: str
    submit_mode: str = "acknowledged"
    submit_steps: tuple[SimulatorStep, ...] = ()
    reconciliation_steps: tuple[SimulatorStep, ...] = ()


class DeterministicExecutionSimulator:
    """Fault-injection laboratory; never a networked or live execution adapter."""

    adapter_name = "deterministic_execution_simulator"
    adapter_mode = "SIMULATOR_ONLY"

    def __init__(
        self, plans: Iterable[SimulatorPlan] = (), *, scenarios: Iterable[SimulatorScenario] = (),
        clock: SimulatedClock | None = None,
    ) -> None:
        self._plans = list(plans)
        self._scenarios = list(scenarios)
        self.clock = clock or SimulatedClock()
        self._orders: dict[str, VenueOrder] = {}
        self._fills: dict[str, list[VenueFill]] = {}
        self._requests: dict[str, SubmissionRequest] = {}
        self._hidden_reads: set[str] = set()
        self._reported_positions: dict[str, float] | None = None
        self._external_position_symbols: set[str] = set()
        self._external_orders: dict[str, VenueOrder] = {}
        self._unavailable = False
        self._stale_positions: list[VenuePosition] | None = None
        self._pending_reconciliation_steps: dict[str, tuple[SimulatorStep, ...]] = {}
        self.submit_calls = 0

    def submit(self, request: SubmissionRequest) -> VenueOrder:
        # A simulator retry returns the original order.  The engine itself is
        # deliberately stricter and will reconcile rather than call submit a
        # second time after ambiguity.
        self._require_available()
        self.submit_calls += 1
        if request.client_order_id in self._orders:
            return self._orders[request.client_order_id]
        scenario = self._scenarios.pop(0) if self._scenarios else None
        plan = self._plans.pop(0) if self._plans else SimulatorPlan(mode=scenario.submit_mode if scenario else "immediate_fill")
        if plan.mode not in {
            "immediate_fill", "acknowledged", "partial", "rejected", "rejected_timeout", "accepted_timeout",
            "timeout_before_accept", "delayed_ack",
        }:
            raise ValueError(f"Unsupported simulator plan: {plan.mode}")
        if plan.mode == "timeout_before_accept":
            raise AmbiguousSubmissionError("simulated_timeout_before_venue_acceptance")
        self._requests[request.client_order_id] = request
        if plan.mode in {"rejected", "rejected_timeout"}:
            order = VenueOrder(
                client_order_id=request.client_order_id, venue_order_id=stable_id("sim_order", request.client_order_id),
                status=VenueOrderStatus.REJECTED, requested_quantity=request.quantity, filled_quantity=0.0,
                reason=plan.reason or "venue_rejected_simulated", venue_timestamp=self.clock.now(),
                raw_payload={"simulator_plan": plan.mode},
            )
            self._orders[request.client_order_id] = order
            self._fills[request.client_order_id] = []
            if plan.mode == "rejected_timeout":
                raise AmbiguousSubmissionError("simulated_timeout_after_venue_rejection")
            return order
        order = VenueOrder(
            client_order_id=request.client_order_id, venue_order_id=stable_id("sim_order", request.client_order_id),
            status=VenueOrderStatus.ACKNOWLEDGED, requested_quantity=request.quantity, filled_quantity=0.0,
            venue_timestamp=self.clock.now(), raw_payload={"simulator_plan": plan.mode},
        )
        self._orders[request.client_order_id] = order
        self._fills[request.client_order_id] = []
        if plan.hide_order_reads:
            self._hidden_reads.add(request.client_order_id)
        quantities = plan.fill_quantities
        if plan.mode == "immediate_fill" and not quantities:
            quantities = (request.quantity,)
        if plan.mode == "partial" and not quantities:
            quantities = (request.quantity / 2,)
        for index, quantity in enumerate(quantities):
            self.emit_fill(
                request.client_order_id, quantity, price=plan.price, fee=quantity * plan.price * plan.fee_rate,
                venue_fill_id=stable_id("sim_fill", request.client_order_id, index),
            )
        if scenario:
            self._apply_steps(request.client_order_id, scenario.submit_steps)
            self._pending_reconciliation_steps[request.client_order_id] = scenario.reconciliation_steps
        if plan.mode == "accepted_timeout":
            raise AmbiguousSubmissionError("simulated_timeout_after_venue_acceptance")
        return self._orders[request.client_order_id]

    def cancel(self, client_order_id: str) -> VenueOrder:
        self._require_available()
        order = self._orders.get(client_order_id)
        if not order:
            raise AmbiguousSubmissionError("simulated_cancel_unknown_order")
        if order.status is not VenueOrderStatus.FILLED:
            order = VenueOrder(
                **{**order.__dict__, "status": VenueOrderStatus.CANCELLED, "venue_timestamp": self.clock.now(),
                   "raw_payload": {**order.raw_payload, "cancelled": True}}
            )
            self._orders[client_order_id] = order
        return order

    def get_order(self, client_order_id: str) -> VenueOrder | None:
        self._require_available()
        self._apply_steps(client_order_id, self._pending_reconciliation_steps.pop(client_order_id, ()))
        return None if client_order_id in self._hidden_reads else self._orders.get(client_order_id)

    def list_fills(self, client_order_id: str) -> list[VenueFill]:
        self._require_available()
        return list(self._fills.get(client_order_id, ()))

    def get_positions(self) -> list[VenuePosition]:
        self._require_available()
        if self._stale_positions is not None:
            return list(self._stale_positions)
        observed = self.clock.now()
        positions = self._reported_positions if self._reported_positions is not None else self._calculated_positions()
        return [
            VenuePosition(
                symbol=symbol, signed_quantity=quantity, observed_at=observed,
                raw_payload={"simulator": True, "stale": self._stale_positions is not None,
                             "external_manual_activity": symbol in self._external_position_symbols},
            )
            for symbol, quantity in sorted(positions.items()) if abs(quantity) > EPSILON
        ]

    def get_balances(self) -> dict[str, Any]:
        self._require_available()
        return {"mode": self.adapter_mode, "currency": "USD", "available": None}

    def get_instrument_metadata(self, symbol: str) -> dict[str, Any]:
        self._require_available()
        return {"symbol": symbol, "mode": self.adapter_mode, "minimum_quantity": 0.0, "quantity_precision": None}

    def emit_fill(
        self, client_order_id: str, quantity: float, *, price: float = 100.0, fee: float = 0.0,
        venue_fill_id: str | None = None, timestamp: object | None = None, side: str | None = None,
    ) -> VenueFill:
        if client_order_id not in self._orders:
            raise KeyError(f"Unknown simulator client order: {client_order_id}")
        fills = self._fills[client_order_id]
        fill = VenueFill(
            venue_fill_id=venue_fill_id or stable_id("sim_fill", client_order_id, len(fills)),
            client_order_id=client_order_id, quantity=abs(quantity), price=price, fee=fee,
            venue_timestamp=as_utc(timestamp) if timestamp is not None else self.clock.advance(), side=side,
            raw_payload={"simulator": True},
        )
        # Duplicate delivery remains observable to the adapter but is later
        # deduplicated by the Phase-D ledger using venue_fill_id.
        fills.append(fill)
        order = self._orders[client_order_id]
        unique_fills = {item.venue_fill_id: item for item in fills}
        total = sum(item.quantity for item in unique_fills.values())
        status = (
            VenueOrderStatus.FILLED if total + EPSILON >= order.requested_quantity else
            VenueOrderStatus.CANCELLED if order.status is VenueOrderStatus.CANCELLED else
            VenueOrderStatus.PARTIALLY_FILLED
        )
        self._orders[client_order_id] = VenueOrder(
            **{**order.__dict__, "status": status, "filled_quantity": min(total, order.requested_quantity),
               "venue_timestamp": fill.venue_timestamp}
        )
        return fill

    def set_order_read_visible(self, client_order_id: str, visible: bool) -> None:
        if visible:
            self._hidden_reads.discard(client_order_id)
        else:
            self._hidden_reads.add(client_order_id)

    def set_reported_positions(self, positions: dict[str, float] | None) -> None:
        """Inject stale/mismatched venue account truth for reconciliation tests."""
        self._reported_positions = dict(positions) if positions is not None else None
        if positions is None:
            self._external_position_symbols.clear()

    def set_temporary_unavailable(self, unavailable: bool = True) -> None:
        self._unavailable = unavailable

    def set_stale_positions(self, positions: list[VenuePosition] | None) -> None:
        self._stale_positions = list(positions) if positions is not None else None

    def positions_observation_is_fresh(self) -> bool:
        """Optional adapter freshness signal used by fail-closed reconciliation."""
        return self._stale_positions is None

    def inject_external_position(self, symbol: str, signed_quantity: float) -> None:
        """Venue-side manual activity: deliberately has no local intent provenance."""
        positions = dict(self._reported_positions or self._calculated_positions())
        positions[symbol] = signed_quantity
        self._reported_positions = positions
        self._external_position_symbols.add(symbol)

    def inject_external_order(self, symbol: str, signed_quantity: float) -> VenueOrder:
        identifier = stable_id("sim_external_order", symbol, signed_quantity, len(self._external_orders))
        order = VenueOrder(
            client_order_id=identifier, venue_order_id=identifier, status=VenueOrderStatus.ACKNOWLEDGED,
            requested_quantity=abs(signed_quantity), filled_quantity=0.0, venue_timestamp=self.clock.now(),
            raw_payload={"simulator": True, "external_manual_activity": True, "symbol": symbol, "signed_quantity": signed_quantity},
        )
        self._external_orders[identifier] = order
        return order

    def clear_external_orders(self) -> None:
        """Remove injected manual orders so a later observation can clear risk."""
        self._external_orders.clear()

    def list_open_orders(self) -> list[VenueOrder]:
        self._require_available()
        local = [order for order in self._orders.values() if order.status in {
            VenueOrderStatus.ACKNOWLEDGED, VenueOrderStatus.PARTIALLY_FILLED,
        }]
        return [*local, *self._external_orders.values()]

    def replay_steps(self, client_order_id: str, steps: Iterable[SimulatorStep]) -> VenueOrder | None:
        """Apply an explicit ordered script after submission or during reconciliation."""
        self._apply_steps(client_order_id, tuple(steps))
        return self._orders.get(client_order_id)

    def _apply_steps(self, client_order_id: str, steps: Iterable[SimulatorStep]) -> None:
        for step in steps:
            self.clock.advance(step.milliseconds)
            if step.action == "fill":
                self.emit_fill(client_order_id, step.quantity, price=step.price, fee=step.fee, venue_fill_id=step.artifact_id)
            elif step.action == "cancel":
                self.cancel(client_order_id)
            elif step.action == "hide_order":
                self.set_order_read_visible(client_order_id, False)
            elif step.action == "show_order":
                self.set_order_read_visible(client_order_id, True)
            elif step.action == "duplicate_fill":
                fills = self._fills.get(client_order_id, [])
                if not fills:
                    raise ValueError("duplicate_fill requires a previous fill")
                fills.append(fills[-1])
            elif step.action == "external_position":
                self.inject_external_position(self._requests[client_order_id].symbol, step.quantity)
            elif step.action == "unavailable":
                self.set_temporary_unavailable(True)
            elif step.action == "available":
                self.set_temporary_unavailable(False)
            elif step.action != "noop":
                raise ValueError(f"Unsupported simulator script action: {step.action}")

    def _require_available(self) -> None:
        if self._unavailable:
            raise ConnectionError("simulated_venue_temporarily_unavailable")

    def _calculated_positions(self) -> dict[str, float]:
        positions: dict[str, float] = {}
        for client_order_id, fills in self._fills.items():
            request = self._requests[client_order_id]
            signed = sum(
                fill.quantity if (fill.side or request.side).upper() == "BUY" else -fill.quantity
                for fill in {fill.venue_fill_id: fill for fill in fills}.values()
            )
            positions[request.symbol] = positions.get(request.symbol, 0.0) + signed
        return positions


class D0ExecutionRiskGate:
    """Execution policy stays above the adapter and preserves safe exits."""

    def evaluate(self, intent: ExecutionIntent, context: ExecutionSafetyContext) -> tuple[bool, str, dict[str, Any]]:
        evidence = {
            "entry_inhibited": context.entry_inhibited,
            "entry_inhibit_reason": context.entry_inhibit_reason,
            "hard_transport_stop": context.hard_transport_stop,
            "source_recovery_continuous": context.source_recovery_continuous,
            "market_evidence_current": context.market_evidence_current,
            "reconciliation_healthy": context.reconciliation_healthy,
            "verified_positions_current": context.verified_positions_current,
            "verified_positions_authoritative": context.verified_positions_authoritative,
            "effect": intent.exposure_effect.value,
        }
        if context.hard_transport_stop:
            return False, "hard_transport_stop", evidence
        if intent.exposure_effect is ExposureEffect.INCREASE:
            if not context.source_recovery_continuous:
                return False, "entry_blocked_recovery_incomplete", evidence
            if not context.reconciliation_healthy:
                return False, "entry_blocked_reconciliation_required", evidence
            if context.entry_inhibited:
                return False, context.entry_inhibit_reason or "entry_inhibited", evidence
            if not context.market_evidence_current:
                return False, "entry_blocked_stale_market_evidence", evidence
        elif intent.exposure_effect in {ExposureEffect.REDUCE, ExposureEffect.FLATTEN}:
            verified = context.verified_positions.get(intent.symbol)
            if not context.reconciliation_healthy:
                if verified is None or not context.verified_positions_current or not context.verified_positions_authoritative:
                    return False, "reduce_only_verified_position_required", evidence
            if verified is not None:
                intended_sign = 1.0 if intent.direction == "long" else -1.0
                if verified * intended_sign <= EPSILON:
                    return False, "reduce_only_direction_mismatch", evidence
                if intent.requested_quantity > abs(verified) + EPSILON:
                    return False, "reduce_only_size_exceeds_position", evidence
        return True, "approved", evidence


class PaperExecutionLedgerBridge:
    """Additive D.2 projection of authoritative PAPER execution evidence.

    Phase C still owns the paper economic transaction (claim, sleeve mutation,
    legacy fills, and portfolio snapshot).  Once that transaction commits,
    this bridge mirrors its immutable result into the Phase-D ledger exactly
    once.  It never writes a sleeve or changes legacy economics, so the two
    systems cannot both apply a fill.  Restarting with the same signal repairs
    a missing projection from the committed legacy attempt/fills.
    """

    def __init__(self, store: CopyTradeDatabase) -> None:
        self.store = store

    def record(self, signal: CopySignal, attempt: ExecutionAttempt, fills: Iterable[ExecutionFill]) -> ExecutionIntent:
        intent = self.store.create_or_get_execution_intent(
            ExecutionIntent.from_copy_signal(
                signal, accepted_at=attempt.decided_at, execution_domain="PAPER_COMPAT",
                execution_account_id="PAPER_COMPAT:legacy_paper",
            )
        )
        if intent.state is ExecutionState.CREATED:
            intent = self.store.transition_execution_intent(
                intent.intent_id, ExecutionState.VALIDATING, reason="paper_execution_projection_started", source="paper_bridge",
            )
        materialized_fills = tuple(fills)
        allowed = attempt.status == "filled" and bool(materialized_fills)
        projection_reason = attempt.reason if allowed or attempt.status != "filled" else "paper_filled_without_fill_evidence"
        decision = ExecutionRiskDecision(
            decision_id=stable_id("phase_d_paper_risk", intent.intent_id, attempt.attempt_id, attempt.status, attempt.reason),
            intent_id=intent.intent_id, allowed=allowed, reason=projection_reason, evaluated_at=attempt.decided_at,
            evidence={"paper_attempt_id": attempt.attempt_id, "paper_status": attempt.status, "paper_compatibility": True},
        )
        self.store.record_execution_risk_decision(decision)
        if intent.state is ExecutionState.VALIDATING:
            intent = self.store.transition_execution_intent(
                intent.intent_id, ExecutionState.READY if allowed else ExecutionState.BLOCKED,
                reason=projection_reason, source="paper_risk", raw_evidence=decision.evidence,
            )
        if not allowed or intent.state is ExecutionState.FILLED:
            return intent
        if intent.state is not ExecutionState.READY:
            return intent
        quantity = sum(fill.quantity for fill in materialized_fills)
        if quantity <= EPSILON:
            raise ValueError("Paper filled execution cannot project zero fill quantity.")
        submission, _ = self.store.prepare_execution_submission(
            intent.intent_id, submission_id=stable_id("phase_d_paper_submission", intent.intent_id),
            client_order_id=stable_id("phase_d_paper_client_order", intent.intent_id),
            side=order_side(intent.exposure_effect, intent.direction), requested_quantity=quantity,
            created_at=attempt.decided_at,
        )
        for fill in materialized_fills:
            self.store.record_execution_venue_fill(
                intent.intent_id, submission.submission_id,
                VenueFill(
                    venue_fill_id=stable_id("phase_d_paper_fill", attempt.attempt_id, fill.execution_fill_id),
                    client_order_id=submission.client_order_id, quantity=fill.quantity, price=fill.price, fee=fill.fee,
                    venue_timestamp=fill.timestamp,
                    raw_payload={"paper_compatibility": True, "legacy_execution_fill_id": fill.execution_fill_id, **fill.raw},
                ), received_at=attempt.decided_at,
            )
        self.store.update_execution_submission(
            intent.intent_id, state=VenueOrderStatus.FILLED.value,
            venue_order_id=stable_id("phase_d_paper_order", attempt.attempt_id), filled_quantity=quantity,
            raw_evidence={"paper_compatibility": True, "legacy_attempt_id": attempt.attempt_id}, updated_at=attempt.decided_at,
        )
        return self.store.transition_execution_intent(
            intent.intent_id, ExecutionState.FILLED, reason="paper_execution_filled", source="paper_bridge",
            occurred_at=attempt.decided_at,
            raw_evidence={"paper_compatibility": True, "legacy_attempt_id": attempt.attempt_id},
        )


class ExecutionEngine:
    """Durable Phase-D lifecycle coordinator over one explicit account scope."""

    def __init__(
        self, store: CopyTradeDatabase, adapter: ExecutionAdapter, risk_gate: D0ExecutionRiskGate | None = None,
        safety_context: ExecutionSafetyContext | None = None, *, execution_domain: str | None = None,
        execution_account_id: str | None = None,
    ) -> None:
        if adapter.adapter_mode != "SIMULATOR_ONLY":
            raise ValueError("The frozen ExecutionEngine remains SIMULATOR_ONLY.")
        if execution_domain is None:
            execution_domain = "SIMULATOR"
        if execution_account_id is None:
            execution_account_id = "SIMULATOR:default" if execution_domain == "SIMULATOR" else None
        if not execution_account_id:
            raise ValueError("An explicit Phase D execution account identity is required.")
        if execution_domain not in {"SIMULATOR", "LANE_II_SIMULATOR"}:
            raise ValueError("Simulator adapters may only serve a commissioned simulator execution domain.")
        self.store, self.adapter, self.risk_gate = store, adapter, risk_gate or D0ExecutionRiskGate()
        self.execution_domain = execution_domain
        self.execution_account_id = execution_account_id
        # A caller may provide a current authority source at engine creation.
        # There is deliberately no permissive default when neither this nor a
        # per-call context is supplied.
        self.safety_context = safety_context

    def accept_signal(
        self, signal: CopySignal, *, accepted_at: object | None = None, provenance: dict[str, Any] | None = None,
    ) -> ExecutionIntent:
        if self.execution_domain != "SIMULATOR" or self.execution_account_id != "SIMULATOR:default":
            raise ValueError("Phase-C signals cannot be admitted into a Lane II or venue execution scope.")
        return self.store.create_or_get_execution_intent(
            ExecutionIntent.from_copy_signal(
                signal, accepted_at=accepted_at, provenance=provenance, execution_domain=self.execution_domain,
                execution_account_id=self.execution_account_id,
            )
        )

    def process_signal(
        self, signal: CopySignal, *, context: ExecutionSafetyContext | None = None, fault_hook: Any = None,
    ) -> ExecutionIntent:
        intent = self.accept_signal(signal)
        self._checkpoint(fault_hook, "after_intent_persistence")
        return self.resume_intent(intent.intent_id, context=context, fault_hook=fault_hook)

    def resume_intent(
        self, intent_id: str, *, context: ExecutionSafetyContext | None = None, fault_hook: Any = None,
    ) -> ExecutionIntent:
        intent = self._required_intent(intent_id)
        admission_context = context or self.safety_context
        if intent.state in {ExecutionState.CREATED, ExecutionState.VALIDATING}:
            # Restart reconciliation is safe without a new admission decision;
            # admission and submission are not.  Missing context therefore
            # leaves pre-submit work pending rather than assuming health.
            if admission_context is None:
                return intent
            intent = self.validate_intent(intent.intent_id, context=admission_context, fault_hook=fault_hook)
        if intent.state is ExecutionState.READY:
            if admission_context is None or admission_context.hard_transport_stop:
                return intent
            return self.submit_ready_intent(intent.intent_id, context=admission_context, fault_hook=fault_hook)
        if intent.state in {
            ExecutionState.SUBMITTING, ExecutionState.SUBMISSION_UNKNOWN, ExecutionState.RECONCILIATION_REQUIRED,
            ExecutionState.CANCEL_PENDING,
        }:
            return self.reconcile_intent(intent.intent_id, fault_hook=fault_hook)
        return intent

    def validate_intent(self, intent_id: str, *, context: ExecutionSafetyContext, fault_hook: Any = None) -> ExecutionIntent:
        intent = self._required_intent(intent_id)
        if intent.state is ExecutionState.CREATED:
            intent = self.store.transition_execution_intent(
                intent_id, ExecutionState.VALIDATING, reason="risk_validation_started", source="execution_engine",
            )
            self._checkpoint(fault_hook, "after_state_transition_persistence")
        if intent.state is not ExecutionState.VALIDATING:
            return intent
        safety_health = self.store.execution_safety_health(
            execution_domain=intent.execution_domain, execution_account_id=intent.execution_account_id,
        )
        safety = replace(context, reconciliation_healthy=context.reconciliation_healthy and not safety_health["unhealthy"])
        allowed, reason, evidence = self.risk_gate.evaluate(intent, safety)
        if intent.exposure_effect is ExposureEffect.INCREASE and self.store.execution_has_unresolved_entry_risk(
            execution_domain=intent.execution_domain, execution_account_id=intent.execution_account_id,
        ):
            allowed, reason = False, "entry_blocked_reconciliation_required"
            evidence = {
                **evidence,
                "unresolved_phase_d_execution": True,
                "execution_safety_health": safety_health,
            }
        decision = ExecutionRiskDecision(
            decision_id=stable_id("phase_d_risk_decision", intent_id, allowed, reason, evidence),
            intent_id=intent_id, allowed=allowed, reason=reason, evaluated_at=utc_now(), evidence=evidence,
        )
        self.store.record_execution_risk_decision(decision)
        # Another worker may have completed validation and even prepared the
        # submission while this worker evaluated the same immutable intent.
        # Its durable state wins; this worker must not move it backwards.
        current = self._required_intent(intent_id)
        if current.state is not ExecutionState.VALIDATING:
            return current
        result = self.store.transition_execution_intent(
            intent_id, ExecutionState.READY if allowed else ExecutionState.BLOCKED,
            reason=reason, source="risk_gate", raw_evidence=evidence,
        )
        self._checkpoint(fault_hook, "after_state_transition_persistence")
        return result

    def submit_ready_intent(
        self, intent_id: str, *, context: ExecutionSafetyContext, fault_hook: Any = None,
    ) -> ExecutionIntent:
        intent = self._required_intent(intent_id)
        if intent.state is not ExecutionState.READY:
            return self.resume_intent(intent_id, context=context, fault_hook=fault_hook)
        if context.hard_transport_stop:
            return intent
        if intent.exposure_effect is ExposureEffect.INCREASE:
            transport_health = getattr(self.adapter, "entry_transport_health", None)
            if callable(transport_health):
                healthy, reason = transport_health()
                if not healthy:
                    evidence = {"adapter_name": self.adapter.adapter_name, "transport_health": reason}
                    self.store.record_execution_risk_decision(ExecutionRiskDecision(
                        decision_id=stable_id("phase_d_transport_health_decision", intent_id, reason),
                        intent_id=intent_id, allowed=False, reason=reason, evaluated_at=utc_now(), evidence=evidence,
                    ))
                    return self.store.transition_execution_intent(
                        intent_id, ExecutionState.BLOCKED, reason=reason,
                        source="transport_health_gate", raw_evidence=evidence,
                    )
        # Re-check volatile account safety at the adapter boundary.  An intent
        # can be READY from an earlier admission while new reconciliation or
        # integrity evidence arrives; that earlier approval must never authorize
        # an increase, or an unbounded exit, after the safety change.
        safety_health = self.store.execution_safety_health(
            execution_domain=intent.execution_domain, execution_account_id=intent.execution_account_id,
        )
        if safety_health["unhealthy"]:
            safety = replace(context, reconciliation_healthy=context.reconciliation_healthy and not safety_health["unhealthy"])
            allowed, reason, evidence = self.risk_gate.evaluate(intent, safety)
            if not allowed:
                evidence = {**evidence, "execution_safety_health": safety_health, "checked_at_submit_boundary": True}
                self.store.record_execution_risk_decision(ExecutionRiskDecision(
                    decision_id=stable_id("phase_d_submit_safety_decision", intent_id, allowed, reason, evidence),
                    intent_id=intent_id, allowed=allowed, reason=reason, evaluated_at=utc_now(), evidence=evidence,
                ))
                return self.store.transition_execution_intent(
                    intent_id, ExecutionState.BLOCKED, reason=reason, source="submit_safety_gate", raw_evidence=evidence,
                )
        submission, should_submit = self.store.prepare_execution_submission(
            intent_id,
            submission_id=stable_id("phase_d_submission_v1", intent_id),
            client_order_id=stable_id("phase_d_client_order_v1", intent_id),
            side=order_side(intent.exposure_effect, intent.direction), requested_quantity=intent.requested_quantity,
        )
        # A concurrent worker found a durably prepared identity.  It has no
        # evidence that the first worker did not reach the adapter, so it must
        # reconcile instead of issuing another submission call.
        if not should_submit:
            return self.reconcile_intent(intent_id)
        request = SubmissionRequest(
            intent_id=intent.intent_id, submission_id=submission.submission_id, client_order_id=submission.client_order_id,
            symbol=intent.symbol, side=submission.side, quantity=submission.requested_quantity,
            exposure_effect=intent.exposure_effect, reduce_only=intent.exposure_effect in {ExposureEffect.REDUCE, ExposureEffect.FLATTEN},
        )
        try:
            self._checkpoint(fault_hook, "before_submit")
            order = self.adapter.submit(request)
            self._checkpoint(fault_hook, "after_external_submit")
        except AmbiguousSubmissionError as exc:
            return self.store.transition_execution_intent(
                intent_id, ExecutionState.SUBMISSION_UNKNOWN, reason="submission_unknown_timeout",
                source="adapter", raw_evidence={
                    "error_class": type(exc).__name__, "client_order_id": submission.client_order_id,
                },
            )
        except Exception as exc:
            # At this boundary failure is ambiguous unless an adapter returns
            # authoritative venue rejection evidence.
            return self.store.transition_execution_intent(
                intent_id, ExecutionState.SUBMISSION_UNKNOWN, reason="submission_unknown_adapter_error",
                source="adapter", raw_evidence={
                    "error_class": type(exc).__name__, "client_order_id": submission.client_order_id,
                },
            )
        self._checkpoint(fault_hook, "before_local_ack_persistence")
        return self._apply_venue_order(intent, submission, order, source="submit_ack", fault_hook=fault_hook)

    def request_cancel(self, intent_id: str, *, fault_hook: Any = None) -> ExecutionIntent:
        intent = self._required_intent(intent_id)
        if intent.state not in {ExecutionState.ACKNOWLEDGED, ExecutionState.PARTIALLY_FILLED}:
            return intent
        intent = self.store.transition_execution_intent(
            intent_id, ExecutionState.CANCEL_PENDING, reason="cancellation_requested", source="execution_engine",
        )
        submission = self._required_submission(intent_id)
        try:
            self._checkpoint(fault_hook, "before_cancel")
            order = self.adapter.cancel(submission.client_order_id)
            self._checkpoint(fault_hook, "after_cancel_acceptance")
        except Exception as exc:
            return self.store.transition_execution_intent(
                intent_id, ExecutionState.RECONCILIATION_REQUIRED, reason="cancel_outcome_unknown", source="adapter",
                raw_evidence={"error_class": type(exc).__name__},
            )
        return self._apply_venue_order(intent, submission, order, source="cancel_ack", fault_hook=fault_hook)

    def reconcile_intent(self, intent_id: str, *, fault_hook: Any = None) -> ExecutionIntent:
        intent = self._required_intent(intent_id)
        submission = self.store.get_execution_submission(intent_id)
        if submission is None:
            return intent
        now = utc_now()
        run_id = stable_id("phase_d_reconcile_order", intent_id, now)
        self.store.start_execution_reconciliation(
            run_id, scope="intent_order", started_at=now, evidence={"intent_id": intent_id},
            execution_domain=intent.execution_domain, execution_account_id=intent.execution_account_id,
        )
        self._checkpoint(fault_hook, "before_reconciliation")
        try:
            fills = self.adapter.list_fills(submission.client_order_id)
            for fill in fills:
                self.store.record_execution_venue_fill(intent_id, submission.submission_id, fill, received_at=now)
            order = self.adapter.get_order(submission.client_order_id)
        except Exception as exc:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "adapter_error"),
                item_type="order", state=ReconciliationState.INCOMPLETE.value, reason="reconciliation_adapter_error",
                intent_id=intent_id, submission_id=submission.submission_id,
                venue={"error_class": type(exc).__name__}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now)
            if intent.state is ExecutionState.SUBMITTING:
                return self.store.transition_execution_intent(
                    intent_id, ExecutionState.SUBMISSION_UNKNOWN, reason="reconciliation_unavailable", source="reconciliation",
                )
            return intent
        if order is None:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "unknown_order"),
                item_type="order", state=ReconciliationState.UNKNOWN_ORDER.value, reason="submission_identity_not_found",
                intent_id=intent_id, submission_id=submission.submission_id,
                local={"client_order_id": submission.client_order_id}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now)
            if intent.state is ExecutionState.SUBMITTING:
                return self.store.transition_execution_intent(
                    intent_id, ExecutionState.SUBMISSION_UNKNOWN, reason="submission_unknown_reconciliation_pending",
                    source="reconciliation",
                )
            return intent
        self.store.record_execution_reconciliation_item(
            reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "order", order.status.value),
            item_type="order", state=ReconciliationState.MATCHED.value, reason="venue_order_matched",
            intent_id=intent_id, submission_id=submission.submission_id,
            local={"client_order_id": submission.client_order_id}, venue=self._venue_order_evidence(order), recorded_at=now,
        )
        result = self._apply_venue_order(intent, submission, order, source="reconciliation", fault_hook=fault_hook)
        reconciliation_state = (
            ReconciliationState.INCOMPLETE.value
            if result.state in {ExecutionState.SUBMISSION_UNKNOWN, ExecutionState.RECONCILIATION_REQUIRED}
            else ReconciliationState.MATCHED.value
        )
        self.store.complete_execution_reconciliation(run_id, state=reconciliation_state, completed_at=now)
        return result

    def reconcile_positions(self, *, fault_hook: Any = None) -> dict[str, Any]:
        """Record account evidence; unavailable or stale reads never clear risk."""
        now = utc_now()
        run_id = stable_id("phase_d_reconcile_positions", now)
        self.store.start_execution_reconciliation(
            run_id, scope="account_positions", started_at=now, execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
        )
        local = self.store.phase_d_local_positions(
            execution_domain=self.execution_domain, execution_account_id=self.execution_account_id,
        )
        try:
            self._checkpoint(fault_hook, "before_reconciliation")
            venue_rows = self.adapter.get_positions()
            freshness = getattr(self.adapter, "positions_observation_is_fresh", None)
            if not callable(freshness) or not freshness():
                raise _StaleVenueObservation("position_observation_stale_or_freshness_unavailable")
        except _StaleVenueObservation as exc:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "stale_positions"),
                item_type="position", state=ReconciliationState.INCOMPLETE.value, reason=str(exc),
                local={"positions": local}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(
                run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now, evidence={"reason": str(exc)},
            )
            return {"reconciliation_run_id": run_id, "state": ReconciliationState.INCOMPLETE.value,
                    "mismatches": 0, "local_positions": local, "reason": str(exc)}
        except Exception as exc:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "adapter_error"),
                item_type="position", state=ReconciliationState.INCOMPLETE.value, reason="reconciliation_adapter_error",
                local={"positions": local}, venue={"error_class": type(exc).__name__}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(
                run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now,
                evidence={"reason": "reconciliation_adapter_error"},
            )
            return {"reconciliation_run_id": run_id, "state": ReconciliationState.INCOMPLETE.value,
                    "mismatches": 0, "local_positions": local, "reason": "reconciliation_adapter_error"}
        venue = {row.symbol: row for row in venue_rows}
        symbols = sorted(set(local) | set(venue))
        mismatches = 0
        try:
            for symbol in symbols:
                expected = local.get(symbol, 0.0)
                observed = venue.get(symbol)
                actual = observed.signed_quantity if observed else 0.0
                raw = observed.raw_payload if observed else {}
                if abs(expected - actual) <= EPSILON:
                    state, reason = ReconciliationState.MATCHED.value, "position_matched"
                elif raw.get("external_manual_activity") and abs(expected) <= EPSILON:
                    state, reason = ReconciliationState.UNKNOWN_POSITION.value, "external_manual_position"
                elif observed is None:
                    state, reason = ReconciliationState.MISMATCH.value, "local_position_missing_at_venue"
                elif expected * actual < -EPSILON:
                    state, reason = ReconciliationState.MISMATCH.value, "position_direction_mismatch"
                else:
                    state, reason = ReconciliationState.MISMATCH.value, "position_quantity_mismatch"
                if state != ReconciliationState.MATCHED.value:
                    mismatches += 1
                observation_id = stable_id("phase_d_position_observation", run_id, symbol)
                self.store.record_execution_position_observation(
                    observation_id=observation_id, reconciliation_run_id=run_id, symbol=symbol,
                    local_signed_quantity=expected, venue_signed_quantity=actual, state=state,
                    observed_at=now, raw_evidence=raw, execution_domain=self.execution_domain,
                    execution_account_id=self.execution_account_id,
                )
                self._checkpoint(fault_hook, "after_position_observation")
                self.store.record_execution_reconciliation_item(
                    reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "position", symbol),
                    item_type="position", state=state, reason=reason,
                    local={"symbol": symbol, "signed_quantity": expected},
                    venue={"symbol": symbol, "signed_quantity": actual, "raw": raw}, recorded_at=now,
                )
        except Exception as exc:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "partial"),
                item_type="position", state=ReconciliationState.INCOMPLETE.value, reason="reconciliation_interrupted",
                local={"positions": local}, venue={"error_class": type(exc).__name__}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(
                run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now,
                evidence={"reason": "reconciliation_interrupted"},
            )
            return {"reconciliation_run_id": run_id, "state": ReconciliationState.INCOMPLETE.value,
                    "mismatches": mismatches, "local_positions": local, "reason": "reconciliation_interrupted"}
        result_state = (
            ReconciliationState.VERIFIED_FLAT.value if not symbols else
            ReconciliationState.MATCHED.value if mismatches == 0 else ReconciliationState.MISMATCH.value
        )
        self.store.complete_execution_reconciliation(run_id, state=result_state, completed_at=now, evidence={"mismatches": mismatches})
        return {"reconciliation_run_id": run_id, "state": result_state, "mismatches": mismatches, "local_positions": local}

    def reconcile_open_orders(self) -> dict[str, Any]:
        """Record the independent authority for orders able to create exposure.

        Position reconciliation is not evidence that no venue order remains.
        This narrow observation therefore has its own scope and only a newer
        successful observation of that scope can clear an open-order latch.
        """
        now = utc_now()
        run_id = stable_id("phase_d_reconcile_open_orders", now)
        self.store.start_execution_reconciliation(
            run_id, scope="open_orders", started_at=now, execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
        )
        try:
            list_open_orders = getattr(self.adapter, "list_open_orders", None)
            if not callable(list_open_orders):
                raise _StaleVenueObservation("open_order_observation_unavailable")
            open_orders = list_open_orders()
        except Exception as exc:
            reason = str(exc) if isinstance(exc, _StaleVenueObservation) else "open_order_reconciliation_adapter_error"
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "observation_error"),
                item_type="open_order", state=ReconciliationState.INCOMPLETE.value, reason=reason,
                venue={"error_class": type(exc).__name__}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(
                run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now, evidence={"reason": reason},
            )
            return {
                "reconciliation_run_id": run_id,
                "state": ReconciliationState.INCOMPLETE.value,
                "reason": reason,
                "active_orders": [],
            }
        active_orders = [
            order for order in open_orders
            if order.status in {VenueOrderStatus.ACKNOWLEDGED, VenueOrderStatus.PARTIALLY_FILLED}
        ]
        state = ReconciliationState.MATCHED.value if not active_orders else ReconciliationState.INCOMPLETE.value
        reason = "open_orders_clear" if not active_orders else "open_order_present"
        for order in active_orders:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id,
                item_id=stable_id("phase_d_reconcile_item", run_id, "open_order", order.client_order_id),
                item_type="open_order", state=ReconciliationState.INCOMPLETE.value, reason="open_order_present",
                venue=self._venue_order_evidence(order), recorded_at=now,
            )
        self.store.record_execution_reconciliation_item(
            reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "summary"),
            item_type="open_order_authority", state=state, reason=reason,
            venue={"open_order_ids": [item.client_order_id for item in active_orders]}, recorded_at=now,
        )
        self.store.complete_execution_reconciliation(
            run_id, state=state, completed_at=now,
            evidence={"reason": reason, "open_orders": len(active_orders)},
        )
        return {
            "reconciliation_run_id": run_id,
            "state": state,
            "reason": reason,
            "active_orders": active_orders,
        }

    def verify_flat(self, *, fault_hook: Any = None) -> dict[str, Any]:
        """Fail closed before declaring a recovery baseline safely flat.

        This observes venue positions and orders but never rebaselines or
        rewrites local accounting.  A future explicit operator action must
        consume this evidence separately.
        """
        now = utc_now()
        run_id = stable_id("phase_d_verify_flat", now)
        self.store.start_execution_reconciliation(
            run_id, scope="verified_flat", started_at=now, execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
        )
        local = self.store.phase_d_local_positions(
            execution_domain=self.execution_domain, execution_account_id=self.execution_account_id,
        )
        open_order_observation = self.reconcile_open_orders()
        try:
            self._checkpoint(fault_hook, "before_reconciliation")
            venue_rows = self.adapter.get_positions()
            freshness = getattr(self.adapter, "positions_observation_is_fresh", None)
            if not callable(freshness) or not freshness():
                raise _StaleVenueObservation("position_observation_stale_or_freshness_unavailable")
        except Exception as exc:
            reason = str(exc) if isinstance(exc, _StaleVenueObservation) else "verified_flat_adapter_error"
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "verification_error"),
                item_type="verified_flat", state=ReconciliationState.INCOMPLETE.value, reason=reason,
                local={"positions": local}, venue={"error_class": type(exc).__name__}, recorded_at=now,
            )
            self.store.complete_execution_reconciliation(
                run_id, state=ReconciliationState.INCOMPLETE.value, completed_at=now, evidence={"reason": reason},
            )
            return {"reconciliation_run_id": run_id, "state": ReconciliationState.INCOMPLETE.value, "reason": reason}
        venue_positions = {row.symbol: row.signed_quantity for row in venue_rows if abs(row.signed_quantity) > EPSILON}
        unresolved = self.store.execution_unresolved_submissions(
            execution_domain=self.execution_domain, execution_account_id=self.execution_account_id,
        )
        active_orders = open_order_observation["active_orders"]
        failures: list[str] = []
        if local:
            failures.append("local_position_not_flat")
        if venue_positions:
            failures.append("venue_position_not_flat")
        if open_order_observation["state"] != ReconciliationState.MATCHED.value:
            failures.append(str(open_order_observation["reason"]))
        if unresolved:
            failures.append("unresolved_submission_present")
        state = ReconciliationState.VERIFIED_FLAT.value if not failures else ReconciliationState.INCOMPLETE.value
        reason = "verified_flat" if not failures else ",".join(failures)
        for order in active_orders:
            self.store.record_execution_reconciliation_item(
                reconciliation_run_id=run_id,
                item_id=stable_id("phase_d_reconcile_item", run_id, "open_order", order.client_order_id),
                item_type="open_order", state=ReconciliationState.INCOMPLETE.value, reason="open_order_present",
                venue=self._venue_order_evidence(order), recorded_at=now,
            )
        self.store.record_execution_reconciliation_item(
            reconciliation_run_id=run_id, item_id=stable_id("phase_d_reconcile_item", run_id, "verified_flat"),
            item_type="verified_flat", state=state, reason=reason,
            local={"positions": local, "unresolved_intent_ids": [item["intent_id"] for item in unresolved]},
            venue={"positions": venue_positions, "open_order_ids": [item.client_order_id for item in active_orders]}, recorded_at=now,
        )
        self.store.complete_execution_reconciliation(
            run_id, state=state, completed_at=now,
            evidence={"reason": reason, "local_positions": local, "venue_positions": venue_positions,
                      "open_orders": len(active_orders), "unresolved_submissions": len(unresolved)},
        )
        return {"reconciliation_run_id": run_id, "state": state, "reason": reason,
                "local_positions": local, "venue_positions": venue_positions,
                "open_orders": len(active_orders), "unresolved_submissions": len(unresolved),
                "open_order_reconciliation_run_id": open_order_observation["reconciliation_run_id"]}

    def startup_reconcile(self, *, fault_hook: Any = None) -> dict[str, Any]:
        """Rebuild venue truth before a networked adapter may accept entries."""
        active_states = {
            ExecutionState.SUBMITTING, ExecutionState.SUBMISSION_UNKNOWN, ExecutionState.ACKNOWLEDGED,
            ExecutionState.PARTIALLY_FILLED, ExecutionState.CANCEL_PENDING,
            ExecutionState.RECONCILIATION_REQUIRED,
        }
        repaired: list[str] = []
        for intent in self.store.list_execution_intents(states=active_states):
            if (
                intent.execution_domain == self.execution_domain
                and intent.execution_account_id == self.execution_account_id
            ):
                self.reconcile_intent(intent.intent_id, fault_hook=fault_hook)
                repaired.append(intent.intent_id)
        positions = self.reconcile_positions(fault_hook=fault_hook)
        orders = self.reconcile_open_orders()
        matched = (
            positions["state"] in {ReconciliationState.MATCHED.value, ReconciliationState.VERIFIED_FLAT.value}
            and orders["state"] == ReconciliationState.MATCHED.value
        )
        if matched:
            mark_reconciled = getattr(self.adapter, "mark_startup_reconciled", None)
            if callable(mark_reconciled):
                mark_reconciled()
        return {
            "state": ReconciliationState.MATCHED.value if matched else ReconciliationState.INCOMPLETE.value,
            "positions": positions,
            "open_orders": orders,
            "reconciled_intent_ids": repaired,
            "entries_eligible": matched,
        }

    def _apply_venue_order(
        self, intent: ExecutionIntent, submission: ExecutionSubmission, order: VenueOrder, *, source: str,
        fault_hook: Any = None,
    ) -> ExecutionIntent:
        # Fill events can arrive before their acknowledgement.  Persist and
        # deduplicate them first, then derive state from the strongest evidence.
        for fill in self.adapter.list_fills(submission.client_order_id):
            self._checkpoint(fault_hook, "before_local_fill_persistence")
            self.store.record_execution_venue_fill(intent.intent_id, submission.submission_id, fill)
            self._checkpoint(fault_hook, "after_local_fill_persistence")
        persisted_fills = sum(float(row["quantity"]) for row in self.store.list_execution_fills(
            intent.intent_id, execution_domain=intent.execution_domain, execution_account_id=intent.execution_account_id,
        ))
        updated_submission = self.store.update_execution_submission(
            intent.intent_id, state=order.status.value, venue_order_id=order.venue_order_id,
            filled_quantity=order.filled_quantity, raw_evidence=self._venue_order_evidence(order), updated_at=order.venue_timestamp,
        )
        self._checkpoint(fault_hook, "after_submission_persistence")
        if self.store.execution_has_integrity_issue(
            intent_id=intent.intent_id, execution_domain=intent.execution_domain,
            execution_account_id=intent.execution_account_id,
        ):
            return self.store.transition_execution_intent(
                intent.intent_id, ExecutionState.RECONCILIATION_REQUIRED, reason="execution_integrity_failure",
                source=source, raw_evidence={"submission_id": submission.submission_id},
            )
        if order.filled_quantity > persisted_fills + EPSILON:
            # A venue position/order is evidence of exposure, not permission to
            # invent fills.  Surface missing fill provenance and fail closed.
            return self.store.transition_execution_intent(
                intent.intent_id, ExecutionState.RECONCILIATION_REQUIRED, reason="venue_fill_evidence_incomplete",
                source=source, raw_evidence={"venue_filled_quantity": order.filled_quantity, "local_filled_quantity": persisted_fills},
            )
        next_state = self._state_for_order(order, persisted_fills, updated_submission.requested_quantity)
        current = self._required_intent(intent.intent_id)
        if current.state is ExecutionState.CANCEL_PENDING and next_state is ExecutionState.ACKNOWLEDGED:
            # A restart between cancellation persistence and venue response
            # must not treat a stale open-order read as a cancellation rollback.
            return current
        result = self.store.transition_execution_intent(
            intent.intent_id, next_state, reason=self._order_reason(order), source=source,
            raw_evidence=self._venue_order_evidence(order),
        )
        self._checkpoint(fault_hook, "after_state_transition_persistence")
        return result

    @staticmethod
    def _state_for_order(order: VenueOrder, persisted_fills: float, requested_quantity: float) -> ExecutionState:
        if order.status is VenueOrderStatus.REJECTED:
            return ExecutionState.REJECTED_BY_VENUE
        if order.status is VenueOrderStatus.EXPIRED:
            return ExecutionState.EXPIRED
        if persisted_fills + EPSILON >= requested_quantity or order.status is VenueOrderStatus.FILLED:
            return ExecutionState.FILLED
        if order.status is VenueOrderStatus.CANCELLED:
            return ExecutionState.CANCELLED
        if persisted_fills > EPSILON or order.status is VenueOrderStatus.PARTIALLY_FILLED:
            return ExecutionState.PARTIALLY_FILLED
        return ExecutionState.ACKNOWLEDGED

    @staticmethod
    def _order_reason(order: VenueOrder) -> str:
        return order.reason or f"venue_{order.status.value.lower()}"

    @staticmethod
    def _venue_order_evidence(order: VenueOrder) -> dict[str, Any]:
        return {
            "client_order_id": order.client_order_id, "venue_order_id": order.venue_order_id,
            "status": order.status.value, "requested_quantity": order.requested_quantity,
            "filled_quantity": order.filled_quantity, "reason": order.reason,
            "venue_timestamp": order.venue_timestamp, "raw_payload": order.raw_payload,
        }

    def _required_intent(self, intent_id: str) -> ExecutionIntent:
        intent = self.store.get_execution_intent(intent_id)
        if not intent:
            raise KeyError(f"Unknown Phase-D execution intent: {intent_id}")
        if (
            intent.execution_domain != self.execution_domain
            or intent.execution_account_id != self.execution_account_id
        ):
            raise ValueError("Phase-D intent does not belong to this engine's execution scope.")
        return intent

    def _required_submission(self, intent_id: str) -> ExecutionSubmission:
        submission = self.store.get_execution_submission(intent_id)
        if not submission:
            raise KeyError(f"No Phase-D submission for intent: {intent_id}")
        return submission

    @staticmethod
    def _checkpoint(fault_hook: Any, checkpoint: str) -> None:
        if fault_hook:
            fault_hook(checkpoint)


class HyperliquidTestnetExecutionEngine(ExecutionEngine):
    """Additive F.3 coordinator that cannot weaken the frozen simulator engine."""

    def __init__(
        self, store: CopyTradeDatabase, adapter: ExecutionAdapter,
        risk_gate: D0ExecutionRiskGate | None = None,
        safety_context: ExecutionSafetyContext | None = None,
        *, execution_account_id: str,
    ) -> None:
        if adapter.adapter_mode != "HYPERLIQUID_TESTNET":
            raise ValueError("HyperliquidTestnetExecutionEngine requires HYPERLIQUID_TESTNET.")
        if not execution_account_id or execution_account_id in {"SIMULATOR:default", "HYPERLIQUID:default"}:
            raise ValueError("An explicit Hyperliquid testnet execution account identity is required.")
        adapter_account = getattr(adapter, "execution_account_id", None)
        if adapter_account is not None and adapter_account != execution_account_id:
            raise ValueError("Hyperliquid testnet adapter and engine account identities do not match.")
        self.store = store
        self.adapter = adapter
        self.risk_gate = risk_gate or D0ExecutionRiskGate()
        self.execution_domain = "HYPERLIQUID_TESTNET"
        self.execution_account_id = execution_account_id
        self.safety_context = safety_context
