"""Lane III-G paper runtime, state machine, and observation fan-out."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading
from typing import Callable, Mapping

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation, NinjaTraderObservationError
from src.l3f_provider.tradovate_observation import StreamHealth
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    ACCOUNT_BINDING,
    AUTHORITY,
    POLICY,
    RISK_PROFILE,
    ExecutionAction,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperExecutionCommand,
    PaperRuntimeState,
    deterministic_id,
)
from .ledger import PaperLedger
from .ninjatrader_transport import NinjaTraderSim101PaperAdapter, PaperExecutionTransport
from .policy import ExperimentalPaperPolicy
from .risk import PaperRiskAuthority, PaperRiskSnapshot


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ObservationFanout:
    """Ordered independent sinks behind the one existing observation owner."""

    def __init__(
        self,
        *,
        shadow_observation: Callable[[NinjaTraderObservation], None],
        shadow_transport: Callable[[StreamHealth], None],
        shadow_rejection: Callable[[NinjaTraderObservationError], None],
        shadow_duplicate: Callable[[], None],
        paper_observation: Callable[[NinjaTraderObservation], None],
        paper_transport: Callable[[StreamHealth], None],
        paper_rejection: Callable[[NinjaTraderObservationError], None],
        paper_duplicate: Callable[[], None],
        record_failure: Callable[[str, str, str], None],
    ) -> None:
        callbacks = (
            shadow_observation, shadow_transport, shadow_rejection, shadow_duplicate,
            paper_observation, paper_transport, paper_rejection, paper_duplicate, record_failure,
        )
        if not all(callable(callback) for callback in callbacks):
            raise ValueError("Observation fan-out sinks must be callable.")
        self._shadow_observation = shadow_observation
        self._shadow_transport = shadow_transport
        self._shadow_rejection = shadow_rejection
        self._shadow_duplicate = shadow_duplicate
        self._paper_observation = paper_observation
        self._paper_transport = paper_transport
        self._paper_rejection = paper_rejection
        self._paper_duplicate = paper_duplicate
        self._record_failure = record_failure
        self._lock = threading.RLock()

    def _deliver(self, event: str, shadow: Callable[..., None], paper: Callable[..., None], *args: object) -> None:
        # One lock preserves admitted order across listener callbacks. Each
        # sink failure is isolated and durably recorded by the paper ledger.
        with self._lock:
            try:
                shadow(*args)
            except Exception as exc:
                self._record_failure("SHADOW", event, type(exc).__name__)
            try:
                paper(*args)
            except Exception as exc:
                self._record_failure("EXPERIMENTAL_PAPER", event, type(exc).__name__)

    def on_observation(self, observation: NinjaTraderObservation) -> None:
        self._deliver("OBSERVATION", self._shadow_observation, self._paper_observation, observation)

    def on_transport_state(self, state: StreamHealth) -> None:
        self._deliver("TRANSPORT_STATE", self._shadow_transport, self._paper_transport, state)

    def on_rejection(self, error: NinjaTraderObservationError) -> None:
        self._deliver("REJECTION", self._shadow_rejection, self._paper_rejection, error)

    def on_duplicate(self) -> None:
        self._deliver("DUPLICATE", self._shadow_duplicate, self._paper_duplicate)


class LaneIIIPaperRuntime:
    """Explicit paper execution state machine; starts disarmed every time."""

    def __init__(
        self,
        ledger: PaperLedger,
        *,
        policy: ExperimentalPaperPolicy | None = None,
        risk: PaperRiskAuthority | None = None,
    ) -> None:
        if type(ledger) is not PaperLedger:
            raise ValueError("Paper runtime requires the exact durable ledger.")
        self.ledger = ledger
        self.policy = policy or ExperimentalPaperPolicy()
        self.risk = risk or PaperRiskAuthority()
        if type(self.policy) is not ExperimentalPaperPolicy or type(self.risk) is not PaperRiskAuthority:
            raise ValueError("Paper runtime components must retain exact authority types.")
        self._lock = threading.RLock()
        self._state = PaperRuntimeState.DISABLED
        self._position = PaperDirection.FLAT
        self._position_quantity = 0
        self._entries_paused = False
        self._disarm_after_flat = False
        self._transport: PaperExecutionTransport | None = None
        self._adapter: NinjaTraderSim101PaperAdapter | None = None
        self._snapshot = PaperRiskSnapshot(_now())
        self._last_decision: PaperDecision | None = None
        self._last_command: PaperExecutionCommand | None = None
        self._last_order_state: Mapping[str, object] | None = None
        self._last_execution: Mapping[str, object] | None = None
        self._last_reconciliation: Mapping[str, object] | None = None
        self._last_quote: tuple[Decimal, Decimal, str] | None = None
        self._last_trade: tuple[Decimal, str] | None = None
        self._last_depth_at: str | None = None
        self._recorded_evidence: set[str] = set()
        self._pending_intent: object | None = None
        self._pending_grant: object | None = None
        self._entry_fill_price: Decimal | None = None
        self._entry_fill_quantity = 0
        self._entry_direction = PaperDirection.FLAT
        self._command_sequence = 0
        self._fault_reason: str | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._transitions = 0

    def bind_transport(self, transport: PaperExecutionTransport) -> None:
        if type(transport) is not PaperExecutionTransport:
            raise ValueError("Paper runtime accepts only the signed Sim101 transport.")
        with self._lock:
            if self._transport is not None or self._state is not PaperRuntimeState.DISABLED:
                raise RuntimeError("Paper execution transport may be bound exactly once before startup.")
            self._transport = transport
            self._adapter = NinjaTraderSim101PaperAdapter(transport)

    @property
    def state(self) -> PaperRuntimeState:
        with self._lock:
            return self._state

    def _transition(self, target: PaperRuntimeState, reason: str) -> None:
        prior = self._state
        allowed: dict[PaperRuntimeState, set[PaperRuntimeState]] = {
            PaperRuntimeState.DISABLED: {PaperRuntimeState.STARTING},
            PaperRuntimeState.STARTING: {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.FAULTED},
            PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE: {PaperRuntimeState.RECONCILING, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.RECONCILING: {PaperRuntimeState.READY_DISARMED, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.READY_DISARMED: {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING},
            PaperRuntimeState.ARMED_FLAT: {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.ENTRY_PENDING: {PaperRuntimeState.LONG, PaperRuntimeState.SHORT, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.LONG: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.SHORT: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.EXIT_PENDING: {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.PAUSED: {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.LONG, PaperRuntimeState.SHORT, PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
            PaperRuntimeState.LOCKED_OUT: {PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
            PaperRuntimeState.FAULTED: {PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
            PaperRuntimeState.STOPPING: {PaperRuntimeState.STOPPED},
            PaperRuntimeState.STOPPED: set(),
        }
        if target not in allowed[prior]:
            raise RuntimeError(f"Illegal paper state transition {prior.value} -> {target.value}.")
        self._state = target
        self._transitions += 1
        transition_identity = "l3g-transition-" + canonical_hash({"number": self._transitions, "prior": prior.value, "target": target.value, "reason": reason, "process_session": id(self)})
        self.ledger.append("SESSION_TRANSITION", {"prior_state": prior.value, "state": target.value, "reason": reason}, identity=transition_identity, execution_session_id=self._execution_session_id())

    def start(self) -> None:
        with self._lock:
            if self._transport is None:
                raise RuntimeError("Paper execution transport must be bound before startup.")
            self._transition(PaperRuntimeState.STARTING, "PROCESS_START")
            self._transition(PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, "STARTS_DISARMED")
            self.ledger.append("SESSION_AUTHORITY", AUTHORITY.authority_payload(), identity="l3g-authority-" + canonical_hash({"started_at": _now(), "object": id(self)}))
            self._heartbeat_stop = threading.Event()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="L3GPaperHeartbeat", daemon=True)
            self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(1.0):
            with self._lock:
                transport = self._transport
                armed = self._state in {
                    PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.LONG,
                    PaperRuntimeState.SHORT, PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAUSED,
                }
            if transport is None:
                continue
            try:
                transport.send_heartbeat(armed=armed)
            except RuntimeError:
                # The transport callback owns the state transition. Heartbeat
                # failures are expected during reconnect and never imply flat.
                continue

    def _execution_session_id(self) -> str | None:
        return None if self._transport is None else self._transport.status().execution_session_id

    def on_execution_bridge_state(self, state: str) -> None:
        with self._lock:
            healthy = state == "AUTHENTICATED"
            self._snapshot = replace(self._snapshot, observed_at=_now(), execution_bridge_healthy=healthy, reconciliation_current=False if state in {"CONNECTED", "DISCONNECTED", "AUTHENTICATED"} else self._snapshot.reconciliation_current)
            if state == "AUTHENTICATED":
                self._command_sequence = 0
                if self._state in {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED}:
                    self._entries_paused = False
                    self._transition(PaperRuntimeState.RECONCILING, "EXECUTION_BRIDGE_AUTHENTICATED")
            elif state == "DISCONNECTED":
                self.policy.reset("EXECUTION_BRIDGE_DISCONNECTED")
                if self._position is not PaperDirection.FLAT or self._state in {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.EXIT_PENDING}:
                    self._fault_reason = "EXECUTION_BRIDGE_DISCONNECTED_WITH_ACTIVITY"
                    self.risk.lock_out(self._fault_reason)
                    if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                elif self._state not in {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    # A clean flat reconnect still requires a fresh snapshot.
                    if self._state in {PaperRuntimeState.RECONCILING, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.FAULTED, PaperRuntimeState.LOCKED_OUT}:
                        self._transition(PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, "EXECUTION_BRIDGE_DISCONNECTED")

    def on_observation_transport_state(self, state: StreamHealth) -> None:
        self.policy.on_transport_state(state)
        with self._lock:
            self._snapshot = replace(self._snapshot, observed_at=_now(), local_bridge_healthy=state is StreamHealth.HEALTHY, local_sequence_gap=state is StreamHealth.DISCONNECTED or self._snapshot.local_sequence_gap)
            if state is StreamHealth.DISCONNECTED and self._position is not PaperDirection.FLAT:
                self._request_exit("LOCAL_OBSERVATION_BRIDGE_DISCONNECTED", emergency=True)

    def on_observation_rejection(self, error: NinjaTraderObservationError) -> None:
        self.policy.on_rejection(error)
        with self._lock:
            self._snapshot = replace(self._snapshot, observed_at=_now(), local_sequence_gap=True, evidence_warmed=False)
            self.ledger.append("INCIDENT_OBSERVATION_REJECTION", {"code": error.code.value, "detail": error.detail or "unspecified"})
            if self._position is not PaperDirection.FLAT:
                self._request_exit("MALFORMED_OBSERVATION", emergency=True)

    def on_observation_duplicate(self) -> None:
        self.policy.on_duplicate()
        self.ledger.append("INCIDENT_DUPLICATE_OBSERVATION", {"effect": "NO_NEW_PAPER_EVIDENCE"})

    def record_sink_failure(self, sink: str, event: str, error_type: str) -> None:
        self.ledger.append("INCIDENT_OBSERVATION_SINK_FAILURE", {"sink": sink, "event": event, "error_type": error_type})

    def ingest(self, observation: NinjaTraderObservation) -> None:
        with self._lock:
            before_classified = self.policy.classified_trade_count() if observation.observation_type == "TRADE" else 0
            decision = self.policy.ingest_runtime(observation, current_position=self._position, pending_order=self._state in {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.EXIT_PENDING})
            after_classified, warmed, depth_recovering = self.policy.runtime_gate_state()
            # Paper freshness follows the declared local callback authority.
            # Provider timestamps remain source provenance but independent
            # market-data streams do not form one provider-ordered clock.
            event_at = observation.ninja_receipt_time
            update: dict[str, object] = {"observed_at": normalized_utc(observation.ninja_receipt_time, "Runtime observation time")}
            if observation.observation_type == "QUOTE" and self._valid_quote(observation):
                bid, ask = Decimal(str(observation.payload["bid"])), Decimal(str(observation.payload["ask"]))
                self._last_quote = (bid, ask, event_at)
                update["quote_observed_at"] = event_at
                update["market_price_connected"] = True
            elif observation.observation_type == "TRADE":
                price = self._decimal(observation.payload.get("price"))
                if price is not None:
                    self._last_trade = (price, event_at)
                if after_classified > before_classified:
                    update["classified_trade_observed_at"] = event_at
            elif observation.observation_type == "DEPTH" and observation.payload.get("is_reset") is not True:
                self._last_depth_at = event_at
                update["depth_mutation_observed_at"] = event_at
            if decision is not None and decision.reason_code == "LOCAL_SEQUENCE_GAP":
                update["local_sequence_gap"] = True
            if decision is not None and decision.reason_code in {"DEPTH_RESET", "OBSERVATION_SESSION_CHANGED", "BRIDGE_RECONNECT", "MARKET_DATA_RECONNECTED"}:
                update["depth_reset_recovery"] = True
                update["evidence_warmed"] = False
            update["evidence_warmed"] = warmed
            update["depth_reset_recovery"] = depth_recovering
            if warmed:
                update["local_sequence_gap"] = False
            self._snapshot = replace(self._snapshot, **update)
            if decision is None:
                self._evaluate_risk_exit(observation.ninja_receipt_time)
                return
            self._last_decision = decision
            for evidence in self.policy.active_evidence(event_at):
                if evidence.evidence_id not in self._recorded_evidence:
                    self.ledger.append_deferred("EVIDENCE", evidence.payload(), identity=evidence.evidence_id, occurred_at=evidence.observed_at, execution_session_id=self._execution_session_id())
                    self._recorded_evidence.add(evidence.evidence_id)
            can_cause_side_effect = (
                decision.decision in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}
                and self._state is PaperRuntimeState.ARMED_FLAT
                and not self._entries_paused
            ) or (
                decision.decision is PaperDecisionKind.EXIT
                and self._position is not PaperDirection.FLAT
            )
            if not can_cause_side_effect:
                self.ledger.append_deferred("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
            else:
                # append() first flushes every prior evidence/NO_TRADE batch;
                # a decision eligible to mutate paper state is then committed
                # synchronously before any intent, grant, command, or socket
                # side effect. Directional decisions observed while disarmed
                # have no such authority and remain safely batchable.
                self.ledger.append("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
            self._evaluate_risk_exit(observation.ninja_receipt_time)
            if decision.decision is PaperDecisionKind.NO_TRADE:
                return
            if decision.decision is PaperDecisionKind.EXIT:
                if self._position is not PaperDirection.FLAT:
                    self._request_exit(decision.reason_code)
                return
            if self._state is PaperRuntimeState.ARMED_FLAT and not self._entries_paused:
                self._request_entry(decision)

    @staticmethod
    def _decimal(value: object) -> Decimal | None:
        try:
            result = Decimal(str(value))
        except Exception:
            return None
        return result if result.is_finite() else None

    @staticmethod
    def _valid_quote(observation: NinjaTraderObservation) -> bool:
        try:
            bid, ask = Decimal(str(observation.payload["bid"])), Decimal(str(observation.payload["ask"]))
            return bid > 0 and ask > bid and int(observation.payload["bid_size"]) > 0 and int(observation.payload["ask_size"]) > 0
        except Exception:
            return False

    def _evaluate_risk_exit(self, at: str) -> None:
        if self._position is PaperDirection.FLAT or self._state is PaperRuntimeState.EXIT_PENDING:
            return
        pnl = self._snapshot.daily_realized_pnl + self._snapshot.daily_unrealized_pnl
        if self._snapshot.foreign_activity:
            self._request_exit("FOREIGN_ACTIVITY", emergency=True)
        elif pnl <= -RISK_PROFILE.daily_loss_limit_dollars:
            self.risk.lock_out("DAILY_LOSS_LIMIT")
            self._request_exit("DAILY_LOSS_LIMIT", emergency=True)
        elif self.risk.hard_flat_due(at):
            self._request_exit("HARD_FLAT_DEADLINE")
        elif self.risk.maximum_age_due(self._snapshot, at):
            self._request_exit("MAXIMUM_POSITION_AGE")
        else:
            now = datetime.fromisoformat(normalized_utc(at, "Risk exit time").replace("Z", "+00:00"))
            stale = (
                (self._snapshot.quote_observed_at, RISK_PROFILE.quote_maximum_age_seconds, "QUOTE_STALE"),
                (self._snapshot.classified_trade_observed_at, RISK_PROFILE.classified_trade_maximum_age_seconds, "CLASSIFIED_TRADE_STALE"),
                (self._snapshot.depth_mutation_observed_at, RISK_PROFILE.depth_mutation_maximum_age_seconds, "DEPTH_STALE"),
            )
            for source, seconds, reason in stale:
                if source is None or now - datetime.fromisoformat(source.replace("Z", "+00:00")) > timedelta(seconds=seconds):
                    self._request_exit(reason, emergency=True)
                    break

    def _references(self) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        bid = None if self._last_quote is None else self._last_quote[0]
        ask = None if self._last_quote is None else self._last_quote[1]
        last = None if self._last_trade is None else self._last_trade[0]
        return bid, ask, last

    def _request_entry(self, decision: PaperDecision) -> None:
        bid, ask, last = self._references()
        intent = self.risk.make_intent(decision, reference_bid=bid, reference_ask=ask, reference_last=last)
        self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
        grant = self.risk.evaluate(intent, self._snapshot, at=_now())
        self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
        if not grant.granted:
            return
        action = ExecutionAction.ENTER_LONG if decision.decision is PaperDecisionKind.LONG else ExecutionAction.ENTER_SHORT
        command = self._make_command(intent.intent_id, decision.paper_decision_id, grant.grant_id, action, intent.target_position, "AUTONOMOUS_PAPER_ENTRY")
        self._persist_and_send(command, grant)
        self._pending_intent = intent
        self._pending_grant = grant
        self.policy.mark_entry_used(decision)
        self._transition(PaperRuntimeState.ENTRY_PENDING, "ENTRY_COMMAND_SENT")

    def _request_exit(self, reason: str, *, emergency: bool = False) -> None:
        if self._position is PaperDirection.FLAT or self._state is PaperRuntimeState.EXIT_PENDING:
            return
        if self._last_decision is None:
            decision_id = "l3g-pd-safety-" + canonical_hash({"reason": reason, "at": _now()})[:24]
            created_at = _now()
        else:
            decision_id = self._last_decision.paper_decision_id
            created_at = self._last_decision.created_at
        # Safety exits use a directional decision provenance but remain an
        # independently risk-evaluated flat intent.
        pseudo = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": reason, "decision": decision_id, "at": created_at})[:32],
            POLICY.policy_id, POLICY.configuration_hash, PaperDecisionKind.EXIT, created_at,
            (datetime.fromisoformat(normalized_utc(created_at, "Exit decision time").replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"risk_exit": reason}, (decision_id,), (max(0, self.policy.status().get("last_local_sequence") or 0),), (canonical_hash({"reason": reason}),),
            POLICY.sequence_authority, POLICY.book_completeness, False, reason,
        )
        self.ledger.append("DECISION", pseudo.payload(), identity=pseudo.paper_decision_id, occurred_at=pseudo.created_at, execution_session_id=self._execution_session_id())
        bid, ask, last = self._references()
        intent = self.risk.make_intent(pseudo, reference_bid=bid, reference_ask=ask, reference_last=last)
        self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
        grant = self.risk.evaluate(intent, self._snapshot, at=_now())
        self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
        if not grant.granted:
            self._fault_reason = "EXIT_RISK_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes)
            if self._state not in {PaperRuntimeState.FAULTED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.FAULTED, self._fault_reason)
            return
        action = ExecutionAction.EMERGENCY_FLATTEN if emergency else ExecutionAction.EXIT
        command = self._make_command(intent.intent_id, pseudo.paper_decision_id, grant.grant_id, action, PaperDirection.FLAT, reason)
        self._persist_and_send(command, grant)
        self._transition(PaperRuntimeState.EXIT_PENDING, reason)

    def _make_command(self, intent_id: str, decision_id: str, grant_id: str, action: ExecutionAction, expected: PaperDirection, reason: str) -> PaperExecutionCommand:
        session = self._execution_session_id()
        if session is None:
            raise RuntimeError("No authenticated execution session is available.")
        self._command_sequence += 1
        created = _now()
        quantity = 0 if action in {ExecutionAction.HEARTBEAT, ExecutionAction.RECONCILE, ExecutionAction.CANCEL_OWNED_ORDERS} else 1
        payload = {
            "command_sequence": self._command_sequence,
            "session_id": session,
            "intent_id": intent_id,
            "decision_id": decision_id,
            "action": action.value,
            "account_name": ACCOUNT_BINDING.account_name,
            "account_class": ACCOUNT_BINDING.account_class,
            "instrument": ACCOUNT_BINDING.instrument,
            "quantity": quantity,
            "expected_position": expected.value,
            "created_at": created,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=POLICY.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
            "policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "reason_code": reason,
            "risk_grant_id": grant_id,
        }
        return PaperExecutionCommand(deterministic_id("l3g-pc-", payload), **payload)

    def _persist_and_send(self, command: PaperExecutionCommand, grant: object) -> None:
        self.ledger.append("COMMAND", command.payload(), identity=command.command_id, occurred_at=command.created_at, execution_session_id=command.session_id)
        adapter = self._adapter
        if adapter is None:
            raise RuntimeError("Sim101 paper adapter is unavailable.")
        try:
            adapter.submit(command, grant)  # type: ignore[arg-type]
        except Exception:
            self._fault_reason = "DURABLE_COMMAND_SEND_FAILED"
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            raise
        self._last_command = command

    def on_execution_message(self, message: Mapping[str, object]) -> None:
        with self._lock:
            message_type = str(message.get("message_type", ""))
            if message_type == "RECONCILIATION":
                self._apply_reconciliation(message)
            elif message_type in {"ORDER_EVENT", "COMMAND_ACK", "COMMAND_REJECTED"}:
                self._last_order_state = dict(message)
                if message_type == "COMMAND_REJECTED":
                    self._fault_reason = "EXECUTION_COMMAND_REJECTED:" + str(message.get("reason_code", "UNKNOWN"))
                    self.risk.lock_out(self._fault_reason)
                    if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                if message.get("order_role") == "PROTECTIVE" and str(message.get("order_state", "")).upper() in {"REJECTED", "CANCELLED", "CANCELED"} and self._position is not PaperDirection.FLAT:
                    self.risk.lock_out("PROTECTIVE_STOP_REJECTED")
                    self._request_exit("PROTECTIVE_STOP_REJECTED", emergency=True)
                role = str(message.get("order_role", "")).upper()
                order_state = str(message.get("order_state", "")).upper()
                if role == "PROTECTIVE":
                    self._snapshot = replace(self._snapshot, observed_at=_now(), protective_stop_state=order_state or self._snapshot.protective_stop_state)
                if role == "ENTRY" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._state is PaperRuntimeState.ENTRY_PENDING:
                    self._fault_reason = "MARKET_ENTRY_ORDER_" + order_state
                    self.risk.lock_out(self._fault_reason)
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                if role == "EXIT" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._state is PaperRuntimeState.EXIT_PENDING:
                    self._fault_reason = "EXIT_ORDER_" + order_state
                    self.risk.lock_out(self._fault_reason)
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            elif message_type == "EXECUTION_EVENT":
                self._last_execution = dict(message)
                self._apply_execution(message)
            elif message_type == "POSITION_EVENT":
                self._apply_position(message)
            elif message_type == "SAFETY_EVENT":
                self._fault_reason = "NINJATRADER_SAFETY_EVENT:" + str(message.get("reason_code", "UNKNOWN"))
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)

    def _apply_reconciliation(self, message: Mapping[str, object]) -> None:
        quantity = message.get("position_quantity")
        orders = message.get("working_order_count")
        if type(quantity) is not int or type(orders) is not int:
            self._fault_reason = "RECONCILIATION_MALFORMED"
            self.risk.lock_out(self._fault_reason)
            if self._state is PaperRuntimeState.RECONCILING:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        direction = PaperDirection.FLAT if quantity == 0 else PaperDirection.LONG if quantity > 0 else PaperDirection.SHORT
        foreign = bool(message.get("foreign_activity", False)) or abs(quantity) > 1
        self._position = direction
        self._position_quantity = abs(quantity)
        self._last_reconciliation = dict(message)
        self._snapshot = replace(
            self._snapshot, observed_at=_now(), account_name=str(message.get("account_name", "")),
            account_class=str(message.get("account_class", "")), instrument=str(message.get("instrument", "")),
            current_position=direction, current_position_quantity=abs(quantity), working_owned_orders=orders,
            working_entry_orders=int(message.get("working_entry_count", 0)), foreign_activity=foreign,
            position_snapshot_complete=message.get("position_snapshot_complete") is True,
            order_snapshot_complete=message.get("order_snapshot_complete") is True,
            reconciliation_current=True, execution_bridge_healthy=True,
            protective_stop_state=str(message.get("protective_stop_state", "NONE")),
        )
        self.ledger.append("POSITION_SNAPSHOT_RECONCILIATION", dict(message), identity=str(message.get("receipt_id", "l3g-reconcile-" + canonical_hash(dict(message)))), execution_session_id=self._execution_session_id())
        if foreign or (quantity != 0 or orders != 0):
            self._fault_reason = "RECONCILIATION_BLOCKED"
            self.risk.lock_out(self._fault_reason)
            if self._state is PaperRuntimeState.RECONCILING:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
        elif self._state is PaperRuntimeState.RECONCILING:
            self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_RECONCILIATION_COMPLETE")

    def _apply_execution(self, message: Mapping[str, object]) -> None:
        role = str(message.get("order_role", ""))
        price = self._decimal(message.get("price"))
        quantity = message.get("quantity")
        if price is None or type(quantity) is not int or quantity <= 0:
            self._fault_reason = "MALFORMED_EXECUTION_EVENT"
            self.risk.lock_out(self._fault_reason)
            return
        if role == "ENTRY":
            expected = PaperDirection.LONG if str(message.get("direction")) == "LONG" else PaperDirection.SHORT
            intent = self._pending_intent
            if intent is None:
                self._fault_reason = "FILL_WITHOUT_EXPECTED_ORDER"
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                return
            fill_ok, reason = self.risk.enforce_fill(expected, intent, price)  # type: ignore[arg-type]
            self._position = expected
            self._position_quantity = quantity
            self._entry_fill_price = price
            self._entry_fill_quantity = quantity
            self._entry_direction = expected
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), current_position=expected,
                current_position_quantity=quantity,
                position_opened_at=str(message.get("timestamp", _now())),
                protective_stop_state="PENDING",
                session_entry_count=self._snapshot.session_entry_count + 1,
            )
            if self._state is PaperRuntimeState.ENTRY_PENDING:
                self._transition(PaperRuntimeState.LONG if expected is PaperDirection.LONG else PaperRuntimeState.SHORT, "ENTRY_FILL_CONFIRMED")
            if not fill_ok:
                self._request_exit(reason, emergency=True)
        elif role in {"EXIT", "PROTECTIVE"}:
            # Final flat truth still requires a position event/reconciliation.
            realized = Decimal("0")
            if self._entry_fill_price is not None and self._entry_fill_quantity > 0:
                points = price - self._entry_fill_price if self._entry_direction is PaperDirection.LONG else self._entry_fill_price - price
                realized = points * Decimal("2") * self._entry_fill_quantity
            daily = self._snapshot.daily_realized_pnl + realized
            losses = self._snapshot.consecutive_losses + 1 if realized < 0 else 0 if realized > 0 else self._snapshot.consecutive_losses
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), daily_realized_pnl=daily,
                consecutive_losses=losses,
            )

    def _apply_position(self, message: Mapping[str, object]) -> None:
        quantity = message.get("quantity")
        if type(quantity) is not int or abs(quantity) > 1:
            self._fault_reason = "POSITION_UPDATE_MISMATCH"
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        self._position = PaperDirection.FLAT if quantity == 0 else PaperDirection.LONG if quantity > 0 else PaperDirection.SHORT
        self._position_quantity = abs(quantity)
        self._snapshot = replace(self._snapshot, observed_at=_now(), current_position=self._position, current_position_quantity=abs(quantity), position_opened_at=None if quantity == 0 else self._snapshot.position_opened_at)
        if quantity == 0 and self._state is PaperRuntimeState.EXIT_PENDING:
            self.policy.confirm_flat(str(message.get("timestamp", _now())))
            self._pending_intent = None
            self._pending_grant = None
            self._entry_fill_price = None
            self._entry_fill_quantity = 0
            self._entry_direction = PaperDirection.FLAT
            target = PaperRuntimeState.READY_DISARMED if self._disarm_after_flat else PaperRuntimeState.PAUSED if self._entries_paused else PaperRuntimeState.ARMED_FLAT
            self._transition(target, "FLAT_POSITION_CONFIRMED")
            self._disarm_after_flat = False

    def arm(self) -> dict[str, object]:
        with self._lock:
            if self._state is not PaperRuntimeState.READY_DISARMED:
                return {"armed": False, "reason_codes": ("STATE_NOT_READY_DISARMED",), "state": self._state.value}
            allowed, reasons = self.risk.preflight(self._snapshot, at=_now())
            self.ledger.append("RISK_EVENT_ARM_ATTEMPT", {"allowed": allowed, "reason_codes": reasons, "authority_hash": canonical_hash(AUTHORITY.authority_payload())})
            if not allowed:
                return {"armed": False, "reason_codes": reasons, "state": self._state.value}
            self._entries_paused = False
            self._transition(PaperRuntimeState.ARMED_FLAT, "OPERATOR_ARM_AFTER_PREFLIGHT")
            return {"armed": True, "reason_codes": ("PAPER_ARMED",), "state": self._state.value}

    def pause_entries(self) -> dict[str, object]:
        with self._lock:
            if self._state in {PaperRuntimeState.READY_DISARMED, PaperRuntimeState.DISABLED, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                return {"paused": False, "state": self._state.value, "reason": "PAPER_NOT_ARMED"}
            self._entries_paused = True
            if self._state not in {PaperRuntimeState.PAUSED, PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED}:
                self._transition(PaperRuntimeState.PAUSED, "OPERATOR_PAUSE_ENTRIES")
            return {"paused": True, "state": self._state.value, "exits_and_stops": "ENABLED"}

    def resume_entries(self) -> dict[str, object]:
        with self._lock:
            if not self._entries_paused or self._state is not PaperRuntimeState.PAUSED:
                return {"resumed": False, "state": self._state.value, "reason": "NOT_PAUSED"}
            self._entries_paused = False
            target = PaperRuntimeState.ARMED_FLAT if self._position is PaperDirection.FLAT else PaperRuntimeState.LONG if self._position is PaperDirection.LONG else PaperRuntimeState.SHORT
            self._transition(target, "OPERATOR_RESUME_ENTRIES")
            return {"resumed": True, "state": self._state.value}

    def flatten_and_disarm(self) -> dict[str, object]:
        with self._lock:
            self._entries_paused = True
            self._disarm_after_flat = True
            self.ledger.append("RISK_EVENT_FLATTEN_AND_DISARM", {"position": self._position.value, "state": self._state.value})
            if self._position is PaperDirection.FLAT and (self._state is PaperRuntimeState.ENTRY_PENDING or self._snapshot.working_owned_orders > 0):
                self._cancel_pending_and_reconcile()
                return {"initiated": True, "flat_confirmed": False, "state": self._state.value}
            if self._position is PaperDirection.FLAT:
                if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED}:
                    self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_CONFIRMED_DISARM")
                self._disarm_after_flat = False
                return {"initiated": True, "flat_confirmed": True, "state": self._state.value}
            self._request_exit("OPERATOR_FLATTEN_AND_DISARM", emergency=True)
            return {"initiated": True, "flat_confirmed": False, "state": self._state.value}

    def _cancel_pending_and_reconcile(self) -> None:
        created = _now()
        decision = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": "CANCEL_PENDING_AND_DISARM", "at": created})[:32],
            POLICY.policy_id, POLICY.configuration_hash, PaperDecisionKind.EXIT, created,
            (datetime.fromisoformat(created.replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"safety": "CANCEL_PENDING_AND_DISARM"},
            ("pending-order-safety-control",), (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash({"reason": "CANCEL_PENDING_AND_DISARM"}),), POLICY.sequence_authority,
            POLICY.book_completeness, False, "CANCEL_PENDING_AND_DISARM",
        )
        self.ledger.append("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
        bid, ask, last = self._references()
        intent = self.risk.make_intent(decision, reference_bid=bid, reference_ask=ask, reference_last=last)
        self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
        grant = self.risk.evaluate(intent, self._snapshot, at=created)
        self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
        if not grant.granted:
            self._fault_reason = "CANCEL_RISK_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes)
            self.risk.lock_out(self._fault_reason)
            self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        cancel = self._make_command(intent.intent_id, decision.paper_decision_id, grant.grant_id, ExecutionAction.CANCEL_OWNED_ORDERS, PaperDirection.FLAT, "OPERATOR_FLATTEN_AND_DISARM")
        self._persist_and_send(cancel, grant)
        self._transition(PaperRuntimeState.RECONCILING, "PENDING_ENTRY_CANCEL_SENT")
        reconcile = self._make_command(intent.intent_id, decision.paper_decision_id, grant.grant_id, ExecutionAction.RECONCILE, PaperDirection.FLAT, "POST_CANCEL_RECONCILIATION")
        self._persist_and_send(reconcile, grant)

    def stop(self) -> None:
        with self._lock:
            if self._state in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                return
            if self._state is PaperRuntimeState.DISABLED:
                self._state = PaperRuntimeState.STOPPED
                return
            if self._position is not PaperDirection.FLAT:
                self.ledger.append("INCIDENT_SHUTDOWN_WITH_POSITION", {"position": self._position.value, "watchdog": "INDEPENDENT_CSHARP_FLATTEN_REQUIRED"})
            self._transition(PaperRuntimeState.STOPPING, "PROCESS_STOP")
            self._heartbeat_stop.set()
            thread = self._heartbeat_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._lock:
            self._transition(PaperRuntimeState.STOPPED, "PROCESS_STOPPED")

    def status(self) -> dict[str, object]:
        with self._lock:
            policy = self.policy.status()
            risk = self.risk.status()
            transport = None if self._transport is None else self._transport.status().as_dict()
            return {
                "schema": "lane-iii-phase-g-paper-runtime-status-v1",
                "mode": "PAPER_SIM101",
                "display_mode": "EXPERIMENTAL PAPER",
                "state": self._state.value,
                "paper_execution": "POSITIONED" if self._position is not PaperDirection.FLAT else "ARMED" if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED} else "LOCKED" if self._state in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED} else "DISARMED",
                "scientific_lane_iii": "INCOMPLETE / BLOCKED ON SEQUENCING",
                "scientific_eligibility": False,
                "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
                "book_completeness": "UNVERIFIED",
                "market_connection": "LucidFlex",
                "market_instrument": "MNQ SEP26",
                "paper_account": "Sim101",
                "account_class": "LOCAL_SIMULATION",
                "maximum_quantity": 1,
                "live_capital": "DENIED",
                "warning": "EXPERIMENTAL PAPER EXECUTION / NOT SCIENTIFICALLY COMMISSIONED / SIM101 ONLY / LIVE CAPITAL DENIED",
                "current_position": self._position.value,
                "current_quantity": self._position_quantity,
                "working_owned_orders": self._snapshot.working_owned_orders,
                "protective_stop_state": self._snapshot.protective_stop_state,
                "daily_realized_pnl": str(self._snapshot.daily_realized_pnl),
                "daily_unrealized_pnl": str(self._snapshot.daily_unrealized_pnl),
                "session_entries": self._snapshot.session_entry_count,
                "consecutive_losses": self._snapshot.consecutive_losses,
                "last_paper_decision": None if self._last_decision is None else self._last_decision.payload(),
                "last_risk_result": risk.get("last_risk_result"),
                "last_command": None if self._last_command is None else self._last_command.payload(),
                "last_order_state": self._last_order_state,
                "last_execution": self._last_execution,
                "last_reconciliation": self._last_reconciliation,
                "lockout_or_fault_reason": self._fault_reason or risk.get("lockout_reason"),
                "authority": AUTHORITY.authority_payload(),
                "policy": policy,
                "risk": risk,
                "transport": transport,
                "ledger": {"counts": self.ledger.counts(), "chain_valid": self.ledger.chain_status()[0]},
            }
