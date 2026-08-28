"""Lane III-G paper runtime, state machine, and observation fan-out."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading
import time
from typing import Callable, Mapping
from uuid import uuid4

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation, NinjaTraderObservationError
from src.l3f_provider.tradovate_observation import StreamHealth
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    ACCOUNT_BINDING,
    AUTHORITY,
    POLICY,
    RISK_PROFILE,
    EvidenceFamily,
    ExecutionAction,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEntryOwner,
    PaperExecutionCommand,
    PaperRuntimeState,
    PaperSessionArmGrant,
    deterministic_id,
)
from .ledger import (
    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS,
    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS,
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    COMMISSIONING_READINESS_RECORD_SEMANTICS,
    COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
    PaperLedger,
)
from .ninjatrader_transport import ExecutionTransportStatus, NinjaTraderSim101PaperAdapter, PaperExecutionTransport
from .policy import ExperimentalPaperPolicy
from .risk import PaperRiskAuthority, PaperRiskSnapshot
from .sessions import (
    PaperSessionContext,
    PaperSessionFamily,
    PaperSessionKind,
    PaperSessionResolver,
    UNSPECIFIED_OFF_SESSION_CONTEXT,
    context_from_identity,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class _TradeDateRisk:
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    entry_count: int = 0
    consecutive_losses: int = 0


@dataclass(frozen=True)
class _CommissioningOwnership:
    """The immutable commissioning credential and its lifecycle-local state."""

    commissioning_id: str
    commissioning_token: str
    context: PaperSessionContext
    reserved_at: str
    entry_consumed: bool = False
    entry_decision_id: str | None = None
    recovered_after_restart: bool = False
    request_id: str | None = None
    ledger_preflight: Mapping[str, object] | None = None


@dataclass(frozen=True)
class _CommissioningReadinessCapture:
    """Immutable runtime facts released to the blocking ledger validator."""

    generation: int
    guard_token: str
    context: PaperSessionContext
    risk_snapshot: PaperRiskSnapshot
    state: PaperRuntimeState
    position: PaperDirection
    position_quantity: int
    entry_owner: PaperEntryOwner
    commissioning_ownership: _CommissioningOwnership | None
    transport_status: ExecutionTransportStatus | None
    commissioning_warmup_seen: Mapping[str, Mapping[str, object]]
    commissioning_warmup_warmed_at: str | None


class _CommissioningAuthorizationExpired(RuntimeError):
    """A sealed commissioning authorization expired before transport admission."""


_COMMISSIONING_REQUIRED_FAMILIES = (
    EvidenceFamily.STRUCTURAL_CONTEXT,
    EvidenceFamily.ORDER_FLOW,
    EvidenceFamily.RESTING_LIQUIDITY,
)
_COMMISSIONING_WARMUP_POLICY = {
    "version": "l3g-commissioning-session-warmup-v1",
    "required_families": tuple(family.value for family in _COMMISSIONING_REQUIRED_FAMILIES),
    "binding": (
        "session_kind", "session_id", "trade_date", "session_profile_hash", "session_generation",
    ),
    "natural_evidence_expiration_clears_latch": False,
    "runtime_restart_clears_latch": True,
}
_COMMISSIONING_WARMUP_POLICY_HASH = canonical_hash(_COMMISSIONING_WARMUP_POLICY)
_COMMISSIONING_WARMUP_RECORD_MARKERS = {
    "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
    "record_semantics": COMMISSIONING_READINESS_RECORD_SEMANTICS,
    "record_semantics_version": COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
}


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
        self._commissioning_ownership = self._load_unresolved_commissioning_ownership()
        self._entry_owner = PaperEntryOwner.COMMISSIONING if self._commissioning_ownership is not None else PaperEntryOwner.NONE
        if self._commissioning_ownership is not None:
            # A process restart must not accidentally restore normal strategy
            # admission before the signed execution bridge has reconciled it.
            self._entries_paused = True
        self._disarm_after_flat = False
        self._transport: PaperExecutionTransport | None = None
        self._adapter: NinjaTraderSim101PaperAdapter | None = None
        self._snapshot = PaperRiskSnapshot(_now())
        self._session_resolver = PaperSessionResolver()
        self._session_context = UNSPECIFIED_OFF_SESSION_CONTEXT
        self._session_generation = 0
        self._armed_session: PaperSessionArmGrant | None = None
        self._session_closed_ids: set[tuple[str, int]] = set()
        # Risk accounting is family-local: NEW_YORK_RTH and NY_AFTER share
        # one cumulative envelope, while ASIA is independently accounted.
        # Evidence and arm grants remain scoped to the exact session below.
        self._family_risk: dict[tuple[str, PaperSessionFamily], _TradeDateRisk] = {}
        self._session_pnl: dict[str, Decimal] = {}
        self._entry_session_context: PaperSessionContext | None = None
        self._hard_flat_started_for: tuple[str, int] | None = None
        self._last_decision: PaperDecision | None = None
        self._last_command: PaperExecutionCommand | None = None
        self._last_order_state: Mapping[str, object] | None = None
        self._last_execution: Mapping[str, object] | None = None
        self._last_reconciliation: Mapping[str, object] | None = None
        self._last_quote: tuple[Decimal, Decimal, str] | None = None
        self._last_trade: tuple[Decimal, str] | None = None
        self._last_depth_at: str | None = None
        self._recorded_evidence: set[str] = set()
        # Commissioning warmup is deliberately process-local and starts cold.
        # It is never recovered from the ledger after restart.
        self._commissioning_warmup_seen: dict[str, dict[str, object]] = {}
        self._commissioning_warmup_warmed_at: str | None = None
        self._commissioning_warmup_context = UNSPECIFIED_OFF_SESSION_CONTEXT
        self._last_policy_reset_count = self.policy.reset_count()
        self._last_commissioning_closure: dict[str, object] | None = None
        self._runtime_identity: dict[str, object] = {
            "git_sha": "UNKNOWN", "ledger": str(self.ledger.path), "audit": "UNKNOWN",
        }
        self._pending_intent: object | None = None
        self._pending_grant: object | None = None
        self._entry_fill_price: Decimal | None = None
        self._entry_fill_quantity = 0
        self._entry_direction = PaperDirection.FLAT
        self._entry_execution: dict[str, object] | None = None
        self._entry_authority_artifact: dict[str, object] | None = None
        self._exit_execution: dict[str, object] | None = None
        self._seen_native_execution_ids: set[str] = set()
        self._protective_order_id: str | None = None
        self._lifecycle_realized_pnl = Decimal("0")
        self._post_exit_reconciliation_pending = False
        self._command_sequence = 0
        # A venue callback can arrive synchronously while a durable exit is
        # being sent.  It must not create a second exit before EXIT_PENDING is
        # recorded.
        self._exit_submission_in_progress = False
        self._fault_reason: str | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._transitions = 0
        self._commissioning_readiness_generation = 0
        self._commissioning_authority_epoch = 0
        self._commissioning_stale_snapshot_refusals = 0
        self._last_commissioning_preflight_duration_seconds: float | None = None
        self._last_commissioning_snapshot_token: str | None = None

    @staticmethod
    def _ownership_context_matches(left: PaperSessionContext, right: PaperSessionContext) -> bool:
        return (
            left.session_kind, left.session_id, left.trade_date, left.session_profile_hash, left.session_generation,
        ) == (
            right.session_kind, right.session_id, right.trade_date, right.session_profile_hash, right.session_generation,
        )

    def _advance_commissioning_authority_epoch(self) -> None:
        """Record an authority-relevant mutation; market timestamp advances are excluded."""
        self._commissioning_authority_epoch += 1

    def _ownership_payload(self, ownership: _CommissioningOwnership, *, reason: str) -> dict[str, object]:
        return {
            **ownership.context.payload(),
            "commissioning_id": ownership.commissioning_id,
            "entry_owner": PaperEntryOwner.COMMISSIONING.value,
            "entry_consumed": ownership.entry_consumed,
            "entry_decision_id": ownership.entry_decision_id,
            "request_id": ownership.request_id,
            "account": ACCOUNT_BINDING.account_name,
            "account_class": ACCOUNT_BINDING.account_class,
            "instrument": ACCOUNT_BINDING.instrument,
            "occurred_at": _now(),
            "reason": reason,
        }

    @staticmethod
    def _commissioning_transport_guard(status: ExecutionTransportStatus | None) -> dict[str, object] | None:
        if status is None:
            return None
        return {
            key: getattr(status, key)
            for key in (
                "state", "listener_ready", "authenticated_client", "client_count",
                "execution_session_id", "reconciled", "error", "addon_protocol_version",
                "addon_source_fingerprint", "addon_build_fingerprint",
                "expected_addon_source_fingerprint", "addon_provenance_valid",
            )
        }

    def _commissioning_guard_payload_locked(
        self, transport_status: ExecutionTransportStatus | None,
    ) -> dict[str, object]:
        ignored_snapshot_fields = {
            "observed_at", "quote_observed_at", "classified_trade_observed_at", "depth_mutation_observed_at",
        }
        ownership = self._commissioning_ownership
        risk_status = self.risk.status()
        return {
            "authority_epoch": self._commissioning_authority_epoch,
            "state_transitions": self._transitions,
            "policy_reset_count": self._last_policy_reset_count,
            "state": self._state.value,
            "position": self._position.value,
            "position_quantity": self._position_quantity,
            "entries_paused": self._entries_paused,
            "disarm_after_flat": self._disarm_after_flat,
            "entry_owner": self._entry_owner.value,
            "session": self._session_context.payload(),
            "risk_snapshot": {
                key: str(value)
                for key, value in self._snapshot.__dict__.items()
                if key not in ignored_snapshot_fields
            },
            "risk_lockout": {
                "locked_out": risk_status.get("locked_out"),
                "lockout_reason": risk_status.get("lockout_reason"),
            },
            "armed_session": None if self._armed_session is None else self._armed_session.payload(),
            "commissioning_ownership": None if ownership is None else {
                "commissioning_id": ownership.commissioning_id,
                "context": ownership.context.payload(),
                "entry_consumed": ownership.entry_consumed,
                "entry_decision_id": ownership.entry_decision_id,
                "recovered_after_restart": ownership.recovered_after_restart,
                "request_id": ownership.request_id,
            },
            "commissioning_warmup": {
                "context": self._commissioning_warmup_context.payload(),
                "seen": {key: dict(value) for key, value in self._commissioning_warmup_seen.items()},
                "warmed_at": self._commissioning_warmup_warmed_at,
            },
            "transport": self._commissioning_transport_guard(transport_status),
        }

    def _capture_commissioning_readiness_locked(self) -> _CommissioningReadinessCapture:
        transport_status = None if self._transport is None else self._transport.status()
        guard_token = canonical_hash(self._commissioning_guard_payload_locked(transport_status))
        self._commissioning_readiness_generation += 1
        self._last_commissioning_snapshot_token = guard_token
        return _CommissioningReadinessCapture(
            generation=self._commissioning_readiness_generation,
            guard_token=guard_token,
            context=self._session_context,
            risk_snapshot=self._snapshot,
            state=self._state,
            position=self._position,
            position_quantity=self._position_quantity,
            entry_owner=self._entry_owner,
            commissioning_ownership=self._commissioning_ownership,
            transport_status=transport_status,
            commissioning_warmup_seen={
                key: dict(value) for key, value in self._commissioning_warmup_seen.items()
            },
            commissioning_warmup_warmed_at=self._commissioning_warmup_warmed_at,
        )

    @staticmethod
    def _commissioning_runtime_snapshot(
        commissioning_id: str, capture: _CommissioningReadinessCapture,
    ) -> dict[str, object]:
        snapshot = capture.risk_snapshot
        return {
            **capture.context.payload(),
            "commissioning_id": commissioning_id,
            "runtime_snapshot_generation": capture.generation,
            "runtime_snapshot_token": capture.guard_token,
            "account": snapshot.account_name,
            "account_class": snapshot.account_class,
            "instrument": snapshot.instrument,
            "current_position": capture.position.value,
            "current_position_quantity": capture.position_quantity,
            "broker_snapshot_position": snapshot.current_position.value,
            "broker_snapshot_position_quantity": snapshot.current_position_quantity,
            "working_owned_orders": snapshot.working_owned_orders,
            "working_entry_orders": snapshot.working_entry_orders,
            "position_snapshot_complete": snapshot.position_snapshot_complete,
            "order_snapshot_complete": snapshot.order_snapshot_complete,
            "reconciliation_current": snapshot.reconciliation_current,
            "unresolved_command": snapshot.unresolved_command,
            "unresolved_native_order": snapshot.unresolved_native_order,
            "unresolved_execution": snapshot.unresolved_execution,
            "entry_owner": capture.entry_owner.value,
            "commissioning_ownership_active": capture.commissioning_ownership is not None,
            "live_capital": "DENIED",
            "runtime_state": capture.state.value,
            "transport": None if capture.transport_status is None else capture.transport_status.as_dict(),
        }

    def _load_unresolved_commissioning_ownership(self) -> _CommissioningOwnership | None:
        """Rehydrate only an unresolved reservation; never restore strategy authority."""
        recovery = self.ledger.unresolved_commissioning_ownership()
        if recovery is None:
            return None
        record, consumed = recovery
        payload = record.get("payload")
        if not isinstance(payload, Mapping):
            payload = {}
        try:
            context = context_from_identity(
                PaperSessionKind(str(payload["session_kind"])), str(payload["session_id"]),
                str(payload["trade_date"]), str(payload["session_profile_hash"]), int(payload["session_generation"]),
            )
        except (KeyError, TypeError, ValueError):
            # A malformed durable record may not grant ordinary entry authority.
            # Keep a synthetic unresolved marker which forces reconciliation and
            # lockout rather than silently discarding the evidence.
            context = UNSPECIFIED_OFF_SESSION_CONTEXT
        return _CommissioningOwnership(
            str(payload["commissioning_id"]), "RECOVERY_TOKEN_UNAVAILABLE", context,
            str(payload.get("occurred_at", _now())), consumed, None, True,
            str(payload["request_id"]) if isinstance(payload.get("request_id"), str) else None,
            dict(payload["ledger_preflight"]) if isinstance(payload.get("ledger_preflight"), Mapping) else None,
        )

    @staticmethod
    def _context_identity(context: PaperSessionContext) -> tuple[object, ...]:
        return (
            context.session_kind, context.session_id, context.trade_date,
            context.session_profile_hash, context.session_generation,
        )

    def _reset_commissioning_warmup(self, reason: str) -> None:
        """Clear only the session-latched commissioning readiness domain."""
        seen = dict(self._commissioning_warmup_seen)
        warmed_at = self._commissioning_warmup_warmed_at
        prior = self._commissioning_warmup_context
        changed = bool(seen) or warmed_at is not None or self._snapshot.commissioning_session_warmed
        self._commissioning_warmup_seen.clear()
        self._commissioning_warmup_warmed_at = None
        self._commissioning_warmup_context = self._session_context
        self._snapshot = replace(
            self._snapshot, observed_at=_now(), commissioning_session_warmed=False,
        )
        if changed:
            self._advance_commissioning_authority_epoch()
            self.ledger.append(
                "COMMISSIONING_SESSION_WARMUP_RESET",
                {
                    **prior.payload(),
                    **_COMMISSIONING_WARMUP_RECORD_MARKERS,
                    "commissioning_warmup_state": "NOT_WARMED",
                    "reset_at": _now(),
                    "reason": reason,
                    "required_families": list(_COMMISSIONING_WARMUP_POLICY["required_families"]),
                    "seen_families": sorted(seen),
                    "warmed_at": warmed_at,
                    "policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
                },
                execution_session_id=self._execution_session_id(),
            )

    def _observe_commissioning_warmup(self, at: str) -> None:
        """Latch once every required evidence family has appeared in this exact context."""
        context = self._session_context
        if context.session_kind is PaperSessionKind.OFF_SESSION:
            return
        if self._context_identity(self._commissioning_warmup_context) != self._context_identity(context):
            self._reset_commissioning_warmup("SESSION_IDENTITY_CHANGED")
            self._commissioning_warmup_context = context
        if self._commissioning_warmup_warmed_at is not None:
            return
        prior_families = set(self._commissioning_warmup_seen)
        for evidence in self.policy.active_evidence(at):
            if (
                evidence.family not in _COMMISSIONING_REQUIRED_FAMILIES
                or evidence.session_kind is not context.session_kind
                or evidence.session_id != context.session_id
                or evidence.trade_date != context.trade_date
                or evidence.session_profile_hash != context.session_profile_hash
                or evidence.session_generation != context.session_generation
            ):
                continue
            self._commissioning_warmup_seen.setdefault(evidence.family.value, {
                "evidence_id": evidence.evidence_id,
                "observed_at": evidence.observed_at,
                "source_observation_ids": list(evidence.source_observation_ids),
                "source_local_sequences": list(evidence.source_local_sequences),
            })
        if set(self._commissioning_warmup_seen) != prior_families:
            self._advance_commissioning_authority_epoch()
        required = set(_COMMISSIONING_WARMUP_POLICY["required_families"])
        if set(self._commissioning_warmup_seen) != required:
            return
        warmed_at = normalized_utc(at, "Commissioning warmup time")
        self._commissioning_warmup_warmed_at = warmed_at
        self._snapshot = replace(
            self._snapshot, observed_at=_now(), commissioning_session_warmed=True,
        )
        self.ledger.append(
            "COMMISSIONING_SESSION_WARMED",
            {
                **context.payload(),
                **_COMMISSIONING_WARMUP_RECORD_MARKERS,
                "commissioning_warmup_state": "WARMED",
                "warmed_at": warmed_at,
                "required_families": list(_COMMISSIONING_WARMUP_POLICY["required_families"]),
                "evidence_provenance": dict(self._commissioning_warmup_seen),
                "reason": "ALL_REQUIRED_FAMILIES_GENUINELY_OBSERVED",
                "policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
            },
            execution_session_id=self._execution_session_id(),
        )

    def _release_commissioning_ownership(self, reason: str) -> None:
        ownership = self._commissioning_ownership
        if ownership is None:
            return
        self.ledger.append(
            "COMMISSIONING_OWNERSHIP_RELEASED", self._ownership_payload(ownership, reason=reason),
            identity="l3g-commissioning-ownership-release-" + ownership.commissioning_id,
            execution_session_id=self._execution_session_id(),
        )
        self._commissioning_ownership = None
        self._entry_owner = PaperEntryOwner.NONE
        self._advance_commissioning_authority_epoch()

    def _record_strategy_suppression(self, decision: PaperDecision, context: PaperSessionContext) -> None:
        ownership = self._commissioning_ownership
        if ownership is None:
            return
        self.ledger.append(
            "COMMISSIONING_STRATEGY_ENTRY_SUPPRESSED",
            {
                **self._ownership_payload(ownership, reason="COMMISSIONING_ENTRY_RESERVED"),
                "decision_id": decision.paper_decision_id,
                "decision": decision.decision.value,
                "decision_session_id": context.session_id,
            },
            identity="l3g-commissioning-strategy-suppressed-" + decision.paper_decision_id,
            execution_session_id=self._execution_session_id(),
        )

    def _settle_recovered_commissioning_ownership(self) -> bool:
        """Return true when reconciliation handled a recovered reservation."""
        ownership = self._commissioning_ownership
        if ownership is None or not ownership.recovered_after_restart:
            return False
        if ownership.entry_consumed:
            self.ledger.append(
                "COMMISSIONING_OWNERSHIP_RECOVERED",
                self._ownership_payload(ownership, reason="RECOVERY_ENTRY_SUBMISSION_AMBIGUOUS"),
                identity="l3g-commissioning-ownership-recovered-" + ownership.commissioning_id,
                execution_session_id=self._execution_session_id(),
            )
            self._fault_reason = "COMMISSIONING_OWNERSHIP_RECOVERY_AMBIGUOUS"
            self.risk.lock_out(self._fault_reason)
            self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return True
        self.ledger.append(
            "COMMISSIONING_OWNERSHIP_RECOVERED",
            self._ownership_payload(ownership, reason="RECOVERY_FLAT_UNCONSUMED_RESERVATION"),
            identity="l3g-commissioning-ownership-recovered-" + ownership.commissioning_id,
            execution_session_id=self._execution_session_id(),
        )
        self._release_commissioning_ownership("RECOVERY_FLAT_UNCONSUMED_RESERVATION")
        self._transition(PaperRuntimeState.READY_DISARMED, "COMMISSIONING_OWNERSHIP_RECOVERED_DISARMED")
        return True

    @staticmethod
    def _market_event_timestamp(observation: NinjaTraderObservation) -> str:
        # This runtime's declared sequence authority is the queue-locked
        # local callback order. Provider and exchange timestamps are recorded
        # as immutable provenance, but are not a cross-stream ordering clock.
        return observation.ninja_receipt_time

    @staticmethod
    def _context_payload(context: PaperSessionContext) -> dict[str, object]:
        return context.payload()

    def _set_session_context(self, context: PaperSessionContext, *, reason: str) -> None:
        prior = self._session_context
        if self._context_identity(prior) == self._context_identity(context):
            return
        self._advance_commissioning_authority_epoch()
        self._reset_commissioning_warmup("SESSION_CHANGED:" + reason)
        if prior.session_kind is not PaperSessionKind.OFF_SESSION:
            self._close_session(prior, reason)
        self._session_context = context
        self._commissioning_warmup_context = context
        self.ledger.set_session_context(context)
        if context.session_kind is not PaperSessionKind.OFF_SESSION:
            trade_risk = self._family_risk.setdefault((context.trade_date, context.session_family), _TradeDateRisk())
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
                session_entry_count=0, daily_realized_pnl=trade_risk.realized_pnl,
                daily_unrealized_pnl=trade_risk.unrealized_pnl,
                trade_date_entry_count=trade_risk.entry_count, consecutive_losses=trade_risk.consecutive_losses,
                evidence_warmed=False, commissioning_session_warmed=False, depth_reset_recovery=True,
            )
            self.ledger.append("SESSION_OPENED", {**context.payload(), "reason": reason}, identity="l3g-paper-session-open-" + canonical_hash(context.payload()))
        else:
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
                session_entry_count=0, evidence_warmed=False,
                commissioning_session_warmed=False, depth_reset_recovery=True,
            )

    def _close_session(self, context: PaperSessionContext, reason: str) -> None:
        marker = (context.session_id, context.session_generation)
        if marker in self._session_closed_ids:
            return
        self._session_closed_ids.add(marker)
        self._armed_session = None
        self._entries_paused = True
        self.ledger.append(
            "SESSION_CLOSED", {
                **context.payload(), "reason": reason,
                "session_realized_pnl": str(self._session_pnl.get(context.session_id, Decimal("0"))),
                "position": self._position.value, "working_owned_orders": self._snapshot.working_owned_orders,
            }, identity="l3g-paper-session-close-" + canonical_hash({**context.payload(), "reason": reason}),
        )
        self.policy.reset("SESSION_CLOSED")
        ownership = self._commissioning_ownership
        if ownership is not None and not ownership.entry_consumed and self._position is PaperDirection.FLAT and not self._snapshot.working_owned_orders:
            if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED}:
                self._transition(PaperRuntimeState.READY_DISARMED, "COMMISSIONING_RESERVATION_SESSION_CLOSED")
            self._release_commissioning_ownership("SESSION_CLOSED_BEFORE_COMMISSIONING_ENTRY")
            return
        if self._position is not PaperDirection.FLAT or self._snapshot.working_owned_orders:
            self._disarm_after_flat = True
            self._request_exit("SESSION_BOUNDARY_FLATTEN", emergency=True)

    def _resolve_observation_session(self, observation: NinjaTraderObservation) -> tuple[PaperSessionContext, str | None]:
        resolution = self._session_resolver.resolve(self._market_event_timestamp(observation), generation=self._session_generation)
        # A reconnect can drain an older local callback stream after the
        # active stream has already advanced.  It is not a market-session
        # transition and must never close, re-open, or donate evidence to the
        # current session.  Keep the current exact context; ingest() records
        # and refuses the stale callback before policy/risk admission.
        if resolution.reason_code == "EVENT_TIMESTAMP_MOVED_BACKWARD":
            return self._session_context, resolution.reason_code
        context = resolution.context
        if context.session_kind is not PaperSessionKind.OFF_SESSION and (
            context.session_id != self._session_context.session_id or context.session_kind != self._session_context.session_kind
        ):
            self._session_generation += 1
            # A repeat at the same instant is allowed; resolver monotonicity
            # protects only backward event time.
            context = self._session_resolver.resolve(
                self._market_event_timestamp(observation), generation=self._session_generation,
            ).context
        elif context.session_kind is PaperSessionKind.OFF_SESSION:
            context = self._session_resolver.resolve(
                self._market_event_timestamp(observation), generation=self._session_generation,
            ).context
        self._set_session_context(context, reason=resolution.reason_code or "MARKET_EVENT_SESSION")
        return context, resolution.reason_code

    def _enforce_session_boundary(self) -> None:
        context = self._session_context
        if context.session_kind is PaperSessionKind.OFF_SESSION:
            return
        moment = datetime.now(timezone.utc)
        if context.hard_flat_due_at(moment) and self._hard_flat_started_for != (context.session_id, context.session_generation):
            self._hard_flat_started_for = (context.session_id, context.session_generation)
            self._armed_session = None
            self._entries_paused = True
            self.ledger.append("RISK_EVENT_HARD_FLAT", {**context.payload(), "reason": "HARD_FLAT_DEADLINE"})
            self.flatten_and_disarm()

    def bind_transport(self, transport: PaperExecutionTransport) -> None:
        if type(transport) is not PaperExecutionTransport:
            raise ValueError("Paper runtime accepts only the signed Sim101 transport.")
        with self._lock:
            if self._transport is not None or self._state is not PaperRuntimeState.DISABLED:
                raise RuntimeError("Paper execution transport may be bound exactly once before startup.")
            self._transport = transport
            self._adapter = NinjaTraderSim101PaperAdapter(transport)

    def bind_runtime_identity(self, identity: Mapping[str, object]) -> None:
        """Bind non-authoritative deployment provenance before runtime start."""
        with self._lock:
            if self._state is not PaperRuntimeState.DISABLED:
                raise RuntimeError("Runtime identity must be bound before Lane III startup.")
            self._runtime_identity = {
                key: identity.get(key) for key in ("git_sha", "ledger", "audit", "control_center", "python", "pid")
            }

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
            PaperRuntimeState.RECONCILING: {PaperRuntimeState.READY_DISARMED, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
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
        self._advance_commissioning_authority_epoch()
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
                self._enforce_session_boundary()
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
            self._advance_commissioning_authority_epoch()
            healthy = state == "AUTHENTICATED"
            was_healthy = self._snapshot.execution_bridge_healthy
            if state == "DISCONNECTED":
                self._reset_commissioning_warmup("EXECUTION_BRIDGE_DISCONNECTED")
            elif healthy and not was_healthy:
                self._reset_commissioning_warmup("EXECUTION_BRIDGE_RECONNECTED")
            self._snapshot = replace(self._snapshot, observed_at=_now(), execution_bridge_healthy=healthy, reconciliation_current=False if state in {"CONNECTED", "DISCONNECTED", "AUTHENTICATED"} else self._snapshot.reconciliation_current)
            if state == "AUTHENTICATED":
                self._command_sequence = 0
                if self._state in {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED}:
                    self._entries_paused = self._commissioning_ownership is not None
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
        with self._lock:
            self._advance_commissioning_authority_epoch()
            reset_before = self.policy.reset_count()
            self.policy.on_transport_state(state)
            reset_after = self.policy.reset_count()
            if reset_after != reset_before:
                self._reset_commissioning_warmup(
                    "LOCAL_BRIDGE_DISCONNECTED" if state is StreamHealth.DISCONNECTED else "LOCAL_BRIDGE_RECONNECTED"
                )
            self._last_policy_reset_count = reset_after
            self._snapshot = replace(self._snapshot, observed_at=_now(), local_bridge_healthy=state is StreamHealth.HEALTHY, local_sequence_gap=state is StreamHealth.DISCONNECTED or self._snapshot.local_sequence_gap)
            if state is StreamHealth.DISCONNECTED and self._position is not PaperDirection.FLAT:
                self._request_exit("LOCAL_OBSERVATION_BRIDGE_DISCONNECTED", emergency=True)

    def on_observation_rejection(self, error: NinjaTraderObservationError) -> None:
        with self._lock:
            self._advance_commissioning_authority_epoch()
            self.policy.on_rejection(error)
            self._reset_commissioning_warmup("OBSERVATION_REJECTED:" + error.code.value)
            self._last_policy_reset_count = self.policy.reset_count()
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), local_sequence_gap=True,
                evidence_warmed=False, commissioning_session_warmed=False,
            )
            self.ledger.append("INCIDENT_OBSERVATION_REJECTION", {"code": error.code.value, "detail": error.detail or "unspecified"})
            if self._position is not PaperDirection.FLAT:
                self._request_exit("MALFORMED_OBSERVATION", emergency=True)

    def on_observation_duplicate(self) -> None:
        with self._lock:
            self.policy.on_duplicate()
            self.ledger.append("INCIDENT_DUPLICATE_OBSERVATION", {"effect": "NO_NEW_PAPER_EVIDENCE"})

    def record_sink_failure(self, sink: str, event: str, error_type: str) -> None:
        with self._lock:
            self.ledger.append("INCIDENT_OBSERVATION_SINK_FAILURE", {"sink": sink, "event": event, "error_type": error_type})

    def ingest(self, observation: NinjaTraderObservation) -> None:
        with self._lock:
            context, session_reason = self._resolve_observation_session(observation)
            raw_payload = {
                **context.payload(),
                "observation_id": observation.observation_id,
                "observation_type": observation.observation_type,
                "observed_at": self._market_event_timestamp(observation),
                "ninja_receipt_time": observation.ninja_receipt_time,
                "provider_timestamp": observation.provider_timestamp,
                "exchange_timestamp": observation.exchange_timestamp,
                "local_monotonic_sequence": observation.local_monotonic_sequence,
                "source_payload_hash": canonical_hash(dict(observation.payload)),
            }
            if (
                observation.observation_type == "ACCOUNT"
                and set(observation.payload) == {"item", "value"}
                and isinstance(observation.payload.get("item"), str)
                and self._decimal(observation.payload.get("value")) is not None
                and observation.account_alias in {"Sim101", "Lucid25kflex01"}
                and observation.account_class is not None
                and (observation.account_alias, observation.account_class.value)
                in {
                    ("Sim101", "LOCAL_SIMULATION"),
                    ("Lucid25kflex01", "PROVIDER_EVALUATION"),
                }
            ):
                raw_payload.update({
                    "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
                    "observation_semantics": "INFORMATIONAL_ACCOUNT_ITEM",
                    "observation_payload_keys": ["item", "value"],
                    "observation_account_alias": observation.account_alias,
                    "observation_account_class": observation.account_class.value,
                })
            expected_authority_payload_keys = COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS.get(
                observation.observation_type
            )
            if (
                expected_authority_payload_keys is not None
                and set(observation.payload) == expected_authority_payload_keys
                and observation.account_alias in {"Sim101", "Lucid25kflex01"}
                and observation.account_class is not None
                and (observation.account_alias, observation.account_class.value)
                in {
                    ("Sim101", "LOCAL_SIMULATION"),
                    ("Lucid25kflex01", "PROVIDER_EVALUATION"),
                }
            ):
                raw_payload.update({
                    "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
                    "observation_semantics": COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS,
                    "observation_payload_keys": sorted(expected_authority_payload_keys),
                    "observation_account_alias": observation.account_alias,
                    "observation_account_class": observation.account_class.value,
                })
            self.ledger.append_deferred(
                "OBSERVATION_ENVELOPE", raw_payload,
                identity="l3g-paper-observation-" + canonical_hash(raw_payload),
                occurred_at=observation.ninja_receipt_time,
            )
            if session_reason == "EVENT_TIMESTAMP_MOVED_BACKWARD":
                self._reset_commissioning_warmup(session_reason)
                self.ledger.append(
                    "INCIDENT_STALE_CALLBACK_REFUSED",
                    {
                        **context.payload(),
                        "reason": session_reason,
                        "observation_id": observation.observation_id,
                        "observation_type": observation.observation_type,
                        "ninja_receipt_time": observation.ninja_receipt_time,
                        "local_monotonic_sequence": observation.local_monotonic_sequence,
                    },
                    identity="l3g-stale-callback-" + canonical_hash(raw_payload),
                    occurred_at=observation.ninja_receipt_time,
                )
                return
            before_classified = self.policy.classified_trade_count() if observation.observation_type == "TRADE" else 0
            reset_before = self.policy.reset_count()
            decision = self.policy.ingest_runtime(
                observation, current_position=self._position,
                pending_order=self._state in {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.EXIT_PENDING},
                session_context=context,
            )
            reset_after = self.policy.reset_count()
            if reset_after != reset_before:
                self._reset_commissioning_warmup(
                    decision.reason_code if decision is not None else "PROVISIONAL_OBSERVATION_DOMAIN_RESET"
                )
            self._last_policy_reset_count = reset_after
            after_classified, warmed, depth_recovering = self.policy.runtime_gate_state()
            # Paper freshness follows the declared local callback authority.
            # Provider timestamps remain source provenance but independent
            # market-data streams do not form one provider-ordered clock.
            event_at = self._market_event_timestamp(observation)
            update: dict[str, object] = {"observed_at": normalized_utc(event_at, "Runtime observation time")}
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
            prior_snapshot = self._snapshot
            self._snapshot = replace(self._snapshot, **update)
            commissioning_guard_fields = (
                "market_price_connected", "evidence_warmed", "commissioning_session_warmed",
                "local_sequence_gap", "depth_reset_recovery",
            )
            if any(
                getattr(prior_snapshot, field) != getattr(self._snapshot, field)
                for field in commissioning_guard_fields
            ):
                self._advance_commissioning_authority_epoch()
            self._observe_commissioning_warmup(event_at)
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
                and self._armed_session is not None
                and self._armed_session.valid_at(_now())
                and self._armed_session.session_id == context.session_id
                and context.entry_permitted_at(datetime.fromisoformat(normalized_utc(event_at, "Entry event time").replace("Z", "+00:00")))
            ) or (
                decision.decision is PaperDecisionKind.EXIT
                and self._position is not PaperDirection.FLAT
            )
            if not can_cause_side_effect:
                self.ledger.append_deferred(
                    "DECISION",
                    {**decision.payload(), "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT},
                    identity=decision.paper_decision_id,
                    occurred_at=decision.created_at,
                    execution_session_id=self._execution_session_id(),
                )
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
            if (
                self._state is PaperRuntimeState.ARMED_FLAT and not self._entries_paused
                and self._armed_session is not None and self._armed_session.valid_at(_now())
                and self._armed_session.session_id == context.session_id
                and context.entry_permitted_at(datetime.fromisoformat(normalized_utc(event_at, "Entry event time").replace("Z", "+00:00")))
            ):
                if self._entry_owner is PaperEntryOwner.COMMISSIONING:
                    self._record_strategy_suppression(decision, context)
                    return
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
        elif self._session_context.hard_flat_due_at(datetime.fromisoformat(normalized_utc(at, "Risk exit time").replace("Z", "+00:00"))):
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

    def _request_entry(self, decision: PaperDecision) -> bool:
        """The shared final entry-admission boundary for strategy and commissioning."""
        with self._lock:
            return self._request_entry_locked(decision)

    def _request_entry_locked(self, decision: PaperDecision) -> bool:
        if decision.commissioning:
            ownership = self._commissioning_ownership
            if (
                ownership is None or self._entry_owner is not PaperEntryOwner.COMMISSIONING
                or ownership.entry_consumed
            ):
                return False
        elif self._entry_owner is PaperEntryOwner.COMMISSIONING:
            self._record_strategy_suppression(decision, self._session_context)
            return False
        elif self._entry_owner is not PaperEntryOwner.NONE:
            return False
        bid, ask, last = self._references()
        intent = self.risk.make_intent(decision, reference_bid=bid, reference_ask=ask, reference_last=last)
        self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
        grant = self.risk.evaluate(intent, self._snapshot, at=_now())
        self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
        if not grant.granted:
            return False
        action = ExecutionAction.ENTER_LONG if decision.decision is PaperDecisionKind.LONG else ExecutionAction.ENTER_SHORT
        command = self._make_command(
            intent.intent_id, decision.paper_decision_id, grant.grant_id, action, intent.target_position,
            "COMMISSIONING_PAPER_ENTRY" if decision.commissioning else "AUTONOMOUS_PAPER_ENTRY",
            commissioning=decision.commissioning,
            strategy_generated=decision.strategy_generated,
            scientific_evidence=decision.scientific_evidence,
        )
        self._entry_authority_artifact = {
            "decision": decision.payload(),
            "intent": intent.payload(),
            "risk_grant": grant.payload(),
            "command": command.payload(),
        }
        if decision.commissioning:
            # The authorization becomes single-use only after its exact
            # decision, intent, grant, and command exist and remain current at
            # the transport boundary.
            admission_at = _now()
            if not grant.valid_at(admission_at) or datetime.fromisoformat(
                normalized_utc(decision.expires_at, "Commissioning authorization expiry").replace("Z", "+00:00")
            ) < datetime.fromisoformat(
                normalized_utc(admission_at, "Commissioning transport admission time").replace("Z", "+00:00")
            ):
                raise _CommissioningAuthorizationExpired("COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED")
            assert ownership is not None
            ownership = replace(
                ownership, entry_consumed=True, entry_decision_id=decision.paper_decision_id,
            )
            self._commissioning_ownership = ownership
            self.ledger.append(
                "COMMISSIONING_ENTRY_CONSUMED",
                self._ownership_payload(ownership, reason="EXPLICIT_COMMISSIONING_ENTRY"),
                identity="l3g-commissioning-entry-consumed-" + ownership.commissioning_id,
                execution_session_id=self._execution_session_id(),
            )
        else:
            self._entry_owner = PaperEntryOwner.STRATEGY
        self._pending_intent = intent
        self._pending_grant = grant
        if not decision.commissioning:
            self.policy.mark_entry_used(decision)
        # Install every callback-visible authority fact before transport.  A
        # synchronous AddOn acknowledgement or fill can safely re-enter this
        # runtime through the RLock without observing an unowned command.
        self._transition(PaperRuntimeState.ENTRY_PENDING, "ENTRY_COMMAND_AUTHORIZED")
        self._persist_and_send(command, grant)
        return True

    def _request_exit(self, reason: str, *, emergency: bool = False) -> None:
        if self._position is PaperDirection.FLAT or self._state in {
            PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.LOCKED_OUT,
            PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED,
        } or self._exit_submission_in_progress:
            return
        if self._last_decision is None:
            decision_id = "l3g-pd-safety-" + canonical_hash({"reason": reason, "at": _now()})[:24]
        else:
            decision_id = self._last_decision.paper_decision_id
        commissioning = self._commissioning_ownership is not None
        if commissioning:
            self._entries_paused = True
            self._armed_session = None
            self._disarm_after_flat = True
        # Safety authority must be fresh even when the directional decision
        # which prompted it is older than the five-second intent TTL.
        created_at = _now()
        # Safety exits use a directional decision provenance but remain an
        # independently risk-evaluated flat intent.
        pseudo = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": reason, "decision": decision_id, "at": created_at})[:32],
            POLICY.policy_id, POLICY.configuration_hash, PaperDecisionKind.EXIT, created_at,
            (datetime.fromisoformat(normalized_utc(created_at, "Exit decision time").replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"risk_exit": reason}, (decision_id,), (max(0, self.policy.status().get("last_local_sequence") or 0),), (canonical_hash({"reason": reason}),),
            POLICY.sequence_authority, POLICY.book_completeness, False, reason,
            self._session_context.session_kind, self._session_context.session_id,
            self._session_context.trade_date, self._session_context.session_profile_hash,
            self._session_context.session_generation,
            commissioning, not commissioning, False,
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
        command = self._make_command(
            intent.intent_id, pseudo.paper_decision_id, grant.grant_id, action, PaperDirection.FLAT, reason,
            commissioning=commissioning, strategy_generated=not commissioning, scientific_evidence=False,
        )
        self._exit_submission_in_progress = True
        try:
            self._transition(PaperRuntimeState.EXIT_PENDING, reason)
            self._persist_and_send(command, grant)
        finally:
            self._exit_submission_in_progress = False

    def _make_command(
        self,
        intent_id: str,
        decision_id: str,
        grant_id: str,
        action: ExecutionAction,
        expected: PaperDirection,
        reason: str,
        *,
        commissioning: bool = False,
        strategy_generated: bool = True,
        scientific_evidence: bool = False,
    ) -> PaperExecutionCommand:
        execution_session = self._execution_session_id()
        if execution_session is None:
            raise RuntimeError("No authenticated execution session is available.")
        context = self._session_context
        self._command_sequence += 1
        created = _now()
        quantity = 0 if action in {ExecutionAction.HEARTBEAT, ExecutionAction.RECONCILE, ExecutionAction.CANCEL_OWNED_ORDERS} else 1
        payload = {
            "command_sequence": self._command_sequence,
            "session_id": context.session_id,
            "session_kind": context.session_kind,
            "trade_date": context.trade_date,
            "session_profile_hash": context.session_profile_hash,
            "session_generation": context.session_generation,
            "execution_session_id": execution_session,
            "intent_id": intent_id,
            "decision_id": decision_id,
            "action": action,
            "account_name": ACCOUNT_BINDING.account_name,
            "account_class": ACCOUNT_BINDING.account_class,
            "instrument": ACCOUNT_BINDING.instrument,
            "quantity": quantity,
            "expected_position": expected,
            "created_at": created,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=POLICY.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
            "policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "reason_code": reason,
            "risk_grant_id": grant_id,
            "commissioning": commissioning,
            "strategy_generated": strategy_generated,
            "scientific_evidence": scientific_evidence,
        }
        return PaperExecutionCommand(deterministic_id("l3g-pc-", payload), **payload)

    def _persist_and_send(self, command: PaperExecutionCommand, grant: object) -> None:
        self.ledger.append("COMMAND", command.payload(), identity=command.command_id, occurred_at=command.created_at, execution_session_id=command.execution_session_id)
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
            self._advance_commissioning_authority_epoch()
            message_type = str(message.get("message_type", ""))
            if message_type == "RECONCILIATION":
                self._apply_reconciliation(message)
            elif message_type in {"ORDER_EVENT", "COMMAND_ACK", "COMMAND_REJECTED"}:
                self._last_order_state = dict(message)
                if message_type == "COMMAND_REJECTED":
                    self._fault_reason = "EXECUTION_COMMAND_REJECTED:" + str(message.get("reason_code", "UNKNOWN"))
                    self.risk.lock_out(self._fault_reason)
                    # Preserve the owned safety-exit path while a position is
                    # open; role-specific handling below initiates or audits it.
                    if self._position is PaperDirection.FLAT and self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                role = str(message.get("order_role", "")).upper()
                order_state = str(message.get("order_state", "")).upper()
                if role == "PROTECTIVE" and self._position is not PaperDirection.FLAT:
                    protective_reason: str | None = None
                    account = message.get("account_name")
                    instrument = message.get("instrument")
                    quantity = message.get("quantity")
                    native_order_id = message.get("native_order_id")
                    if account is not None and account != ACCOUNT_BINDING.account_name:
                        protective_reason = "PROTECTIVE_STOP_WRONG_ACCOUNT"
                    elif instrument is not None and instrument != ACCOUNT_BINDING.instrument:
                        protective_reason = "PROTECTIVE_STOP_WRONG_INSTRUMENT"
                    elif quantity is not None and quantity != self._position_quantity:
                        protective_reason = "PROTECTIVE_STOP_WRONG_QUANTITY"
                    elif (
                        isinstance(native_order_id, str) and self._protective_order_id is not None
                        and native_order_id != self._protective_order_id
                    ):
                        protective_reason = "DUPLICATE_PROTECTIVE_STOP"
                    if protective_reason is not None:
                        self._fault_reason = protective_reason
                        self.risk.lock_out(protective_reason)
                        self._request_exit(protective_reason, emergency=True)
                    elif isinstance(native_order_id, str) and native_order_id:
                        self._protective_order_id = native_order_id
                expected_protective_cancellation = (
                    role == "PROTECTIVE"
                    and order_state in {"CANCELLED", "CANCELED"}
                    and (self._exit_submission_in_progress or self._state is PaperRuntimeState.EXIT_PENDING)
                )
                if role == "PROTECTIVE" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._position is not PaperDirection.FLAT and not expected_protective_cancellation:
                    self.risk.lock_out("PROTECTIVE_STOP_REJECTED")
                    self._request_exit("PROTECTIVE_STOP_REJECTED", emergency=True)
                if role == "PROTECTIVE":
                    self._snapshot = replace(self._snapshot, observed_at=_now(), protective_stop_state=order_state or self._snapshot.protective_stop_state)
                if role == "ENTRY" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._state is PaperRuntimeState.ENTRY_PENDING:
                    self._fault_reason = "MARKET_ENTRY_ORDER_" + order_state
                    self.risk.lock_out(self._fault_reason)
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                if (
                    message_type == "COMMAND_REJECTED"
                    and str(message.get("reason_code", "")).upper() in {"COMMAND_ACK_TIMEOUT", "ACKNOWLEDGEMENT_MISSING"}
                    and self._position is not PaperDirection.FLAT
                ):
                    self._fault_reason = "PROTECTIVE_STOP_ACKNOWLEDGEMENT_MISSING"
                    self.risk.lock_out(self._fault_reason)
                    self._request_exit(self._fault_reason, emergency=True)
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
            ownership = self._commissioning_ownership
            if ownership is not None and ownership.recovered_after_restart:
                self.ledger.append(
                    "COMMISSIONING_OWNERSHIP_RECOVERED",
                    self._ownership_payload(ownership, reason="RECOVERY_ACTIVITY_REQUIRES_LOCKOUT"),
                    identity="l3g-commissioning-ownership-recovered-" + ownership.commissioning_id,
                    execution_session_id=self._execution_session_id(),
                )
            self._fault_reason = "RECONCILIATION_BLOCKED"
            self.risk.lock_out(self._fault_reason)
            if self._state is PaperRuntimeState.RECONCILING:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
        elif self._state is PaperRuntimeState.RECONCILING:
            if self._settle_recovered_commissioning_ownership():
                return
            if self._post_exit_reconciliation_pending:
                self._complete_post_exit_reconciliation(message)
            else:
                self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_RECONCILIATION_COMPLETE")

    def _apply_execution(self, message: Mapping[str, object]) -> None:
        role = str(message.get("order_role", ""))
        native_execution_id = message.get("native_execution_id")
        if isinstance(native_execution_id, str) and native_execution_id:
            if native_execution_id in self._seen_native_execution_ids:
                self.ledger.append(
                    "INCIDENT_DUPLICATE_EXECUTION_CALLBACK",
                    {"native_execution_id": native_execution_id, "effect": "IDEMPOTENT_NO_STATE_CHANGE"},
                    identity="l3g-duplicate-execution-" + native_execution_id,
                    execution_session_id=self._execution_session_id(),
                )
                return
            self._seen_native_execution_ids.add(native_execution_id)
        supplied_account = message.get("account_name")
        supplied_instrument = message.get("instrument")
        if (
            (supplied_account is not None and supplied_account != ACCOUNT_BINDING.account_name)
            or (supplied_instrument is not None and supplied_instrument != ACCOUNT_BINDING.instrument)
        ):
            self._fault_reason = "FOREIGN_EXECUTION_CLASSIFICATION"
            self._snapshot = replace(self._snapshot, foreign_activity=True, observed_at=_now())
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        price = self._decimal(message.get("price"))
        quantity = message.get("quantity")
        if price is None or type(quantity) is not int or quantity <= 0:
            self._fault_reason = "MALFORMED_EXECUTION_EVENT"
            self.risk.lock_out(self._fault_reason)
            return
        if role == "ENTRY":
            if self._state is not PaperRuntimeState.ENTRY_PENDING:
                self._fault_reason = "UNEXPECTED_ENTRY_EXECUTION_STATE"
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                return
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
            self._entry_execution = dict(message)
            entry_context = context_from_identity(
                intent.session_kind, intent.session_id, intent.trade_date,
                intent.session_profile_hash, intent.session_generation,
            )
            self._entry_session_context = entry_context
            trade_risk = self._family_risk.setdefault((entry_context.trade_date, entry_context.session_family), _TradeDateRisk())
            trade_risk.entry_count += 1
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), current_position=expected,
                current_position_quantity=quantity,
                position_opened_at=str(message.get("timestamp", _now())),
                protective_stop_state="PENDING",
                session_entry_count=self._snapshot.session_entry_count + 1,
                trade_date_entry_count=trade_risk.entry_count,
            )
            if self._state is PaperRuntimeState.ENTRY_PENDING:
                self._transition(PaperRuntimeState.LONG if expected is PaperDirection.LONG else PaperRuntimeState.SHORT, "ENTRY_FILL_CONFIRMED")
            if not fill_ok:
                self._request_exit(reason, emergency=True)
        elif role in {"EXIT", "PROTECTIVE"}:
            if (
                self._position is PaperDirection.FLAT or self._entry_fill_price is None
                or self._entry_fill_quantity <= 0
            ):
                self._fault_reason = "UNEXPECTED_EXIT_EXECUTION_STATE"
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                return
            # Final flat truth still requires a position event/reconciliation.
            realized = Decimal("0")
            if self._entry_fill_price is not None and self._entry_fill_quantity > 0:
                points = price - self._entry_fill_price if self._entry_direction is PaperDirection.LONG else self._entry_fill_price - price
                realized = points * Decimal("2") * self._entry_fill_quantity
            entry_context = self._entry_session_context or self._session_context
            trade_risk = self._family_risk.setdefault((entry_context.trade_date, entry_context.session_family), _TradeDateRisk())
            trade_risk.realized_pnl += realized
            trade_risk.consecutive_losses = trade_risk.consecutive_losses + 1 if realized < 0 else 0 if realized > 0 else trade_risk.consecutive_losses
            self._session_pnl[entry_context.session_id] = self._session_pnl.get(entry_context.session_id, Decimal("0")) + realized
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), daily_realized_pnl=trade_risk.realized_pnl,
                daily_unrealized_pnl=trade_risk.unrealized_pnl,
                trade_date_entry_count=trade_risk.entry_count,
                consecutive_losses=trade_risk.consecutive_losses,
            )
            self._exit_execution = dict(message)
            self._lifecycle_realized_pnl = realized
            entry = self._entry_execution or {}
            self.ledger.append(
                "EXECUTION_REALIZED_PNL",
                {
                    "commissioning": self._commissioning_ownership is not None,
                    "strategy_generated": self._commissioning_ownership is None,
                    "scientific_evidence": False,
                    "entry_decision_id": entry.get("decision_id"),
                    "entry_command_id": entry.get("command_id"),
                    "entry_execution_id": entry.get("native_execution_id"),
                    "entry_order_id": entry.get("native_order_id"),
                    "entry_price": str(self._entry_fill_price) if self._entry_fill_price is not None else None,
                    "entry_quantity": self._entry_fill_quantity,
                    "entry_timestamp": entry.get("timestamp"),
                    "exit_command_id": message.get("command_id"),
                    "exit_execution_id": message.get("native_execution_id"),
                    "exit_order_id": message.get("native_order_id"),
                    "exit_price": str(price),
                    "exit_quantity": quantity,
                    "exit_timestamp": message.get("timestamp"),
                    "contract_value_per_point": "2",
                    "simulated_fees": "0",
                    "realized_pnl": str(realized),
                    "pnl_basis": "AUTHENTIC_ENTRY_AND_EXIT_FILLS",
                    "position_confirmation": "PENDING",
                },
                identity="l3g-realized-pnl-" + str(message.get("native_execution_id", canonical_hash(dict(message)))),
                execution_session_id=self._execution_session_id(),
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
            self._post_exit_reconciliation_pending = True
            self._transition(PaperRuntimeState.RECONCILING, "FLAT_POSITION_PENDING_RECONCILIATION")
            self._request_reconciliation_after_exit()

    def _request_reconciliation_after_exit(self) -> None:
        """Require a new signed flat/order snapshot before lifecycle completion."""
        created_at = _now()
        commissioning = self._commissioning_ownership is not None
        decision = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": "POST_EXIT_RECONCILIATION", "at": created_at})[:32],
            POLICY.policy_id, POLICY.configuration_hash, PaperDecisionKind.EXIT, created_at,
            (datetime.fromisoformat(normalized_utc(created_at, "Post-exit reconciliation time").replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"safety": "POST_EXIT_RECONCILIATION"},
            ((self._last_decision.paper_decision_id if self._last_decision is not None else "post-exit-safety"),),
            (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash({"reason": "POST_EXIT_RECONCILIATION"}),), POLICY.sequence_authority,
            POLICY.book_completeness, False, "POST_EXIT_RECONCILIATION",
            self._session_context.session_kind, self._session_context.session_id,
            self._session_context.trade_date, self._session_context.session_profile_hash,
            self._session_context.session_generation,
            commissioning, not commissioning, False,
        )
        self.ledger.append("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
        bid, ask, last = self._references()
        intent = self.risk.make_intent(decision, reference_bid=bid, reference_ask=ask, reference_last=last)
        self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
        grant = self.risk.evaluate(intent, self._snapshot, at=created_at)
        self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
        if not grant.granted:
            self._fault_reason = "POST_EXIT_RECONCILIATION_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes)
            self.risk.lock_out(self._fault_reason)
            self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        command = self._make_command(
            intent.intent_id, decision.paper_decision_id, grant.grant_id, ExecutionAction.RECONCILE, PaperDirection.FLAT,
            "POST_EXIT_RECONCILIATION", commissioning=commissioning, strategy_generated=not commissioning,
            scientific_evidence=False,
        )
        self._persist_and_send(command, grant)

    def _complete_post_exit_reconciliation(self, reconciliation: Mapping[str, object]) -> None:
        """Close a fully evidenced lifecycle only after a fresh clean reconciliation."""
        ownership = self._commissioning_ownership
        commissioning = ownership is not None and ownership.entry_consumed
        self.policy.confirm_flat(str(reconciliation.get("timestamp", _now())))
        self._pending_intent = None
        self._pending_grant = None
        self._entry_authority_artifact = None
        target = PaperRuntimeState.READY_DISARMED if self._disarm_after_flat or commissioning else PaperRuntimeState.PAUSED if self._entries_paused else PaperRuntimeState.ARMED_FLAT
        self._transition(target, "POST_EXIT_FLAT_RECONCILIATION_COMPLETE")
        if commissioning:
            entry = self._entry_execution or {}
            exit_fill = self._exit_execution or {}
            transport = None if self._transport is None else self._transport.status().as_dict()
            ledger_health = self.ledger.health_status()
            closure = {
                    "commissioning": True,
                    "commissioning_id": None if ownership is None else ownership.commissioning_id,
                    "request_id": None if ownership is None else ownership.request_id,
                    "strategy_generated": False,
                    "scientific_evidence": False,
                    "classification": "EXPLICIT_PAPER_COMMISSIONING",
                    "git_sha": self._runtime_identity.get("git_sha"),
                    "runtime_binding": {
                        "ledger_path": ledger_health.get("path"),
                        "ledger_epoch": ledger_health.get("epoch_id"),
                        "audit_root": self._runtime_identity.get("audit"),
                        "control_center": self._runtime_identity.get("control_center"),
                        "python": self._runtime_identity.get("python"),
                        "pid": self._runtime_identity.get("pid"),
                    },
                    "addon_provenance": None if transport is None else transport.get("addon_provenance"),
                    "session_identity": (self._entry_session_context or self._session_context).payload(),
                    "observer_status": {
                        "local_bridge_healthy": self._snapshot.local_bridge_healthy,
                        "market_price_connected": self._snapshot.market_price_connected,
                    },
                    "commissioning_warmup": {
                        "warmed": self._snapshot.commissioning_session_warmed,
                        "warmed_at": self._commissioning_warmup_warmed_at,
                        "policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
                    },
                    "strategy_evidence_state": "ACTIVE" if self._snapshot.evidence_warmed else "INCOMPLETE",
                    "ledger_preflight": None if ownership is None else ownership.ledger_preflight,
                    "entry_owner": PaperEntryOwner.COMMISSIONING.value,
                    "entry_authority": self._entry_authority_artifact,
                    "entry_direction": self._entry_direction.value,
                    "entry_decision_id": entry.get("decision_id"),
                    "entry_command_id": entry.get("command_id"),
                    "entry_order_id": entry.get("native_order_id"),
                    "entry_execution_id": entry.get("native_execution_id"),
                    "entry_price": str(self._entry_fill_price) if self._entry_fill_price is not None else None,
                    "entry_quantity": self._entry_fill_quantity,
                    "exit_command_id": exit_fill.get("command_id"),
                    "exit_order_id": exit_fill.get("native_order_id"),
                    "exit_execution_id": exit_fill.get("native_execution_id"),
                    "exit_price": exit_fill.get("price"),
                    "exit_quantity": exit_fill.get("quantity"),
                    "protective_stop": {
                        "native_order_id": self._protective_order_id,
                        "state": self._snapshot.protective_stop_state,
                    },
                    "contract_value_per_point": "2",
                    "simulated_fees": "0",
                    "realized_pnl": str(self._lifecycle_realized_pnl),
                    "final_position": "FLAT",
                    "final_quantity": 0,
                    "final_working_order_count": 0,
                    "foreign_activity": False,
                    "reconciliation_state": "CLEAN",
                    "final_reconciliation": dict(reconciliation),
                    "lock_disarm_state": target.value,
                    "ledger_hash_chain_required": True,
                    "post_run_verification": {
                        "required": True,
                        "status": "PENDING",
                        "verification_id": None,
                        "verified_through_sequence": None,
                    },
                    "lucid_mutation_count": 0,
                    "incidents": [],
                    "final_judgment": "COMMISSIONING_INCOMPLETE_PENDING_POST_RUN_VERIFICATION",
                }
            self.ledger.append(
                "COMMISSIONING_CLOSURE",
                closure,
                identity="l3g-commissioning-closure-" + str(exit_fill.get("native_execution_id", canonical_hash(dict(reconciliation)))),
                execution_session_id=self._execution_session_id(),
            )
            self._last_commissioning_closure = {
                **closure,
                "closure_ledger_sequence": int(self.ledger.health_status()["highest_sequence"]),
            }
            self._release_commissioning_ownership("CLEAN_COMMISSIONING_LIFECYCLE_COMPLETED")
        elif self._entry_owner is PaperEntryOwner.STRATEGY:
            self._entry_owner = PaperEntryOwner.NONE
        self._entry_fill_price = None
        self._entry_fill_quantity = 0
        self._entry_direction = PaperDirection.FLAT
        self._entry_execution = None
        self._entry_authority_artifact = None
        self._exit_execution = None
        self._protective_order_id = None
        self._lifecycle_realized_pnl = Decimal("0")
        self._entry_session_context = None
        self._post_exit_reconciliation_pending = False
        self._disarm_after_flat = False

    def _abort_unsubmitted_commissioning(self, reason: str) -> None:
        """Release only the pre-broker, provably flat commissioning failure."""
        ownership = self._commissioning_ownership
        if ownership is None:
            return
        if (
            self._position is not PaperDirection.FLAT or self._position_quantity != 0
            or self._snapshot.working_owned_orders != 0 or self._snapshot.working_entry_orders != 0
        ):
            self.ledger.append(
                "INCIDENT_COMMISSIONING_ENTRY_AMBIGUOUS",
                self._ownership_payload(ownership, reason=reason + "_NONFLAT_OR_ORDERS"),
                identity="l3g-commissioning-entry-ambiguous-" + ownership.commissioning_id,
                execution_session_id=self._execution_session_id(),
            )
            self._fault_reason = "COMMISSIONING_ENTRY_AMBIGUOUS"
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        self.ledger.append(
            "INCIDENT_COMMISSIONING_ENTRY_REJECTED",
            self._ownership_payload(ownership, reason=reason),
            identity="l3g-commissioning-entry-rejected-" + ownership.commissioning_id,
            execution_session_id=self._execution_session_id(),
        )
        self._entries_paused = True
        self._armed_session = None
        self._disarm_after_flat = False
        self._pending_intent = None
        self._pending_grant = None
        if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.ENTRY_PENDING}:
            self._transition(PaperRuntimeState.READY_DISARMED, "COMMISSIONING_ENTRY_REJECTED_BEFORE_COMMAND")
        self._release_commissioning_ownership(reason)

    @staticmethod
    def _freshness_gate(source: str | None, maximum_seconds: int, at: str) -> dict[str, object]:
        if source is None:
            return {
                "observed_at": None, "age_seconds": None, "maximum_age_seconds": maximum_seconds,
                "fresh": False,
            }
        moment = datetime.fromisoformat(normalized_utc(at, "Commissioning readiness time").replace("Z", "+00:00"))
        observed = datetime.fromisoformat(normalized_utc(source, "Commissioning freshness time").replace("Z", "+00:00"))
        age = (moment - observed).total_seconds()
        return {
            "observed_at": normalized_utc(source, "Commissioning freshness time"),
            "age_seconds": round(age, 6),
            "maximum_age_seconds": maximum_seconds,
            "fresh": 0 <= age <= maximum_seconds,
        }

    def _run_commissioning_ledger_preflight(
        self,
        commissioning_id: str,
        capture: _CommissioningReadinessCapture,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None,
    ) -> tuple[dict[str, object] | None, str | None, float]:
        """Run a ledger validator; callers keep the live pass outside the runtime lock."""
        started = time.perf_counter()
        ledger_evidence: dict[str, object] | None = None
        ledger_blocker: str | None = None
        if ledger_preflight is None:
            ledger_blocker = "COMMISSIONING_LEDGER_STATUS_UNKNOWN"
        else:
            try:
                result = ledger_preflight(
                    commissioning_id, self._commissioning_runtime_snapshot(commissioning_id, capture),
                )
                if not isinstance(result, Mapping):
                    raise RuntimeError("Commissioning ledger preflight returned an invalid result.")
                ledger_evidence = dict(result)
            except Exception as exc:
                detail = str(getattr(exc, "detail", None) or exc or type(exc).__name__)
                prefix = detail.split(":", 1)[0].strip()
                ledger_blocker = (
                    prefix if prefix.startswith("COMMISSIONING_")
                    else "COMMISSIONING_LEDGER_PREFLIGHT_FAILED"
                )
                ledger_evidence = {"status": "BLOCKED", "detail": detail}
        return ledger_evidence, ledger_blocker, time.perf_counter() - started

    def _commissioning_readiness_locked(
        self,
        *,
        at: str,
        commissioning_id: str,
        capture: _CommissioningReadinessCapture,
        ledger_evidence: dict[str, object] | None,
        ledger_blocker: str | None,
        preflight_duration_seconds: float,
    ) -> dict[str, object]:
        """Revalidate a completed ledger preflight against current authority facts."""
        context = self._session_context
        transport_status = None if self._transport is None else self._transport.status()
        current_guard_token = canonical_hash(self._commissioning_guard_payload_locked(transport_status))
        stale_snapshot = current_guard_token != capture.guard_token
        blocking: list[str] = []
        if stale_snapshot:
            blocking.append("COMMISSIONING_READINESS_SNAPSHOT_STALE")
            self._commissioning_stale_snapshot_refusals += 1
        if self._state is not PaperRuntimeState.READY_DISARMED:
            blocking.append("STATE_NOT_READY_DISARMED")
        if self._entry_owner is not PaperEntryOwner.NONE or self._commissioning_ownership is not None:
            blocking.append("COMMISSIONING_OWNERSHIP_ACTIVE")
        addon_match = transport_status is not None and transport_status.addon_provenance_valid
        if not addon_match:
            blocking.append("ADDON_BUILD_MISMATCH")
        current = PaperSessionResolver().resolve(at, generation=context.session_generation).context
        session_current = (
            context.session_kind is not PaperSessionKind.OFF_SESSION
            and self._ownership_context_matches(context, current)
        )
        if not session_current:
            blocking.append("NO_CURRENT_EVENT_SESSION")
        blocking.extend(self.risk.preflight_reasons(self._snapshot, at=at, commissioning=True))
        if ledger_blocker is not None:
            blocking.append(ledger_blocker)
        freshness = {
            "quote": self._freshness_gate(
                self._snapshot.quote_observed_at, RISK_PROFILE.quote_maximum_age_seconds, at,
            ),
            "classified_trade": self._freshness_gate(
                self._snapshot.classified_trade_observed_at,
                RISK_PROFILE.classified_trade_maximum_age_seconds, at,
            ),
            "depth_mutation": self._freshness_gate(
                self._snapshot.depth_mutation_observed_at,
                RISK_PROFILE.depth_mutation_maximum_age_seconds, at,
            ),
        }
        family_progress = {
            family.value: {
                "seen": family.value in self._commissioning_warmup_seen,
                "provenance": self._commissioning_warmup_seen.get(family.value),
            }
            for family in _COMMISSIONING_REQUIRED_FAMILIES
        }
        reasons = tuple(dict.fromkeys(blocking))
        self._last_commissioning_preflight_duration_seconds = preflight_duration_seconds
        payload: dict[str, object] = {
            "schema": "lane-iii-phase-g-commissioning-readiness-v1",
            "result": "READY" if not reasons else "BLOCKED",
            "generated_at": normalized_utc(at, "Commissioning readiness time"),
            "commissioning_id": commissioning_id,
            "runtime_snapshot": {
                "generation": capture.generation,
                "token": capture.guard_token,
                "current_token": current_guard_token,
                "stale": stale_snapshot,
            },
            "commissioning_preflight_duration_seconds": round(preflight_duration_seconds, 6),
            "session": {
                **context.payload(),
                "current": session_current,
                "entry_window": "OPEN" if context.entry_permitted_at(
                    datetime.fromisoformat(normalized_utc(at, "Commissioning readiness time").replace("Z", "+00:00"))
                ) else "CLOSED",
            },
            "observer": {
                "status": "ACTIVE" if self._snapshot.local_bridge_healthy else "NOT_ACTIVE",
                "continuity_healthy": not self._snapshot.local_sequence_gap and not self._snapshot.depth_reset_recovery,
                "local_bridge_healthy": self._snapshot.local_bridge_healthy,
                "market_price_connected": self._snapshot.market_price_connected,
            },
            "market_freshness": freshness,
            "commissioning_warmup": {
                "status": "WARMED" if self._snapshot.commissioning_session_warmed else "NOT_WARMED",
                "warmed_at": self._commissioning_warmup_warmed_at,
                "policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
                "required_families": family_progress,
            },
            "strategy_evidence": {
                "status": "ACTIVE" if self._snapshot.evidence_warmed else "INCOMPLETE",
                "current_transient_evidence_required_for_strategy": True,
            },
            "ledger": ledger_evidence or {"status": "UNKNOWN"},
            "broker": {
                "account": self._snapshot.account_name,
                "account_class": self._snapshot.account_class,
                "instrument": self._snapshot.instrument,
                "position": self._position.value,
                "quantity": self._position_quantity,
                "owned_orders": self._snapshot.working_owned_orders,
                "working_entry_orders": self._snapshot.working_entry_orders,
                "reconciliation_current": self._snapshot.reconciliation_current,
            },
            "addon": {
                "status": "MATCH" if addon_match else "MISMATCH",
                "expected_source_fingerprint": None if transport_status is None else transport_status.expected_addon_source_fingerprint,
                "source_fingerprint": None if transport_status is None else transport_status.addon_source_fingerprint,
                "build_fingerprint": None if transport_status is None else transport_status.addon_build_fingerprint,
                "protocol_version": None if transport_status is None else transport_status.addon_protocol_version,
            },
            "ownership": {
                "entry_owner": self._entry_owner.value,
                "commissioning_active": self._commissioning_ownership is not None,
            },
            "live_capital": "DENIED",
            "blocking_reasons": list(reasons),
        }
        payload["snapshot_hash"] = canonical_hash(payload)
        return payload

    def commissioning_rehearsal(
        self,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
    ) -> dict[str, object]:
        """Return a hash-bound, authority-free rehearsal of production validators."""
        with self._lock:
            started_at = _now()
            commissioning_id = "l3g-commissioning-rehearsal-" + canonical_hash({
                "at": started_at, "session_id": self._session_context.session_id,
                "session_generation": self._session_context.session_generation,
            })[:32]
            capture = self._capture_commissioning_readiness_locked()
        ledger_evidence, ledger_blocker, duration = self._run_commissioning_ledger_preflight(
            commissioning_id, capture, ledger_preflight,
        )
        with self._lock:
            return self._commissioning_readiness_locked(
                at=_now(), commissioning_id=commissioning_id, capture=capture,
                ledger_evidence=ledger_evidence, ledger_blocker=ledger_blocker,
                preflight_duration_seconds=duration,
            )

    def arm(self) -> dict[str, object]:
        with self._lock:
            if self._entry_owner is not PaperEntryOwner.NONE:
                return {"armed": False, "reason_codes": ("COMMISSIONING_OWNERSHIP_ACTIVE",), "state": self._state.value}
            if self._state is not PaperRuntimeState.READY_DISARMED:
                return {"armed": False, "reason_codes": ("STATE_NOT_READY_DISARMED",), "state": self._state.value}
            transport = None if self._transport is None else self._transport.status()
            if transport is None or not transport.addon_provenance_valid:
                reasons = ("ADDON_BUILD_MISMATCH",)
                self.ledger.append(
                    "RISK_EVENT_ARM_ATTEMPT",
                    {
                        **self._session_context.payload(), "allowed": False, "reason_codes": reasons,
                        "expected_addon_source_fingerprint": None if transport is None else transport.expected_addon_source_fingerprint,
                        "runtime_addon_source_fingerprint": None if transport is None else transport.addon_source_fingerprint,
                        "runtime_addon_protocol_version": None if transport is None else transport.addon_protocol_version,
                    },
                    execution_session_id=self._execution_session_id(),
                )
                return {"armed": False, "reason_codes": reasons, "state": self._state.value}
            context = self._session_context
            now = _now()
            current = PaperSessionResolver().resolve(now, generation=context.session_generation)
            if context.session_kind is PaperSessionKind.OFF_SESSION or current.context.session_id != context.session_id:
                reasons = ("NO_CURRENT_EVENT_SESSION",)
                self.ledger.append("RISK_EVENT_ARM_ATTEMPT", {**context.payload(), "allowed": False, "reason_codes": reasons, "authority_hash": canonical_hash(AUTHORITY.authority_payload())})
                return {"armed": False, "reason_codes": reasons, "state": self._state.value}
            allowed, reasons = self.risk.preflight(self._snapshot, at=now)
            self.ledger.append("RISK_EVENT_ARM_ATTEMPT", {**context.payload(), "allowed": allowed, "reason_codes": reasons, "authority_hash": canonical_hash(AUTHORITY.authority_payload())})
            if not allowed:
                return {"armed": False, "reason_codes": reasons, "state": self._state.value}
            self._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now,
                context.boundary_at("entry_cutoff").isoformat().replace("+00:00", "Z"),
            )
            self._entries_paused = False
            self._transition(PaperRuntimeState.ARMED_FLAT, "OPERATOR_ARM_AFTER_PREFLIGHT")
            return {
                "armed": True, "reason_codes": ("PAPER_ARMED",), "state": self._state.value,
                "session_armed_state": "ARMED_" + context.session_kind.value, "arm_grant": self._armed_session.payload(),
            }

    def commissioning_arm(
        self,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
        *,
        commissioning_id: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        """Atomically reserve commissioning ownership before exposing ARMED_FLAT."""
        identity = commissioning_id or "l3g-commissioning-" + uuid4().hex
        with self._lock:
            capture = self._capture_commissioning_readiness_locked()
        ledger_evidence, ledger_blocker, duration = self._run_commissioning_ledger_preflight(
            identity, capture, ledger_preflight,
        )
        with self._lock:
            return self._commit_commissioning_arm_locked(
                identity=identity, request_id=request_id, capture=capture,
                ledger_evidence=ledger_evidence, ledger_blocker=ledger_blocker,
                preflight_duration_seconds=duration, ledger_preflight=ledger_preflight,
            )

    def _commit_commissioning_arm_locked(
        self,
        *,
        identity: str,
        request_id: str | None,
        capture: _CommissioningReadinessCapture,
        ledger_evidence: dict[str, object] | None,
        ledger_blocker: str | None,
        preflight_duration_seconds: float,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None,
    ) -> dict[str, object]:
        """Revalidate and reserve while holding the sole entry-admission lock."""
        now = _now()
        readiness = self._commissioning_readiness_locked(
            at=now, commissioning_id=identity, capture=capture,
            ledger_evidence=ledger_evidence, ledger_blocker=ledger_blocker,
            preflight_duration_seconds=preflight_duration_seconds,
        )
        reasons = tuple(str(value) for value in readiness["blocking_reasons"])
        if reasons or ledger_preflight is None:
            return self._finalize_commissioning_arm_locked(
                identity=identity, request_id=request_id, now=now,
                readiness=readiness, reasons=reasons,
            )
        # Seal ledger admission as well as runtime admission across the final
        # proof and durable reservation.  The callback's barrier is reentrant
        # on the ledger ordering lock, while external receipts and observations
        # must order strictly after the reservation.
        with self.ledger.commissioning_authority_fence():
            final_capture = self._capture_commissioning_readiness_locked()
            final_evidence, final_blocker, final_duration = self._run_commissioning_ledger_preflight(
                identity, final_capture, ledger_preflight,
            )
            preflight_duration_seconds += final_duration
            now = _now()
            readiness = self._commissioning_readiness_locked(
                at=now, commissioning_id=identity, capture=final_capture,
                ledger_evidence=final_evidence, ledger_blocker=final_blocker,
                preflight_duration_seconds=preflight_duration_seconds,
            )
            reasons = tuple(str(value) for value in readiness["blocking_reasons"])
            return self._finalize_commissioning_arm_locked(
                identity=identity, request_id=request_id, now=now,
                readiness=readiness, reasons=reasons,
            )

    def _finalize_commissioning_arm_locked(
        self,
        *,
        identity: str,
        request_id: str | None,
        now: str,
        readiness: Mapping[str, object],
        reasons: tuple[str, ...],
    ) -> dict[str, object]:
        """Record the final decision and reserve while caller-owned fences hold."""
        allowed = not reasons
        risk_allowed, risk_reasons = self.risk.preflight(
            self._snapshot, at=now, commissioning=True,
        )
        if not risk_allowed:
            reasons = tuple(dict.fromkeys((*reasons, *risk_reasons)))
            allowed = False
        context = self._session_context
        self.ledger.append(
            "RISK_EVENT_COMMISSIONING_ARM_ATTEMPT",
            {
                **context.payload(), "allowed": allowed, "reason_codes": reasons,
                "authority_hash": canonical_hash(AUTHORITY.authority_payload()),
                "readiness_snapshot_hash": readiness["snapshot_hash"],
            },
            execution_session_id=self._execution_session_id(),
        )
        if not allowed:
            return {"armed": False, "reason_codes": reasons, "state": self._state.value}
        accepted_ledger_evidence = dict(readiness["ledger"])  # type: ignore[arg-type]
        self.ledger.append(
            "COMMISSIONING_PREFLIGHT_ACCEPTED",
            {
                **context.payload(), "commissioning_id": identity,
                "request_id": request_id,
                "readiness_snapshot_hash": readiness["snapshot_hash"],
                "commissioning_warmup_policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
                "reason": "ALL_PRODUCTION_COMMISSIONING_VALIDATORS_ACCEPTED",
            },
            identity="l3g-commissioning-preflight-accepted-" + identity,
            execution_session_id=self._execution_session_id(),
        )
        ownership = _CommissioningOwnership(
            identity,
            "l3g-commissioning-token-" + uuid4().hex,
            context, now, request_id=request_id, ledger_preflight=accepted_ledger_evidence,
        )
        self._commissioning_ownership = ownership
        self._entry_owner = PaperEntryOwner.COMMISSIONING
        self._advance_commissioning_authority_epoch()
        self._armed_session = PaperSessionArmGrant(
            context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
            context.session_generation, now,
            context.boundary_at("entry_cutoff").isoformat().replace("+00:00", "Z"),
        )
        self._entries_paused = False
        reservation_payload = self._ownership_payload(ownership, reason="COMMISSIONING_ARM_AFTER_PREFLIGHT")
        reservation_payload["ledger_preflight"] = accepted_ledger_evidence
        self.ledger.append(
            "COMMISSIONING_OWNERSHIP_RESERVED", reservation_payload,
            identity="l3g-commissioning-ownership-reserved-" + ownership.commissioning_id,
            execution_session_id=self._execution_session_id(),
        )
        self._transition(PaperRuntimeState.ARMED_FLAT, "COMMISSIONING_OWNERSHIP_RESERVED")
        accepted_ledger_evidence["authority_commit_checkpoint"] = (
            self.ledger.commissioning_authority_checkpoint()
        )
        ownership = replace(ownership, ledger_preflight=accepted_ledger_evidence)
        self._commissioning_ownership = ownership
        return {
            "armed": True, "commissioning": True, "reason_codes": ("COMMISSIONING_OWNERSHIP_RESERVED",),
            "state": self._state.value, "commissioning_id": ownership.commissioning_id,
            "commissioning_token": ownership.commissioning_token,
            "session_armed_state": "ARMED_" + context.session_kind.value,
            "arm_grant": self._armed_session.payload(),
            "ledger_preflight": accepted_ledger_evidence,
        }

    def _active_commissioning_replay_locked(
        self, commissioning_id: str, request_id: str,
    ) -> dict[str, object] | None:
        ownership = self._commissioning_ownership
        if ownership is None or ownership.commissioning_id != commissioning_id:
            return None
        return {
            "submitted": ownership.entry_consumed,
            "commissioning": True,
            "commissioning_id": commissioning_id,
            "request_id": request_id,
            "idempotent_replay": True,
            "entry_consumed": ownership.entry_consumed,
            "ownership_released": False,
            "decision_id": ownership.entry_decision_id,
            "state": self._state.value,
            "reason_codes": ("COMMISSIONING_START_ALREADY_ACCEPTED",),
        }

    def commissioning_start(
        self,
        request_id: str,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
    ) -> dict[str, object]:
        """Atomically preflight, reserve, arm, authorize, and submit exactly once."""
        if (
            not isinstance(request_id, str) or not 8 <= len(request_id) <= 128
            or any(not (character.isalnum() or character in "-_.:") for character in request_id)
        ):
            raise ValueError("Commissioning start requires an 8-128 character idempotency request ID.")
        commissioning_id = "l3g-commissioning-" + canonical_hash({"request_id": request_id})[:32]
        with self._lock:
            active_replay = self._active_commissioning_replay_locked(commissioning_id, request_id)
        if active_replay is not None:
            return active_replay
        prior = self.ledger.commissioning_ownership(commissioning_id)
        if prior is not None:
            record, consumed, released = prior
            payload = record.get("payload") if isinstance(record, Mapping) else None
            with self._lock:
                state = self._state.value
            return {
                "submitted": consumed,
                "commissioning": True,
                "commissioning_id": commissioning_id,
                "request_id": request_id,
                "idempotent_replay": True,
                "entry_consumed": consumed,
                "ownership_released": released,
                "decision_id": payload.get("entry_decision_id") if isinstance(payload, Mapping) else None,
                "state": state,
                "reason_codes": ("COMMISSIONING_START_ALREADY_ACCEPTED",),
            }
        with self._lock:
            capture = self._capture_commissioning_readiness_locked()
        ledger_evidence, ledger_blocker, duration = self._run_commissioning_ledger_preflight(
            commissioning_id, capture, ledger_preflight,
        )
        with self._lock:
            concurrent_replay = self._active_commissioning_replay_locked(commissioning_id, request_id)
            if concurrent_replay is not None:
                return concurrent_replay
            with self.ledger.commissioning_authority_fence():
                armed = self._commit_commissioning_arm_locked(
                    identity=commissioning_id, request_id=request_id, capture=capture,
                    ledger_evidence=ledger_evidence, ledger_blocker=ledger_blocker,
                    preflight_duration_seconds=duration, ledger_preflight=ledger_preflight,
                )
                if not armed.get("armed"):
                    return {
                        **armed, "submitted": False, "commissioning": True,
                        "commissioning_id": commissioning_id, "request_id": request_id,
                    }
                submitted = self.commission_entry(
                    str(armed["commissioning_id"]), str(armed["commissioning_token"]),
                )
                return {
                    **submitted,
                    "armed": True,
                    "atomic_start": True,
                    "request_id": request_id,
                    "ledger_preflight": armed.get("ledger_preflight"),
                }

    def commission_entry(self, commissioning_id: str, commissioning_token: str) -> dict[str, object]:
        """Submit one sealed, non-strategy commissioning entry through normal safety gates.

        This deliberately has no operator-supplied account, instrument, size, or
        direction input.  It is a one-contract Sim101 long solely to prove the
        authenticated execution lifecycle when Trader V0 has not naturally
        emitted an admissible decision.  The decision provenance is marked as
        commissioning-only and is excluded from strategy and scientific evidence.
        """
        with self._lock, self.ledger.commissioning_authority_fence():
            ownership = self._commissioning_ownership
            if ownership is None or self._entry_owner is not PaperEntryOwner.COMMISSIONING:
                return {"submitted": False, "reason_codes": ("COMMISSIONING_OWNERSHIP_REQUIRED",), "state": self._state.value}
            if not isinstance(commissioning_id, str) or not isinstance(commissioning_token, str) or (
                commissioning_id != ownership.commissioning_id or commissioning_token != ownership.commissioning_token
            ):
                return {"submitted": False, "reason_codes": ("COMMISSIONING_CREDENTIAL_MISMATCH",), "state": self._state.value}
            if ownership.entry_consumed:
                return {
                    "submitted": False, "reason_codes": ("COMMISSIONING_ENTRY_ALREADY_CONSUMED",),
                    "decision_id": ownership.entry_decision_id, "state": self._state.value,
                }
            if self._state is not PaperRuntimeState.ARMED_FLAT or self._entries_paused:
                return {"submitted": False, "reason_codes": ("PAPER_NOT_ARMED_FLAT",), "state": self._state.value}
            expected_checkpoint = (
                ownership.ledger_preflight.get("authority_commit_checkpoint")
                if isinstance(ownership.ledger_preflight, Mapping) else None
            )
            current_checkpoint = self.ledger.commissioning_authority_checkpoint()
            checkpoint_keys = (
                "policy_version", "ledger_identity", "ledger_epoch", "ledger_schema_version",
                "last_external_authority_sequence", "last_external_authority_hash",
            )
            if not isinstance(expected_checkpoint, Mapping) or any(
                expected_checkpoint.get(key) != current_checkpoint.get(key)
                for key in checkpoint_keys
            ):
                reasons = ("COMMISSIONING_LEDGER_AUTHORITY_CHANGED_AFTER_ARM",)
                self.ledger.append(
                    "RISK_EVENT_COMMISSIONING_PREFLIGHT",
                    {
                        **self._ownership_payload(ownership, reason=reasons[0]),
                        "commissioning": True, "strategy_generated": False,
                        "scientific_evidence": False, "allowed": False,
                        "reason_codes": reasons,
                    },
                    execution_session_id=self._execution_session_id(),
                )
                self._abort_unsubmitted_commissioning(reasons[0])
                return {"submitted": False, "reason_codes": reasons, "state": self._state.value}
            context = self._session_context
            now = _now()
            current = self._session_resolver.resolve(now, generation=context.session_generation).context
            if (
                context.session_kind is PaperSessionKind.OFF_SESSION
                or not self._ownership_context_matches(context, ownership.context)
                or not self._ownership_context_matches(current, ownership.context)
            ):
                reasons = ("COMMISSIONING_SESSION_IDENTITY_MISMATCH",)
                self.ledger.append(
                    "RISK_EVENT_COMMISSIONING_PREFLIGHT",
                    {**self._ownership_payload(ownership, reason=reasons[0]), "commissioning": True, "strategy_generated": False,
                     "scientific_evidence": False, "allowed": False, "reason_codes": reasons},
                    execution_session_id=self._execution_session_id(),
                )
                return {"submitted": False, "reason_codes": reasons, "state": self._state.value}
            if self._armed_session is None or not self._armed_session.valid_at(now):
                reasons = ("SESSION_ARM_EXPIRED",)
                self.ledger.append(
                    "RISK_EVENT_COMMISSIONING_PREFLIGHT",
                    {**self._ownership_payload(ownership, reason=reasons[0]), "commissioning": True, "strategy_generated": False,
                     "scientific_evidence": False, "allowed": False, "reason_codes": reasons},
                    execution_session_id=self._execution_session_id(),
                )
                return {"submitted": False, "reason_codes": reasons, "state": self._state.value}
            explicit_identity_reasons: list[str] = []
            if self._position is not PaperDirection.FLAT or self._position_quantity != 0:
                explicit_identity_reasons.append("COMMISSIONING_POSITION_NOT_FLAT")
            if self._snapshot.working_owned_orders != 0 or self._snapshot.working_entry_orders != 0:
                explicit_identity_reasons.append("COMMISSIONING_WORKING_ORDERS_PRESENT")
            if (
                self._snapshot.account_name != ACCOUNT_BINDING.account_name
                or self._snapshot.account_class != ACCOUNT_BINDING.account_class
                or self._snapshot.instrument != ACCOUNT_BINDING.instrument
            ):
                explicit_identity_reasons.append("COMMISSIONING_ACCOUNT_INSTRUMENT_MISMATCH")
            allowed, preflight_reasons = self.risk.preflight(
                self._snapshot, at=now, commissioning=True,
            )
            reasons = tuple(dict.fromkeys((*explicit_identity_reasons, *preflight_reasons)))
            allowed = allowed and not explicit_identity_reasons
            self.ledger.append(
                "RISK_EVENT_COMMISSIONING_PREFLIGHT",
                {**self._ownership_payload(ownership, reason="COMMISSIONING_ENTRY_PREFLIGHT"), "commissioning": True, "strategy_generated": False,
                 "scientific_evidence": False, "allowed": allowed, "reason_codes": reasons},
                execution_session_id=self._execution_session_id(),
            )
            if not allowed:
                self._abort_unsubmitted_commissioning("COMMISSIONING_ENTRY_PREFLIGHT_DENIED")
                return {"submitted": False, "reason_codes": reasons, "state": self._state.value}
            source = canonical_hash({"commissioning": True, "commissioning_id": ownership.commissioning_id, "at": now, "session_id": context.session_id})
            payload = {
                "paper_policy_id": POLICY.policy_id,
                "paper_policy_hash": POLICY.configuration_hash,
                "decision": PaperDecisionKind.LONG.value,
                "created_at": now,
                "session_kind": context.session_kind.value,
                "session_id": context.session_id,
                "trade_date": context.trade_date,
                "session_profile_hash": context.session_profile_hash,
                "session_generation": context.session_generation,
                "commissioning": True,
                "strategy_generated": False,
                "scientific_evidence": False,
            }
            decision = PaperDecision(
                deterministic_id("l3g-pd-", payload), POLICY.policy_id, POLICY.configuration_hash,
                PaperDecisionKind.LONG, now,
                (datetime.fromisoformat(now.replace("Z", "+00:00")) + timedelta(seconds=POLICY.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
                None, PaperDirection.LONG, Decimal("0"), {"commissioning": True},
                ("commissioning-operator-intent",),
                (max(0, int(self.policy.status().get("last_local_sequence") or 0)),),
                (source,), POLICY.sequence_authority, POLICY.book_completeness, False,
                "COMMISSIONING_OPERATOR_ENTRY", context.session_kind, context.session_id,
                context.trade_date, context.session_profile_hash, context.session_generation,
                True, False, False,
            )
            self.ledger.append(
                "COMMISSIONING_ENTRY_AUTHORIZED",
                {
                    **self._ownership_payload(ownership, reason="SINGLE_USE_ENTRY_AUTHORIZED"),
                    "decision_id": decision.paper_decision_id,
                    "direction": PaperDirection.LONG.value,
                    "quantity": 1,
                    "reference_market_snapshot": {
                        "bid": None if self._last_quote is None else str(self._last_quote[0]),
                        "ask": None if self._last_quote is None else str(self._last_quote[1]),
                        "last": None if self._last_trade is None else str(self._last_trade[0]),
                    },
                    "risk_snapshot_hash": canonical_hash({
                        key: str(value) for key, value in self._snapshot.__dict__.items()
                    }),
                    "created_at": now,
                    "expires_at": decision.expires_at,
                    "single_use": True,
                },
                identity="l3g-commissioning-entry-authorized-" + ownership.commissioning_id,
                execution_session_id=self._execution_session_id(),
            )
            self._last_decision = decision
            self.ledger.append(
                "DECISION", decision.payload(), identity=decision.paper_decision_id,
                occurred_at=decision.created_at, execution_session_id=self._execution_session_id(),
            )
            try:
                submitted = self._request_entry(decision)
            except _CommissioningAuthorizationExpired:
                self.ledger.append(
                    "INCIDENT_COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED",
                    self._ownership_payload(ownership, reason="COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED"),
                    identity="l3g-commissioning-entry-authorization-expired-" + ownership.commissioning_id,
                    execution_session_id=self._execution_session_id(),
                )
                self._abort_unsubmitted_commissioning("COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED")
                return {
                    "submitted": False,
                    "commissioning": True,
                    "commissioning_id": ownership.commissioning_id,
                    "reason_codes": ("COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED",),
                    "state": self._state.value,
                }
            except Exception:
                self.ledger.append(
                    "INCIDENT_COMMISSIONING_ENTRY_AMBIGUOUS",
                    self._ownership_payload(ownership, reason="COMMISSIONING_COMMAND_SEND_AMBIGUOUS"),
                    identity="l3g-commissioning-entry-ambiguous-" + ownership.commissioning_id,
                    execution_session_id=self._execution_session_id(),
                )
                raise
            if not submitted:
                self._abort_unsubmitted_commissioning("COMMISSIONING_ENTRY_REJECTED_BEFORE_COMMAND")
                return {"submitted": False, "reason_codes": ("COMMISSIONING_ENTRY_REJECTED_BEFORE_COMMAND",), "state": self._state.value}
            self.ledger.append(
                "COMMISSIONING_ENTRY_SUBMITTED",
                {
                    **self._ownership_payload(
                        self._commissioning_ownership or ownership,
                        reason="SINGLE_USE_ENTRY_SUBMITTED",
                    ),
                    "decision_id": decision.paper_decision_id,
                    "command_id": None if self._last_command is None else self._last_command.command_id,
                },
                identity="l3g-commissioning-entry-submitted-" + ownership.commissioning_id,
                execution_session_id=self._execution_session_id(),
            )
            return {
                "submitted": True,
                "commissioning": True,
                "commissioning_id": ownership.commissioning_id,
                "strategy_generated": False,
                "scientific_evidence": False,
                "decision_id": decision.paper_decision_id,
                "state": self._state.value,
            }

    def commission_exit(self) -> dict[str, object]:
        """Close the active explicit commissioning position with a normal owned exit."""
        with self._lock:
            if (
                self._state not in {PaperRuntimeState.LONG, PaperRuntimeState.SHORT}
                or self._commissioning_ownership is None
                or not self._commissioning_ownership.entry_consumed
            ):
                return {"submitted": False, "reason_codes": ("NO_ACTIVE_COMMISSIONING_POSITION",), "state": self._state.value}
            self._entries_paused = True
            self._armed_session = None
            self._disarm_after_flat = True
            self._request_exit("COMMISSIONING_OPERATOR_EXIT")
            return {
                "submitted": True,
                "commissioning": True,
                "strategy_generated": False,
                "scientific_evidence": False,
                "state": self._state.value,
            }

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
            if self._armed_session is None or not self._armed_session.valid_at(_now()):
                return {"resumed": False, "state": self._state.value, "reason": "SESSION_ARM_EXPIRED"}
            self._entries_paused = False
            target = PaperRuntimeState.ARMED_FLAT if self._position is PaperDirection.FLAT else PaperRuntimeState.LONG if self._position is PaperDirection.LONG else PaperRuntimeState.SHORT
            self._transition(target, "OPERATOR_RESUME_ENTRIES")
            return {"resumed": True, "state": self._state.value}

    def flatten_and_disarm(self) -> dict[str, object]:
        with self._lock:
            self._entries_paused = True
            self._armed_session = None
            self._disarm_after_flat = True
            self.ledger.append("RISK_EVENT_FLATTEN_AND_DISARM", {"position": self._position.value, "state": self._state.value})
            if self._position is PaperDirection.FLAT and (self._state is PaperRuntimeState.ENTRY_PENDING or self._snapshot.working_owned_orders > 0):
                self._cancel_pending_and_reconcile()
                return {"initiated": True, "flat_confirmed": False, "state": self._state.value}
            if self._position is PaperDirection.FLAT:
                if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED}:
                    self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_CONFIRMED_DISARM")
                self._disarm_after_flat = False
                ownership = self._commissioning_ownership
                if ownership is not None and not ownership.entry_consumed and self._snapshot.working_owned_orders == 0:
                    self._release_commissioning_ownership("OPERATOR_FLATTEN_DISARM_BEFORE_COMMISSIONING_ENTRY")
                elif self._entry_owner is PaperEntryOwner.STRATEGY:
                    self._entry_owner = PaperEntryOwner.NONE
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
            self._session_context.session_kind, self._session_context.session_id,
            self._session_context.trade_date, self._session_context.session_profile_hash,
            self._session_context.session_generation,
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
            context = self._session_context
            trade_risk = self._family_risk.get((context.trade_date, context.session_family), _TradeDateRisk())
            status_now = _now()
            arm_valid = self._armed_session is not None and self._armed_session.valid_at(status_now)
            ownership = self._commissioning_ownership
            loss_remaining = max(Decimal("0"), RISK_PROFILE.daily_loss_limit_dollars + min(Decimal("0"), trade_risk.realized_pnl + trade_risk.unrealized_pnl))
            next_context = PaperSessionResolver().next_valid_session(status_now, generation=context.session_generation)
            market_freshness = {
                "quote": self._freshness_gate(
                    self._snapshot.quote_observed_at, RISK_PROFILE.quote_maximum_age_seconds, status_now,
                ),
                "classified_trade": self._freshness_gate(
                    self._snapshot.classified_trade_observed_at,
                    RISK_PROFILE.classified_trade_maximum_age_seconds, status_now,
                ),
                "depth_mutation": self._freshness_gate(
                    self._snapshot.depth_mutation_observed_at,
                    RISK_PROFILE.depth_mutation_maximum_age_seconds, status_now,
                ),
            }
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
                "commissioning_readiness_snapshot_generation": self._commissioning_readiness_generation,
                "commissioning_authority_epoch": self._commissioning_authority_epoch,
                "commissioning_readiness_snapshot_token": self._last_commissioning_snapshot_token,
                "commissioning_preflight_duration_seconds": self._last_commissioning_preflight_duration_seconds,
                "commissioning_stale_snapshot_refusal_count": self._commissioning_stale_snapshot_refusals,
                "warning": "EXPERIMENTAL PAPER EXECUTION / NOT SCIENTIFICALLY COMMISSIONED / SIM101 ONLY / LIVE CAPITAL DENIED",
                "current_position": self._position.value,
                "current_quantity": self._position_quantity,
                "working_owned_orders": self._snapshot.working_owned_orders,
                "protective_stop_state": self._snapshot.protective_stop_state,
                "daily_realized_pnl": str(self._snapshot.daily_realized_pnl),
                "daily_unrealized_pnl": str(self._snapshot.daily_unrealized_pnl),
                "session_entries": self._snapshot.session_entry_count,
                "consecutive_losses": self._snapshot.consecutive_losses,
                "current_session": context.session_kind.value,
                "current_session_family": context.session_family.value,
                "current_session_id": context.session_id,
                "trade_date": context.trade_date,
                "session_generation": context.session_generation,
                "session_state": context.calendar_state.value,
                "entry_window": f"{context.entry_start}-{context.entry_cutoff} {context.timezone}",
                "entry_cutoff": context.entry_cutoff,
                "hard_flat_deadline": context.hard_flat_deadline,
                "session_armed_state": "ARMED_" + context.session_kind.value if arm_valid else "DISARMED",
                "session_arm_grant": None if self._armed_session is None else self._armed_session.payload(),
                "next_valid_session": None if next_context is None else {
                    "session_kind": next_context.session_kind.value,
                    "session_family": next_context.session_family.value,
                    "session_id": next_context.session_id,
                    "trade_date": next_context.trade_date,
                },
                "session_evidence_warmup": bool(self._snapshot.evidence_warmed),
                "strategy_evidence_warmed": bool(self._snapshot.evidence_warmed),
                "strategy_evidence_status": "ACTIVE" if self._snapshot.evidence_warmed else "INCOMPLETE",
                "commissioning_session_warmed": bool(self._snapshot.commissioning_session_warmed),
                "commissioning_warmup": {
                    "status": "WARMED" if self._snapshot.commissioning_session_warmed else "NOT_WARMED",
                    "warmed_at": self._commissioning_warmup_warmed_at,
                    "policy_hash": _COMMISSIONING_WARMUP_POLICY_HASH,
                    "required_families": {
                        family.value: {
                            "seen": family.value in self._commissioning_warmup_seen,
                            "provenance": self._commissioning_warmup_seen.get(family.value),
                        }
                        for family in _COMMISSIONING_REQUIRED_FAMILIES
                    },
                },
                "market_freshness": market_freshness,
                "continuity": {
                    "healthy": not self._snapshot.local_sequence_gap and not self._snapshot.depth_reset_recovery,
                    "local_sequence_gap": self._snapshot.local_sequence_gap,
                    "depth_reset_recovery": self._snapshot.depth_reset_recovery,
                    "local_bridge_healthy": self._snapshot.local_bridge_healthy,
                    "market_price_connected": self._snapshot.market_price_connected,
                },
                "session_support_scores": {
                    "bullish": str(self.policy.score(_now(), HypothesisKind.BULLISH_REVERSAL)[0]),
                    "bearish": str(self.policy.score(_now(), HypothesisKind.BEARISH_CONTINUATION)[0]),
                },
                "session_pnl": str(self._session_pnl.get(context.session_id, Decimal("0"))),
                "asia_session_pnl": str(sum(value for key, value in self._session_pnl.items() if ":ASIA:" in key and key.endswith(context.trade_date))),
                "new_york_session_pnl": str(sum(value for key, value in self._session_pnl.items() if (":NEW_YORK_RTH:" in key or ":NY_AFTER:" in key) and key.endswith(context.trade_date))),
                "family_cumulative_pnl": str(trade_risk.realized_pnl + trade_risk.unrealized_pnl),
                "combined_trade_date_pnl": str(sum(value.realized_pnl + value.unrealized_pnl for (date_key, _), value in self._family_risk.items() if date_key == context.trade_date)),
                "family_entry_count": trade_risk.entry_count,
                "combined_trade_date_loss_allowance_remaining": str(loss_remaining),
                "trade_date_entry_count": trade_risk.entry_count,
                "entry_owner": self._entry_owner.value,
                "commissioning_lifecycle": {
                    "classification": "EXPLICIT_PAPER_COMMISSIONING" if ownership is not None else "STRATEGY_GENERATED_PAPER",
                    "active": ownership is not None,
                    "commissioning_id": None if ownership is None else ownership.commissioning_id,
                    "entry_consumed": False if ownership is None else ownership.entry_consumed,
                    "recovered_after_restart": False if ownership is None else ownership.recovered_after_restart,
                    "decision_id": None if ownership is None else ownership.entry_decision_id,
                    "request_id": None if ownership is None else ownership.request_id,
                    "strategy_generated": ownership is None,
                    "scientific_evidence": False,
                },
                "last_paper_decision": None if self._last_decision is None else self._last_decision.payload(),
                "last_risk_result": risk.get("last_risk_result"),
                "last_command": None if self._last_command is None else self._last_command.payload(),
                "last_order_state": self._last_order_state,
                "last_execution": self._last_execution,
                "last_reconciliation": self._last_reconciliation,
                "lockout_or_fault_reason": self._fault_reason or risk.get("lockout_reason"),
                "last_commissioning_closure": self._last_commissioning_closure,
                "authority": AUTHORITY.authority_payload(),
                "policy": policy,
                "risk": risk,
                "transport": transport,
                "ledger": self.ledger.health_status(),
            }
