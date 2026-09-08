"""Lane III-G paper runtime, state machine, and observation fan-out."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import threading
import time
from typing import Callable, Mapping
from uuid import uuid4

from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from src.l3f_provider.tradovate_observation import StreamHealth
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    CAPABILITY,
    FIVE_MINUTE_ENTRY_PROFILE_VERSION,
    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
    BookCompleteness,
    PAPER_ACCOUNT_DAILY_LOSS_LIMIT_DOLLARS,
    PAPER_ACCOUNT_DAILY_LOSS_POLICY_ID,
    PAPER_ACCOUNT_DAILY_LOSS_POLICY_PROVENANCE,
    EvidenceFamily,
    ExecutionAction,
    FiveMinutePerpetualPaperPolicyArtifact,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEntryOwner,
    PaperExecutionCommand,
    PaperAuthorityBundle,
    PaperRuntimeState,
    PaperSessionArmGrant,
    SequenceAuthority,
    deterministic_id,
    resolve_paper_profile,
)
from .ledger import (
    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS,
    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS,
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    COMMISSIONING_READINESS_RECORD_SEMANTICS,
    COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
    HEALTH_AUTHORITY_OBSERVATION_PAYLOAD_KEY_SETS,
    HEALTH_AUTHORITY_OBSERVATION_SEMANTICS,
    deferred_capacity_allows_authority,
    LedgerCapacityError,
    PaperLedger,
)
from .ninjatrader_transport import (
    HEARTBEAT_WATCHDOG_SECONDS,
    ExecutionTransportStatus,
    NinjaTraderSim101PaperAdapter,
    PaperExecutionTransport,
)
from .policy import ExperimentalPaperPolicy
from .perpetual_startup_seed import (
    PERPETUAL_STARTUP_SEED_EXPORT_KIND,
    build_perpetual_boundary_bundle,
    build_perpetual_observation_proof,
    build_perpetual_startup_seed_core,
    perpetual_startup_seed_export_identity,
    perpetual_startup_seed_export_payload,
    validate_perpetual_startup_seed_artifact,
    validate_perpetual_startup_seed_proof,
    write_perpetual_startup_seed_artifact,
)
from .risk import PaperRiskAuthority, PaperRiskSnapshot
from .risk_continuity import (
    RISK_CONTINUITY_SNAPSHOT_SCHEMA,
    validate_risk_continuity_snapshot,
)
from .sessions import (
    PaperCalendarState,
    PaperSessionContext,
    PaperSessionFamily,
    PaperSessionKind,
    PaperSessionResolver,
    SESSION_PROFILES,
    UNSPECIFIED_OFF_SESSION_CONTEXT,
    context_from_identity,
    perpetual_exchange_blocker,
    session_catalog,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


_RISK_LOCKOUT_PENDING_SCHEMA = "lane-iii-risk-lockout-pending-v1"
_PERPETUAL_SIGNAL_SCHEMA = "lane-iii-five-minute-perpetual-signal-v3"
_PERPETUAL_SIGNAL_KIND = "RISK_EVENT_FIVE_MINUTE_DIRECTION_CHECKPOINT"
_PERPETUAL_SEED_CHECKPOINT_BINDING_SCHEMA = (
    "lane-iii-five-minute-perpetual-seed-checkpoint-binding-v1"
)
_PERPETUAL_SEED_IMPORT_KIND = (
    "RISK_EVENT_FIVE_MINUTE_PERPETUAL_STARTUP_SEED_IMPORTED"
)
_PERPETUAL_DEFERRED_ENTRY_REASONS = frozenset({
    "NO_CURRENT_EVENT_SESSION",
    "OFF_SESSION",
    "PROFILE_SESSION_MISMATCH",
    "HOLIDAY_SESSION_UNVERIFIED",
    "OUTSIDE_ENTRY_SESSION",
    "HARD_FLAT_DEADLINE",
    "SESSION_CLOSED",
    "EXCHANGE_DAILY_MAINTENANCE",
    "EXCHANGE_WEEKEND_CLOSED",
    "EXCHANGE_INTRADAY_HALT",
    "MARKET_OBSERVER_UNHEALTHY",
    "MARKET_BRIDGE_UNHEALTHY",
    "COMMISSIONING_SESSION_NOT_WARMED",
    "PAPER_EVIDENCE_NOT_WARMED",
    "PAPER_CONTINUITY_UNUSABLE",
    "LOCAL_SEQUENCE_GAP",
    "DEPTH_RESET_RECOVERY",
    "QUOTE_STALE",
    "CLASSIFIED_TRADE_STALE",
    "DEPTH_MUTATION_STALE",
})


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n"
        os.write(descriptor, encoded.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(path)


@dataclass
class _TradeDateRisk:
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    entry_count: int = 0


@dataclass
class _ProfileTradeDateRisk:
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


@dataclass(frozen=True)
class _OperationalPaperSession:
    """Backend-owned authority for one continuous operator paper session.

    This is intentionally distinct from the single-use commissioning
    credential.  Browser views only project this state; neither a refresh nor
    a mode switch can release it.
    """

    request_id: str
    started_at: str
    context: PaperSessionContext
    ledger_preflight: Mapping[str, object]
    stopping_reason: str | None = None


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

# The AddOn has an independent exact-instrument watchdog which acts after it
# stops receiving authenticated heartbeats.  Keep its transport connected long
# enough for its 250 ms poll plus dispatcher hand-off when Python has lost its
# own durable-command path.
_INDEPENDENT_WATCHDOG_GRACE_SECONDS = HEARTBEAT_WATCHDOG_SECONDS + 2.0
_DURABILITY_UNAVAILABLE_MARKER = "_l3g_durable_receipt_unavailable"
_WATCHDOG_SETTLED_RECONCILIATIONS_REQUIRED = 2


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
        # A startup verifier must be able to hold the paper ledger tip still
        # without disconnecting the sole native observation owner.  Shadow
        # observation remains live; only the paper sink is delayed, in exact
        # callback order, until the durable startup decision is recorded.
        self._paper_observations_paused = False
        self._paused_paper_observations: deque[NinjaTraderObservation] = deque()

    def begin_startup_paper_observation_pause(self) -> dict[str, object]:
        """Delay paper observation delivery across one startup-only boundary."""
        with self._lock:
            if self._paper_observations_paused:
                raise RuntimeError("STARTUP_PAPER_OBSERVATION_PAUSE_ALREADY_ACTIVE")
            if self._paused_paper_observations:
                raise RuntimeError("STARTUP_PAPER_OBSERVATION_PAUSE_NOT_DRAINED")
            self._paper_observations_paused = True
            return {"paused": True, "buffered_observations": 0}

    def end_startup_paper_observation_pause(self) -> dict[str, object]:
        """Replay held observations only after the startup decision is durable."""
        with self._lock:
            if not self._paper_observations_paused:
                raise RuntimeError("STARTUP_PAPER_OBSERVATION_PAUSE_NOT_ACTIVE")
            replayed = 0
            while self._paused_paper_observations:
                observation = self._paused_paper_observations.popleft()
                try:
                    self._paper_observation(observation)
                except Exception as exc:
                    try:
                        self._record_failure(
                            "EXPERIMENTAL_PAPER", "OBSERVATION", type(exc).__name__,
                        )
                    except Exception:
                        pass
                replayed += 1
            self._paper_observations_paused = False
            return {"paused": False, "replayed_observations": replayed}

    def _deliver(self, event: str, shadow: Callable[..., None], paper: Callable[..., None], *args: object) -> None:
        # One lock preserves admitted order across listener callbacks. Each
        # sink failure is isolated and durably recorded by the paper ledger.
        with self._lock:
            try:
                shadow(*args)
            except Exception as exc:
                try:
                    self._record_failure("SHADOW", event, type(exc).__name__)
                except Exception:
                    # A failed/sealed ledger cannot be allowed to turn an
                    # already-isolated listener failure into a fan-out crash.
                    pass
            try:
                paper(*args)
            except Exception as exc:
                try:
                    self._record_failure("EXPERIMENTAL_PAPER", event, type(exc).__name__)
                except Exception:
                    pass

    def on_observation(self, observation: NinjaTraderObservation) -> None:
        with self._lock:
            try:
                self._shadow_observation(observation)
            except Exception as exc:
                try:
                    self._record_failure("SHADOW", "OBSERVATION", type(exc).__name__)
                except Exception:
                    pass
            if self._paper_observations_paused:
                self._paused_paper_observations.append(observation)
                return
            try:
                self._paper_observation(observation)
            except Exception as exc:
                try:
                    self._record_failure(
                        "EXPERIMENTAL_PAPER", "OBSERVATION", type(exc).__name__,
                    )
                except Exception:
                    pass

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
        risk_continuity: Mapping[str, object] | None = None,
    ) -> None:
        if type(ledger) is not PaperLedger:
            raise ValueError("Paper runtime requires the exact durable ledger.")
        self.ledger = ledger
        self.policy = policy or ExperimentalPaperPolicy(ledger.policy)
        self.risk = risk or PaperRiskAuthority(profile=ledger.risk, policy=self.policy.artifact)
        if type(self.policy) is not ExperimentalPaperPolicy or type(self.risk) is not PaperRiskAuthority:
            raise ValueError("Paper runtime components must retain exact authority types.")
        if not (
            self.ledger.policy.configuration_hash
            == self.policy.artifact.configuration_hash
            == self.risk.policy.configuration_hash
        ):
            raise ValueError("Paper runtime, risk, and ledger policy identities must match.")
        if self.ledger.risk.configuration_hash != self.risk.profile.configuration_hash:
            raise ValueError("Paper runtime, risk, and ledger risk identities must match.")
        self.authority = PaperAuthorityBundle(
            self.policy.artifact, self.risk.profile, self.risk.binding, CAPABILITY,
        )
        self._perpetual_position_profile = (
            type(self.policy.artifact) is FiveMinutePerpetualPaperPolicyArtifact
            and self.policy.artifact.perpetual_position
        )
        # V1 may be the source of a controlled switch into the perpetual V2
        # profile.  Its detached V2 evaluator is passive: it owns no adapter,
        # grant, command, or account state and can only prepare authenticated
        # startup evidence from the same admitted market callbacks.
        self._perpetual_seed_shadow: ExperimentalPaperPolicy | None = (
            ExperimentalPaperPolicy(
                resolve_paper_profile(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                ).policy,
            )
            if self.policy.artifact.entry_profile_version
            == FIVE_MINUTE_ENTRY_PROFILE_VERSION
            else None
        )
        self._perpetual_seed_observations: dict[str, NinjaTraderObservation] = {}
        self._perpetual_seed_source_envelopes: dict[str, dict[str, object]] = {}
        self._perpetual_seed_boundary_chain: list[dict[str, object]] = []
        self._perpetual_seed_latest_bundle: dict[str, object] | None = None
        self._perpetual_seed_latest_non_tied_bundle: dict[str, object] | None = None
        self._perpetual_seed_shadow_fault: str | None = (
            "PERPETUAL_STARTUP_SEED_WAITING_FOR_COMPLETED_BOUNDARY"
            if self._perpetual_seed_shadow is not None else None
        )
        self._perpetual_seed_import: dict[str, object] | None = None
        self._active_perpetual_seed_checkpoint_binding: dict[str, object] | None = None
        self._lock = threading.RLock()
        self._state = PaperRuntimeState.DISABLED
        self._position = PaperDirection.FLAT
        self._position_quantity = 0
        self._entries_paused = False
        self._commissioning_ownership = self._load_unresolved_commissioning_ownership()
        self._operational_session: _OperationalPaperSession | None = None
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
        # Financial accounting is one account/exchange-trade-date envelope
        # across sessions and profiles. Entry caps and loss streaks retain the
        # profile which created them, so choosing another profile cannot erase
        # its counters or apply High confidence's one-entry cap globally.
        self._trade_date_risk: dict[str, _TradeDateRisk] = {}
        self._profile_trade_date_risk: dict[tuple[str, str], _ProfileTradeDateRisk] = {}
        self._session_entry_counts: dict[tuple[str, str], int] = {}
        self._session_risk_contexts: dict[tuple[str, str], PaperSessionContext] = {}
        self._session_pnl: dict[tuple[str, str], Decimal] = {}
        self._entry_session_context: PaperSessionContext | None = None
        self._hard_flat_started_for: tuple[str, int] | None = None
        self._last_decision: PaperDecision | None = None
        self._last_qualifying_entry_decision: PaperDecision | None = None
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
        # Authenticated broker fills remain physical truth even when their
        # risk-accounting context is corrupt. Such a lifecycle may be
        # flattened and reconciled but cannot create fresh entry authority.
        self._entry_accounting_ambiguous = False
        self._retain_safety_lockout_after_flat = False
        self._seen_native_execution_ids: set[str] = set()
        self._entry_execution_ids: set[str] = set()
        self._exit_execution_ids: set[str] = set()
        self._imported_execution_ids: set[str] = set()
        self._raw_execution_facts: dict[str, tuple[object, ...]] = {}
        self._entry_accounting_facts: dict[str, tuple[object, ...]] = {}
        self._exit_accounting_facts: dict[str, tuple[object, ...]] = {}
        self._protective_order_id: str | None = None
        self._lifecycle_realized_pnl = Decimal("0")
        self._post_entry_reconciliation_pending = False
        self._post_entry_reconciliation_complete = False
        self._post_entry_reconciliation_command_id: str | None = None
        # NinjaTrader can publish the protective WORKING callback before the
        # entry execution callback which caused the stop submission. Preserve
        # that authenticated ordering fact until the entry fill is accounted,
        # then require the same positioned aggregate reconciliation.
        self._early_protective_order_event: dict[str, object] | None = None
        self._post_exit_reconciliation_pending = False
        self._post_exit_position_flat_observed = False
        self._post_exit_order_terminal_observed = False
        self._pending_exit_command_id: str | None = None
        self._pending_five_minute_reversal: PaperDecision | None = None
        self._latest_five_minute_direction_checkpoint: dict[str, object] | None = None
        self._perpetual_flat_blocker: str | None = (
            "NO_COMPLETED_FIVE_MINUTE_SIGNAL"
            if self._perpetual_position_profile else None
        )
        self._perpetual_entry_attempted_checkpoint: str | None = None
        # A recovered checkpoint is only a candidate until the operational
        # Full-ledger proof covers its exact chain coordinate. A checkpoint
        # created in this process is linked to an already-durable source
        # DECISION and is immediately usable by the active operation.
        self._perpetual_signal_ledger_verified = False
        self._perpetual_signal_requires_start_verification = False
        self._perpetual_signal_fault: str | None = None
        self._last_five_minute_analysis: dict[str, object] | None = None
        self._command_sequence = 0
        # A venue callback can arrive synchronously while a durable exit is
        # being sent.  It must not create a second exit before EXIT_PENDING is
        # recorded.
        self._exit_submission_in_progress = False
        self._fault_reason: str | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._watchdog_failsafe_reason: str | None = None
        self._watchdog_failsafe_deadline_monotonic: float | None = None
        # A pre-stop flat snapshot cannot prove that a durably submitted
        # command will not materialize after Python stops heartbeating.  Keep
        # this latch until a *post-failsafe*, signed AddOn reconciliation
        # proves exact flat/no-owned-orders.  It deliberately survives the
        # runtime's terminal STOPPED state.
        self._watchdog_failsafe_requires_flat_confirmation = False
        self._watchdog_failsafe_activation_message_sequence: int | None = None
        self._watchdog_failsafe_safety_event_id: str | None = None
        self._watchdog_failsafe_flat_confirmation: dict[str, object] | None = None
        self._watchdog_failsafe_durable_confirmation: bool | None = None
        self._watchdog_failsafe_safety_event_durable: bool | None = None
        self._watchdog_failsafe_reconciliation_durable: bool | None = None
        self._watchdog_failsafe_last_settlement_sequence = 0
        self._watchdog_failsafe_settled_reconciliation_count = 0
        self._watchdog_failsafe_available: bool | None = None
        # Mirrors the AddOn's lifetime latch after one exact-provenance
        # authenticated session. A socket retirement does not erase the native
        # watchdog that remains resident in NinjaTrader.
        self._native_watchdog_authority_established = False
        self._execution_message_sequence = 0
        # A reconciliation recovery lease is deliberately distinct from an
        # entry owner.  It pauses every normal entry path while an operator
        # acknowledgement is followed by a server-owned, observation-only
        # native account probe.  It can never submit or cancel an order.
        self._reconciliation_recovery_lease_id: str | None = None
        self._transitions = 0
        self._commissioning_readiness_generation = 0
        self._commissioning_authority_epoch = 0
        self._commissioning_stale_snapshot_refusals = 0
        self._last_commissioning_preflight_duration_seconds: float | None = None
        self._last_commissioning_snapshot_token: str | None = None
        self._risk_continuity_fault: str | None = None
        try:
            # Validate (or first-time seed) the independent risk rollback
            # guard before start/arm can publish any authority transition.
            self.ledger.publish_risk_continuity_anchor()
            self._recover_risk_continuity(risk_continuity)
            if self._perpetual_position_profile:
                self._latest_five_minute_direction_checkpoint = (
                    self._load_latest_five_minute_direction_checkpoint()
                )
                if self._latest_five_minute_direction_checkpoint is not None:
                    self._perpetual_signal_requires_start_verification = True
                    self._perpetual_flat_blocker = "PERPETUAL_OPERATION_NOT_ACTIVE"
        except RuntimeError as error:
            # Accounting ambiguity revokes entry authority, but the runtime
            # must still be able to attach the bridge, reconcile, and flatten
            # a broker position whose existence cannot be inferred from disk.
            self._risk_continuity_fault = str(error)
            self._entries_paused = True
            self.risk.lock_out(self._risk_continuity_fault)
        self.risk.set_lockout_recorder(self._record_authority_lockout)
        if self._risk_continuity_fault is not None:
            self._record_authority_lockout(True, self._risk_continuity_fault, None)

    def _record_authority_lockout(
        self, locked_out: bool, reason: str | None, trade_date: str | None,
    ) -> None:
        payload = {
            "locked_out": locked_out,
            "lockout_reason": reason,
            "lockout_trade_date": trade_date if locked_out else None,
            "effective_trade_date": trade_date,
            "effect": "ENTRY_AUTHORITY_LOCKED" if locked_out else "DAILY_LOSS_LOCKOUT_EXPIRED",
        }
        kind = (
            "RISK_EVENT_AUTHORITY_LOCKOUT"
            if locked_out else "RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED"
        )
        marker = Path(str(self.ledger.path) + ".risk-authority-pending.json")
        marker_payload = {
            "schema": _RISK_LOCKOUT_PENDING_SCHEMA,
            "ledger_path": str(self.ledger.path),
            "kind": kind,
            "payload": payload,
            "created_at": _now(),
        }
        marker_written = False
        try:
            # The sidecar is a write-ahead fail-closed fence. If the ledger
            # append fails or the process dies before it is durable, startup
            # sees the marker and cannot restore fresh entry allowance.
            _atomic_json(marker, marker_payload)
            marker_written = True
        except Exception:
            # The authoritative ledger row is an independent durable path;
            # still attempt it when sidecar publication is unavailable.
            pass
        try:
            self.ledger.append(
                kind, payload,
                identity="l3g-risk-authority-state-" + canonical_hash(payload),
                execution_session_id=self._execution_session_id(),
            )
        except Exception:
            self._entries_paused = True
            self._risk_continuity_fault = "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED"
            if not marker_written:
                try:
                    _atomic_json(marker, marker_payload)
                    marker_written = True
                except Exception as marker_error:
                    # Do not return to PaperRiskAuthority: returning would
                    # publish an in-memory transition with no restart proof.
                    raise RuntimeError(
                        "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED"
                    ) from marker_error
            return
        else:
            try:
                marker.unlink(missing_ok=True)
            except OSError:
                # A leftover write-ahead marker deliberately blocks the next
                # start; do not claim complete continuity in this process.
                self._entries_paused = True
                self._risk_continuity_fault = "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED"

    @staticmethod
    def _perpetual_seed_wire(
        observation: NinjaTraderObservation,
    ) -> dict[str, object]:
        # The source observation and ordinary ledger envelope retain the exact
        # NinjaTrader text.  Only the detached seed proof is normalized to the
        # policy's Python UTC precision so its wire and source envelope remain
        # byte-identical under strict seed validation; callback sequence and
        # observation identity remain the ordering/provenance authorities.
        account = (
            None
            if observation.account_alias is None
            else {
                "alias": observation.account_alias,
                "class": None
                if observation.account_class is None
                else observation.account_class.value,
            }
        )
        return {
            "schema": L3F2_SCHEMA,
            "observation_id": observation.observation_id,
            "session_id": observation.session_id,
            "observation_type": observation.observation_type,
            "ninja_receipt_time": normalized_utc(
                observation.ninja_receipt_time, "Perpetual seed Ninja receipt time",
            ),
            "local_monotonic_sequence": observation.local_monotonic_sequence,
            "provider_timestamp": (
                None
                if observation.provider_timestamp is None
                else normalized_utc(
                    observation.provider_timestamp,
                    "Perpetual seed provider timestamp",
                )
            ),
            "provider_sequence": observation.provider_sequence,
            "exchange_timestamp": (
                None
                if observation.exchange_timestamp is None
                else normalized_utc(
                    observation.exchange_timestamp,
                    "Perpetual seed exchange timestamp",
                )
            ),
            "account": account,
            "payload": dict(observation.payload),
        }

    def _clear_perpetual_seed_capture_locked(self, reason: str) -> None:
        self._perpetual_seed_observations.clear()
        self._perpetual_seed_source_envelopes.clear()
        self._perpetual_seed_boundary_chain.clear()
        self._perpetual_seed_latest_bundle = None
        self._perpetual_seed_latest_non_tied_bundle = None
        self._perpetual_seed_shadow_fault = reason

    def _perpetual_seed_source_closure_locked(
        self,
        decision: PaperDecision,
        evidence: tuple[object, ...],
        fallback_id: str,
    ) -> list[str]:
        direct = {fallback_id, *decision.source_observation_ids}
        reference = decision.family_summary.get("decision_reference_observation_id")
        if isinstance(reference, str) and reference:
            direct.add(reference)
        for item in evidence:
            identifiers = getattr(item, "source_observation_ids", ())
            direct.update(str(identifier) for identifier in identifiers)
        closure = set(direct)
        pending = list(direct)
        while pending:
            identifier = pending.pop()
            observation = self._perpetual_seed_observations.get(identifier)
            if observation is None:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE")
            payload = observation.payload
            quote_id = payload.get("derivation_quote_observation_id")
            if isinstance(quote_id, str) and quote_id and quote_id not in closure:
                closure.add(quote_id)
                pending.append(quote_id)
        return sorted(closure)

    def _prune_perpetual_seed_capture_locked(self) -> None:
        """Retain only policy-reachable raw callbacks and sealed chain roots."""
        shadow = self._perpetual_seed_shadow
        if shadow is None:
            return
        roots = set(shadow.startup_seed_provenance_observation_ids())
        for bundle in self._perpetual_seed_boundary_chain:
            roots.update(str(item) for item in bundle["source_observation_ids"])
        closure = set(roots)
        pending = list(roots)
        while pending:
            identifier = pending.pop()
            observation = self._perpetual_seed_observations.get(identifier)
            envelope = self._perpetual_seed_source_envelopes.get(identifier)
            if observation is None or envelope is None:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE")
            dependency = observation.payload.get(
                "derivation_quote_observation_id",
            )
            if (
                isinstance(dependency, str)
                and dependency
                and dependency not in closure
            ):
                closure.add(dependency)
                pending.append(dependency)
        for identifier in tuple(self._perpetual_seed_observations):
            if identifier not in closure:
                self._perpetual_seed_observations.pop(identifier, None)
                self._perpetual_seed_source_envelopes.pop(identifier, None)

    def _capture_perpetual_seed_boundary_locked(
        self, boundary_decision: PaperDecision,
    ) -> None:
        shadow = self._perpetual_seed_shadow
        if shadow is None:
            return
        summary = boundary_decision.family_summary
        bias = summary.get("bias")
        if bias not in {"LONG", "SHORT", "TIE"}:
            return
        if summary.get("missed_boundary_count") != 0:
            shadow.reset(
                "PERPETUAL_STARTUP_SEED_BOUNDARY_CONTINUITY_UNPROVEN",
            )
            self._clear_perpetual_seed_capture_locked(
                "PERPETUAL_STARTUP_SEED_BOUNDARY_CONTINUITY_UNPROVEN"
            )
            return
        close = summary.get("candle_close_utc")
        opened = summary.get("candle_open_utc")
        if not isinstance(close, str) or not isinstance(opened, str):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_INVALID")
        closed = datetime.fromisoformat(close.replace("Z", "+00:00"))
        candidates = [
            observation
            for observation in self._perpetual_seed_observations.values()
            if datetime.fromisoformat(
                observation.ninja_receipt_time.replace("Z", "+00:00"),
            ) < closed
        ]
        if not candidates:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_FALLBACK_UNAVAILABLE")
        fallback = max(
            candidates,
            key=lambda observation: (
                datetime.fromisoformat(
                    observation.ninja_receipt_time.replace("Z", "+00:00"),
                ),
                observation.local_monotonic_sequence,
                observation.observation_id,
            ),
        )
        boundary = {
            "candle_open_utc": opened,
            "candle_close_utc": close,
            "decision_observed_at": boundary_decision.created_at,
            "decision_latency_ms": max(
                0,
                int((
                    datetime.fromisoformat(
                        boundary_decision.created_at.replace("Z", "+00:00"),
                    ) - closed
                ).total_seconds() * 1000),
            ),
            "missed_boundary_count": 0,
            "decision_interval_seconds": shadow.artifact.decision_interval_seconds,
            "decision_clock": shadow.artifact.decision_clock,
            "startup_reconstruction": True,
        }
        startup_decision = shadow._evaluate_five_minute_boundary(
            fallback,
            boundary,
            current_position=PaperDirection.FLAT,
            pending_order=False,
            decision_at=boundary_decision.created_at,
            source_at=close,
            prior_non_tied_available=(
                self._perpetual_seed_latest_non_tied_bundle is not None
            ),
        )
        if startup_decision.family_summary.get("bias") != bias:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_REEVALUATION_MISMATCH")
        active_evidence = shadow.active_evidence(close)
        source_ids = self._perpetual_seed_source_closure_locked(
            startup_decision, active_evidence, fallback.observation_id,
        )
        bundle = build_perpetual_boundary_bundle(
            candle_open_utc=opened,
            candle_close_utc=close,
            decision_observed_at=startup_decision.created_at,
            bias=str(bias),
            fallback_observation_id=fallback.observation_id,
            session_context=shadow.session_context.payload(),
            evidence=[item.payload() for item in active_evidence],
            decision=startup_decision.payload(),
            source_observation_ids=source_ids,
        )
        if bias in {"LONG", "SHORT"}:
            self._perpetual_seed_boundary_chain = [bundle]
            self._perpetual_seed_latest_non_tied_bundle = bundle
        else:
            if (
                self._perpetual_seed_latest_non_tied_bundle is None
                or not self._perpetual_seed_boundary_chain
            ):
                self._perpetual_seed_latest_bundle = bundle
                self._perpetual_seed_shadow_fault = (
                    "PERPETUAL_STARTUP_SEED_TIE_WITHOUT_NON_TIED_SIGNAL"
                )
                return
            prior = self._perpetual_seed_boundary_chain[-1]
            if prior.get("candle_close_utc") != opened:
                self._clear_perpetual_seed_capture_locked(
                    "PERPETUAL_STARTUP_SEED_BOUNDARY_CONTINUITY_UNPROVEN",
                )
                return
            self._perpetual_seed_boundary_chain.append(bundle)
        self._perpetual_seed_latest_bundle = bundle
        self._perpetual_seed_shadow_fault = None

    def _ingest_perpetual_seed_shadow_locked(
        self,
        observation: NinjaTraderObservation,
        context: PaperSessionContext,
        source_envelope: Mapping[str, object],
    ) -> None:
        shadow = self._perpetual_seed_shadow
        if shadow is None:
            return
        try:
            prior_context = shadow.session_context
            context_identity_changed = (
                prior_context.session_id,
                prior_context.session_generation,
            ) != (context.session_id, context.session_generation)
            routine_session_rollover = (
                context_identity_changed
                and prior_context.session_id
                != UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
                and prior_context.session_id != context.session_id
            )
            # Paper session labels are provenance domains, not trading windows
            # for the perpetual profile.  Seal the boundary which ended at a
            # routine label transition while the old domain and all of its
            # pre-close facts are still active.  The subsequent policy ingest
            # may then clear provisional evidence for the new label without
            # erasing the already-completed directional chain.
            if routine_session_rollover:
                rollover_decision = shadow.evaluate_latest_completed_on_start(
                    observation.ninja_receipt_time,
                    current_position=PaperDirection.FLAT,
                    pending_order=False,
                    prior_non_tied_available=(
                        self._perpetual_seed_latest_non_tied_bundle is not None
                    ),
                )
                if rollover_decision is not None:
                    self._capture_perpetual_seed_boundary_locked(
                        rollover_decision,
                    )
            if observation.observation_type in {"QUOTE", "TRADE", "DEPTH"}:
                # Retain the already-admitted immutable inputs, not a hashed
                # proof for every high-rate callback. Proofs are materialized
                # only for the tiny reachable closure at export time.
                self._perpetual_seed_observations[
                    observation.observation_id
                ] = observation
                self._perpetual_seed_source_envelopes[
                    observation.observation_id
                ] = dict(source_envelope)
            reset_before = shadow.reset_count()
            decision = shadow.ingest_runtime(
                observation,
                current_position=PaperDirection.FLAT,
                pending_order=False,
                session_context=context,
            )
            reset_delta = shadow.reset_count() - reset_before
            expected_context_resets = 1 if routine_session_rollover else 0
            if reset_delta != expected_context_resets:
                shadow.reset(
                    "PERPETUAL_STARTUP_SEED_WAITING_AFTER_CONTINUITY_RESET",
                )
                self._clear_perpetual_seed_capture_locked(
                    "PERPETUAL_STARTUP_SEED_WAITING_AFTER_CONTINUITY_RESET",
                )
                return
            if decision is not None:
                self._capture_perpetual_seed_boundary_locked(decision)
            self._prune_perpetual_seed_capture_locked()
        except Exception as error:
            shadow.reset("PERPETUAL_STARTUP_SEED_CAPTURE_FAILED")
            reason = str(error)
            self._clear_perpetual_seed_capture_locked(
                reason
                if reason and reason == reason.upper()
                else "PERPETUAL_STARTUP_SEED_CAPTURE_FAILED:"
                + type(error).__name__.upper(),
            )

    def export_perpetual_startup_seed(
        self, operation_id: str, artifact_path: str | Path,
    ) -> dict[str, object]:
        """Seal V1's passive V2 evaluation at an exact flat switch fence."""
        with self._lock:
            if self._perpetual_seed_shadow is None:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_PROFILE_INVALID")
            snapshot = self._snapshot
            if not (
                self._position is PaperDirection.FLAT
                and self._position_quantity == 0
                and snapshot.current_position is PaperDirection.FLAT
                and snapshot.current_position_quantity == 0
                and snapshot.working_owned_orders == 0
                and snapshot.working_entry_orders == 0
                and not snapshot.foreign_activity
                and snapshot.position_snapshot_complete
                and snapshot.order_snapshot_complete
                and snapshot.reconciliation_current
                and not snapshot.unresolved_command
                and not snapshot.unresolved_native_order
                and not snapshot.unresolved_execution
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_NOT_EXACT_FLAT")
            now = _now()
            moment = datetime.fromisoformat(now.replace("Z", "+00:00"))
            interval = self._perpetual_seed_shadow.artifact.decision_interval_seconds
            boundary = datetime.fromtimestamp(
                int(moment.timestamp()) - int(moment.timestamp()) % interval,
                tz=timezone.utc,
            ).isoformat().replace("+00:00", "Z")
            latest = self._perpetual_seed_latest_bundle
            non_tied = self._perpetual_seed_latest_non_tied_bundle
            if latest is None or latest.get("candle_close_utc") != boundary:
                raise RuntimeError(
                    self._perpetual_seed_shadow_fault
                    or "PERPETUAL_STARTUP_SEED_CURRENT_BOUNDARY_UNAVAILABLE",
                )
            if non_tied is None:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_NON_TIED_UNAVAILABLE")
            chain = tuple(self._perpetual_seed_boundary_chain)
            if (
                not chain
                or chain[0].get("bundle_sha256")
                != non_tied.get("bundle_sha256")
                or chain[-1].get("bundle_sha256")
                != latest.get("bundle_sha256")
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_CHAIN_UNAVAILABLE")
            identifiers = {
                str(identifier)
                for bundle in chain
                for identifier in bundle["source_observation_ids"]
            }
            try:
                proofs = [
                    build_perpetual_observation_proof(
                        wire=self._perpetual_seed_wire(
                            self._perpetual_seed_observations[identifier],
                        ),
                        source_envelope=(
                            self._perpetual_seed_source_envelopes[identifier]
                        ),
                    )
                    for identifier in sorted(identifiers)
                ]
            except KeyError as error:
                raise RuntimeError(
                    "PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE",
                ) from error
            session_context = latest.get("session_context")
            if (
                not isinstance(session_context, Mapping)
                or session_context.get("session_family")
                != self._session_context.session_family.value
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_SESSION_MISMATCH")
            core = build_perpetual_startup_seed_core(
                operation_id=operation_id,
                created_at=now,
                source_ledger={
                    "path": str(self.ledger.path.resolve()),
                    "ledger_identity": self.ledger.ledger_identity,
                    "ledger_epoch": self.ledger.ledger_epoch,
                },
                current_five_minute_boundary_utc=boundary,
                latest_completed=latest,
                latest_non_tied=non_tied,
                boundary_chain=chain,
                observations=proofs,
            )
            export_payload = {
                **perpetual_startup_seed_export_payload(core),
                "session_family": self._session_context.session_family.value,
            }
            identity = perpetual_startup_seed_export_identity(core)
            execution_session_id = self._execution_session_id()
        self.ledger.append(
            PERPETUAL_STARTUP_SEED_EXPORT_KIND,
            export_payload,
            identity=identity,
            occurred_at=now,
            execution_session_id=execution_session_id,
        )
        export_record = self.ledger.record_by_identity(identity)
        if export_record is None:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_EXPORT_NOT_DURABLE")
        # The immutable core and its source-ledger anchor are fixed. Keep the
        # potentially multi-megabyte create/fsync path off the market/runtime
        # authority lock; a boundary rollover during this write is detected by
        # the supervisor's expected-at revalidation and fails closed.
        return write_perpetual_startup_seed_artifact(
            artifact_path,
            core=core,
            export_record=export_record,
        )

    @staticmethod
    def _perpetual_seed_decision(payload: Mapping[str, object]) -> PaperDecision:
        hypothesis = payload.get("hypothesis_kind")
        decision = PaperDecision(
            str(payload["paper_decision_id"]),
            str(payload["paper_policy_id"]),
            str(payload["paper_policy_hash"]),
            PaperDecisionKind(str(payload["decision"])),
            str(payload["created_at"]),
            str(payload["expires_at"]),
            None if hypothesis is None else HypothesisKind(str(hypothesis)),
            PaperDirection(str(payload["direction"])),
            Decimal(str(payload["relative_support"])),
            dict(payload["family_summary"]),  # type: ignore[arg-type]
            tuple(str(item) for item in payload["source_observation_ids"]),  # type: ignore[union-attr]
            tuple(int(item) for item in payload["source_local_sequences"]),  # type: ignore[union-attr]
            tuple(str(item) for item in payload["source_payload_hashes"]),  # type: ignore[union-attr]
            SequenceAuthority(str(payload["sequence_authority"])),
            BookCompleteness(str(payload["book_completeness"])),
            bool(payload["scientific_eligibility"]),
            str(payload["reason_code"]),
            PaperSessionKind(str(payload["session_kind"])),
            str(payload["session_id"]),
            str(payload["trade_date"]),
            str(payload["session_profile_hash"]),
            int(payload["session_generation"]),
            bool(payload["commissioning"]),
            bool(payload["strategy_generated"]),
            bool(payload["scientific_evidence"]),
        )
        if decision.payload() != dict(payload):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_INVALID")
        return decision

    def import_perpetual_startup_seed(
        self,
        artifact: Mapping[str, object],
        proof: Mapping[str, object],
        *,
        operation_id: str,
        manifest_sha256: str,
        expected_at: str | None = None,
    ) -> dict[str, object]:
        """Import only a fully verified detached V1-to-V2 startup signal."""
        at = expected_at or _now()
        seed_artifact = validate_perpetual_startup_seed_artifact(
            artifact, operation_id=operation_id, expected_at=at,
        )
        seed_proof = validate_perpetual_startup_seed_proof(
            proof,
            artifact=seed_artifact,
            operation_id=operation_id,
            expected_at=at,
        )
        if seed_proof.get("manifest_sha256") != manifest_sha256:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_MANIFEST_MISMATCH")
        with self._lock:
            if not self._perpetual_position_profile:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_TARGET_PROFILE_INVALID")
            if self._state is not PaperRuntimeState.DISABLED:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_IMPORT_TOO_LATE")
            if self._latest_five_minute_direction_checkpoint is not None:
                raise RuntimeError("PERPETUAL_STARTUP_SEED_TARGET_NOT_EMPTY")
            core = seed_artifact["core"]
            latest = core["latest_completed"]  # type: ignore[index]
            non_tied = core["latest_non_tied"]  # type: ignore[index]
            chain = core["boundary_chain"]  # type: ignore[index]
            latest_context = latest["session_context"]  # type: ignore[index]
            imported_context = context_from_identity(
                PaperSessionKind(str(latest_context["session_kind"])),
                str(latest_context["session_id"]),
                str(latest_context["trade_date"]),
                str(latest_context["session_profile_hash"]),
                int(latest_context["session_generation"]),
                calendar_state=PaperCalendarState(
                    str(latest_context["calendar_state"]),
                ),
            )
            if imported_context.payload() != dict(latest_context):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_SESSION_MISMATCH")
            self._session_context = imported_context
            self._session_generation = imported_context.session_generation
            self.policy._activate_session(imported_context)
            self.ledger.set_session_context(imported_context)
            # Seed import installs the current session without going through
            # _set_session_context(). Project that same identity into the risk
            # snapshot now; otherwise the first perpetual entry compares the
            # imported decision against the constructor's generation zero and
            # is permanently refused as SESSION_IDENTITY_MISMATCH.
            self._activate_risk_snapshot_context_locked(
                imported_context,
                reset_evidence=True,
            )
            if (
                not isinstance(chain, list)
                or not chain
                or chain[0] != non_tied
                or chain[-1] != latest
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_CHAIN_INVALID")
            bundles = chain
            bundle_contexts: list[PaperSessionContext] = []
            for bundle in bundles:
                bundle_context_payload = bundle["session_context"]
                bundle_context = context_from_identity(
                    PaperSessionKind(str(bundle_context_payload["session_kind"])),
                    str(bundle_context_payload["session_id"]),
                    str(bundle_context_payload["trade_date"]),
                    str(bundle_context_payload["session_profile_hash"]),
                    int(bundle_context_payload["session_generation"]),
                    calendar_state=PaperCalendarState(
                        str(bundle_context_payload["calendar_state"]),
                    ),
                )
                if bundle_context.payload() != dict(bundle_context_payload):
                    raise RuntimeError("PERPETUAL_STARTUP_SEED_SESSION_MISMATCH")
                bundle_contexts.append(bundle_context)
            export_binding = perpetual_startup_seed_export_payload(core)
            checkpoint_binding = {
                "schema": _PERPETUAL_SEED_CHECKPOINT_BINDING_SCHEMA,
                "operation_id": operation_id,
                "artifact_sha256": seed_artifact["artifact_sha256"],
                "proof_sha256": seed_proof["proof_sha256"],
                "manifest_sha256": manifest_sha256,
                "seed_core_sha256": core["seed_core_sha256"],  # type: ignore[index]
                "boundary_chain_sha256": export_binding[
                    "boundary_chain_sha256"
                ],
                "boundary_chain_count": export_binding[
                    "boundary_chain_count"
                ],
                "source_tip_sequence": seed_proof["shutdown_tip"]["tip_sequence"],  # type: ignore[index]
                "source_tip_sha256": seed_proof["shutdown_tip"]["tip_sha256"],  # type: ignore[index]
            }
            decisions = [
                self._perpetual_seed_decision(bundle["decision"])
                for bundle in bundles
            ]
            checkpoint_records: list[dict[str, object]] = []
            self._active_perpetual_seed_checkpoint_binding = checkpoint_binding
            try:
                for bundle, bundle_context, decision in zip(
                    bundles, bundle_contexts, decisions, strict=True,
                ):
                    # The direction chain may legitimately cross a routine
                    # paper-session label.  Append each historical component
                    # under its own immutable context, then restore the latest
                    # context before exposing the imported tip.
                    self.ledger.set_session_context(bundle_context)
                    for evidence in bundle["evidence"]:  # type: ignore[index]
                        evidence_id = str(evidence["evidence_id"])
                        if evidence_id in self._recorded_evidence:
                            continue
                        self.ledger.append(
                            "EVIDENCE",
                            evidence,
                            identity=evidence_id,
                            occurred_at=str(evidence["observed_at"]),
                            execution_session_id=self._execution_session_id(),
                        )
                        self._recorded_evidence.add(evidence_id)
                    self.ledger.append(
                        "DECISION",
                        decision.payload(),
                        identity=decision.paper_decision_id,
                        occurred_at=decision.created_at,
                        execution_session_id=self._execution_session_id(),
                    )
                    if not self._record_five_minute_direction_checkpoint_locked(decision):
                        raise RuntimeError(
                            self._perpetual_signal_fault
                            or "PERPETUAL_STARTUP_SEED_CHECKPOINT_FAILED",
                        )
                    checkpoint = self._latest_five_minute_direction_checkpoint
                    assert checkpoint is not None
                    checkpoint_records.append({
                        "checkpoint_identity": checkpoint["checkpoint_identity"],
                        "record_hash": checkpoint["record_hash"],
                    })
                self.ledger.set_session_context(imported_context)
                import_summary = {
                    "operation_id": operation_id,
                    "artifact_sha256": seed_artifact["artifact_sha256"],
                    "proof_sha256": seed_proof["proof_sha256"],
                    "manifest_sha256": manifest_sha256,
                    "source_tip_sequence": seed_proof["shutdown_tip"]["tip_sequence"],  # type: ignore[index]
                    "source_tip_sha256": seed_proof["shutdown_tip"]["tip_sha256"],  # type: ignore[index]
                    "imported_at": at,
                }
                self.ledger.append(
                    _PERPETUAL_SEED_IMPORT_KIND,
                    {
                        **import_summary,
                        "source_profile": core["source_profile"]["selection_key"],  # type: ignore[index]
                        "target_profile": core["target_profile"]["selection_key"],  # type: ignore[index]
                        "source_ledger": core["source_ledger"],  # type: ignore[index]
                        "seed_core_sha256": core["seed_core_sha256"],  # type: ignore[index]
                        "session_family": latest["session_context"]["session_family"],  # type: ignore[index]
                        "checkpoint_binding": checkpoint_binding,
                        "checkpoint_records": checkpoint_records,
                    },
                    identity="l3g-perpetual-startup-seed-import-"
                    + str(seed_artifact["artifact_sha256"])[:32],
                    occurred_at=at,
                    execution_session_id=self._execution_session_id(),
                )
            finally:
                self.ledger.set_session_context(imported_context)
                self._active_perpetual_seed_checkpoint_binding = None
            current_decision = decisions[-1]
            non_tied_decision = decisions[0]
            self._last_decision = current_decision
            self._last_qualifying_entry_decision = non_tied_decision
            self._last_five_minute_analysis = {
                "status": "VERIFIED_PROFILE_SWITCH_SEED_IMPORTED",
                **dict(current_decision.family_summary),
                "reason_code": current_decision.reason_code,
                "paper_decision_id": current_decision.paper_decision_id,
            }
            self._perpetual_signal_ledger_verified = True
            self._perpetual_signal_requires_start_verification = False
            self._perpetual_flat_blocker = "PERPETUAL_OPERATION_NOT_ACTIVE"
            self._perpetual_seed_import = import_summary
            return dict(self._perpetual_seed_import)

    def _load_latest_five_minute_direction_checkpoint(self) -> dict[str, object] | None:
        rows = self.ledger.recent_kind_records((_PERPETUAL_SIGNAL_KIND,), limit=1)
        if not rows:
            return None
        latest = self._validate_five_minute_direction_checkpoint(rows[0])
        # Walk to the directional root so a live tie layered on an imported
        # seed cannot hide an incomplete seed checkpoint deeper in the chain.
        self._perpetual_direction_chain_locked(latest)
        return latest

    def _validate_seed_checkpoint_binding_locked(
        self,
        binding: object,
        row: Mapping[str, object],
    ) -> None:
        if binding is None:
            return
        required = {
            "schema", "operation_id", "artifact_sha256", "proof_sha256",
            "manifest_sha256", "seed_core_sha256", "boundary_chain_sha256",
            "boundary_chain_count", "source_tip_sequence", "source_tip_sha256",
        }
        if not isinstance(binding, Mapping) or set(binding) != required:
            raise RuntimeError("FIVE_MINUTE_SEED_CHECKPOINT_BINDING_INVALID")
        hashes = (
            binding.get("artifact_sha256"), binding.get("proof_sha256"),
            binding.get("manifest_sha256"), binding.get("seed_core_sha256"),
            binding.get("boundary_chain_sha256"), binding.get("source_tip_sha256"),
        )
        operation_id = binding.get("operation_id")
        if (
            binding.get("schema") != _PERPETUAL_SEED_CHECKPOINT_BINDING_SCHEMA
            or not isinstance(operation_id, str)
            or not operation_id.startswith("profile-switch-")
            or len(operation_id) != len("profile-switch-") + 32
            or any(character not in "0123456789abcdef" for character in operation_id.removeprefix("profile-switch-"))
            or any(
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in hashes
            )
            or type(binding.get("boundary_chain_count")) is not int
            or int(binding["boundary_chain_count"]) <= 0
            or type(binding.get("source_tip_sequence")) is not int
            or int(binding["source_tip_sequence"]) <= 0
        ):
            raise RuntimeError("FIVE_MINUTE_SEED_CHECKPOINT_BINDING_INVALID")
        if self._active_perpetual_seed_checkpoint_binding == dict(binding):
            return
        marker_identity = (
            "l3g-perpetual-startup-seed-import-"
            + str(binding["artifact_sha256"])[:32]
        )
        marker_row = self.ledger.record_by_identity(marker_identity)
        marker_record = (
            None if marker_row is None else marker_row.get("record")
        )
        marker_payload = (
            marker_record.get("payload")
            if isinstance(marker_record, Mapping) else None
        )
        records = (
            marker_payload.get("checkpoint_records")
            if isinstance(marker_payload, Mapping) else None
        )
        checkpoint_identity = row.get("record", {}).get("identity") if isinstance(row.get("record"), Mapping) else None
        if (
            not isinstance(marker_record, Mapping)
            or marker_record.get("kind") != _PERPETUAL_SEED_IMPORT_KIND
            or not isinstance(marker_payload, Mapping)
            or marker_payload.get("checkpoint_binding") != dict(binding)
            or not isinstance(records, list)
            or {
                "checkpoint_identity": checkpoint_identity,
                "record_hash": row.get("record_hash"),
            } not in records
        ):
            raise RuntimeError("FIVE_MINUTE_SEED_IMPORT_COMPLETION_MISSING")

    def _validate_five_minute_direction_checkpoint(
        self, row: Mapping[str, object],
    ) -> dict[str, object]:
        try:
            sequence = row["ledger_sequence"]
            record_hash = row["record_hash"]
            record = row["record"]
            if type(sequence) is not int or sequence <= 0:
                raise ValueError
            if (
                not isinstance(record_hash, str)
                or len(record_hash) != 64
                or any(character not in "0123456789abcdef" for character in record_hash)
            ):
                raise ValueError
            if not isinstance(record, Mapping) or record.get("kind") != _PERPETUAL_SIGNAL_KIND:
                raise ValueError
            if (
                record.get("paper_policy_hash") != self.policy.artifact.configuration_hash
                or record.get("risk_profile_hash") != self.risk.profile.configuration_hash
                or record.get("entry_profile_version")
                != self.policy.artifact.entry_profile_version
            ):
                raise ValueError
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                raise ValueError
            required = {
                "schema", "paper_policy_id", "paper_policy_hash", "risk_profile_hash",
                "entry_profile_version", "direction", "boundary_bias",
                "candle_open_utc", "candle_close_utc", "source_decision_id",
                "source_decision_ledger_sequence", "source_decision_record_hash",
                "source_decision", "session_family", "prior_checkpoint_identity",
                "prior_checkpoint_ledger_sequence", "prior_checkpoint_record_hash",
                "prior_signal_hash", "startup_seed_binding", "signal_hash",
            }
            if set(payload) != required:
                raise ValueError
            direction = str(payload.get("direction", ""))
            boundary_bias = str(payload.get("boundary_bias", ""))
            directions = {PaperDirection.LONG.value, PaperDirection.SHORT.value}
            if direction not in directions or boundary_bias not in directions | {"TIE"}:
                raise ValueError
            source = payload.get("source_decision")
            if not isinstance(source, Mapping):
                raise ValueError
            summary = source.get("family_summary")
            if not isinstance(summary, Mapping):
                raise ValueError
            expected_shapes = {
                PaperDirection.LONG.value: {
                    (PaperDecisionKind.LONG.value, "FIVE_MINUTE_ENTER_LONG", "ENTER"),
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_HOLD_LONG", "HOLD"),
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_PENDING_ORDER_LONG", "PENDING"),
                    (PaperDecisionKind.EXIT.value, "FIVE_MINUTE_REVERSE_TO_LONG", "REVERSE"),
                },
                PaperDirection.SHORT.value: {
                    (PaperDecisionKind.SHORT.value, "FIVE_MINUTE_ENTER_SHORT", "ENTER"),
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_HOLD_SHORT", "HOLD"),
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_PENDING_ORDER_SHORT", "PENDING"),
                    (PaperDecisionKind.EXIT.value, "FIVE_MINUTE_REVERSE_TO_SHORT", "REVERSE"),
                },
            }
            shape = (str(source.get("decision", "")), str(source.get("reason_code", "")), str(summary.get("action", "")))
            if boundary_bias == "TIE":
                if shape not in {
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_BIAS_TIE_HOLD", "HOLD"),
                    (PaperDecisionKind.NO_TRADE.value, "FIVE_MINUTE_BIAS_TIE_FLAT", "BLOCKED"),
                }:
                    raise ValueError
            elif boundary_bias != direction or shape not in expected_shapes[direction]:
                raise ValueError
            if (
                payload.get("schema") != _PERPETUAL_SIGNAL_SCHEMA
                or payload.get("paper_policy_id") != self.policy.artifact.policy_id
                or payload.get("paper_policy_hash") != self.policy.artifact.configuration_hash
                or payload.get("risk_profile_hash") != self.risk.profile.configuration_hash
                or payload.get("entry_profile_version")
                != self.policy.artifact.entry_profile_version
                or payload.get("source_decision_id") != source.get("paper_decision_id")
                or payload.get("session_family") != source.get("session_family")
                or source.get("paper_policy_id") != self.policy.artifact.policy_id
                or source.get("paper_policy_hash") != self.policy.artifact.configuration_hash
                or source.get("commissioning") is not False
                or source.get("strategy_generated") is not True
                or source.get("scientific_evidence") is not False
                or source.get("scientific_eligibility") is not False
                or summary.get("bias") != boundary_bias
                or payload.get("candle_open_utc") != summary.get("candle_open_utc")
                or payload.get("candle_close_utc") != summary.get("candle_close_utc")
                or summary.get("missed_boundary_count") != 0
            ):
                raise ValueError
            source_sequence = payload.get("source_decision_ledger_sequence")
            source_record_hash = payload.get("source_decision_record_hash")
            if (
                type(source_sequence) is not int
                or not 0 < source_sequence < sequence
                or not isinstance(source_record_hash, str)
                or len(source_record_hash) != 64
                or any(character not in "0123456789abcdef" for character in source_record_hash)
            ):
                raise ValueError
            source_row = self.ledger.record_by_identity(str(payload["source_decision_id"]))
            source_record = None if source_row is None else source_row.get("record")
            if (
                source_row is None
                or source_row.get("ledger_sequence") != source_sequence
                or source_row.get("record_hash") != source_record_hash
                or not isinstance(source_record, Mapping)
                or source_record.get("kind") != "DECISION"
                or source_record.get("payload") != source
            ):
                raise ValueError
            source_ids = source.get("source_observation_ids")
            source_sequences = source.get("source_local_sequences")
            source_hashes = source.get("source_payload_hashes")
            if (
                not isinstance(source_ids, list) or not source_ids
                or not isinstance(source_sequences, list)
                or not isinstance(source_hashes, list)
                or not (len(source_ids) == len(source_sequences) == len(source_hashes))
            ):
                raise ValueError
            opened = datetime.fromisoformat(
                normalized_utc(str(payload["candle_open_utc"]), "Five-minute signal open").replace("Z", "+00:00")
            )
            closed = datetime.fromisoformat(
                normalized_utc(str(payload["candle_close_utc"]), "Five-minute signal close").replace("Z", "+00:00")
            )
            created = datetime.fromisoformat(
                normalized_utc(str(source["created_at"]), "Five-minute signal decision").replace("Z", "+00:00")
            )
            interval = self.policy.artifact.decision_interval_seconds
            if (
                closed - opened != timedelta(seconds=interval)
                or int(closed.timestamp()) % interval != 0
                or created < closed
            ):
                raise ValueError
            prior_fields = (
                payload.get("prior_checkpoint_identity"),
                payload.get("prior_checkpoint_ledger_sequence"),
                payload.get("prior_checkpoint_record_hash"),
                payload.get("prior_signal_hash"),
            )
            if boundary_bias == "TIE":
                prior_identity, prior_sequence, prior_record_hash, prior_signal_hash = prior_fields
                if (
                    not isinstance(prior_identity, str) or not prior_identity
                    or type(prior_sequence) is not int or not 0 < prior_sequence < sequence
                    or not isinstance(prior_record_hash, str) or len(prior_record_hash) != 64
                    or not isinstance(prior_signal_hash, str) or len(prior_signal_hash) != 64
                ):
                    raise ValueError
                prior_row = self.ledger.record_by_identity(prior_identity)
                prior_record = None if prior_row is None else prior_row.get("record")
                prior_payload = prior_record.get("payload") if isinstance(prior_record, Mapping) else None
                if (
                    prior_row is None
                    or prior_row.get("ledger_sequence") != prior_sequence
                    or prior_row.get("record_hash") != prior_record_hash
                    or not isinstance(prior_record, Mapping)
                    or prior_record.get("kind") != _PERPETUAL_SIGNAL_KIND
                    or not isinstance(prior_payload, Mapping)
                    or prior_payload.get("schema") != _PERPETUAL_SIGNAL_SCHEMA
                    or prior_payload.get("signal_hash") != prior_signal_hash
                    or prior_payload.get("direction") != direction
                    or prior_payload.get("candle_close_utc") != payload.get("candle_open_utc")
                ):
                    raise ValueError
            elif any(value is not None for value in prior_fields):
                raise ValueError
            base = {key: payload[key] for key in required if key != "signal_hash"}
            if payload.get("signal_hash") != canonical_hash(base):
                raise ValueError
            self._validate_seed_checkpoint_binding_locked(
                payload.get("startup_seed_binding"), row,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("FIVE_MINUTE_SIGNAL_CHECKPOINT_INVALID") from exc
        return {
            **dict(payload),
            "ledger_sequence": sequence,
            "record_hash": record_hash,
            "checkpoint_identity": record.get("identity"),
        }

    def _record_five_minute_direction_checkpoint_locked(
        self, decision: PaperDecision,
    ) -> bool:
        if not self._perpetual_position_profile:
            return True
        summary = decision.family_summary
        boundary_bias = summary.get("bias")
        candle_open = summary.get("candle_open_utc")
        candle_close = summary.get("candle_close_utc")
        if boundary_bias not in {
            PaperDirection.LONG.value, PaperDirection.SHORT.value, "TIE",
        }:
            return True
        if not isinstance(candle_open, str) or not isinstance(candle_close, str):
            self._perpetual_signal_fault = "FIVE_MINUTE_DIRECTIONAL_SIGNAL_MALFORMED"
            self._perpetual_flat_blocker = self._perpetual_signal_fault
            self._fail_closed_without_ledger_locked(self._perpetual_signal_fault)
            return False
        if summary.get("missed_boundary_count") != 0:
            self._perpetual_flat_blocker = "FIVE_MINUTE_BOUNDARY_CONTINUITY_UNPROVEN"
            # The decision was durably observed, but a skipped boundary means
            # it cannot become position authority. Refuse every entry/reversal
            # side effect until a complete boundary is checkpointed.
            return False
        prior = self._latest_five_minute_direction_checkpoint
        if boundary_bias == "TIE" and prior is None:
            self._perpetual_flat_blocker = "FIVE_MINUTE_TIE_WITHOUT_PRIOR_NON_TIED_SIGNAL"
            return True
        direction = (
            str(prior["direction"]) if boundary_bias == "TIE" and prior is not None
            else str(boundary_bias)
        )
        source = decision.payload()
        source_row = self.ledger.record_by_identity(decision.paper_decision_id)
        source_record = None if source_row is None else source_row.get("record")
        if (
            source_row is None
            or not isinstance(source_record, Mapping)
            or source_record.get("kind") != "DECISION"
            or source_record.get("payload") != source
        ):
            self._perpetual_signal_fault = "FIVE_MINUTE_SOURCE_DECISION_NOT_DURABLE"
            self._perpetual_flat_blocker = self._perpetual_signal_fault
            self._fail_closed_without_ledger_locked(self._perpetual_signal_fault)
            return False
        base: dict[str, object] = {
            "schema": _PERPETUAL_SIGNAL_SCHEMA,
            "paper_policy_id": self.policy.artifact.policy_id,
            "paper_policy_hash": self.policy.artifact.configuration_hash,
            "risk_profile_hash": self.risk.profile.configuration_hash,
            "entry_profile_version": self.policy.artifact.entry_profile_version,
            "direction": direction,
            "boundary_bias": boundary_bias,
            "candle_open_utc": candle_open,
            "candle_close_utc": candle_close,
            "source_decision_id": decision.paper_decision_id,
            "source_decision_ledger_sequence": source_row["ledger_sequence"],
            "source_decision_record_hash": source_row["record_hash"],
            "source_decision": source,
            # PaperLedger seals this same deterministic field into every
            # payload. Include it in the signed signal base so a legitimate
            # checkpoint validates byte-for-byte after durable enrichment.
            "session_family": source["session_family"],
            "prior_checkpoint_identity": (
                prior.get("checkpoint_identity") if boundary_bias == "TIE" and prior is not None else None
            ),
            "prior_checkpoint_ledger_sequence": (
                prior.get("ledger_sequence") if boundary_bias == "TIE" and prior is not None else None
            ),
            "prior_checkpoint_record_hash": (
                prior.get("record_hash") if boundary_bias == "TIE" and prior is not None else None
            ),
            "prior_signal_hash": (
                prior.get("signal_hash") if boundary_bias == "TIE" and prior is not None else None
            ),
            "startup_seed_binding": (
                None
                if self._active_perpetual_seed_checkpoint_binding is None
                else dict(self._active_perpetual_seed_checkpoint_binding)
            ),
        }
        payload = {**base, "signal_hash": canonical_hash(base)}
        identity = "l3g-five-minute-direction-" + canonical_hash({
            "paper_policy_hash": self.policy.artifact.configuration_hash,
            "candle_close_utc": candle_close,
        })[:32]
        try:
            self.ledger.append(
                _PERPETUAL_SIGNAL_KIND,
                payload,
                identity=identity,
                occurred_at=decision.created_at,
                execution_session_id=self._execution_session_id(),
            )
            latest = self.ledger.recent_kind_records((_PERPETUAL_SIGNAL_KIND,), limit=1)
            if not latest:
                raise RuntimeError("FIVE_MINUTE_SIGNAL_CHECKPOINT_MISSING_AFTER_APPEND")
            self._latest_five_minute_direction_checkpoint = (
                self._validate_five_minute_direction_checkpoint(latest[0])
            )
        except Exception as error:
            self._perpetual_signal_fault = (
                "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED:"
                + type(error).__name__
            )
            self._perpetual_flat_blocker = "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED"
            self._fail_closed_without_ledger_locked(self._perpetual_flat_blocker)
            return False
        self._perpetual_entry_attempted_checkpoint = None
        self._perpetual_signal_ledger_verified = True
        self._perpetual_signal_requires_start_verification = False
        self._perpetual_signal_fault = None
        self._perpetual_flat_blocker = None
        return True

    def _calculate_perpetual_startup_signal_locked(self, at: str) -> bool:
        """Durably calculate V2's latest closed boundary before any entry."""
        if not self._perpetual_position_profile:
            return False
        try:
            decision = self.policy.evaluate_latest_completed_on_start(
                at,
                current_position=self._position,
                pending_order=self._state in {
                    PaperRuntimeState.ENTRY_PENDING,
                    PaperRuntimeState.EXIT_PENDING,
                },
                prior_non_tied_available=(
                    self._latest_five_minute_direction_checkpoint is not None
                ),
            )
            if decision is None:
                return False
            boundary_at = decision.family_summary.get("candle_close_utc")
            if not isinstance(boundary_at, str):
                raise ValueError("PERPETUAL_STARTUP_BOUNDARY_MISSING")
            for evidence in self.policy.active_evidence(boundary_at):
                if evidence.evidence_id in self._recorded_evidence:
                    continue
                self.ledger.append(
                    "EVIDENCE",
                    evidence.payload(),
                    identity=evidence.evidence_id,
                    occurred_at=evidence.observed_at,
                    execution_session_id=self._execution_session_id(),
                )
                self._recorded_evidence.add(evidence.evidence_id)
            # Startup direction is never allowed to exist only in memory. The
            # source decision must commit before its restart-authority row.
            self.ledger.append(
                "DECISION",
                decision.payload(),
                identity=decision.paper_decision_id,
                occurred_at=decision.created_at,
                execution_session_id=self._execution_session_id(),
            )
            self._last_decision = decision
            if decision.decision in {
                PaperDecisionKind.LONG,
                PaperDecisionKind.SHORT,
            }:
                self._last_qualifying_entry_decision = decision
            self._last_five_minute_analysis = {
                "status": "STARTUP_BOUNDARY_EVALUATED",
                **dict(decision.family_summary),
                "reason_code": decision.reason_code,
                "paper_decision_id": decision.paper_decision_id,
            }
            if not self._record_five_minute_direction_checkpoint_locked(decision):
                return False
            return True
        except Exception as error:
            reason = (
                "PERPETUAL_STARTUP_SIGNAL_DURABILITY_FAILED:"
                + type(error).__name__
            )
            self._perpetual_flat_blocker = reason
            self._fail_closed_without_ledger_locked(reason)
            return False

    def _strategy_entry_context_authorized_locked(
        self, context: PaperSessionContext, at: str,
    ) -> bool:
        if (
            self._state not in {PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT}
            or self._entries_paused
        ):
            return False
        if self._perpetual_position_profile:
            return (
                self._operational_session is not None
                and not self._operational_session_is_stopping_locked()
                and self._ownership_context_matches(context, self._session_context)
                and perpetual_exchange_blocker(at, context) is None
            )
        return (
            self._armed_session is not None
            and self._armed_session.valid_at(_now())
            and self._armed_session.session_id == context.session_id
            and context.entry_permitted_at(
                datetime.fromisoformat(
                    normalized_utc(at, "Entry event time").replace("Z", "+00:00")
                )
            )
        )

    def _perpetual_position_blockers_locked(self, at: str | None = None) -> tuple[str, ...]:
        if not self._perpetual_position_profile:
            return ()
        now = normalized_utc(at or _now(), "Perpetual position readiness time")
        blockers: list[str] = []
        operational = self._operational_session
        if operational is None:
            blockers.append("PERPETUAL_OPERATION_NOT_ACTIVE")
        elif operational.stopping_reason is not None:
            blockers.append(operational.stopping_reason)
        if self._state is PaperRuntimeState.ENTRY_PENDING:
            blockers.append("ENTRY_PENDING")
        elif self._state is PaperRuntimeState.EXIT_PENDING:
            blockers.append("REVERSAL_OR_SAFETY_EXIT_PENDING")
        elif self._state is not PaperRuntimeState.PAPER_RUNNING:
            blockers.append("PERPETUAL_RUNTIME_NOT_RUNNING")
        if self._entries_paused:
            blockers.append(self._fault_reason or "ENTRIES_PAUSED")
        exchange_blocker = perpetual_exchange_blocker(now, self._session_context)
        if exchange_blocker is not None:
            blockers.append(exchange_blocker)
        if not self._snapshot.market_price_connected:
            blockers.append("MARKET_DATA_DISCONNECTED")
        elif not self._snapshot.local_bridge_healthy:
            blockers.append("LOCAL_OBSERVATION_BRIDGE_DISCONNECTED")
        transport = None if self._transport is None else self._transport.status()
        if transport is None or not transport.addon_provenance_valid:
            blockers.append("ADDON_BUILD_MISMATCH")
        blockers.extend(self.risk.preflight_reasons(self._snapshot, at=now))
        freshness = (
            (
                self._snapshot.quote_observed_at,
                self.risk.profile.quote_maximum_age_seconds,
                "QUOTE_STALE",
            ),
            (
                self._snapshot.classified_trade_observed_at,
                self.risk.profile.classified_trade_maximum_age_seconds,
                "CLASSIFIED_TRADE_STALE",
            ),
            (
                self._snapshot.depth_mutation_observed_at,
                self.risk.profile.depth_mutation_maximum_age_seconds,
                "DEPTH_MUTATION_STALE",
            ),
        )
        for source, maximum, reason in freshness:
            if self._freshness_gate(source, maximum, now)["fresh"] is not True:
                blockers.append(reason)
        if self._latest_five_minute_direction_checkpoint is None:
            blockers.append("NO_COMPLETED_FIVE_MINUTE_SIGNAL")
        elif not self._perpetual_signal_ledger_verified:
            blockers.append("FIVE_MINUTE_SIGNAL_LEDGER_UNVERIFIED")
        else:
            moment = datetime.fromisoformat(now.replace("Z", "+00:00"))
            interval = self.policy.artifact.decision_interval_seconds
            expected_close = datetime.fromtimestamp(
                int(moment.timestamp()) - (int(moment.timestamp()) % interval),
                tz=timezone.utc,
            ).isoformat().replace("+00:00", "Z")
            if self._latest_five_minute_direction_checkpoint.get("candle_close_utc") != expected_close:
                blockers.append("LATEST_COMPLETED_FIVE_MINUTE_SIGNAL_STALE")
        return tuple(dict.fromkeys(blockers))

    def _perpetual_direction_chain_locked(
        self, checkpoint: Mapping[str, object],
    ) -> tuple[dict[str, object], ...]:
        """Resolve and validate the non-tied root through the current tip."""
        newest_to_oldest: list[dict[str, object]] = [dict(checkpoint)]
        seen: set[str] = set()
        while newest_to_oldest[-1].get("boundary_bias") == "TIE":
            current = newest_to_oldest[-1]
            identity = current.get("checkpoint_identity")
            prior_identity = current.get("prior_checkpoint_identity")
            if (
                not isinstance(identity, str)
                or identity in seen
                or not isinstance(prior_identity, str)
                or not prior_identity
            ):
                raise RuntimeError("FIVE_MINUTE_SIGNAL_CHAIN_INVALID")
            seen.add(identity)
            row = self.ledger.record_by_identity(prior_identity)
            if row is None:
                raise RuntimeError("FIVE_MINUTE_SIGNAL_CHAIN_INCOMPLETE")
            newest_to_oldest.append(
                self._validate_five_minute_direction_checkpoint(row),
            )
            if len(newest_to_oldest) > 512:
                raise RuntimeError("FIVE_MINUTE_SIGNAL_CHAIN_LIMIT_EXCEEDED")
        chain = tuple(reversed(newest_to_oldest))
        root = chain[0]
        if (
            root.get("boundary_bias") not in {"LONG", "SHORT"}
            or root.get("direction") != checkpoint.get("direction")
        ):
            raise RuntimeError("FIVE_MINUTE_SIGNAL_DIRECTION_ROOT_INVALID")
        return chain

    def _maintain_perpetual_position_locked(self, trigger: str) -> bool:
        """Fill an authenticated flat only from the latest durable non-tied bias."""
        if not self._perpetual_position_profile or self._position is not PaperDirection.FLAT:
            return False
        blockers = self._perpetual_position_blockers_locked()
        if blockers:
            self._perpetual_flat_blocker = blockers[0]
            if blockers[0] not in {
                "NO_COMPLETED_FIVE_MINUTE_SIGNAL", "ENTRY_PENDING",
                "REVERSAL_OR_SAFETY_EXIT_PENDING",
            }:
                self._perpetual_entry_attempted_checkpoint = None
            return False
        checkpoint = self._latest_five_minute_direction_checkpoint
        assert checkpoint is not None
        checkpoint_hash = str(checkpoint["record_hash"])
        if self._perpetual_entry_attempted_checkpoint == checkpoint_hash:
            return False
        self._perpetual_entry_attempted_checkpoint = checkpoint_hash
        direction = PaperDirection(str(checkpoint["direction"]))
        kind = (
            PaperDecisionKind.LONG
            if direction is PaperDirection.LONG else PaperDecisionKind.SHORT
        )
        hypothesis = (
            HypothesisKind.BULLISH_REVERSAL
            if direction is PaperDirection.LONG
            else HypothesisKind.BEARISH_CONTINUATION
        )
        try:
            direction_chain = self._perpetual_direction_chain_locked(checkpoint)
        except RuntimeError as error:
            self._perpetual_flat_blocker = str(error)
            self._perpetual_signal_fault = str(error)
            self._perpetual_entry_attempted_checkpoint = None
            return False
        directional_root = direction_chain[0]
        source = directional_root["source_decision"]
        assert isinstance(source, Mapping)
        source_summary = source["family_summary"]
        assert isinstance(source_summary, Mapping)
        provenance: dict[str, tuple[int, str]] = {}
        for item in direction_chain:
            chain_source = item["source_decision"]
            assert isinstance(chain_source, Mapping)
            for identifier, sequence, payload_hash in zip(
                chain_source["source_observation_ids"],
                chain_source["source_local_sequences"],
                chain_source["source_payload_hashes"],
                strict=True,
            ):
                candidate = (int(sequence), str(payload_hash))
                known = provenance.get(str(identifier))
                if known is not None and known != candidate:
                    self._perpetual_flat_blocker = (
                        "FIVE_MINUTE_SIGNAL_PROVENANCE_CONFLICT"
                    )
                    self._perpetual_entry_attempted_checkpoint = None
                    return False
                provenance[str(identifier)] = candidate
        ordered_provenance = sorted(
            (
                (identifier, sequence, payload_hash)
                for identifier, (sequence, payload_hash) in provenance.items()
            ),
            key=lambda item: (item[1], item[0]),
        )
        created = _now()
        summary = {
            **dict(source_summary),
            "action": "ENTER_FROM_LATEST_NON_TIED",
            "prior_position": PaperDirection.FLAT.value,
            "target_position": direction.value,
            "perpetual_trigger": trigger,
            "source_signal_ledger_sequence": checkpoint["ledger_sequence"],
            "source_signal_record_hash": checkpoint_hash,
            "source_signal_hash": checkpoint["signal_hash"],
            "directional_source_signal_ledger_sequence": directional_root[
                "ledger_sequence"
            ],
            "directional_source_signal_record_hash": directional_root[
                "record_hash"
            ],
            "directional_source_signal_hash": directional_root["signal_hash"],
            "directional_source_decision_id": source["paper_decision_id"],
            "directional_source_candle_close_utc": directional_root[
                "candle_close_utc"
            ],
            "continuity_tip_candle_close_utc": checkpoint["candle_close_utc"],
            "continuity_tip_boundary_bias": checkpoint["boundary_bias"],
            "continuity_chain_length": len(direction_chain),
            "continuity_chain_sha256": canonical_hash([
                item["signal_hash"] for item in direction_chain
            ]),
        }
        seed = {
            "policy_hash": self.policy.artifact.configuration_hash,
            "checkpoint_hash": checkpoint_hash,
            "trigger": trigger,
            "created_at": created,
            "session_id": self._session_context.session_id,
            "session_generation": self._session_context.session_generation,
        }
        decision = PaperDecision(
            paper_decision_id=deterministic_id("l3g-pd-", seed),
            paper_policy_id=self.policy.artifact.policy_id,
            paper_policy_hash=self.policy.artifact.configuration_hash,
            decision=kind,
            created_at=created,
            expires_at=(
                datetime.fromisoformat(created.replace("Z", "+00:00"))
                + timedelta(seconds=self.policy.artifact.decision_ttl_seconds)
            ).isoformat().replace("+00:00", "Z"),
            hypothesis_kind=hypothesis,
            direction=direction,
            relative_support=Decimal(str(source["relative_support"])),
            family_summary=summary,
            source_observation_ids=tuple(item[0] for item in ordered_provenance),
            source_local_sequences=tuple(item[1] for item in ordered_provenance),
            source_payload_hashes=tuple(item[2] for item in ordered_provenance),
            sequence_authority=self.policy.artifact.sequence_authority,
            book_completeness=self.policy.artifact.book_completeness,
            scientific_eligibility=False,
            reason_code=f"FIVE_MINUTE_PERPETUAL_ENTER_{direction.value}",
            session_kind=self._session_context.session_kind,
            session_id=self._session_context.session_id,
            trade_date=self._session_context.trade_date,
            session_profile_hash=self._session_context.session_profile_hash,
            session_generation=self._session_context.session_generation,
            commissioning=False,
            strategy_generated=True,
            scientific_evidence=False,
        )
        try:
            self.ledger.append(
                "DECISION", decision.payload(), identity=decision.paper_decision_id,
                occurred_at=decision.created_at,
                execution_session_id=self._execution_session_id(),
            )
            self._last_decision = decision
            self._last_qualifying_entry_decision = decision
            submitted = self._request_entry_locked(decision)
        except Exception as error:
            self._perpetual_flat_blocker = (
                self._fault_reason
                if isinstance(self._fault_reason, str)
                and self._fault_reason.startswith("DURABLE_COMMAND_SEND_FAILED:")
                else self._bounded_exception_reason(
                    "PERPETUAL_ENTRY_DURABILITY_FAILED", error,
                )
            )
            self._fail_closed_without_ledger_locked(self._perpetual_flat_blocker)
            return False
        if submitted:
            self._perpetual_flat_blocker = "ENTRY_PENDING"
            return True
        # A clean atomic recheck can refuse after the earlier pure preflight.
        # With no command/order submitted, keep the same durable checkpoint
        # eligible for a later health/reconciliation callback.
        self._perpetual_entry_attempted_checkpoint = None
        risk_status = self.risk.status().get("last_risk_result")
        reasons = (
            risk_status.get("reason_codes")
            if isinstance(risk_status, Mapping) else None
        )
        self._perpetual_flat_blocker = (
            str(reasons[0])
            if isinstance(reasons, (list, tuple)) and reasons else "PERPETUAL_ENTRY_REFUSED"
        )
        return False

    def _align_perpetual_position_to_latest_signal_locked(self, trigger: str) -> bool:
        """Converge a proved V2 position to the newest durable direction."""
        if not self._perpetual_position_profile:
            return False
        if self._position is PaperDirection.FLAT:
            return self._maintain_perpetual_position_locked(trigger)
        checkpoint = self._latest_five_minute_direction_checkpoint
        if checkpoint is None:
            return False
        desired = PaperDirection(str(checkpoint["direction"]))
        if desired is self._position:
            return False
        if self._state not in {PaperRuntimeState.LONG, PaperRuntimeState.SHORT}:
            return False
        # A newer boundary can complete while the old-side entry is pending.
        # Once that fill and its protective order are independently proved,
        # immediately begin the ordinary exit -> signed flat -> opposite-entry
        # sequence rather than carrying the stale side to the next boundary.
        return self._request_exit(f"FIVE_MINUTE_REVERSE_TO_{desired.value}")

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
            "account": self.risk.binding.account_name,
            "account_class": self.risk.binding.account_class,
            "instrument": self.risk.binding.instrument,
            # The record envelope owns its event timestamp. Keeping this
            # lifecycle value stable makes a same-identity retry exact rather
            # than fabricating a conflict solely because wall time advanced.
            "reserved_at": ownership.reserved_at,
            "reason": reason,
        }

    def _operational_session_payload(self) -> dict[str, object] | None:
        session = self._operational_session
        if session is None:
            return None
        return {
            "active": True,
            "request_id": session.request_id,
            "started_at": session.started_at,
            "context": session.context.payload(),
            "stopping": session.stopping_reason is not None,
            "stopping_reason": session.stopping_reason,
            "ledger_preflight": dict(session.ledger_preflight),
        }

    def _operational_session_is_stopping_locked(self) -> bool:
        return self._operational_session is not None and self._operational_session.stopping_reason is not None

    def _request_operational_stop_locked(self, reason: str) -> None:
        """Seal new entries while retaining backend authority through settlement."""
        session = self._operational_session
        if session is None:
            return
        if session.stopping_reason is None:
            self._operational_session = replace(session, stopping_reason=reason)
            self._entries_paused = True
            self._armed_session = None
            self._disarm_after_flat = True

    def _complete_operational_stop_locked(self, reason: str) -> None:
        """Release paper-session authority only after a clean flat reconciliation."""
        session = self._operational_session
        if session is None:
            return
        if self._position is not PaperDirection.FLAT or self._snapshot.working_owned_orders:
            raise RuntimeError("Operational paper session cannot release before flat reconciliation.")
        self._entries_paused = True
        self._armed_session = None
        retain_lockout = (
            self._retain_safety_lockout_after_flat
            or self._risk_continuity_fault is not None
        )
        if retain_lockout and self._state is not PaperRuntimeState.LOCKED_OUT:
            self._transition(PaperRuntimeState.LOCKED_OUT, reason + "_ENTRY_LOCKOUT_RETAINED")
        elif not retain_lockout and self._state is not PaperRuntimeState.READY_DISARMED:
            self._transition(PaperRuntimeState.READY_DISARMED, reason)
        self.ledger.append(
            "SESSION_OPERATIONAL_PAPER_STOPPED",
            {
                **session.context.payload(),
                "request_id": session.request_id,
                "started_at": session.started_at,
                "reason": session.stopping_reason or reason,
                "final_position": "FLAT",
                "final_quantity": 0,
                "final_working_order_count": 0,
                "live_capital": "DENIED",
            },
            identity="l3g-operational-paper-stop-" + canonical_hash({
                "request_id": session.request_id,
                "started_at": session.started_at,
            }),
            execution_session_id=self._execution_session_id(),
        )
        self._export_five_minute_analysis_locked(session.context)
        self._operational_session = None
        self._disarm_after_flat = False

    def _export_five_minute_analysis_locked(self, context: PaperSessionContext) -> None:
        """Produce the immutable experiment log after exact flat stop evidence."""
        if self.policy.artifact.entry_profile_version != "BEELZEBUB_FIVE_MINUTE_BIAS_V1":
            return
        try:
            from .five_minute_analysis import export_session_analysis

            audit_value = self._runtime_identity.get("audit")
            if not isinstance(audit_value, str) or not audit_value or audit_value == "UNKNOWN":
                raise RuntimeError("RUNTIME_AUDIT_ROOT_UNAVAILABLE")
            result = export_session_analysis(
                self.ledger.path,
                Path(audit_value) / "five-minute-session-analysis",
                session_id=context.session_id,
            )
            self._last_five_minute_analysis = {"status": "EXPORTED", **result}
            self.ledger.append(
                "SESSION_FIVE_MINUTE_ANALYSIS_EXPORTED",
                {**context.payload(), **result},
                identity="l3g-five-minute-analysis-export-" + str(result["analysis_id"]),
                execution_session_id=self._execution_session_id(),
            )
        except Exception as error:
            self._last_five_minute_analysis = {
                "status": "FAILED", "error_type": type(error).__name__, "error": str(error),
                "session_id": context.session_id,
            }
            try:
                self.ledger.append(
                    "INCIDENT_FIVE_MINUTE_ANALYSIS_EXPORT_FAILED",
                    {
                        **context.payload(),
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                    identity="l3g-five-minute-analysis-failed-" + canonical_hash({
                        "session_id": context.session_id,
                        "session_generation": context.session_generation,
                        "error_type": type(error).__name__,
                    }),
                    execution_session_id=self._execution_session_id(),
                )
            except Exception:
                # Reporting never weakens the already completed flat/disarmed stop.
                pass

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
            # This is a no-authority observation attestation emitted from the
            # market callback path.  Keep it in the ordered deferred stream so
            # a continuity reset cannot globally drain that same live stream.
            # Commissioning barriers persist and classify it before any
            # readiness or authority decision is allowed to use the tail.
            try:
                receipt = self.ledger.append_commissioning_attestation_deferred(
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
            except LedgerCapacityError as error:
                self._pause_for_ledger_capacity_locked(error.capacity)
            else:
                if not self._deferred_admission_receipt_healthy_locked(receipt):
                    self._pause_for_ledger_capacity_locked(receipt)

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
        # Like the reset marker, this is a no-authority observation
        # attestation on the market callback path.  The commissioning barrier
        # provides its durability boundary without stalling socket receive.
        try:
            receipt = self.ledger.append_commissioning_attestation_deferred(
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
        except LedgerCapacityError as error:
            self._pause_for_ledger_capacity_locked(error.capacity)
        else:
            if not self._deferred_admission_receipt_healthy_locked(receipt):
                self._pause_for_ledger_capacity_locked(receipt)

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

    def _activate_risk_snapshot_context_locked(
        self, context: PaperSessionContext, *, reset_evidence: bool,
    ) -> None:
        """Project current identity without resetting a carried V2 risk epoch.

        A perpetual position can span an exchange trade-date boundary. Until
        signed flat reconciliation, its original trade-date budget remains the
        conservative account envelope; no unproven boundary mark is used to
        manufacture a daily reset. Once flat, the current context becomes the
        new risk epoch and an expired daily-only lock may clear durably.
        """
        profile = self.policy.artifact.entry_profile_version
        carried = (
            self._entry_session_context
            if self._perpetual_position_profile and self._entry_session_context is not None
            else None
        )
        risk_context = carried or context
        if carried is None:
            self.risk.clear_trade_date_limit_lockout(context.trade_date)
        trade_risk = self._trade_date_risk.setdefault(
            risk_context.trade_date, _TradeDateRisk(),
        )
        profile_risk = self._profile_trade_date_risk.setdefault(
            (risk_context.trade_date, profile), _ProfileTradeDateRisk(),
        )
        session_key = (context.session_id, profile)
        self._session_entry_counts.setdefault(session_key, 0)
        self._session_risk_contexts[session_key] = context
        changes: dict[str, object] = {
            "observed_at": _now(),
            "session_kind": context.session_kind,
            "session_id": context.session_id,
            "trade_date": context.trade_date,
            "session_profile_hash": context.session_profile_hash,
            "session_generation": context.session_generation,
            "session_entry_count": self._session_entry_counts.get(session_key, 0),
            "daily_realized_pnl": trade_risk.realized_pnl,
            "daily_unrealized_pnl": trade_risk.unrealized_pnl,
            "trade_date_entry_count": profile_risk.entry_count,
            "consecutive_losses": profile_risk.consecutive_losses,
        }
        if reset_evidence:
            changes.update({
                "evidence_warmed": False,
                "commissioning_session_warmed": False,
                "depth_reset_recovery": True,
            })
        self._snapshot = replace(self._snapshot, **changes)

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
            self._activate_risk_snapshot_context_locked(context, reset_evidence=True)
            self.ledger.append("SESSION_OPENED", {**context.payload(), "reason": reason}, identity="l3g-paper-session-open-" + canonical_hash(context.payload()))
        elif self._perpetual_position_profile:
            self._activate_risk_snapshot_context_locked(context, reset_evidence=True)
            self.ledger.append(
                "SESSION_OPENED", {**context.payload(), "reason": reason},
                identity="l3g-paper-session-open-" + canonical_hash(context.payload()),
            )
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
        self.ledger.append(
            "SESSION_CLOSED", {
                **context.payload(), "reason": reason,
                "session_realized_pnl": str(self._session_pnl.get(
                    (context.session_id, self.policy.artifact.entry_profile_version), Decimal("0"),
                )),
                "position": self._position.value, "working_owned_orders": self._snapshot.working_owned_orders,
            }, identity="l3g-paper-session-close-" + canonical_hash({**context.payload(), "reason": reason}),
        )
        if self._perpetual_position_profile:
            # This is an evidence-domain rollover only. It does not stop the
            # persistent operation, expire authority, or flatten a position.
            return
        self._armed_session = None
        self._entries_paused = True
        self._pending_five_minute_reversal = None
        self.policy.reset("SESSION_CLOSED")
        if self._operational_session is not None:
            self._request_operational_stop_locked("SCHEDULED_SESSION_CLOSE")
            if (
                self._position is PaperDirection.FLAT
                and self._snapshot.working_owned_orders == 0
                and self._snapshot.reconciliation_current
                and self._snapshot.position_snapshot_complete
                and self._snapshot.order_snapshot_complete
            ):
                self._complete_operational_stop_locked("SCHEDULED_SESSION_CLOSE_RECONCILED")
                return
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
        if self._perpetual_position_profile:
            return
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
        if (
            transport.policy.configuration_hash != self.policy.artifact.configuration_hash
            or transport.risk.configuration_hash != self.risk.profile.configuration_hash
        ):
            raise ValueError("Paper runtime and transport profile identities must match.")
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

    def _reconciliation_recovery_authority_locked(self) -> dict[str, object]:
        transport = None if self._transport is None else self._transport.status()
        risk = self.risk.status()
        return {
            "schema": "lane-iii-reconciliation-recovery-authority-v1",
            "entry_profile_version": self.policy.artifact.entry_profile_version,
            "runtime_state": self._state.value,
            "entries_paused": self._entries_paused,
            "entry_owner": self._entry_owner.value,
            "operational_owner_active": self._operational_session is not None,
            "runtime_position": self._position.value,
            "runtime_quantity": self._position_quantity,
            "account": self._snapshot.account_name,
            "environment": self._snapshot.account_class,
            "instrument": self._snapshot.instrument,
            "broker_position": self._snapshot.current_position.value,
            "broker_quantity": self._snapshot.current_position_quantity,
            "working_orders": self._snapshot.working_owned_orders,
            "working_entry_orders": self._snapshot.working_entry_orders,
            "position_snapshot_complete": self._snapshot.position_snapshot_complete,
            "order_snapshot_complete": self._snapshot.order_snapshot_complete,
            "foreign_activity": self._snapshot.foreign_activity,
            "protective_stop_state": self._snapshot.protective_stop_state,
            "reconciliation_current": self._snapshot.reconciliation_current,
            "unresolved_command": self._snapshot.unresolved_command,
            "unresolved_native_order": self._snapshot.unresolved_native_order,
            "unresolved_execution": self._snapshot.unresolved_execution,
            "risk_locked_out": risk.get("locked_out"),
            "risk_lockout_reason": risk.get("lockout_reason"),
            "risk_lockout_trade_date": risk.get("lockout_trade_date"),
            "risk_continuity_fault": self._risk_continuity_fault,
            "retained_safety_lockout": self._retain_safety_lockout_after_flat,
            "fault_reason": self._fault_reason,
            "transport_authenticated": False if transport is None else transport.authenticated_client,
            "transport_reconciled": False if transport is None else transport.reconciled,
            "transport_commands_sent": 0 if transport is None else transport.commands_sent,
            "transport_addon_provenance_valid": False if transport is None else transport.addon_provenance_valid,
            "execution_message_sequence": self._execution_message_sequence,
            "paper_only": True,
            "live_capital": "DENIED",
            "maximum_quantity": 1,
        }

    def reconciliation_recovery_authority(self) -> dict[str, object]:
        """Return the exact, caller-bindable state of the narrow recovery gate."""
        with self._lock:
            authority = self._reconciliation_recovery_authority_locked()
            return {**authority, "authority_state_hash": canonical_hash(authority)}

    @staticmethod
    def _reconciliation_recovery_safe_snapshot(authority: Mapping[str, object]) -> bool:
        return all((
            authority.get("entry_profile_version") == FIVE_MINUTE_ENTRY_PROFILE_VERSION,
            authority.get("runtime_state") == PaperRuntimeState.READY_DISARMED.value,
            authority.get("entry_owner") == PaperEntryOwner.NONE.value,
            authority.get("operational_owner_active") is False,
            authority.get("runtime_position") == PaperDirection.FLAT.value,
            authority.get("runtime_quantity") == 0,
            authority.get("account") == "Sim101",
            authority.get("environment") == "LOCAL_SIMULATION",
            authority.get("instrument") == "MNQ SEP26",
            authority.get("broker_position") == PaperDirection.FLAT.value,
            authority.get("broker_quantity") == 0,
            authority.get("working_orders") == 0,
            authority.get("working_entry_orders") == 0,
            authority.get("position_snapshot_complete") is True,
            authority.get("order_snapshot_complete") is True,
            authority.get("foreign_activity") is False,
            authority.get("protective_stop_state") == "NONE",
            authority.get("reconciliation_current") is True,
            authority.get("unresolved_command") is False,
            authority.get("unresolved_native_order") is False,
            authority.get("unresolved_execution") is False,
            authority.get("risk_locked_out") is True,
            authority.get("risk_lockout_reason") == "RECONCILIATION_BLOCKED",
            authority.get("risk_lockout_trade_date") is None,
            authority.get("risk_continuity_fault") is None,
            authority.get("retained_safety_lockout") is False,
            authority.get("fault_reason") in {None, "RECONCILIATION_BLOCKED"},
            authority.get("transport_authenticated") is True,
            authority.get("transport_reconciled") is True,
            authority.get("transport_addon_provenance_valid") is True,
            authority.get("paper_only") is True,
            authority.get("live_capital") == "DENIED",
            authority.get("maximum_quantity") == 1,
        ))

    def begin_reconciliation_recovery_lease(
        self, lease_id: str, expected_authority_state_hash: str,
    ) -> dict[str, object]:
        """Pause normal entry admission for one exact acknowledged recovery."""
        if not isinstance(lease_id, str) or not lease_id:
            raise ValueError("RECONCILIATION_RECOVERY_LEASE_ID_INVALID")
        with self._lock, self.ledger.commissioning_authority_fence():
            authority = self._reconciliation_recovery_authority_locked()
            authority_hash = canonical_hash(authority)
            if expected_authority_state_hash != authority_hash:
                raise RuntimeError("RECONCILIATION_RECOVERY_AUTHORITY_STATE_STALE")
            if not self._reconciliation_recovery_safe_snapshot(authority):
                raise RuntimeError("RECONCILIATION_RECOVERY_PREFLIGHT_BLOCKED")
            if self._reconciliation_recovery_lease_id not in {None, lease_id}:
                raise RuntimeError("RECONCILIATION_RECOVERY_LEASE_ACTIVE")
            self._reconciliation_recovery_lease_id = lease_id
            self._entries_paused = True
            return {
                "lease_id": lease_id,
                "authority_state_hash": authority_hash,
                "transport_commands_sent": authority["transport_commands_sent"],
            }

    def abort_reconciliation_recovery_lease(self, lease_id: str) -> None:
        """Release only the named lease; the historical lockout remains active."""
        with self._lock:
            if self._reconciliation_recovery_lease_id == lease_id:
                self._reconciliation_recovery_lease_id = None
                self._entries_paused = True

    @staticmethod
    def _recovery_reconciliation_payload_safe(payload: Mapping[str, object]) -> bool:
        return all((
            payload.get("account_name") == "Sim101",
            payload.get("account_class") == "LOCAL_SIMULATION",
            payload.get("instrument") == "MNQ SEP26",
            payload.get("position_quantity") == 0,
            payload.get("working_order_count") == 0,
            payload.get("working_entry_count") == 0,
            payload.get("position_snapshot_complete") is True,
            payload.get("order_snapshot_complete") is True,
            payload.get("foreign_activity") is False,
            payload.get("protective_stop_state") == "NONE",
        ))

    def _assert_reconciliation_recovery_suffix_locked(
        self, acknowledgement_sequence: int,
    ) -> None:
        allowed_transition_pairs = {
            (
                "READY_DISARMED", "WAITING_FOR_EXECUTION_BRIDGE",
                "EXECUTION_BRIDGE_DISCONNECTED",
            ),
            (
                "WAITING_FOR_EXECUTION_BRIDGE", "RECONCILING",
                "EXECUTION_BRIDGE_AUTHENTICATED",
            ),
            ("READY_DISARMED", "RECONCILING", "EXECUTION_BRIDGE_AUTHENTICATED"),
            ("RECONCILING", "READY_DISARMED", "FLAT_RECONCILIATION_COMPLETE"),
        }
        for item in self.ledger.authority_records_after(acknowledgement_sequence):
            kind = item.get("kind")
            record = item.get("record")
            payload = record.get("payload") if isinstance(record, Mapping) else None
            if not isinstance(payload, Mapping):
                raise RuntimeError("RECONCILIATION_RECOVERY_AUTHORITY_SUFFIX_INVALID")
            if kind == "SESSION_HANDSHAKE":
                if (
                    payload.get("account_name"), payload.get("account_class"),
                    payload.get("instrument"), payload.get("capability"),
                ) != ("Sim101", "LOCAL_SIMULATION", "MNQ SEP26", "PAPER_ONLY"):
                    raise RuntimeError("RECONCILIATION_RECOVERY_CONFLICTING_AUTHORITY")
                continue
            if kind == "SESSION_TRANSITION":
                transition = (
                    payload.get("prior_state"), payload.get("state"), payload.get("reason"),
                )
                if transition not in allowed_transition_pairs:
                    raise RuntimeError("RECONCILIATION_RECOVERY_CONFLICTING_AUTHORITY")
                continue
            if kind in {
                "COMMAND_RECEIPT_RECONCILIATION", "POSITION_SNAPSHOT_RECONCILIATION",
            } and self._recovery_reconciliation_payload_safe(payload):
                continue
            raise RuntimeError("RECONCILIATION_RECOVERY_CONFLICTING_AUTHORITY")

    def complete_reconciliation_recovery(
        self,
        *,
        lease_id: str,
        request_id: str,
        request_hash: str,
        incident: Mapping[str, object],
        lockouts: tuple[Mapping[str, object], ...],
        acknowledgement: Mapping[str, object],
        proof: Mapping[str, object],
        evidence_digest: str,
        commands_before: int,
    ) -> dict[str, object]:
        """Persist proof and clear only the incident-8631 reconciliation latch."""
        with self._lock, self.ledger.commissioning_authority_fence():
            if self._reconciliation_recovery_lease_id != lease_id:
                raise RuntimeError("RECONCILIATION_RECOVERY_LEASE_MISMATCH")
            authority = self._reconciliation_recovery_authority_locked()
            if not self._reconciliation_recovery_safe_snapshot(authority):
                raise RuntimeError("RECONCILIATION_RECOVERY_FINAL_PREFLIGHT_BLOCKED")
            if authority.get("transport_commands_sent") != commands_before:
                raise RuntimeError("RECONCILIATION_RECOVERY_EXECUTION_COMMAND_RACE")
            acknowledgement_sequence = acknowledgement.get("ledger_sequence")
            if type(acknowledgement_sequence) is not int:
                raise RuntimeError("RECONCILIATION_RECOVERY_ACKNOWLEDGEMENT_INVALID")
            self._assert_reconciliation_recovery_suffix_locked(acknowledgement_sequence)

            latest_safety = self.ledger.recent_kind_records(("INCIDENT_SAFETY_EVENT",), 1)
            latest_lockout = self.ledger.recent_kind_records(("RISK_EVENT_AUTHORITY_LOCKOUT",), 1)
            if (
                len(latest_safety) != 1
                or latest_safety[0]["ledger_sequence"] != incident.get("ledger_sequence")
                or latest_safety[0]["record_hash"] != incident.get("record_hash")
                or len(latest_lockout) != 1
                or latest_lockout[0]["ledger_sequence"] != lockouts[-1].get("ledger_sequence")
                or latest_lockout[0]["record_hash"] != lockouts[-1].get("record_hash")
            ):
                raise RuntimeError("RECONCILIATION_RECOVERY_NEWER_SAFETY_AUTHORITY")

            observations = proof.get("observations")
            if not isinstance(observations, list) or len(observations) != 2:
                raise RuntimeError("RECONCILIATION_RECOVERY_PROOF_INVALID")
            for observation in observations:
                result = observation.get("probe_result") if isinstance(observation, Mapping) else None
                if not isinstance(result, Mapping) or not self._recovery_reconciliation_payload_safe(result):
                    raise RuntimeError("RECONCILIATION_RECOVERY_PROOF_UNSAFE")
            proof_commands = proof.get("commands_sent")
            if proof_commands != 0:
                raise RuntimeError("RECONCILIATION_RECOVERY_PROBE_USED_COMMANDS")

            evidence_identity = "l3g-reconciliation-recovery-evidence-" + evidence_digest
            evidence_payload = {
                "schema": "lane-iii-reconciliation-recovery-v1",
                "request_id": request_id,
                "request_hash": request_hash,
                "incident": dict(incident),
                "acknowledgement": dict(acknowledgement),
                "evidence_digest": evidence_digest,
                "proof": dict(proof),
                "transport_commands_before": commands_before,
                "transport_commands_after": authority["transport_commands_sent"],
                "transport_command_delta": 0,
                "effect": "FRESH_SERVER_REQUESTED_FLAT_NO_ORDERS_OBSERVATION",
            }
            self.ledger.append(
                "RISK_EVENT_RECONCILIATION_RECOVERY_EVIDENCE", evidence_payload,
                identity=evidence_identity, execution_session_id=self._execution_session_id(),
            )
            evidence_record = self.ledger.record_by_identity(evidence_identity)
            if evidence_record is None:
                raise RuntimeError("RECONCILIATION_RECOVERY_EVIDENCE_NOT_DURABLE")
            evidence_coordinate = {
                "identity": evidence_identity,
                "ledger_sequence": evidence_record["ledger_sequence"],
                "record_hash": evidence_record["record_hash"],
                "evidence_digest": evidence_digest,
                "proof_hash": proof.get("proof_hash"),
            }
            clear_payload = {
                "schema": "lane-iii-reconciliation-recovery-v1",
                "request_id": request_id,
                "request_hash": request_hash,
                "operator": "Joseph",
                "incident": dict(incident),
                "preserved_lockouts": [dict(value) for value in lockouts],
                "acknowledgement": dict(acknowledgement),
                "reconciliation": evidence_coordinate,
                "account": "Sim101",
                "environment": "LOCAL_SIMULATION",
                "instrument": "MNQ SEP26",
                "maximum_quantity": 1,
                "live_capital": "DENIED",
                "locked_out": False,
                "cleared_lockout_reason": "RECONCILIATION_BLOCKED",
                "effect": "ONLY_RECONCILIATION_BLOCKED_CLEARED",
            }
            clear_identity = "l3g-reconciliation-lockout-clear-" + canonical_hash(
                {"request_id": request_id}
            )
            self.ledger.append(
                "RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED", clear_payload,
                identity=clear_identity, execution_session_id=self._execution_session_id(),
            )
            clear_record = self.ledger.record_by_identity(clear_identity)
            if clear_record is None:
                raise RuntimeError("RECONCILIATION_RECOVERY_CLEAR_NOT_DURABLE")

            # The durable clear and its automatically advanced continuity
            # anchor are committed before in-memory entry authority changes.
            self.risk.restore_lockout(False, None, None)
            self._fault_reason = None
            self._retain_safety_lockout_after_flat = False
            self._entries_paused = False
            self._reconciliation_recovery_lease_id = None
            return {
                "schema": "lane-iii-reconciliation-recovery-v1",
                "status": "RECOVERED",
                "request_id": request_id,
                "request_hash": request_hash,
                "idempotent_replay": False,
                "acknowledgement": dict(acknowledgement),
                "reconciliation": evidence_coordinate,
                "clear": {
                    "identity": clear_identity,
                    "ledger_sequence": clear_record["ledger_sequence"],
                    "record_hash": clear_record["record_hash"],
                },
                "cleared_lockout_reason": "RECONCILIATION_BLOCKED",
                "preserved_incident": dict(incident),
                "risk_locked_out": False,
                "transport_command_delta": 0,
                "live_capital": "DENIED",
            }

    @property
    def state(self) -> PaperRuntimeState:
        with self._lock:
            return self._state

    def _transition(self, target: PaperRuntimeState, reason: str) -> None:
        prior = self._state
        allowed: dict[PaperRuntimeState, set[PaperRuntimeState]] = {
            PaperRuntimeState.DISABLED: {PaperRuntimeState.STARTING},
            PaperRuntimeState.STARTING: {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE: {PaperRuntimeState.RECONCILING, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.RECONCILING: {PaperRuntimeState.READY_DISARMED, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.READY_DISARMED: {PaperRuntimeState.STARTING, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING},
            PaperRuntimeState.PAPER_RUNNING: {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.ARMED_FLAT: {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.STOPPING},
            PaperRuntimeState.ENTRY_PENDING: {PaperRuntimeState.LONG, PaperRuntimeState.SHORT, PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.LONG: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.SHORT: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.EXIT_PENDING: {PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.STOPPING},
            PaperRuntimeState.PAUSED: {PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.LONG, PaperRuntimeState.SHORT, PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
            # A lockout revokes entries, never the exact-account emergency
            # flatten path. Only _request_exit(emergency=True) uses these arcs.
            PaperRuntimeState.LOCKED_OUT: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
            PaperRuntimeState.FAULTED: {PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.RECONCILING, PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING},
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
            self.ledger.append("SESSION_AUTHORITY", self.authority.authority_payload(), identity="l3g-authority-" + canonical_hash({"started_at": _now(), "object": id(self)}))
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
                    PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.LONG,
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
                transport_status = None if self._transport is None else self._transport.status()
                if (
                    transport_status is not None
                    and transport_status.authenticated_client
                    and transport_status.addon_provenance_valid
                ):
                    self._native_watchdog_authority_established = True
                self._command_sequence = 0
                if self._state in {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED}:
                    self._entries_paused = (
                        self._commissioning_ownership is not None
                        or self._risk_continuity_fault is not None
                        or bool(self.risk.status().get("locked_out"))
                    )
                    self._transition(PaperRuntimeState.RECONCILING, "EXECUTION_BRIDGE_AUTHENTICATED")
            elif state == "DISCONNECTED":
                self.policy.reset("EXECUTION_BRIDGE_DISCONNECTED")
                if self._position is not PaperDirection.FLAT or self._state in {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.EXIT_PENDING}:
                    self._fault_reason = "EXECUTION_BRIDGE_DISCONNECTED_WITH_ACTIVITY"
                    self.risk.lock_out(self._fault_reason)
                    # A dropped or intentionally retired ordered session makes
                    # the last mutation ambiguous. Stop heartbeats across the
                    # reconnect so the independently owned AddOn watchdog is
                    # the only flatten authority.
                    self._activate_independent_watchdog_locked(
                        self._fault_reason, force=True,
                    )
                    if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                elif self._state not in {PaperRuntimeState.WAITING_FOR_EXECUTION_BRIDGE, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    # A clean flat reconnect still requires a fresh snapshot.
                    if self._operational_session is not None:
                        self._request_operational_stop_locked("EXECUTION_BRIDGE_DISCONNECTED")
                    if self._state in {PaperRuntimeState.RECONCILING, PaperRuntimeState.READY_DISARMED, PaperRuntimeState.PAPER_RUNNING, PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED, PaperRuntimeState.FAULTED, PaperRuntimeState.LOCKED_OUT}:
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
            shadow = self._perpetual_seed_shadow
            if shadow is not None:
                shadow_reset_before = shadow.reset_count()
                shadow.on_transport_state(state)
                if shadow.reset_count() != shadow_reset_before:
                    self._clear_perpetual_seed_capture_locked(
                        "PERPETUAL_STARTUP_SEED_WAITING_AFTER_TRANSPORT_RESET",
                    )
            self._snapshot = replace(self._snapshot, observed_at=_now(), local_bridge_healthy=state is StreamHealth.HEALTHY, local_sequence_gap=state is StreamHealth.DISCONNECTED or self._snapshot.local_sequence_gap)
            if state is StreamHealth.DISCONNECTED and self._position is not PaperDirection.FLAT:
                self._request_exit("LOCAL_OBSERVATION_BRIDGE_DISCONNECTED", emergency=True)

    def on_observation_rejection(self, error: NinjaTraderObservationError) -> None:
        with self._lock:
            self._advance_commissioning_authority_epoch()
            self.policy.on_rejection(error)
            if self._perpetual_seed_shadow is not None:
                self._perpetual_seed_shadow.on_rejection(error)
                self._clear_perpetual_seed_capture_locked(
                    "PERPETUAL_STARTUP_SEED_WAITING_AFTER_OBSERVATION_REJECTION",
                )
            self._reset_commissioning_warmup("OBSERVATION_REJECTED:" + error.code.value)
            self._last_policy_reset_count = self.policy.reset_count()
            self._snapshot = replace(
                self._snapshot, observed_at=_now(), local_sequence_gap=True,
                evidence_warmed=False, commissioning_session_warmed=False,
            )
            if self._position is not PaperDirection.FLAT:
                self._request_exit("MALFORMED_OBSERVATION", emergency=True)
            self._append_best_effort_safety_audit_locked(
                "INCIDENT_OBSERVATION_REJECTION",
                {"code": error.code.value, "detail": error.detail or "unspecified"},
            )

    def on_observation_duplicate(self) -> None:
        with self._lock:
            self.policy.on_duplicate()
            if self._perpetual_seed_shadow is not None:
                self._perpetual_seed_shadow.on_duplicate()
            self.ledger.append("INCIDENT_DUPLICATE_OBSERVATION", {"effect": "NO_NEW_PAPER_EVIDENCE"})

    def _pause_for_ledger_capacity_locked(self, capacity: Mapping[str, object] | None = None) -> None:
        """Deny new entry authority without making a congested writer drain again."""
        state = "UNKNOWN" if capacity is None else str(capacity.get("state") or "UNKNOWN")
        reason = "LEDGER_CAPACITY_INADEQUATE:" + state
        self._entries_paused = True
        self._armed_session = None
        self._fault_reason = reason
        self.risk.lock_out(reason)
        self._request_operational_stop_locked(reason)
        self._advance_commissioning_authority_epoch()

    def _append_best_effort_safety_audit_locked(
        self, kind: str, payload: Mapping[str, object],
    ) -> None:
        """Audit a safety transition without making audit availability a safety gate."""
        try:
            self.ledger.append(kind, payload, execution_session_id=self._execution_session_id())
        except LedgerCapacityError as error:
            self._pause_for_ledger_capacity_locked(error.capacity)
        except Exception:
            # The ledger is already sealed, failed, or unavailable. The caller
            # has performed its state/exit safety action first; do not recurse
            # through another synchronous audit append.
            self._pause_for_ledger_capacity_locked()

    def _has_unresolved_execution_activity_locked(self) -> bool:
        """Return whether an AddOn command may still alter the exact account."""
        return (
            self._position is not PaperDirection.FLAT
            or self._snapshot.working_owned_orders > 0
            or self._snapshot.working_entry_orders > 0
            or self._state in {PaperRuntimeState.ENTRY_PENDING, PaperRuntimeState.EXIT_PENDING}
            or self._exit_submission_in_progress
        )

    def _watchdog_transport_available_locked(self) -> bool:
        """Check the exact condition required by the independently owned watchdog."""
        transport = self._transport
        if transport is None:
            return False
        try:
            status = transport.status()
        except Exception:
            return False
        return bool(
            (
                getattr(status, "authenticated_client", False)
                and getattr(status, "addon_provenance_valid", False)
            )
            or self._native_watchdog_authority_established
        )

    def _native_safety_correlation_active_locked(self) -> bool:
        return self._watchdog_failsafe_requires_flat_confirmation

    def _activate_independent_watchdog_locked(self, reason: str, *, force: bool = False) -> None:
        """Stop Python heartbeats so the independently owned AddOn fails flat.

        This deliberately emits no unrecorded command.  The signed AddOn owns
        an exact-account/instrument watchdog which cancels owned work and
        flattens if authenticated activity survives a missed-heartbeat window.
        The control-center shutdown path retains that transport for the bounded
        grace interval exposed by :meth:`watchdog_shutdown_status`.
        """
        self._heartbeat_stop.set()
        if not force and not self._has_unresolved_execution_activity_locked():
            return
        # Do not reset a pending correlation on a retry.  A later exact
        # reconciliation must remain tied to the same watchdog event.
        if not self._watchdog_failsafe_requires_flat_confirmation:
            self._watchdog_failsafe_requires_flat_confirmation = True
            self._watchdog_failsafe_activation_message_sequence = self._execution_message_sequence
            self._watchdog_failsafe_safety_event_id = None
            self._watchdog_failsafe_flat_confirmation = None
            self._watchdog_failsafe_durable_confirmation = None
            self._watchdog_failsafe_safety_event_durable = None
            self._watchdog_failsafe_reconciliation_durable = None
            self._watchdog_failsafe_last_settlement_sequence = 0
            self._watchdog_failsafe_settled_reconciliation_count = 0
            self._watchdog_failsafe_available = self._watchdog_transport_available_locked()
        self._watchdog_failsafe_reason = reason
        # A disconnected or unproven bridge cannot execute the C# watchdog.
        # Report it immediately as unsafe rather than pretending a grace
        # window has an independent safety owner.
        deadline = time.monotonic() + (
            _INDEPENDENT_WATCHDOG_GRACE_SECONDS
            if self._watchdog_failsafe_available is True else 0.0
        )
        previous = self._watchdog_failsafe_deadline_monotonic
        self._watchdog_failsafe_deadline_monotonic = (
            deadline if previous is None else max(previous, deadline)
        )

    def _fail_closed_without_ledger_locked(self, reason: str) -> None:
        """Preserve a live safety fallback when durable command authority fails."""
        self._entries_paused = True
        self._armed_session = None
        self._disarm_after_flat = True
        self._fault_reason = reason
        self.risk.lock_out(reason)
        self._request_operational_stop_locked(reason)
        self._activate_independent_watchdog_locked(reason)
        # _transition() writes the in-memory state before it records its audit
        # row.  On a sealed/failed ledger that can leave a phantom pending
        # state.  Replace it with an explicit unarmed fault state without
        # attempting another ledger write.
        if self._state not in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
            self._state = PaperRuntimeState.FAULTED
        self._advance_commissioning_authority_epoch()

    def _force_shutdown_state_without_ledger_locked(
        self, target: PaperRuntimeState, reason: str,
    ) -> None:
        """Finish a terminal lifecycle transition even if its audit append failed."""
        if target not in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
            raise ValueError("Only terminal shutdown states may bypass a failed ledger audit.")
        self._entries_paused = True
        self._armed_session = None
        self._disarm_after_flat = self._has_unresolved_execution_activity_locked()
        self._fault_reason = reason
        self.risk.lock_out(reason)
        self._state = target
        self._advance_commissioning_authority_epoch()

    def watchdog_shutdown_status(self) -> dict[str, object]:
        """Return the bounded transport-retention requirement for a safety stop."""
        with self._lock:
            deadline = self._watchdog_failsafe_deadline_monotonic
            remaining = 0.0 if deadline is None else max(0.0, deadline - time.monotonic())
            awaiting_confirmation = self._watchdog_failsafe_requires_flat_confirmation
            confirmed = self._watchdog_failsafe_flat_confirmation is not None
            return {
                "required": deadline is not None and awaiting_confirmation,
                "reason": self._watchdog_failsafe_reason,
                "remaining_seconds": round(remaining, 3),
                "flat_confirmed": confirmed or not awaiting_confirmation,
                "durable_confirmation": self._watchdog_failsafe_durable_confirmation,
                "watchdog_available": self._watchdog_failsafe_available,
                "safety_event_id": self._watchdog_failsafe_safety_event_id,
                "safety_event_durable": self._watchdog_failsafe_safety_event_durable,
                "reconciliation_durable": self._watchdog_failsafe_reconciliation_durable,
                "settled_reconciliation_count": self._watchdog_failsafe_settled_reconciliation_count,
            }

    def _record_watchdog_reconciliation_locked(
        self,
        message: Mapping[str, object],
        *,
        quantity: int,
        orders: int,
        entry_orders: int,
        foreign: bool,
        durable_receipt_unavailable: bool,
    ) -> None:
        """Accept only a settled, correlated, post-failsafe flat proof.

        A snapshot cached before heartbeats stopped is specifically not enough:
        a submitted entry can become working after that snapshot.  The AddOn
        sends ``SAFETY_EVENT`` before the correlated safety reconciliation, so
        TCP ordering plus the event id prevents an earlier snapshot from
        completing a shutdown.
        """
        if not self._watchdog_failsafe_requires_flat_confirmation:
            return
        activation = self._watchdog_failsafe_activation_message_sequence
        if activation is None or self._execution_message_sequence <= activation:
            return
        expected_safety_event_id = self._watchdog_failsafe_safety_event_id
        supplied_safety_event_id = message.get("safety_event_id")
        # A generic reconciliation may have been queued before Python stopped
        # heartbeating.  Only the AddOn's safety-correlated snapshot can prove
        # the independently owned watchdog has observed and settled activity.
        if expected_safety_event_id is None or supplied_safety_event_id != expected_safety_event_id:
            return
        settlement_final = message.get("safety_settlement_final") is True
        settlement_sequence = message.get("safety_settlement_sequence")
        if (
            not settlement_final
            or type(settlement_sequence) is not int
            or settlement_sequence <= self._watchdog_failsafe_last_settlement_sequence
        ):
            return
        complete = (
            message.get("position_snapshot_complete") is True
            and message.get("order_snapshot_complete") is True
        )
        if not (
            complete
            and not foreign
            and quantity == 0
            and orders == 0
            and entry_orders == 0
        ):
            return
        self._watchdog_failsafe_last_settlement_sequence = settlement_sequence
        self._watchdog_failsafe_settled_reconciliation_count += 1
        # A persisted safety event and its persisted final reconciliation are
        # one indivisible audit pair. Once either arrived through the
        # ledger-outage fallback, a later row cannot upgrade this shutdown to
        # a durable clean result.
        self._watchdog_failsafe_reconciliation_durable = (
            self._watchdog_failsafe_reconciliation_durable is not False
            and not durable_receipt_unavailable
        )
        if self._watchdog_failsafe_settled_reconciliation_count < _WATCHDOG_SETTLED_RECONCILIATIONS_REQUIRED:
            return
        self._watchdog_failsafe_requires_flat_confirmation = False
        self._watchdog_failsafe_flat_confirmation = {
            "receipt_id": message.get("receipt_id"),
            "safety_event_id": supplied_safety_event_id,
            "message_sequence": self._execution_message_sequence,
            "safety_settlement_sequence": settlement_sequence,
            "settled_reconciliation_count": self._watchdog_failsafe_settled_reconciliation_count,
            "position_quantity": quantity,
            "working_order_count": orders,
        }
        self._watchdog_failsafe_durable_confirmation = (
            self._watchdog_failsafe_safety_event_durable is True
            and self._watchdog_failsafe_reconciliation_durable is True
        )

    def _deferred_capacity_healthy_locked(self, receipt: Mapping[str, object]) -> bool:
        return deferred_capacity_allows_authority(receipt)

    @staticmethod
    def _deferred_admission_receipt_healthy_locked(receipt: Mapping[str, object]) -> bool:
        """Accept a record the writer admitted without treating that enqueue as growth.

        ``append_deferred`` returns its capacity snapshot immediately after it
        enqueues the accepted record.  That snapshot may transiently report a
        positive queue-growth rate solely because of the successful admission.
        Authority gates still use ``deferred_capacity_allows_authority`` and
        require a later non-growing snapshot; this receipt check only proves
        that the writer accepted the no-authority record without a latch or
        writer failure.
        """
        return (
            receipt.get("schema") == "l3g-ledger-writer-capacity-v1"
            and receipt.get("state") == "HEALTHY"
            and receipt.get("admission_open") is True
            and receipt.get("capacity_fault_latched") is False
            and receipt.get("wal_capacity_fault_latched") is False
            and receipt.get("writer_error") is None
        )

    def _append_deferred_or_pause_locked(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> bool:
        try:
            receipt = self.ledger.append_deferred(
                kind, payload, identity=identity, occurred_at=occurred_at,
                execution_session_id=execution_session_id,
            )
        except LedgerCapacityError as error:
            self._pause_for_ledger_capacity_locked(error.capacity)
            return False
        if not self._deferred_admission_receipt_healthy_locked(receipt):
            self._pause_for_ledger_capacity_locked(receipt)
            return False
        return True

    def record_sink_failure(self, sink: str, event: str, error_type: str) -> None:
        with self._lock:
            if error_type == "LedgerCapacityError":
                self._pause_for_ledger_capacity_locked()
                return
            if self._state in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                return
            try:
                self.ledger.append(
                    "INCIDENT_OBSERVATION_SINK_FAILURE",
                    {"sink": sink, "event": event, "error_type": error_type},
                )
            except (LedgerCapacityError, RuntimeError):
                # Failure recording is best-effort once persistence itself is
                # unavailable. Preserve the listener boundary and pause new
                # authority rather than synchronously trying to drain it.
                self._pause_for_ledger_capacity_locked()

    def ingest(self, observation: NinjaTraderObservation) -> None:
        with self._lock:
            if self._state in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                return
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
            health_payload_keys = frozenset(observation.payload)
            if (
                observation.observation_type == "HEALTH"
                and health_payload_keys in HEALTH_AUTHORITY_OBSERVATION_PAYLOAD_KEY_SETS
            ):
                raw_payload.update({
                    "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
                    "observation_semantics": HEALTH_AUTHORITY_OBSERVATION_SEMANTICS,
                    "observation_payload_keys": sorted(health_payload_keys),
                })
            if not self._append_deferred_or_pause_locked(
                "OBSERVATION_ENVELOPE", raw_payload,
                identity="l3g-paper-observation-" + canonical_hash(raw_payload),
                occurred_at=observation.ninja_receipt_time,
            ):
                return
            if session_reason == "EVENT_TIMESTAMP_MOVED_BACKWARD":
                self._reset_commissioning_warmup(session_reason)
                shadow = self._perpetual_seed_shadow
                if shadow is not None:
                    shadow.reset(
                        "PERPETUAL_STARTUP_SEED_TEMPORAL_CONTINUITY_UNPROVEN",
                    )
                    self._clear_perpetual_seed_capture_locked(
                        "PERPETUAL_STARTUP_SEED_TEMPORAL_CONTINUITY_UNPROVEN",
                    )
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
            seed_source_envelope = dict(raw_payload)
            for timestamp_field in (
                "observed_at", "ninja_receipt_time", "provider_timestamp",
                "exchange_timestamp",
            ):
                timestamp = seed_source_envelope.get(timestamp_field)
                if timestamp is not None:
                    seed_source_envelope[timestamp_field] = normalized_utc(
                        str(timestamp),
                        f"Perpetual seed {timestamp_field.replace('_', ' ')}",
                    )
            self._ingest_perpetual_seed_shadow_locked(
                observation, context, seed_source_envelope,
            )
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
                mark = bid if self._position is PaperDirection.LONG else ask
                self._update_unrealized_pnl_locked(mark)
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
            if decision is not None and decision.reason_code == "MARKET_DATA_DISCONNECTED":
                update["market_price_connected"] = False
                update["evidence_warmed"] = False
                if self._perpetual_position_profile:
                    self._perpetual_flat_blocker = "MARKET_DATA_DISCONNECTED"
            elif (
                decision is not None
                and decision.reason_code == "MARKET_DATA_RECONNECTED"
                and self._perpetual_position_profile
                and self._position is PaperDirection.FLAT
            ):
                self._perpetual_flat_blocker = None
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
            if (
                decision is None
                and self._perpetual_position_profile
                and self._operational_session is not None
                and not self._operational_session_is_stopping_locked()
                and self._position is PaperDirection.FLAT
                and self._latest_five_minute_direction_checkpoint is None
            ):
                # If startup began before all three authentic evidence families
                # were warm, the first callback that completes a closed-boundary
                # source set gets the same durable startup calculation.
                decision = self.policy.evaluate_latest_completed_on_start(
                    event_at,
                    current_position=self._position,
                    pending_order=self._state in {
                        PaperRuntimeState.ENTRY_PENDING,
                        PaperRuntimeState.EXIT_PENDING,
                    },
                    prior_non_tied_available=False,
                )
            if decision is None:
                self._evaluate_risk_exit(observation.ninja_receipt_time)
                self._maintain_perpetual_position_locked("HEALTH_OR_DATA_RECOVERY")
                return
            self._last_decision = decision
            if decision.decision in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}:
                self._last_qualifying_entry_decision = decision
            evidence_audit_at = (
                str(decision.family_summary["candle_close_utc"])
                if decision.family_summary.get("startup_reconstruction") is True
                and isinstance(decision.family_summary.get("candle_close_utc"), str)
                else event_at
            )
            for evidence in self.policy.active_evidence(evidence_audit_at):
                if evidence.evidence_id not in self._recorded_evidence:
                    if not self._append_deferred_or_pause_locked(
                        "EVIDENCE", evidence.payload(), identity=evidence.evidence_id,
                        occurred_at=evidence.observed_at, execution_session_id=self._execution_session_id(),
                    ):
                        return
                    self._recorded_evidence.add(evidence.evidence_id)
            can_cause_side_effect = (
                decision.decision in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}
                and self._strategy_entry_context_authorized_locked(context, event_at)
            ) or (
                decision.decision is PaperDecisionKind.EXIT
                and self._position is not PaperDirection.FLAT
                and self._entry_owner is not PaperEntryOwner.COMMISSIONING
            )
            perpetual_boundary = (
                self._perpetual_position_profile
                and decision.family_summary.get("bias")
                in {
                    PaperDirection.LONG.value,
                    PaperDirection.SHORT.value,
                    "TIE",
                }
            )
            if perpetual_boundary:
                # Every completed boundary becomes restart authority. Ties are
                # chained to the last non-tied root so a cold restart can keep
                # that direction without waiting for another decisive bar.
                # Commit the source decision synchronously first; append()
                # flushes the already-enqueued evidence before the checkpoint.
                self.ledger.append(
                    "DECISION", decision.payload(), identity=decision.paper_decision_id,
                    occurred_at=decision.created_at,
                    execution_session_id=self._execution_session_id(),
                )
            elif not can_cause_side_effect:
                if not self._append_deferred_or_pause_locked(
                    "DECISION",
                    {**decision.payload(), "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT},
                    identity=decision.paper_decision_id,
                    occurred_at=decision.created_at,
                    execution_session_id=self._execution_session_id(),
                ):
                    return
            else:
                # append() first flushes every prior evidence/NO_TRADE batch;
                # a decision eligible to mutate paper state is then committed
                # synchronously before any intent, grant, command, or socket
                # side effect. Directional decisions observed while disarmed
                # have no such authority and remain safely batchable.
                self.ledger.append("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
            if (
                perpetual_boundary
                and not self._record_five_minute_direction_checkpoint_locked(decision)
            ):
                return
            self._evaluate_risk_exit(observation.ninja_receipt_time)
            if decision.decision is PaperDecisionKind.NO_TRADE:
                self._maintain_perpetual_position_locked("LATEST_NON_TIED_SIGNAL")
                return
            if decision.decision is PaperDecisionKind.EXIT:
                if self._position is not PaperDirection.FLAT and self._entry_owner is not PaperEntryOwner.COMMISSIONING:
                    if (
                        self._state is not PaperRuntimeState.EXIT_PENDING
                        and decision.reason_code.startswith("FIVE_MINUTE_REVERSE_TO_")
                        and decision.family_summary.get("action") == "REVERSE"
                    ):
                        self._pending_five_minute_reversal = decision
                    self._request_exit(decision.reason_code)
                return
            if self._strategy_entry_context_authorized_locked(context, event_at):
                if self._entry_owner is PaperEntryOwner.COMMISSIONING:
                    ownership = self._commissioning_ownership
                    if ownership is not None and not ownership.entry_consumed:
                        self.commission_entry(
                            ownership.commissioning_id,
                            ownership.commissioning_token,
                            candidate=decision,
                        )
                    else:
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
    def _native_order_failure_reason(
        prefix: str, state: str, message: Mapping[str, object],
    ) -> str:
        """Retain bounded native NinjaTrader diagnostics in the flat blocker."""
        code = str(message.get("native_error_code", "")).strip() or "UNAVAILABLE"
        raw_comment = str(message.get("native_error_comment", "")).strip()
        # Signed broker text is evidence, but never allow control characters or
        # an unbounded adapter message to corrupt the operator projection.
        comment = " ".join(raw_comment.split())[:240] or "UNAVAILABLE"
        return (
            f"{prefix}_{state}:"
            f"NATIVE_ERROR={code}:NATIVE_COMMENT={comment}"
        )

    @staticmethod
    def _bounded_exception_reason(prefix: str, error: Exception) -> str:
        detail = " ".join(str(error).split())[:240]
        return f"{prefix}:{type(error).__name__}:{detail or 'NO_DETAIL'}"

    def _protective_order_identity_reason_locked(
        self,
        message: Mapping[str, object],
        *,
        expected_quantity: int,
    ) -> str | None:
        """Validate one owned protective order before trusting its state."""
        account = message.get("account_name")
        instrument = message.get("instrument")
        quantity = message.get("quantity")
        native_order_id = message.get("native_order_id")
        if self._perpetual_position_profile and (
            not isinstance(account, str)
            or not isinstance(instrument, str)
            or type(quantity) is not int
            or not isinstance(native_order_id, str)
            or not native_order_id
        ):
            return "PROTECTIVE_STOP_IDENTITY_INCOMPLETE"
        if account is not None and account != self.risk.binding.account_name:
            return "PROTECTIVE_STOP_WRONG_ACCOUNT"
        if instrument is not None and instrument != self.risk.binding.instrument:
            return "PROTECTIVE_STOP_WRONG_INSTRUMENT"
        if quantity is not None and quantity != expected_quantity:
            return "PROTECTIVE_STOP_WRONG_QUANTITY"
        if (
            isinstance(native_order_id, str)
            and self._protective_order_id is not None
            and native_order_id != self._protective_order_id
        ):
            return "DUPLICATE_PROTECTIVE_STOP"
        return None

    @staticmethod
    def _valid_quote(observation: NinjaTraderObservation) -> bool:
        try:
            bid, ask = Decimal(str(observation.payload["bid"])), Decimal(str(observation.payload["ask"]))
            return bid > 0 and ask > bid and int(observation.payload["bid_size"]) > 0 and int(observation.payload["ask_size"]) > 0
        except Exception:
            return False

    def _update_unrealized_pnl_locked(self, mark_price: Decimal) -> None:
        """Update in-memory paper mark-to-market without persisting every quote."""
        if (
            self._position is PaperDirection.FLAT
            or self._entry_fill_price is None
            or self._entry_fill_quantity <= 0
            or self._entry_accounting_ambiguous
        ):
            return
        points = (
            mark_price - self._entry_fill_price
            if self._entry_direction is PaperDirection.LONG
            else self._entry_fill_price - mark_price
        )
        unrealized = points * Decimal("2") * self._entry_fill_quantity
        entry_context = self._entry_session_context or self._session_context
        trade_risk = self._trade_date_risk.setdefault(entry_context.trade_date, _TradeDateRisk())
        trade_risk.unrealized_pnl = unrealized
        self._snapshot = replace(
            self._snapshot,
            daily_realized_pnl=trade_risk.realized_pnl,
            daily_unrealized_pnl=unrealized,
        )

    def _evaluate_risk_exit(self, at: str) -> None:
        if self._position is PaperDirection.FLAT or self._state is PaperRuntimeState.EXIT_PENDING:
            return
        pnl = self._snapshot.daily_realized_pnl + self._snapshot.daily_unrealized_pnl
        if self._perpetual_position_profile and not self._snapshot.market_price_connected:
            self._request_operational_stop_locked("MARKET_DATA_DISCONNECTED")
            self._request_exit("MARKET_DATA_DISCONNECTED", emergency=True)
        elif self._snapshot.foreign_activity:
            self._request_operational_stop_locked("FOREIGN_ACTIVITY")
            self._request_exit("FOREIGN_ACTIVITY", emergency=True)
        elif pnl <= -PAPER_ACCOUNT_DAILY_LOSS_LIMIT_DOLLARS:
            entry_context = self._entry_session_context or self._session_context
            lockout_trade_date = (
                self._session_context.trade_date
                if self._perpetual_position_profile
                else entry_context.trade_date
            )
            self.risk.lock_out("DAILY_LOSS_LIMIT", trade_date=lockout_trade_date)
            self._request_operational_stop_locked("DAILY_LOSS_LIMIT")
            self._request_exit("DAILY_LOSS_LIMIT", emergency=True)
        elif (
            not self._perpetual_position_profile
            and self._session_context.hard_flat_due_at(
                datetime.fromisoformat(normalized_utc(at, "Risk exit time").replace("Z", "+00:00"))
            )
        ):
            self._request_operational_stop_locked("HARD_FLAT_DEADLINE")
            self._request_exit("HARD_FLAT_DEADLINE", emergency=True)
        elif not self._perpetual_position_profile and self.risk.maximum_age_due(self._snapshot, at):
            self._request_operational_stop_locked("MAXIMUM_POSITION_AGE")
            self._request_exit("MAXIMUM_POSITION_AGE")
        else:
            now = datetime.fromisoformat(normalized_utc(at, "Risk exit time").replace("Z", "+00:00"))
            if (
                self._perpetual_position_profile
                and perpetual_exchange_blocker(now, self._session_context) is not None
            ):
                # Expected exchange closures naturally stop quote/trade/depth
                # callbacks. Retain the protected position; on reopening the
                # same freshness gates become active before any new entry.
                return
            stale = (
                (self._snapshot.quote_observed_at, self.risk.profile.quote_maximum_age_seconds, "QUOTE_STALE"),
                (self._snapshot.classified_trade_observed_at, self.risk.profile.classified_trade_maximum_age_seconds, "CLASSIFIED_TRADE_STALE"),
                (self._snapshot.depth_mutation_observed_at, self.risk.profile.depth_mutation_maximum_age_seconds, "DEPTH_STALE"),
            )
            for source, seconds, reason in stale:
                if source is None or now - datetime.fromisoformat(source.replace("Z", "+00:00")) > timedelta(seconds=seconds):
                    # Source-specific freshness can lapse briefly even while
                    # the authenticated bridge and the other feeds remain
                    # healthy.  Flatten with the emergency transport action,
                    # but preserve persistent-paper ownership.  The ordinary
                    # entry risk gate still refuses a replacement entry until
                    # quote, classified-trade, and depth freshness all recover.
                    self._request_exit(reason, emergency=True, stop_operational=False)
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
        try:
            with self.ledger.authority_capacity_fence():
                return self._request_entry_with_capacity_fence_locked(decision)
        except LedgerCapacityError as error:
            self._pause_for_ledger_capacity_locked(error.capacity)
            return False

    def _request_entry_with_capacity_fence_locked(self, decision: PaperDecision) -> bool:
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
        self._post_entry_reconciliation_pending = False
        self._post_entry_reconciliation_complete = False
        self._post_entry_reconciliation_command_id = None
        self._early_protective_order_event = None
        self._protective_order_id = None
        # Install every callback-visible authority fact before transport.  A
        # synchronous AddOn acknowledgement or fill can safely re-enter this
        # runtime through the RLock without observing an unowned command.
        self._transition(PaperRuntimeState.ENTRY_PENDING, "ENTRY_COMMAND_AUTHORIZED")
        self._persist_and_send(command, grant)
        return True

    def _request_exit(
        self,
        reason: str,
        *,
        emergency: bool = False,
        stop_operational: bool | None = None,
    ) -> bool:
        self._post_entry_reconciliation_pending = False
        self._post_entry_reconciliation_command_id = None
        self._early_protective_order_event = None
        if not reason.startswith("FIVE_MINUTE_REVERSE_TO_"):
            self._pending_five_minute_reversal = None
        if stop_operational is None:
            stop_operational = emergency
        if stop_operational:
            self._request_operational_stop_locked(reason)
        if self._native_safety_correlation_active_locked():
            # The authenticated AddOn has already published the correlation
            # that owns cancellation/flattening. A Python EXIT here would be a
            # competing mutation and can invert or duplicate the position.
            self._entries_paused = True
            self._retain_safety_lockout_after_flat = True
            self.risk.lock_out(self._fault_reason or reason)
            if self._state not in {
                PaperRuntimeState.LOCKED_OUT,
                PaperRuntimeState.STOPPING,
                PaperRuntimeState.STOPPED,
            }:
                self._transition(
                    PaperRuntimeState.LOCKED_OUT,
                    "NATIVE_SAFETY_FLATTEN_OWNS_SETTLEMENT",
                )
            return False
        terminal_states = {
            PaperRuntimeState.EXIT_PENDING, PaperRuntimeState.STOPPING,
            PaperRuntimeState.STOPPED,
        }
        if not emergency:
            terminal_states.update({PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED})
        if (
            self._position is PaperDirection.FLAT
            or self._state in terminal_states
            or self._exit_submission_in_progress
        ):
            return False
        if emergency and (
            self._state in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED}
            or self._risk_continuity_fault is not None
        ):
            # Entry authority remains latched after physical settlement. The
            # state transition below represents safety-exit progress only.
            self._retain_safety_lockout_after_flat = True
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
            self.policy.artifact.policy_id, self.policy.artifact.configuration_hash, PaperDecisionKind.EXIT, created_at,
            (datetime.fromisoformat(normalized_utc(created_at, "Exit decision time").replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"risk_exit": reason}, (decision_id,), (max(0, self.policy.status().get("last_local_sequence") or 0),), (canonical_hash({"reason": reason}),),
            self.policy.artifact.sequence_authority, self.policy.artifact.book_completeness, False, reason,
            self._session_context.session_kind, self._session_context.session_id,
            self._session_context.trade_date, self._session_context.session_profile_hash,
            self._session_context.session_generation,
            commissioning, not commissioning, False,
        )
        try:
            self.ledger.append("DECISION", pseudo.payload(), identity=pseudo.paper_decision_id, occurred_at=pseudo.created_at, execution_session_id=self._execution_session_id())
            bid, ask, last = self._references()
            intent = self.risk.make_intent(pseudo, reference_bid=bid, reference_ask=ask, reference_last=last)
            self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
            grant = self.risk.evaluate(intent, self._snapshot, at=_now())
            self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
            if not grant.granted:
                self._fail_closed_without_ledger_locked(
                    "EXIT_RISK_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes),
                )
                return False
            action = ExecutionAction.EMERGENCY_FLATTEN if emergency else ExecutionAction.EXIT
            command = self._make_command(
                intent.intent_id, pseudo.paper_decision_id, grant.grant_id, action, PaperDirection.FLAT, reason,
                commissioning=commissioning, strategy_generated=not commissioning, scientific_evidence=False,
            )
            self._post_exit_reconciliation_pending = False
            self._post_exit_position_flat_observed = False
            self._post_exit_order_terminal_observed = False
            self._pending_exit_command_id = command.command_id
            self._exit_submission_in_progress = True
            try:
                self._transition(PaperRuntimeState.EXIT_PENDING, reason)
                self._persist_and_send(command, grant)
            finally:
                self._exit_submission_in_progress = False
            return True
        except Exception as error:
            # Never leave an open position behind a phantom EXIT_PENDING state
            # merely because its durable exit evidence could not be written.
            # The independently owned AddOn watchdog is the only safe action
            # available without fabricating an unrecorded command.
            self._fail_closed_without_ledger_locked(
                "EXIT_DURABLE_AUTHORITY_UNAVAILABLE:" + type(error).__name__,
            )
            return False

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
            "account_name": self.risk.binding.account_name,
            "account_class": self.risk.binding.account_class,
            "instrument": self.risk.binding.instrument,
            "quantity": quantity,
            "expected_position": expected,
            "created_at": created,
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=self.policy.artifact.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
            "policy_hash": self.policy.artifact.configuration_hash,
            "risk_profile_hash": self.risk.profile.configuration_hash,
            "account_binding_hash": self.risk.binding.binding_hash,
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
        except Exception as error:
            self._fault_reason = self._bounded_exception_reason(
                "DURABLE_COMMAND_SEND_FAILED", error,
            )
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            raise
        self._last_command = command

    def on_execution_message(self, message: Mapping[str, object]) -> None:
        with self._lock:
            self._advance_commissioning_authority_epoch()
            # The transport attaches this internal marker only after a frame
            # has passed HMAC/schema/session validation but its audit row
            # could not be persisted.  Preserve authenticated account truth
            # for the safety watchdog without inventing durable evidence.
            durable_receipt_unavailable = message.get(_DURABILITY_UNAVAILABLE_MARKER) is True
            inbound = (
                {key: value for key, value in message.items() if key != _DURABILITY_UNAVAILABLE_MARKER}
                if durable_receipt_unavailable else message
            )
            self._execution_message_sequence += 1
            message_type = str(inbound.get("message_type", ""))
            if message_type == "RECONCILIATION":
                self._apply_reconciliation(inbound, durable_receipt_unavailable=durable_receipt_unavailable)
            elif message_type in {"ORDER_EVENT", "COMMAND_ACK", "COMMAND_REJECTED"}:
                self._last_order_state = dict(inbound)
                if (
                    message_type == "COMMAND_REJECTED"
                    and self._perpetual_position_profile
                    and self._post_entry_reconciliation_command_id is not None
                    and inbound.get("command_id")
                    == self._post_entry_reconciliation_command_id
                ):
                    detail = str(inbound.get("reason_code", "UNKNOWN")).strip() or "UNKNOWN"
                    self._fault_reason = (
                        "POST_ENTRY_RECONCILIATION_COMMAND_REJECTED:" + detail
                    )
                    self._post_entry_reconciliation_pending = False
                    self._post_entry_reconciliation_complete = False
                    self._post_entry_reconciliation_command_id = None
                    self._retain_safety_lockout_after_flat = True
                    self.risk.lock_out(self._fault_reason)
                    if self._position is not PaperDirection.FLAT:
                        self._request_exit(self._fault_reason, emergency=True)
                    elif self._state not in {
                        PaperRuntimeState.LOCKED_OUT,
                        PaperRuntimeState.STOPPING,
                        PaperRuntimeState.STOPPED,
                    }:
                        self._perpetual_flat_blocker = self._fault_reason
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                    return
                if message_type == "COMMAND_REJECTED":
                    self._fault_reason = "EXECUTION_COMMAND_REJECTED:" + str(inbound.get("reason_code", "UNKNOWN"))
                    if self._perpetual_position_profile and self._position is PaperDirection.FLAT:
                        self._perpetual_flat_blocker = self._fault_reason
                    self.risk.lock_out(self._fault_reason)
                    # Preserve the owned safety-exit path while a position is
                    # open; role-specific handling below initiates or audits it.
                    if self._position is PaperDirection.FLAT and self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                role = str(inbound.get("order_role", "")).upper()
                order_state = str(inbound.get("order_state", "")).upper()
                if role in {"EXIT", "PROTECTIVE"} and order_state == "FILLED":
                    exit_order_matches = (
                        role == "EXIT"
                        and isinstance(self._pending_exit_command_id, str)
                        and inbound.get("command_id") == self._pending_exit_command_id
                    )
                    protective_order_matches = (
                        role == "PROTECTIVE"
                        and isinstance(self._protective_order_id, str)
                        and bool(self._protective_order_id)
                        and inbound.get("native_order_id") == self._protective_order_id
                    )
                    if exit_order_matches or protective_order_matches:
                        if durable_receipt_unavailable:
                            self._retain_safety_lockout_after_flat = True
                            self._fail_closed_without_ledger_locked(
                                "EXIT_ORDER_RECEIPT_DURABILITY_UNAVAILABLE",
                            )
                        else:
                            self._post_exit_order_terminal_observed = True
                            self._maybe_request_reconciliation_after_exit_locked()
                protective_can_belong_to_pending_entry = (
                    self._perpetual_position_profile
                    and self._position is PaperDirection.FLAT
                    and self._state is PaperRuntimeState.ENTRY_PENDING
                )
                if role == "PROTECTIVE" and (
                    self._position is not PaperDirection.FLAT
                    or protective_can_belong_to_pending_entry
                ):
                    protective_reason = self._protective_order_identity_reason_locked(
                        inbound,
                        expected_quantity=(
                            self._position_quantity
                            if self._position is not PaperDirection.FLAT else 1
                        ),
                    )
                    native_order_id = inbound.get("native_order_id")
                    if protective_reason is not None:
                        self._fault_reason = protective_reason
                        self.risk.lock_out(protective_reason)
                        if self._position is not PaperDirection.FLAT:
                            self._request_exit(protective_reason, emergency=True)
                        elif self._state not in {
                            PaperRuntimeState.LOCKED_OUT,
                            PaperRuntimeState.STOPPING,
                            PaperRuntimeState.STOPPED,
                        }:
                            self._perpetual_flat_blocker = protective_reason
                            self._transition(PaperRuntimeState.LOCKED_OUT, protective_reason)
                    elif isinstance(native_order_id, str) and native_order_id:
                        self._protective_order_id = native_order_id
                        if protective_can_belong_to_pending_entry:
                            self._early_protective_order_event = dict(inbound)
                expected_protective_cancellation = (
                    role == "PROTECTIVE"
                    and order_state in {"CANCELLED", "CANCELED"}
                    and (self._exit_submission_in_progress or self._state is PaperRuntimeState.EXIT_PENDING)
                )
                if role == "PROTECTIVE" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._position is not PaperDirection.FLAT and not expected_protective_cancellation:
                    protective_failure = self._native_order_failure_reason(
                        "PROTECTIVE_STOP", order_state, inbound,
                    )
                    self.risk.lock_out(protective_failure)
                    self._request_exit(protective_failure, emergency=True)
                if role == "PROTECTIVE":
                    self._snapshot = replace(self._snapshot, observed_at=_now(), protective_stop_state=order_state or self._snapshot.protective_stop_state)
                    if (
                        self._perpetual_position_profile
                        and order_state == "WORKING"
                        and self._position in {PaperDirection.LONG, PaperDirection.SHORT}
                        and self._state in {PaperRuntimeState.LONG, PaperRuntimeState.SHORT}
                        and not self._post_entry_reconciliation_pending
                    ):
                        self._request_post_entry_reconciliation_locked()
                if role == "ENTRY" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._state is PaperRuntimeState.ENTRY_PENDING:
                    if message_type != "COMMAND_REJECTED":
                        self._fault_reason = self._native_order_failure_reason(
                            "MARKET_ENTRY_ORDER", order_state, inbound,
                        )
                    if self._perpetual_position_profile:
                        self._perpetual_flat_blocker = self._fault_reason
                    self.risk.lock_out(self._fault_reason)
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                if (
                    message_type == "COMMAND_REJECTED"
                    and str(inbound.get("reason_code", "")).upper() in {
                        "COMMAND_ACK_TIMEOUT",
                        "ACKNOWLEDGEMENT_MISSING",
                        "ACKNOWLEDGEMENT_TIMEOUT",
                        "ACKNOWLEDGEMENT_SESSION_ABORTED",
                    }
                    and self._position is not PaperDirection.FLAT
                ):
                    self._fault_reason = "PROTECTIVE_STOP_ACKNOWLEDGEMENT_MISSING"
                    self.risk.lock_out(self._fault_reason)
                    self._request_exit(self._fault_reason, emergency=True)
                if role == "EXIT" and order_state in {"REJECTED", "CANCELLED", "CANCELED"} and self._state is PaperRuntimeState.EXIT_PENDING:
                    self._fault_reason = self._native_order_failure_reason(
                        "EXIT_ORDER", order_state, inbound,
                    )
                    self.risk.lock_out(self._fault_reason)
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            elif message_type == "EXECUTION_EVENT":
                self._last_execution = dict(inbound)
                self._apply_execution(
                    inbound,
                    durable_receipt_unavailable=durable_receipt_unavailable,
                )
            elif message_type == "POSITION_EVENT":
                self._apply_position(inbound, durable_receipt_unavailable=durable_receipt_unavailable)
            elif message_type == "SAFETY_EVENT":
                safety_event_id = inbound.get("safety_event_id", inbound.get("receipt_id"))
                valid_correlation = isinstance(safety_event_id, str) and bool(safety_event_id)
                if valid_correlation:
                    if not self._watchdog_failsafe_requires_flat_confirmation:
                        # This native-origin incident may precede any Python
                        # failsafe request. Adopt it as the sole flatten owner
                        # and require its correlated settled-flat proof.
                        self._watchdog_failsafe_requires_flat_confirmation = True
                        self._watchdog_failsafe_activation_message_sequence = (
                            self._execution_message_sequence - 1
                        )
                        self._watchdog_failsafe_available = True
                        self._watchdog_failsafe_deadline_monotonic = (
                            time.monotonic() + _INDEPENDENT_WATCHDOG_GRACE_SECONDS
                        )
                    if self._watchdog_failsafe_safety_event_id != safety_event_id:
                        # A newer independently owned safety action starts a
                        # fresh correlated proof pair. It may supersede an
                        # earlier failed pair only with its own durable event.
                        self._watchdog_failsafe_safety_event_id = safety_event_id
                        self._watchdog_failsafe_safety_event_durable = not durable_receipt_unavailable
                        self._watchdog_failsafe_reconciliation_durable = None
                        self._watchdog_failsafe_last_settlement_sequence = 0
                        self._watchdog_failsafe_settled_reconciliation_count = 0
                        self._watchdog_failsafe_flat_confirmation = None
                        self._watchdog_failsafe_durable_confirmation = None
                    else:
                        self._watchdog_failsafe_safety_event_durable = (
                            self._watchdog_failsafe_safety_event_durable is not False
                            and not durable_receipt_unavailable
                        )
                else:
                    # A signed but uncorrelatable native safety incident can
                    # never be called settled. Stop competing Python authority
                    # and leave the flat proof permanently unresolved.
                    self._watchdog_failsafe_requires_flat_confirmation = True
                    self._watchdog_failsafe_activation_message_sequence = (
                        self._execution_message_sequence - 1
                    )
                    self._watchdog_failsafe_available = True
                    self._watchdog_failsafe_deadline_monotonic = (
                        time.monotonic() + _INDEPENDENT_WATCHDOG_GRACE_SECONDS
                    )
                self._heartbeat_stop.set()
                self._entries_paused = True
                self._retain_safety_lockout_after_flat = True
                self._fault_reason = "NINJATRADER_SAFETY_EVENT:" + str(inbound.get("reason_code", "UNKNOWN"))
                self._watchdog_failsafe_reason = self._fault_reason
                self._request_operational_stop_locked(self._fault_reason)
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    if durable_receipt_unavailable:
                        # No durable audit is available.  Lock in-memory
                        # authority without retrying the failed ledger path.
                        self._state = PaperRuntimeState.LOCKED_OUT
                    else:
                        self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)

    def _consume_early_protective_order_event_locked(self) -> bool:
        """Apply a protective callback which raced ahead of its entry fill."""
        event = self._early_protective_order_event
        self._early_protective_order_event = None
        if event is None:
            return False
        state = str(event.get("order_state", "")).upper()
        reason = self._protective_order_identity_reason_locked(
            event, expected_quantity=self._position_quantity,
        )
        if reason is None and state != "WORKING":
            reason = self._native_order_failure_reason(
                "PROTECTIVE_STOP",
                state or "UNKNOWN",
                event,
            )
        if reason is not None:
            self._fault_reason = reason
            self._retain_safety_lockout_after_flat = True
            self.risk.lock_out(reason)
            self._request_exit(reason, emergency=True)
            return False
        native_order_id = event.get("native_order_id")
        assert isinstance(native_order_id, str) and native_order_id
        self._protective_order_id = native_order_id
        self._snapshot = replace(
            self._snapshot,
            observed_at=_now(),
            protective_stop_state="WORKING",
        )
        return self._request_post_entry_reconciliation_locked()

    def _request_post_entry_reconciliation_locked(self) -> bool:
        """Request one signed snapshot proving the V2 position and stop set."""
        if (
            not self._perpetual_position_profile
            or self._post_entry_reconciliation_pending
            or self._post_entry_reconciliation_complete
            or self._position not in {PaperDirection.LONG, PaperDirection.SHORT}
            or self._state not in {PaperRuntimeState.LONG, PaperRuntimeState.SHORT}
        ):
            return False
        created = _now()
        direction = self._position
        decision = PaperDecision(
            "l3g-pd-" + canonical_hash({
                "reason": "POST_ENTRY_POSITION_RECONCILIATION",
                "direction": direction.value,
                "at": created,
            })[:32],
            self.policy.artifact.policy_id,
            self.policy.artifact.configuration_hash,
            PaperDecisionKind.EXIT,
            created,
            (
                datetime.fromisoformat(
                    normalized_utc(created, "Post-entry reconciliation time").replace("Z", "+00:00")
                ) + timedelta(seconds=5)
            ).isoformat().replace("+00:00", "Z"),
            None,
            PaperDirection.FLAT,
            Decimal("1"),
            {
                "safety": "POST_ENTRY_POSITION_RECONCILIATION",
                "expected_position": direction.value,
                "expected_quantity": 1,
                "expected_owned_protective_orders": 1,
            },
            ((self._last_decision.paper_decision_id if self._last_decision is not None else "post-entry-proof"),),
            (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash({"reason": "POST_ENTRY_POSITION_RECONCILIATION"}),),
            self.policy.artifact.sequence_authority,
            self.policy.artifact.book_completeness,
            False,
            "POST_ENTRY_POSITION_RECONCILIATION",
            self._session_context.session_kind,
            self._session_context.session_id,
            self._session_context.trade_date,
            self._session_context.session_profile_hash,
            self._session_context.session_generation,
            False,
            True,
            False,
        )
        try:
            self.ledger.append(
                "DECISION", decision.payload(), identity=decision.paper_decision_id,
                occurred_at=decision.created_at, execution_session_id=self._execution_session_id(),
            )
            bid, ask, last = self._references()
            intent = self.risk.make_intent(
                decision, reference_bid=bid, reference_ask=ask, reference_last=last,
            )
            self.ledger.append(
                "INTENT", intent.payload(), identity=intent.intent_id,
                occurred_at=intent.created_at, execution_session_id=self._execution_session_id(),
            )
            grant = self.risk.evaluate(intent, self._snapshot, at=created)
            self.ledger.append(
                "RISK_GRANT", grant.payload(), identity=grant.grant_id,
                occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id(),
            )
            if not grant.granted:
                reason = "POST_ENTRY_RECONCILIATION_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes)
                self._fault_reason = reason
                self._retain_safety_lockout_after_flat = True
                self.risk.lock_out(reason)
                self._request_exit(reason, emergency=True)
                return False
            command = self._make_command(
                intent.intent_id, decision.paper_decision_id, grant.grant_id,
                ExecutionAction.RECONCILE, PaperDirection.FLAT,
                "POST_ENTRY_POSITION_RECONCILIATION",
                commissioning=False, strategy_generated=True, scientific_evidence=False,
            )
            # A real fill and stop changed broker state after the prior flat
            # snapshot. Keep Slim red until this exact signed aggregate arrives.
            self._post_entry_reconciliation_pending = True
            self._post_entry_reconciliation_command_id = command.command_id
            self._snapshot = replace(
                self._snapshot,
                observed_at=created,
                reconciliation_current=False,
                order_snapshot_complete=False,
            )
            self._persist_and_send(command, grant)
            return True
        except Exception as error:
            self._post_entry_reconciliation_pending = False
            self._post_entry_reconciliation_command_id = None
            self._retain_safety_lockout_after_flat = True
            self._fail_closed_without_ledger_locked(
                "POST_ENTRY_RECONCILIATION_DURABILITY_OR_SEND_FAILED:" + type(error).__name__,
            )
            return False

    def _apply_reconciliation(
        self, message: Mapping[str, object], *, durable_receipt_unavailable: bool = False,
    ) -> None:
        quantity = message.get("position_quantity")
        orders = message.get("working_order_count")
        entry_orders = message.get("working_entry_count", 0)
        if (
            type(quantity) is not int
            or type(orders) is not int
            or type(entry_orders) is not int
            or orders < 0
            or entry_orders < 0
            or entry_orders > orders
        ):
            self._fault_reason = "RECONCILIATION_MALFORMED"
            self.risk.lock_out(self._fault_reason)
            if self._state is PaperRuntimeState.RECONCILING and not durable_receipt_unavailable:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            elif durable_receipt_unavailable and self._state not in {PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._state = PaperRuntimeState.LOCKED_OUT
            return
        direction = PaperDirection.FLAT if quantity == 0 else PaperDirection.LONG if quantity > 0 else PaperDirection.SHORT
        # Only an affirmative, signed ``false`` proves absence of foreign
        # activity. Missing or malformed authority is unsafe, never clean.
        foreign = message.get("foreign_activity") is not False or abs(quantity) > 1
        self._position = direction
        self._position_quantity = abs(quantity)
        self._last_reconciliation = dict(message)
        self._snapshot = replace(
            self._snapshot, observed_at=_now(), account_name=str(message.get("account_name", "")),
            account_class=str(message.get("account_class", "")), instrument=str(message.get("instrument", "")),
            current_position=direction, current_position_quantity=abs(quantity), working_owned_orders=orders,
            working_entry_orders=entry_orders, foreign_activity=foreign,
            position_snapshot_complete=message.get("position_snapshot_complete") is True,
            order_snapshot_complete=message.get("order_snapshot_complete") is True,
            reconciliation_current=True, execution_bridge_healthy=True,
            protective_stop_state=str(message.get("protective_stop_state", "NONE")),
        )
        self._record_watchdog_reconciliation_locked(
            message, quantity=quantity, orders=orders, entry_orders=entry_orders,
            foreign=foreign, durable_receipt_unavailable=durable_receipt_unavailable,
        )
        if durable_receipt_unavailable:
            # The transport has already established that this callback is
            # authentic, but its durable ingress record failed.  Its physical
            # truth can release the watchdog transport only; it can never
            # produce a clean ledger receipt or normal lifecycle transition.
            return
        receipt_id = str(message.get("receipt_id", "")).strip()
        projection_identity = "l3g-position-snapshot-reconciliation-" + (
            receipt_id or canonical_hash(dict(message))
        )
        # The transport has already persisted the authenticated wire receipt
        # under ``receipt_id``.  This row is a distinct runtime projection of
        # that receipt and must never reuse the transport identity: doing so
        # turns a valid reconciliation into an idempotency conflict after the
        # durable receipt is already committed.
        self.ledger.append(
            "POSITION_SNAPSHOT_RECONCILIATION",
            dict(message),
            identity=projection_identity,
            execution_session_id=self._execution_session_id(),
        )
        if abs(quantity) > 1:
            # Preserve the signed broker quantity, classify the exact breach,
            # and leave physical settlement to the native one-shot safety
            # owner. A Python flatten here could double-close or invert.
            reason = "MAXIMUM_QUANTITY_BREACH"
            self._fault_reason = reason
            self._risk_continuity_fault = reason
            self._entries_paused = True
            self._retain_safety_lockout_after_flat = True
            self._request_operational_stop_locked(reason)
            self.risk.lock_out(reason)
            if not self._native_safety_correlation_active_locked():
                self._activate_independent_watchdog_locked(reason, force=True)
            if self._state not in {
                PaperRuntimeState.LOCKED_OUT,
                PaperRuntimeState.STOPPING,
                PaperRuntimeState.STOPPED,
            }:
                self._transition(PaperRuntimeState.LOCKED_OUT, reason)
            return
        if self._perpetual_position_profile and self._post_entry_reconciliation_pending:
            self._post_entry_reconciliation_pending = False
            self._post_entry_reconciliation_command_id = None
            identity_exact = (
                message.get("account_name") == self.risk.binding.account_name
                and message.get("account_class") == self.risk.binding.account_class
                and message.get("instrument") == self.risk.binding.instrument
            )
            snapshots_complete = (
                message.get("position_snapshot_complete") is True
                and message.get("order_snapshot_complete") is True
            )
            unresolved = (
                self._snapshot.unresolved_command
                or self._snapshot.unresolved_native_order
                or self._snapshot.unresolved_execution
            )
            entry_direction_matches = (
                self._entry_direction in {PaperDirection.LONG, PaperDirection.SHORT}
                and direction is self._entry_direction
                and abs(quantity) == 1
                and self._state.value == direction.value
            )
            positioned_clean = (
                identity_exact
                and not foreign
                and entry_direction_matches
                and orders == 1
                and entry_orders == 0
                and snapshots_complete
                and not unresolved
                and self._snapshot.protective_stop_state == "WORKING"
                and isinstance(self._protective_order_id, str)
                and bool(self._protective_order_id)
            )
            if positioned_clean:
                try:
                    self.ledger.append(
                        "RISK_EVENT_POSITIONED_RECONCILIATION",
                        {
                            **self._session_context.payload(),
                            "account_name": self.risk.binding.account_name,
                            "account_class": self.risk.binding.account_class,
                            "instrument": self.risk.binding.instrument,
                            "position": direction.value,
                            "quantity": abs(quantity),
                            "working_owned_orders": orders,
                            "working_entry_orders": entry_orders,
                            "protective_stop_state": self._snapshot.protective_stop_state,
                            "foreign_activity": False,
                            "effect": "POSITION_AND_SINGLE_PROTECTIVE_ORDER_PROVEN",
                        },
                        identity="l3g-positioned-reconciliation-" + projection_identity,
                        execution_session_id=self._execution_session_id(),
                    )
                except Exception as error:
                    reason = (
                        "POST_ENTRY_RECONCILIATION_DURABILITY_FAILED:"
                        + type(error).__name__
                    )
                    self._fault_reason = reason
                    self._retain_safety_lockout_after_flat = True
                    self.risk.lock_out(reason)
                    self._request_exit(reason, emergency=True)
                    return
                self._post_entry_reconciliation_complete = True
                self._perpetual_flat_blocker = None
                self._align_perpetual_position_to_latest_signal_locked(
                    "POST_ENTRY_POSITION_RECONCILIATION_COMPLETE",
                )
                return

            if not identity_exact:
                reason = "POST_ENTRY_RECONCILIATION_IDENTITY_MISMATCH"
            elif foreign:
                reason = "POST_ENTRY_RECONCILIATION_FOREIGN_ACTIVITY"
            elif not entry_direction_matches:
                reason = "POST_ENTRY_RECONCILIATION_POSITION_MISMATCH"
            elif entry_orders != 0:
                reason = "POST_ENTRY_RECONCILIATION_WORKING_ENTRY_REMAINS"
            elif orders != 1:
                reason = "POST_ENTRY_RECONCILIATION_PROTECTIVE_ORDER_COUNT_NOT_ONE"
            elif not snapshots_complete:
                reason = "POST_ENTRY_RECONCILIATION_INCOMPLETE"
            elif unresolved:
                reason = "POST_ENTRY_RECONCILIATION_UNRESOLVED_EXECUTION_TRUTH"
            else:
                reason = "POST_ENTRY_RECONCILIATION_PROTECTIVE_STOP_NOT_WORKING"
            self._fault_reason = reason
            self._perpetual_flat_blocker = reason if direction is PaperDirection.FLAT else None
            self._retain_safety_lockout_after_flat = True
            self.risk.lock_out(reason)
            if direction is not PaperDirection.FLAT:
                self._request_exit(reason, emergency=True)
            elif orders:
                self._disarm_after_flat = True
                self._cancel_pending_and_reconcile()
            elif self._state not in {
                PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED,
            }:
                self._transition(PaperRuntimeState.LOCKED_OUT, reason)
            return
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
            elif self._operational_session_is_stopping_locked():
                self._complete_operational_stop_locked("OPERATIONAL_STOP_RECONCILIATION_COMPLETE")
            elif self._risk_continuity_fault is not None or self._retain_safety_lockout_after_flat:
                self._transition(
                    PaperRuntimeState.LOCKED_OUT,
                    "FLAT_RECONCILIATION_ENTRY_LOCKOUT_RETAINED",
                )
            elif self._operational_session is not None:
                self._transition(PaperRuntimeState.PAPER_RUNNING, "OPERATIONAL_PAPER_RECONCILIATION_COMPLETE")
                self._maintain_perpetual_position_locked(
                    "EXECUTION_RECONCILIATION_RECOVERY",
                )
            else:
                self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_RECONCILIATION_COMPLETE")

    def _capture_entry_fill_truth_locked(
        self,
        message: Mapping[str, object],
        *,
        direction: PaperDirection,
        price: Decimal,
        quantity: int,
        context: PaperSessionContext | None,
    ) -> None:
        """Record authenticated physical truth before accounting decisions."""
        self._position = direction
        self._position_quantity = quantity
        self._entry_fill_price = price
        self._entry_fill_quantity = quantity
        self._entry_direction = direction
        self._entry_execution = dict(message)
        self._entry_session_context = context
        self._snapshot = replace(
            self._snapshot,
            observed_at=_now(),
            current_position=direction,
            current_position_quantity=quantity,
            position_opened_at=str(message.get("timestamp", _now())),
            protective_stop_state="PENDING",
        )

    def _flatten_ambiguous_entry_fill_locked(self, reason: str) -> None:
        """Revoke entries but preserve the owned emergency-exit authority."""
        self._entry_accounting_ambiguous = True
        self._retain_safety_lockout_after_flat = True
        self._risk_continuity_fault = reason
        self._fault_reason = reason
        self._entries_paused = True
        try:
            self.risk.lock_out(reason)
        except Exception:
            # The runtime-owned exit below still gets one durable attempt. If
            # the ledger is the failing component it will activate the
            # independently owned AddOn watchdog instead.
            self._fault_reason = reason + ":LOCKOUT_EVIDENCE_UNAVAILABLE"
        submitted = self._request_exit(reason, emergency=True)
        if not submitted:
            # Preserve the primary authenticated-fill classification and the
            # legacy lockout contract even when no normal exit command can be
            # built. _request_exit has already activated the independent
            # watchdog/fault evidence for its secondary authority failure.
            self._fault_reason = reason
            self._risk_continuity_fault = reason
            self.risk.restore_lockout(True, reason, None)
            self._state = PaperRuntimeState.LOCKED_OUT

    def _latch_exit_accounting_failure_locked(self, error: Exception) -> None:
        """Keep a real exit fill fail-closed until physical flat is reconciled."""
        reason = "RISK_CONTINUITY_EXIT_ACCOUNTING_FAILED:" + type(error).__name__
        self._fault_reason = reason
        self._risk_continuity_fault = reason
        self._entries_paused = True
        self._retain_safety_lockout_after_flat = True
        try:
            self.risk.lock_out(reason)
        except Exception:
            # The accounting writer may itself be unavailable. Preserve the
            # in-memory denial without pretending that a durable lockout row
            # exists; startup continuity will independently reject the broken
            # ledger boundary.
            self.risk.restore_lockout(True, reason, None)
        if self._state is not PaperRuntimeState.EXIT_PENDING:
            try:
                self._transition(PaperRuntimeState.EXIT_PENDING, reason)
            except Exception:
                # The authenticated fill is physical truth and a subsequent
                # signed POSITION_EVENT must still enter flat reconciliation.
                self._state = PaperRuntimeState.EXIT_PENDING

    def _apply_execution(
        self,
        message: Mapping[str, object],
        *,
        durable_receipt_unavailable: bool = False,
    ) -> None:
        role = str(message.get("order_role", ""))
        execution_id = self._risk_execution_id(message)
        supplied_account = message.get("account_name")
        supplied_instrument = message.get("instrument")
        if (
            (supplied_account is not None and supplied_account != self.risk.binding.account_name)
            or (supplied_instrument is not None and supplied_instrument != self.risk.binding.instrument)
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
        profile_version = self.policy.artifact.entry_profile_version
        fact_context = self._session_context
        callback_context_invalid = False
        context_keys = {
            "session_kind", "session_id", "trade_date",
            "session_profile_hash", "session_generation",
        }
        supplied_context_keys = context_keys.intersection(message)
        if supplied_context_keys:
            if supplied_context_keys != context_keys:
                callback_context_invalid = True
            else:
                try:
                    fact_context = self._record_context(message)
                except RuntimeError:
                    callback_context_invalid = True
        pending_intent = self._pending_intent
        if not supplied_context_keys and role == "ENTRY" and pending_intent is not None:
            try:
                fact_context = context_from_identity(
                    pending_intent.session_kind, pending_intent.session_id,
                    pending_intent.trade_date, pending_intent.session_profile_hash,
                    pending_intent.session_generation,
                )
            except (AttributeError, TypeError, ValueError):
                callback_context_invalid = True
        elif (
            not supplied_context_keys
            and role in {"EXIT", "PROTECTIVE"}
            and self._entry_session_context is not None
        ):
            fact_context = self._entry_session_context
        raw_fact = self._raw_execution_fact(
            message, fact_context, profile_version, price, quantity,
        )
        if execution_id in self._seen_native_execution_ids:
            if self._raw_execution_facts.get(execution_id) != raw_fact:
                self._fault_reason = "RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT"
                self._risk_continuity_fault = self._fault_reason
                self._entries_paused = True
                self.risk.lock_out(self._fault_reason)
                self.ledger.append(
                    "INCIDENT_CONFLICTING_EXECUTION_CALLBACK",
                    {"native_execution_id": execution_id, "effect": "ENTRY_AUTHORITY_LOCKED"},
                    identity="l3g-conflicting-execution-" + canonical_hash({"execution_id": execution_id}),
                    execution_session_id=self._execution_session_id(),
                )
                if self._state not in {
                    PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED,
                }:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                return
            self.ledger.append(
                "INCIDENT_DUPLICATE_EXECUTION_CALLBACK",
                {"native_execution_id": execution_id, "effect": "IDEMPOTENT_NO_STATE_CHANGE"},
                identity="l3g-duplicate-execution-" + canonical_hash({"execution_id": execution_id}),
                execution_session_id=self._execution_session_id(),
            )
            return
        self._seen_native_execution_ids.add(execution_id)
        self._raw_execution_facts[execution_id] = raw_fact
        if role == "ENTRY":
            if message.get("direction") not in {"LONG", "SHORT"}:
                self._fault_reason = "MALFORMED_EXECUTION_EVENT"
                self.risk.lock_out(self._fault_reason)
                return
            expected = PaperDirection(str(message["direction"]))
            self._post_entry_reconciliation_complete = False
            intent = self._pending_intent
            entry_context: PaperSessionContext | None = None
            if intent is not None:
                try:
                    entry_context = context_from_identity(
                        intent.session_kind, intent.session_id, intent.trade_date,
                        intent.session_profile_hash, intent.session_generation,
                    )
                except (AttributeError, TypeError, ValueError):
                    entry_context = None
            # This must precede every lifecycle/accounting refusal below. An
            # authenticated owned fill is a broker fact, not optional state.
            self._capture_entry_fill_truth_locked(
                message,
                direction=expected,
                price=price,
                quantity=quantity,
                context=entry_context,
            )
            if self._state is not PaperRuntimeState.ENTRY_PENDING:
                self._flatten_ambiguous_entry_fill_locked(
                    "UNEXPECTED_ENTRY_EXECUTION_STATE",
                )
                return
            if quantity != 1:
                self._flatten_ambiguous_entry_fill_locked(
                    "ENTRY_EXECUTION_QUANTITY_LIMIT_BREACH",
                )
                return
            if intent is None:
                self._flatten_ambiguous_entry_fill_locked(
                    "FILL_WITHOUT_EXPECTED_ORDER",
                )
                return
            if (
                callback_context_invalid
                or entry_context is None
                or (
                    supplied_context_keys == context_keys
                    and self._context_identity(fact_context)
                    != self._context_identity(entry_context)
                )
            ):
                self._flatten_ambiguous_entry_fill_locked(
                    "ENTRY_EXECUTION_CONTEXT_INVALID",
                )
                return
            if not self._risk_context_allowed(entry_context, profile_version):
                self._flatten_ambiguous_entry_fill_locked(
                    "RISK_CONTINUITY_ENTRY_OFF_SESSION",
                )
                return
            try:
                fill_ok, reason = self.risk.enforce_fill(  # type: ignore[arg-type]
                    expected, intent, price,
                )
                self.ledger.append(
                    "RISK_EVENT_ENTRY_ACCOUNTED",
                    {
                        **entry_context.payload(),
                        "risk_profile_version": self.policy.artifact.entry_profile_version,
                        "native_execution_id": execution_id,
                        "effect": "ENTRY_COUNT_INCREMENTED_ONCE",
                    },
                    identity="l3g-risk-entry-accounted-" + canonical_hash({"execution_id": execution_id}),
                    execution_session_id=self._execution_session_id(),
                )
                self._account_recovered_entry(
                    execution_id, entry_context,
                    self.policy.artifact.entry_profile_version,
                )
                trade_risk = self._trade_date_risk[entry_context.trade_date]
                profile_risk = self._profile_trade_date_risk[
                    (entry_context.trade_date, self.policy.artifact.entry_profile_version)
                ]
                session_key = (
                    entry_context.session_id,
                    self.policy.artifact.entry_profile_version,
                )
                self._snapshot = replace(
                    self._snapshot,
                    session_entry_count=self._session_entry_counts[session_key],
                    trade_date_entry_count=profile_risk.entry_count,
                )
                if self._state is PaperRuntimeState.ENTRY_PENDING:
                    self._transition(
                        PaperRuntimeState.LONG
                        if expected is PaperDirection.LONG
                        else PaperRuntimeState.SHORT,
                        "ENTRY_FILL_CONFIRMED",
                    )
            except Exception as error:
                self._flatten_ambiguous_entry_fill_locked(
                    "RISK_CONTINUITY_ENTRY_ACCOUNTING_FAILED:"
                    + type(error).__name__,
                )
                return
            if not fill_ok:
                self._request_exit(reason, emergency=True)
            elif self._perpetual_position_profile:
                self._consume_early_protective_order_event_locked()
        elif role in {"EXIT", "PROTECTIVE"}:
            owned_exit_after_position = (
                self._position is PaperDirection.FLAT
                and (
                    (
                        role == "EXIT"
                        and self._state is PaperRuntimeState.EXIT_PENDING
                        and isinstance(self._pending_exit_command_id, str)
                        and message.get("command_id") == self._pending_exit_command_id
                    )
                    or (
                        role == "PROTECTIVE"
                        and isinstance(self._protective_order_id, str)
                        and bool(self._protective_order_id)
                        and message.get("native_order_id") == self._protective_order_id
                    )
                )
            )
            if self._position is PaperDirection.FLAT and not owned_exit_after_position:
                self._fault_reason = "UNEXPECTED_EXIT_EXECUTION_STATE"
                self.risk.lock_out(self._fault_reason)
                if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                    self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
                return
            local_entry_accounting_missing = (
                self._entry_fill_price is None
                or self._entry_fill_quantity <= 0
            )
            if role == "PROTECTIVE":
                # The protective order itself already performed the physical
                # exit. Do not submit a duplicate flatten; move directly into
                # settlement and retain this genuine safety shutdown after the
                # signed flat/no-order reconciliation completes.
                reason = "PROTECTIVE_STOP_FILLED"
                self._post_entry_reconciliation_pending = False
                self._post_entry_reconciliation_complete = False
                self._post_entry_reconciliation_command_id = None
                self._early_protective_order_event = None
                if self._perpetual_position_profile:
                    self._pending_five_minute_reversal = None
                    self._retain_safety_lockout_after_flat = True
                    self._fault_reason = reason
                    self._risk_continuity_fault = reason
                    self._entries_paused = True
                    self.risk.lock_out(reason)
                    self._request_operational_stop_locked(reason)
                if self._state is not PaperRuntimeState.EXIT_PENDING:
                    self._transition(PaperRuntimeState.EXIT_PENDING, reason)
            # Final flat truth still requires a position event/reconciliation.
            realized = Decimal("0")
            if self._entry_fill_price is not None and self._entry_fill_quantity > 0:
                points = price - self._entry_fill_price if self._entry_direction is PaperDirection.LONG else self._entry_fill_price - price
                realized = points * Decimal("2") * self._entry_fill_quantity
            entry_context = self._entry_session_context or self._session_context
            if (
                callback_context_invalid
                or self._entry_accounting_ambiguous
                or local_entry_accounting_missing
                or not self._risk_context_allowed(entry_context, profile_version)
            ):
                # A startup reconciliation can discover a real position for
                # which this process has no local entry lifecycle. Its owned
                # emergency exit is still physical truth and must progress to
                # signed flat/no-order reconciliation. Keep entry authority
                # revoked and never manufacture PnL/accounting for it.
                self._fault_reason = (
                    self._risk_continuity_fault
                    or "RISK_CONTINUITY_EXIT_INVALID"
                )
                self._risk_continuity_fault = self._fault_reason
                self._entries_paused = True
                self._retain_safety_lockout_after_flat = True
                # Reconciliation may already have durably locked entry
                # authority under a stronger broker-state reason. Do not
                # churn that latch back to an earlier reason merely to accept
                # the physical exit fact; recurring authority-state identities
                # can span execution sessions and are not lifecycle evidence.
                if not self.risk.status()["locked_out"]:
                    self.risk.lock_out(self._fault_reason)
                self._exit_execution = dict(message)
                self._lifecycle_realized_pnl = realized
                self.ledger.append(
                    "INCIDENT_RISK_CONTINUITY_EXIT_UNACCOUNTED",
                    {
                        "entry_execution_id": None if self._entry_execution is None
                        else self._entry_execution.get("native_execution_id"),
                        "exit_execution_id": execution_id,
                        "realized_pnl_observation": str(realized),
                        "effect": "ENTRY_AUTHORITY_LOCKED_PHYSICAL_SETTLEMENT_REQUIRED",
                    },
                    identity="l3g-risk-exit-unaccounted-" + canonical_hash({
                        "execution_id": execution_id,
                    }),
                    execution_session_id=self._execution_session_id(),
                )
                if durable_receipt_unavailable:
                    self._retain_safety_lockout_after_flat = True
                    self._fail_closed_without_ledger_locked(
                        "EXIT_EXECUTION_RECEIPT_DURABILITY_UNAVAILABLE",
                    )
                else:
                    self._maybe_request_reconciliation_after_exit_locked()
                return
            # Preserve the authenticated exit fact before any accounting
            # operation which can fail. Position settlement still requires a
            # later signed POSITION_EVENT and clean reconciliation.
            self._exit_execution = dict(message)
            self._lifecycle_realized_pnl = realized
            entry = self._entry_execution or {}
            try:
                self.ledger.append(
                    "RISK_EVENT_EXIT_ACCOUNTED",
                    {
                        **entry_context.payload(),
                        "risk_profile_version": self.policy.artifact.entry_profile_version,
                        "exit_execution_id": execution_id,
                        "realized_pnl": str(realized),
                        "effect": "REALIZED_PNL_AND_LOSS_STREAK_APPLIED_ONCE",
                    },
                    identity="l3g-risk-exit-accounted-" + canonical_hash({"execution_id": execution_id}),
                    execution_session_id=self._execution_session_id(),
                )
                self._account_recovered_exit(
                    execution_id, entry_context, realized,
                    self.policy.artifact.entry_profile_version,
                )
                trade_risk = self._trade_date_risk[entry_context.trade_date]
                profile_risk = self._profile_trade_date_risk[
                    (entry_context.trade_date, self.policy.artifact.entry_profile_version)
                ]
                self._snapshot = replace(
                    self._snapshot, observed_at=_now(), daily_realized_pnl=trade_risk.realized_pnl,
                    daily_unrealized_pnl=trade_risk.unrealized_pnl,
                    trade_date_entry_count=profile_risk.entry_count,
                    consecutive_losses=profile_risk.consecutive_losses,
                )
                self.ledger.append(
                    "EXECUTION_REALIZED_PNL",
                    {
                        **entry_context.payload(),
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
                        "exit_execution_id": execution_id,
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
                    identity="l3g-realized-pnl-" + canonical_hash({"execution_id": execution_id}),
                    execution_session_id=self._execution_session_id(),
                )
            except Exception as error:
                self._latch_exit_accounting_failure_locked(error)
                if not durable_receipt_unavailable:
                    self._maybe_request_reconciliation_after_exit_locked()
                return
            if durable_receipt_unavailable:
                self._retain_safety_lockout_after_flat = True
                self._fail_closed_without_ledger_locked(
                    "EXIT_EXECUTION_RECEIPT_DURABILITY_UNAVAILABLE",
                )
            else:
                self._maybe_request_reconciliation_after_exit_locked()
        else:
            self._fault_reason = "EXECUTION_ROLE_INVALID"
            self.risk.lock_out(self._fault_reason)
            if self._state not in {
                PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED,
            }:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)

    def _apply_position(
        self, message: Mapping[str, object], *, durable_receipt_unavailable: bool = False,
    ) -> None:
        quantity = message.get("quantity")
        if type(quantity) is not int:
            self._fault_reason = "POSITION_UPDATE_MISMATCH"
            self.risk.lock_out(self._fault_reason)
            if self._state not in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.STOPPING, PaperRuntimeState.STOPPED}:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        if abs(quantity) > 1:
            # Preserve the signed broker fact instead of leaving status at a
            # stale apparently-safe quantity. The exact-provenance AddOn owns
            # the independent cancel/flatten response as soon as it publishes
            # this event; Python permanently denies further entry authority.
            direction = PaperDirection.LONG if quantity > 0 else PaperDirection.SHORT
            self._position = direction
            self._position_quantity = abs(quantity)
            self._snapshot = replace(
                self._snapshot,
                observed_at=_now(),
                current_position=direction,
                current_position_quantity=abs(quantity),
                foreign_activity=True,
            )
            self._fault_reason = "MAXIMUM_QUANTITY_BREACH"
            self._risk_continuity_fault = self._fault_reason
            self._entries_paused = True
            self._retain_safety_lockout_after_flat = True
            self._request_operational_stop_locked(self._fault_reason)
            self.risk.lock_out(self._fault_reason)
            if not self._native_safety_correlation_active_locked():
                self._activate_independent_watchdog_locked(
                    self._fault_reason, force=True,
                )
            if self._state not in {
                PaperRuntimeState.LOCKED_OUT,
                PaperRuntimeState.STOPPING,
                PaperRuntimeState.STOPPED,
            }:
                self._transition(PaperRuntimeState.LOCKED_OUT, self._fault_reason)
            return
        prior_position = self._position
        self._position = PaperDirection.FLAT if quantity == 0 else PaperDirection.LONG if quantity > 0 else PaperDirection.SHORT
        self._position_quantity = abs(quantity)
        self._snapshot = replace(self._snapshot, observed_at=_now(), current_position=self._position, current_position_quantity=abs(quantity), position_opened_at=None if quantity == 0 else self._snapshot.position_opened_at)
        if quantity == 0 and (
            self._state is PaperRuntimeState.EXIT_PENDING
            or (
                prior_position in {PaperDirection.LONG, PaperDirection.SHORT}
                and self._entry_fill_price is not None
                and self._entry_fill_quantity > 0
            )
        ):
            if durable_receipt_unavailable:
                # Do not manufacture a reconciliation command after inbound
                # durable evidence has failed.  The correlated AddOn snapshot
                # will still update the watchdog latch if it arrives.
                self._state = PaperRuntimeState.LOCKED_OUT
                self._fault_reason = "POSITION_RECEIPT_DURABILITY_UNAVAILABLE"
                self.risk.lock_out(self._fault_reason)
                return
            self._post_exit_position_flat_observed = True
            self._maybe_request_reconciliation_after_exit_locked()

    def _maybe_request_reconciliation_after_exit_locked(self) -> None:
        """Request one post-exit snapshot only after all native exit facts."""
        if (
            self._state is not PaperRuntimeState.EXIT_PENDING
            or self._post_exit_reconciliation_pending
            or not self._post_exit_position_flat_observed
            or self._exit_execution is None
            or not self._post_exit_order_terminal_observed
        ):
            return
        self._post_exit_reconciliation_pending = True
        self._transition(
            PaperRuntimeState.RECONCILING,
            "OWNED_EXIT_SETTLED_PENDING_RECONCILIATION",
        )
        self._request_reconciliation_after_exit()

    def _request_reconciliation_after_exit(self) -> None:
        """Require a new signed flat/order snapshot before lifecycle completion."""
        created_at = _now()
        commissioning = self._commissioning_ownership is not None
        decision = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": "POST_EXIT_RECONCILIATION", "at": created_at})[:32],
            self.policy.artifact.policy_id, self.policy.artifact.configuration_hash, PaperDecisionKind.EXIT, created_at,
            (datetime.fromisoformat(normalized_utc(created_at, "Post-exit reconciliation time").replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"safety": "POST_EXIT_RECONCILIATION"},
            ((self._last_decision.paper_decision_id if self._last_decision is not None else "post-exit-safety"),),
            (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash({"reason": "POST_EXIT_RECONCILIATION"}),), self.policy.artifact.sequence_authority,
            self.policy.artifact.book_completeness, False, "POST_EXIT_RECONCILIATION",
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
        operational_stopping = self._operational_session_is_stopping_locked()
        operational_active = self._operational_session is not None
        pending_reversal = self._pending_five_minute_reversal
        self._pending_five_minute_reversal = None
        self.policy.confirm_flat(str(reconciliation.get("timestamp", _now())))
        self._pending_intent = None
        self._pending_grant = None
        self._entry_authority_artifact = None
        target = (
            PaperRuntimeState.LOCKED_OUT
            if self._retain_safety_lockout_after_flat or self._risk_continuity_fault is not None
            else PaperRuntimeState.READY_DISARMED
            if operational_stopping or self._disarm_after_flat or commissioning
            else PaperRuntimeState.PAUSED
            if self._entries_paused
            else PaperRuntimeState.PAPER_RUNNING
            if operational_active
            else PaperRuntimeState.ARMED_FLAT
        )
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
        self._entry_accounting_ambiguous = False
        self._entry_execution = None
        self._entry_authority_artifact = None
        self._exit_execution = None
        self._protective_order_id = None
        self._post_entry_reconciliation_pending = False
        self._post_entry_reconciliation_complete = False
        self._post_entry_reconciliation_command_id = None
        self._early_protective_order_event = None
        self._lifecycle_realized_pnl = Decimal("0")
        self._entry_session_context = None
        if self._perpetual_position_profile:
            self._activate_risk_snapshot_context_locked(
                self._session_context, reset_evidence=False,
            )
            self._perpetual_entry_attempted_checkpoint = None
        self._post_exit_reconciliation_pending = False
        self._post_exit_position_flat_observed = False
        self._post_exit_order_terminal_observed = False
        self._pending_exit_command_id = None
        self._disarm_after_flat = False
        if operational_stopping:
            self._complete_operational_stop_locked("OPERATIONAL_STOP_FLAT_RECONCILIATION_COMPLETE")
        elif self._perpetual_position_profile and operational_active:
            # The opposite completed bias is already a durable checkpoint.
            # Rebuild entry authority in the *current* context only after the
            # signed flat/no-order reconciliation above.
            self._maintain_perpetual_position_locked(
                "POST_EXIT_FLAT_RECONCILIATION",
            )
        elif (
            pending_reversal is not None
            and operational_active
            and not self._entries_paused
            and self._armed_session is not None
            and self._armed_session.valid_at(_now())
            and self._armed_session.session_id == self._session_context.session_id
            and self._session_context.entry_permitted_at(datetime.now(timezone.utc))
            and pending_reversal.session_id == self._session_context.session_id
            and pending_reversal.session_generation == self._session_context.session_generation
        ):
            self._request_five_minute_reversal_entry(pending_reversal)

    @staticmethod
    def _risk_execution_id(message: Mapping[str, object]) -> str:
        native = message.get("native_execution_id")
        if isinstance(native, str) and native:
            return native
        receipt = message.get("receipt_id")
        if isinstance(receipt, str) and receipt:
            return "receipt:" + receipt
        return "payload:" + canonical_hash(dict(message))

    @staticmethod
    def _risk_context_fact(context: PaperSessionContext) -> tuple[object, ...]:
        return (
            context.session_kind.value, context.session_id, context.trade_date,
            context.session_profile_hash, context.session_generation,
        )

    @classmethod
    def _raw_execution_fact(
        cls,
        message: Mapping[str, object],
        context: PaperSessionContext,
        profile: str,
        price: Decimal,
        quantity: int,
    ) -> tuple[object, ...]:
        return (
            str(message.get("order_role", "")), str(price), quantity,
            message.get("direction"),
            message.get("account_name"), message.get("instrument"),
            profile, *cls._risk_context_fact(context),
        )

    @staticmethod
    def _record_context(record: Mapping[str, object]) -> PaperSessionContext:
        try:
            return context_from_identity(
                PaperSessionKind(str(record["session_kind"])), str(record["session_id"]),
                str(record["trade_date"]), str(record["session_profile_hash"]),
                int(record["session_generation"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("RISK_CONTINUITY_LEDGER_SESSION_INVALID") from exc

    @staticmethod
    def _record_profile(
        record: Mapping[str, object], payload: Mapping[str, object] | None = None,
    ) -> str:
        raw = None if payload is None else payload.get("risk_profile_version")
        if raw is None:
            raw = record.get("entry_profile_version")
        try:
            profile = resolve_paper_profile(str(raw or ""))
        except ValueError as exc:
            raise RuntimeError("RISK_CONTINUITY_LEDGER_PROFILE_INVALID") from exc
        if raw != profile.selection_key:
            raise RuntimeError("RISK_CONTINUITY_LEDGER_PROFILE_INVALID")
        return profile.selection_key

    @staticmethod
    def _risk_context_allowed(context: PaperSessionContext, profile: str) -> bool:
        if context.session_kind is not PaperSessionKind.OFF_SESSION:
            return True
        return (
            profile == FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION
            and context.trade_date != "1970-01-01"
            and context.session_id
            == f"MNQU6:OFF_SESSION:{context.trade_date}"
        )

    @staticmethod
    def _legacy_pnl_context(
        record: Mapping[str, object], payload: Mapping[str, object],
    ) -> PaperSessionContext:
        if all(key in payload for key in (
            "session_kind", "session_id", "trade_date", "session_profile_hash", "session_generation",
        )):
            return LaneIIIPaperRuntime._record_context(payload)
        entry_timestamp = payload.get("entry_timestamp")
        if isinstance(entry_timestamp, str) and entry_timestamp:
            generation = record.get("session_generation")
            resolution = PaperSessionResolver().resolve(
                entry_timestamp, generation=generation if type(generation) is int else 0,
            )
            if resolution.context.session_kind is not PaperSessionKind.OFF_SESSION:
                return resolution.context
        legacy_session_id = payload.get("session_id")
        if isinstance(legacy_session_id, str):
            parts = legacy_session_id.split(":")
            if len(parts) == 3:
                try:
                    kind = PaperSessionKind(parts[1])
                    profile = SESSION_PROFILES[kind]
                    generation = record.get("session_generation")
                    return context_from_identity(
                        kind, legacy_session_id, parts[2], profile.profile_hash,
                        generation if type(generation) is int else 0,
                    )
                except (KeyError, ValueError):
                    pass
        context = LaneIIIPaperRuntime._record_context(record)
        if context.session_kind is PaperSessionKind.OFF_SESSION:
            raise RuntimeError("RISK_CONTINUITY_PNL_SESSION_UNRESOLVED")
        return context

    def _merge_risk_snapshot(self, snapshot: Mapping[str, object]) -> None:
        canonical = validate_risk_continuity_snapshot(snapshot, require_flat=True)
        authority = canonical["authority_lockout"]
        assert isinstance(authority, Mapping)
        self.risk.restore_lockout(
            bool(authority["locked_out"]),
            None if authority["lockout_reason"] is None else str(authority["lockout_reason"]),
            None if authority["lockout_trade_date"] is None else str(authority["lockout_trade_date"]),
        )
        for raw in canonical["trade_dates"]:  # type: ignore[index]
            if not isinstance(raw, Mapping):  # pragma: no cover - validator guarantees this
                continue
            key = str(raw["trade_date"])
            if key in self._trade_date_risk:
                raise RuntimeError("RISK_CONTINUITY_DUPLICATE_BASELINE")
            self._trade_date_risk[key] = _TradeDateRisk(
                Decimal(str(raw["realized_pnl"])),
                Decimal(str(raw["unrealized_pnl"])),
                int(raw["entry_count"]),
            )
        for raw in canonical["profile_trade_dates"]:  # type: ignore[index]
            if not isinstance(raw, Mapping):  # pragma: no cover - validator guarantees this
                continue
            key = (str(raw["trade_date"]), str(raw["profile"]))
            if key in self._profile_trade_date_risk:
                raise RuntimeError("RISK_CONTINUITY_DUPLICATE_PROFILE_BASELINE")
            self._profile_trade_date_risk[key] = _ProfileTradeDateRisk(
                int(raw["entry_count"]), int(raw["consecutive_losses"]),
            )
        for raw in canonical["sessions"]:  # type: ignore[index]
            if not isinstance(raw, Mapping):  # pragma: no cover - validator guarantees this
                continue
            context = self._record_context(raw)
            key = (context.session_id, str(raw["profile"]))
            self._session_entry_counts[key] = int(raw["entry_count"])
            self._session_risk_contexts[key] = context
            self._session_pnl[key] = Decimal(str(raw["realized_pnl"]))
        self._entry_execution_ids.update(str(value) for value in canonical["entry_execution_ids"])  # type: ignore[index]
        self._exit_execution_ids.update(str(value) for value in canonical["exit_execution_ids"])  # type: ignore[index]
        self._imported_execution_ids.update(self._entry_execution_ids | self._exit_execution_ids)
        self._seen_native_execution_ids.update(self._entry_execution_ids | self._exit_execution_ids)

    def _account_recovered_entry(
        self, execution_id: str, context: PaperSessionContext, profile: str,
    ) -> None:
        fact = (*self._risk_context_fact(context), profile)
        if execution_id in self._entry_execution_ids:
            existing = self._entry_accounting_facts.get(execution_id)
            if existing is None:
                raise RuntimeError("RISK_CONTINUITY_REPLAY_UNVERIFIABLE")
            if existing != fact:
                raise RuntimeError("RISK_CONTINUITY_ENTRY_EVIDENCE_CONFLICT")
            return
        if execution_id in self._exit_execution_ids:
            raise RuntimeError("RISK_CONTINUITY_EXECUTION_ROLE_CONFLICT")
        if not self._risk_context_allowed(context, profile):
            raise RuntimeError("RISK_CONTINUITY_ENTRY_OFF_SESSION")
        self._entry_accounting_facts[execution_id] = fact
        self._entry_execution_ids.add(execution_id)
        self._seen_native_execution_ids.add(execution_id)
        bucket = self._trade_date_risk.setdefault(context.trade_date, _TradeDateRisk())
        bucket.entry_count += 1
        profile_bucket = self._profile_trade_date_risk.setdefault(
            (context.trade_date, profile), _ProfileTradeDateRisk(),
        )
        profile_bucket.entry_count += 1
        session_key = (context.session_id, profile)
        self._session_entry_counts[session_key] = self._session_entry_counts.get(session_key, 0) + 1
        self._session_risk_contexts[session_key] = context

    def _account_recovered_exit(
        self, execution_id: str, context: PaperSessionContext, realized: Decimal,
        profile: str,
    ) -> None:
        fact = (*self._risk_context_fact(context), profile, str(realized))
        if execution_id in self._exit_execution_ids:
            existing = self._exit_accounting_facts.get(execution_id)
            if existing is None:
                raise RuntimeError("RISK_CONTINUITY_REPLAY_UNVERIFIABLE")
            if existing != fact:
                raise RuntimeError("RISK_CONTINUITY_EXIT_EVIDENCE_CONFLICT")
            return
        if execution_id in self._entry_execution_ids:
            raise RuntimeError("RISK_CONTINUITY_EXECUTION_ROLE_CONFLICT")
        if not realized.is_finite() or not self._risk_context_allowed(context, profile):
            raise RuntimeError("RISK_CONTINUITY_EXIT_INVALID")
        self._exit_accounting_facts[execution_id] = fact
        self._exit_execution_ids.add(execution_id)
        self._seen_native_execution_ids.add(execution_id)
        bucket = self._trade_date_risk.setdefault(context.trade_date, _TradeDateRisk())
        bucket.realized_pnl += realized
        bucket.unrealized_pnl = Decimal("0")
        profile_bucket = self._profile_trade_date_risk.setdefault(
            (context.trade_date, profile), _ProfileTradeDateRisk(),
        )
        profile_bucket.consecutive_losses = (
            profile_bucket.consecutive_losses + 1 if realized < 0
            else 0 if realized > 0 else profile_bucket.consecutive_losses
        )
        session_key = (context.session_id, profile)
        self._session_risk_contexts[session_key] = context
        self._session_pnl[session_key] = self._session_pnl.get(session_key, Decimal("0")) + realized

    def _recover_risk_continuity(self, external: Mapping[str, object] | None) -> None:
        if Path(str(self.ledger.path) + ".risk-authority-pending.json").exists():
            raise RuntimeError("RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED")
        if external is not None:
            canonical = validate_risk_continuity_snapshot(external, require_flat=True)
            self.ledger.append(
                "RISK_EVENT_CONTINUITY_IMPORTED",
                {"snapshot": canonical, "effect": "RISK_LIMITS_ONLY_NO_STRATEGY_AUTHORITY"},
                identity="l3g-risk-continuity-import-" + canonical_hash(canonical),
            )

        imported = False
        open_entry: tuple[Decimal, int, PaperDirection, PaperSessionContext, str] | None = None
        for record in self.ledger.risk_continuity_records():
            kind = str(record.get("kind", ""))
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                raise RuntimeError("RISK_CONTINUITY_LEDGER_PAYLOAD_INVALID")
            if kind == "RISK_EVENT_CONTINUITY_IMPORTED":
                if imported or self._entry_execution_ids or self._exit_execution_ids:
                    raise RuntimeError("RISK_CONTINUITY_IMPORT_ORDER_INVALID")
                snapshot = payload.get("snapshot")
                if not isinstance(snapshot, Mapping):
                    raise RuntimeError("RISK_CONTINUITY_IMPORTED_SNAPSHOT_INVALID")
                self._merge_risk_snapshot(snapshot)
                imported = True
                continue
            if kind == "RISK_EVENT_AUTHORITY_LOCKOUT":
                reason = payload.get("lockout_reason")
                lockout_trade_date = payload.get("lockout_trade_date")
                if (
                    payload.get("locked_out") is not True
                    or not isinstance(reason, str) or not reason
                    or (
                        lockout_trade_date is not None
                        and not isinstance(lockout_trade_date, str)
                    )
                ):
                    raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID")
                if lockout_trade_date is not None:
                    try:
                        date.fromisoformat(lockout_trade_date)
                    except ValueError as exc:
                        raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID") from exc
                self.risk.restore_lockout(True, reason, lockout_trade_date)
                continue
            if kind == "RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED":
                status = self.risk.status()
                effective_trade_date = payload.get("effective_trade_date")
                if (
                    payload.get("locked_out") is not False
                    or payload.get("lockout_reason") is not None
                    or payload.get("lockout_trade_date") is not None
                    or not isinstance(effective_trade_date, str)
                    or status.get("lockout_reason") != "DAILY_LOSS_LIMIT"
                    or status.get("lockout_trade_date") == effective_trade_date
                ):
                    raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_CLEAR_INVALID")
                try:
                    date.fromisoformat(effective_trade_date)
                except ValueError as exc:
                    raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_CLEAR_INVALID") from exc
                self.risk.restore_lockout(False, None, None)
                continue
            if kind == "RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED":
                status = self.risk.status()
                expected_keys = {
                    "schema", "request_id", "request_hash", "operator", "incident",
                    "preserved_lockouts", "acknowledgement", "reconciliation",
                    "account", "environment", "instrument", "maximum_quantity",
                    "live_capital", "locked_out", "cleared_lockout_reason", "effect",
                    "session_family",
                }
                incident = payload.get("incident")
                lockouts = payload.get("preserved_lockouts")
                acknowledgement = payload.get("acknowledgement")
                reconciliation = payload.get("reconciliation")
                if (
                    set(payload) != expected_keys
                    or payload.get("schema") != "lane-iii-reconciliation-recovery-v1"
                    or payload.get("operator") != "Joseph"
                    or payload.get("account") != "Sim101"
                    or payload.get("environment") != "LOCAL_SIMULATION"
                    or payload.get("instrument") != "MNQ SEP26"
                    or payload.get("maximum_quantity") != 1
                    or payload.get("live_capital") != "DENIED"
                    or payload.get("locked_out") is not False
                    or payload.get("cleared_lockout_reason") != "RECONCILIATION_BLOCKED"
                    or payload.get("effect") != "ONLY_RECONCILIATION_BLOCKED_CLEARED"
                    or status.get("lockout_reason") != "RECONCILIATION_BLOCKED"
                    or not isinstance(incident, Mapping)
                    or not isinstance(lockouts, list) or len(lockouts) != 2
                    or not all(isinstance(value, Mapping) for value in lockouts)
                    or not isinstance(acknowledgement, Mapping)
                    or not isinstance(reconciliation, Mapping)
                ):
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_INVALID")
                assert isinstance(incident, Mapping)
                assert isinstance(acknowledgement, Mapping)
                assert isinstance(reconciliation, Mapping)
                incident_sequence = incident.get("ledger_sequence")
                lockout_sequences = [value.get("ledger_sequence") for value in lockouts]
                if (
                    type(incident_sequence) is not int
                    or not all(type(value) is int for value in lockout_sequences)
                    or not incident_sequence < lockout_sequences[0] < lockout_sequences[1]
                    or lockouts[-1].get("lockout_reason") != "RECONCILIATION_BLOCKED"
                ):
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_INVALID")
                coordinate_sets = (
                    (incident, self.ledger.record_by_sequence(int(incident_sequence))),
                    (lockouts[0], self.ledger.record_by_sequence(int(lockout_sequences[0]))),
                    (lockouts[1], self.ledger.record_by_sequence(int(lockout_sequences[1]))),
                )
                if any(
                    stored is None
                    or coordinate.get("record_hash") != stored.get("record_hash")
                    for coordinate, stored in coordinate_sets
                ):
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_REFERENCE_INVALID")
                ack_identity = acknowledgement.get("identity")
                evidence_identity = reconciliation.get("identity")
                if not isinstance(ack_identity, str) or not isinstance(evidence_identity, str):
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_REFERENCE_INVALID")
                ack_record = self.ledger.record_by_identity(ack_identity)
                evidence_record = self.ledger.record_by_identity(evidence_identity)
                if ack_record is None or evidence_record is None:
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_REFERENCE_INVALID")
                ack_envelope = ack_record.get("record")
                evidence_envelope = evidence_record.get("record")
                ack_payload = ack_envelope.get("payload") if isinstance(ack_envelope, Mapping) else None
                evidence_payload = (
                    evidence_envelope.get("payload") if isinstance(evidence_envelope, Mapping) else None
                )
                request_hash = payload.get("request_hash")
                if (
                    acknowledgement.get("ledger_sequence") != ack_record.get("ledger_sequence")
                    or acknowledgement.get("record_hash") != ack_record.get("record_hash")
                    or reconciliation.get("ledger_sequence") != evidence_record.get("ledger_sequence")
                    or reconciliation.get("record_hash") != evidence_record.get("record_hash")
                    or not isinstance(ack_payload, Mapping)
                    or not isinstance(evidence_payload, Mapping)
                    or ack_payload.get("request_hash") != request_hash
                    or evidence_payload.get("request_hash") != request_hash
                    or evidence_payload.get("evidence_digest") != reconciliation.get("evidence_digest")
                    or evidence_payload.get("transport_command_delta") != 0
                ):
                    raise RuntimeError("RISK_CONTINUITY_RECONCILIATION_CLEAR_REFERENCE_INVALID")
                self.risk.restore_lockout(False, None, None)
                continue
            if kind == "EXECUTION":
                role = str(payload.get("order_role", ""))
                execution_id = self._risk_execution_id(payload)
                record_profile = self._record_profile(record, payload)
                context = self._record_context(record)
                price = self._decimal(payload.get("price"))
                quantity = payload.get("quantity")
                direction_value = payload.get("direction")
                if price is None or type(quantity) is not int or quantity <= 0:
                    raise RuntimeError("RISK_CONTINUITY_EXECUTION_FACT_INVALID")
                fact = self._raw_execution_fact(
                    payload, context, record_profile, price, quantity,
                )
                existing_fact = self._raw_execution_facts.get(execution_id)
                if existing_fact is not None:
                    if existing_fact != fact:
                        raise RuntimeError("RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT")
                    continue
                if execution_id in self._imported_execution_ids:
                    raise RuntimeError("RISK_CONTINUITY_REPLAY_UNVERIFIABLE")
                self._raw_execution_facts[execution_id] = fact
                self._seen_native_execution_ids.add(execution_id)
                if role == "ENTRY":
                    self._account_recovered_entry(execution_id, context, record_profile)
                    if direction_value not in {"LONG", "SHORT"}:
                        raise RuntimeError("RISK_CONTINUITY_ENTRY_DIRECTION_INVALID")
                    direction = PaperDirection(str(direction_value))
                    if open_entry is not None:
                        raise RuntimeError("RISK_CONTINUITY_ENTRY_LIFECYCLE_INVALID")
                    open_entry = (price, quantity, direction, context, record_profile)
                elif role in {"EXIT", "PROTECTIVE"}:
                    if open_entry is None:
                        raise RuntimeError("RISK_CONTINUITY_EXIT_WITHOUT_ENTRY")
                    entry_price, entry_quantity, direction, context, entry_profile = open_entry
                    if record_profile != entry_profile:
                        raise RuntimeError("RISK_CONTINUITY_LIFECYCLE_PROFILE_MISMATCH")
                    points = price - entry_price if direction is PaperDirection.LONG else entry_price - price
                    self._account_recovered_exit(
                        execution_id, context, points * Decimal("2") * min(entry_quantity, quantity),
                        entry_profile,
                    )
                    open_entry = None
                else:
                    raise RuntimeError("RISK_CONTINUITY_EXECUTION_ROLE_INVALID")
                continue
            if kind == "RISK_EVENT_ENTRY_ACCOUNTED":
                self._account_recovered_entry(
                    self._risk_execution_id(payload), self._record_context(payload),
                    self._record_profile(record, payload),
                )
                continue
            if kind in {"RISK_EVENT_EXIT_ACCOUNTED", "EXECUTION_REALIZED_PNL"}:
                execution_id = str(payload.get("exit_execution_id") or self._risk_execution_id(payload))
                try:
                    realized = Decimal(str(payload["realized_pnl"]))
                except (KeyError, ArithmeticError, ValueError) as exc:
                    raise RuntimeError("RISK_CONTINUITY_REALIZED_PNL_INVALID") from exc
                context = self._legacy_pnl_context(record, payload)
                self._account_recovered_exit(
                    execution_id, context, realized, self._record_profile(record, payload),
                )

        if open_entry is not None or len(self._entry_execution_ids) != len(self._exit_execution_ids):
            raise RuntimeError("RISK_CONTINUITY_OPEN_LIFECYCLE_UNRESOLVED")

    def risk_continuity_snapshot(self) -> dict[str, object]:
        """Export the cumulative exchange-trade-date budget without authority."""
        with self._lock:
            trade_dates = [
                {
                    "trade_date": trade_date,
                    "realized_pnl": str(value.realized_pnl),
                    "unrealized_pnl": str(value.unrealized_pnl),
                    "entry_count": value.entry_count,
                }
                for trade_date, value in self._trade_date_risk.items()
            ]
            profile_trade_dates = [
                {
                    "trade_date": trade_date,
                    "profile": profile,
                    "entry_count": value.entry_count,
                    "consecutive_losses": value.consecutive_losses,
                }
                for (trade_date, profile), value in self._profile_trade_date_risk.items()
            ]
            sessions = [
                {
                    "profile": profile,
                    "session_kind": context.session_kind.value,
                    "session_family": context.session_family.value,
                    "session_id": context.session_id,
                    "trade_date": context.trade_date,
                    "session_profile_hash": context.session_profile_hash,
                    "session_generation": context.session_generation,
                    "entry_count": self._session_entry_counts.get((session_id, profile), 0),
                    "realized_pnl": str(self._session_pnl.get((session_id, profile), Decimal("0"))),
                }
                for (session_id, profile), context in self._session_risk_contexts.items()
            ]
            # This external monotonic fence is advanced while the runtime lock
            # excludes a concurrent execution callback. Remembered startup can
            # therefore detect an older same-UUID database before construction.
            try:
                self.ledger.publish_risk_continuity_anchor()
            except RuntimeError as error:
                # Keep status/reconciliation available while revoking entries.
                # The missing/mismatched external fence is itself durable
                # evidence that continuity cannot be asserted.
                self._risk_continuity_fault = str(error)
                self._entries_paused = True
                self.risk.restore_lockout(True, self._risk_continuity_fault, None)
            records, source_ledger = self.ledger.risk_continuity_evidence()
            coverage_complete = (
                self._risk_continuity_fault is None
                and len(self._entry_execution_ids) == len(self._exit_execution_ids)
                and sum(value.entry_count for value in self._trade_date_risk.values())
                == len(self._entry_execution_ids)
            )
            for record in records:
                kind = str(record.get("kind", ""))
                payload = record.get("payload")
                if not isinstance(payload, Mapping):
                    coverage_complete = False
                    break
                if kind == "EXECUTION":
                    role = str(payload.get("order_role", ""))
                    execution_id = self._risk_execution_id(payload)
                    if role == "ENTRY" and execution_id not in self._entry_execution_ids:
                        coverage_complete = False
                        break
                    if role in {"EXIT", "PROTECTIVE"} and execution_id not in self._exit_execution_ids:
                        coverage_complete = False
                        break
                    if role not in {"ENTRY", "EXIT", "PROTECTIVE"}:
                        coverage_complete = False
                        break
                elif kind == "RISK_EVENT_ENTRY_ACCOUNTED":
                    if self._risk_execution_id(payload) not in self._entry_execution_ids:
                        coverage_complete = False
                        break
                elif kind in {"RISK_EVENT_EXIT_ACCOUNTED", "EXECUTION_REALIZED_PNL"}:
                    execution_id = str(payload.get("exit_execution_id") or self._risk_execution_id(payload))
                    if execution_id not in self._exit_execution_ids:
                        coverage_complete = False
                        break
            source_ledger["coverage_complete"] = coverage_complete
            risk_status = self.risk.status()
            snapshot = {
                "schema": RISK_CONTINUITY_SNAPSHOT_SCHEMA,
                "generated_at": _now(),
                "account_name": self.risk.binding.account_name,
                "account_class": self.risk.binding.account_class,
                "instrument": self.risk.binding.instrument,
                "source_profile": self.policy.artifact.entry_profile_version,
                "source_ledger": source_ledger,
                "authority_lockout": {
                    "locked_out": risk_status["locked_out"],
                    "lockout_reason": risk_status["lockout_reason"],
                    "lockout_trade_date": risk_status["lockout_trade_date"],
                },
                "trade_dates": trade_dates,
                "profile_trade_dates": profile_trade_dates,
                "sessions": sessions,
                "entry_execution_ids": sorted(self._entry_execution_ids),
                "exit_execution_ids": sorted(self._exit_execution_ids),
            }
        return validate_risk_continuity_snapshot(snapshot)

    def _request_five_minute_reversal_entry(self, reversal: PaperDecision) -> None:
        """Enter the opposite side only after signed flat/order reconciliation."""
        target_value = reversal.family_summary.get("target_position")
        target = (
            PaperDirection.LONG if target_value == PaperDirection.LONG.value
            else PaperDirection.SHORT if target_value == PaperDirection.SHORT.value
            else None
        )

        if target is None:
            self.ledger.append(
                "INCIDENT_FIVE_MINUTE_REVERSAL_REFUSED",
                {"reason": "INVALID_REVERSAL_TARGET", "source_decision_id": reversal.paper_decision_id},
                identity="l3g-five-minute-reversal-refused-" + reversal.paper_decision_id,
                execution_session_id=self._execution_session_id(),
            )
            return
        created = _now()
        kind = PaperDecisionKind.LONG if target is PaperDirection.LONG else PaperDecisionKind.SHORT
        hypothesis = (
            HypothesisKind.BULLISH_REVERSAL
            if target is PaperDirection.LONG else HypothesisKind.BEARISH_CONTINUATION
        )
        summary = {
            **dict(reversal.family_summary),
            "action": "REVERSE_ENTRY",
            "reversal_stage": "FLAT_RECONCILED",
            "source_reversal_decision_id": reversal.paper_decision_id,
            "reconciliation_timestamp": reconciliation_timestamp
            if (reconciliation_timestamp := self._last_reconciliation_timestamp()) is not None
            else created,
        }
        payload = {
            "source_reversal_decision_id": reversal.paper_decision_id,
            "created_at": created,
            "target_position": target.value,
            "session_id": self._session_context.session_id,
            "session_generation": self._session_context.session_generation,
            "policy_hash": self.policy.artifact.configuration_hash,
        }
        decision = PaperDecision(
            deterministic_id("l3g-pd-", payload),
            self.policy.artifact.policy_id,
            self.policy.artifact.configuration_hash,
            kind,
            created,
            (datetime.fromisoformat(created.replace("Z", "+00:00")) + timedelta(seconds=self.policy.artifact.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
            hypothesis,
            target,
            reversal.relative_support,
            summary,
            (reversal.paper_decision_id,),
            (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash(reversal.payload()),),
            self.policy.artifact.sequence_authority,
            self.policy.artifact.book_completeness,
            False,
            f"FIVE_MINUTE_REVERSE_ENTRY_{target.value}",
            self._session_context.session_kind,
            self._session_context.session_id,
            self._session_context.trade_date,
            self._session_context.session_profile_hash,
            self._session_context.session_generation,
        )
        self._last_decision = decision
        self._last_qualifying_entry_decision = decision
        self.ledger.append(
            "DECISION", decision.payload(), identity=decision.paper_decision_id,
            occurred_at=decision.created_at, execution_session_id=self._execution_session_id(),
        )
        self._request_entry(decision)

    def _last_reconciliation_timestamp(self) -> str | None:
        value = None if self._last_reconciliation is None else self._last_reconciliation.get("timestamp")
        return str(value) if isinstance(value, str) else None

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
                self._snapshot.quote_observed_at, self.risk.profile.quote_maximum_age_seconds, at,
            ),
            "classified_trade": self._freshness_gate(
                self._snapshot.classified_trade_observed_at,
                self.risk.profile.classified_trade_maximum_age_seconds, at,
            ),
            "depth_mutation": self._freshness_gate(
                self._snapshot.depth_mutation_observed_at,
                self.risk.profile.depth_mutation_maximum_age_seconds, at,
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

    def operational_paper_readiness(
        self,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
    ) -> dict[str, object]:
        """Run the authority-free production start proof for an operator session.

        The shared validator is intentionally reused, but this method never
        reserves commissioning ownership, creates a one-shot credential, or
        submits a commissioning entry.
        """
        result = self.commissioning_rehearsal(ledger_preflight)
        if self._perpetual_position_profile:
            raw_reasons = [
                str(value) for value in result.get("blocking_reasons", [])
                if isinstance(value, str)
            ]
            deferred = [
                reason for reason in raw_reasons
                if reason in _PERPETUAL_DEFERRED_ENTRY_REASONS
            ]
            blocking = [
                reason for reason in raw_reasons
                if reason not in _PERPETUAL_DEFERRED_ENTRY_REASONS
            ]
            with self._lock:
                checkpoint = self._latest_five_minute_direction_checkpoint
                evidence = result.get("ledger")
                verified_through = (
                    evidence.get("verified_through_sequence")
                    if isinstance(evidence, Mapping) else None
                )
                if checkpoint is not None and self._perpetual_signal_requires_start_verification:
                    self._perpetual_signal_ledger_verified = (
                        type(verified_through) is int
                        and verified_through >= int(checkpoint["ledger_sequence"])
                    )
                    if self._perpetual_signal_ledger_verified:
                        self._perpetual_signal_requires_start_verification = False
                    if not self._perpetual_signal_ledger_verified:
                        deferred.append("FIVE_MINUTE_SIGNAL_LEDGER_UNVERIFIED")
                elif "NO_COMPLETED_FIVE_MINUTE_SIGNAL" not in deferred:
                    deferred.append("NO_COMPLETED_FIVE_MINUTE_SIGNAL")
            result = {
                **result,
                "result": "READY" if not blocking else "BLOCKED",
                "blocking_reasons": list(dict.fromkeys(blocking)),
                "deferred_entry_reasons": list(dict.fromkeys(deferred)),
            }
            hash_payload = dict(result)
            hash_payload.pop("snapshot_hash", None)
            result["snapshot_hash"] = canonical_hash(hash_payload)
        return {
            **result,
            "schema": "lane-iii-phase-g-operational-paper-readiness-v1",
            "operational_paper": True,
            "commissioning": False,
        }

    def operational_paper_start(
        self,
        request_id: str,
        ledger_preflight: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
    ) -> dict[str, object]:
        """Start one continuous Sim101 paper session without commissioning it."""
        if (
            not isinstance(request_id, str) or not 8 <= len(request_id) <= 128
            or any(not (character.isalnum() or character in "-_.:") for character in request_id)
        ):
            raise ValueError("Operational paper start requires an 8-128 character idempotency request ID.")
        with self._lock:
            existing = self._operational_session
            if existing is not None:
                return {
                    "started": existing.stopping_reason is None,
                    "operational_paper": True,
                    "request_id": existing.request_id,
                    "idempotent_replay": existing.request_id == request_id,
                    "reason_codes": (
                        "OPERATIONAL_PAPER_SESSION_ALREADY_ACTIVE"
                        if existing.stopping_reason is None else "OPERATIONAL_PAPER_SESSION_STOPPING",
                    ),
                    "state": self._state.value,
                }

        readiness = self.operational_paper_readiness(ledger_preflight)
        reasons = tuple(str(value) for value in readiness.get("blocking_reasons", []) if isinstance(value, str))
        if readiness.get("result") != "READY" or reasons:
            with self._lock:
                return {
                    "started": False,
                    "operational_paper": True,
                    "request_id": request_id,
                    "idempotent_replay": False,
                    "reason_codes": reasons or ("OPERATIONAL_PAPER_PREFLIGHT_FAILED",),
                    "state": self._state.value,
                    "readiness": readiness,
                }

        with self._lock:
            if self._operational_session is not None:
                return {
                    "started": self._operational_session.stopping_reason is None,
                    "operational_paper": True,
                    "request_id": self._operational_session.request_id,
                    "idempotent_replay": self._operational_session.request_id == request_id,
                    "reason_codes": ("OPERATIONAL_PAPER_SESSION_ALREADY_ACTIVE",),
                    "state": self._state.value,
                }
            try:
                with self.ledger.authority_capacity_fence():
                    if self._state is not PaperRuntimeState.READY_DISARMED:
                        return {
                            "started": False,
                            "operational_paper": True,
                            "request_id": request_id,
                            "idempotent_replay": False,
                            "reason_codes": ("STATE_NOT_READY_DISARMED",),
                            "state": self._state.value,
                        }
                    self._transition(PaperRuntimeState.STARTING, "OPERATOR_OPERATIONAL_PAPER_START")
                    started_at = _now()
                    session = _OperationalPaperSession(
                        request_id=request_id,
                        started_at=started_at,
                        context=self._session_context,
                        ledger_preflight=dict(readiness.get("ledger") or {}),
                    )
                    self.ledger.append(
                        "SESSION_OPERATIONAL_PAPER_STARTED",
                        {
                            **session.context.payload(),
                            "request_id": request_id,
                            "started_at": started_at,
                            "classification": "OPERATIONAL_PAPER_SESSION",
                            "pre_start_verification": dict(readiness.get("ledger") or {}),
                            "live_capital": "DENIED",
                        },
                        identity="l3g-operational-paper-start-" + canonical_hash({
                            "request_id": request_id,
                            "started_at": started_at,
                        }),
                        execution_session_id=self._execution_session_id(),
                    )
                    self._operational_session = session
                    armed = self._arm_with_capacity_fence_locked(
                        target_state=PaperRuntimeState.PAPER_RUNNING,
                        allow_starting=True,
                    )
                    if not armed.get("armed"):
                        self._operational_session = None
                        if self._state is PaperRuntimeState.STARTING:
                            self._transition(PaperRuntimeState.READY_DISARMED, "OPERATIONAL_PAPER_START_REFUSED")
                        return {
                            **armed,
                            "started": False,
                            "operational_paper": True,
                            "request_id": request_id,
                            "idempotent_replay": False,
                        }
                    if self._perpetual_position_profile:
                        self._calculate_perpetual_startup_signal_locked(started_at)
                        self._maintain_perpetual_position_locked(
                            "OPERATIONAL_START",
                        )
                    return {
                        **armed,
                        "started": True,
                        "operational_paper": True,
                        "request_id": request_id,
                        "idempotent_replay": False,
                        "readiness": readiness,
                    }
            except LedgerCapacityError as error:
                self._pause_for_ledger_capacity_locked(error.capacity)
                return {
                    "started": False,
                    "operational_paper": True,
                    "request_id": request_id,
                    "idempotent_replay": False,
                    "reason_codes": ("LEDGER_CAPACITY_INADEQUATE",),
                    "state": self._state.value,
                }

    def arm(self) -> dict[str, object]:
        with self._lock:
            if self._perpetual_position_profile:
                return {
                    "armed": False,
                    "reason_codes": ("PROFILE_REQUIRES_OPERATIONAL_START",),
                    "state": self._state.value,
                }
            try:
                with self.ledger.authority_capacity_fence():
                    return self._arm_with_capacity_fence_locked()
            except LedgerCapacityError as error:
                self._pause_for_ledger_capacity_locked(error.capacity)
                return {
                    "armed": False,
                    "reason_codes": ("LEDGER_CAPACITY_INADEQUATE",),
                    "state": self._state.value,
                }

    def _arm_with_capacity_fence_locked(
        self,
        *,
        target_state: PaperRuntimeState = PaperRuntimeState.ARMED_FLAT,
        allow_starting: bool = False,
    ) -> dict[str, object]:
        """Persist an arm outcome while the ledger capacity/order fence is held."""
        perpetual_operation = (
            self._perpetual_position_profile
            and target_state is PaperRuntimeState.PAPER_RUNNING
            and self._operational_session is not None
        )
        if self._perpetual_position_profile and not perpetual_operation:
            return {
                "armed": False,
                "reason_codes": ("PROFILE_REQUIRES_OPERATIONAL_START",),
                "state": self._state.value,
            }
        if self._entry_owner is not PaperEntryOwner.NONE:
            return {"armed": False, "reason_codes": ("COMMISSIONING_OWNERSHIP_ACTIVE",), "state": self._state.value}
        if self._state is not PaperRuntimeState.READY_DISARMED and not (
            allow_starting and self._state is PaperRuntimeState.STARTING
        ):
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
        if (
            not perpetual_operation
            and (
                context.session_kind is PaperSessionKind.OFF_SESSION
                or current.context.session_id != context.session_id
            )
        ):
            reasons = ("NO_CURRENT_EVENT_SESSION",)
            self.ledger.append("RISK_EVENT_ARM_ATTEMPT", {**context.payload(), "allowed": False, "reason_codes": reasons, "authority_hash": canonical_hash(self.authority.authority_payload())})
            return {"armed": False, "reason_codes": reasons, "state": self._state.value}
        if perpetual_operation:
            all_reasons = self.risk.preflight_reasons(self._snapshot, at=now)
            reasons = tuple(
                reason for reason in all_reasons
                if reason not in _PERPETUAL_DEFERRED_ENTRY_REASONS
            )
            deferred_entry_reasons = tuple(
                reason for reason in all_reasons
                if reason in _PERPETUAL_DEFERRED_ENTRY_REASONS
            )
            allowed = not reasons
        else:
            allowed, reasons = self.risk.preflight(self._snapshot, at=now)
            deferred_entry_reasons = ()
        self.ledger.append(
            "RISK_EVENT_ARM_ATTEMPT",
            {
                **context.payload(), "allowed": allowed, "reason_codes": reasons,
                "deferred_entry_reasons": deferred_entry_reasons,
                "authority_hash": canonical_hash(self.authority.authority_payload()),
            },
        )
        if not allowed:
            return {"armed": False, "reason_codes": reasons, "state": self._state.value}
        self._armed_session = None if perpetual_operation else PaperSessionArmGrant(
            context.session_kind, context.session_id, context.trade_date,
            context.session_profile_hash, context.session_generation, now,
            context.boundary_at("entry_cutoff").isoformat().replace("+00:00", "Z"),
        )
        self._entries_paused = False
        transition_reason = (
            "OPERATOR_OPERATIONAL_PAPER_SESSION_STARTED"
            if target_state is PaperRuntimeState.PAPER_RUNNING
            else "OPERATOR_ARM_AFTER_PREFLIGHT"
        )
        self._transition(target_state, transition_reason)
        return {
            "armed": True,
            "reason_codes": (
                ("PERPETUAL_PAPER_OPERATION_RUNNING",)
                if perpetual_operation else ("PAPER_ARMED",)
            ),
            "deferred_entry_reasons": deferred_entry_reasons,
            "state": self._state.value,
            "session_armed_state": (
                "ARMED_PERPETUAL" if perpetual_operation
                else "ARMED_" + context.session_kind.value
            ),
            "arm_grant": None if self._armed_session is None else self._armed_session.payload(),
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
            try:
                with self.ledger.authority_capacity_fence():
                    return self._commit_commissioning_arm_locked(
                        identity=identity, request_id=request_id, capture=capture,
                        ledger_evidence=ledger_evidence, ledger_blocker=ledger_blocker,
                        preflight_duration_seconds=duration, ledger_preflight=ledger_preflight,
                    )
            except LedgerCapacityError as error:
                self._pause_for_ledger_capacity_locked(error.capacity)
                return {
                    "armed": False,
                    "reason_codes": ("LEDGER_CAPACITY_INADEQUATE",),
                    "state": self._state.value,
                }

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
                "authority_hash": canonical_hash(self.authority.authority_payload()),
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
            try:
                with self.ledger.authority_capacity_fence(), self.ledger.commissioning_authority_fence():
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
            except LedgerCapacityError as error:
                self._pause_for_ledger_capacity_locked(error.capacity)
                return {
                    "submitted": False,
                    "commissioning": True,
                    "armed": False,
                    "reason_codes": ("LEDGER_CAPACITY_INADEQUATE",),
                    "state": self._state.value,
                    "commissioning_id": commissioning_id,
                    "request_id": request_id,
                }

    def commission_entry(
        self,
        commissioning_id: str,
        commissioning_token: str,
        *,
        candidate: PaperDecision | None = None,
    ) -> dict[str, object]:
        """Consume one commissioning entry only from fresh profile-qualified policy evidence."""
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
            selected = candidate or self._last_qualifying_entry_decision
            now = _now()
            if self._state is not PaperRuntimeState.ARMED_FLAT or self._entries_paused:
                return {"submitted": False, "reason_codes": ("PAPER_NOT_ARMED_FLAT",), "state": self._state.value}
            expected_checkpoint = (
                ownership.ledger_preflight.get("authority_commit_checkpoint")
                if isinstance(ownership.ledger_preflight, Mapping) else None
            )
            current_checkpoint = self.ledger.commissioning_authority_checkpoint()
            checkpoint_capacity = current_checkpoint.get("deferred_capacity")
            if not isinstance(checkpoint_capacity, Mapping) or not self._deferred_capacity_healthy_locked(
                checkpoint_capacity
            ):
                self._pause_for_ledger_capacity_locked(
                    checkpoint_capacity if isinstance(checkpoint_capacity, Mapping) else None
                )
                return {
                    "submitted": False,
                    "reason_codes": ("LEDGER_CAPACITY_INADEQUATE",),
                    "state": self._state.value,
                }
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
                self._snapshot.account_name != self.risk.binding.account_name
                or self._snapshot.account_class != self.risk.binding.account_class
                or self._snapshot.instrument != self.risk.binding.instrument
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
            if not self._commissioning_candidate_valid(selected, ownership.context, now):
                if selected is not None and datetime.fromisoformat(
                    normalized_utc(selected.expires_at, "Commissioning candidate expiry").replace("Z", "+00:00")
                ) < datetime.fromisoformat(now.replace("Z", "+00:00")):
                    self._last_qualifying_entry_decision = None
                return {
                    "submitted": False,
                    "commissioning": True,
                    "commissioning_id": ownership.commissioning_id,
                    "reason_codes": ("COMMISSIONING_WAITING_FOR_PROFILE_SIGNAL",),
                    "state": self._state.value,
                }
            assert selected is not None
            source = canonical_hash({
                "commissioning": True,
                "commissioning_id": ownership.commissioning_id,
                "source_decision_id": selected.paper_decision_id,
                "at": now,
                "session_id": context.session_id,
            })
            payload = {
                "paper_policy_id": self.policy.artifact.policy_id,
                "paper_policy_hash": self.policy.artifact.configuration_hash,
                "decision": selected.decision.value,
                "created_at": now,
                "source_decision_id": selected.paper_decision_id,
                "relative_support": str(selected.relative_support),
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
                deterministic_id("l3g-pd-", payload), self.policy.artifact.policy_id, self.policy.artifact.configuration_hash,
                selected.decision, now,
                (datetime.fromisoformat(now.replace("Z", "+00:00")) + timedelta(seconds=self.policy.artifact.decision_ttl_seconds)).isoformat().replace("+00:00", "Z"),
                None, selected.direction, selected.relative_support,
                {
                    "commissioning": True,
                    "qualification": "PROFILE_POLICY_SIGNAL",
                    "source_decision_id": selected.paper_decision_id,
                    "source_hypothesis": None if selected.hypothesis_kind is None else selected.hypothesis_kind.value,
                    "source_family_summary": dict(selected.family_summary),
                },
                selected.source_observation_ids,
                selected.source_local_sequences,
                selected.source_payload_hashes,
                self.policy.artifact.sequence_authority, self.policy.artifact.book_completeness, False,
                "COMMISSIONING_PROFILE_SIGNAL_ENTRY", context.session_kind, context.session_id,
                context.trade_date, context.session_profile_hash, context.session_generation,
                True, False, False,
            )
            self.ledger.append(
                "COMMISSIONING_ENTRY_AUTHORIZED",
                {
                    **self._ownership_payload(ownership, reason="SINGLE_USE_ENTRY_AUTHORIZED"),
                    "decision_id": decision.paper_decision_id,
                    "direction": decision.direction.value,
                    "quantity": 1,
                    "qualification": {
                        "source_decision_id": selected.paper_decision_id,
                        "relative_support": str(selected.relative_support),
                        "required_support": str(self.policy.artifact.entry_support_threshold),
                        "dominance": selected.family_summary.get("dominance"),
                        "required_dominance": str(self.policy.artifact.entry_dominance_margin),
                        "positive_family_count": selected.family_summary.get("positive_family_count"),
                        "required_family_count": self.policy.artifact.entry_family_count,
                    },
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
            self._last_qualifying_entry_decision = None
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

    def _commissioning_candidate_valid(
        self,
        candidate: PaperDecision | None,
        context: PaperSessionContext,
        at: str,
    ) -> bool:
        if type(candidate) is not PaperDecision:
            return False
        moment = datetime.fromisoformat(normalized_utc(at, "Commissioning candidate time").replace("Z", "+00:00"))
        created = datetime.fromisoformat(normalized_utc(candidate.created_at, "Commissioning candidate creation").replace("Z", "+00:00"))
        expiry = datetime.fromisoformat(normalized_utc(candidate.expires_at, "Commissioning candidate expiry").replace("Z", "+00:00"))
        family_summary = candidate.family_summary
        try:
            dominance = Decimal(str(family_summary.get("dominance")))
            positive_families = int(family_summary.get("positive_family_count", 0))
        except Exception:
            return False
        return (
            candidate.decision in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}
            and candidate.strategy_generated
            and not candidate.commissioning
            and not candidate.scientific_evidence
            and candidate.paper_policy_id == self.policy.artifact.policy_id
            and candidate.paper_policy_hash == self.policy.artifact.configuration_hash
            and candidate.session_kind in self.policy.artifact.entry_session_kinds
            and candidate.session_kind is context.session_kind
            and candidate.session_id == context.session_id
            and candidate.trade_date == context.trade_date
            and candidate.session_profile_hash == context.session_profile_hash
            and candidate.session_generation == context.session_generation
            and created <= moment <= expiry
            and candidate.relative_support >= self.policy.artifact.entry_support_threshold
            and dominance >= self.policy.artifact.entry_dominance_margin
            and positive_families >= self.policy.artifact.entry_family_count
            and family_summary.get("blocking_contradiction") is False
        )

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
            if self._perpetual_position_profile:
                if (
                    self._operational_session is None
                    or self._operational_session_is_stopping_locked()
                ):
                    return {
                        "resumed": False, "state": self._state.value,
                        "reason": "PERPETUAL_OPERATION_NOT_ACTIVE",
                    }
            elif self._armed_session is None or not self._armed_session.valid_at(_now()):
                return {"resumed": False, "state": self._state.value, "reason": "SESSION_ARM_EXPIRED"}
            self._entries_paused = False
            target = (
                PaperRuntimeState.PAPER_RUNNING
                if self._position is PaperDirection.FLAT and self._operational_session is not None
                else PaperRuntimeState.ARMED_FLAT
                if self._position is PaperDirection.FLAT
                else PaperRuntimeState.LONG
                if self._position is PaperDirection.LONG
                else PaperRuntimeState.SHORT
            )
            self._transition(target, "OPERATOR_RESUME_ENTRIES")
            if self._perpetual_position_profile and self._position is PaperDirection.FLAT:
                self._maintain_perpetual_position_locked("OPERATOR_RESUME")
            return {"resumed": True, "state": self._state.value}

    def flatten_and_disarm(self) -> dict[str, object]:
        with self._lock:
            operational = self._operational_session
            if operational is not None:
                self._request_operational_stop_locked("OPERATOR_STOP_TRADING")
                audit_payload = {
                    "position": self._position.value,
                    "state": self._state.value,
                    "request_id": operational.request_id,
                    "operational_paper": True,
                }
                if self._position is PaperDirection.FLAT and (
                    self._state is PaperRuntimeState.ENTRY_PENDING or self._snapshot.working_owned_orders > 0
                ):
                    self._cancel_pending_and_reconcile()
                    result = {
                        "initiated": True,
                        "stopping": True,
                        "flat_confirmed": False,
                        "state": self._state.value,
                        "reason_codes": ("OPERATIONAL_STOP_PENDING_RECONCILIATION",),
                    }
                elif self._position is PaperDirection.FLAT and (
                    self._snapshot.reconciliation_current
                    and self._snapshot.position_snapshot_complete
                    and self._snapshot.order_snapshot_complete
                    and self._snapshot.working_owned_orders == 0
                ):
                    self._complete_operational_stop_locked("OPERATOR_STOP_TRADING_RECONCILED")
                    result = {
                        "initiated": True,
                        "stopping": False,
                        "flat_confirmed": True,
                        "state": self._state.value,
                        "reason_codes": ("OPERATIONAL_STOP_RECONCILED",),
                    }
                else:
                    submitted = self._request_exit("OPERATOR_STOP_TRADING", emergency=True)
                    result = {
                        "initiated": submitted,
                        "stopping": True,
                        "flat_confirmed": False,
                        "state": self._state.value,
                        "reason_codes": (
                            "OPERATIONAL_STOP_PENDING_RECONCILIATION"
                            if self._state is not PaperRuntimeState.FAULTED
                            else "OPERATIONAL_STOP_REQUIRES_EMERGENCY_KILL",
                        ),
                    }
                self._append_best_effort_safety_audit_locked("RISK_EVENT_FLATTEN_AND_DISARM", audit_payload)
                return result

            self._entries_paused = True
            self._armed_session = None
            self._disarm_after_flat = True
            audit_payload = {"position": self._position.value, "state": self._state.value}
            if self._position is PaperDirection.FLAT and (self._state is PaperRuntimeState.ENTRY_PENDING or self._snapshot.working_owned_orders > 0):
                self._cancel_pending_and_reconcile()
                result = {"initiated": True, "flat_confirmed": False, "state": self._state.value}
            elif self._position is PaperDirection.FLAT:
                if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED}:
                    try:
                        self._transition(PaperRuntimeState.READY_DISARMED, "FLAT_CONFIRMED_DISARM")
                    except Exception as error:
                        self._fail_closed_without_ledger_locked(
                            "FLAT_DISARM_DURABLE_AUTHORITY_UNAVAILABLE:" + type(error).__name__,
                        )
                if self._state is not PaperRuntimeState.FAULTED:
                    self._disarm_after_flat = False
                ownership = self._commissioning_ownership
                if ownership is not None and not ownership.entry_consumed and self._snapshot.working_owned_orders == 0:
                    self._release_commissioning_ownership("OPERATOR_FLATTEN_DISARM_BEFORE_COMMISSIONING_ENTRY")
                elif self._entry_owner is PaperEntryOwner.STRATEGY:
                    self._entry_owner = PaperEntryOwner.NONE
                result = {"initiated": True, "flat_confirmed": True, "state": self._state.value}
            else:
                submitted = self._request_exit(
                    "OPERATOR_FLATTEN_AND_DISARM", emergency=True,
                )
                result = {
                    "initiated": submitted,
                    "flat_confirmed": False,
                    "state": self._state.value,
                }
            self._append_best_effort_safety_audit_locked("RISK_EVENT_FLATTEN_AND_DISARM", audit_payload)
            return result

    def _cancel_pending_and_reconcile(self) -> None:
        created = _now()
        decision = PaperDecision(
            "l3g-pd-" + canonical_hash({"reason": "CANCEL_PENDING_AND_DISARM", "at": created})[:32],
            self.policy.artifact.policy_id, self.policy.artifact.configuration_hash, PaperDecisionKind.EXIT, created,
            (datetime.fromisoformat(created.replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            None, PaperDirection.FLAT, Decimal("1"), {"safety": "CANCEL_PENDING_AND_DISARM"},
            ("pending-order-safety-control",), (max(0, self.policy.status().get("last_local_sequence") or 0),),
            (canonical_hash({"reason": "CANCEL_PENDING_AND_DISARM"}),), self.policy.artifact.sequence_authority,
            self.policy.artifact.book_completeness, False, "CANCEL_PENDING_AND_DISARM",
            self._session_context.session_kind, self._session_context.session_id,
            self._session_context.trade_date, self._session_context.session_profile_hash,
            self._session_context.session_generation,
        )
        try:
            self.ledger.append("DECISION", decision.payload(), identity=decision.paper_decision_id, occurred_at=decision.created_at, execution_session_id=self._execution_session_id())
            bid, ask, last = self._references()
            intent = self.risk.make_intent(decision, reference_bid=bid, reference_ask=ask, reference_last=last)
            self.ledger.append("INTENT", intent.payload(), identity=intent.intent_id, occurred_at=intent.created_at, execution_session_id=self._execution_session_id())
            grant = self.risk.evaluate(intent, self._snapshot, at=created)
            self.ledger.append("RISK_GRANT", grant.payload(), identity=grant.grant_id, occurred_at=grant.evaluated_at, execution_session_id=self._execution_session_id())
            if not grant.granted:
                self._fail_closed_without_ledger_locked(
                    "CANCEL_RISK_AUTHORITY_UNAVAILABLE:" + ",".join(grant.reason_codes),
                )
                return
            cancel = self._make_command(intent.intent_id, decision.paper_decision_id, grant.grant_id, ExecutionAction.CANCEL_OWNED_ORDERS, PaperDirection.FLAT, "OPERATOR_FLATTEN_AND_DISARM")
            self._persist_and_send(cancel, grant)
            self._transition(PaperRuntimeState.RECONCILING, "PENDING_ENTRY_CANCEL_SENT")
            reconcile = self._make_command(intent.intent_id, decision.paper_decision_id, grant.grant_id, ExecutionAction.RECONCILE, PaperDirection.FLAT, "POST_CANCEL_RECONCILIATION")
            self._persist_and_send(reconcile, grant)
        except Exception as error:
            # A still-working entry order is activity for the AddOn watchdog;
            # do not leave it live merely because Python cannot prove its
            # cancel/reconcile command durably.
            self._fail_closed_without_ledger_locked(
                "CANCEL_DURABLE_AUTHORITY_UNAVAILABLE:" + type(error).__name__,
            )

    def stop(self) -> dict[str, object]:
        """Disarm safely and return any AddOn watchdog transport-retention need."""
        thread: threading.Thread | None = None
        with self._lock:
            if self._state is PaperRuntimeState.STOPPED:
                return self.watchdog_shutdown_status()
            if self._state is PaperRuntimeState.DISABLED:
                self._heartbeat_stop.set()
                self._state = PaperRuntimeState.STOPPED
                return self.watchdog_shutdown_status()
            position_at_shutdown = self._position
            working_orders_at_shutdown = self._snapshot.working_owned_orders
            working_entry_orders_at_shutdown = self._snapshot.working_entry_orders
            state_at_shutdown = self._state
            activity_at_shutdown = self._has_unresolved_execution_activity_locked()
            if self._state is not PaperRuntimeState.STOPPING:
                # Arm the independently owned watchdog *before* dispatching
                # PROCESS_STOP_OPEN_POSITION. The AddOn publishes its signed
                # SAFETY_EVENT before flattening; on a low-latency bridge that
                # callback can re-enter this RLock synchronously. Recording
                # the activation sequence first prevents the event from being
                # discarded as pre-watchdog evidence.
                if activity_at_shutdown:
                    self._activate_independent_watchdog_locked(
                        "PROCESS_STOP_OPEN_ACTIVITY",
                        force=True,
                    )
                else:
                    self._heartbeat_stop.set()
                # A normal, durable emergency exit remains preferred for an
                # open position. The already-armed AddOn watchdog is the
                # independently owned fallback if that path cannot settle.
                if position_at_shutdown is not PaperDirection.FLAT:
                    try:
                        self._request_exit(
                            "PROCESS_STOP_OPEN_POSITION", emergency=True,
                        )
                    except Exception as error:
                        # The native watchdog was armed above.  Even a second
                        # failure while publishing the Python exit lockout may
                        # not trap process teardown or keep heartbeats alive.
                        self._force_shutdown_state_without_ledger_locked(
                            PaperRuntimeState.STOPPING,
                            "PROCESS_STOP_EXIT_AUDIT_UNAVAILABLE:"
                            + type(error).__name__,
                        )
                if self._state is not PaperRuntimeState.STOPPING:
                    try:
                        self._transition(PaperRuntimeState.STOPPING, "PROCESS_STOP")
                    except Exception as error:
                        # _transition has already changed state before its ledger
                        # append.  Make this terminal state explicit so a retry
                        # cannot return early and leave heartbeats alive.
                        self._force_shutdown_state_without_ledger_locked(
                            PaperRuntimeState.STOPPING,
                            "PROCESS_STOP_DURABLE_AUDIT_UNAVAILABLE:"
                            + type(error).__name__,
                        )
            else:
                # Recover safely from a historical/state-before-audit stop
                # failure on a later caller rather than treating it as done.
                if activity_at_shutdown:
                    self._activate_independent_watchdog_locked(
                        "PROCESS_STOP_RETRY_OPEN_ACTIVITY",
                        force=True,
                    )
                else:
                    self._heartbeat_stop.set()
            thread = self._heartbeat_thread
            if activity_at_shutdown:
                self._append_best_effort_safety_audit_locked(
                    "INCIDENT_SHUTDOWN_WITH_POSITION",
                    {
                        "position": position_at_shutdown.value,
                        "working_owned_orders": working_orders_at_shutdown,
                        "working_entry_orders": working_entry_orders_at_shutdown,
                        "state_at_shutdown": state_at_shutdown.value,
                        "watchdog": "INDEPENDENT_CSHARP_FLATTEN_REQUIRED",
                    },
                )
        if thread is not None:
            thread.join(timeout=2.0)
        with self._lock:
            if self._state is not PaperRuntimeState.STOPPED:
                try:
                    if self._state is not PaperRuntimeState.STOPPING:
                        self._force_shutdown_state_without_ledger_locked(
                            PaperRuntimeState.STOPPING,
                            "PROCESS_STOP_FINAL_STATE_REPAIR",
                        )
                    self._transition(PaperRuntimeState.STOPPED, "PROCESS_STOPPED")
                except Exception as error:
                    self._force_shutdown_state_without_ledger_locked(
                        PaperRuntimeState.STOPPED,
                        "PROCESS_STOPPED_DURABLE_AUDIT_UNAVAILABLE:" + type(error).__name__,
                    )
            return self.watchdog_shutdown_status()

    def _position_requirement_locked(self, at: str) -> dict[str, object]:
        checkpoint = self._latest_five_minute_direction_checkpoint
        source_signal = None if checkpoint is None else {
            "direction": checkpoint.get("direction"),
            "candle_close_utc": checkpoint.get("candle_close_utc"),
            "signal_hash": checkpoint.get("signal_hash"),
            "ledger_sequence": checkpoint.get("ledger_sequence"),
            "record_hash": checkpoint.get("record_hash"),
            "ledger_verified": self._perpetual_signal_ledger_verified,
        }
        if not self._perpetual_position_profile:
            return {
                "required": False, "state": "INACTIVE",
                "actual_position": self._position.value,
                "actual_quantity": self._position_quantity,
                "desired_position": None,
                "primary_blocker": None, "blocking_reasons": [],
                "source_signal": None,
            }
        desired = None if checkpoint is None else checkpoint.get("direction")
        if self._position is PaperDirection.FLAT:
            blockers = list(self._perpetual_position_blockers_locked(at))
            if self._perpetual_signal_fault is not None:
                blockers.insert(0, self._perpetual_signal_fault)
            if self._perpetual_flat_blocker is not None:
                blockers.insert(0, self._perpetual_flat_blocker)
            blockers = list(dict.fromkeys(blockers))
            state = (
                "ENTRY_PENDING" if self._state is PaperRuntimeState.ENTRY_PENDING
                else "REVERSING" if self._state is PaperRuntimeState.EXIT_PENDING
                else "BLOCKED_FLAT"
            )
            return {
                "required": True, "state": state,
                "actual_position": PaperDirection.FLAT.value,
                "actual_quantity": 0,
                "desired_position": desired,
                "primary_blocker": blockers[0] if blockers else "PERPETUAL_ENTRY_NOT_SUBMITTED",
                "blocking_reasons": blockers or ["PERPETUAL_ENTRY_NOT_SUBMITTED"],
                "source_signal": source_signal,
            }

        blockers: list[str] = []
        if self._state is PaperRuntimeState.EXIT_PENDING:
            blockers.append("REVERSAL_OR_SAFETY_EXIT_PENDING")
        elif self._state not in {
            PaperRuntimeState.PAPER_RUNNING,
            PaperRuntimeState.LONG,
            PaperRuntimeState.SHORT,
        }:
            blockers.append("PERPETUAL_RUNTIME_NOT_RUNNING")
        if self._entries_paused:
            blockers.append(self._fault_reason or "ENTRIES_PAUSED")
        if not self._post_entry_reconciliation_complete:
            blockers.append("POSITIONED_RECONCILIATION_PENDING")
        if self._position_quantity != 1:
            blockers.append("POSITION_QUANTITY_NOT_ONE")
        if (
            self._snapshot.current_position is not self._position
            or self._snapshot.current_position_quantity != 1
        ):
            blockers.append("BROKER_RUNTIME_POSITION_MISMATCH")
        if self._snapshot.foreign_activity:
            blockers.append("FOREIGN_ACTIVITY_LOCKOUT")
        if self._snapshot.working_entry_orders != 0:
            blockers.append("WORKING_ENTRY_ORDER_REMAINS")
        if self._snapshot.working_owned_orders != 1:
            blockers.append("PROTECTIVE_ORDER_COUNT_NOT_ONE")
        if (
            self._snapshot.protective_stop_state != "WORKING"
            or not isinstance(self._protective_order_id, str)
            or not self._protective_order_id
        ):
            blockers.append("PROTECTIVE_STOP_NOT_WORKING")
        if (
            not self._snapshot.reconciliation_current
            or not self._snapshot.position_snapshot_complete
            or not self._snapshot.order_snapshot_complete
        ):
            blockers.append("RECONCILIATION_INCOMPLETE")
        if (
            self._snapshot.unresolved_command
            or self._snapshot.unresolved_native_order
            or self._snapshot.unresolved_execution
        ):
            blockers.append("UNRESOLVED_EXECUTION_TRUTH")
        if desired is not None and desired != self._position.value:
            blockers.append("POSITION_DIRECTION_NOT_LATEST_BIAS")
        if checkpoint is None:
            blockers.append("NO_COMPLETED_FIVE_MINUTE_SIGNAL")
        elif not self._perpetual_signal_ledger_verified:
            blockers.append("FIVE_MINUTE_SIGNAL_LEDGER_UNVERIFIED")
        exchange = perpetual_exchange_blocker(at, self._session_context)
        if exchange is not None:
            blockers.append(exchange)
        elif not self._snapshot.local_bridge_healthy or not self._snapshot.market_price_connected:
            blockers.append("MARKET_OBSERVER_UNHEALTHY")
        if not self._snapshot.execution_bridge_healthy:
            blockers.append("EXECUTION_BRIDGE_UNHEALTHY")
        blockers = list(dict.fromkeys(blockers))
        return {
            "required": True,
            "state": "REVERSING" if self._state is PaperRuntimeState.EXIT_PENDING else "POSITIONED",
            "actual_position": self._position.value,
            "actual_quantity": self._position_quantity,
            "desired_position": desired,
            "primary_blocker": blockers[0] if blockers else None,
            "blocking_reasons": blockers,
            "source_signal": source_signal,
        }

    def status(self) -> dict[str, object]:
        with self._lock:
            policy = self.policy.status()
            risk = self.risk.status()
            transport = None if self._transport is None else self._transport.status().as_dict()
            context = self._session_context
            risk_context = (
                self._entry_session_context
                if self._perpetual_position_profile and self._entry_session_context is not None
                else context
            )
            trade_risk = self._trade_date_risk.get(risk_context.trade_date, _TradeDateRisk())
            active_profile = self.policy.artifact.entry_profile_version
            profile_risk = self._profile_trade_date_risk.get(
                (risk_context.trade_date, active_profile), _ProfileTradeDateRisk(),
            )
            session_key = (context.session_id, active_profile)
            status_now = _now()
            arm_valid = self._armed_session is not None and self._armed_session.valid_at(status_now)
            ownership = self._commissioning_ownership
            loss_remaining = max(
                Decimal("0"), PAPER_ACCOUNT_DAILY_LOSS_LIMIT_DOLLARS
                + min(Decimal("0"), trade_risk.realized_pnl + trade_risk.unrealized_pnl),
            )
            family_realized = sum(
                self._session_pnl.get(key, Decimal("0"))
                for key, session_context in self._session_risk_contexts.items()
                if session_context.trade_date == context.trade_date
                and session_context.session_family is context.session_family
            )
            family_unrealized = (
                trade_risk.unrealized_pnl
                if self._entry_session_context is not None
                and self._entry_session_context.trade_date == context.trade_date
                and self._entry_session_context.session_family is context.session_family
                else Decimal("0")
            )
            family_entry_count = sum(
                self._session_entry_counts.get(key, 0)
                for key, session_context in self._session_risk_contexts.items()
                if session_context.trade_date == context.trade_date
                and session_context.session_family is context.session_family
                and key[1] == active_profile
            )
            account_session_realized = sum(
                value for (session_id, _), value in self._session_pnl.items()
                if session_id == context.session_id
            )
            next_context = PaperSessionResolver().next_valid_session(status_now, generation=context.session_generation)
            market_freshness = {
                "quote": self._freshness_gate(
                    self._snapshot.quote_observed_at, self.risk.profile.quote_maximum_age_seconds, status_now,
                ),
                "classified_trade": self._freshness_gate(
                    self._snapshot.classified_trade_observed_at,
                    self.risk.profile.classified_trade_maximum_age_seconds, status_now,
                ),
                "depth_mutation": self._freshness_gate(
                    self._snapshot.depth_mutation_observed_at,
                    self.risk.profile.depth_mutation_maximum_age_seconds, status_now,
                ),
            }
            position_requirement = self._position_requirement_locked(status_now)
            status = {
                "schema": "lane-iii-phase-g-paper-runtime-status-v1",
                "mode": "PAPER_SIM101",
                "display_mode": "EXPERIMENTAL PAPER",
                "state": self._state.value,
                "entries_paused": self._entries_paused,
                "paper_execution": "POSITIONED" if self._position is not PaperDirection.FLAT else "RUNNING" if self._state is PaperRuntimeState.PAPER_RUNNING else "ARMED" if self._state in {PaperRuntimeState.ARMED_FLAT, PaperRuntimeState.PAUSED} else "LOCKED" if self._state in {PaperRuntimeState.LOCKED_OUT, PaperRuntimeState.FAULTED} else "DISARMED",
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
                "entry_profile": self.policy.artifact.entry_profile,
                "entry_profile_version": self.policy.artifact.entry_profile_version,
                "entry_session_kind": "ALL_CONFIGURED",
                "entry_session_kinds": [value.value for value in self.policy.artifact.entry_session_kinds],
                "effective_confidence_threshold": str(self.policy.artifact.entry_support_threshold),
                "entry_dominance_margin": str(self.policy.artifact.entry_dominance_margin),
                "entry_family_count": self.policy.artifact.entry_family_count,
                "reentry_cooldown_seconds": self.policy.artifact.reentry_cooldown_seconds,
                "retention_confidence_threshold": str(self.policy.artifact.retention_support_threshold),
                "maximum_position_age_seconds": (
                    None if self._perpetual_position_profile
                    else self.risk.profile.maximum_position_age_seconds
                ),
                "maximum_position_age_enforced": not self._perpetual_position_profile,
                "maximum_session_entries": self.risk.profile.maximum_session_entries,
                "session_definitions": list(session_catalog()),
                "commissioning_readiness_snapshot_generation": self._commissioning_readiness_generation,
                "commissioning_authority_epoch": self._commissioning_authority_epoch,
                "commissioning_readiness_snapshot_token": self._last_commissioning_snapshot_token,
                "commissioning_preflight_duration_seconds": self._last_commissioning_preflight_duration_seconds,
                "commissioning_stale_snapshot_refusal_count": self._commissioning_stale_snapshot_refusals,
                "warning": "EXPERIMENTAL PAPER EXECUTION / NOT SCIENTIFICALLY COMMISSIONED / SIM101 ONLY / LIVE CAPITAL DENIED",
                "current_position": self._position.value,
                "current_quantity": self._position_quantity,
                "current_position_quantity": self._snapshot.current_position_quantity,
                "broker_snapshot_position": self._snapshot.current_position.value,
                "broker_snapshot_position_quantity": self._snapshot.current_position_quantity,
                "working_owned_orders": self._snapshot.working_owned_orders,
                "working_entry_orders": self._snapshot.working_entry_orders,
                "foreign_activity": self._snapshot.foreign_activity,
                "protective_stop_state": self._snapshot.protective_stop_state,
                "position_snapshot_complete": self._snapshot.position_snapshot_complete,
                "order_snapshot_complete": self._snapshot.order_snapshot_complete,
                "reconciliation_current": self._snapshot.reconciliation_current,
                "unresolved_command": self._snapshot.unresolved_command,
                "unresolved_native_order": self._snapshot.unresolved_native_order,
                "unresolved_execution": self._snapshot.unresolved_execution,
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
                "session_timezone": context.timezone,
                "entry_window": f"{context.entry_start}-{context.entry_cutoff} {context.timezone}",
                "entry_cutoff": context.entry_cutoff,
                "hard_flat_deadline": context.hard_flat_deadline,
                "session_armed_state": (
                    "ARMED_PERPETUAL"
                    if self._perpetual_position_profile
                    and self._operational_session is not None
                    and not self._operational_session_is_stopping_locked()
                    else "ARMED_" + context.session_kind.value if arm_valid else "DISARMED"
                ),
                "session_arm_grant": None if self._armed_session is None else self._armed_session.payload(),
                "next_valid_session": None if next_context is None else {
                    "session_kind": next_context.session_kind.value,
                    "session_family": next_context.session_family.value,
                    "session_id": next_context.session_id,
                    "trade_date": next_context.trade_date,
                    "timezone": next_context.timezone,
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
                "session_pnl": str(account_session_realized),
                "profile_session_pnl": str(self._session_pnl.get(session_key, Decimal("0"))),
                "paper_session_pnl": {
                    # Keep both terms inside the same risk-accounting epoch.
                    # Current-session realized P&L remains available above as
                    # session_pnl and must not be mixed with a carried D1 mark.
                    "trade_date": risk_context.trade_date,
                    "realized": str(trade_risk.realized_pnl),
                    "unrealized": str(trade_risk.unrealized_pnl),
                },
                "asia_session_pnl": str(sum(value for (key, _), value in self._session_pnl.items() if ":ASIA:" in key and key.endswith(context.trade_date))),
                "london_session_pnl": str(sum(value for (key, _), value in self._session_pnl.items() if ":LONDON:" in key and key.endswith(context.trade_date))),
                "new_york_session_pnl": str(sum(value for (key, _), value in self._session_pnl.items() if (":NEW_YORK_RTH:" in key or ":NY_AFTER:" in key) and key.endswith(context.trade_date))),
                "family_cumulative_pnl": str(family_realized + family_unrealized),
                "combined_trade_date_pnl": str(trade_risk.realized_pnl + trade_risk.unrealized_pnl),
                "family_entry_count": family_entry_count,
                "combined_trade_date_loss_allowance_remaining": str(loss_remaining),
                "trade_date_entry_count": profile_risk.entry_count,
                "profile_trade_date_entry_count": profile_risk.entry_count,
                "account_trade_date_entry_count": trade_risk.entry_count,
                "risk_accounting_trade_date": risk_context.trade_date,
                "risk_accounting_scope": (
                    "OPEN_LIFECYCLE_STICKY_UNTIL_FLAT"
                    if self._perpetual_position_profile and self._entry_session_context is not None
                    else "CURRENT_EXCHANGE_TRADE_DATE"
                ),
                "account_trade_date_loss_policy": {
                    "policy_id": PAPER_ACCOUNT_DAILY_LOSS_POLICY_ID,
                    "limit_dollars": str(PAPER_ACCOUNT_DAILY_LOSS_LIMIT_DOLLARS),
                    "scope": "Sim101 / MNQ SEP26 / EXCHANGE_TRADE_DATE / ALL_PROFILES",
                    "provenance": PAPER_ACCOUNT_DAILY_LOSS_POLICY_PROVENANCE,
                    "broker_daily_loss_limit": "DISABLED; INTERNAL CEILING IS STRICTER",
                },
                "risk_continuity_fault": self._risk_continuity_fault,
                "risk_continuity": self.risk_continuity_snapshot(),
                "entry_owner": self._entry_owner.value,
                "commissioning_lifecycle": {
                    "classification": "EXPLICIT_PAPER_COMMISSIONING" if ownership is not None else "STRATEGY_GENERATED_PAPER",
                    "active": ownership is not None,
                    "phase": (
                        "INACTIVE" if ownership is None
                        else "ENTRY_CONSUMED" if ownership.entry_consumed
                        else "WAITING_FOR_PROFILE_SIGNAL"
                    ),
                    "waiting_for_profile_signal": ownership is not None and not ownership.entry_consumed,
                    "commissioning_id": None if ownership is None else ownership.commissioning_id,
                    "entry_consumed": False if ownership is None else ownership.entry_consumed,
                    "recovered_after_restart": False if ownership is None else ownership.recovered_after_restart,
                    "decision_id": None if ownership is None else ownership.entry_decision_id,
                    "request_id": None if ownership is None else ownership.request_id,
                    "strategy_generated": ownership is None,
                    "scientific_evidence": False,
                },
                "operational_paper_session": self._operational_session_payload(),
                "position_requirement": position_requirement,
                "pending_five_minute_reversal": None if self._pending_five_minute_reversal is None else {
                    "decision_id": self._pending_five_minute_reversal.paper_decision_id,
                    "target_position": self._pending_five_minute_reversal.family_summary.get("target_position"),
                    "candle_close_utc": self._pending_five_minute_reversal.family_summary.get("candle_close_utc"),
                },
                "perpetual_startup_seed": {
                    "source_shadow_enabled": self._perpetual_seed_shadow is not None,
                    "source_shadow_fault": self._perpetual_seed_shadow_fault,
                    "latest_completed_boundary": (
                        None
                        if self._perpetual_seed_latest_bundle is None
                        else self._perpetual_seed_latest_bundle.get(
                            "candle_close_utc",
                        )
                    ),
                    "latest_completed_bias": (
                        None
                        if self._perpetual_seed_latest_bundle is None
                        else self._perpetual_seed_latest_bundle.get("bias")
                    ),
                    "latest_non_tied_boundary": (
                        None
                        if self._perpetual_seed_latest_non_tied_bundle is None
                        else self._perpetual_seed_latest_non_tied_bundle.get(
                            "candle_close_utc",
                        )
                    ),
                    "captured_observations": len(
                        self._perpetual_seed_observations,
                    ),
                    "boundary_chain_length": len(
                        self._perpetual_seed_boundary_chain,
                    ),
                    "import": (
                        None
                        if self._perpetual_seed_import is None
                        else dict(self._perpetual_seed_import)
                    ),
                },
                "last_five_minute_analysis": self._last_five_minute_analysis,
                "last_paper_decision": None if self._last_decision is None else self._last_decision.payload(),
                "last_risk_result": risk.get("last_risk_result"),
                "last_command": None if self._last_command is None else self._last_command.payload(),
                "last_order_state": self._last_order_state,
                "last_execution": self._last_execution,
                "last_reconciliation": self._last_reconciliation,
                "lockout_or_fault_reason": self._fault_reason or risk.get("lockout_reason"),
                "last_commissioning_closure": self._last_commissioning_closure,
                "watchdog_failsafe": {
                    "reason": self._watchdog_failsafe_reason,
                    "required": (
                        self._watchdog_failsafe_deadline_monotonic is not None
                        and self._watchdog_failsafe_requires_flat_confirmation
                    ),
                    "flat_confirmed": (
                        self._watchdog_failsafe_flat_confirmation is not None
                        or not self._watchdog_failsafe_requires_flat_confirmation
                    ),
                    "durable_confirmation": self._watchdog_failsafe_durable_confirmation,
                    "watchdog_available": self._watchdog_failsafe_available,
                    "safety_event_id": self._watchdog_failsafe_safety_event_id,
                    "safety_event_durable": self._watchdog_failsafe_safety_event_durable,
                    "reconciliation_durable": self._watchdog_failsafe_reconciliation_durable,
                    "settled_reconciliation_count": self._watchdog_failsafe_settled_reconciliation_count,
                    "remaining_seconds": round(
                        0.0 if self._watchdog_failsafe_deadline_monotonic is None else max(
                            0.0, self._watchdog_failsafe_deadline_monotonic - time.monotonic(),
                        ),
                        3,
                    ),
                },
                "authority": self.authority.authority_payload(),
                "policy": policy,
                "risk": risk,
                "transport": transport,
            }
        # Ledger telemetry can wait behind an active hash-chain batch or slow
        # filesystem metadata.  It is informational and must never hold the
        # runtime authority/ingest lock while the UI polls status.
        status["ledger"] = self.ledger.health_status()
        return status
