"""Lifespan-owned production host for the bounded Sim101 Pong experiment.

The pure Pong, Discernment, DailyBias, and controller modules intentionally own
no transport.  This module is the one production composition root: it admits
authenticated native inputs, evaluates the frozen adapters, preserves a
portable qualifying-hour certificate, binds one durable runtime bridge, and
hosts the independently persisted one-hour controller.  It never grants live
authority and never treats accelerated fixtures as market history.
"""

from __future__ import annotations

from functools import lru_cache

from collections import deque
from concurrent.futures import Future
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import hashlib
import os
import queue
from pathlib import Path
import threading
import time
from typing import TYPE_CHECKING, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import PONG_ENTRY_PROFILE_VERSION
from .pong_authorization import PongLaunchAuthorization
from .pong_collection import PongAuthenticHourCollector
from .pong_contract import (
    DEC26_TOPOLOGY,
    SEP26_TOPOLOGY,
    PongContractTopology,
    validate_topology_bundle,
)
from .pong_compute import COMPUTE_BUILD_HASH, PongWindowRequest, PongWindowResult, evaluate_window_batch, open_compute_pool
from .pong_volume_history import HISTORY_TYPE, PongVolumeReferenceHistory
from .daily_bias import (
    BiasUpdateReason,
    ClassifiedSide,
    ClassifiedTradeInput,
    CompletedHourlyOrderFlow,
    DailyBiasEngine,
    DailyBiasResult,
    HourlyBiasFrame,
    HourlyOrderFlowEvaluator,
    default_unconfigured_bias,
)
from .discernment import DiscernmentEngine
from .pong_qualifying import (
    FHB_ARCHIVE_DISPOSITION,
    FHB_QUALIFYING_RUNTIME_STATE,
    PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
    PONG_QUALIFYING_INTERVAL_MS,
    PONG_QUALIFYING_MAXIMUM_EVENT_AGE_MS,
    PONG_QUALIFYING_MAXIMUM_QUEUE_AGE_MS,
    PONG_QUALIFYING_PERSISTENT_BREACH_SECONDS,
    PONG_QUALIFYING_SCHEDULING_MODE,
    PONG_QUALIFYING_SOURCE_HEARTBEAT_SECONDS,
)

if TYPE_CHECKING:
    from .fhb import FHBConfig
from .pong import PongActionKind, PongState, PongStateMachine
from .pong_scheduler import (
    PongPeriodicDeadline,
    PongSchedulingConfig,
    PongSchedulingMode,
)
from .pong_periodic_telemetry import (
    PERIODIC_DISQUALIFIER_KEYS,
    PONG_PERIODIC_TELEMETRY_SCHEMA,
)
from .pong_controller import (
    PONG_ACTIVATION_DATE_LOCAL,
    PongBoundedController,
    PongControllerCommand,
    PongControllerObservation,
    PongControllerPhase,
)
from .pong_experiment import (
    CompletedPongHour,
    hourly_structure_components,
    unavailable_nonvoting_options,
)
from .pong_inputs import PongCausalInputAdapter, PongNativeInputRefused, PongWarmStart
from .pong_microstructure import (
    AggressorProvenance,
    AggressorSide,
    BookSide,
    CausalEventRef,
    ClassifiedFlowAdapter,
    ClassifiedTrade,
    DepthEventKind,
    DepthObservation,
    LiquidityClusterAdapter,
    MicrostructureEvidenceMapper,
    MicrostructureEvidenceMappingPolicy,
    PriceResponse,
    VolumeRegimeAdapter,
    VolumeWindow,
    VolumeWindowKind,
)
from .pong_quantity import PongQuantityPolicy, PongReconciliationProof
from .pong_runtime import (
    PongBridgeUpdate,
    PongLoadedRuntimeIdentity,
    PongNativeDirective,
    PongNativeDirectiveKind,
    PongNativeEventProof,
    PongRuntimeBridge,
)


PONG_PRODUCTION_HOST_SCHEMA = "beelzebub-pong-production-host-v1"
PONG_QUALIFYING_HOUR_SCHEMA = "beelzebub-pong-qualified-hour-certificate-v1"
PONG_PRODUCTION_EXPERIMENT_ID = "pong-ny-open-20260911"
_ARTIFACT_ROOT = Path(__file__).resolve().parents[2]
_LOADED_SOURCE_HASHES = {
    name: hashlib.sha256((_ARTIFACT_ROOT / name).read_bytes()).hexdigest()
    for name in ("beez_console.py", "src/copytrade/control_center.py",
                 "src/l3g_paper/pong_production.py", "src/l3g_paper/pong_controller.py",
                 "src/l3g_paper/pong_authorization.py", "src/l3g_paper/pong_volume_history.py",
                 "src/l3g_paper/runtime.py", "src/l3g_paper/pending_lockout.py",
                 "src/l3g_paper/ledger.py",
                 "src/l3g_paper/pong_compute.py", "src/l3g_paper/pong.py",
                 "src/l3g_paper/discernment.py", "src/l3g_paper/pong_scheduler.py",
                 "src/l3g_paper/pong_periodic_telemetry.py",
                 "src/l3g_paper/pong_qualifying.py",
                 "src/l3g_paper/pong_collection.py", "src/l3f_provider/ninjatrader_observation.py",
                 "src/l3f_provider/ninjatrader_commission.py", "src/l3g_paper/pong_microstructure.py")
}
PONG_LOADED_ARTIFACT_HASH = canonical_hash(_LOADED_SOURCE_HASHES)
_RUNNING_CONTROLLER_PHASES = {
    PongControllerPhase.RUNNING_PRE_BLACKOUT,
    PongControllerPhase.RUNNING_POST_BLACKOUT,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: object) -> datetime:
    return _time_text(str(value))


@lru_cache(maxsize=65536)
def _time_text(value: str) -> datetime:
    return datetime.fromisoformat(
        normalized_utc(value, "Pong production time").replace("Z", "+00:00")
    )


class _DepthWindow:
    """The same bounded event sequence, indexed by exact Decimal price.

    Window selection still uses original availability and the last eligible
    event, including backwards/future time refusal by the frozen adapter.
    No event is sampled and no final evaluation is cached.
    """
    def __init__(self, maxlen: int = 4096) -> None:
        self._events: deque[DepthObservation] = deque(maxlen=maxlen)
        self._by_price: dict[Decimal, deque[DepthObservation]] = {}

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self):
        return iter(self._events)

    def __getitem__(self, index: int) -> DepthObservation:
        return self._events[index]

    def append(self, item: DepthObservation) -> None:
        if len(self._events) == self._events.maxlen:
            expired = self._events[0]
            values = self._by_price[expired.price]
            assert values.popleft() is expired
            if not values:
                del self._by_price[expired.price]
        self._events.append(item)
        self._by_price.setdefault(item.price, deque()).append(item)

    def extend(self, items: tuple[DepthObservation, ...]) -> None:
        for item in items:
            self.append(item)

    def clear(self) -> None:
        self._events.clear()
        self._by_price.clear()

    def price_window(self, cutoff: datetime) -> tuple[DepthObservation, ...]:
        latest = next((item for item in reversed(self._events)
                       if item.event._ordering_key[0] >= cutoff), None)
        if latest is None:
            return ()
        return tuple(item for item in self._by_price[latest.price]
                     if item.event._ordering_key[0] >= cutoff)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _atomic_write(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    raw = _canonical(value)
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


PONG_HOST_TELEMETRY_SCHEMA = "pong-host-telemetry-v1"
_HOST_TELEMETRY_CHECKPOINT_SECONDS = 5.0
_HOST_TELEMETRY_FAULT_RETRY_SECONDS = 5.0
_HOST_TELEMETRY_RUNTIME_KEYS = (
    "observed_at",
    "runtime_state",
    "runtime_active",
    "entry_profile_version",
    "entry_fence_active",
    "evidence_sealed",
    "entries_paused",
    "position_quantity",
    "working_owned_orders",
    "working_entry_orders",
    "unresolved_execution",
    "reconciliation_current",
    "protection_current",
    "safety_lockout",
    "lockout_reason",
    "execution_session_id",
    "addon_protocol_version",
    "addon_source_fingerprint",
    "addon_build_fingerprint",
    "addon_provenance_valid",
    "account_name",
    "account_class",
    "instrument",
    "maximum_quantity",
    "live_authority",
)
_HOST_TELEMETRY_DURABILITY_KEYS = (
    "journal_path",
    "collector_status_path",
    "current_status_path",
    "enqueued_records",
    "written_records",
    "durable_records",
    "journal_bytes",
    "journal_tip_sha256",
    "last_fsync_at",
    "queue_depth",
    "queue_high_water_records",
    "queue_capacity",
    "last_queue_age_milliseconds",
    "maximum_queue_age_milliseconds",
    "queue_overflow_records",
    "producer_queue_overflow_records",
    "producer_queue_overflow_baseline",
    "producer_queue_overflow_delta",
    "producer_transport_failed_records",
    "producer_transport_failed_baseline",
    "producer_transport_failed_delta",
    "rejected_records",
    "duplicate_records",
)


class PongProductionHost:
    """One process owner for native Pong inputs, bridge, and controller."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        selected_profile: str,
        expected_market_source_fingerprint: str,
        runtime_snapshot: Callable[[], Mapping[str, object]],
        collector_status: Callable[[], Mapping[str, object]],
        controller_dispatch: Callable[[str, str], bool],
        native_dispatch: Callable[
            [PongNativeDirective, Mapping[str, object]], Mapping[str, object]
        ],
        activation_authorized: bool = False,
        launch_authorization: PongLaunchAuthorization | None = None,
        history_verification_key: bytes | None = None,
        compute_workers: int = 0,
        fhb_config: FHBConfig | None = None,
        fhb_requested_profile: str | None = None,
        qualifying_architecture: str | None = None,
        scheduling: PongSchedulingConfig = PongSchedulingConfig(),
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        monotonic_clock: Callable[[], float] = time.monotonic,
        topology: PongContractTopology = SEP26_TOPOLOGY,
    ) -> None:
        if not callable(native_dispatch):
            raise TypeError("Pong production host requires one native dispatcher.")
        if (
            type(scheduling) is not PongSchedulingConfig
            or not callable(clock_ns)
            or not callable(monotonic_clock)
        ):
            raise ValueError("PONG_SCHEDULING_CONTRACT_INVALID")
        if (
            scheduling.enabled
            and selected_profile != PONG_ENTRY_PROFILE_VERSION
        ):
            raise ValueError("PONG_PERIODIC_PROFILE_NOT_SELECTED")
        if scheduling.enabled and compute_workers:
            raise ValueError("PONG_PERIODIC_COMPUTE_WORKERS_NOT_SUPPORTED")
        if fhb_config is not None and selected_profile != PONG_ENTRY_PROFILE_VERSION:
            raise ValueError("FHB_PONG_PROFILE_NOT_SELECTED")
        if fhb_config is not None and compute_workers:
            raise ValueError("FHB_SERIAL_ORDERED_OWNER_REQUIRED")
        normalized_fhb_request = (
            None
            if fhb_requested_profile is None or not fhb_requested_profile.strip()
            else fhb_requested_profile.strip()
        )
        if normalized_fhb_request is not None and fhb_config is None:
            raise ValueError("FHB_REQUEST_NOT_ADMITTED")
        if (
            fhb_config is not None
            and normalized_fhb_request is not None
            and normalized_fhb_request != fhb_config.profile_id
        ):
            raise ValueError("FHB_REQUEST_EFFECTIVE_POLICY_MISMATCH")
        if (
            fhb_config is not None
            and scheduling.enabled
            and fhb_config.entry_evaluation_interval_ms
        ):
            raise ValueError("FHB_PERIODIC_DUPLICATE_ENTRY_CADENCE")
        if qualifying_architecture is not None:
            if qualifying_architecture != PONG_NO_FHB_QUALIFYING_ARCHITECTURE:
                raise ValueError("PONG_QUALIFYING_ARCHITECTURE_UNSUPPORTED")
            if fhb_config is not None or normalized_fhb_request is not None:
                raise ValueError("FHB_ARCHIVED_NOT_PART_OF_QUALIFYING_BUILD")
            if (
                selected_profile != PONG_ENTRY_PROFILE_VERSION
                or scheduling.mode.value != PONG_QUALIFYING_SCHEDULING_MODE
                or scheduling.interval_ms != PONG_QUALIFYING_INTERVAL_MS
                or topology != DEC26_TOPOLOGY
                or activation_authorized
            ):
                raise ValueError("PONG_QUALIFYING_ARCHITECTURE_IDENTITY_INVALID")
        if type(topology) is not PongContractTopology:
            raise ValueError("PONG_CONTRACT_TOPOLOGY_INVALID")
        self.state_root = Path(state_root).resolve()
        self.topology = topology
        self._effective_state_root = (
            self.state_root if topology.version == SEP26_TOPOLOGY.version
            else self.state_root / topology.state_namespace
        )
        self.selected_profile = selected_profile
        self.expected_market_source_fingerprint = expected_market_source_fingerprint
        self._runtime_snapshot = runtime_snapshot
        self._collector_status = collector_status
        self._controller_dispatch = controller_dispatch
        self._native_dispatch = native_dispatch
        self.activation_authorized = activation_authorized
        self.launch_authorization = launch_authorization
        self._experiment_id = (
            PONG_PRODUCTION_EXPERIMENT_ID if launch_authorization is None
            else launch_authorization.experiment_id
        )
        self._activation_date = (
            PONG_ACTIVATION_DATE_LOCAL if launch_authorization is None
            else launch_authorization.activation_date_local
        )
        self._operation_root = (
            self._effective_state_root if launch_authorization is None
            else self._effective_state_root / "runs" / self._experiment_id
        )
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._observations: queue.Queue[
            tuple[NinjaTraderObservation, float]
        ] = queue.Queue(maxsize=262144)
        self._admission_lock = threading.Lock()
        self._observation_queue_occupancy = 0
        self._observation_overflow = 0
        self._observation_queue_high_water = 0
        self._last_depth_callback_monotonic: float | None = None
        self._last_host_queue_age_ms = 0.0
        self._maximum_host_queue_age_ms = 0.0
        self._last_source_receipt_age_ms = 0.0
        self._maximum_source_receipt_age_ms = 0.0
        self._freshness_breach_count = 0
        self._fhb_guard_breach_started: float | None = None
        self._fhb_guard_last_breach_seconds: float | None = None
        self._fhb_guard_maximum_breach_seconds = 0.0
        self._fhb_guard_recoveries = 0
        self._fhb_guard_blocker: str | None = (
            "PONG_WATCHDOG_WAITING_FOR_OBSERVATION"
            if qualifying_architecture is not None
            else None
            if fhb_config is None
            else "FHB_WATCHDOG_WAITING_FOR_OBSERVATION"
        )
        if type(compute_workers) is not int or not 0 <= compute_workers <= 3:
            raise ValueError('PONG_COMPUTE_WORKER_BOUND_INVALID')
        self._compute_workers = compute_workers
        self._compute_pool = None
        self._compute_pending: deque = deque()
        self._compute_generation = uuid4().hex
        self._compute_submitted = 0
        self._compute_applied = 0
        self._compute_refused = 0
        self._compute_limit = 512
        self._compute_wakeup = threading.Event()
        self._compute_batch: list[tuple[PongWindowRequest, Future]] = []
        self._compute_batch_started = 0.0
        self._compute_batch_limit = 32
        self._scheduling = scheduling
        self._qualifying_architecture = qualifying_architecture
        self._clock_ns = clock_ns
        self._monotonic_clock = monotonic_clock
        self._clock_info = vars(time.get_clock_info("perf_counter"))
        if scheduling.enabled and (
            self._clock_info["monotonic"] is not True
            or self._clock_info["adjustable"] is not False
            or self._clock_info["resolution"] > scheduling.interval_ms / 10_000
        ):
            raise RuntimeError("PONG_PERIODIC_CLOCK_RESOLUTION_UNSUPPORTED")
        self._periodic_deadline = PongPeriodicDeadline(scheduling, clock_ns=clock_ns)
        self._source_ingress_records = 0
        self._authoritative_admission_records = 0
        self._event_driven_evaluations = 0
        self._periodic_evaluations = 0
        self._ordinary_completed_full_analyses = 0
        self._protection_completed_full_analyses = 0
        self._microstructure_evaluations = 0
        self._fhb_requested_profile = (
            None if fhb_config is None
            else normalized_fhb_request or fhb_config.profile_id
        )
        self._fhb_output_batches = 0
        self._fhb_output_observations = 0
        self._fhb_last_output_source_sequence: int | None = None
        self._fhb_consumer_count = 0
        self._fhb_last_consumer_source_sequence: int | None = None
        self._fhb_last_consumer_at: datetime | None = None
        self._market_state_revision = 0
        self._entry_evaluation_starts = 0
        self._entry_evaluation_deferred = 0
        self._last_entry_evaluation_started: float | None = None
        self._minimum_entry_evaluation_spacing_ms: float | None = None
        self._last_entry_evaluation: dict[str, object] | None = None
        self._last_admitted_receipt_at: datetime | None = None
        self._last_periodic_cutoff: datetime | None = None
        self._last_protection_transition: str | None = None
        self._process_instance_id = "pong-controller-process-" + uuid4().hex
        self._periodic_counter_epoch_id = "pong-periodic-counter-" + uuid4().hex
        self._periodic_counter_reset_ordinal = 0
        self._controller: PongBoundedController | None = None
        self._controller_error: str | None = None
        self._host_first_fault: str | None = None
        self._host_first_fault_persisted = False
        self._host_first_fault_retry_after_monotonic: float | None = None
        self._host_telemetry_error: str | None = None
        self._last_host_checkpoint_monotonic: float | None = None
        self._host_service_count = 0
        self._host_service_total_ns = 0
        self._host_service_maximum_ns = 0
        self._observation_processing_count = 0
        self._observation_processing_total_ns = 0
        self._observation_processing_maximum_ns = 0
        self._analysis_processing_count = 0
        self._analysis_processing_total_ns = 0
        self._analysis_processing_maximum_ns = 0
        self._receipt_write_count = 0
        self._receipt_write_failure_count = 0
        self._receipt_write_total_ns = 0
        self._receipt_write_maximum_ns = 0
        self._protection_accounting_last_monotonic: float | None = None
        self._protection_accounting_state: str | None = None
        self._protection_accounting_reason: str | None = None
        self._protection_eligible_elapsed_seconds = 0.0
        self._protection_protected_elapsed_seconds = 0.0
        self._protection_transition_count = 0
        self._periodic_protection_reason_events = {
            key: 0 for key in PERIODIC_DISQUALIFIER_KEYS
            if key.endswith("_events") and key not in {
                "durability_lag_events",
            }
        }
        self._periodic_durability_lag_events = 0
        self._periodic_durability_counter_regressions = 0
        self._periodic_durability_lag_active: bool | None = None
        self._periodic_last_durability_counts: tuple[int, int, int] | None = None
        self._bridge: PongRuntimeBridge | None = None
        self._bridge_error: str | None = None
        self._pending_native: deque[
            tuple[PongNativeDirective, dict[str, object]]
        ] = deque()
        self._directive_command_ids: dict[str, str] = {}
        self._command_directive_ids: dict[str, str] = {}
        self._active_directive_ids: dict[PongNativeDirectiveKind, str] = {}
        self._runtime: dict[str, object] = {}
        self._collection: dict[str, object] = {}
        self._warm_start: PongWarmStart | None = None
        self._retained_hour_seed = False
        self._input_adapter: PongCausalInputAdapter | None = None
        self._input_error: str | None = None
        self._producer: tuple[str, str, str] | None = None
        self._source_generation: int | None = None
        self._last_quote_record: NinjaTraderObservation | None = None
        self._classified_retention_hour: datetime | None = None
        self._stream_sequences: dict[str, int] = {}
        self._stream_counts = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._stream_missing = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._stream_out_of_order = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._classified_hour: list[ClassifiedTradeInput] = []
        self._hour_pair: tuple[CompletedPongHour, CompletedPongHour] | None = None
        self._daily_bias: DailyBiasResult = default_unconfigured_bias()
        self._certificate: dict[str, object] | None = None
        self._certificate_error: str | None = None
        self._trades: deque[ClassifiedTrade] = deque(maxlen=8192)
        if fhb_config is None:
            self._fhb = None
        else:
            # Archived research compatibility is lazy. The qualifying host
            # neither imports nor instantiates the FHB implementation.
            from .fhb import FocusedHorizonBand

            self._fhb = FocusedHorizonBand(fhb_config)
        depth_capacity = 4096 if fhb_config is None else fhb_config.maximum_shadow_events
        self._depth = {
            BookSide.BID: _DepthWindow(depth_capacity),
            BookSide.ASK: _DepthWindow(depth_capacity),
        }
        self._canonical_depth: dict[
            BookSide, tuple[tuple[Decimal, Decimal], ...]
        ] = {BookSide.BID: (), BookSide.ASK: ()}
        self._canonical_depth_ready = False
        self._canonical_depth_reason: str | None = (
            "PONG_CANONICAL_DEPTH_WAITING_FOR_COHERENT_SNAPSHOT"
        )
        self._canonical_depth_source_sequence: int | None = None
        self._canonical_depth_recoveries = 0
        self._canonical_depth_losses = 0
        self._canonical_depth_last_receipt_at: datetime | None = None
        self._volume_buckets: dict[str, Decimal] = {}
        self._volume_windows: dict[str, VolumeWindow] = {}
        self._last_microstructure_at: datetime | None = None
        self._microstructure_blockers: tuple[str, ...] = (
            "PONG_MICROSTRUCTURE_WAITING_FOR_NATIVE_INPUT",
        )
        mapping_policy = MicrostructureEvidenceMappingPolicy(
            topology_version=topology.version,
            topology_hash=topology.binding_hash,
        )
        self._mapper = MicrostructureEvidenceMapper(
            policy=mapping_policy, topology=topology,
        )
        self._discernment = DiscernmentEngine(
            self._mapper.policy.discernment_policy(),
            instrument=topology.native_instrument,
            contract=topology.canonical_contract,
            topology=topology,
        )
        self._flow = ClassifiedFlowAdapter()
        self._liquidity = LiquidityClusterAdapter()
        self._volume = VolumeRegimeAdapter()
        self._volume_history = (
            None if history_verification_key is None else PongVolumeReferenceHistory(
                self._effective_state_root / "volume-reference.json", root_key=history_verification_key,
                expected_source=expected_market_source_fingerprint,
                topology=topology,
            )
        )
        self._load_certificate()

    @property
    def certificate_path(self) -> Path:
        return self._effective_state_root / "qualified-hour.json"

    @property
    def controller_path(self) -> Path:
        return self._operation_root / "controller-state.json"

    @property
    def host_telemetry_root(self) -> Path:
        return self._operation_root / "host-telemetry"

    def _observation_queue_metrics_locked(
        self,
        now_monotonic: float,
    ) -> tuple[int, float | None]:
        # Admission and worker dequeue bookkeeping share one bounded counter.
        # Queue.mutex is used only to inspect the oldest still-pending item and
        # is never held while waiting for the host lock.
        with self._admission_lock:
            depth = self._observation_queue_occupancy
        with self._observations.mutex:
            oldest = (
                None
                if not self._observations.queue
                else self._observations.queue[0][1]
            )
        age_ms = (
            None
            if oldest is None
            else max(0.0, (now_monotonic - oldest) * 1000.0)
        )
        return depth, age_ms

    def _update_protection_accounting_locked(
        self,
        now_monotonic: float | None = None,
    ) -> str | None:
        now = self._monotonic_clock() if now_monotonic is None else now_monotonic
        reason = self._event_protection_reason_locked()
        state = "ELIGIBLE" if reason is None else "PROTECTED"
        if self._protection_accounting_last_monotonic is not None:
            elapsed = max(
                0.0,
                now - self._protection_accounting_last_monotonic,
            )
            if self._protection_accounting_state == "ELIGIBLE":
                self._protection_eligible_elapsed_seconds += elapsed
            elif self._protection_accounting_state == "PROTECTED":
                self._protection_protected_elapsed_seconds += elapsed
        transitioned = (
            self._protection_accounting_state is not None
            and (
                state != self._protection_accounting_state
                or reason != self._protection_accounting_reason
            )
        )
        if transitioned:
            self._protection_transition_count += 1
            if reason is not None:
                key = self._periodic_protection_reason_key(reason)
                self._periodic_protection_reason_events[key] += 1
        self._protection_accounting_last_monotonic = now
        self._protection_accounting_state = state
        self._protection_accounting_reason = reason
        return reason

    @staticmethod
    def _periodic_protection_reason_key(reason: str) -> str:
        if reason == "RUNTIME_BRIDGE_UNAVAILABLE":
            return "bridge_unavailable_events"
        if reason.startswith("BRIDGE_STATE_REQUIRES_PROTECTION:"):
            return "bridge_state_protection_events"
        if reason == "BRIDGE_EXPOSURE_OR_INTENT_UNRESOLVED":
            return "bridge_exposure_or_intent_events"
        if reason == "RUNTIME_RECONCILIATION_UNRESOLVED":
            return "runtime_reconciliation_loss_events"
        if reason == "RUNTIME_EXECUTION_UNRESOLVED":
            return "runtime_execution_unresolved_events"
        if reason == "RUNTIME_POSITION_NOT_FLAT":
            return "runtime_position_not_flat_events"
        if reason == "RUNTIME_OWNED_ORDERS_PRESENT":
            return "runtime_owned_orders_events"
        if reason == "RUNTIME_ENTRY_ORDERS_PRESENT":
            return "runtime_entry_orders_events"
        if reason == "NATIVE_DIRECTIVE_PENDING":
            return "native_directive_pending_events"
        return "other_protection_events"

    def _observe_periodic_durability_locked(self) -> None:
        durability = self._collection.get("durability")
        if not isinstance(durability, Mapping):
            return
        values = tuple(
            durability.get(key)
            for key in ("enqueued_records", "written_records", "durable_records")
        )
        if not all(type(value) is int and value >= 0 for value in values):
            return
        counts = (int(values[0]), int(values[1]), int(values[2]))
        prior = self._periodic_last_durability_counts
        if prior is not None and any(
            current < previous for current, previous in zip(counts, prior)
        ):
            self._periodic_durability_counter_regressions += 1
        queue_depth = durability.get("queue_depth")
        lag_active = (
            len(set(counts)) != 1
            or (type(queue_depth) is int and queue_depth != 0)
        )
        if lag_active and self._periodic_durability_lag_active is not True:
            self._periodic_durability_lag_events += 1
        self._periodic_durability_lag_active = lag_active
        self._periodic_last_durability_counts = counts

    def _host_receipt_payload_locked(
        self,
        receipt_kind: str,
        *,
        fault: str | None = None,
        snapshot_stage: str = "CACHED_HOST_STATE",
        collector_snapshot_at_utc: str | None = None,
        collector_refresh_error: str | None = None,
    ) -> dict[str, object]:
        """Build one bounded process receipt without calling a status endpoint.

        UTC values are wall-clock provenance and source-receipt age. Host queue
        age/checkpoint cadence use the process monotonic clock. Scheduler and
        bounded service timers use QPC/perf_counter_ns. Values from these clock
        domains are reported separately and are never subtracted.
        """
        captured_monotonic = self._monotonic_clock()
        protection_reason = self._update_protection_accounting_locked(
            captured_monotonic,
        )
        observation_queue_depth, oldest_pending_age_ms = (
            self._observation_queue_metrics_locked(captured_monotonic)
        )
        runtime = {
            key: self._runtime.get(key)
            for key in _HOST_TELEMETRY_RUNTIME_KEYS
            if key in self._runtime
        }
        durability_source = self._collection.get("durability")
        durability = (
            {
                key: durability_source.get(key)
                for key in _HOST_TELEMETRY_DURABILITY_KEYS
                if key in durability_source
            }
            if isinstance(durability_source, Mapping)
            else {}
        )
        required_interval = self._collection.get("required_interval")
        collector = self._collection.get("collector")
        collection_producer = self._collection.get("producer")
        return {
            "schema": PONG_HOST_TELEMETRY_SCHEMA,
            "receipt_kind": receipt_kind,
            "persisted_at_utc": _now(),
            "fault": fault,
            "snapshot": {
                "stage": snapshot_stage,
                "captured_at_utc": _now(),
                "runtime_snapshot_status": (
                    "AVAILABLE"
                    if runtime
                    else "MISSING_NOT_YET_POLLED_OR_STATUS_UNAVAILABLE"
                ),
                "collector_snapshot_status": (
                    "AVAILABLE"
                    if self._collection
                    else "MISSING_NOT_YET_POLLED_OR_STATUS_UNAVAILABLE"
                ),
                "collector_snapshot_at_utc": collector_snapshot_at_utc,
                "collector_refresh_error": collector_refresh_error,
                "collector_durability_scope": (
                    "PRE_COLLECTOR_STOP_SNAPSHOT_NOT_FINAL_DURABILITY"
                    if receipt_kind == "final"
                    else "CACHED_OR_RUNNING_COLLECTOR_SNAPSHOT"
                ),
            },
            "clock_domains": {
                "persisted_at_utc": "UTC_WALL_CLOCK_RFC3339",
                "host_queue_age_and_checkpoint": "PROCESS_MONOTONIC_SECONDS",
                "source_receipt_age": "UTC_WALL_CLOCK_DURATION",
                "scheduler_deadlines": "PERF_COUNTER_NS_QUERY_PERFORMANCE_COUNTER",
                "service_and_write_aggregates": (
                    "PERF_COUNTER_NS_QUERY_PERFORMANCE_COUNTER"
                ),
                "cross_domain_subtraction_allowed": False,
            },
            "identity": {
                "host_process_instance_id": self._process_instance_id,
                "experiment_id": self._experiment_id,
                "selected_profile": self.selected_profile,
                "qualifying_architecture": self._qualifying_architecture,
                "loaded_artifact_hash": PONG_LOADED_ARTIFACT_HASH,
                "module_import_source_sha256": dict(_LOADED_SOURCE_HASHES),
                "topology_version": self.topology.version,
                "topology_hash": self.topology.binding_hash,
                "account": self.topology.account_name,
                "account_class": self.topology.account_class,
                "instrument": self.topology.native_instrument,
                "canonical_contract": self.topology.canonical_contract,
                "activation_authorized": self.activation_authorized,
                "live_authority": self.topology.live_authority,
                "live_capital": self.topology.live_capital,
                "launch_authorization_hash": (
                    None
                    if self.launch_authorization is None
                    else self.launch_authorization.manifest_hash
                ),
            },
            "runtime_identity_and_authority": runtime,
            "producer_identity": (
                None
                if self._producer is None
                else {
                    "session_id": self._producer[0],
                    "source_fingerprint": self._producer[1],
                    "build_fingerprint": self._producer[2],
                    "source_generation": self._source_generation,
                }
            ),
            "receiver": {
                "source_ingress_records": self._source_ingress_records,
                "authoritative_admission_records": self._authoritative_admission_records,
                "stream_counts": dict(self._stream_counts),
                "last_stream_sequences": dict(self._stream_sequences),
                "missing_events": dict(self._stream_missing),
                "out_of_order_events": dict(self._stream_out_of_order),
            },
            "host_work": {
                "observation_queue_depth": observation_queue_depth,
                "observation_queue_capacity": self._observations.maxsize,
                "observation_queue_high_water": self._observation_queue_high_water,
                "oldest_pending_queue_age_ms": oldest_pending_age_ms,
                "observation_queue_overflow": self._observation_overflow,
                "compute_pending": len(self._compute_pending),
                "compute_batch_unsubmitted": len(self._compute_batch),
                "compute_queue_limit": self._compute_limit,
                "compute_submitted": self._compute_submitted,
                "compute_applied": self._compute_applied,
                "compute_refused": self._compute_refused,
                "pending_native_directives": len(self._pending_native),
                "unprocessed_work": (
                    observation_queue_depth
                    + len(self._compute_pending)
                    + len(self._compute_batch)
                    + len(self._pending_native)
                ),
            },
            "collector": {
                "state": self._collection.get("state"),
                "identity": dict(collector) if isinstance(collector, Mapping) else None,
                "producer_identity": (
                    dict(collection_producer)
                    if isinstance(collection_producer, Mapping)
                    else None
                ),
                "watermark": (
                    dict(required_interval)
                    if isinstance(required_interval, Mapping)
                    else None
                ),
                "durability": durability,
            },
            "freshness": {
                "last_host_queue_age_ms": self._last_host_queue_age_ms,
                "maximum_host_queue_age_ms": self._maximum_host_queue_age_ms,
                "last_source_receipt_age_ms": self._last_source_receipt_age_ms,
                "maximum_source_receipt_age_ms": self._maximum_source_receipt_age_ms,
                "breach_count": self._freshness_breach_count,
                "current_blocker": self._fhb_guard_blocker,
                "recovery_count": self._fhb_guard_recoveries,
            },
            "scheduler": {
                "metrics": self._periodic_deadline.metrics(),
                "source_receiver_ingress_records": self._source_ingress_records,
                "authoritative_admission_records": self._authoritative_admission_records,
                "event_driven_analytical_evaluations": self._event_driven_evaluations,
                "periodic_flat_analytical_evaluations": self._periodic_evaluations,
                "periodic_ratio_telemetry": self._periodic_telemetry_snapshot_locked(
                    captured_monotonic=captured_monotonic,
                    observation_queue_depth=observation_queue_depth,
                    oldest_pending_queue_age_ms=oldest_pending_age_ms,
                ),
            },
            "protection": {
                "active_path": (
                    "PERIODIC_FLAT"
                    if protection_reason is None
                    else "EVENT_DRIVEN_PROTECTION"
                ),
                "reason": protection_reason,
                "last_transition": self._last_protection_transition,
                "elapsed_clock": "PROCESS_MONOTONIC_SECONDS",
                "eligible_elapsed_seconds": round(
                    self._protection_eligible_elapsed_seconds, 9,
                ),
                "protected_elapsed_seconds": round(
                    self._protection_protected_elapsed_seconds, 9,
                ),
                "transition_count": self._protection_transition_count,
            },
            "bounded_aggregates": {
                "host_service": {
                    "count": self._host_service_count,
                    "total_ns": self._host_service_total_ns,
                    "maximum_ns": self._host_service_maximum_ns,
                },
                "observation_processing": {
                    "count": self._observation_processing_count,
                    "total_ns": self._observation_processing_total_ns,
                    "maximum_ns": self._observation_processing_maximum_ns,
                },
                "analysis_processing": {
                    "count": self._analysis_processing_count,
                    "total_ns": self._analysis_processing_total_ns,
                    "maximum_ns": self._analysis_processing_maximum_ns,
                },
                "receipt_write_stalls": {
                    "completed_count_before_current_write": self._receipt_write_count,
                    "failure_count_before_current_write": (
                        self._receipt_write_failure_count
                    ),
                    "total_ns_before_current_write": self._receipt_write_total_ns,
                    "maximum_ns_before_current_write": self._receipt_write_maximum_ns,
                },
            },
            "missing_metrics": {
                "receiver_authenticated_processing_duration": "MISSING_NOT_EXPOSED",
                "receiver_queue_lock_wait": "MISSING_NOT_EXPOSED",
                "host_rlock_wait": "MISSING_NOT_INSTRUMENTED",
            },
            "fhb": {
                "disposition": FHB_ARCHIVE_DISPOSITION,
                "instantiated": self._fhb is not None,
            },
            "test_only": False,
        }

    def _persist_host_receipt_locked(
        self,
        receipt_kind: str,
        *,
        fault: str | None = None,
        snapshot_stage: str = "CACHED_HOST_STATE",
        collector_snapshot_at_utc: str | None = None,
        collector_refresh_error: str | None = None,
    ) -> bool:
        started_ns = time.perf_counter_ns()
        succeeded = False
        try:
            _atomic_write(
                self.host_telemetry_root / f"{receipt_kind}.json",
                self._host_receipt_payload_locked(
                    receipt_kind,
                    fault=fault,
                    snapshot_stage=snapshot_stage,
                    collector_snapshot_at_utc=collector_snapshot_at_utc,
                    collector_refresh_error=collector_refresh_error,
                ),
            )
            self._host_telemetry_error = None
            succeeded = True
        except Exception as error:
            # Telemetry cannot grant authority or change runtime decisions.
            self._host_telemetry_error = (
                "PONG_HOST_TELEMETRY_WRITE_FAILED:" + type(error).__name__
            )
        finally:
            elapsed_ns = max(0, time.perf_counter_ns() - started_ns)
            self._receipt_write_count += 1
            self._receipt_write_failure_count += int(not succeeded)
            self._receipt_write_total_ns += elapsed_ns
            self._receipt_write_maximum_ns = max(
                self._receipt_write_maximum_ns,
                elapsed_ns,
            )
        return succeeded

    def _persist_first_host_fault_locked(
        self,
        fault: str,
        *,
        now_monotonic: float | None = None,
    ) -> None:
        now = self._monotonic_clock() if now_monotonic is None else now_monotonic
        if self._host_first_fault is None:
            # A callback/compute path can set the canonical input latch before
            # the worker reaches its next persistence point. Preserve that
            # earlier safety fact ahead of a later host-loop exception.
            self._host_first_fault = self._input_error or fault
        if self._host_first_fault_persisted:
            return
        if (
            self._host_first_fault_retry_after_monotonic is not None
            and now < self._host_first_fault_retry_after_monotonic
        ):
            return
        self._host_first_fault_persisted = self._persist_host_receipt_locked(
            "first-fault",
            fault=self._host_first_fault,
            snapshot_stage="FIRST_FAULT_CACHED_HOST_STATE",
        )
        if not self._host_first_fault_persisted:
            self._host_first_fault_retry_after_monotonic = (
                now + _HOST_TELEMETRY_FAULT_RETRY_SECONDS
            )

    def _persist_host_checkpoint_locked(self, now_monotonic: float) -> None:
        if (
            self._input_error is not None
            and self._host_first_fault is None
        ):
            self._persist_first_host_fault_locked(
                self._input_error,
                now_monotonic=now_monotonic,
            )
        elif (
            self._host_first_fault is not None
            and not self._host_first_fault_persisted
        ):
            self._persist_first_host_fault_locked(
                self._host_first_fault,
                now_monotonic=now_monotonic,
            )
        if (
            self._last_host_checkpoint_monotonic is not None
            and now_monotonic - self._last_host_checkpoint_monotonic
            < _HOST_TELEMETRY_CHECKPOINT_SECONDS
        ):
            return
        self._persist_host_receipt_locked(
            "checkpoint",
            snapshot_stage="RUNNING_CHECKPOINT_AFTER_STATUS_POLL",
            collector_snapshot_at_utc=_now(),
        )
        self._last_host_checkpoint_monotonic = now_monotonic

    def _qualifying_certificate_identity(self) -> dict[str, object]:
        return {
            "qualifying_architecture": self._qualifying_architecture,
            "topology_version": self.topology.version,
            "topology_hash": self.topology.binding_hash,
            "native_instrument": self.topology.native_instrument,
            "canonical_contract": self.topology.canonical_contract,
            "scheduling_mode": self._scheduling.mode.value,
            "periodic_interval_ms": self._scheduling.interval_ms,
            "fhb_profile": None,
            "fhb_disposition": FHB_ARCHIVE_DISPOSITION,
            "activation_authorized": self.activation_authorized,
            "live_authority": self.topology.live_authority,
            "live_capital": self.topology.live_capital,
            "loaded_artifact_hash": PONG_LOADED_ARTIFACT_HASH,
        }

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._compute_workers and self._compute_pool is None:
                self._compute_pool = open_compute_pool(self._compute_workers)
            self._stop.clear()
            self._update_protection_accounting_locked()
            self._persist_host_receipt_locked(
                "start",
                snapshot_stage="HOST_START_BEFORE_STATUS_POLL",
            )
            self._thread = threading.Thread(
                target=self._run,
                name="PONG_PRODUCTION_CONTROLLER_HOST",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        drain_incomplete = False
        if thread is not None:
            thread.join(timeout_seconds)
            if thread.is_alive():
                drain_incomplete = True
        if self._compute_pool is not None:
            self._compute_pool.shutdown(wait=False, cancel_futures=True)
            self._compute_pool = None
        collector_snapshot_at_utc = _now()
        collector_refresh_error = None
        final_collection: dict[str, object] | None = None
        try:
            # This is deliberately outside the host RLock. The collector owner
            # stops later, so this is a pre-collector-stop snapshot, not proof
            # of final collector durability.
            final_collection = dict(self._collector_status())
        except Exception as error:
            collector_refresh_error = (
                "PONG_FINAL_COLLECTOR_REFRESH_FAILED:" + type(error).__name__
            )
        with self._lock:
            if final_collection is not None:
                self._collection = final_collection
                self._observe_periodic_durability_locked()
            if self._input_error is not None and self._host_first_fault is None:
                self._persist_first_host_fault_locked(self._input_error)
            final_fault = (
                "PONG_HOST_DRAIN_INCOMPLETE"
                if drain_incomplete
                else self._host_first_fault
            )
            self._persist_host_receipt_locked(
                "final",
                fault=final_fault,
                snapshot_stage="POST_HOST_JOIN_PRE_COLLECTOR_STOP_REFRESH",
                collector_snapshot_at_utc=collector_snapshot_at_utc,
                collector_refresh_error=collector_refresh_error,
            )
        if drain_incomplete:
            raise RuntimeError("PONG_HOST_DRAIN_INCOMPLETE")

    def _load_certificate(self) -> None:
        path = self.certificate_path
        if not path.exists():
            return
        try:
            raw = path.read_bytes()
            value = json.loads(raw)
            if type(value) is not dict or raw != _canonical(value):
                raise ValueError("canonical certificate")
            digest = value.get("certificate_hash")
            body = {key: item for key, item in value.items() if key != "certificate_hash"}
            if (
                value.get("schema") != PONG_QUALIFYING_HOUR_SCHEMA
                or digest != canonical_hash(body)
                or value.get("test_only") is not False
                or value.get("eligible") is not True
            ):
                raise ValueError("certificate identity")
            if (
                self._qualifying_architecture is not None
                and value.get("qualifying_identity")
                != self._qualifying_certificate_identity()
            ):
                raise ValueError("certificate qualifying identity")
            journal = Path(str(value.get("journal_path"))).resolve()
            if not journal.is_file():
                raise ValueError("certificate journal missing")
            self._certificate = value
            self._daily_bias = self._bias_from_certificate(value)
            self._retained_hour_seed = True
        except Exception as error:
            self._certificate_error = (
                "PONG_QUALIFYING_HOUR_CERTIFICATE_INVALID:" + type(error).__name__
            )

    def on_observation(self, record: NinjaTraderObservation) -> None:
        """Bounded callback admission; the host worker owns processing and I/O."""
        admitted = self._monotonic_clock()
        with self._admission_lock:
            try:
                self._observations.put_nowait((record, admitted))
                self._observation_queue_occupancy += 1
                self._source_ingress_records += 1
                self._observation_queue_high_water = max(
                    self._observation_queue_high_water,
                    self._observation_queue_occupancy,
                )
                if record.observation_type == "DEPTH":
                    self._last_depth_callback_monotonic = admitted
            except queue.Full:
                self._observation_overflow += 1
                self._input_error = "PONG_PRODUCTION_OBSERVATION_QUEUE_OVERFLOW"

    def _consume_observation(
        self,
        record: NinjaTraderObservation,
        *,
        admitted_monotonic: float | None = None,
    ) -> None:
        """Process exactly one queued causal event before admitting the next."""
        def cursor() -> tuple[object, ...]:
            return tuple(
                values[-1].event.event_id if values else None
                for values in (self._trades, self._depth[BookSide.BID], self._depth[BookSide.ASK])
            )
        with self._lock:
            if (
                record.observation_type == "DEPTH"
                and (
                    self._fhb is not None
                    or self._qualifying_architecture is not None
                )
            ):
                now_monotonic = self._monotonic_clock()
                depth_admitted = (
                    now_monotonic
                    if admitted_monotonic is None
                    else admitted_monotonic
                )
                self._last_depth_callback_monotonic = max(
                    depth_admitted,
                    self._last_depth_callback_monotonic or depth_admitted,
                )
                if admitted_monotonic is None:
                    if self._fhb_guard_blocker in {
                        "FHB_WATCHDOG_WAITING_FOR_OBSERVATION",
                        "PONG_WATCHDOG_WAITING_FOR_OBSERVATION",
                    }:
                        self._fhb_guard_blocker = None
                else:
                    host_queue_age_ms = max(
                        0.0,
                        (now_monotonic - admitted_monotonic) * 1000.0,
                    )
                    source_receipt_age_ms = max(
                        0.0,
                        (_time(_now()) - _time(record.ninja_receipt_time)).total_seconds()
                        * 1000.0,
                    )
                    self._last_host_queue_age_ms = host_queue_age_ms
                    self._maximum_host_queue_age_ms = max(
                        self._maximum_host_queue_age_ms,
                        host_queue_age_ms,
                    )
                    self._last_source_receipt_age_ms = source_receipt_age_ms
                    self._maximum_source_receipt_age_ms = max(
                        self._maximum_source_receipt_age_ms,
                        source_receipt_age_ms,
                    )
                    blocker = None
                    maximum_queue_age_ms = (
                        PONG_QUALIFYING_MAXIMUM_QUEUE_AGE_MS
                        if self._qualifying_architecture is not None
                        else self._fhb.config.maximum_queue_age_milliseconds
                    )
                    maximum_event_age_ms = (
                        PONG_QUALIFYING_MAXIMUM_EVENT_AGE_MS
                        if self._qualifying_architecture is not None
                        else self._fhb.config.maximum_event_age_milliseconds
                    )
                    if host_queue_age_ms > maximum_queue_age_ms:
                        blocker = (
                            "PONG_HOST_QUEUE_AGE_EXCEEDED"
                            if self._qualifying_architecture is not None
                            else "FHB_HOST_QUEUE_AGE_EXCEEDED"
                        )
                    elif source_receipt_age_ms > maximum_event_age_ms:
                        blocker = (
                            "PONG_EVENT_PROCESSING_AGE_EXCEEDED"
                            if self._qualifying_architecture is not None
                            else "FHB_EVENT_PROCESSING_AGE_EXCEEDED"
                        )
                    if blocker is not None:
                        self._fhb_guard_blocker = blocker
                        self._freshness_breach_count += 1
                        if self._fhb_guard_breach_started is None:
                            self._fhb_guard_breach_started = now_monotonic
                    elif self._fhb_guard_breach_started is not None:
                        duration = max(
                            0.0,
                            now_monotonic - self._fhb_guard_breach_started,
                        )
                        self._fhb_guard_last_breach_seconds = duration
                        self._fhb_guard_maximum_breach_seconds = max(
                            self._fhb_guard_maximum_breach_seconds,
                            duration,
                        )
                        self._fhb_guard_recoveries += 1
                        self._fhb_guard_breach_started = None
                        self._fhb_guard_blocker = None
                    else:
                        self._fhb_guard_blocker = None
            before = cursor()
            stream_before = tuple(sorted(self._stream_sequences.items()))
            self._process_observation(record)
            if self._input_error is not None:
                self._persist_first_host_fault_locked(self._input_error)
                return
            receipt = _time(record.ninja_receipt_time)
            self._last_admitted_receipt_at = max(
                receipt, self._last_admitted_receipt_at or receipt,
            )
            self._authoritative_admission_records += 1
            feature_state_changed = cursor() != before
            canonical_market_state_changed = (
                tuple(sorted(self._stream_sequences.items())) != stream_before
            )
            if canonical_market_state_changed:
                self._market_state_revision += 1
            if not feature_state_changed and not canonical_market_state_changed:
                return
            protection_reason = self._update_protection_accounting_locked()
            evaluate_now = (
                feature_state_changed
                if not self._scheduling.enabled
                else protection_reason is not None and canonical_market_state_changed
            )
            if evaluate_now:
                if protection_reason is not None:
                    self._last_protection_transition = protection_reason
                cadence_due = True
                now_monotonic: float | None = None
                if (
                    not self._scheduling.enabled
                    and self._fhb is not None
                    and self._fhb.config.entry_evaluation_interval_ms
                ):
                    now_monotonic = self._monotonic_clock()
                    interval_seconds = (
                        self._fhb.config.entry_evaluation_interval_ms / 1000.0
                    )
                    cadence_due = (
                        self._last_entry_evaluation_started is None
                        or now_monotonic - self._last_entry_evaluation_started
                        >= interval_seconds
                    )
                if cadence_due:
                    if now_monotonic is not None:
                        prior = self._last_entry_evaluation_started
                        if prior is not None:
                            spacing_ms = max(0.0, (now_monotonic - prior) * 1000.0)
                            self._minimum_entry_evaluation_spacing_ms = (
                                spacing_ms
                                if self._minimum_entry_evaluation_spacing_ms is None
                                else min(self._minimum_entry_evaluation_spacing_ms, spacing_ms)
                            )
                        self._last_entry_evaluation_started = now_monotonic
                        self._entry_evaluation_starts += 1
                        self._last_entry_evaluation = {
                            "policy_id": self._fhb.config.entry_policy_id,
                            "evaluation_started_at": receipt.isoformat().replace("+00:00", "Z"),
                            "source_generation": self._source_generation,
                            "applied_state_revision": self._market_state_revision,
                            "source_sequences": dict(self._stream_sequences),
                        }
                    self._event_driven_evaluations += 1
                    if self._timed_evaluate_microstructure(receipt):
                        self._protection_completed_full_analyses += 1
                else:
                    self._entry_evaluation_deferred += 1

    def _event_protection_reason_locked(self) -> str | None:
        """Return why the event path must remain authoritative right now.

        Periodic work is allowed only while both independently observed runtime
        truth and the durable bridge are cleanly flat.  An absent or stale field
        is deliberately unresolved rather than interpreted as flat.
        """
        if not self._scheduling.enabled:
            return "EVENT_DRIVEN_CONFIGURATION"
        bridge = self._bridge
        if bridge is None:
            return "RUNTIME_BRIDGE_UNAVAILABLE"
        machine = bridge.machine
        if machine.state not in {
            PongState.FLAT_WAIT_ALIGNMENT,
            PongState.FLAT_WAIT_BEAT,
        }:
            return "BRIDGE_STATE_REQUIRES_PROTECTION:" + machine.state.value
        if machine.actual_quantity != 0 or machine.pending_action is not None:
            return "BRIDGE_EXPOSURE_OR_INTENT_UNRESOLVED"
        runtime = self._runtime
        if runtime.get("reconciliation_current") is not True:
            return "RUNTIME_RECONCILIATION_UNRESOLVED"
        if runtime.get("unresolved_execution") is not False:
            return "RUNTIME_EXECUTION_UNRESOLVED"
        if int(runtime.get("position_quantity") or 0) != 0:
            return "RUNTIME_POSITION_NOT_FLAT"
        if int(runtime.get("working_owned_orders") or 0) != 0:
            return "RUNTIME_OWNED_ORDERS_PRESENT"
        if int(runtime.get("working_entry_orders") or 0) != 0:
            return "RUNTIME_ENTRY_ORDERS_PRESENT"
        if self._pending_native:
            return "NATIVE_DIRECTIVE_PENDING"
        return None

    def _service_periodic_deadline(self, now_ns: int | None = None) -> None:
        """Run one current flat-mode evaluation or account for its supersession."""
        due = self._periodic_deadline.take_due(now_ns)
        if due is None:
            return
        with self._lock:
            if self._stop.is_set():
                self._periodic_deadline.mark_shutdown(due)
                return
            protection_reason = self._update_protection_accounting_locked()
            if protection_reason is not None:
                self._last_protection_transition = protection_reason
                self._periodic_deadline.supersede_for_protection(due)
                return
            if self._last_admitted_receipt_at is None:
                self._periodic_deadline.complete(
                    due, applied=False, intentional_no_work=True,
                )
                return
            cutoff = max(self._last_admitted_receipt_at, _time(_now()))
            if (
                self._last_periodic_cutoff is not None
                and cutoff < self._last_periodic_cutoff
            ):
                self._input_error = "PONG_PERIODIC_RECEIPT_CUTOFF_REGRESSED"
                self._microstructure_blockers = (self._input_error,)
                self._periodic_deadline.complete(due, applied=False)
                return
            self._last_periodic_cutoff = cutoff
            self._periodic_deadline.dispatch(due)
            self._periodic_evaluations += 1
            completed_full_analysis = bool(
                self._timed_evaluate_microstructure(cutoff)
            )
            if completed_full_analysis:
                self._ordinary_completed_full_analyses += 1
            if (
                self._microstructure_blockers
                and self._discernment.last_snapshot is not None
            ):
                update = self._discernment.on_expiry(
                    cutoff.isoformat().replace("+00:00", "Z"),
                )
                self._apply_discernment_update(update)
            self._periodic_deadline.complete(
                due,
                applied=(
                    completed_full_analysis and self._input_error is None
                ),
            )

    def _periodic_wait_seconds(self) -> float | None:
        return self._periodic_deadline.next_wait_seconds()

    def _process_observation(self, record: NinjaTraderObservation) -> None:
        """Validate the native envelope and update causal state on the worker."""
        if type(record) is not NinjaTraderObservation or record.signature_verified is not True:
            with self._lock:
                self._input_error = "PONG_NATIVE_INPUT_ATTESTATION_UNVERIFIED"
            return
        producer = (
            record.session_id,
            record.producer.source_fingerprint,
            record.producer.build_fingerprint,
        )
        with self._lock:
            # Native continuity and attestation faults are terminal for this
            # host instance. Collection/durability is owned upstream, while
            # this analytical projection must not silently rebuild after a
            # sequence or generation discontinuity.
            if (
                self._qualifying_architecture is not None
                and self._input_error is not None
            ):
                return
            if record.producer.source_fingerprint != self.expected_market_source_fingerprint:
                self._input_error = "PONG_NATIVE_INPUT_SOURCE_FINGERPRINT_MISMATCH"
                return
            if self._producer is None:
                self._producer = producer
            elif self._producer != producer:
                self._input_error = "PONG_NATIVE_INPUT_PRODUCER_CHANGED"
                return
            try:
                if record.observation_type == HISTORY_TYPE:
                    if self._volume_history is not None:
                        self._volume_history.admit(record, received_at=_now())
                    return
                if record.observation_type == "HISTORICAL_WARM_START":
                    warm = PongWarmStart.from_observation(record, topology=self.topology)
                    self._warm_start = warm
                    self._input_adapter = PongCausalInputAdapter(
                        warm, topology=self.topology,
                    )
                    return
                if record.observation_type == "HOURLY_BAR":
                    if self._input_adapter is None:
                        if self._retained_hour_seed:
                            # The hash-bound retained certificate already owns
                            # the two completed hours required for this bounded
                            # run.  A later live bar is never backfilled into it.
                            return
                        raise PongNativeInputRefused("PONG_WARM_START_MISSING")
                    self._input_adapter.admit_live_hour(record)
                    self._hour_pair = self._input_adapter.latest_hour_pair()
                    return
                if record.observation_type not in self._stream_counts:
                    return
                if record.payload.get("contract_id") != self.topology.native_instrument:
                    raise ValueError("PONG_NATIVE_CONTRACT_MISMATCH")
                generation = record.payload.get("source_generation")
                if generation is not None:
                    if type(generation) is not int or generation <= 0:
                        raise ValueError("PONG_NATIVE_SOURCE_GENERATION_INVALID")
                    if self._source_generation is not None and generation != self._source_generation:
                        self._input_error = "PONG_NATIVE_SOURCE_GENERATION_CHANGED"
                        self._invalidate_canonical_depth(self._input_error)
                        if self._fhb is not None:
                            self._fhb.invalidate(self._input_error)
                        self._source_generation = generation
                        return
                    self._source_generation = generation
                sequence = int(record.payload["source_event_sequence"])
                prior = self._stream_sequences.get(record.observation_type)
                if prior is not None:
                    if sequence <= prior:
                        self._stream_out_of_order[record.observation_type] += 1
                        self._input_error = "PONG_NATIVE_INPUT_DUPLICATE_OR_OUT_OF_ORDER"
                        return
                    if sequence != prior + 1:
                        self._stream_missing[record.observation_type] += sequence - prior - 1
                        # Advance the observed source cursor even though this
                        # record cannot bridge the missing range. Otherwise
                        # every later record re-counts the same absent events
                        # and freezes an already-invalid stream projection.
                        # The input-error latch remains fail-closed.
                        self._stream_sequences[record.observation_type] = sequence
                        self._input_error = "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP"
                        self._invalidate_canonical_depth(self._input_error)
                        if self._fhb is not None:
                            self._fhb.invalidate(self._input_error)
                        return
                self._stream_sequences[record.observation_type] = sequence
                self._stream_counts[record.observation_type] += 1
                if record.observation_type == "TRADE":
                    self._admit_trade(record)
                elif record.observation_type == "QUOTE":
                    self._last_quote_record = record
                elif record.observation_type == "DEPTH":
                    if self._fhb is None:
                        self._admit_depth(record)
                    else:
                        self._admit_depth_fhb(record)
            except (
                KeyError,
                TypeError,
                ValueError,
                RuntimeError,
                PongNativeInputRefused,
            ) as error:
                self._input_error = str(error) or type(error).__name__
                if record.observation_type == "DEPTH":
                    self._invalidate_canonical_depth(self._input_error)
                if self._fhb is not None and record.observation_type == "DEPTH":
                    self._fhb.invalidate(self._input_error)

    @staticmethod
    def _event_ref(record: NinjaTraderObservation) -> CausalEventRef:
        received = record.ninja_receipt_time
        event_at = record.provider_timestamp or record.exchange_timestamp or received
        if _time(event_at) > _time(received):
            raise ValueError("PONG_NATIVE_PROVIDER_TIMESTAMP_AFTER_RECEIPT")
        producer = record.producer
        return CausalEventRef(
            record.observation_id,
            record.payload_hash,
            event_at,
            received,
            received,
            record.local_monotonic_sequence,
            "LOCAL_CALLBACK_ORDER_ONLY",
            "NINJATRADER_NATIVE:"
            + record.session_id + ":" + producer.source_fingerprint + ":"
            + producer.build_fingerprint,
        )

    def _admit_trade(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        classification = PongAuthenticHourCollector.classify_trade(record, self._last_quote_record)
        side = AggressorSide(classification["side"])
        provenance = AggressorProvenance(classification["provenance"])
        trade = ClassifiedTrade(
            self._event_ref(record), self.topology.native_instrument, Decimal(str(payload["price"])),
            Decimal(str(payload["size"])), side, provenance,
        )
        self._trades.append(trade)
        event = _time(trade.event.event_at)
        bucket = event.replace(minute=(event.minute // 5) * 5, second=0, microsecond=0)
        key = bucket.isoformat().replace("+00:00", "Z")
        self._volume_buckets[key] = self._volume_buckets.get(key, Decimal("0")) + trade.volume
        # Retain the causal hour seam independently of a status snapshot that
        # may already point at the next interval. Certification filters exactly.
        hour = event.replace(minute=0, second=0, microsecond=0)
        if self._classified_retention_hour is None or hour > self._classified_retention_hour:
            self._classified_retention_hour = hour
            self._classified_hour = [item for item in self._classified_hour
                                     if _time(item.event_at) >= hour - timedelta(hours=1)]
        if side in {AggressorSide.BUY, AggressorSide.SELL}:
            self._classified_hour.append(ClassifiedTradeInput(
                event_id=record.observation_id, payload_hash=record.payload_hash,
                source="NINJATRADER_NATIVE_AGGRESSOR:" + str(payload.get("aggressor_source") or "UNKNOWN"),
                instrument=self.topology.native_instrument, side=ClassifiedSide(side.value), size=trade.volume,
                event_at=trade.event.event_at, observed_at=record.ninja_receipt_time,
                available_at=record.ninja_receipt_time, authenticated=True,
            ))

    def _invalidate_canonical_depth(self, reason: str) -> None:
        if self._canonical_depth_ready:
            self._canonical_depth_losses += 1
        self._canonical_depth = {BookSide.BID: (), BookSide.ASK: ()}
        self._canonical_depth_ready = False
        self._canonical_depth_reason = reason
        self._canonical_depth_last_receipt_at = None
        self._depth[BookSide.BID].clear()
        self._depth[BookSide.ASK].clear()

    @staticmethod
    def _canonical_book_side(
        payload: Mapping[str, object],
        *,
        key: str,
        side: BookSide,
    ) -> tuple[tuple[Decimal, Decimal], ...]:
        raw = payload.get(key)
        if not isinstance(raw, list) or not raw or len(raw) > 10:
            raise ValueError("PONG_CANONICAL_DEPTH_LEVEL_COUNT_INVALID")
        rows: list[tuple[Decimal, Decimal]] = []
        seen: set[Decimal] = set()
        previous: Decimal | None = None
        for item in raw:
            if not isinstance(item, Mapping):
                raise ValueError("PONG_CANONICAL_DEPTH_LEVEL_INVALID")
            try:
                price = Decimal(str(item.get("price")))
                quantity = Decimal(str(item.get("size")))
            except (InvalidOperation, TypeError, ValueError) as error:
                raise ValueError("PONG_CANONICAL_DEPTH_VALUE_INVALID") from error
            if (
                not price.is_finite()
                or price <= 0
                or not quantity.is_finite()
                or quantity <= 0
            ):
                raise ValueError("PONG_CANONICAL_DEPTH_VALUE_INVALID")
            if price in seen:
                raise ValueError("PONG_CANONICAL_DEPTH_DUPLICATE_PRICE")
            if previous is not None and (
                (side is BookSide.BID and previous < price)
                or (side is BookSide.ASK and previous > price)
            ):
                raise ValueError("PONG_CANONICAL_DEPTH_ORDER_INVALID")
            seen.add(price)
            rows.append((price, quantity))
            previous = price
        return tuple(rows)

    def _admit_depth(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        if (
            payload.get("is_reset") is True
            or payload.get("mutation_is_reset") is True
        ):
            self._invalidate_canonical_depth(
                "PONG_CANONICAL_DEPTH_RESET_REWARM_REQUIRED",
            )
            return
        if (
            str(payload.get("status") or "").upper() == "UNAVAILABLE"
            or payload.get("available", True) is not True
        ):
            reason = str(payload.get("reason") or "NATIVE_DEPTH_UNAVAILABLE")
            self._invalidate_canonical_depth(
                "PONG_CANONICAL_DEPTH_UNAVAILABLE:" + reason,
            )
            return
        try:
            bids = self._canonical_book_side(
                payload, key="bids", side=BookSide.BID,
            )
            asks = self._canonical_book_side(
                payload, key="asks", side=BookSide.ASK,
            )
            if (
                self._qualifying_architecture is not None
                and (len(bids) != 10 or len(asks) != 10)
            ):
                raise ValueError("PONG_CANONICAL_DEPTH_10X10_REQUIRED")
            if bids[0][0] >= asks[0][0]:
                raise ValueError("PONG_CANONICAL_DEPTH_CROSSED")
        except ValueError:
            if self._qualifying_architecture is not None:
                raise
            bids = asks = ()
        if bids and asks:
            was_ready = self._canonical_depth_ready
            self._canonical_depth = {BookSide.BID: bids, BookSide.ASK: asks}
            self._canonical_depth_ready = True
            self._canonical_depth_reason = None
            self._canonical_depth_source_sequence = int(
                payload["source_event_sequence"],
            )
            self._canonical_depth_last_receipt_at = _time(record.ninja_receipt_time)
            if not was_ready and self._canonical_depth_source_sequence is not None:
                self._canonical_depth_recoveries += 1
        raw_side = str(payload.get("side") or "").upper()
        if raw_side not in {"BID", "ASK"}:
            raise ValueError("PONG_CANONICAL_DEPTH_MUTATION_SIDE_INVALID")
        side = BookSide(raw_side)
        levels = payload.get("bids" if side is BookSide.BID else "asks")
        if not isinstance(levels, list) or not levels:
            return
        # Native position-shift notifications can carry mutation_price=0
        # while the signed post-mutation book snapshot is valid. They retain
        # their stream sequence and durable collection evidence, but are not a
        # price-bearing microstructure observation.
        try:
            price = Decimal(str(
                payload.get("mutation_resolved_price")
                or payload["mutation_price"]
            ))
        except (InvalidOperation, KeyError, TypeError, ValueError) as error:
            if self._qualifying_architecture is not None:
                raise ValueError(
                    "PONG_CANONICAL_DEPTH_MUTATION_IDENTITY_UNRESOLVED",
                ) from error
            return
        if not price.is_finite() or price <= 0:
            if self._qualifying_architecture is not None:
                raise ValueError(
                    "PONG_CANONICAL_DEPTH_MUTATION_IDENTITY_UNRESOLVED",
                )
            return
        operation = str(
            payload.get("operation") or payload.get("mutation_operation") or ""
        ).upper()
        post_prices = {item[0] for item in self._canonical_depth[side]}
        post_touch = Decimal(str(levels[0]["price"]))
        if operation in {"ADD", "UPDATE"}:
            if price not in post_prices:
                mutation_position = payload.get("mutation_position")
                outside_retained_book = (
                    type(mutation_position) is int
                    and mutation_position >= len(levels)
                    and (
                        (
                            side is BookSide.BID
                            and price < self._canonical_depth[side][-1][0]
                        )
                        or (
                            side is BookSide.ASK
                            and price > self._canonical_depth[side][-1][0]
                        )
                    )
                )
                if outside_retained_book:
                    # The signed exact 10x10 snapshot remains coherent when a
                    # native mutation occurs strictly beyond its retained
                    # core. Preserve the source cursor/canonical recovery but
                    # do not manufacture a visible analytical observation.
                    return
                if self._qualifying_architecture is not None:
                    raise ValueError(
                        "PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK",
                    )
                return
        elif operation == "REMOVE":
            # A removal is identified by the authenticated resolved price from
            # the native positional cursor. The removed level is intentionally
            # absent from the post-mutation snapshot and may have been the old
            # best level, outside the new touch.
            touch = (
                price
                if (
                    (side is BookSide.BID and price > post_touch)
                    or (side is BookSide.ASK and price < post_touch)
                )
                else post_touch
            )
        elif self._qualifying_architecture is not None:
            raise ValueError("PONG_CANONICAL_DEPTH_MUTATION_OPERATION_INVALID")
        else:
            touch = post_touch
            if (
                (side is BookSide.BID and price > touch)
                or (side is BookSide.ASK and price < touch)
            ):
                return
        if operation in {"ADD", "UPDATE"}:
            touch = post_touch
        kind = DepthEventKind.UPDATE
        if payload.get("is_reset") is True:
            kind = DepthEventKind.RECONNECT_RESET
        nearby = sum((Decimal(str(item["size"])) for item in levels), Decimal("0"))
        self._depth[side].append(DepthObservation(
            self._event_ref(record), self.topology.native_instrument, side, price,
            Decimal(str(payload.get("mutation_volume") or 0)), nearby, touch, kind,
            Decimal("0"), Decimal("0"), False, PriceResponse.UNAVAILABLE,
        ))

    def _admit_depth_fhb(self, record: NinjaTraderObservation) -> None:
        """Maintain all signed FHB state and forward only eligible feature work."""
        assert self._fhb is not None
        decision = self._fhb.process_depth(record.payload, self._event_ref(record))
        if decision.clear_feature_windows:
            self._depth[BookSide.BID].clear()
            self._depth[BookSide.ASK].clear()
        if decision.action.value == "REWARM_REQUIRED":
            self._microstructure_blockers = (decision.reason,)
            return
        if decision.action.value == "SKIP_OUT_OF_CORE":
            if self._microstructure_blockers and self._microstructure_blockers[0].startswith(
                "FHB_REWARM_REQUIRED"
            ):
                self._microstructure_blockers = (
                    "PONG_MICROSTRUCTURE_WAITING_FOR_FHB_ELIGIBLE_EVENT",
                )
            return
        if decision.side is None or not decision.seed_observations:
            self._microstructure_blockers = ("FHB_ELIGIBLE_EVENT_MISSING_SEED",)
            return
        for observation in decision.seed_observations:
            self._depth[observation.side].append(observation)
        self._fhb_output_batches += 1
        self._fhb_output_observations += len(decision.seed_observations)
        self._fhb_last_output_source_sequence = int(
            record.payload["source_event_sequence"]
        )

    def _timed_evaluate_microstructure(self, at: datetime) -> bool:
        started_ns = time.perf_counter_ns()
        try:
            return self._evaluate_microstructure(at)
        finally:
            elapsed_ns = max(0, time.perf_counter_ns() - started_ns)
            self._analysis_processing_count += 1
            self._analysis_processing_total_ns += elapsed_ns
            self._analysis_processing_maximum_ns = max(
                self._analysis_processing_maximum_ns,
                elapsed_ns,
            )

    def _evaluate_microstructure(self, at: datetime) -> bool:
        if (
            self._qualifying_architecture is not None
            and not self._canonical_depth_ready
        ):
            self._last_microstructure_at = at
            self._microstructure_blockers = (
                self._canonical_depth_reason
                or "PONG_CANONICAL_DEPTH_NOT_READY",
            )
            return False
        if self._fhb is not None and not self._fhb.entry_ready:
            self._last_microstructure_at = at
            self._microstructure_blockers = ("FHB_REWARM_REQUIRED",)
            return False
        self._microstructure_evaluations += 1
        if self._compute_pool is None:
            self._last_microstructure_at = at
        cutoff = at - timedelta(seconds=5)
        # Protected mode deliberately evaluates every canonical market
        # update.  When one required input family has never been observed,
        # materializing the other rolling windows cannot change the exact
        # fail-closed result.  In particular, scanning a growing BID history
        # for every callback while ASK is empty creates avoidable receiver
        # backpressure without performing an analysis.  Preserve the
        # evaluation count and blocker, but prove incompleteness in O(1).
        if (
            not self._trades
            or not self._depth[BookSide.BID]
            or not self._depth[BookSide.ASK]
        ):
            blockers = ("PONG_MICROSTRUCTURE_NATIVE_WINDOW_INCOMPLETE",)
            if self._compute_pool is None:
                self._microstructure_blockers = blockers
            else:
                self._queue_compute(None, at, blockers)
            return False
        # Flow's frozen window is event-time based. A delayed receipt must not
        # pull an older trade into it; all trades remain in durable retention.
        trades = tuple(item for item in self._trades
                       if item.event._ordering_key[1] >= cutoff
                       and item.event._ordering_key[0] >= cutoff)
        bids = self._depth[BookSide.BID].price_window(cutoff)
        asks = self._depth[BookSide.ASK].price_window(cutoff)
        if not trades or not bids or not asks:
            blockers = ("PONG_MICROSTRUCTURE_NATIVE_WINDOW_INCOMPLETE",)
            if self._compute_pool is None:
                self._microstructure_blockers = blockers
            else:
                self._queue_compute(None, at, blockers)
            return False
        as_of = at.isoformat().replace("+00:00", "Z")
        try:
            bucket = at.replace(minute=(at.minute // 5) * 5, second=0, microsecond=0)
            key = bucket.isoformat().replace("+00:00", "Z")
            window = VolumeWindow(
                window_id="pong-volume-" + canonical_hash({"start": key, "at": as_of})[:32],
                payload_hash=canonical_hash({"start": key, "at": as_of, "volume": str(self._volume_buckets.get(key, 0))}),
                instrument=self.topology.native_instrument,
                normalization_key=f"UTC-{bucket.hour:02d}:{bucket.minute:02d}-5M",
                window_start=key,
                window_end=as_of,
                available_at=as_of,
                volume=self._volume_buckets.get(key, Decimal("0")),
                kind=VolumeWindowKind.AS_OF,
                continuity_valid=self._input_error is None,
                feed_fresh=True,
            )
            prior = () if self._volume_history is None else self._volume_history.prior(bucket)
            if self._compute_pool is not None:
                request = PongWindowRequest(self._compute_generation, self._compute_submitted + 1,
                    self._producer or (), as_of, cutoff.isoformat().replace('+00:00', 'Z'),
                    trades, bids, asks, window, prior, self._input_error is None,
                    self._flow.policy, self._liquidity.policy, self._volume.policy, self._mapper.policy)
                self._queue_compute(request, at)
                return False
            flow = self._flow.evaluate(trades, window_start=cutoff.isoformat().replace('+00:00', 'Z'),
                                       as_of=as_of, feed_continuity_valid=self._input_error is None)
            bid = self._liquidity.evaluate(bids, as_of=as_of, feed_continuity_valid=self._input_error is None)
            ask = self._liquidity.evaluate(asks, as_of=as_of, feed_continuity_valid=self._input_error is None)
            if self._fhb is not None:
                # FHB forbids helper workers, so successful evaluation of both
                # liquidity sides proves the real serial strategy adapters
                # consumed only windows populated by _admit_depth_fhb. There
                # is no full-depth fallback while the policy is instantiated.
                self._fhb_consumer_count += 1
                self._fhb_last_consumer_source_sequence = (
                    self._stream_sequences.get("DEPTH")
                )
                self._fhb_last_consumer_at = at
            regime = self._volume.evaluate(window, prior, as_of=as_of)
            mapped = self._mapper.map_frame(
                flow, bid, ask, regime,
                first_evidence_revision=self._discernment.evidence_revision + 1,
                include_blocked_artifacts=False,
            )
            self._microstructure_blockers = mapped.blockers
            if not mapped.blockers:
                update = self._discernment.on_evidence_frame(
                    mapped.evidence, volume_regime=mapped.volume_regime,
                )
                self._apply_discernment_update(update)
            # A bucket observed only partially since process startup is not a
            # completed historical reference. Only the verified provider loader
            # supplies prior windows; no zero-fill or shortened history fallback.
            return True
        except Exception as error:
            blockers = (
                "PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:" + type(error).__name__,
            )
            if self._compute_pool is None:
                self._microstructure_blockers = blockers
            elif self._input_error is None:
                self._queue_compute(None, at, blockers)
            return False

    def _queue_compute(self, request: PongWindowRequest | None, at: datetime,
                       blockers: tuple[str, ...] = ()) -> None:
        if len(self._compute_pending) >= self._compute_limit:
            self._compute_refused += 1
            self._input_error = 'PONG_COMPUTE_QUEUE_OVERFLOW'
            raise RuntimeError(self._input_error)
        ordinal = self._compute_submitted + 1
        as_of = at.isoformat().replace('+00:00', 'Z')
        producer = self._producer or ()
        if request is None:
            future = Future()
            future.set_result(PongWindowResult(self._compute_generation, ordinal, producer,
                as_of, COMPUTE_BUILD_HASH, blockers, None))
        else:
            future = Future()
            if not self._compute_batch:
                self._compute_batch_started = time.monotonic()
            self._compute_batch.append((request, future))
        self._compute_pending.append((ordinal, as_of, producer, future, time.monotonic()))
        self._compute_submitted = ordinal
        if len(self._compute_batch) >= self._compute_batch_limit:
            self._flush_compute_batch()

    def _flush_compute_batch(self) -> None:
        if not self._compute_batch:
            return
        batch = tuple(self._compute_batch)
        self._compute_batch.clear()
        try:
            future = self._compute_pool.submit(evaluate_window_batch, tuple(item[0] for item in batch))
        except Exception as error:
            self._input_error = 'PONG_COMPUTE_SUBMISSION_FAILED'
            for _, pending in batch:
                pending.set_exception(error)
            self._compute_wakeup.set()
            return

        def complete(done: Future) -> None:
            try:
                values = done.result()
                if type(values) is not tuple or len(values) != len(batch):
                    raise RuntimeError('PONG_COMPUTE_BATCH_RESULT_SHAPE_INVALID')
                for (_, pending), value in zip(batch, values):
                    pending.set_result(value)
            except Exception as error:
                for _, pending in batch:
                    if not pending.done():
                        pending.set_exception(error)
            finally:
                self._compute_wakeup.set()
        future.add_done_callback(complete)

    def _drain_compute(self) -> None:
        # A completed B cannot overtake paused A. Only this host thread applies
        # results, assigns Discernment revisions and queues execution directives.
        with self._lock:
            while self._compute_pending:
                ordinal, as_of, producer, future, admitted = self._compute_pending[0]
                if not future.done():
                    if time.monotonic() - admitted > 15:
                        self._input_error = 'PONG_COMPUTE_RESPONSE_TIMEOUT'
                        self._microstructure_blockers = (self._input_error,)
                    return
                self._compute_pending.popleft()
                try:
                    result = future.result()  # completed, never blocks the host
                    if (type(result) is not PongWindowResult or result.ordinal != ordinal
                            or result.generation != self._compute_generation
                            or result.producer != producer or producer != (self._producer or ())
                            or result.as_of != as_of or result.build_hash != COMPUTE_BUILD_HASH):
                        self._input_error = 'PONG_COMPUTE_IDENTITY_OR_ORDER_MISMATCH'
                        raise RuntimeError(self._input_error)
                    if self._input_error is not None:
                        self._compute_refused += 1
                        self._microstructure_blockers = (self._input_error,)
                        continue
                    self._last_microstructure_at = _time(as_of)
                    self._microstructure_blockers = result.blockers
                    if result.frame is not None:
                        mapped = self._mapper.map_frame(*result.frame,
                            first_evidence_revision=self._discernment.evidence_revision + 1,
                            include_blocked_artifacts=False)
                        self._microstructure_blockers = mapped.blockers
                        if not mapped.blockers:
                            update = self._discernment.on_evidence_frame(mapped.evidence,
                                volume_regime=mapped.volume_regime)
                            self._apply_discernment_update(update)
                    elif not result.blockers:
                        raise RuntimeError('PONG_COMPUTE_RESULT_MISSING')
                    self._compute_applied += 1
                except Exception as error:
                    self._compute_refused += 1
                    self._microstructure_blockers = ('PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:' + type(error).__name__,)
                    if self._input_error is None:
                        self._input_error = 'PONG_COMPUTE_WORKER_FAILED:' + type(error).__name__

    def _apply_discernment_update(self, update: object) -> None:
        self._update_protection_accounting_locked()
        bridge = self._bridge
        if bridge is None:
            return
        snapshot = getattr(update, "snapshot", None)
        switch = getattr(update, "switch", None)
        if snapshot is None:
            return
        if bridge.machine.shadow_direction is None and self._discernment.latched_direction is not None:
            applied = bridge.apply_transition(bridge.machine.observe_startup_seed(
                self._discernment.latched_direction, snapshot,
            ))
        elif switch is None:
            applied = bridge.apply_transition(bridge.machine.on_snapshot_update(snapshot))
        else:
            controller = self._controller
            allowed = False
            if controller is not None:
                allowed = controller.assess_entry(
                    source_switch_id=switch.event_id,
                    source_switch_sequence=switch.sequence,
                    source_event_at=switch.available_at,
                    at=_now(),
                ).allowed
            candidate = bridge.machine.on_switch(
                switch, self._daily_bias, entry_gates_pass=allowed,
            )
            entry_actions = tuple(
                action for action in candidate.actions
                if action.kind in {
                    PongActionKind.REQUEST_ENTRY_LONG,
                    PongActionKind.REQUEST_ENTRY_SHORT,
                }
            )
            if entry_actions:
                # The scheduler changes to the protected event path before the
                # pending entry is revalidated, persisted, and exposed to the
                # native dispatcher.  A timer callback never owns this state.
                self._last_protection_transition = "ENTRY_SUBMISSION_PENDING"
                action = entry_actions[0]
                now = _now()
                source_fresh = (
                    _time(now) - _time(switch.available_at)
                    <= timedelta(seconds=5)
                )
                admission = bridge.machine.on_entry_admission(
                    action_id=action.action_id,
                    bias=self._daily_bias,
                    at=now,
                    current_snapshot=switch.snapshot,
                    current_switch_cursor=switch,
                    source_fresh=source_fresh,
                    entry_gates_pass=(allowed and not self._blockers_locked()),
                    reconciliation=self._reconciliation(self._runtime),
                    available_loss_allowance_dollars=Decimal(str(
                        self._runtime.get("available_loss_allowance_dollars") or "0"
                    )),
                )
                applied = bridge.apply_transition(admission)
            else:
                applied = bridge.apply_transition(candidate)
        self._queue_bridge_update(applied, snapshot=snapshot, switch=switch)
        self._update_protection_accounting_locked()

    def _queue_bridge_update(
        self,
        update: PongBridgeUpdate,
        *,
        snapshot: object | None = None,
        switch: object | None = None,
    ) -> None:
        for directive in update.directives:
            action = None
            if self._bridge is not None:
                action = self._bridge.machine.pending_action
                if action is not None and action.action_id != directive.action_id:
                    action = None
            evidence: dict[str, object] = {
                "source_available_at": getattr(snapshot, "available_at", _now()),
                "snapshot_hash": getattr(snapshot, "result_hash", None),
                "daily_bias_result_hash": self._daily_bias.result_hash,
                "position_side_support": (
                    None if action is None else action.position_side_support
                ),
                "source_switch_id": directive.source_switch_id,
                "source_switch_sequence": directive.source_switch_sequence,
            }
            # Non-entry directives do not consume directional support, but a
            # fixed neutral value keeps the dispatch envelope canonical.
            if evidence["position_side_support"] is None:
                evidence["position_side_support"] = "0.5"
            self._pending_native.append((directive, evidence))
        self._update_protection_accounting_locked()

    def _dispatch_native(
        self,
        directive: PongNativeDirective,
        evidence: Mapping[str, object],
    ) -> None:
        with self._lock:
            bridge = self._bridge
            if bridge is None:
                return
            # Persist DISPATCHED before crossing into the runtime.  A crash
            # after this point restores as reconciliation-required instead of
            # replaying an ambiguously sent command.
            bridge.mark_dispatched(directive.directive_id)
            self._active_directive_ids[directive.kind] = directive.directive_id
        result = self._native_dispatch(directive, evidence)
        if result.get("accepted") is not True:
            raise RuntimeError("PONG_NATIVE_RUNTIME_COMMAND_REFUSED")
        command_id = result.get("command_id")
        if isinstance(command_id, str) and command_id:
            with self._lock:
                self._directive_command_ids[directive.directive_id] = command_id
                self._command_directive_ids[command_id] = directive.directive_id

    def _event_proof_locked(self, message: Mapping[str, object]) -> PongNativeEventProof:
        runtime = self._runtime
        observed_at = str(
            message.get("timestamp")
            or message.get("observed_at")
            or runtime.get("observed_at")
            or _now()
        )
        receipt_id = str(message.get("receipt_id") or "").strip()
        if not receipt_id:
            receipt_id = "pong-native-receipt-" + canonical_hash(dict(message))
        return PongNativeEventProof(
            receipt_id=receipt_id,
            observed_at=observed_at,
            source_fingerprint=str(runtime.get("addon_source_fingerprint") or ""),
            build_fingerprint=str(runtime.get("addon_build_fingerprint") or ""),
            execution_session_id=str(runtime.get("execution_session_id") or ""),
            signature_verified=True,
            account_name=self.topology.account_name,
            account_class=self.topology.account_class,
            instrument=self.topology.native_instrument,
            canonical_contract=self.topology.canonical_contract,
            live_authority=self.topology.live_authority,
        )

    def _directive_for_message_locked(
        self,
        message: Mapping[str, object],
    ) -> str | None:
        command_id = message.get("command_id")
        if isinstance(command_id, str) and command_id in self._command_directive_ids:
            return self._command_directive_ids[command_id]
        role = str(message.get("order_role") or "").upper()
        state = str(message.get("order_state") or "").upper()
        candidates: tuple[PongNativeDirectiveKind, ...]
        if role == "ENTRY":
            candidates = (
                PongNativeDirectiveKind.ENTER_LONG,
                PongNativeDirectiveKind.ENTER_SHORT,
                PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
            )
        elif role == "EXIT":
            candidates = (
                PongNativeDirectiveKind.EXIT,
                PongNativeDirectiveKind.EMERGENCY_FLATTEN,
            )
        elif role == "PROTECTIVE" and state in {"CANCELLED", "CANCELED"}:
            candidates = (PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,)
        elif role == "PROTECTIVE":
            candidates = (PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,)
        else:
            candidates = ()
        return next(
            (
                self._active_directive_ids[kind]
                for kind in candidates
                if kind in self._active_directive_ids
            ),
            None,
        )

    def on_execution_message(self, message: Mapping[str, object]) -> None:
        """Advance the durable bridge from runtime-accepted native evidence."""
        if not isinstance(message, Mapping):
            return
        with self._lock:
            bridge = self._bridge
            if bridge is None:
                return
            self._update_protection_accounting_locked()
            try:
                message_type = str(message.get("message_type") or "").upper()
                role = str(message.get("order_role") or "").upper()
                state = str(message.get("order_state") or "").upper()
                directive_id = self._directive_for_message_locked(message)
                proof = self._event_proof_locked(message)
                update: PongBridgeUpdate | None = None
                if directive_id is not None and message_type == "COMMAND_ACK":
                    update = bridge.on_command_ack(directive_id, proof)
                elif directive_id is not None and message_type == "COMMAND_REJECTED":
                    update = bridge.on_command_rejected(
                        directive_id, proof,
                        reason=str(message.get("reason_code") or "UNKNOWN"),
                    )
                elif directive_id is not None and message_type == "EXECUTION_EVENT":
                    quantity = abs(int(message.get("quantity") or 0))
                    fill_id = str(
                        message.get("execution_id")
                        or message.get("fill_id")
                        or message.get("receipt_id")
                        or ""
                    )
                    if role == "ENTRY":
                        update = bridge.on_entry_fill(
                            directive_id, proof, quantity=quantity, fill_id=fill_id,
                        )
                    elif role in {"EXIT", "PROTECTIVE"}:
                        update = bridge.on_reducing_fill(
                            directive_id, proof, quantity=quantity, fill_id=fill_id,
                        )
                elif (
                    directive_id is not None
                    and message_type == "ORDER_EVENT"
                    and role == "PROTECTIVE"
                    and state in {"ACCEPTED", "WORKING"}
                ):
                    update = bridge.on_protection_working(
                        directive_id, proof,
                        quantity=abs(int(message.get("quantity") or 1)),
                    )
                elif (
                    directive_id is not None
                    and message_type == "ORDER_EVENT"
                    and role == "PROTECTIVE"
                    and state in {"CANCELLED", "CANCELED"}
                ):
                    update = bridge.on_protective_cancelled(directive_id, proof)
                elif (
                    message_type == "RECONCILIATION"
                    and int(message.get("position_quantity") or 0) == 0
                    and int(message.get("working_order_count") or 0) == 0
                    and bridge.machine.state.value in {"EXIT_PENDING", "SAFETY_BLOCKED"}
                ):
                    runtime = dict(self._runtime_snapshot())
                    self._runtime = runtime
                    update = bridge.on_signed_flat_reconciliation(
                        self._reconciliation(runtime), proof,
                        at=str(runtime["observed_at"]),
                    )
                if update is not None:
                    self._queue_bridge_update(update)
            except Exception as error:
                self._bridge_error = (
                    "PONG_NATIVE_CALLBACK_BRIDGE_FAILED:" + type(error).__name__
                )
            finally:
                self._update_protection_accounting_locked()

    @staticmethod
    def _bias_from_inputs(
        previous: CompletedPongHour,
        current: CompletedPongHour,
        trades: tuple[ClassifiedTradeInput, ...],
        *,
        published_at: str,
        instrument: str = SEP26_TOPOLOGY.native_instrument,
    ) -> DailyBiasResult:
        structural = hourly_structure_components(
            previous, current, available_at=published_at,
        )
        flow = HourlyOrderFlowEvaluator().evaluate(CompletedHourlyOrderFlow(
            current.start, current.end, published_at, current.start, current.end,
            True, trades,
        ))
        options = unavailable_nonvoting_options(
            start=current.start, end=current.end, available_at=published_at,
        )
        frame = HourlyBiasFrame(
            instrument, current.start, current.end, published_at,
            (*structural, flow), options, True, None,
        )
        return DailyBiasEngine().update(
            frame, BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        ).result

    def _bias_from_certificate(self, value: Mapping[str, object]) -> DailyBiasResult:
        raw_hours = value.get("hour_pair")
        raw_trades = value.get("classified_trades")
        if not isinstance(raw_hours, list) or len(raw_hours) != 2 or not isinstance(raw_trades, list):
            raise ValueError("certificate bias inputs")
        hours = tuple(CompletedPongHour(
            start=str(item["start"]), end=str(item["end"]),
            available_at=str(item["available_at"]), open=Decimal(str(item["open"])),
            high=Decimal(str(item["high"])), low=Decimal(str(item["low"])),
            close=Decimal(str(item["close"])), source=str(item["source"]),
            continuity_domain=str(item["continuity_domain"]),
            source_ids=tuple(str(x) for x in item["source_ids"]),
            source_hashes=tuple(str(x) for x in item["source_hashes"]),
            complete=bool(item["complete"]), instrument=str(item["instrument"]),
        ) for item in raw_hours)
        trades = tuple(ClassifiedTradeInput(
            event_id=str(item["event_id"]), payload_hash=str(item["payload_hash"]),
            source=str(item["source"]), instrument=str(item["instrument"]),
            side=ClassifiedSide(str(item["side"])), size=Decimal(str(item["size"])),
            event_at=str(item["event_at"]), observed_at=str(item["observed_at"]),
            available_at=str(item["available_at"]), authenticated=bool(item["authenticated"]),
        ) for item in raw_trades)
        result = self._bias_from_inputs(
            hours[0], hours[1], trades,
            published_at=str(value["published_at"]),
            instrument=self.topology.native_instrument,
        )
        if result.result_hash != value.get("daily_bias_result_hash"):
            raise ValueError("certificate daily bias hash")
        return result

    def _maybe_certify_hour(self) -> None:
        if self._certificate is not None or self._certificate_error is not None:
            return
        durability = self._collection.get("durability")
        if (
            self._input_error is not None
            or self._hour_pair is None
            or not isinstance(durability, Mapping)
        ):
            return
        # status() advances required_interval to the new, incomplete hour.
        # Consume the exact completed receipt, without stopping collection.
        candidates = [self._collection.get("required_interval"), *reversed(
            self._collection.get("recent_completed_intervals") or [],
        )]
        current = self._hour_pair[1]
        required = next((item for item in candidates if (
            isinstance(item, Mapping) and item.get("eligible") is True
            and item.get("complete") is True and item.get("blockers") in ([], ())
            and item.get("start_utc") == current.start
            and item.get("end_utc") == current.end
        )), None)
        if required is None:
            return
        counts = [durability.get(key) for key in (
            "enqueued_records", "written_records", "durable_records",
        )]
        if (any(type(value) is not int or value <= 0 for value in counts)
                or len(set(counts)) != 1
                or any(durability.get(key) != 0 for key in (
                    "queue_overflow_records", "rejected_records", "duplicate_records",
                ))
                or not durability.get("last_fsync_at")
                or _time(durability["last_fsync_at"]) < _time(current.available_at)):
            return
        published_at = self._hour_pair[1].available_at
        try:
            trades = tuple(item for item in self._classified_hour
                           if _time(current.start) <= _time(item.event_at) < _time(current.end))
            bias = self._bias_from_inputs(
                self._hour_pair[0], self._hour_pair[1],
                trades, published_at=published_at,
                instrument=self.topology.native_instrument,
            )
            if not bias.configured:
                return
            body: dict[str, object] = {
                "schema": PONG_QUALIFYING_HOUR_SCHEMA,
                "created_at": _now(),
                "test_only": False,
                "eligible": True,
                "collector_id": (self._collection.get("collector") or {}).get("collector_id"),
                "producer": self._collection.get("producer"),
                "required_interval": dict(required),
                "journal_path": durability.get("journal_path"),
                "journal_tip_sha256": durability.get("journal_tip_sha256"),
                "hour_pair": [item.payload() for item in self._hour_pair],
                "classified_trades": [
                    {
                        **asdict(item), "side": item.side.value,
                        "size": str(item.size),
                    }
                    for item in trades
                ],
                "published_at": published_at,
                "daily_bias": bias.payload(),
                "daily_bias_result_hash": bias.result_hash,
                "qualifying_identity": self._qualifying_certificate_identity(),
            }
            body["certificate_hash"] = canonical_hash(body)
            _atomic_write(self.certificate_path, body)
            self._certificate = body
            self._daily_bias = bias
        except Exception as error:
            self._certificate_error = "PONG_QUALIFYING_HOUR_CERTIFICATION_FAILED:" + type(error).__name__

    def _reconciliation(self, runtime: Mapping[str, object]) -> PongReconciliationProof:
        observed_at = str(runtime["observed_at"])
        return PongReconciliationProof.create(
            source_id="l3g-paper-addon-provenance-v1",
            source_fingerprint=str(runtime["addon_source_fingerprint"]),
            build_fingerprint=str(runtime["addon_build_fingerprint"]),
            source_hash=canonical_hash(dict(runtime)),
            observed_at=observed_at,
            available_at=observed_at,
            position_quantity=int(runtime["position_quantity"]),
            working_owned_orders=int(runtime["working_owned_orders"]),
            unresolved_execution=bool(runtime["unresolved_execution"]),
            account_name=self.topology.account_name,
            account_class=self.topology.account_class,
            instrument=self.topology.native_instrument,
            canonical_contract=self.topology.canonical_contract,
            topology_version=self.topology.version,
            topology_hash=self.topology.binding_hash,
        )

    def _maybe_create_bridge(self) -> None:
        if self._bridge is not None or self._bridge_error is not None:
            return
        runtime = self._runtime
        if (
            self.selected_profile != PONG_ENTRY_PROFILE_VERSION
            or self._certificate is None
            or not self._native_input_ready_locked()
            or runtime.get("addon_provenance_valid") is not True
            or not isinstance(runtime.get("execution_session_id"), str)
        ):
            return
        try:
            identity = PongLoadedRuntimeIdentity(
                execution_session_id=str(runtime["execution_session_id"]),
                addon_protocol_version=str(runtime["addon_protocol_version"]),
                addon_source_fingerprint=str(runtime["addon_source_fingerprint"]),
                addon_build_fingerprint=str(runtime["addon_build_fingerprint"]),
                verified_at=str(runtime["observed_at"]),
                attestation_verified=True,
                instrument=self.topology.native_instrument,
                canonical_contract=self.topology.canonical_contract,
                topology_version=self.topology.version,
                topology_hash=self.topology.binding_hash,
            )
            policy = PongQuantityPolicy(
                instrument=self.topology.native_instrument,
                canonical_contract=self.topology.canonical_contract,
                native_maximum_quantity=1,
                native_capability_status="CURRENT_AUTHENTICATED_ONE_MNQ_CAPABILITY",
                reconciliation_source_fingerprint=identity.addon_source_fingerprint,
                reconciliation_build_fingerprint=identity.addon_build_fingerprint,
                topology_version=self.topology.version,
                topology_hash=self.topology.binding_hash,
            )
            machine = PongStateMachine(
                quantity_policy=policy,
                discernment_policy=self._discernment.policy,
            )
            machine.on_bias_update(self._daily_bias)
            bridge_path = self._operation_root / "bridges" / (
                identity.execution_session_id + ".json"
            )
            if bridge_path.exists():
                self._bridge = PongRuntimeBridge.restore(
                    bridge_path,
                    current_snapshot=None,
                    current_bias=self._daily_bias,
                    reconciliation=self._reconciliation(runtime),
                    current_runtime_identity=identity,
                    quantity_policy=policy,
                    at=str(runtime["observed_at"]),
                )
            else:
                self._bridge = PongRuntimeBridge.create(
                    machine, bridge_path, runtime_identity=identity,
                )
            self._update_protection_accounting_locked()
        except Exception as error:
            self._bridge_error = "PONG_RUNTIME_BRIDGE_START_FAILED:" + type(error).__name__

    def _native_input_ready_locked(self) -> bool:
        if (
            self._certificate is None
            or (self._warm_start is None and not self._retained_hour_seed)
            or self._input_error is not None
        ):
            return False
        producer = self._certificate.get("producer")
        if not isinstance(producer, Mapping) or self._producer is None:
            return False
        # A supported observer replacement creates a new producer session and
        # loaded assembly identity.  The retained hour remains historical
        # evidence; current microstructure starts a fresh continuity domain.
        # Source semantics must remain exact, while current market/execution
        # build congruency is checked separately below.
        runtime = self._runtime
        if self.topology.version != SEP26_TOPOLOGY.version:
            bundle = {
                "topology_version": runtime.get("topology_version"),
                "topology_hash": runtime.get("topology_hash"),
                "native_instrument": runtime.get("instrument"),
                "canonical_contract": runtime.get("canonical_contract"),
                "state_namespace": runtime.get("state_namespace"),
                "account_name": runtime.get("account_name"),
                "account_class": runtime.get("account_class"),
                "live_authority": runtime.get("live_authority"),
                "live_capital": runtime.get("live_capital"),
                "maximum_quantity": runtime.get("maximum_quantity"),
            }
            try:
                validate_topology_bundle(self.topology, bundle)
            except ValueError:
                return False
        if (
            runtime.get("instrument") not in {None, self.topology.native_instrument}
            or runtime.get("canonical_contract") not in {None, self.topology.canonical_contract}
            or runtime.get("topology_version") not in {None, self.topology.version}
            or runtime.get("topology_hash") not in {None, self.topology.binding_hash}
        ):
            return False
        return self._producer[1] == str(
            producer.get("source_fingerprint"),
        )

    def _fhb_heartbeat_blocker_locked(self) -> str | None:
        if self._fhb is None:
            return None
        if self._last_depth_callback_monotonic is None:
            return "FHB_WATCHDOG_WAITING_FOR_OBSERVATION"
        if (
            self._monotonic_clock() - self._last_depth_callback_monotonic
            > self._fhb.config.source_heartbeat_timeout_seconds
        ):
            return "FHB_SOURCE_HEARTBEAT_TIMEOUT"
        return None

    def _qualifying_heartbeat_blocker_locked(self) -> str | None:
        if self._qualifying_architecture is None:
            return None
        if self._last_depth_callback_monotonic is None:
            return "PONG_WATCHDOG_WAITING_FOR_OBSERVATION"
        if (
            self._monotonic_clock() - self._last_depth_callback_monotonic
            > PONG_QUALIFYING_SOURCE_HEARTBEAT_SECONDS
        ):
            return "PONG_SOURCE_HEARTBEAT_TIMEOUT"
        return None

    def _blockers_locked(self) -> list[str]:
        blockers: list[str] = []
        if self._stop.is_set():
            blockers.append('PONG_HOST_STOPPING')
        runtime = self._runtime
        if self.selected_profile != PONG_ENTRY_PROFILE_VERSION:
            blockers.append("PONG_PROFILE_NOT_SELECTED")
        if not self.activation_authorized:
            blockers.append("PONG_BOUNDED_ACTIVATION_NOT_AUTHORIZED")
        if self._certificate is None:
            blockers.append(self._certificate_error or "PONG_AUTHENTIC_HOUR_NOT_QUALIFIED")
        elif (self._daily_bias.expires_at is None
              or _time(_now()) >= _time(self._daily_bias.expires_at)):
            blockers.append("PONG_QUALIFIED_HOUR_EXPIRED")
        if not self._native_input_ready_locked():
            blockers.append(self._input_error or "PONG_NATIVE_INPUT_CONTRACT_NOT_READY")
        blockers.extend(self._microstructure_blockers)
        if (self._last_microstructure_at is not None
                and _time(_now()) - self._last_microstructure_at >= timedelta(seconds=5)):
            blockers.append("PONG_MICROSTRUCTURE_INPUT_STALE")
        if self._bridge is None or self._bridge_error is not None:
            blockers.append(self._bridge_error or "PONG_NATIVE_RUNTIME_BRIDGE_NOT_READY")
        if runtime.get("runtime_active") is not True:
            blockers.append("PONG_RUNTIME_NOT_ACTIVE")
        if runtime.get("addon_provenance_valid") is not True:
            blockers.append("PONG_EXECUTION_ADDON_PROVENANCE_INVALID")
        if (
            self._producer is not None
            and runtime.get("addon_build_fingerprint") != self._producer[2]
        ):
            blockers.append("PONG_MARKET_EXECUTION_BUILD_MISMATCH")
        if runtime.get("reconciliation_current") is not True:
            blockers.append("PONG_RECONCILIATION_NOT_CURRENT")
        if runtime.get("unresolved_execution") is True:
            blockers.append("PONG_UNRESOLVED_EXECUTION")
        if runtime.get("safety_lockout") is True:
            blockers.append("PONG_SAFETY_LOCKOUT_RETAINED")
        if runtime.get("entries_paused") is True:
            blockers.append("PONG_RUNTIME_ENTRIES_PAUSED")
        if int(runtime.get("maximum_quantity") or 0) != 1:
            blockers.append("PONG_NATIVE_QUANTITY_CAPABILITY_INVALID")
        if self._fhb is not None:
            heartbeat_blocker = self._fhb_heartbeat_blocker_locked()
            if heartbeat_blocker is not None:
                blockers.append(heartbeat_blocker)
            if self._fhb_guard_blocker is not None:
                blockers.append(self._fhb_guard_blocker)
        if self._qualifying_architecture is not None:
            heartbeat_blocker = self._qualifying_heartbeat_blocker_locked()
            if heartbeat_blocker is not None:
                blockers.append(heartbeat_blocker)
            if self._fhb_guard_blocker is not None:
                blockers.append(self._fhb_guard_blocker)
        return list(dict.fromkeys(blockers))

    def authority_status(self) -> dict[str, object]:
        with self._lock:
            blockers = self._blockers_locked()
            now = _time(_now())
            controller = self._controller
            if self._controller_error is not None:
                blockers.append(self._controller_error)
            if controller is not None:
                if now > _time(controller.state.heartbeat_valid_until):
                    blockers.append("PONG_CONTROLLER_HEARTBEAT_EXPIRED")
                if (controller.state.execution_deadline_utc is not None
                        and now >= _time(controller.state.execution_deadline_utc)):
                    blockers.append("PONG_BOUNDED_RUN_DEADLINE_REACHED")
                if (controller.state.phase is PongControllerPhase.PREPARED
                        and now.astimezone(ZoneInfo("America/Denver")).date()
                        != self._activation_date):
                    blockers.append("PONG_CONTROLLER_ACTIVATION_DATE_REFUSED")
            activation_blockers = [
                value for value in blockers
                if value not in {
                    "PONG_BOUNDED_STOP_UNVERIFIED",
                    "PONG_CONTROLLER_ENTRY_FENCE_ACTIVE",
                }
            ]
            phase = None if self._controller is None else self._controller.state.phase
            candidate = not activation_blockers
            ready = candidate and phase in _RUNNING_CONTROLLER_PHASES
            start_blockers = [
                value for value in blockers
                if value != "PONG_RUNTIME_NOT_ACTIVE"
            ]
            return {
                "schema": PONG_PRODUCTION_HOST_SCHEMA,
                "native_input_contract": "READY" if self._native_input_ready_locked() else "BLOCKED",
                "native_runtime_bridge": (
                    "READY"
                    if self._bridge is not None and self._bridge_error is None
                    else "BLOCKED"
                ),
                "bounded_controller": "READY" if ready else "BLOCKED",
                "activation_candidate": candidate,
                "operational_start_ready": (
                    not start_blockers
                    and phase is PongControllerPhase.PREPARED
                ),
                "entry_authority_ready": ready,
                "blocking_reasons": blockers,
            }

    def _controller_observation_locked(self) -> PongControllerObservation:
        runtime = self._runtime
        authority = self.authority_status()
        return PongControllerObservation(
            observed_at=str(runtime.get("observed_at") or _now()),
            entry_fence_active=bool(runtime.get("entry_fence_active", True)),
            position_quantity=int(runtime.get("position_quantity") or 0),
            working_owned_orders=int(runtime.get("working_owned_orders") or 0),
            working_entry_orders=int(runtime.get("working_entry_orders") or 0),
            unresolved_execution=bool(runtime.get("unresolved_execution", True)),
            reconciliation_current=bool(runtime.get("reconciliation_current", False)),
            protection_current=bool(runtime.get("protection_current", False)),
            entry_gates_healthy=bool(authority["activation_candidate"]),
            safety_lockout=bool(runtime.get("safety_lockout", True)),
            runtime_active=bool(runtime.get("runtime_active", False)),
            evidence_sealed=bool(runtime.get("evidence_sealed", False)),
            signature_verified=bool(runtime.get("addon_provenance_valid", False)),
            instrument=self.topology.native_instrument,
            canonical_contract=self.topology.canonical_contract,
        )

    def _open_controller(self) -> None:
        if self._controller is not None:
            return
        try:
            self._controller = PongBoundedController.open(
                self.controller_path,
                experiment_id=self._experiment_id,
                at=_now(),
                process_instance_id=self._process_instance_id,
                activation_date_local=self._activation_date,
                ny_open_blackout=(
                    True if self.launch_authorization is None
                    else self.launch_authorization.ny_open_blackout
                ),
            )
            self._controller_error = None
        except Exception as error:
            self._controller_error = "PONG_CONTROLLER_HOST_START_FAILED:" + type(error).__name__

    def _dispatch_controller(self, command: PongControllerCommand) -> None:
        controller = self._controller
        if controller is None:
            return
        controller.mark_dispatched(command.command_id, at=_now())
        accepted = False
        reason = ""
        try:
            accepted = self._controller_dispatch(command.kind.value, command.reason)
            if not accepted:
                reason = "PONG_CONTROLLER_RUNTIME_COMMAND_REFUSED"
        except Exception as error:
            reason = "PONG_CONTROLLER_DISPATCH_FAILED:" + type(error).__name__
        controller.acknowledge_command(
            command.command_id, at=_now(), accepted=accepted, reason=reason,
        )

    def _run(self) -> None:
        next_control = 0.0
        while not self._stop.is_set() or not self._observations.empty() or self._compute_pending:
            service_started_ns = time.perf_counter_ns()
            try:
                if self._compute_batch and (self._stop.is_set() or self._observations.empty()
                        or time.monotonic() - self._compute_batch_started >= 0.01):
                    self._flush_compute_batch()
                self._drain_compute()
                try:
                    if len(self._compute_pending) >= self._compute_limit:
                        self._compute_wakeup.wait(0.005)
                        self._compute_wakeup.clear()
                        queued = None  # bounded backpressure; control ticks continue
                    else:
                        timeout = 0.005 if self._compute_pending else 0.5
                        periodic_wait = self._periodic_wait_seconds()
                        if periodic_wait is not None:
                            timeout = min(timeout, periodic_wait)
                        queued = self._observations.get(timeout=timeout)
                        with self._admission_lock:
                            if self._observation_queue_occupancy <= 0:
                                raise RuntimeError(
                                    "PONG_OBSERVATION_QUEUE_ACCOUNTING_UNDERFLOW"
                                )
                            self._observation_queue_occupancy -= 1
                except queue.Empty:
                    queued = None
                if queued is not None:
                    record, admitted_monotonic = queued
                    processing_started_ns = time.perf_counter_ns()
                    try:
                        self._consume_observation(
                            record,
                            admitted_monotonic=admitted_monotonic,
                        )
                    finally:
                        self._observations.task_done()
                        processing_elapsed_ns = max(
                            0,
                            time.perf_counter_ns() - processing_started_ns,
                        )
                        with self._lock:
                            self._observation_processing_count += 1
                            self._observation_processing_total_ns += (
                                processing_elapsed_ns
                            )
                            self._observation_processing_maximum_ns = max(
                                self._observation_processing_maximum_ns,
                                processing_elapsed_ns,
                            )
                # A queued market record is applied first when it was already
                # available at the same QPC deadline.  Otherwise the timer
                # uses only the admitted prefix and never invents a backlog
                # completion.  Protected states supersede, rather than delay,
                # obsolete flat-mode opportunities.
                self._service_periodic_deadline()
                # Native evidence remains event-driven. Snapshot copying and
                # durable controller heartbeats are control-plane work, not one
                # fsync per quote/trade. Pending directives still dispatch now.
                control_due = time.monotonic() >= next_control
                if not control_due:
                    while True:
                        with self._lock:
                            if not self._pending_native:
                                break
                            directive, evidence = self._pending_native.popleft()
                            self._update_protection_accounting_locked()
                        self._dispatch_native(directive, evidence)
                    continue
                next_control = time.monotonic() + 0.5
                runtime = dict(self._runtime_snapshot())
                collection = dict(self._collector_status())
                prepare_activation = False
                command: PongControllerCommand | None = None
                with self._lock:
                    self._update_protection_accounting_locked()
                    self._runtime = runtime
                    self._collection = collection
                    self._observe_periodic_durability_locked()
                    self._update_protection_accounting_locked()
                    self._persist_host_checkpoint_locked(self._monotonic_clock())
                    self._open_controller()
                    self._maybe_certify_hour()
                    self._maybe_create_bridge()
                    controller = self._controller
                    if controller is None:
                        continue
                    observation = self._controller_observation_locked()
                    authority = self.authority_status()
                    if (
                        controller.state.phase is PongControllerPhase.PREPARED
                        and not self._stop.is_set()
                        and self.activation_authorized
                        and authority["activation_candidate"] is True
                    ):
                        prepare_activation = True
                    else:
                        command = controller.tick(
                            observation, at=observation.observed_at,
                        )
                if prepare_activation:
                    if not self._controller_dispatch(
                        "PREPARE_ACTIVATION", "PONG_CONTROLLER_ACTIVATION",
                    ):
                        raise RuntimeError("PONG_CONTROLLER_ACTIVATION_PREPARATION_REFUSED")
                    refreshed = dict(self._runtime_snapshot())
                    with self._lock:
                        self._update_protection_accounting_locked()
                        self._runtime = refreshed
                        self._update_protection_accounting_locked()
                        controller = self._controller
                        if controller is None or self._stop.is_set():
                            continue
                        observation = self._controller_observation_locked()
                        controller.activate(observation, at=observation.observed_at)
                        command = controller.tick(
                            observation, at=observation.observed_at,
                        )
                while True:
                    with self._lock:
                        if not self._pending_native:
                            break
                        directive, evidence = self._pending_native.popleft()
                        self._update_protection_accounting_locked()
                    self._dispatch_native(directive, evidence)
                if command is not None:
                    self._dispatch_controller(command)
            except Exception as error:
                with self._lock:
                    fault = "PONG_PRODUCTION_HOST_LOOP_FAILED:" + type(error).__name__
                    self._controller_error = fault
                    self._persist_first_host_fault_locked(fault)
            finally:
                service_elapsed_ns = max(
                    0,
                    time.perf_counter_ns() - service_started_ns,
                )
                with self._lock:
                    self._host_service_count += 1
                    self._host_service_total_ns += service_elapsed_ns
                    self._host_service_maximum_ns = max(
                        self._host_service_maximum_ns,
                        service_elapsed_ns,
                    )

    def _periodic_telemetry_snapshot_locked(
        self,
        *,
        captured_monotonic: float,
        observation_queue_depth: int,
        oldest_pending_queue_age_ms: float | None,
    ) -> dict[str, object]:
        self._observe_periodic_durability_locked()
        deadline = self._periodic_deadline.metrics()
        durability = self._collection.get("durability")
        durability = durability if isinstance(durability, Mapping) else {}
        enqueued = durability.get("enqueued_records")
        durable = durability.get("durable_records")
        durable_lag = (
            enqueued - durable
            if type(enqueued) is int and type(durable) is int
            else None
        )
        durability_counts_available = all(
            type(durability.get(key)) is int
            for key in ("enqueued_records", "written_records", "durable_records")
        )
        collector_transport_counters_available = all(
            type(durability.get(key)) is int
            for key in (
                "queue_overflow_records",
                "producer_queue_overflow_records",
                "producer_transport_failed_records",
            )
        )
        collector_durability_coherent = bool(
            durability_counts_available
            and enqueued == durability.get("written_records") == durable
            and durability.get("queue_depth") == 0
        )
        def collector_counter(key: str) -> int:
            value = durability.get(key)
            return value if type(value) is int and value >= 0 else 0

        producer = self._producer
        return {
            "schema": PONG_PERIODIC_TELEMETRY_SCHEMA,
            "captured_at_utc": _now(),
            "captured_monotonic_ns": max(
                0, int(captured_monotonic * 1_000_000_000),
            ),
            "identity": {
                "counter_epoch_id": self._periodic_counter_epoch_id,
                "counter_reset_ordinal": self._periodic_counter_reset_ordinal,
                "counter_reset_reason": "HOST_CONSTRUCTION",
                "host_process_instance_id": self._process_instance_id,
                "experiment_id": self._experiment_id,
                "loaded_artifact_hash": PONG_LOADED_ARTIFACT_HASH,
                "selected_profile": self.selected_profile,
                "qualifying_architecture": self._qualifying_architecture,
                "topology_hash": self.topology.binding_hash,
                "runtime_execution_session_id": self._runtime.get(
                    "execution_session_id",
                ),
                "runtime_addon_source_fingerprint": self._runtime.get(
                    "addon_source_fingerprint",
                ),
                "runtime_addon_build_fingerprint": self._runtime.get(
                    "addon_build_fingerprint",
                ),
                "source_session_id": None if producer is None else producer[0],
                "source_fingerprint": None if producer is None else producer[1],
                "source_build_fingerprint": None if producer is None else producer[2],
                "source_generation": self._source_generation,
            },
            "counters": {
                "depth_events": self._stream_counts["DEPTH"],
                "ordinary_evaluation_dispatches": self._periodic_evaluations,
                "ordinary_completed_full_analyses": (
                    self._ordinary_completed_full_analyses
                ),
                "protection_evaluation_dispatches": self._event_driven_evaluations,
                "protection_completed_full_analyses": (
                    self._protection_completed_full_analyses
                ),
                "scheduled_opportunities": deadline["scheduled_opportunities"],
                "claimed_current_opportunities": deadline[
                    "claimed_current_opportunities"
                ],
                "dispatched_opportunities": deadline["dispatched"],
                "completed_opportunities": deadline["completed"],
                "applied_opportunities": deadline["applied"],
                "late_opportunities": deadline["late_opportunities"],
                "on_time_opportunities": deadline["on_time_opportunities"],
                "obsolete_opportunities": deadline["obsolete_opportunities"],
                "protection_supersessions": deadline[
                    "protection_mode_supersessions"
                ],
                "intentional_no_work": deadline["intentional_no_work"],
                "shutdown_opportunities": deadline["shutdown_deadlines"],
            },
            "disqualifier_counters": {
                "protection_transitions": self._protection_transition_count,
                **self._periodic_protection_reason_events,
                "freshness_breaches": self._freshness_breach_count,
                "host_queue_overflows": self._observation_overflow,
                "collector_queue_overflows": collector_counter(
                    "queue_overflow_records",
                ),
                "producer_queue_overflows": collector_counter(
                    "producer_queue_overflow_records",
                ),
                "producer_transport_failures": collector_counter(
                    "producer_transport_failed_records",
                ),
                "durability_lag_events": self._periodic_durability_lag_events,
                "durability_counter_regressions": (
                    self._periodic_durability_counter_regressions
                ),
                "canonical_depth_losses": self._canonical_depth_losses,
                "stream_missing_events": sum(self._stream_missing.values()),
                "stream_out_of_order_events": sum(
                    self._stream_out_of_order.values()
                ),
            },
            "scheduler": {
                "origin_qpc_ns": deadline["origin_qpc_ns"],
                "interval_ms": deadline["interval_ms"],
                "deadline_reconciliation": deadline["deadline_reconciliation"],
                "completion_reconciliation": deadline[
                    "completion_reconciliation"
                ],
                "outstanding_claimed": deadline["outstanding_claimed"],
                "outstanding_dispatched": deadline["outstanding_dispatched"],
                "outstanding_opportunities": deadline[
                    "outstanding_opportunities"
                ],
            },
            "context": {
                "active_path": (
                    "PERIODIC_FLAT"
                    if self._event_protection_reason_locked() is None
                    else "EVENT_DRIVEN_PROTECTION"
                ),
                "protection_reason": self._event_protection_reason_locked(),
                "input_error": self._input_error,
                "canonical_depth_ready": self._canonical_depth_ready,
                "canonical_depth_reason": self._canonical_depth_reason,
                "host_observation_queue_depth": observation_queue_depth,
                "host_observation_queue_high_water": (
                    self._observation_queue_high_water
                ),
                "host_oldest_pending_queue_age_ms": (
                    oldest_pending_queue_age_ms
                ),
                "host_queue_overflow": self._observation_overflow,
                "compute_pending": len(self._compute_pending),
                "last_host_queue_age_ms": self._last_host_queue_age_ms,
                "maximum_host_queue_age_ms": self._maximum_host_queue_age_ms,
                "last_source_receipt_age_ms": self._last_source_receipt_age_ms,
                "maximum_source_receipt_age_ms": (
                    self._maximum_source_receipt_age_ms
                ),
                "freshness_blocker": self._fhb_guard_blocker,
                "collector_enqueued_records": enqueued,
                "collector_written_records": durability.get("written_records"),
                "collector_durable_records": durable,
                "collector_durable_lag_records": durable_lag,
                "durability_counts_available": durability_counts_available,
                "collector_durability_coherent": (
                    collector_durability_coherent
                ),
                "collector_transport_counters_available": (
                    collector_transport_counters_available
                ),
                "collector_queue_depth": durability.get("queue_depth"),
                "collector_queue_high_water": durability.get(
                    "queue_high_water_records",
                ),
                "collector_last_queue_age_ms": durability.get(
                    "last_queue_age_milliseconds",
                ),
                "collector_maximum_queue_age_ms": durability.get(
                    "maximum_queue_age_milliseconds",
                ),
                "collector_queue_overflow_records": durability.get(
                    "queue_overflow_records",
                ),
                "producer_queue_overflow_records": durability.get(
                    "producer_queue_overflow_records",
                ),
                "producer_transport_failed_records": durability.get(
                    "producer_transport_failed_records",
                ),
            },
            "semantics": {
                "ratio": (
                    "aligned delta DEPTH / aligned delta ordinary completed "
                    "full analyses"
                ),
                "ordinary_completion": (
                    "serial full microstructure adapters returned after all "
                    "readiness and native-window prerequisites"
                ),
                "protection_separated": True,
                "missed_opportunities": (
                    "alias of obsolete_opportunities; never an additional "
                    "additive outcome"
                ),
                "numeric_threshold": None,
                "numeric_threshold_reason": "POLICY_NOT_DEFINED",
            },
        }

    def status(self) -> dict[str, object]:
        with self._lock:
            status_monotonic = self._monotonic_clock()
            self._update_protection_accounting_locked(status_monotonic)
            observation_queue_depth, oldest_pending_queue_age_ms = (
                self._observation_queue_metrics_locked(status_monotonic)
            )
            authority = self.authority_status()
            controller = self._controller
            bridge = self._bridge
            fhb_status = None if self._fhb is None else self._fhb.status()
            fhb_heartbeat_blocker = self._fhb_heartbeat_blocker_locked()
            qualifying_heartbeat_blocker = (
                self._qualifying_heartbeat_blocker_locked()
            )
            fhb_unavailable_reason = (
                None
                if fhb_status is None
                else self._input_error
                or self._fhb_guard_blocker
                or fhb_heartbeat_blocker
                or fhb_status["unavailable_reason"]
            )
            fhb_activation = {
                "disposition": FHB_ARCHIVE_DISPOSITION,
                "qualifying_runtime_state": (
                    FHB_QUALIFYING_RUNTIME_STATE
                    if self._qualifying_architecture is not None
                    else None
                ),
                "qualification_required": False,
                "requested": self._fhb_requested_profile is not None,
                "requested_profile": self._fhb_requested_profile,
                "admitted": self._fhb is not None,
                "instantiated": self._fhb is not None,
                "enabled": self._fhb is not None,
                "effective_profile": (
                    None if self._fhb is None else self._fhb.config.profile_id
                ),
                "effective_policy": (
                    None if self._fhb is None else self._fhb.config.payload()
                ),
                "effective_policy_fingerprint": (
                    None
                    if self._fhb is None
                    else self._fhb.config.policy_fingerprint
                ),
                "ready": bool(
                    self._fhb is not None
                    and self._fhb.entry_ready
                    and self._input_error is None
                    and self._fhb_guard_blocker is None
                    and fhb_heartbeat_blocker is None
                ),
                "maintained": bool(
                    fhb_status is not None
                    and int(fhb_status["maintenance_ticks"]) > 0
                ),
                "maintenance_ticks": (
                    0 if fhb_status is None else fhb_status["maintenance_ticks"]
                ),
                "last_maintained_source_sequence": (
                    None
                    if fhb_status is None
                    else fhb_status["last_source_sequence"]
                ),
                "output_batches": self._fhb_output_batches,
                "output_observations": self._fhb_output_observations,
                "last_output_source_sequence": (
                    self._fhb_last_output_source_sequence
                ),
                "consumer_count": self._fhb_consumer_count,
                "last_consumer_source_sequence": (
                    self._fhb_last_consumer_source_sequence
                ),
                "last_consumer_at": (
                    None
                    if self._fhb_last_consumer_at is None
                    else self._fhb_last_consumer_at.isoformat().replace(
                        "+00:00", "Z",
                    )
                ),
                "consumed": self._fhb_consumer_count > 0,
                "reported": True,
                "unavailable_reason": fhb_unavailable_reason,
                "fallback": {
                    "mode": "NONE",
                    "full_depth_fallback_allowed": False,
                    "count": 0,
                },
                "execution_owner": "SERIAL_SINGLE_ORDERED_OWNER",
                "helper_enabled": False,
                "input_retention": "ALL_AUTHENTIC_SOURCE_RECORDS",
                "upstream_event_reduction": "NONE",
                "entry_evaluation_trigger": (
                    "PONG_PERIODIC_DEADLINE"
                    if self._scheduling.enabled
                    and self._event_protection_reason_locked() is None
                    else "EVENT_DRIVEN_PROTECTION"
                    if self._scheduling.enabled
                    else "FHB_ELIGIBLE_AUTHENTIC_EVENT"
                ),
                "event_trigger_reduction_claimed": False,
            }
            canonical_depth = {
                "ready": self._canonical_depth_ready,
                "required_for_qualifying_build": (
                    self._qualifying_architecture is not None
                ),
                "reason": self._canonical_depth_reason,
                "bid_levels": len(self._canonical_depth[BookSide.BID]),
                "ask_levels": len(self._canonical_depth[BookSide.ASK]),
                "best_bid": (
                    None
                    if not self._canonical_depth[BookSide.BID]
                    else str(self._canonical_depth[BookSide.BID][0][0])
                ),
                "best_ask": (
                    None
                    if not self._canonical_depth[BookSide.ASK]
                    else str(self._canonical_depth[BookSide.ASK][0][0])
                ),
                "last_source_sequence": self._canonical_depth_source_sequence,
                "last_receipt_at": (
                    None
                    if self._canonical_depth_last_receipt_at is None
                    else self._canonical_depth_last_receipt_at.isoformat().replace(
                        "+00:00", "Z",
                    )
                ),
                "recovery_count": self._canonical_depth_recoveries,
            }
            return {
                **authority,
                "state": "RUNNING" if self._thread is not None and self._thread.is_alive() else "STOPPED",
                "selected_profile": self.selected_profile,
                "qualifying_architecture": self._qualifying_architecture,
                "canonical_depth_state": canonical_depth,
                "ingress_freshness": {
                    "maximum_queue_age_milliseconds": (
                        PONG_QUALIFYING_MAXIMUM_QUEUE_AGE_MS
                    ),
                    "maximum_event_age_milliseconds": (
                        PONG_QUALIFYING_MAXIMUM_EVENT_AGE_MS
                    ),
                    "source_heartbeat_timeout_seconds": (
                        PONG_QUALIFYING_SOURCE_HEARTBEAT_SECONDS
                    ),
                    "last_host_queue_age_ms": self._last_host_queue_age_ms,
                    "maximum_host_queue_age_ms": self._maximum_host_queue_age_ms,
                    "last_source_receipt_age_ms": self._last_source_receipt_age_ms,
                    "maximum_source_receipt_age_ms": (
                        self._maximum_source_receipt_age_ms
                    ),
                    "breach_count": self._freshness_breach_count,
                    "current_blocker": (
                        self._fhb_guard_blocker
                        if self._qualifying_architecture is not None
                        else None
                    ),
                    "current_heartbeat_blocker": (
                        qualifying_heartbeat_blocker
                    ),
                    "persistent_breach_threshold_seconds": (
                        PONG_QUALIFYING_PERSISTENT_BREACH_SECONDS
                    ),
                    "persistent_breach": bool(
                        self._qualifying_architecture is not None
                        and self._fhb_guard_breach_started is not None
                        and self._monotonic_clock()
                        - self._fhb_guard_breach_started
                        >= PONG_QUALIFYING_PERSISTENT_BREACH_SECONDS
                    ),
                    "recovery_count": self._fhb_guard_recoveries,
                },
                "evaluation_schedule": {
                    "configuration": {
                        "mode": self._scheduling.mode.value,
                        "interval_ms": self._scheduling.interval_ms,
                    },
                    "active_path": (
                        "PERIODIC_FLAT"
                        if self._event_protection_reason_locked() is None
                        else "EVENT_DRIVEN_PROTECTION"
                    ),
                    "protection_reason": self._event_protection_reason_locked(),
                    "last_protection_transition": self._last_protection_transition,
                    "qpc_clock": dict(self._clock_info),
                    "deadline_metrics": self._periodic_deadline.metrics(),
                    "source_receiver_ingress_records": self._source_ingress_records,
                    "authoritative_admission_records": self._authoritative_admission_records,
                    "event_driven_analytical_evaluations": self._event_driven_evaluations,
                    "periodic_flat_analytical_evaluations": self._periodic_evaluations,
                    "ordinary_completed_full_analyses": (
                        self._ordinary_completed_full_analyses
                    ),
                    "protection_completed_full_analyses": (
                        self._protection_completed_full_analyses
                    ),
                    "depth_events_per_analytical_evaluation": None,
                    "depth_events_per_analytical_evaluation_unavailable_reason": (
                        "UNALIGNED_MIXED_TRIGGER_METRIC_RETIRED"
                    ),
                    "periodic_ratio_telemetry": self._periodic_telemetry_snapshot_locked(
                        captured_monotonic=status_monotonic,
                        observation_queue_depth=observation_queue_depth,
                        oldest_pending_queue_age_ms=oldest_pending_queue_age_ms,
                    ),
                    "periodic_last_cutoff": (
                        None if self._last_periodic_cutoff is None
                        else self._last_periodic_cutoff.isoformat().replace("+00:00", "Z")
                    ),
                    "wake_mechanism": "bounded Queue.get timeout measured by QPC grid",
                    "entry_scoring_owner": (
                        "PONG_PERIODIC_DEADLINE"
                        if self._scheduling.enabled
                        else "AUTHENTICATED_EVENT_OWNER"
                    ),
                    "fhb_entry_cadence_policy": (
                        None if self._fhb is None else self._fhb.config.entry_policy_id
                    ),
                    "fhb_entry_cadence_interval_ms": (
                        0 if self._fhb is None
                        else self._fhb.config.entry_evaluation_interval_ms
                    ),
                    "duplicate_entry_throttle": False,
                },
                "registration": "PRODUCTION_REGISTERED",
                "persisted_host_telemetry": {
                    "schema": PONG_HOST_TELEMETRY_SCHEMA,
                    "root": str(self.host_telemetry_root),
                    "bounded_slots": [
                        "start.json",
                        "first-fault.json",
                        "checkpoint.json",
                        "final.json",
                    ],
                    "first_fault": self._host_first_fault,
                    "first_fault_persisted": self._host_first_fault_persisted,
                    "write_error": self._host_telemetry_error,
                    "checkpoint_interval_seconds": (
                        _HOST_TELEMETRY_CHECKPOINT_SECONDS
                    ),
                    "fault_retry_interval_seconds": (
                        _HOST_TELEMETRY_FAULT_RETRY_SECONDS
                    ),
                },
                "loaded_artifact_hash": PONG_LOADED_ARTIFACT_HASH,
                "module_import_source_sha256": dict(_LOADED_SOURCE_HASHES),
                "activation_authorized": self.activation_authorized,
                "activation_date_local": self._activation_date.isoformat(),
                "experiment_id": self._experiment_id,
                "launch_authorization_hash": (
                    None if self.launch_authorization is None
                    else self.launch_authorization.manifest_hash
                ),
                "launch_authorization_expires_at": (
                    None if self.launch_authorization is None else self.launch_authorization.expires_at_utc
                ),
                "launch_operator_acknowledged_at": (
                    None if self.launch_authorization is None else self.launch_authorization.operator_acknowledged_at_utc
                ),
                "account": self.topology.account_name,
                "account_class": self.topology.account_class,
                "instrument": self.topology.native_instrument,
                "canonical_contract": self.topology.canonical_contract,
                "topology_version": self.topology.version,
                "topology_hash": self.topology.binding_hash,
                "state_namespace": self.topology.state_namespace,
                "maximum_quantity": self.topology.maximum_quantity,
                "live_authority": self.topology.live_authority,
                "live_capital": self.topology.live_capital,
                "source_identity": None if self._producer is None else {
                    "session_id": self._producer[0],
                    "source_fingerprint": self._producer[1],
                    "build_fingerprint": self._producer[2],
                    "source_generation": self._source_generation,
                },
                "stream_counts": dict(self._stream_counts),
                "missing_events": dict(self._stream_missing),
                "out_of_order_events": dict(self._stream_out_of_order),
                "observation_queue_depth": observation_queue_depth,
                "observation_queue_high_water": self._observation_queue_high_water,
                "oldest_pending_queue_age_ms": oldest_pending_queue_age_ms,
                "observation_queue_overflow": self._observation_overflow,
                "compute_workers": self._compute_workers,
                "compute_build_hash": COMPUTE_BUILD_HASH,
                "compute_pending": len(self._compute_pending),
                "compute_queue_limit": self._compute_limit,
                "compute_submitted": self._compute_submitted,
                "compute_applied": self._compute_applied,
                "compute_refused": self._compute_refused,
                "fhb": fhb_status,
                "fhb_activation": fhb_activation,
                "fhb_freshness": None if self._fhb is None else {
                    "maximum_queue_age_milliseconds": (
                        self._fhb.config.maximum_queue_age_milliseconds
                    ),
                    "maximum_event_age_milliseconds": (
                        self._fhb.config.maximum_event_age_milliseconds
                    ),
                    "source_heartbeat_timeout_seconds": (
                        self._fhb.config.source_heartbeat_timeout_seconds
                    ),
                    "last_host_queue_age_ms": self._last_host_queue_age_ms,
                    "maximum_host_queue_age_ms": self._maximum_host_queue_age_ms,
                    "last_source_receipt_age_ms": self._last_source_receipt_age_ms,
                    "maximum_source_receipt_age_ms": (
                        self._maximum_source_receipt_age_ms
                    ),
                    "breach_count": self._freshness_breach_count,
                    "current_blocker": self._fhb_guard_blocker,
                    "current_heartbeat_blocker": fhb_heartbeat_blocker,
                    "heartbeat_stream": "DEPTH",
                    "last_depth_callback_monotonic": (
                        self._last_depth_callback_monotonic
                    ),
                    "current_breach_seconds": (
                        0.0
                        if self._fhb_guard_breach_started is None
                        else max(
                            0.0,
                            self._monotonic_clock()
                            - self._fhb_guard_breach_started,
                        )
                    ),
                    "persistent_breach_threshold_seconds": (
                        self._fhb.config.persistent_breach_seconds
                    ),
                    "persistent_breach": bool(
                        self._fhb_guard_breach_started is not None
                        and self._monotonic_clock()
                        - self._fhb_guard_breach_started
                        >= self._fhb.config.persistent_breach_seconds
                    ),
                    "last_breach_seconds": self._fhb_guard_last_breach_seconds,
                    "maximum_breach_seconds": (
                        self._fhb_guard_maximum_breach_seconds
                    ),
                    "recovery_count": self._fhb_guard_recoveries,
                },
                "compute_oldest_pending_seconds": (
                    0.0 if not self._compute_pending
                    else time.monotonic() - self._compute_pending[0][4]
                ),
                "input_error": self._input_error,
                "volume_reference": (
                    {"loaded_windows": 0, "ready_for_current_slot": False,
                     "error": "PONG_VOLUME_HISTORY_VERIFIER_NOT_CONFIGURED"}
                    if self._volume_history is None else self._volume_history.status(_now())
                ),
                "warm_start_complete": self._warm_start is not None,
                "hour_seed": (
                    "AUTHENTICATED_WARM_START"
                    if self._warm_start is not None
                    else "QUALIFIED_RETAINED_HOUR_CERTIFICATE"
                    if self._retained_hour_seed
                    else "UNAVAILABLE"
                ),
                "qualified_hour_certificate": None if self._certificate is None else {
                    "path": str(self.certificate_path),
                    "certificate_hash": self._certificate["certificate_hash"],
                    "interval": self._certificate["required_interval"],
                    "daily_bias_result_hash": self._certificate["daily_bias_result_hash"],
                },
                "daily_bias": self._daily_bias.payload(),
                "microstructure_pipeline": {
                    "state": "HOSTED",
                    "evaluation_count": self._microstructure_evaluations,
                    "entry_policy_id": (
                        None if self._fhb is None else self._fhb.config.entry_policy_id
                    ),
                    "entry_evaluation_interval_ms": (
                        0 if self._fhb is None
                        else self._fhb.config.entry_evaluation_interval_ms
                    ),
                    "entry_evaluation_starts": self._entry_evaluation_starts,
                    "entry_evaluation_deferred": self._entry_evaluation_deferred,
                    "minimum_entry_evaluation_spacing_ms": (
                        self._minimum_entry_evaluation_spacing_ms
                    ),
                    "last_entry_evaluation": self._last_entry_evaluation,
                    "applied_state_revision": self._market_state_revision,
                    "mapping_hash": self._mapper.policy.mapping_hash,
                    "discernment_policy_hash": self._discernment.policy.policy_hash,
                    "blockers": list(self._microstructure_blockers),
                },
                "runtime_bridge": None if bridge is None else bridge.status(),
                "controller": None if controller is None else {
                    "state_path": str(controller.path),
                    "controller_id": controller.state.controller_id,
                    "phase": controller.state.phase.value,
                    "activated_at_utc": controller.state.activated_at_utc,
                    "execution_deadline_utc": controller.state.execution_deadline_utc,
                    "heartbeat_valid_until": controller.state.heartbeat_valid_until,
                    "pending_command": (
                        None if controller.state.pending_command is None
                        else asdict(controller.state.pending_command)
                    ),
                    "state_hash": controller.state.state_hash,
                },
                "controller_error": self._controller_error,
                "bridge_error": self._bridge_error,
                "test_only": False,
            }
