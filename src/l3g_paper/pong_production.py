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
from typing import Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import PONG_ENTRY_PROFILE_VERSION
from .pong_authorization import PongLaunchAuthorization
from .pong_collection import PongAuthenticHourCollector
from .pong_contract import (
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
from .pong import PongActionKind, PongState, PongStateMachine
from .pong_scheduler import (
    PongPeriodicDeadline,
    PongSchedulingConfig,
    PongSchedulingMode,
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
        scheduling: PongSchedulingConfig = PongSchedulingConfig(),
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        topology: PongContractTopology = SEP26_TOPOLOGY,
    ) -> None:
        if not callable(native_dispatch):
            raise TypeError("Pong production host requires one native dispatcher.")
        if type(scheduling) is not PongSchedulingConfig or not callable(clock_ns):
            raise ValueError("PONG_SCHEDULING_CONTRACT_INVALID")
        if (
            scheduling.enabled
            and selected_profile != PONG_ENTRY_PROFILE_VERSION
        ):
            raise ValueError("PONG_PERIODIC_PROFILE_NOT_SELECTED")
        if scheduling.enabled and compute_workers:
            raise ValueError("PONG_PERIODIC_COMPUTE_WORKERS_NOT_SUPPORTED")
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
        self._observations: queue.Queue[NinjaTraderObservation] = queue.Queue(maxsize=262144)
        self._admission_lock = threading.Lock()
        self._observation_overflow = 0
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
        self._clock_ns = clock_ns
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
        self._last_admitted_receipt_at: datetime | None = None
        self._last_periodic_cutoff: datetime | None = None
        self._last_protection_transition: str | None = None
        self._process_instance_id = "pong-controller-process-" + uuid4().hex
        self._controller: PongBoundedController | None = None
        self._controller_error: str | None = None
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
        self._depth = {
            BookSide.BID: _DepthWindow(),
            BookSide.ASK: _DepthWindow(),
        }
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

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._compute_workers and self._compute_pool is None:
                self._compute_pool = open_compute_pool(self._compute_workers)
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="PONG_PRODUCTION_CONTROLLER_HOST",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout_seconds)
            if thread.is_alive():
                raise RuntimeError('PONG_HOST_DRAIN_INCOMPLETE')
        if self._compute_pool is not None:
            self._compute_pool.shutdown(wait=False, cancel_futures=True)
            self._compute_pool = None

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
        try:
            self._observations.put_nowait(record)
            with self._admission_lock:
                self._source_ingress_records += 1
        except queue.Full:
            with self._admission_lock:
                self._observation_overflow += 1
                self._input_error = "PONG_PRODUCTION_OBSERVATION_QUEUE_OVERFLOW"

    def _consume_observation(self, record: NinjaTraderObservation) -> None:
        """Process exactly one queued causal event before admitting the next."""
        def cursor() -> tuple[object, ...]:
            return tuple(
                values[-1].event.event_id if values else None
                for values in (self._trades, self._depth[BookSide.BID], self._depth[BookSide.ASK])
            )
        with self._lock:
            before = cursor()
            self._process_observation(record)
            if self._input_error is not None:
                return
            receipt = _time(record.ninja_receipt_time)
            self._last_admitted_receipt_at = max(
                receipt, self._last_admitted_receipt_at or receipt,
            )
            self._authoritative_admission_records += 1
            if cursor() == before:
                return
            protection_reason = self._event_protection_reason_locked()
            if not self._scheduling.enabled or protection_reason is not None:
                if protection_reason is not None:
                    self._last_protection_transition = protection_reason
                self._event_driven_evaluations += 1
                self._evaluate_microstructure(receipt)

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
            protection_reason = self._event_protection_reason_locked()
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
            self._periodic_evaluations += 1
            self._evaluate_microstructure(cutoff)
            if (
                self._microstructure_blockers
                and self._discernment.last_snapshot is not None
            ):
                update = self._discernment.on_expiry(
                    cutoff.isoformat().replace("+00:00", "Z"),
                )
                self._apply_discernment_update(update)
            self._periodic_deadline.complete(
                due, applied=self._input_error is None,
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
                        return
                self._stream_sequences[record.observation_type] = sequence
                self._stream_counts[record.observation_type] += 1
                if record.observation_type == "TRADE":
                    self._admit_trade(record)
                elif record.observation_type == "QUOTE":
                    self._last_quote_record = record
                elif record.observation_type == "DEPTH":
                    self._admit_depth(record)
            except (KeyError, TypeError, ValueError, PongNativeInputRefused) as error:
                self._input_error = str(error) or type(error).__name__

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

    def _admit_depth(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        side = BookSide.BID if str(payload.get("side")).upper() == "BID" else BookSide.ASK
        levels = payload.get("bids" if side is BookSide.BID else "asks")
        if not isinstance(levels, list) or not levels:
            return
        # Native position-shift notifications can carry mutation_price=0
        # while the signed post-mutation book snapshot is valid. They retain
        # their stream sequence and durable collection evidence, but are not a
        # price-bearing microstructure observation.
        try:
            price = Decimal(str(payload["mutation_price"]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return
        if not price.is_finite() or price <= 0:
            return
        touch = Decimal(str(levels[0]["price"]))
        if (side is BookSide.BID and price > touch) or (side is BookSide.ASK and price < touch):
            return
        kind = DepthEventKind.UPDATE
        if payload.get("is_reset") is True:
            kind = DepthEventKind.RECONNECT_RESET
        nearby = sum((Decimal(str(item["size"])) for item in levels), Decimal("0"))
        self._depth[side].append(DepthObservation(
            self._event_ref(record), self.topology.native_instrument, side, price,
            Decimal(str(payload.get("mutation_volume") or 0)), nearby, touch, kind,
            Decimal("0"), Decimal("0"), False, PriceResponse.UNAVAILABLE,
        ))

    def _evaluate_microstructure(self, at: datetime) -> None:
        if self._compute_pool is None:
            self._last_microstructure_at = at
        cutoff = at - timedelta(seconds=5)
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
            return
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
                return
            flow = self._flow.evaluate(trades, window_start=cutoff.isoformat().replace('+00:00', 'Z'),
                                       as_of=as_of, feed_continuity_valid=self._input_error is None)
            bid = self._liquidity.evaluate(bids, as_of=as_of, feed_continuity_valid=self._input_error is None)
            ask = self._liquidity.evaluate(asks, as_of=as_of, feed_continuity_valid=self._input_error is None)
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
        except Exception as error:
            blockers = (
                "PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:" + type(error).__name__,
            )
            if self._compute_pool is None:
                self._microstructure_blockers = blockers
            elif self._input_error is None:
                self._queue_compute(None, at, blockers)

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

    def _bias_from_inputs(
        previous: CompletedPongHour,
        current: CompletedPongHour,
        trades: tuple[ClassifiedTradeInput, ...],
        *,
        published_at: str,
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
            self.topology.native_instrument, current.start, current.end, published_at,
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
            hours[0], hours[1], trades, published_at=str(value["published_at"]),
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
            try:
                if self._compute_batch and (self._stop.is_set() or self._observations.empty()
                        or time.monotonic() - self._compute_batch_started >= 0.01):
                    self._flush_compute_batch()
                self._drain_compute()
                try:
                    if len(self._compute_pending) >= self._compute_limit:
                        self._compute_wakeup.wait(0.005)
                        self._compute_wakeup.clear()
                        record = None  # bounded backpressure; control ticks continue
                    else:
                        timeout = 0.005 if self._compute_pending else 0.5
                        periodic_wait = self._periodic_wait_seconds()
                        if periodic_wait is not None:
                            timeout = min(timeout, periodic_wait)
                        record = self._observations.get(timeout=timeout)
                except queue.Empty:
                    record = None
                if record is not None:
                    try:
                        self._consume_observation(record)
                    finally:
                        self._observations.task_done()
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
                        self._dispatch_native(directive, evidence)
                    continue
                next_control = time.monotonic() + 0.5
                runtime = dict(self._runtime_snapshot())
                collection = dict(self._collector_status())
                prepare_activation = False
                command: PongControllerCommand | None = None
                with self._lock:
                    self._runtime = runtime
                    self._collection = collection
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
                        self._runtime = refreshed
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
                    self._dispatch_native(directive, evidence)
                if command is not None:
                    self._dispatch_controller(command)
            except Exception as error:
                with self._lock:
                    self._controller_error = "PONG_PRODUCTION_HOST_LOOP_FAILED:" + type(error).__name__

    def status(self) -> dict[str, object]:
        with self._lock:
            authority = self.authority_status()
            controller = self._controller
            bridge = self._bridge
            return {
                **authority,
                "state": "RUNNING" if self._thread is not None and self._thread.is_alive() else "STOPPED",
                "selected_profile": self.selected_profile,
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
                    "periodic_last_cutoff": (
                        None if self._last_periodic_cutoff is None
                        else self._last_periodic_cutoff.isoformat().replace("+00:00", "Z")
                    ),
                    "wake_mechanism": "bounded Queue.get timeout measured by QPC grid",
                },
                "registration": "PRODUCTION_REGISTERED",
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
                "observation_queue_depth": self._observations.qsize(),
                "observation_queue_overflow": self._observation_overflow,
                "compute_workers": self._compute_workers,
                "compute_build_hash": COMPUTE_BUILD_HASH,
                "compute_pending": len(self._compute_pending),
                "compute_queue_limit": self._compute_limit,
                "compute_submitted": self._compute_submitted,
                "compute_applied": self._compute_applied,
                "compute_refused": self._compute_refused,
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
