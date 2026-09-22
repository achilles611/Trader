"""Durable, authenticated market-input collection for bounded Pong.

The collector is an observation-only sink owned by the existing Control Center
listener.  It never opens a socket, selects an account, or creates execution
authority.  Only records whose native producer signature was already verified
at the loopback boundary can enter its append-only journal.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import queue
import threading
import time
from collections.abc import Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

from src.lane_iii.contracts import normalized_utc
from src.l3f_provider.ninjatrader_observation import (
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from .pong_contract import SEP26_TOPOLOGY, PongContractTopology


PONG_COLLECTION_SCHEMA = "beelzebub-pong-authentic-hour-collection-v1"
PONG_COLLECTION_RECORD_SCHEMA = "beelzebub-pong-authenticated-observation-record-v1"
PONG_COLLECTION_STATUS_SCHEMA = "beelzebub-pong-authentic-hour-status-v1"
PONG_COLLECTION_CONTRACT = "MNQ SEP26"
PONG_COLLECTION_CANONICAL_CONTRACT = "MNQU6"
PONG_COLLECTION_TIMEZONE = "America/Denver"
_MARKET_TYPES = frozenset({"TRADE", "QUOTE", "DEPTH"})
_RETAINED_TYPES = _MARKET_TYPES | frozenset({"HISTORICAL_WARM_START", "HOURLY_BAR", "VOLUME_REFERENCE_HISTORY"})
_ZERO_HASH = "0" * 64
# CPython releases the GIL when hashlib receives more than 2,047 bytes in one
# constructor or update call.  Authentic observation records are commonly just
# over that boundary, so a busy receiver can preempt the sole ordered writer on
# nearly every chain hash.  Bounded updates preserve the exact SHA-256 digest
# while keeping this small writer-owned operation contiguous.
_HASHLIB_GIL_RELEASE_BYTES = 2048
_STATUS_REPLACE_RETRY_SECONDS = 1.0
_STATUS_REPLACE_RETRY_INTERVAL_SECONDS = 0.01
# Fixed, process-local timing buckets. The collector retains no per-record
# samples: authentic hours can contain millions of callbacks, so diagnostics
# must remain constant-memory and leave the ordered writer as durability owner.
_COLLECTOR_TIMING_BUCKETS_MS = (
    0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0,
    50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10_000.0, 30_000.0, 60_000.0,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: object, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _next_full_hour(value: datetime) -> datetime:
    boundary = value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # Starting exactly on a boundary cannot prove that the listener and native
    # transport were already healthy before that boundary.  Select the next
    # hour so every accepted interval has a pre-boundary collection witness.
    return boundary + timedelta(hours=1)


def _hour_start(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _canonical_bytes(value: object) -> bytes:
    return _CANONICAL_ENCODER.encode(value).encode("utf-8")


_CANONICAL_ENCODER = json.JSONEncoder(
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=True,
    check_circular=False,
)


def _add_record_hash(canonical_body: bytes, digest: str) -> bytes:
    """Insert the chain digest without canonicalizing the large body twice."""
    # Canonical top-level ordering places record_hash immediately before the
    # outer schema key.  Observation payloads can contain their own schema, so
    # use the final marker and then assert the exact canonical result in tests.
    marker = b',"schema":'
    index = canonical_body.rfind(marker)
    if index < 0:
        raise RuntimeError("PONG_COLLECTION_CANONICAL_SCHEMA_MARKER_MISSING")
    return (
        canonical_body[:index]
        + b',"record_hash":"'
        + digest.encode("ascii")
        + b'"'
        + canonical_body[index:]
    )


def _writer_sha256(value: bytes) -> str:
    """Return exact SHA-256 without a per-record hashlib GIL handoff."""
    digest = hashlib.sha256()
    maximum_update = _HASHLIB_GIL_RELEASE_BYTES - 1
    for offset in range(0, len(value), maximum_update):
        digest.update(value[offset:offset + maximum_update])
    return digest.hexdigest()


@dataclass
class _BoundedTiming:
    count: int = 0
    total_nanoseconds: int = 0
    maximum_nanoseconds: int = 0
    histogram_counts: list[int] | None = None

    def __post_init__(self) -> None:
        if self.histogram_counts is None:
            self.histogram_counts = [0] * (len(_COLLECTOR_TIMING_BUCKETS_MS) + 1)

    def record_nanoseconds(self, elapsed_nanoseconds: int) -> None:
        elapsed = max(0, elapsed_nanoseconds)
        self.count += 1
        self.total_nanoseconds += elapsed
        self.maximum_nanoseconds = max(self.maximum_nanoseconds, elapsed)
        elapsed_ms = elapsed / 1_000_000.0
        bucket = bisect_left(_COLLECTOR_TIMING_BUCKETS_MS, elapsed_ms)
        assert self.histogram_counts is not None
        self.histogram_counts[bucket] += 1

    def snapshot(self) -> dict[str, object]:
        def percentile_upper_bound(percentile: float) -> float:
            if self.count <= 0:
                return 0.0
            rank = max(1, int(self.count * percentile + 0.999999999))
            cumulative = 0
            assert self.histogram_counts is not None
            for index, value in enumerate(self.histogram_counts):
                cumulative += value
                if cumulative >= rank:
                    if index < len(_COLLECTOR_TIMING_BUCKETS_MS):
                        return _COLLECTOR_TIMING_BUCKETS_MS[index]
                    return round(self.maximum_nanoseconds / 1_000_000.0, 3)
            return round(self.maximum_nanoseconds / 1_000_000.0, 3)

        return {
            "count": self.count,
            "total_milliseconds": round(self.total_nanoseconds / 1_000_000.0, 3),
            "mean_milliseconds": round(
                self.total_nanoseconds / self.count / 1_000_000.0, 3,
            ) if self.count else 0.0,
            "maximum_milliseconds": round(self.maximum_nanoseconds / 1_000_000.0, 3),
            "p50_milliseconds": percentile_upper_bound(0.50),
            "p95_milliseconds": percentile_upper_bound(0.95),
            "p99_milliseconds": percentile_upper_bound(0.99),
            "quantile_method": "FIXED_HISTOGRAM_UPPER_BOUND",
            "histogram_upper_bounds_milliseconds": [
                *list(_COLLECTOR_TIMING_BUCKETS_MS), "overflow",
            ],
            "histogram_counts": list(self.histogram_counts or ()),
        }


def _finite_decimal(value: object, *, positive: bool = False) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite() or (positive and result <= 0):
        return None
    return result


def _identity(record: NinjaTraderObservation) -> tuple[str, str, str, str]:
    assert record.producer is not None
    return (
        record.session_id,
        record.producer.source_fingerprint,
        record.producer.build_fingerprint,
        record.producer.build_timestamp,
    )


def _new_stream_state() -> dict[str, object]:
    return {
        "records": 0,
        "first_source_event_sequence": None,
        "last_source_event_sequence": None,
        "source_sequence_gaps": 0,
        "duplicate_or_out_of_order": 0,
        "first_event_at": None,
        "last_event_at": None,
    }


def _new_interval(start: datetime) -> dict[str, object]:
    return {
        "start": _wire(start),
        "end": _wire(start + timedelta(hours=1)),
        "boundary_checked": False,
        "reasons": [],
        "producer_identity": None,
        "streams": {kind: _new_stream_state() for kind in sorted(_MARKET_TYPES)},
        "signature_verified_records": 0,
        "classified_trades": {"BUY": 0, "SELL": 0, "UNKNOWN": 0},
        "classified_volume": {"BUY": "0", "SELL": "0", "UNKNOWN": "0"},
        "depth": {"available": 0, "unavailable": 0, "invalid": 0},
        "hourly_bar": None,
    }


@dataclass(frozen=True)
class _CollectorItem:
    kind: str
    value: object
    enqueued_at: str
    enqueued_monotonic: float


class PongAuthenticHourCollector:
    """Persist signed native events and prove complete UTC-hour coverage."""

    _QUEUE_CAPACITY = 262_144
    _JOURNAL_BUFFER_BYTES = 1024 * 1024
    _FLUSH_INTERVAL_SECONDS = 0.25
    _STATUS_INTERVAL_SECONDS = 1.0
    _START_TIMEOUT_SECONDS = 5.0
    _STOP_TIMEOUT_SECONDS = 10.0

    def __init__(
        self,
        root: Path,
        *,
        expected_source_fingerprint: str = EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
        independent_market_collection: bool = False,
        now: datetime | None = None,
        topology: PongContractTopology = SEP26_TOPOLOGY,
    ) -> None:
        if not isinstance(root, Path):
            raise ValueError("Pong collection root must be a Path.")
        if not isinstance(expected_source_fingerprint, str) or len(expected_source_fingerprint) != 64:
            raise ValueError("Pong collector requires a source SHA-256 fingerprint.")
        if type(independent_market_collection) is not bool:
            raise ValueError("Pong collection independence policy must be boolean.")
        if type(topology) is not PongContractTopology:
            raise ValueError("Pong collection topology is invalid.")
        started = (now or _utc_now()).astimezone(timezone.utc)
        stamp = started.strftime("%Y%m%dT%H%M%S%fZ")
        self.root = root.expanduser().resolve()
        self.collector_id = f"pong-collector-{stamp}-{uuid4().hex[:12]}"
        self.collector_root = self.root / "collectors" / self.collector_id
        self.journal_path = self.collector_root / "authenticated-observations.jsonl"
        self.collector_status_path = self.collector_root / "status.json"
        self.status_path = self.root / "collection-status.json"
        self.expected_source_fingerprint = expected_source_fingerprint
        self.topology = topology
        self._source_generation: int | None = None
        self._producer_overflow_records = 0
        self._producer_overflow_baseline: int | None = None
        self._producer_overflow_delta = 0
        self._producer_transport_failed_records = 0
        self._producer_transport_failed_baseline: int | None = None
        self._producer_transport_failed_delta = 0
        self.independent_market_collection = independent_market_collection
        self.started_at = _wire(started)
        self._initial_target = _next_full_hour(started)
        self._queue: queue.Queue[_CollectorItem] = queue.Queue(self._QUEUE_CAPACITY)
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = "NEW"
        self._error: str | None = None
        self._transport_state = "UNKNOWN"
        self._transport_changed_at: str | None = None
        self._duplicate_records = 0
        self._rejected_records = 0
        self._queue_overflow_records = 0
        self._queue_high_water_records = 0
        self._last_queue_age_milliseconds = 0.0
        self._maximum_queue_age_milliseconds = 0.0
        self._enqueued_records = 0
        self._written_records = 0
        self._durable_records = 0
        self._journal_bytes = 0
        self._last_fsync_at: str | None = None
        self._journal_tip = _ZERO_HASH
        self._timings = {
            name: _BoundedTiming()
            for name in (
                "observation_enqueue_service",
                "queue_wait",
                "record_encode_and_hash",
                "journal_buffer_write",
                "collector_state_apply",
                "journal_flush",
                "journal_fsync",
                "status_persist",
            )
        }
        self._active_identity: tuple[str, str, str, str] | None = None
        self._last_source_sequences: dict[str, int] = {}
        self._last_quote: NinjaTraderObservation | None = None
        self._target_start = self._initial_target
        self._intervals: dict[str, dict[str, object]] = {
            _wire(self._initial_target): _new_interval(self._initial_target),
        }
        self._completed_starts: list[str] = []

    def start(self, *, timeout_seconds: float = _START_TIMEOUT_SECONDS) -> dict[str, object]:
        with self._lock:
            if self._state in {"COLLECTING", "STARTING"}:
                return self.status()
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            self._state = "STARTING"
            self._error = None
            self._ready.clear()
            self._stopped.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="PONG_AUTHENTIC_HOUR_COLLECTOR",
                daemon=False,
            )
            self._thread.start()
        if not self._ready.wait(timeout_seconds):
            raise RuntimeError("PONG_COLLECTION_START_TIMEOUT")
        value = self.status()
        if value["state"] != "COLLECTING":
            raise RuntimeError(f"PONG_COLLECTION_START_FAILED:{value.get('error') or value['state']}")
        return value

    def stop(self, *, timeout_seconds: float = _STOP_TIMEOUT_SECONDS) -> dict[str, object]:
        with self._lock:
            thread = self._thread
            if self._state == "NEW":
                self._state = "STOPPED"
                return self.status()
        if thread is not None and thread.is_alive():
            self._put_control("STOP", None)
            thread.join(timeout_seconds)
        with self._lock:
            if thread is not None and thread.is_alive():
                self._state = "FAILED"
                self._error = "collector_shutdown_timeout"
            elif self._state != "FAILED":
                self._state = "STOPPED"
            return self.status()

    def ingest(self, record: NinjaTraderObservation) -> None:
        enqueue_started = time.perf_counter_ns()
        if type(record) is not NinjaTraderObservation:
            self._record_drop("UNVERIFIED_OBSERVATION_TYPE")
            return
        if record.observation_type not in _RETAINED_TYPES:
            return
        if (
            record.schema != L3F3_SCHEMA
            or record.signature_verified is not True
            or record.producer is None
            or record.producer.protocol_version != NINJATRADER_ATTESTATION_PROTOCOL
            or record.producer.source_fingerprint != self.expected_source_fingerprint
        ):
            self._record_drop("UNVERIFIED_NATIVE_ATTESTATION")
            return
        self._put(
            _CollectorItem("OBSERVATION", record, _wire(_utc_now()), time.monotonic()),
            observation_enqueue_started=enqueue_started,
        )

    def on_transport_state(self, value: object) -> None:
        state = getattr(value, "value", value)
        self._put_control("TRANSPORT", str(state))

    def on_rejection(self, error: NinjaTraderObservationError) -> None:
        code = getattr(getattr(error, "code", None), "value", "UNKNOWN")
        detail = getattr(error, "detail", None) or "unspecified"
        self._put_control("REJECTION", {"code": str(code), "detail": str(detail)})

    def on_duplicate(self) -> None:
        self._put_control("DUPLICATE", None)

    def status(self) -> dict[str, object]:
        with self._lock:
            now = _utc_now()
            self._advance_intervals_locked(now)
            target = self._intervals[_wire(self._target_start)]
            target_status = self._interval_status_locked(target, now)
            completed = [
                self._interval_status_locked(self._intervals[key], now)
                for key in self._completed_starts[-3:]
                if key in self._intervals
            ]
            identity = self._identity_payload(self._active_identity)
            thread = self._thread
            return {
                "schema": PONG_COLLECTION_STATUS_SCHEMA,
                "collection_schema": PONG_COLLECTION_SCHEMA,
                "state": self._state,
                "error": self._error,
                "authority": "OBSERVE_ONLY",
                "paper_execution": "DISARMED",
                "live_authority": "DISARMED",
                "live_capital": "DENIED",
                "independence": {
                    "perpetual_operational_session_required": False,
                    "perpetual_execution_required": False,
                    "market_fanout": (
                        "PONG_ONLY"
                        if self.independent_market_collection
                        else "SHARED_WITH_DISARMED_RUNTIMES"
                    ),
                },
                "collector": {
                    "collector_id": self.collector_id,
                    "process_id": os.getpid(),
                    "parent_process_id": os.getppid(),
                    "thread_name": None if thread is None else thread.name,
                    "thread_native_id": None if thread is None else thread.native_id,
                    "thread_alive": bool(thread and thread.is_alive()),
                    "started_at": self.started_at,
                    "expected_source_fingerprint": self.expected_source_fingerprint,
                },
                "instrument": {
                    "native": self.topology.native_instrument,
                    "canonical": self.topology.canonical_contract,
                    "topology_version": self.topology.version,
                    "topology_hash": self.topology.binding_hash,
                    "state_namespace": self.topology.state_namespace,
                },
                "transport": {
                    "state": self._transport_state,
                    "changed_at": self._transport_changed_at,
                },
                "producer": identity,
                "durability": {
                    "journal_path": str(self.journal_path),
                    "collector_status_path": str(self.collector_status_path),
                    "current_status_path": str(self.status_path),
                    "enqueued_records": self._enqueued_records,
                    "written_records": self._written_records,
                    "durable_records": self._durable_records,
                    "journal_bytes": self._journal_bytes,
                    "journal_tip_sha256": self._journal_tip,
                    "last_fsync_at": self._last_fsync_at,
                    "queue_depth": self._queue.qsize(),
                    "queue_high_water_records": self._queue_high_water_records,
                    "queue_capacity": self._QUEUE_CAPACITY,
                    "last_queue_age_milliseconds": round(self._last_queue_age_milliseconds, 3),
                    "maximum_queue_age_milliseconds": round(self._maximum_queue_age_milliseconds, 3),
                    "queue_overflow_records": self._queue_overflow_records,
                    "producer_queue_overflow_records": self._producer_overflow_records,
                    "producer_queue_overflow_baseline": self._producer_overflow_baseline,
                    "producer_queue_overflow_delta": self._producer_overflow_delta,
                    "producer_transport_failed_records": self._producer_transport_failed_records,
                    "producer_transport_failed_baseline": (
                        self._producer_transport_failed_baseline
                    ),
                    "producer_transport_failed_delta": (
                        self._producer_transport_failed_delta
                    ),
                    "rejected_records": self._rejected_records,
                    "duplicate_records": self._duplicate_records,
                    "bounded_timing": {
                        name: timing.snapshot()
                        for name, timing in self._timings.items()
                    },
                },
                "required_interval": target_status,
                "recent_completed_intervals": completed,
            }

    def _put_control(self, kind: str, value: object) -> None:
        self._put(_CollectorItem(kind, value, _wire(_utc_now()), time.monotonic()))

    def _put(
        self,
        item: _CollectorItem,
        *,
        observation_enqueue_started: int | None = None,
    ) -> None:
        try:
            self._queue.put_nowait(item)
            enqueue_elapsed = (
                None
                if observation_enqueue_started is None
                else time.perf_counter_ns() - observation_enqueue_started
            )
            with self._lock:
                self._queue_high_water_records = max(
                    self._queue_high_water_records, self._queue.qsize(),
                )
                if item.kind == "OBSERVATION":
                    self._enqueued_records += 1
                    if enqueue_elapsed is not None:
                        self._timings["observation_enqueue_service"].record_nanoseconds(
                            enqueue_elapsed,
                        )
        except queue.Full:
            self._record_drop("COLLECTOR_QUEUE_OVERFLOW")

    def _record_drop(self, reason: str) -> None:
        with self._lock:
            self._queue_overflow_records += 1
            self._invalidate_at_locked(_utc_now(), reason)

    def _run(self) -> None:
        journal = None
        pending_records = 0
        last_flush = time.monotonic()
        last_status = 0.0
        try:
            self.collector_root.mkdir(parents=True, exist_ok=False)
            # Network-backed audit roots make one physical write per market
            # event too slow for authentic MNQ bursts. Buffer the append-only
            # stream, then explicitly flush it before every fsync so the
            # durable-record watermark keeps its exact meaning.
            journal = self.journal_path.open(
                "ab", buffering=self._JOURNAL_BUFFER_BYTES,
            )
            with self._lock:
                self._state = "COLLECTING"
                self._error = None
            self._timed_persist_status()
            # The initial status is already current and fully fsynced. Start
            # the cadence here instead of forcing a redundant two-file status
            # fsync immediately after the first queued item.
            last_status = time.monotonic()
            self._ready.set()
            stopping = False
            while not stopping:
                item: _CollectorItem | None
                try:
                    item = self._queue.get(timeout=self._FLUSH_INTERVAL_SECONDS)
                except queue.Empty:
                    item = None
                if item is not None:
                    queue_age_milliseconds = max(
                        0.0, (time.monotonic() - item.enqueued_monotonic) * 1000.0,
                    )
                    if item.kind == "STOP":
                        with self._lock:
                            self._record_queue_wait_locked(queue_age_milliseconds)
                            stopping = True
                    elif item.kind == "OBSERVATION":
                        assert isinstance(item.value, NinjaTraderObservation)
                        encode_started = time.perf_counter_ns()
                        line = self._observation_line(item.value, item.enqueued_at)
                        encode_elapsed = time.perf_counter_ns() - encode_started
                        write_started = time.perf_counter_ns()
                        journal.write(line)
                        write_elapsed = time.perf_counter_ns() - write_started
                        pending_records += 1
                        with self._lock:
                            self._record_queue_wait_locked(queue_age_milliseconds)
                            self._timings["record_encode_and_hash"].record_nanoseconds(
                                encode_elapsed,
                            )
                            self._timings["journal_buffer_write"].record_nanoseconds(
                                write_elapsed,
                            )
                            self._written_records += 1
                            self._journal_bytes += len(line)
                            apply_started = time.perf_counter_ns()
                            self._apply_observation_locked(item.value)
                            self._timings["collector_state_apply"].record_nanoseconds(
                                time.perf_counter_ns() - apply_started,
                            )
                    else:
                        with self._lock:
                            self._record_queue_wait_locked(queue_age_milliseconds)
                            self._apply_control_locked(item)
                    self._queue.task_done()
                now_monotonic = time.monotonic()
                with self._lock:
                    self._advance_intervals_locked(_utc_now())
                if pending_records and (
                    stopping
                    or item is None
                    or pending_records >= 1024
                    or now_monotonic - last_flush >= self._FLUSH_INTERVAL_SECONDS
                ):
                    flush_started = time.perf_counter_ns()
                    journal.flush()
                    flush_elapsed = time.perf_counter_ns() - flush_started
                    fsync_started = time.perf_counter_ns()
                    os.fsync(journal.fileno())
                    fsync_elapsed = time.perf_counter_ns() - fsync_started
                    with self._lock:
                        self._timings["journal_flush"].record_nanoseconds(flush_elapsed)
                        self._timings["journal_fsync"].record_nanoseconds(fsync_elapsed)
                        self._durable_records = self._written_records
                        self._last_fsync_at = _wire(_utc_now())
                    pending_records = 0
                    last_flush = now_monotonic
                if stopping or now_monotonic - last_status >= self._STATUS_INTERVAL_SECONDS:
                    self._timed_persist_status()
                    last_status = now_monotonic
            # Queue order guarantees STOP follows every record accepted before
            # stop(); perform one final durable checkpoint and status write.
            if pending_records:
                flush_started = time.perf_counter_ns()
                journal.flush()
                flush_elapsed = time.perf_counter_ns() - flush_started
                fsync_started = time.perf_counter_ns()
                os.fsync(journal.fileno())
                fsync_elapsed = time.perf_counter_ns() - fsync_started
                with self._lock:
                    self._timings["journal_flush"].record_nanoseconds(flush_elapsed)
                    self._timings["journal_fsync"].record_nanoseconds(fsync_elapsed)
                    self._durable_records = self._written_records
                    self._last_fsync_at = _wire(_utc_now())
            with self._lock:
                self._state = "STOPPED"
            self._timed_persist_status()
        except Exception as exc:
            with self._lock:
                self._state = "FAILED"
                self._error = f"{type(exc).__name__}: {exc}"
            self._ready.set()
            try:
                self._timed_persist_status()
            except Exception:
                pass
        finally:
            if journal is not None:
                journal.close()
            self._ready.set()
            self._stopped.set()

    def _record_queue_wait_locked(self, queue_age_milliseconds: float) -> None:
        self._last_queue_age_milliseconds = queue_age_milliseconds
        self._maximum_queue_age_milliseconds = max(
            self._maximum_queue_age_milliseconds,
            queue_age_milliseconds,
        )
        self._timings["queue_wait"].record_nanoseconds(
            int(queue_age_milliseconds * 1_000_000.0),
        )

    def _observation_line(self, record: NinjaTraderObservation, enqueued_at: str) -> bytes:
        # The FIFO writer thread is the sole owner of the journal sequence and
        # hash-chain tip.  Reading and advancing them here therefore needs no
        # cross-thread state-lock handoff.  Status readers can still observe
        # the new tip before the matching written watermark, exactly as they
        # could when the old method released its second lock before write().
        # Durability authority remains the later flush/fsync watermark.
        prior = self._journal_tip
        journal_sequence = self._written_records + 1
        prefix = b"{"
        if record.observation_type == "TRADE":
            prefix += (
                b'"aggressor_classification":'
                + _canonical_bytes(self._classify_trade(record))
                + b','
            )
        # These fragments are the exact sorted-key canonical representation
        # previously produced from the small wrapper dict. The already-cached
        # signed observation bytes remain embedded verbatim; only immutable
        # scalar wrapper fields are encoded here.
        canonical_body = b"".join((
            prefix,
            b'"collector_id":', _canonical_bytes(self.collector_id),
            b',"journal_sequence":', str(journal_sequence).encode("ascii"),
            b',"observation":', record.signed_wire_bytes(),
            b',"previous_record_hash":', _canonical_bytes(prior),
            b',"received_by_collector_at":', _canonical_bytes(enqueued_at),
            b',"schema":', _canonical_bytes(PONG_COLLECTION_RECORD_SCHEMA),
            b',"signature_verified":true',
            b',"wire_hash":', _canonical_bytes(record.wire_hash()),
            b'}',
        ))
        digest = _writer_sha256(canonical_body)
        self._journal_tip = digest
        return _add_record_hash(canonical_body, digest) + b"\n"

    def _classify_trade(self, trade: NinjaTraderObservation) -> dict[str, object]:
        return self.classify_trade(trade, self._last_quote)

    @staticmethod
    def classify_trade(trade: NinjaTraderObservation, quote: NinjaTraderObservation | None) -> dict[str, object]:
        payload = trade.payload
        side = "UNKNOWN"
        provenance = "UNAVAILABLE"
        source = payload.get("aggressor_source")
        reported_side = payload.get("aggressor_side")
        if source == "PROVIDER_NATIVE" and reported_side in {"BUY", "SELL"}:
            side = str(reported_side)
            provenance = "AUTHENTICATED_PROVIDER"
        elif source == "BID_ASK_CLASSIFICATION":
            price = _finite_decimal(payload.get("price"), positive=True)
            bid = _finite_decimal(payload.get("bid_at_trade"), positive=True)
            ask = _finite_decimal(payload.get("ask_at_trade"), positive=True)
            if (
                quote is not None
                and quote.observation_id == payload.get("derivation_quote_observation_id")
                and quote.session_id == trade.session_id
                and quote.provider_timestamp == trade.provider_timestamp
                and quote.payload.get("bid") == payload.get("bid_at_trade")
                and quote.payload.get("ask") == payload.get("ask_at_trade")
                and price is not None
                and bid is not None
                and ask is not None
                and bid < ask
            ):
                if price >= ask:
                    side, provenance = "BUY", "AUTHENTICATED_DERIVED"
                elif price <= bid:
                    side, provenance = "SELL", "AUTHENTICATED_DERIVED"
        return {
            "side": side,
            "provenance": provenance,
            "method": "SAME_CALLBACK_PAIRED_BID_ASK_V1",
            "derivation_quote_observation_id": payload.get("derivation_quote_observation_id"),
        }

    def _apply_observation_locked(self, record: NinjaTraderObservation) -> None:
        current_identity = _identity(record)
        event_at = _parse_time(record.provider_timestamp or record.ninja_receipt_time, "Pong collection event time")
        generation = record.payload.get("source_generation")
        if record.observation_type in _MARKET_TYPES and generation is not None:
            if type(generation) is not int or generation <= 0:
                self._invalidate_at_locked(event_at, "SOURCE_GENERATION_INVALID")
            elif self._source_generation is not None and generation != self._source_generation:
                self._invalidate_at_locked(event_at, "SOURCE_GENERATION_CHANGED")
                self._invalidate_at_locked(_utc_now(), "SOURCE_GENERATION_CHANGED")
                self._last_quote = None
            self._source_generation = generation
        if self._active_identity is None:
            self._active_identity = current_identity
        elif self._active_identity != current_identity:
            self._invalidate_at_locked(event_at, "PRODUCER_IDENTITY_CHANGED")
            self._active_identity = current_identity
            self._last_source_sequences.clear()
            self._last_quote = None
            self._producer_overflow_baseline = None
            self._producer_overflow_delta = 0
            self._producer_transport_failed_baseline = None
            self._producer_transport_failed_delta = 0
        for field, attribute, baseline_attribute, delta_attribute, reason in (
            (
                "observer_queue_overflow_frames",
                "_producer_overflow_records",
                "_producer_overflow_baseline",
                "_producer_overflow_delta",
                "PRODUCER_QUEUE_OVERFLOW",
            ),
            (
                "observer_transport_failed_frames",
                "_producer_transport_failed_records",
                "_producer_transport_failed_baseline",
                "_producer_transport_failed_delta",
                "PRODUCER_TRANSPORT_FAILED",
            ),
        ):
            total = record.payload.get(field)
            if type(total) is not int or total < 0:
                continue
            baseline = getattr(self, baseline_attribute)
            current = getattr(self, attribute)
            if baseline is None:
                setattr(self, baseline_attribute, total)
                setattr(self, attribute, total)
                continue
            if total < current:
                self._invalidate_at_locked(event_at, "PRODUCER_COUNTER_REGRESSION")
                self._invalidate_at_locked(_utc_now(), "PRODUCER_COUNTER_REGRESSION")
                setattr(self, attribute, total)
                setattr(self, delta_attribute, max(0, total - baseline))
            elif total > current:
                setattr(self, attribute, total)
                setattr(self, delta_attribute, total - baseline)
                self._invalidate_at_locked(event_at, reason)
                self._invalidate_at_locked(_utc_now(), reason)

        if record.observation_type == "QUOTE":
            self._last_quote = record
        interval = self._interval_for_event_locked(event_at)
        if interval is None:
            self._track_sequence_locked(record, None)
            return
        interval["signature_verified_records"] = int(interval["signature_verified_records"]) + 1
        interval_identity = interval["producer_identity"]
        identity_payload = self._identity_payload(current_identity)
        if interval_identity is None:
            interval["producer_identity"] = identity_payload
        elif interval_identity != identity_payload:
            self._add_reason(interval, "PRODUCER_IDENTITY_CHANGED")

        if record.observation_type in _MARKET_TYPES:
            self._track_sequence_locked(record, interval)
            stream = interval["streams"][record.observation_type]  # type: ignore[index]
            stream["records"] = int(stream["records"]) + 1
            stream["first_event_at"] = stream["first_event_at"] or _wire(event_at)
            stream["last_event_at"] = _wire(event_at)
            if record.payload.get("contract_id") != self.topology.native_instrument:
                self._add_reason(interval, "NATIVE_CONTRACT_MISMATCH")
            if record.observation_type == "QUOTE" and not self._valid_quote(record.payload):
                self._add_reason(interval, "QUOTE_INVALID")
            elif record.observation_type == "DEPTH":
                depth = interval["depth"]
                validity = self._depth_validity(record.payload)
                depth[validity] = int(depth[validity]) + 1
                if validity == "invalid":
                    self._add_reason(interval, "DEPTH_INVALID")
            elif record.observation_type == "TRADE":
                classification = self._classify_trade(record)
                side = str(classification["side"])
                counts = interval["classified_trades"]
                counts[side] = int(counts[side]) + 1
                size = _finite_decimal(record.payload.get("size"), positive=True)
                if size is None:
                    self._add_reason(interval, "TRADE_INVALID")
                else:
                    volumes = interval["classified_volume"]
                    volumes[side] = str(Decimal(str(volumes[side])) + size)
        elif record.observation_type == "HOURLY_BAR":
            self._apply_hourly_bar_locked(record)

    def _track_sequence_locked(
        self, record: NinjaTraderObservation, interval: dict[str, object] | None,
    ) -> None:
        if record.observation_type not in _MARKET_TYPES:
            return
        raw = record.payload.get("source_event_sequence")
        if type(raw) is not int or raw <= 0:
            if interval is not None:
                self._add_reason(interval, "SOURCE_EVENT_SEQUENCE_INVALID")
            return
        previous = self._last_source_sequences.get(record.observation_type)
        stream = None if interval is None else interval["streams"][record.observation_type]  # type: ignore[index]
        if stream is not None:
            stream["first_source_event_sequence"] = stream["first_source_event_sequence"] or raw
            stream["last_source_event_sequence"] = raw
        if previous is not None and raw != previous + 1:
            if stream is not None:
                if raw <= previous:
                    stream["duplicate_or_out_of_order"] = int(stream["duplicate_or_out_of_order"]) + 1
                    self._add_reason(interval, "SOURCE_SEQUENCE_DUPLICATE_OR_OUT_OF_ORDER")
                else:
                    stream["source_sequence_gaps"] = int(stream["source_sequence_gaps"]) + 1
                    self._add_reason(interval, "SOURCE_SEQUENCE_GAP")
        self._last_source_sequences[record.observation_type] = raw

    def _apply_hourly_bar_locked(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        try:
            start = _parse_time(payload.get("start"), "Pong live hour start")
            end = _parse_time(payload.get("end"), "Pong live hour end")
        except (TypeError, ValueError):
            self._invalidate_at_locked(_utc_now(), "HOURLY_BAR_INVALID")
            return
        interval = self._intervals.get(_wire(start))
        if interval is None:
            return
        if (
            end != start + timedelta(hours=1)
            or payload.get("contract_id") != self.topology.native_instrument
            or payload.get("source") != "NINJATRADER_BARS_REQUEST_PROVIDER"
            or payload.get("publication_policy") != "CAUSAL_LIVE_COMPLETED_HOUR_V1"
        ):
            self._add_reason(interval, "HOURLY_BAR_INVALID")
            return
        interval["hourly_bar"] = {
            "observation_id": record.observation_id,
            "payload_hash": record.payload_hash,
            "observed_at": record.ninja_receipt_time,
            "start": _wire(start),
            "end": _wire(end),
        }

    def _apply_control_locked(self, item: _CollectorItem) -> None:
        at = _parse_time(item.enqueued_at, "Pong collector control time")
        if item.kind == "TRANSPORT":
            self._transport_state = str(item.value)
            self._transport_changed_at = item.enqueued_at
            if self._transport_state not in {"HEALTHY", "CONNECTING"}:
                self._invalidate_at_locked(at, "OBSERVATION_TRANSPORT_DISCONTINUITY")
        elif item.kind == "REJECTION":
            self._rejected_records += 1
            self._invalidate_at_locked(at, "SIGNED_OBSERVATION_REJECTED")
        elif item.kind == "DUPLICATE":
            self._duplicate_records += 1

    def _interval_for_event_locked(self, event_at: datetime) -> dict[str, object] | None:
        key = _wire(_hour_start(event_at))
        return self._intervals.get(key)

    def _invalidate_at_locked(self, at: datetime, reason: str) -> None:
        interval = self._interval_for_event_locked(at)
        if interval is not None:
            self._add_reason(interval, reason)

    @staticmethod
    def _add_reason(interval: dict[str, object], reason: str) -> None:
        reasons = interval["reasons"]
        if reason not in reasons:
            reasons.append(reason)

    def _advance_intervals_locked(self, now: datetime) -> None:
        while now >= self._target_start:
            target = self._intervals[_wire(self._target_start)]
            if not target["boundary_checked"]:
                target["boundary_checked"] = True
                if self._transport_state != "HEALTHY":
                    self._add_reason(target, "TRANSPORT_NOT_HEALTHY_AT_INTERVAL_START")
                if self._active_identity is None:
                    self._add_reason(target, "PRODUCER_IDENTITY_MISSING_AT_INTERVAL_START")
                else:
                    target["producer_identity"] = self._identity_payload(self._active_identity)
            end = self._target_start + timedelta(hours=1)
            if now < end:
                break
            key = _wire(self._target_start)
            if key not in self._completed_starts:
                self._completed_starts.append(key)
            self._target_start = end
            next_key = _wire(self._target_start)
            self._intervals.setdefault(next_key, _new_interval(self._target_start))
        # Retain enough recent state for a live HOURLY_BAR arriving within the
        # frozen five-minute publication allowance, plus three status entries.
        keep_after = now - timedelta(hours=4)
        for key in tuple(self._intervals):
            if _parse_time(key, "Pong interval key") < keep_after and key not in self._completed_starts[-3:]:
                del self._intervals[key]

    def _interval_status_locked(self, interval: dict[str, object], now: datetime) -> dict[str, object]:
        start = _parse_time(interval["start"], "Pong interval start")
        end = _parse_time(interval["end"], "Pong interval end")
        elapsed = max(0.0, min(3600.0, (now - start).total_seconds()))
        reasons = list(interval["reasons"])
        complete = now >= end
        streams = interval["streams"]
        for kind in sorted(_MARKET_TYPES):
            if int(streams[kind]["records"]) == 0:
                reasons.append(f"{kind}_COVERAGE_MISSING")
        if int(interval["depth"]["available"]) == 0:
            reasons.append("COHERENT_DEPTH_COVERAGE_MISSING")
        classified = interval["classified_trades"]
        classified_count = int(classified["BUY"]) + int(classified["SELL"])
        if classified_count < 8:
            reasons.append("ORDER_FLOW_MINIMUM_EVIDENCE_NOT_MET")
        if not complete:
            reasons.append("HOURLY_INTERVAL_INCOMPLETE")
        elif interval["hourly_bar"] is None:
            reasons.append("CAUSAL_LIVE_HOURLY_BAR_MISSING")
        reasons = list(dict.fromkeys(reasons))
        eligible = complete and not reasons
        denver = ZoneInfo(PONG_COLLECTION_TIMEZONE)
        state = (
            "ELIGIBLE"
            if eligible
            else "PENDING"
            if now < start
            else "COLLECTING"
            if now < end
            else "CLOSED_AWAITING_HOURLY_BAR"
            if interval["hourly_bar"] is None and now <= end + timedelta(seconds=300)
            else "INELIGIBLE"
        )
        return {
            "start_utc": _wire(start),
            "end_utc": _wire(end),
            "start_denver": start.astimezone(denver).isoformat(),
            "end_denver": end.astimezone(denver).isoformat(),
            "state": state,
            "complete": complete,
            "eligible": eligible,
            "progress_seconds": elapsed,
            "progress_percent": round(elapsed / 36.0, 3),
            "producer": interval["producer_identity"],
            "signature_verified_records": interval["signature_verified_records"],
            "streams": json.loads(json.dumps(streams)),
            "classified_trades": dict(classified),
            "classified_volume": dict(interval["classified_volume"]),
            "depth": dict(interval["depth"]),
            "hourly_bar": interval["hourly_bar"],
            "continuity_valid": not interval["reasons"],
            "blockers": reasons,
        }

    @staticmethod
    def _identity_payload(value: tuple[str, str, str, str] | None) -> dict[str, object] | None:
        if value is None:
            return None
        return {
            "session_id": value[0],
            "source_fingerprint": value[1],
            "build_fingerprint": value[2],
            "build_timestamp": value[3],
            "signature_verified": True,
        }

    @staticmethod
    def _valid_quote(payload: Mapping[str, object]) -> bool:
        bid = _finite_decimal(payload.get("bid"), positive=True)
        ask = _finite_decimal(payload.get("ask"), positive=True)
        bid_size = _finite_decimal(payload.get("bid_size"), positive=True)
        ask_size = _finite_decimal(payload.get("ask_size"), positive=True)
        return None not in {bid, ask, bid_size, ask_size} and bid < ask  # type: ignore[operator]

    @staticmethod
    def _depth_validity(payload: Mapping[str, object]) -> str:
        if payload.get("status") == "UNAVAILABLE":
            return "unavailable"
        bids = payload.get("bids")
        asks = payload.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
            return "invalid"

        def levels(values: list[object]) -> list[tuple[Decimal, Decimal]] | None:
            result: list[tuple[Decimal, Decimal]] = []
            for value in values:
                if not isinstance(value, Mapping):
                    return None
                price = _finite_decimal(value.get("price"), positive=True)
                size = _finite_decimal(value.get("size"), positive=True)
                if price is None or size is None:
                    return None
                result.append((price, size))
            return result

        bid_levels = levels(bids)
        ask_levels = levels(asks)
        if bid_levels is None or ask_levels is None:
            return "invalid"
        bid_prices = [item[0] for item in bid_levels]
        ask_prices = [item[0] for item in ask_levels]
        if (
            bid_prices != sorted(bid_prices, reverse=True)
            or ask_prices != sorted(ask_prices)
            or bid_prices[0] >= ask_prices[0]
        ):
            return "invalid"
        return "available"

    def _persist_status(self) -> None:
        value = self.status()
        data = _canonical_bytes(value) + b"\n"
        for path in (self.collector_status_path, self.status_path):
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + f".{self.collector_id}.tmp")
            with temporary.open("wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            # On Windows a concurrent read can briefly prevent replacement of
            # the destination even though the fully-fsynced temporary file is
            # valid.  Status publication is diagnostic, not a market callback;
            # retry only this transient sharing failure for a bounded period.
            deadline = time.monotonic() + _STATUS_REPLACE_RETRY_SECONDS
            while True:
                try:
                    os.replace(temporary, path)
                    break
                except PermissionError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(_STATUS_REPLACE_RETRY_INTERVAL_SECONDS)

    def _timed_persist_status(self) -> None:
        started = time.perf_counter_ns()
        self._persist_status()
        elapsed = time.perf_counter_ns() - started
        with self._lock:
            self._timings["status_persist"].record_nanoseconds(elapsed)
