"""Bounded, default-off runtime adapter for LSC shadow evidence.

The listener-facing method, :meth:`LSCShadowRuntime.submit`, performs only a
passive-kind check and ``Queue.put_nowait``.  Encoding, compression, segment
publication, commitment writes, verification, and failure-receipt I/O are
owned by the worker thread.  A shadow failure invalidates this evidence path;
it never raises into or waits on the authority path.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import queue
import threading
import time
from typing import Callable
from uuid import uuid4

from src.l3f_provider.ninjatrader_observation import (
    L3F3_SCHEMA,
    NinjaTraderObservation,
)
from src.lsc.codec import (
    LSCContext,
    context_digest,
    decode_context,
    encode_context,
    encode_observation,
)
from src.lsc.commitment_log import (
    LSC_FORMAT_IDENTITY,
    ShadowCommitmentLog,
    ShadowIdentityContext,
    canonical_storage_identity,
)
from src.lsc.segment_store import (
    LSCStoreLimitError,
    SegmentBuilder,
    SegmentLimits,
    SegmentStore,
)


_PASSIVE_KINDS = frozenset({"QUOTE", "TRADE", "DEPTH"})
_RECEIPT_SCHEMA = "beelzebub-lsc-shadow-runtime-receipt-v1"
_RUN_STATE_SCHEMA = "beelzebub-lsc-shadow-run-state-v1"


class LSCShadowRuntimeError(RuntimeError):
    """The shadow writer cannot truthfully claim a complete durable prefix."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z",
    )


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _process_liveness(process_id: int) -> bool | None:
    """Return True/False only when process liveness is safely knowable."""
    if type(process_id) is not int or process_id <= 0:
        return None
    if os.name == "nt":
        import ctypes

        process_synchronize = 0x00100000
        wait_object_0 = 0x00000000
        wait_timeout = 0x00000102
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
        open_process.restype = ctypes.c_void_p
        wait_for_single_object = kernel32.WaitForSingleObject
        wait_for_single_object.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        wait_for_single_object.restype = ctypes.c_uint32
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        handle = open_process(process_synchronize, False, process_id)
        if not handle:
            # ERROR_INVALID_PARAMETER is the documented result for a process
            # identifier that does not exist. Access denied and every other
            # result remain UNKNOWN and therefore fail closed.
            return False if ctypes.get_last_error() == 87 else None
        try:
            state = wait_for_single_object(handle, 0)
            if state == wait_timeout:
                return True
            if state == wait_object_0:
                return False
            return None
        finally:
            close_handle(handle)
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


@dataclass(frozen=True)
class LSCShadowConfig:
    root: Path
    journal_id: str
    profile_id: str
    native_contract_id: str
    canonical_contract_id: str
    tick_size: str = "0.25"
    queue_capacity: int = 65_536
    segment_limits: SegmentLimits = SegmentLimits()
    maximum_queue_residence_seconds: float = 1.0

    def __post_init__(self) -> None:
        root = Path(self.root).expanduser()
        if not root.is_absolute():
            raise ValueError("LSC shadow root must be absolute")
        object.__setattr__(self, "root", root)
        for name in (
            "journal_id", "profile_id", "native_contract_id",
            "canonical_contract_id", "tick_size",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"{name} must be non-empty trimmed text")
        if type(self.queue_capacity) is not int or self.queue_capacity <= 0:
            raise ValueError("queue_capacity must be a positive integer")
        if (
            isinstance(self.maximum_queue_residence_seconds, bool)
            or not isinstance(self.maximum_queue_residence_seconds, (int, float))
            or not 0 < float(self.maximum_queue_residence_seconds) <= 60.0
        ):
            raise ValueError("maximum_queue_residence_seconds must be in (0, 60]")


@dataclass(frozen=True)
class _QueuedObservation:
    observation: NinjaTraderObservation
    enqueued_monotonic: float


class LSCShadowRuntime:
    """One bounded worker producing an independent, non-authoritative chain."""

    def __init__(
        self,
        config: LSCShadowConfig,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], str] = _utc_now,
    ) -> None:
        if not isinstance(config, LSCShadowConfig):
            raise TypeError("config must be LSCShadowConfig")
        self.config = config
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._queue: queue.Queue[_QueuedObservation] = queue.Queue(config.queue_capacity)
        self._state_lock = threading.RLock()
        self._accepting = False
        self._closing = False
        self._closed = False
        self._worker: threading.Thread | None = None
        self._failure_reason: str | None = None
        self._failure_detail: str | None = None
        self._failure_receipt: Path | None = None
        self._completion_receipt: Path | None = None
        self._submitted = 0
        self._processed = 0
        self._non_passive_ignored = 0
        self._overflow = 0
        self._rejected_after_failure = 0
        self._queue_high_water = 0
        self._maximum_residence_seconds = 0.0
        self._sealed_segments = 0
        self._committed_segments = 0
        self._last_local_sequence: int | None = None
        self._started_at: str | None = None
        self._closed_at: str | None = None
        self._context: LSCContext | None = None
        self._context_bytes: bytes | None = None
        self._builder: SegmentBuilder | None = None
        self._builder_opened_monotonic: float | None = None
        self._store: SegmentStore | None = None
        self._commitments: ShadowCommitmentLog | None = None
        self._active_run_path = self.config.root / "runtime-state" / "active.json"
        self._terminal_status: dict[str, object] | None = None

    def _write_active_run_marker(self) -> None:
        destination = self._active_run_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        value = {
            "schema": _RUN_STATE_SCHEMA,
            "state": "ACTIVE",
            "started_at": self._started_at,
            "pid": os.getpid(),
            "root": str(self.config.root.resolve()),
            "journal_id": self.config.journal_id,
            "profile_id": self.config.profile_id,
            "native_contract_id": self.config.native_contract_id,
            "canonical_contract_id": self.config.canonical_contract_id,
            "tick_size": self.config.tick_size,
        }
        payload = _canonical_json(value) + b"\n"
        with destination.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

    def _complete_active_run_marker(self, status: dict[str, object]) -> None:
        if not self._active_run_path.is_file():
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_RUN_MARKER_MISSING")
        completed_root = self._active_run_path.parent / "completed"
        completed_root.mkdir(parents=True, exist_ok=True)
        marker = self._active_run_path.read_bytes()
        marker_digest = hashlib.sha256(marker).hexdigest()
        value = {
            "schema": _RUN_STATE_SCHEMA,
            "state": "COMPLETE",
            "active_marker_sha256": marker_digest,
            "closed_at": self._closed_at,
            "proof_valid": status["proof_valid"],
            "submitted": status["submitted"],
            "processed": status["processed"],
            "sealed_segments": status["sealed_segments"],
            "committed_segments": status["committed_segments"],
            "last_local_sequence": status["last_local_sequence"],
            "segment_head": status["segment_head"],
            "commitment_head": status["commitment_head"],
            "context": status["context"],
        }
        payload = _canonical_json(value) + b"\n"
        digest = hashlib.sha256(payload).hexdigest()
        destination = completed_root / f"{digest}.json"
        if destination.exists():
            raise LSCShadowRuntimeError("LSC_SHADOW_COMPLETION_MARKER_COLLISION")
        with destination.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        archived_root = self._active_run_path.parent / "archived-active"
        archived_root.mkdir(parents=True, exist_ok=True)
        archived = archived_root / f"{marker_digest}.json"
        if archived.exists():
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_MARKER_ARCHIVE_COLLISION")
        os.replace(self._active_run_path, archived)
        self._completion_receipt = destination

    @classmethod
    def recover_unclean(cls, config: LSCShadowConfig) -> dict[str, object]:
        """Preserve an unclean run as failed and recover its sealed prefix.

        This is an explicit administrative transition, never an automatic
        success path.  The recovered prefix is verified and any sealed but
        uncommitted segment is committed.  The prior active latch and a
        durable recovery receipt remain immutable evidence; the receipt keeps
        ``proof_valid`` false because an in-memory tail may have been lost.
        A new runtime instance may start only after this method succeeds.
        """
        runtime = cls(config)
        active = runtime._active_run_path
        if not active.is_file():
            raise LSCShadowRuntimeError("LSC_SHADOW_UNCLEAN_RUN_MARKER_NOT_FOUND")
        marker_bytes = active.read_bytes()
        try:
            marker = json.loads(marker_bytes)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_RUN_MARKER_INVALID") from exc
        expected = {
            "schema": _RUN_STATE_SCHEMA,
            "state": "ACTIVE",
            "root": str(config.root.resolve()),
            "journal_id": config.journal_id,
            "profile_id": config.profile_id,
            "native_contract_id": config.native_contract_id,
            "canonical_contract_id": config.canonical_contract_id,
            "tick_size": config.tick_size,
        }
        if not isinstance(marker, dict) or any(marker.get(key) != value for key, value in expected.items()):
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_RUN_IDENTITY_MISMATCH")
        marker_pid = marker.get("pid")
        liveness = _process_liveness(marker_pid)  # type: ignore[arg-type]
        if liveness is True:
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_WRITER_PROCESS_ALIVE")
        if liveness is None:
            raise LSCShadowRuntimeError("LSC_SHADOW_ACTIVE_WRITER_PROCESS_STATE_UNKNOWN")

        store = SegmentStore(
            config.root,
            limits=config.segment_limits,
            wall_clock=runtime._wall_clock,
            monotonic=runtime._monotonic,
        )
        runtime._store = store
        store_recovery = store.recover()
        chain = store.verify_chain()
        committed_before = 0
        recovered_commitments: list[str] = []
        commitment_head = None
        if chain:
            context_bytes = store.read_context(chain[0].segment_id)
            context = decode_context(context_bytes)
            if any(item.context_digest != chain[0].context_digest for item in chain):
                raise LSCShadowRuntimeError("LSC_SHADOW_MULTIPLE_CONTEXTS_REFUSED")
            runtime._bind_context(context, context_bytes)
            assert runtime._commitments is not None
            commitment_recovery = runtime._commitments.recover()
            committed_before = len(commitment_recovery.committed_segment_ids)
            for segment_id in commitment_recovery.uncommitted_segment_ids:
                runtime._commitments.commit_segment(segment_id)
                recovered_commitments.append(segment_id)
            verified_commitments = runtime._commitments.verify()
            if len(verified_commitments) != len(chain):
                raise LSCShadowRuntimeError("LSC_SHADOW_RECOVERY_COMMITMENT_COUNT_MISMATCH")
            if verified_commitments:
                commitment_head = {
                    "journal_hash": verified_commitments[-1].journal_hash,
                    "journal_sequence": verified_commitments[-1].journal_sequence,
                    "identity_digest": verified_commitments[-1].identity_digest,
                    "segment_id": verified_commitments[-1].segment_id,
                }

        marker_digest = hashlib.sha256(marker_bytes).hexdigest()
        receipt = {
            "schema": _RUN_STATE_SCHEMA,
            "state": "RECOVERED_UNCLEAN_AS_FAILED",
            "recorded_at": runtime._wall_clock(),
            "proof_valid": False,
            "failure_reason": "LSC_SHADOW_UNCLEAN_PREVIOUS_RUN",
            "possible_unsealed_tail_loss": True,
            "active_marker_sha256": marker_digest,
            "prior_process_id": marker_pid,
            "prior_process_confirmed_absent": True,
            "root": str(config.root.resolve()),
            "journal_id": config.journal_id,
            "profile_id": config.profile_id,
            "native_contract_id": config.native_contract_id,
            "canonical_contract_id": config.canonical_contract_id,
            "tick_size": config.tick_size,
            "sealed_segments": len(chain),
            "committed_segments_before": committed_before,
            "recovered_commitment_segment_ids": recovered_commitments,
            "committed_segments_after": len(chain),
            "quarantined_temporaries": [
                {
                    "source_name": item.source_name,
                    "quarantine_path": str(item.quarantine_path),
                    "sha256": item.sha256,
                    "reason": item.reason,
                }
                for item in store_recovery.quarantined
            ],
            "segment_head": None if not chain else {
                "segment_id": chain[-1].segment_id,
                "segment_commitment": chain[-1].segment_commitment,
                "ordinal": chain[-1].ordinal,
            },
            "commitment_head": commitment_head,
        }
        recovery_root = active.parent / "unclean-recovery"
        recovery_root.mkdir(parents=True, exist_ok=True)
        destination = recovery_root / f"{marker_digest}.json"
        payload = _canonical_json(receipt) + b"\n"
        if destination.exists():
            existing = json.loads(destination.read_text(encoding="utf-8"))
            if not isinstance(existing, dict) or existing.get("active_marker_sha256") != marker_digest:
                raise LSCShadowRuntimeError("LSC_SHADOW_UNCLEAN_RECOVERY_RECEIPT_COLLISION")
            receipt = existing
        else:
            with destination.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        failed_root = active.parent / "failed-active"
        failed_root.mkdir(parents=True, exist_ok=True)
        archived = failed_root / f"{marker_digest}.json"
        if archived.exists():
            raise LSCShadowRuntimeError("LSC_SHADOW_FAILED_ACTIVE_MARKER_COLLISION")
        os.replace(active, archived)
        return {**receipt, "recovery_receipt": str(destination), "archived_active_marker": str(archived)}

    def start(self) -> dict[str, object]:
        with self._state_lock:
            if self._worker is not None or self._accepting or self._closed:
                raise LSCShadowRuntimeError("LSC_SHADOW_RUNTIME_ALREADY_STARTED")
            self.config.root.mkdir(parents=True, exist_ok=True)
            if self._active_run_path.exists():
                raise LSCShadowRuntimeError("LSC_SHADOW_UNCLEAN_PREVIOUS_RUN")
            self._store = SegmentStore(
                self.config.root,
                limits=self.config.segment_limits,
                wall_clock=self._wall_clock,
                monotonic=self._monotonic,
            )
            recovery = self._store.recover()
            chain = self._store.verify_chain()
            if chain:
                context_bytes = self._store.read_context(chain[0].segment_id)
                context = decode_context(context_bytes)
                if any(item.context_digest != chain[0].context_digest for item in chain):
                    raise LSCShadowRuntimeError("LSC_SHADOW_MULTIPLE_CONTEXTS_REFUSED")
                self._bind_context(context, context_bytes)
                assert self._commitments is not None
                commitment_recovery = self._commitments.recover()
                for segment_id in commitment_recovery.uncommitted_segment_ids:
                    self._commitments.commit_segment(segment_id)
                self._sealed_segments = len(chain)
                self._committed_segments = len(self._commitments.verify())
                self._last_local_sequence = chain[-1].last_local_sequence
            self._started_at = self._wall_clock()
            self._write_active_run_marker()
            self._worker = threading.Thread(
                target=self._run,
                name="beelzebub-lsc-shadow-writer",
                daemon=True,
            )
            self._worker.start()
            self._accepting = True
            return {
                "started": True,
                "recovered_segments": len(recovery.sealed_segment_ids),
                "quarantined_temporaries": len(recovery.quarantined),
                "proof_valid": False,
            }

    def submit(self, observation: NinjaTraderObservation) -> bool:
        """Offer one admitted observation without codec, storage, or wait work."""
        with self._state_lock:
            if not isinstance(observation, NinjaTraderObservation):
                return False
            if observation.observation_type not in _PASSIVE_KINDS:
                self._non_passive_ignored += 1
                return False
            if (
                not self._accepting
                or self._closing
                or self._closed
                or self._failure_reason is not None
            ):
                self._rejected_after_failure += 1
                return False
            item = _QueuedObservation(observation, self._monotonic())
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self._overflow += 1
                self._request_failure("LSC_SHADOW_QUEUE_FULL", None)
                return False
            self._submitted += 1
            size = self._queue.qsize()
            if size > self._queue_high_water:
                self._queue_high_water = size
            return True

    def _request_failure(self, reason: str, detail: str | None) -> None:
        # Assignment is intentionally the only listener-thread state change.
        # The worker performs any durable reporting after observing this latch.
        with self._state_lock:
            if self._failure_reason is None:
                self._failure_reason = reason
                self._failure_detail = detail
            self._accepting = False

    def record_callback_failure(self, error: BaseException) -> None:
        """Latch an unexpected listener-side adapter failure without I/O."""
        self._request_failure(
            "LSC_SHADOW_CALLBACK_FAILED",
            f"{type(error).__name__}: {error}",
        )

    def _context_for(self, observation: NinjaTraderObservation) -> LSCContext:
        if observation.schema != L3F3_SCHEMA or observation.signature_verified is not True:
            raise LSCShadowRuntimeError("LSC_SHADOW_REQUIRES_VERIFIED_L3F3")
        producer = observation.producer
        if producer is None:
            raise LSCShadowRuntimeError("LSC_SHADOW_PRODUCER_IDENTITY_MISSING")
        generation = observation.payload.get("source_generation")
        native_contract = observation.payload.get("contract_id")
        if type(generation) is not int or generation < 0:
            raise LSCShadowRuntimeError("LSC_SHADOW_SOURCE_GENERATION_INVALID")
        if not isinstance(native_contract, str) or not native_contract:
            raise LSCShadowRuntimeError("LSC_SHADOW_NATIVE_CONTRACT_INVALID")
        if native_contract != self.config.native_contract_id:
            raise LSCShadowRuntimeError("LSC_SHADOW_NATIVE_CONTRACT_MISMATCH")
        return LSCContext(
            observation_schema=observation.schema,
            source=observation.source,
            provider=observation.provider,
            environment=observation.environment,
            producer_protocol_version=producer.protocol_version,
            producer_source_fingerprint=producer.source_fingerprint,
            producer_build_fingerprint=producer.build_fingerprint,
            producer_build_timestamp=producer.build_timestamp,
            native_contract_id=native_contract,
            canonical_contract_id=self.config.canonical_contract_id,
            tick_size=self.config.tick_size,
            session_id=observation.session_id,
            generation=generation,
            timestamp_origin=observation.ninja_receipt_time,
        )

    def _bind_context(self, context: LSCContext, context_bytes: bytes) -> None:
        if context.canonical_contract_id != self.config.canonical_contract_id:
            raise LSCShadowRuntimeError("LSC_SHADOW_CANONICAL_CONTRACT_MISMATCH")
        if context.native_contract_id != self.config.native_contract_id:
            raise LSCShadowRuntimeError("LSC_SHADOW_NATIVE_CONTRACT_MISMATCH")
        if context.tick_size != self.config.tick_size:
            raise LSCShadowRuntimeError("LSC_SHADOW_TICK_SIZE_MISMATCH")
        store = self._store
        if store is None:
            raise LSCShadowRuntimeError("LSC_SHADOW_STORE_NOT_STARTED")
        digest = context_digest(context_bytes)
        journal_root = store.root / "shadow-commitments"
        identity = ShadowIdentityContext(
            journal_id=self.config.journal_id,
            profile_id=self.config.profile_id,
            context_digest=digest,
            observation_schema=context.observation_schema,
            source=context.source,
            provider=context.provider,
            environment=context.environment,
            producer_protocol_version=context.producer_protocol_version,
            producer_source_fingerprint=context.producer_source_fingerprint,
            producer_build_fingerprint=context.producer_build_fingerprint,
            producer_build_timestamp=context.producer_build_timestamp,
            native_contract_id=context.native_contract_id,
            canonical_contract_id=context.canonical_contract_id,
            tick_size=context.tick_size,
            session_id=context.session_id,
            generation=context.generation,
            legacy_mode=context.legacy_mode,
            format_identity=LSC_FORMAT_IDENTITY,
            storage_identity=canonical_storage_identity(store, journal_root),
        )
        self._commitments = ShadowCommitmentLog(
            store, identity, root=journal_root, wall_clock=self._wall_clock,
        )
        self._context = context
        self._context_bytes = context_bytes

    def _rotate(self) -> None:
        builder = self._builder
        if builder is None or builder.record_count == 0:
            return
        commitments = self._commitments
        if commitments is None:
            raise LSCShadowRuntimeError("LSC_SHADOW_COMMITMENT_LOG_UNBOUND")
        sealed = builder.seal()
        self._sealed_segments += 1
        commitments.commit_segment(sealed.segment_id)
        self._committed_segments += 1
        self._builder = None
        self._builder_opened_monotonic = None

    def _append(self, item: _QueuedObservation) -> None:
        observation = item.observation
        if self._context is None:
            context = self._context_for(observation)
            context_bytes = encode_context(context)
            self._bind_context(context, context_bytes)
        context_bytes = self._context_bytes
        store = self._store
        if context_bytes is None or store is None:
            raise LSCShadowRuntimeError("LSC_SHADOW_CONTEXT_NOT_BOUND")
        if (
            self._last_local_sequence is not None
            and observation.local_monotonic_sequence <= self._last_local_sequence
        ):
            raise LSCShadowRuntimeError("LSC_SHADOW_NON_MONOTONIC_LOCAL_SEQUENCE")
        encoded = encode_observation(observation, context_bytes)
        if self._builder is None:
            self._builder = store.new_segment(context_bytes)
            self._builder_opened_monotonic = self._monotonic()
        try:
            self._builder.append(encoded)
        except LSCStoreLimitError as exc:
            reason = str(exc)
            if self._builder.record_count == 0 or not any(
                marker in reason for marker in ("AGE_LIMIT", "RECORD_LIMIT", "RAW_BODY_LIMIT")
            ):
                raise
            self._rotate()
            self._builder = store.new_segment(context_bytes)
            self._builder_opened_monotonic = self._monotonic()
            self._builder.append(encoded)
        self._last_local_sequence = observation.local_monotonic_sequence
        self._processed += 1

    def _write_failure_receipt(self) -> None:
        if self._failure_receipt is not None or self._store is None:
            return
        root = self._store.root / "runtime-failures"
        root.mkdir(parents=True, exist_ok=True)
        value = {
            "schema": _RECEIPT_SCHEMA,
            "recorded_at": self._wall_clock(),
            "proof_valid": False,
            "failure_reason": self._failure_reason,
            "failure_detail": self._failure_detail,
            "root": str(self.config.root.resolve()),
            "journal_id": self.config.journal_id,
            "profile_id": self.config.profile_id,
            "native_contract_id": self.config.native_contract_id,
            "canonical_contract_id": self.config.canonical_contract_id,
            "context": None if self._context is None else asdict(self._context),
            "last_local_sequence": self._last_local_sequence,
            "submitted": self._submitted,
            "processed": self._processed,
            "queue_depth": self._queue.qsize(),
            "queue_high_water": self._queue_high_water,
            "overflow": self._overflow,
        }
        payload = _canonical_json(value) + b"\n"
        digest = hashlib.sha256(payload).hexdigest()
        destination = root / f"{digest}.json"
        temporary = root / f".{digest}.{uuid4().hex}.tmp"
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        self._failure_receipt = destination

    def _run(self) -> None:
        try:
            while True:
                if self._failure_reason is not None:
                    self._write_failure_receipt()
                    return
                try:
                    item = self._queue.get(timeout=0.05)
                except queue.Empty:
                    if self._failure_reason is not None:
                        self._write_failure_receipt()
                        return
                    if self._closing and self._queue.empty():
                        self._rotate()
                        return
                    builder = self._builder
                    if (
                        builder is not None
                        and builder.record_count
                        and self._builder_opened_monotonic is not None
                        and self._monotonic() - self._builder_opened_monotonic
                        >= self.config.segment_limits.max_segment_age_seconds
                    ):
                        self._rotate()
                    continue
                try:
                    residence = max(0.0, self._monotonic() - item.enqueued_monotonic)
                    self._maximum_residence_seconds = max(
                        self._maximum_residence_seconds, residence,
                    )
                    if residence > self.config.maximum_queue_residence_seconds:
                        raise LSCShadowRuntimeError(
                            "LSC_SHADOW_QUEUE_RESIDENCE_EXCEEDED"
                        )
                    self._append(item)
                finally:
                    self._queue.task_done()
        except Exception as exc:
            self._request_failure(
                "LSC_SHADOW_WORKER_FAILED",
                f"{type(exc).__name__}: {exc}",
            )
            try:
                self._write_failure_receipt()
            except Exception:
                pass

    def close(self, *, timeout_seconds: float = 30.0) -> dict[str, object]:
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        with self._state_lock:
            if self._terminal_status is not None:
                return dict(self._terminal_status)
            worker = self._worker
            if worker is None:
                raise LSCShadowRuntimeError("LSC_SHADOW_RUNTIME_NOT_STARTED")
            self._accepting = False
            self._closing = True
        worker.join(float(timeout_seconds))
        if worker.is_alive():
            self._request_failure("LSC_SHADOW_DRAIN_TIMEOUT", None)
        with self._state_lock:
            self._closed = not worker.is_alive()
            self._closed_at = self._wall_clock()
        status = self.status()
        if status["proof_valid"] is True:
            try:
                self._complete_active_run_marker(status)
                status = self.status()
            except Exception as exc:
                self._request_failure(
                    "LSC_SHADOW_COMPLETION_MARKER_FAILED",
                    f"{type(exc).__name__}: {exc}",
                )
                try:
                    self._write_failure_receipt()
                except Exception:
                    pass
                status = self.status()
        if self._closed:
            self._terminal_status = dict(status)
        return status

    def status(self) -> dict[str, object]:
        commitments = self._commitments
        committed = self._committed_segments
        chain_verified = False
        commitments_verified = False
        segment_head = None
        commitment_head = None
        if self._closed and self._failure_reason is None and self._store is not None:
            chain = self._store.verify_chain()
            verified_commitments = () if commitments is None else commitments.verify()
            chain_verified = len(chain) == self._sealed_segments
            commitments_verified = len(verified_commitments) == committed == self._sealed_segments
            if chain:
                segment_head = {
                    "segment_id": chain[-1].segment_id,
                    "segment_commitment": chain[-1].segment_commitment,
                    "ordinal": chain[-1].ordinal,
                }
            if verified_commitments:
                commitment_head = {
                    "journal_hash": verified_commitments[-1].journal_hash,
                    "journal_sequence": verified_commitments[-1].journal_sequence,
                    "identity_digest": verified_commitments[-1].identity_digest,
                    "segment_id": verified_commitments[-1].segment_id,
                }
        proof_valid = bool(
            self._closed
            and self._failure_reason is None
            and self._queue.empty()
            and self._submitted == self._processed
            and chain_verified
            and commitments_verified
        )
        context = None if self._context is None else asdict(self._context)
        return {
            "schema": _RECEIPT_SCHEMA,
            "root": str(self.config.root.resolve()),
            "started_at": self._started_at,
            "closed_at": self._closed_at,
            "accepting": self._accepting,
            "closing": self._closing,
            "closed": self._closed,
            "proof_valid": proof_valid,
            "failure_reason": self._failure_reason,
            "failure_detail": self._failure_detail,
            "failure_receipt": None if self._failure_receipt is None else str(self._failure_receipt),
            "completion_receipt": (
                None if self._completion_receipt is None else str(self._completion_receipt)
            ),
            "submitted": self._submitted,
            "processed": self._processed,
            "non_passive_ignored": self._non_passive_ignored,
            "overflow": self._overflow,
            "rejected_after_failure": self._rejected_after_failure,
            "queue_depth": self._queue.qsize(),
            "queue_capacity": self.config.queue_capacity,
            "queue_high_water": self._queue_high_water,
            "maximum_queue_residence_seconds": self._maximum_residence_seconds,
            "maximum_allowed_queue_residence_seconds": (
                self.config.maximum_queue_residence_seconds
            ),
            "sealed_segments": self._sealed_segments,
            "committed_segments": committed,
            "last_local_sequence": self._last_local_sequence,
            "context": context,
            "segment_head": segment_head,
            "commitment_head": commitment_head,
            "chain_verified": chain_verified,
            "commitments_verified": commitments_verified,
        }
