"""Offline-only, single-owner periodic experiment around the frozen Pong host.

Nothing in this module starts the host, attaches a bridge, or grants authority.
The caller applies all records available at a cutoff in capture order before
calling ``tick``; equal receipt timestamps precede that tick. A late tick uses
the supplied *current* cutoff, never an obsolete scheduled cutoff. The frozen
host calculation runs under the same lock as the immutable input capture.

Analytical switches also drive exits and entry cancellation in production.
Keeping native/control callbacks immediate does NOT prove those outcomes are
unchanged. This module is a fidelity experiment, not a runtime-ready scheduler.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading
import time
from typing import Callable

from .pong_microstructure import BookSide
from .pong_production import PongProductionHost


PROTECTION_EQUIVALENCE_LIMITATION = (
    "UNPROVEN: analytical switches drive opposite-signal exits, partial-entry "
    "cancellation and invalid-snapshot safety exits; immediate offline control "
    "callbacks do not establish unchanged protective outcomes."
)


def _utc(value: datetime | str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise ValueError("PONG_PERIODIC_AWARE_CUTOFF_REQUIRED")
    return result.astimezone(timezone.utc)


def _wire(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class PongInputView:
    """Complete retained analytical input, not just the latest book snapshot.

    Event/feature/policy objects in these tuples are frozen repository types.
    The state owner holds the lock until the unchanged calculation completes;
    no ingestion or control callback can change this view during a scan.
    """

    epoch: tuple
    evaluation_sequence: int
    watermark: tuple[tuple[str, int], ...]
    input_record_count: int
    available_cutoff: str
    window_start: str
    trades: tuple
    bids: tuple
    asks: tuple
    volume_buckets: tuple[tuple[str, Decimal], ...]
    prior_volume: tuple
    policies: tuple
    input_error: str | None
    quote_identity: tuple | None


@dataclass(frozen=True)
class PeriodicEvaluation:
    view: PongInputView
    scheduled_deadline_ns: int
    started_ns: int
    lateness_ns: int
    missed_deadlines: int
    snapshot_cpu_ns: int
    snapshot_wall_ns: int
    evaluation_cpu_ns: int
    evaluation_wall_ns: int
    lock_hold_ns: int
    blockers: tuple[str, ...]
    evaluated: bool
    expiry_maintenance: bool


class OfflinePeriodicPong:
    """Opt-in timer mode; interval 0 preserves event-driven host behavior.

    Set ``origin_ns`` to the producer's monotonic origin for paced replay and
    use zero for virtual time. ``phase_ns`` is in [0, interval). The first tick
    is origin+phase; callers may deliberately omit it when the capture starts
    before the first scheduled evaluation. All calls belong to one thread.
    """

    def __init__(
        self, host: PongProductionHost, *, interval_ms: int = 0,
        phase_ns: int = 0, origin_ns: int = 0,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        if type(interval_ms) is not int or interval_ms < 0:
            raise ValueError("PONG_PERIODIC_INTERVAL_INVALID")
        if type(origin_ns) is not int or type(phase_ns) is not int or phase_ns < 0:
            raise ValueError("PONG_PERIODIC_ORIGIN_OR_PHASE_INVALID")
        self.interval_ns = interval_ms * 1_000_000
        if (self.interval_ns and phase_ns >= self.interval_ns) or (not self.interval_ns and phase_ns):
            raise ValueError("PONG_PERIODIC_PHASE_INVALID")
        self.host = host
        self.clock_ns = clock_ns
        self.next_deadline_ns = origin_ns + phase_ns if self.interval_ns else None
        self.last_view: PongInputView | None = None
        self.last_evaluation: PeriodicEvaluation | None = None
        self._owner = threading.get_ident()
        self._busy = False
        self._last_now_ns: int | None = None
        self._latest_receipt: datetime | None = None
        self._record_count = 0
        self._evaluation_count = 0
        self._tick_count = 0
        self._fault_ticks = 0
        self._missed = 0
        self._late_starts = 0
        self._overruns = 0
        self._overrun_streak = 0
        self._maximum_overrun_streak = 0
        self._snapshot_cpu_ns = 0
        self._snapshot_wall_ns = 0
        self._evaluation_cpu_ns = 0
        self._evaluation_wall_ns = 0
        self._lock_hold_ns = 0
        self._control_callbacks = 0
        self._expiry_maintenance = 0
        self._ensure_offline()

    def _ensure_offline(self) -> None:
        if threading.get_ident() != self._owner:
            raise RuntimeError("PONG_PERIODIC_SINGLE_OWNER_REQUIRED")
        if (self.host.activation_authorized or self.host._bridge is not None
                or self.host._thread is not None or self.host._controller is not None
                or self.host._compute_pool is not None or self.host._compute_workers
                or self.host._compute_pending or self.host._pending_native):
            raise RuntimeError("PONG_PERIODIC_OFFLINE_DISARMED_HOST_REQUIRED")
        if self._busy:
            raise RuntimeError("PONG_PERIODIC_REENTRANT_EVALUATION_REFUSED")

    def consume(self, record) -> None:
        """Apply every envelope, sequence transition, depth mutation and trade."""
        self._ensure_offline()
        receipt = _utc(record.ninja_receipt_time)
        self._latest_receipt = max(receipt, self._latest_receipt or receipt)
        self._record_count += 1
        if not self.interval_ns:
            self.host._consume_observation(record)
            return
        with self.host._lock:
            self.host._process_observation(record)

    def service_control(self, callback: Callable, *args, **kwargs):
        """Run a detached offline safety/control fixture immediately.

        This is intentionally independent of whether an analytical tick is
        due. Only detached callbacks are supported; a production bridge is
        refused before every call. It is not proof of analytical exit parity.
        """
        self._ensure_offline()
        self._control_callbacks += 1
        return callback(*args, **kwargs)

    def latch_fault(self, reason: str) -> None:
        self._ensure_offline()
        if not isinstance(reason, str) or not reason:
            raise ValueError("PONG_PERIODIC_FAULT_REASON_REQUIRED")
        with self.host._lock:
            if self.host._input_error is None:
                self.host._input_error = reason
            self.host._microstructure_blockers = (self.host._input_error,)

    def _capture(self, at: datetime) -> PongInputView:
        host = self.host
        bucket = at.replace(minute=(at.minute // 5) * 5, second=0, microsecond=0)
        prior = () if host._volume_history is None else tuple(host._volume_history.prior(bucket))
        quote = host._last_quote_record
        return PongInputView(
            epoch=(host._compute_generation, host._source_generation, host._producer),
            evaluation_sequence=self._tick_count,
            watermark=tuple(sorted(host._stream_sequences.items())),
            input_record_count=self._record_count,
            available_cutoff=_wire(at), window_start=_wire(at - timedelta(seconds=5)),
            trades=tuple(host._trades), bids=tuple(host._depth[BookSide.BID]),
            asks=tuple(host._depth[BookSide.ASK]),
            volume_buckets=tuple(sorted(host._volume_buckets.items())),
            prior_volume=prior,
            policies=(host._flow.policy, host._liquidity.policy, host._volume.policy, host._mapper.policy),
            input_error=host._input_error,
            quote_identity=None if quote is None else (quote.observation_id, quote.payload_hash, quote.ninja_receipt_time),
        )

    def tick(self, now_ns: int, as_of: datetime | str) -> PeriodicEvaluation | None:
        self._ensure_offline()
        if type(now_ns) is not int:
            raise ValueError("PONG_PERIODIC_MONOTONIC_NS_REQUIRED")
        if self._last_now_ns is not None and now_ns < self._last_now_ns:
            raise ValueError("PONG_PERIODIC_MONOTONIC_CLOCK_REGRESSED")
        self._last_now_ns = now_ns
        if not self.interval_ns or now_ns < self.next_deadline_ns:
            return None
        at = _utc(as_of)
        if self._latest_receipt is not None and self._latest_receipt > at:
            self.latch_fault("PONG_PERIODIC_INPUT_AFTER_CUTOFF")
            raise ValueError("PONG_PERIODIC_INPUT_AFTER_CUTOFF")
        if self.last_view is not None and at < _utc(self.last_view.available_cutoff):
            self.latch_fault("PONG_PERIODIC_CUTOFF_REGRESSED")
            raise ValueError("PONG_PERIODIC_CUTOFF_REGRESSED")
        deadline = self.next_deadline_ns
        lateness = now_ns - deadline
        missed = lateness // self.interval_ns
        # No backlog of timer jobs. The next deadline remains on the absolute
        # origin grid, rather than sleeping an interval after this scan ends.
        self.next_deadline_ns = deadline + (missed + 1) * self.interval_ns
        self._missed += missed
        self._late_starts += int(lateness > 0)
        self._tick_count += 1
        self._busy = True
        try:
            with self.host._lock:
                lock_started = time.perf_counter_ns()
                snap_cpu = time.thread_time_ns()
                snap_wall = time.perf_counter_ns()
                view = self._capture(at)
                snapshot_wall = time.perf_counter_ns() - snap_wall
                snapshot_cpu = time.thread_time_ns() - snap_cpu
                self.last_view = view
                eval_cpu = time.thread_time_ns()
                eval_wall = time.perf_counter_ns()
                evaluated = self.host._input_error is None
                expiry_maintenance = False
                if evaluated:
                    # The original implementation consumes exactly the state
                    # captured above: same ordered owner and held RLock.
                    self.host._evaluate_microstructure(at)
                    self._evaluation_count += 1
                else:
                    self.host._microstructure_blockers = (self.host._input_error,)
                    self._fault_ticks += 1
                # An incomplete/blocked frame does not replace the existing
                # Discernment snapshot. Still expire its old evidence at this
                # clock boundary; maintenance cannot manufacture a switch.
                if (self.host._microstructure_blockers
                        and self.host._discernment.last_snapshot is not None):
                    update = self.host._discernment.on_expiry(_wire(at))
                    self.host._apply_discernment_update(update)
                    expiry_maintenance = True
                    self._expiry_maintenance += 1
                evaluation_wall = time.perf_counter_ns() - eval_wall
                evaluation_cpu = time.thread_time_ns() - eval_cpu
                blockers = tuple(self.host._microstructure_blockers)
                lock_hold = time.perf_counter_ns() - lock_started
        finally:
            self._busy = False
        self._snapshot_cpu_ns += snapshot_cpu
        self._snapshot_wall_ns += snapshot_wall
        self._evaluation_cpu_ns += evaluation_cpu
        self._evaluation_wall_ns += evaluation_wall
        self._lock_hold_ns += lock_hold
        overrun = lock_hold > self.interval_ns
        self._overruns += int(overrun)
        self._overrun_streak = self._overrun_streak + 1 if overrun else 0
        self._maximum_overrun_streak = max(self._maximum_overrun_streak, self._overrun_streak)
        result = PeriodicEvaluation(view, deadline, now_ns, lateness, missed,
                                    snapshot_cpu, snapshot_wall, evaluation_cpu,
                                    evaluation_wall, lock_hold, blockers, evaluated,
                                    expiry_maintenance)
        self.last_evaluation = result
        return result

    def metrics(self) -> dict[str, object]:
        return {
            "mode": "EVENT_DRIVEN" if not self.interval_ns else "PERIODIC_OFFLINE",
            "interval_ms": self.interval_ns / 1_000_000,
            "records_consumed": self._record_count,
            "ticks": self._tick_count,
            "evaluations": self._evaluation_count if self.interval_ns else None,
            "fault_ticks": self._fault_ticks,
            "missed_ticks": self._missed,
            "late_starts": self._late_starts,
            "service_overruns": self._overruns,
            "maximum_consecutive_service_overruns": self._maximum_overrun_streak,
            "persistent_overrun_failed": self._maximum_overrun_streak >= 3,
            "snapshot_cpu_ns": self._snapshot_cpu_ns,
            "snapshot_wall_ns": self._snapshot_wall_ns,
            "evaluation_cpu_ns": self._evaluation_cpu_ns,
            "evaluation_wall_ns": self._evaluation_wall_ns,
            "lock_hold_ns": self._lock_hold_ns,
            "immediate_control_callbacks": self._control_callbacks,
            "expiry_maintenance_callbacks": self._expiry_maintenance,
            "same_timestamp_ordering": "capture-order records with receipt <= cutoff before tick",
            "snapshot_consistency": "single ordered owner; host RLock held through immutable capture and unchanged calculation",
            "protection_equivalence": PROTECTION_EQUIVALENCE_LIMITATION,
        }
