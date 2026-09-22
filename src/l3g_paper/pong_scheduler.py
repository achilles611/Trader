"""Bounded QPC cadence accounting shared by Pong production integration.

The scheduler owns local service deadlines only.  It deliberately has no market
timestamps, strategy state, bridge, or execution capability: callers supply a
causally valid input prefix and decide whether the protected path supersedes a
flat-mode timer opportunity.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Callable


class PongSchedulingMode(StrEnum):
    """Explicit scheduling selection for the one Pong paper profile."""

    EVENT_DRIVEN = "EVENT_DRIVEN"
    PERIODIC_FLAT_PROTECTED = "PERIODIC_FLAT_PROTECTED"


_PERIODIC_INTERVALS_MS = frozenset((10, 20, 25))
PONG_SCHEDULING_MODE_ENV = "BEELZEBUB_PONG_SCHEDULING_MODE"
PONG_PERIODIC_INTERVAL_ENV = "BEELZEBUB_PONG_PERIODIC_INTERVAL_MS"


@dataclass(frozen=True)
class PongSchedulingConfig:
    """Validated, visible scheduling configuration; never execution authority."""

    mode: PongSchedulingMode = PongSchedulingMode.EVENT_DRIVEN
    interval_ms: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.mode, PongSchedulingMode):
            raise ValueError("PONG_SCHEDULING_MODE_INVALID")
        if type(self.interval_ms) is not int:
            raise ValueError("PONG_SCHEDULING_INTERVAL_TYPE_INVALID")
        if self.mode is PongSchedulingMode.EVENT_DRIVEN and self.interval_ms != 0:
            raise ValueError("PONG_EVENT_DRIVEN_INTERVAL_MUST_BE_ZERO")
        if (
            self.mode is PongSchedulingMode.PERIODIC_FLAT_PROTECTED
            and self.interval_ms not in _PERIODIC_INTERVALS_MS
        ):
            raise ValueError("PONG_PERIODIC_INTERVAL_NOT_APPROVED")

    @property
    def enabled(self) -> bool:
        return self.mode is PongSchedulingMode.PERIODIC_FLAT_PROTECTED


def scheduling_config_from_environment(
    mode_value: str | None,
    interval_value: str | None,
) -> PongSchedulingConfig:
    """Decode explicit launcher values without accepting aliases or defaults."""
    if mode_value is None and interval_value is None:
        return PongSchedulingConfig()
    if mode_value is None or interval_value is None:
        raise ValueError("PONG_SCHEDULING_ENVIRONMENT_INCOMPLETE")
    try:
        mode = PongSchedulingMode(mode_value)
    except ValueError as error:
        raise ValueError("PONG_SCHEDULING_MODE_INVALID") from error
    if not interval_value.isascii() or not interval_value.isdecimal():
        raise ValueError("PONG_SCHEDULING_INTERVAL_INVALID")
    return PongSchedulingConfig(mode=mode, interval_ms=int(interval_value))


@dataclass(frozen=True)
class PeriodicDue:
    """One current opportunity on the fixed QPC grid."""

    ordinal: int
    scheduled_deadline_ns: int
    started_ns: int
    missed_obsolete: int

    @property
    def lateness_ns(self) -> int:
        return self.started_ns - self.scheduled_deadline_ns


class PongPeriodicDeadline:
    """One-owner absolute deadline grid with explicit outcome accounting."""

    def __init__(
        self,
        config: PongSchedulingConfig,
        *,
        clock_ns: Callable[[], int],
    ) -> None:
        if type(config) is not PongSchedulingConfig or not callable(clock_ns):
            raise ValueError("PONG_SCHEDULER_CONTRACT_INVALID")
        self.config = config
        self._clock_ns = clock_ns
        origin = clock_ns()
        if type(origin) is not int or origin < 0:
            raise ValueError("PONG_SCHEDULER_CLOCK_INVALID")
        self.origin_ns = origin
        self.interval_ns = config.interval_ms * 1_000_000
        self._next_ordinal = 1
        self._last_now_ns = origin
        self._scheduled = 0
        self._dispatched = 0
        self._completed = 0
        self._applied = 0
        self._missed_obsolete = 0
        self._late = 0
        self._on_time = 0
        self._protection_supersession = 0
        self._intentional_no_work = 0
        self._shutdown = 0
        self._active_due_ordinal: int | None = None
        self._active_due_state: str | None = None

    def _validate_now(self, now_ns: int) -> None:
        if type(now_ns) is not int or now_ns < self._last_now_ns:
            raise ValueError("PONG_SCHEDULER_MONOTONIC_CLOCK_REGRESSED")
        self._last_now_ns = now_ns

    def next_wait_seconds(self, now_ns: int | None = None) -> float | None:
        """Return the QPC-grid wait, never a completion-relative sleep."""
        if not self.config.enabled:
            return None
        current = self._clock_ns() if now_ns is None else now_ns
        self._validate_now(current)
        deadline = self.origin_ns + self._next_ordinal * self.interval_ns
        return max(0.0, (deadline - current) / 1_000_000_000)

    def take_due(self, now_ns: int | None = None) -> PeriodicDue | None:
        """Claim only the current opportunity and account for obsolete ones."""
        if not self.config.enabled:
            return None
        current = self._clock_ns() if now_ns is None else now_ns
        self._validate_now(current)
        deadline = self.origin_ns + self._next_ordinal * self.interval_ns
        if current < deadline:
            return None
        if self._active_due_state in {"CLAIMED", "DISPATCHED"}:
            raise ValueError("PONG_SCHEDULER_OUTSTANDING_DUE")
        first_ordinal = self._next_ordinal
        missed = (current - deadline) // self.interval_ns
        self._scheduled += missed + 1
        self._missed_obsolete += missed
        self._next_ordinal += missed + 1
        if current == deadline:
            self._on_time += 1
        else:
            self._late += 1
        due = PeriodicDue(first_ordinal + missed, deadline, current, missed)
        self._active_due_ordinal = due.ordinal
        self._active_due_state = "CLAIMED"
        return due

    def supersede_for_protection(self, due: PeriodicDue) -> None:
        self._require_state(due, "CLAIMED")
        self._active_due_state = "PROTECTION_SUPERSEDED"
        self._protection_supersession += 1

    def dispatch(self, due: PeriodicDue) -> None:
        """Record transfer of one claimed opportunity to ordinary service."""
        self._require_state(due, "CLAIMED")
        self._active_due_state = "DISPATCHED"
        self._dispatched += 1

    def complete(self, due: PeriodicDue, *, applied: bool, intentional_no_work: bool = False) -> None:
        if type(applied) is not bool or type(intentional_no_work) is not bool:
            raise ValueError("PONG_SCHEDULER_COMPLETION_CONTRACT_INVALID")
        # Keep the public completion API compatible for direct users while
        # production records dispatch explicitly before ordinary work starts.
        state = self._require_state(due, "CLAIMED", "DISPATCHED")
        if state == "CLAIMED":
            self.dispatch(due)
        self._active_due_state = "COMPLETED"
        self._completed += 1
        self._applied += int(applied)
        self._intentional_no_work += int(intentional_no_work)

    def mark_shutdown(self, due: PeriodicDue) -> None:
        self._require_state(due, "CLAIMED")
        self._active_due_state = "SHUTDOWN"
        self._shutdown += 1

    def _require_state(self, due: PeriodicDue, *states: str) -> str:
        if type(due) is not PeriodicDue:
            raise ValueError("PONG_SCHEDULER_DUE_REQUIRED")
        state = (
            self._active_due_state
            if due.ordinal == self._active_due_ordinal
            else None
        )
        if state not in states:
            raise ValueError("PONG_SCHEDULER_DUE_STATE_INVALID")
        return state

    def metrics(self) -> dict[str, int | str | bool | None]:
        terminal = (
            self._dispatched
            + self._missed_obsolete
            + self._protection_supersession
            + self._shutdown
        )
        return {
            "mode": self.config.mode.value,
            "interval_ms": self.config.interval_ms,
            "enabled": self.config.enabled,
            "clock": "perf_counter_ns/QueryPerformanceCounter",
            "origin_qpc_ns": self.origin_ns,
            "scheduled_opportunities": self._scheduled,
            "claimed_current_opportunities": (
                self._scheduled - self._missed_obsolete
            ),
            "dispatched": self._dispatched,
            "completed": self._completed,
            "applied": self._applied,
            "late_opportunities": self._late,
            "on_time_opportunities": self._on_time,
            "obsolete_opportunities": self._missed_obsolete,
            "missed_opportunities": self._missed_obsolete,
            "missed_opportunities_is_alias_of": "obsolete_opportunities",
            # Compatibility names remain aliases, never additional terminal
            # buckets in deadline reconciliation.
            "late_evaluations": self._late,
            "on_time_evaluations": self._on_time,
            "skipped_obsolete_deadlines": self._missed_obsolete,
            "intentional_no_work": self._intentional_no_work,
            "protection_mode_supersessions": self._protection_supersession,
            "shutdown_deadlines": self._shutdown,
            "deadline_reconciliation": terminal == self._scheduled,
            "completion_reconciliation": self._completed <= self._dispatched,
        }
