"""Single-lifespan asynchronous scheduler engine with durable leadership and run leases."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import inspect
import logging
from typing import Any, Mapping
from uuid import uuid4

from .models import EngineState, MissedRunPolicy, RunStatus, TaskBlocked, TaskInvariantFailure, TaskOutcome, iso_utc, parse_utc, stable_key, utc_now
from .registry import TaskRegistry
from .store import OperationsStore
from .triggers import first_occurrence, resolve_occurrences


LOGGER = logging.getLogger("beelzebub.ops_scheduler")


@dataclass(frozen=True)
class SchedulerSettings:
    enabled: bool = True
    default_timezone: str = "America/Denver"
    poll_interval_seconds: float = 0.5
    leader_lease_seconds: float = 15.0
    run_lease_seconds: float = 15.0
    heartbeat_seconds: float = 3.0
    cancellation_grace_seconds: float = 5.0
    max_concurrent_runs: int = 2
    maximum_catch_up_runs: int = 3
    default_max_lateness_seconds: float = 300.0


class SchedulerTaskContext:
    """Tasks can report through this context but never receive direct table access."""

    def __init__(self, engine: "SchedulerEngine", run: Mapping[str, Any]) -> None:
        self._engine = engine
        self._run = dict(run)
        self.run_id = str(run["run_id"])
        self.schedule_id = str(run.get("schedule_id") or "")
        self.configuration = dict(run.get("configuration") or {})
        self.dependencies = engine.dependencies

    def progress(self, current: float, total: float, stage: str, message: str) -> None:
        self._engine.store.heartbeat(self.run_id, owner_id=self._engine.owner_id, lease_seconds=self._engine.settings.run_lease_seconds,
                                     progress_current=current, progress_total=total, stage=stage, message=message)

    def heartbeat(self, stage: str = "HEARTBEAT", message: str = "Task heartbeat.") -> bool:
        return self._engine.store.heartbeat(self.run_id, owner_id=self._engine.owner_id, lease_seconds=self._engine.settings.run_lease_seconds,
                                            stage=stage, message=message)

    def cancellation_requested(self) -> bool:
        return self._engine.store.is_cancellation_requested(self.run_id)

    def notify(self, severity: str, title: str, body: str) -> str:
        return self._engine.store.notify(severity, title, body, run_id=self.run_id, schedule_id=self.schedule_id or None)

    def run_snapshot(self) -> dict[str, Any]:
        return self._engine.store.get_run(self.run_id) or {"run_id": self.run_id}


class SchedulerEngine:
    def __init__(self, store: OperationsStore, registry: TaskRegistry, *, settings: SchedulerSettings | None = None,
                 dependencies: Mapping[str, Any] | None = None, owner_id: str | None = None) -> None:
        self.store = store
        self.registry = registry
        self.settings = settings or SchedulerSettings()
        self.dependencies: dict[str, Any] = dict(dependencies or {})
        self.owner_id = owner_id or f"scheduler-{uuid4().hex}"
        self.state = EngineState.STOPPED
        self.last_heartbeat: str | None = None
        self.last_error: str | None = None
        self._loop_task: asyncio.Task[None] | None = None
        self._active: dict[str, asyncio.Task[None]] = {}
        self._stopping = False

    async def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self.store.initialize()
        self.store.recover_expired()
        self._stopping = False
        if not self.settings.enabled:
            self.state = EngineState.STOPPED
            return
        await self.tick()
        self._loop_task = asyncio.create_task(self._run(), name="beelzebub-operations-scheduler")

    async def stop(self) -> None:
        self._stopping = True
        loop_task, self._loop_task = self._loop_task, None
        if loop_task is not None:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)
        for run_id, task in list(self._active.items()):
            run = self.store.get_run(run_id)
            if run and run["status"] == RunStatus.ACTIVE.value:
                self.store.transition(run_id, RunStatus.INTERRUPTED, stage="SHUTDOWN", message="BeezConsole shutdown interrupted the active task.", error={"reason": "APPLICATION_SHUTDOWN"})
            task.cancel()
        if self._active:
            await asyncio.gather(*self._active.values(), return_exceptions=True)
        self._active.clear()
        self.store.release_leader(self.owner_id)
        self.state = EngineState.STOPPED

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # scheduler degradation cannot kill FastAPI
                self.state = EngineState.DEGRADED
                self.last_error = str(exc)
                LOGGER.exception("Operations scheduler loop degraded")
                self.store.notify("error", "Scheduler engine degraded", "The scheduler loop encountered an error and will retry conservatively.", payload={"error": str(exc)})
            try:
                await asyncio.sleep(max(0.05, self.settings.poll_interval_seconds))
            except asyncio.CancelledError:
                raise

    async def tick(self) -> None:
        """One safe, idempotent scheduler pass; exposed for deterministic tests."""
        self.store.initialize()
        if not self.settings.enabled:
            self.state = EngineState.STOPPED
            return
        leader = self.store.acquire_or_renew_leader(self.owner_id, lease_seconds=self.settings.leader_lease_seconds)
        self.last_heartbeat = iso_utc()
        if not leader:
            self.state = EngineState.STANDBY
            return
        self.state = EngineState.LEADER
        self.store.recover_expired()
        await self._plan_schedules()
        await self._advance_due_runs()
        await self._dispatch()
        self._reap_tasks()

    def _reap_tasks(self) -> None:
        for run_id, task in tuple(self._active.items()):
            if task.done():
                self._active.pop(run_id, None)

    async def _plan_schedules(self) -> None:
        now = utc_now()
        for schedule in self.store.enabled_schedules():
            next_due = schedule.get("next_due_at")
            if next_due:
                continue
            initial = first_occurrence(schedule["trigger_kind"], schedule["trigger_specification"],
                                       created_at=parse_utc(schedule["created_at"]), default_timezone=self.settings.default_timezone)
            if initial is None:
                continue
            if initial.due_at is None:
                # DST gaps never shift a task. Persist a terminal witnessed result instead.
                due = iso_utc(now)
                run, _ = self.store.create_occurrence(schedule=schedule, due_at=due,
                                                       idempotency_key=stable_key(schedule["schedule_id"], schedule["revision"], "DST", initial.local_time))
                if run["status"] == RunStatus.SCHEDULED.value:
                    self.store.mark_missed(run["run_id"], reason=initial.reason or "DST_NONEXISTENT_LOCAL_TIME")
                later = resolve_occurrences(schedule["trigger_kind"], schedule["trigger_specification"], after=now,
                                            count=1, default_timezone=self.settings.default_timezone)
                next_time = iso_utc(later[0].due_at) if later and later[0].due_at else None
                self.store.set_runtime(schedule["schedule_id"], revision=int(schedule["revision"]), next_due_at=next_time, last_due_at=due, last_run_id=run["run_id"])
                continue
            due = iso_utc(initial.due_at)
            self.store.create_occurrence(schedule=schedule, due_at=due, idempotency_key=stable_key(schedule["schedule_id"], schedule["revision"], due))
            self.store.set_runtime(schedule["schedule_id"], revision=int(schedule["revision"]), next_due_at=due)

    async def _advance_due_runs(self) -> None:
        now = utc_now()
        now_text = iso_utc(now)
        for run in self.store.due_scheduled_runs(now_text):
            if run.get("origin") == "RETRY":
                self.store.queue(run["run_id"], message="Retry delay elapsed; run queued for execution.")
                continue
            schedule = self.store.get_schedule(str(run.get("schedule_id") or ""))
            if schedule is None or schedule["lifecycle"] != "ENABLED":
                if run["status"] == RunStatus.SCHEDULED.value:
                    self.store.transition(run["run_id"], RunStatus.CANCELLED, stage="SCHEDULE", message="Schedule is no longer enabled.")
                continue
            due = parse_utc(run["due_at"])
            lateness = max(0.0, (now - due).total_seconds())
            policy = MissedRunPolicy(str(schedule["missed_run_policy"]))
            maximum = float(schedule["max_lateness_seconds"])
            session_relative = schedule["trigger_kind"] == "SESSION_RELATIVE"
            if (session_relative and lateness > maximum) or (policy is MissedRunPolicy.SKIP and lateness > maximum):
                self.store.mark_missed(run["run_id"], reason="SESSION_WINDOW_EXPIRED" if session_relative else "MISSED_RUN_POLICY_SKIP")
            else:
                self.store.queue(run["run_id"])
            later = resolve_occurrences(schedule["trigger_kind"], schedule["trigger_specification"], after=due,
                                        count=1, default_timezone=self.settings.default_timezone)
            next_time = iso_utc(later[0].due_at) if later and later[0].due_at else None
            self.store.set_runtime(schedule["schedule_id"], revision=int(schedule["revision"]), next_due_at=next_time, last_due_at=run["due_at"], last_run_id=run["run_id"])
            if next_time:
                self.store.create_occurrence(schedule=schedule, due_at=next_time,
                                             idempotency_key=stable_key(schedule["schedule_id"], schedule["revision"], next_time))

    async def _dispatch(self) -> None:
        self._reap_tasks()
        capacity = max(0, self.settings.max_concurrent_runs - len(self._active))
        if not capacity:
            return
        for run in self.store.queued_runs(limit=capacity):
            definition = self.registry.get(str(run["task_type"]))
            claimed, reason = self.store.try_activate(run["run_id"], owner_id=self.owner_id, lease_seconds=self.settings.run_lease_seconds,
                                                      resources=definition.required_resource_keys)
            if claimed is None:
                # A lock is transient; leave the run queued until its valid window expires.
                if reason and not reason.startswith("RESOURCE_LOCKED"):
                    LOGGER.warning("Unable to activate scheduler run %s: %s", run["run_id"], reason)
                continue
            task = asyncio.create_task(self._execute(claimed), name=f"beelzebub-scheduler-run-{claimed['run_id']}")
            self._active[str(claimed["run_id"])] = task

    async def _execute(self, run: Mapping[str, Any]) -> None:
        run_id = str(run["run_id"])
        definition = self.registry.get(str(run["task_type"]))
        context = SchedulerTaskContext(self, run)
        heartbeat_task = asyncio.create_task(self._heartbeat_while_active(run_id))
        try:
            outcome = await asyncio.wait_for(self._invoke_task(definition, context), timeout=max(0.1, definition.default_maximum_runtime_seconds))
            status = RunStatus(str(outcome.status))
            current = self.store.get_run(run_id)
            if not current or current["status"] != RunStatus.ACTIVE.value:
                return
            if current.get("cancellation_requested"):
                self.store.transition(run_id, RunStatus.CANCELLED, stage="CANCELLED", message="Task acknowledged operator cancellation.")
            elif status is RunStatus.COMPLETE:
                self.store.transition(run_id, RunStatus.COMPLETE, stage="COMPLETE", message=outcome.message, result=outcome.result or {})
            elif status is RunStatus.BLOCKED:
                self.store.transition(run_id, RunStatus.BLOCKED, stage="BLOCKED", message=outcome.message, result=outcome.result or {})
            elif status is RunStatus.FAILED:
                self.store.transition(run_id, RunStatus.FAILED, stage="FAILED", message=outcome.message, result=outcome.result or {}, error={"retryable": outcome.retryable})
                self._schedule_automatic_retry(run, definition, outcome)
            else:
                self.store.transition(run_id, RunStatus.FAILED, stage="FAILED", message="Task returned an unsupported terminal status.", error={"status": status.value})
        except asyncio.CancelledError:
            current = self.store.get_run(run_id)
            if current and current["status"] == RunStatus.ACTIVE.value:
                target = RunStatus.CANCELLED if current.get("cancellation_requested") else RunStatus.INTERRUPTED
                self.store.transition(run_id, target, stage=target.value, message="Task execution was cancelled by scheduler control.")
            raise
        except TaskBlocked as exc:
            if (current := self.store.get_run(run_id)) and current["status"] == RunStatus.ACTIVE.value:
                self.store.transition(run_id, RunStatus.BLOCKED, stage="BLOCKED", message=str(exc), error={"category": "EXTERNAL_GATE"})
        except TaskInvariantFailure as exc:
            if (current := self.store.get_run(run_id)) and current["status"] == RunStatus.ACTIVE.value:
                self.store.transition(run_id, RunStatus.FAILED, stage="INVARIANT", message=str(exc), error={"category": "INVARIANT", "retryable": False})
        except asyncio.TimeoutError:
            if (current := self.store.get_run(run_id)) and current["status"] == RunStatus.ACTIVE.value:
                self.store.transition(run_id, RunStatus.FAILED, stage="TIMEOUT", message="Task exceeded its approved maximum runtime.", error={"category": "TIMEOUT", "retryable": definition.retryable})
                self._schedule_automatic_retry(run, definition, TaskOutcome(status=RunStatus.FAILED, retryable=definition.retryable))
        except Exception as exc:
            LOGGER.exception("Scheduler task failed: %s", run_id)
            if (current := self.store.get_run(run_id)) and current["status"] == RunStatus.ACTIVE.value:
                self.store.transition(run_id, RunStatus.FAILED, stage="FAILED", message="Task encountered an operational failure.", error={"category": "OPERATIONAL", "detail": str(exc), "retryable": definition.retryable})
                self._schedule_automatic_retry(run, definition, TaskOutcome(status=RunStatus.FAILED, retryable=definition.retryable))
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            self._active.pop(run_id, None)

    async def _invoke_task(self, definition: Any, context: SchedulerTaskContext) -> TaskOutcome:
        if inspect.iscoroutinefunction(definition.execute):
            return await self.registry.execute(definition.task_type, context)
        raw = await asyncio.to_thread(definition.execute, context)
        if inspect.isawaitable(raw):
            raw = await raw
        return raw if isinstance(raw, TaskOutcome) else TaskOutcome(result=raw if isinstance(raw, Mapping) else {})

    async def _heartbeat_while_active(self, run_id: str) -> None:
        while True:
            await asyncio.sleep(max(0.1, self.settings.heartbeat_seconds))
            if not self.store.heartbeat(run_id, owner_id=self.owner_id, lease_seconds=self.settings.run_lease_seconds):
                return

    def _schedule_automatic_retry(self, run: Mapping[str, Any], definition: Any, outcome: TaskOutcome) -> None:
        """Create a linked delayed retry only for explicitly retryable non-session failures."""
        if not definition.retryable or not outcome.retryable:
            return
        schedule_id = str(run.get("schedule_id") or "")
        schedule = self.store.get_schedule(schedule_id) if schedule_id else None
        if schedule is None or schedule.get("trigger_kind") == "SESSION_RELATIVE":
            return
        policy = dict(schedule.get("retry_policy") or {})
        attempt = int(run.get("attempt") or 1)
        if attempt >= int(policy.get("max_attempts", 1)):
            return
        delay = min(float(policy.get("maximum_delay_seconds", 3600)), float(policy.get("initial_delay_seconds", 30)) * float(policy.get("backoff_multiplier", 2)) ** max(0, attempt - 1))
        due = iso_utc(utc_now() + timedelta(seconds=max(0, delay)))
        self.store.create_retry(str(run["run_id"]), idempotency_key=stable_key(run["run_id"], attempt + 1), due_at=due)

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.request_cancel(run_id)
        task = self._active.get(run_id)
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=self.settings.cancellation_grace_seconds)
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                latest = self.store.get_run(run_id)
                if latest and latest["status"] == RunStatus.ACTIVE.value:
                    self.store.transition(run_id, RunStatus.CANCELLED, stage="CANCELLED_FORCED", message="Task exceeded cancellation grace period and was cancelled.")
        return self.store.get_run(run_id) or run

    def status(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "owner_id": self.owner_id,
            "last_heartbeat": self.last_heartbeat,
            "last_error": self.last_error,
            "timezone": self.settings.default_timezone,
            "concurrency_limit": self.settings.max_concurrent_runs,
            **self.store.status_summary(),
        }
