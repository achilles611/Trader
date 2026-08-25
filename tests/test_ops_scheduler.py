from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from src.ops_scheduler.engine import SchedulerEngine, SchedulerSettings
from src.ops_scheduler.models import AuthorityClassification, RunStatus, TaskOutcome, iso_utc, stable_key
from src.ops_scheduler.registry import TaskDefinition, TaskRegistry
from src.ops_scheduler.service import SchedulerService
from src.ops_scheduler.store import OperationsStore
from src.ops_scheduler.tasks import _authentic_observation_freshness
from src.ops_scheduler.triggers import resolve_occurrences


def _validated(configuration: dict[str, object]) -> dict[str, object]:
    if set(configuration) - {"value"}:
        raise ValueError("Unexpected configuration.")
    return dict(configuration)


def _complete(context: object) -> TaskOutcome:
    return TaskOutcome(result={"observed": True})


class OperationsSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = OperationsStore(Path(self.temp.name) / "operations.sqlite3")
        self.registry = TaskRegistry((TaskDefinition("test.observe", "Test observe", "Read-only test task.", "Test", AuthorityClassification.READ_ONLY, _validated, _complete),))
        self.engine = SchedulerEngine(self.store, self.registry, settings=SchedulerSettings(poll_interval_seconds=0.01, heartbeat_seconds=0.02, run_lease_seconds=1))
        self.service = SchedulerService(self.store, self.registry, self.engine)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def schedule(self) -> dict[str, object]:
        return self.service.create_schedule({
            "name": "Observe", "description": "", "task_type": "test.observe", "task_configuration": {"value": "ok"},
            "trigger_kind": "INTERVAL", "trigger_specification": {"anchor_at_utc": "2030-01-01T00:00:00Z", "interval_seconds": 60},
            "timezone": "America/Denver", "missed_run_policy": "SKIP", "max_lateness_seconds": 60, "retry_policy": {"max_attempts": 1},
        })

    def test_schedule_revisions_and_sensitive_fields_are_rejected(self) -> None:
        schedule = self.schedule()
        with self.assertRaisesRegex(ValueError, "sensitive-looking"):
            self.service.create_schedule({
                "name": "Bad", "task_type": "test.observe", "task_configuration": {"token": "nope"},
                "trigger_kind": "INTERVAL", "trigger_specification": {"anchor_at_utc": "2030-01-01T00:00:00Z", "interval_seconds": 60},
            })
        revised = self.service.update_schedule(str(schedule["schedule_id"]), {
            "name": "Observe revised", "description": "immutable", "task_type": "test.observe", "task_configuration": {"value": "ok"},
            "trigger_kind": "INTERVAL", "trigger_specification": {"anchor_at_utc": "2030-01-01T00:00:00Z", "interval_seconds": 60},
            "timezone": "America/Denver", "missed_run_policy": "SKIP", "max_lateness_seconds": 60, "retry_policy": {"max_attempts": 1}, "current_revision": 1,
        })
        self.assertEqual(revised["current_revision"], 2)
        with self.assertRaisesRegex(RuntimeError, "STALE_REVISION"):
            self.service.update_schedule(str(schedule["schedule_id"]), {
                "name": "stale", "task_type": "test.observe", "task_configuration": {"value": "ok"},
                "trigger_kind": "INTERVAL", "trigger_specification": {"anchor_at_utc": "2030-01-01T00:00:00Z", "interval_seconds": 60}, "current_revision": 1,
            })

    def test_occurrence_idempotency_and_lifecycle(self) -> None:
        schedule = self.schedule()
        due = "2030-01-01T00:00:00Z"
        key = stable_key(schedule["schedule_id"], schedule["current_revision"], due)
        first, created = self.store.create_occurrence(schedule={**schedule, "revision": schedule["current_revision"]}, due_at=due, idempotency_key=key)
        second, duplicate = self.store.create_occurrence(schedule={**schedule, "revision": schedule["current_revision"]}, due_at=due, idempotency_key=key)
        self.assertTrue(created)
        self.assertFalse(duplicate)
        self.assertEqual(first["run_id"], second["run_id"])
        self.store.queue(str(first["run_id"]))
        activated, reason = self.store.try_activate(str(first["run_id"]), owner_id="test", lease_seconds=1, resources=("exclusive",))
        self.assertIsNotNone(activated, reason)
        completed = self.store.transition(str(first["run_id"]), RunStatus.COMPLETE, result={"ok": True})
        self.assertEqual(completed["status"], "COMPLETE")
        with self.assertRaises(ValueError):
            self.store.transition(str(first["run_id"]), RunStatus.ACTIVE)

    def test_engine_run_now_completes_once(self) -> None:
        async def exercise() -> None:
            schedule = self.schedule()
            await self.engine.start()
            run = self.service.run_now(str(schedule["schedule_id"]), operator_request_id="exactly-once")
            for _ in range(50):
                await self.engine.tick()
                latest = self.service.run(str(run["run_id"]))
                if latest["status"] == "COMPLETE":
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(self.service.run(str(run["run_id"]))["status"], "COMPLETE")
            same = self.service.run_now(str(schedule["schedule_id"]), operator_request_id="exactly-once")
            self.assertEqual(same["run_id"], run["run_id"])
            await self.engine.stop()
        asyncio.run(exercise())

    def test_dst_gap_and_session_policy_fail_closed(self) -> None:
        occurrences = resolve_occurrences("ONCE", {"local_datetime": "2027-03-14T02:30:00", "timezone": "America/Denver"}, after=datetime(2027, 3, 1, tzinfo=timezone.utc))
        self.assertEqual(occurrences[0].reason, "DST_NONEXISTENT_LOCAL_TIME")
        with self.assertRaisesRegex(ValueError, "require missed_run_policy SKIP"):
            self.service.create_schedule({
                "name": "Unsafe", "task_type": "test.observe", "task_configuration": {"value": "ok"},
                "trigger_kind": "SESSION_RELATIVE", "trigger_specification": {"session": "ASIA", "event": "OPEN", "offset_minutes": 0}, "missed_run_policy": "BOUNDED_CATCH_UP",
            })


class LaneIIIReadinessFreshnessTests(unittest.TestCase):
    NOW = datetime(2026, 8, 25, 0, 30, tzinfo=timezone.utc)
    THRESHOLD = 15.0

    def freshness(self, timestamp: object) -> dict[str, object]:
        listener = {} if timestamp is None else {"last_observation_at": timestamp}
        return _authentic_observation_freshness(listener, self.THRESHOLD, now=self.NOW)

    def test_fresh_observation(self) -> None:
        result = self.freshness("2026-08-25T00:29:50Z")
        self.assertTrue(result["fresh"])
        self.assertEqual(result["age_seconds"], 10.0)

    def test_exactly_at_threshold_observation(self) -> None:
        result = self.freshness("2026-08-25T00:29:45Z")
        self.assertTrue(result["fresh"])
        self.assertEqual(result["age_seconds"], self.THRESHOLD)

    def test_stale_observation(self) -> None:
        result = self.freshness("2026-08-25T00:29:44.999999Z")
        self.assertFalse(result["fresh"])
        self.assertEqual(result["reason"], "STALE_OBSERVATION_TIMESTAMP")

    def test_missing_observation(self) -> None:
        result = self.freshness(None)
        self.assertFalse(result["fresh"])
        self.assertEqual(result["reason"], "MISSING_OBSERVATION_TIMESTAMP")

    def test_transport_receive_timestamp_is_not_authentic_observation_evidence(self) -> None:
        result = _authentic_observation_freshness(
            {"last_received_at": "2026-08-25T00:29:59Z"}, self.THRESHOLD, now=self.NOW,
        )
        self.assertFalse(result["fresh"])
        self.assertEqual(result["reason"], "MISSING_OBSERVATION_TIMESTAMP")

    def test_invalid_observation(self) -> None:
        result = self.freshness("not-a-timestamp")
        self.assertFalse(result["fresh"])
        self.assertEqual(result["reason"], "INVALID_OBSERVATION_TIMESTAMP")

    def test_future_clock_skew_observation(self) -> None:
        result = self.freshness("2026-08-25T00:30:00.000001Z")
        self.assertFalse(result["fresh"])
        self.assertEqual(result["reason"], "FUTURE_OBSERVATION_TIMESTAMP")
