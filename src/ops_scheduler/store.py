"""SQLite persistence for the independent BeezConsole operations scheduler."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
import json
import sqlite3
from typing import Any, Iterator, Mapping, Sequence
from uuid import uuid4

from .models import ALLOWED_TRANSITIONS, RunOrigin, RunStatus, ScheduleLifecycle, TERMINAL_STATUSES, canonical_json, iso_utc, parse_utc, sanitized, utc_now


SCHEMA_VERSION = 1


class OperationsStore:
    """Short-transaction, WAL-backed scheduler state with durable leases."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000, max_result_bytes: int = 262_144, max_event_bytes: int = 65_536) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self.max_result_bytes = int(max_result_bytes)
        self.max_event_bytes = int(max_event_bytes)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=max(1, self.busy_timeout_ms / 1000), isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    def initialize(self) -> None:
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS scheduler_schema_version (version INTEGER NOT NULL);
                INSERT INTO scheduler_schema_version(version)
                    SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM scheduler_schema_version);
                CREATE TABLE IF NOT EXISTS scheduler_schedule_heads (
                    schedule_id TEXT PRIMARY KEY, current_revision INTEGER NOT NULL, lifecycle TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, archived_at TEXT
                );
                CREATE TABLE IF NOT EXISTS scheduler_schedule_revisions (
                    schedule_id TEXT NOT NULL, revision INTEGER NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL,
                    task_type TEXT NOT NULL, task_configuration_json TEXT NOT NULL, trigger_kind TEXT NOT NULL,
                    trigger_specification_json TEXT NOT NULL, timezone TEXT NOT NULL, missed_run_policy TEXT NOT NULL,
                    max_lateness_seconds REAL NOT NULL, retry_policy_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL, PRIMARY KEY(schedule_id, revision),
                    FOREIGN KEY(schedule_id) REFERENCES scheduler_schedule_heads(schedule_id)
                );
                CREATE TABLE IF NOT EXISTS scheduler_schedule_runtime (
                    schedule_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, next_due_at TEXT, last_due_at TEXT,
                    last_run_id TEXT, last_evaluated_at TEXT, updated_at TEXT NOT NULL,
                    FOREIGN KEY(schedule_id) REFERENCES scheduler_schedule_heads(schedule_id)
                );
                CREATE TABLE IF NOT EXISTS scheduler_task_runs (
                    run_id TEXT PRIMARY KEY, schedule_id TEXT, schedule_revision INTEGER, origin TEXT NOT NULL,
                    task_type TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL, due_at TEXT,
                    queued_at TEXT, started_at TEXT, finished_at TEXT, attempt INTEGER NOT NULL, retry_of_run_id TEXT,
                    progress_current REAL, progress_total REAL, stage TEXT, message TEXT, configuration_json TEXT NOT NULL,
                    result_json TEXT, error_json TEXT, cancellation_requested INTEGER NOT NULL DEFAULT 0,
                    heartbeat_at TEXT, lease_owner TEXT, lease_expires_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    FOREIGN KEY(schedule_id) REFERENCES scheduler_schedule_heads(schedule_id),
                    FOREIGN KEY(retry_of_run_id) REFERENCES scheduler_task_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS scheduler_run_events (
                    event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, occurred_at TEXT NOT NULL, from_status TEXT,
                    to_status TEXT, stage TEXT, message TEXT, payload_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES scheduler_task_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS scheduler_leader_lease (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner_id TEXT NOT NULL, heartbeat_at TEXT NOT NULL, expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scheduler_resource_locks (
                    resource_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, owner_id TEXT NOT NULL, acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL, expires_at TEXT NOT NULL, FOREIGN KEY(run_id) REFERENCES scheduler_task_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS scheduler_notifications (
                    notification_id TEXT PRIMARY KEY, created_at TEXT NOT NULL, severity TEXT NOT NULL, title TEXT NOT NULL,
                    body TEXT NOT NULL, run_id TEXT, schedule_id TEXT, read_at TEXT, payload_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES scheduler_task_runs(run_id), FOREIGN KEY(schedule_id) REFERENCES scheduler_schedule_heads(schedule_id)
                );
                CREATE INDEX IF NOT EXISTS idx_scheduler_runs_status_due ON scheduler_task_runs(status, due_at);
                CREATE INDEX IF NOT EXISTS idx_scheduler_runs_schedule ON scheduler_task_runs(schedule_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_scheduler_events_run ON scheduler_run_events(run_id, occurred_at);
                CREATE INDEX IF NOT EXISTS idx_scheduler_notifications_read ON scheduler_notifications(read_at, created_at DESC);
            """)

    @staticmethod
    def _json(value: Any, maximum: int) -> str:
        return canonical_json(sanitized(value, maximum_bytes=maximum))

    @staticmethod
    def _loads(value: str | None, default: Any) -> Any:
        return json.loads(value) if value else default

    def _run(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        for field, default in (("configuration_json", {}), ("result_json", None), ("error_json", None)):
            item[field.removesuffix("_json")] = self._loads(item.pop(field), default)
        item["cancellation_requested"] = bool(item["cancellation_requested"])
        return item

    def _revision(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["task_configuration"] = self._loads(item.pop("task_configuration_json"), {})
        item["trigger_specification"] = self._loads(item.pop("trigger_specification_json"), {})
        item["retry_policy"] = self._loads(item.pop("retry_policy_json"), {})
        return item

    def _schedule(self, db: sqlite3.Connection, schedule_id: str) -> dict[str, Any] | None:
        row = db.execute("""
            SELECT h.*, r.*, rt.next_due_at, rt.last_due_at, rt.last_run_id, rt.last_evaluated_at AS runtime_last_evaluated_at
            FROM scheduler_schedule_heads h
            JOIN scheduler_schedule_revisions r ON r.schedule_id=h.schedule_id AND r.revision=h.current_revision
            LEFT JOIN scheduler_schedule_runtime rt ON rt.schedule_id=h.schedule_id
            WHERE h.schedule_id=?
        """, (schedule_id,)).fetchone()
        if row is None:
            return None
        item = self._revision(row)
        item["revision"] = item.pop("revision")
        return item

    def get_schedule(self, schedule_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            schedule = self._schedule(db, schedule_id)
            if schedule is None:
                return None
            revisions = db.execute("SELECT * FROM scheduler_schedule_revisions WHERE schedule_id=? ORDER BY revision DESC", (schedule_id,)).fetchall()
            schedule["revisions"] = [self._revision(row) for row in revisions]
            return schedule

    def list_schedules(self, *, lifecycle: str | None = None, task_type: str | None = None, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        predicates, params = [], []
        if lifecycle:
            predicates.append("h.lifecycle=?")
            params.append(lifecycle)
        if task_type:
            predicates.append("r.task_type=?")
            params.append(task_type)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self._connect() as db:
            total = int(db.execute("SELECT COUNT(*) FROM scheduler_schedule_heads h JOIN scheduler_schedule_revisions r ON r.schedule_id=h.schedule_id AND r.revision=h.current_revision" + where, params).fetchone()[0])
            rows = db.execute("""
                SELECT h.*, r.*, rt.next_due_at, rt.last_due_at, rt.last_run_id, rt.last_evaluated_at AS runtime_last_evaluated_at,
                    (SELECT status FROM scheduler_task_runs runs WHERE runs.schedule_id=h.schedule_id ORDER BY runs.created_at DESC LIMIT 1) AS last_result_status
                FROM scheduler_schedule_heads h JOIN scheduler_schedule_revisions r ON r.schedule_id=h.schedule_id AND r.revision=h.current_revision
                LEFT JOIN scheduler_schedule_runtime rt ON rt.schedule_id=h.schedule_id
            """ + where + " ORDER BY h.updated_at DESC LIMIT ? OFFSET ?", [*params, page_size, (page - 1) * page_size]).fetchall()
        return {"items": [self._revision(row) for row in rows], "page": page, "page_size": page_size, "total": total, "pages": max(1, (total + page_size - 1) // page_size)}

    def create_schedule(self, *, name: str, description: str, task_type: str, task_configuration: Mapping[str, Any], trigger_kind: str,
                        trigger_specification: Mapping[str, Any], timezone: str, missed_run_policy: str, max_lateness_seconds: float,
                        retry_policy: Mapping[str, Any], lifecycle: str = "ENABLED", created_by: str = "operator") -> dict[str, Any]:
        now, schedule_id = iso_utc(), f"schedule-{uuid4().hex}"
        with self._transaction() as db:
            db.execute("INSERT INTO scheduler_schedule_heads VALUES(?,?,?,?,?,NULL)", (schedule_id, 1, lifecycle, now, now))
            db.execute("""INSERT INTO scheduler_schedule_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                schedule_id, 1, name, description, task_type, self._json(task_configuration, self.max_result_bytes), trigger_kind,
                self._json(trigger_specification, self.max_result_bytes), timezone, missed_run_policy, max_lateness_seconds,
                self._json(retry_policy, self.max_result_bytes), now, created_by,
            ))
            db.execute("INSERT INTO scheduler_schedule_runtime(schedule_id,revision,next_due_at,last_due_at,last_run_id,last_evaluated_at,updated_at) VALUES(?,?,NULL,NULL,NULL,NULL,?)", (schedule_id, 1, now))
            return self._schedule(db, schedule_id) or {}

    def revise_schedule(self, schedule_id: str, *, expected_revision: int, name: str, description: str, task_type: str,
                        task_configuration: Mapping[str, Any], trigger_kind: str, trigger_specification: Mapping[str, Any], timezone: str,
                        missed_run_policy: str, max_lateness_seconds: float, retry_policy: Mapping[str, Any], created_by: str = "operator") -> dict[str, Any]:
        now = iso_utc()
        with self._transaction() as db:
            head = db.execute("SELECT current_revision,lifecycle FROM scheduler_schedule_heads WHERE schedule_id=?", (schedule_id,)).fetchone()
            if head is None:
                raise KeyError(schedule_id)
            if int(head["current_revision"]) != expected_revision:
                raise RuntimeError("STALE_REVISION")
            next_revision = expected_revision + 1
            db.execute("""INSERT INTO scheduler_schedule_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                schedule_id, next_revision, name, description, task_type, self._json(task_configuration, self.max_result_bytes), trigger_kind,
                self._json(trigger_specification, self.max_result_bytes), timezone, missed_run_policy, max_lateness_seconds,
                self._json(retry_policy, self.max_result_bytes), now, created_by,
            ))
            db.execute("UPDATE scheduler_schedule_heads SET current_revision=?,updated_at=? WHERE schedule_id=?", (next_revision, now, schedule_id))
            db.execute("UPDATE scheduler_schedule_runtime SET revision=?,next_due_at=NULL,last_evaluated_at=NULL,updated_at=? WHERE schedule_id=?", (next_revision, now, schedule_id))
            for row in db.execute("SELECT run_id FROM scheduler_task_runs WHERE schedule_id=? AND schedule_revision=? AND status='SCHEDULED'", (schedule_id, expected_revision)).fetchall():
                self._transition_locked(db, str(row["run_id"]), RunStatus.CANCELLED, stage="REVISION", message="Superseded by an immutable schedule revision.")
            return self._schedule(db, schedule_id) or {}

    def set_lifecycle(self, schedule_id: str, lifecycle: str) -> dict[str, Any]:
        if lifecycle not in {item.value for item in ScheduleLifecycle}:
            raise ValueError("Unsupported schedule lifecycle.")
        now = iso_utc()
        with self._transaction() as db:
            if not db.execute("SELECT 1 FROM scheduler_schedule_heads WHERE schedule_id=?", (schedule_id,)).fetchone():
                raise KeyError(schedule_id)
            db.execute("UPDATE scheduler_schedule_heads SET lifecycle=?, updated_at=?, archived_at=? WHERE schedule_id=?", (lifecycle, now, now if lifecycle == "ARCHIVED" else None, schedule_id))
            if lifecycle in {"PAUSED", "ARCHIVED"}:
                for row in db.execute("SELECT run_id FROM scheduler_task_runs WHERE schedule_id=? AND status='SCHEDULED'", (schedule_id,)).fetchall():
                    self._transition_locked(db, str(row["run_id"]), RunStatus.CANCELLED, stage="SCHEDULE", message=f"Schedule {lifecycle.lower()} by operator.")
            return self._schedule(db, schedule_id) or {}

    def set_runtime(self, schedule_id: str, *, revision: int, next_due_at: str | None, last_due_at: str | None = None, last_run_id: str | None = None) -> None:
        now = iso_utc()
        with self._transaction() as db:
            db.execute("""UPDATE scheduler_schedule_runtime SET revision=?,next_due_at=?,last_due_at=COALESCE(?,last_due_at),
                last_run_id=COALESCE(?,last_run_id),last_evaluated_at=?,updated_at=? WHERE schedule_id=?""",
                (revision, next_due_at, last_due_at, last_run_id, now, now, schedule_id))

    def enabled_schedules(self) -> list[dict[str, Any]]:
        return self.list_schedules(lifecycle="ENABLED", page_size=10_000)["items"]

    def create_occurrence(self, *, schedule: Mapping[str, Any], due_at: str, origin: RunOrigin = RunOrigin.SCHEDULE,
                          idempotency_key: str, attempt: int = 1, retry_of_run_id: str | None = None) -> tuple[dict[str, Any], bool]:
        now, run_id = iso_utc(), f"run-{uuid4().hex}"
        with self._transaction() as db:
            existing = db.execute("SELECT * FROM scheduler_task_runs WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if existing is not None:
                return self._run(existing) or {}, False
            db.execute("""INSERT INTO scheduler_task_runs(run_id,schedule_id,schedule_revision,origin,task_type,idempotency_key,status,due_at,
                attempt,retry_of_run_id,configuration_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                run_id, schedule.get("schedule_id"), schedule.get("revision"), origin.value, schedule["task_type"], idempotency_key,
                RunStatus.SCHEDULED.value, due_at, attempt, retry_of_run_id, self._json(schedule["task_configuration"], self.max_result_bytes), now, now,
            ))
            self._event_locked(db, run_id, None, RunStatus.SCHEDULED, "SCHEDULED", "Concrete occurrence created.", {"origin": origin.value, "due_at": due_at})
            row = db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
            return self._run(row) or {}, True

    def create_manual_run(self, schedule: Mapping[str, Any], *, request_id: str) -> tuple[dict[str, Any], bool]:
        return self.create_occurrence(schedule=schedule, due_at=iso_utc(), origin=RunOrigin.MANUAL,
                                      idempotency_key=f"manual:{request_id}")

    def create_retry(self, prior_run_id: str, *, idempotency_key: str, due_at: str | None = None) -> tuple[dict[str, Any], bool]:
        now, run_id = iso_utc(), f"run-{uuid4().hex}"
        with self._transaction() as db:
            prior = db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (prior_run_id,)).fetchone()
            if prior is None:
                raise KeyError(prior_run_id)
            if prior["status"] != RunStatus.FAILED.value:
                raise ValueError("Only failed runs may be retried.")
            existing = db.execute("SELECT * FROM scheduler_task_runs WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if existing:
                return self._run(existing) or {}, False
            due = due_at or now
            queued = due <= now
            status = RunStatus.QUEUED.value if queued else RunStatus.SCHEDULED.value
            db.execute("""INSERT INTO scheduler_task_runs(run_id,schedule_id,schedule_revision,origin,task_type,idempotency_key,status,due_at,queued_at,
                attempt,retry_of_run_id,configuration_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                run_id, prior["schedule_id"], prior["schedule_revision"], RunOrigin.RETRY.value, prior["task_type"], idempotency_key,
                status, due, now if queued else None, int(prior["attempt"]) + 1, prior_run_id, prior["configuration_json"], now, now,
            ))
            target = RunStatus.QUEUED if queued else RunStatus.SCHEDULED
            self._event_locked(db, run_id, None, target, "RETRY", "Retry run queued." if queued else "Retry run scheduled.", {"retry_of_run_id": prior_run_id})
            return self._run(db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()) or {}, True

    def _event_locked(self, db: sqlite3.Connection, run_id: str, before: RunStatus | None, after: RunStatus | None,
                      stage: str | None, message: str | None, payload: Mapping[str, Any] | None = None) -> None:
        db.execute("INSERT INTO scheduler_run_events VALUES(?,?,?,?,?,?,?,?)", (
            f"event-{uuid4().hex}", run_id, iso_utc(), before.value if before else None, after.value if after else None,
            stage, message, self._json(payload or {}, self.max_event_bytes),
        ))

    def _transition_locked(self, db: sqlite3.Connection, run_id: str, target: RunStatus, *, stage: str | None = None,
                           message: str | None = None, result: Any = None, error: Any = None) -> dict[str, Any]:
        row = db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        current = RunStatus(str(row["status"]))
        if target not in ALLOWED_TRANSITIONS.get(current, frozenset()):
            raise ValueError(f"Illegal scheduler transition {current.value} -> {target.value}.")
        now = iso_utc()
        values: dict[str, Any] = {"status": target.value, "stage": stage, "message": message, "updated_at": now}
        if target is RunStatus.QUEUED:
            values["queued_at"] = now
        if target in TERMINAL_STATUSES:
            values["finished_at"] = now
            values["lease_owner"] = None
            values["lease_expires_at"] = None
        if result is not None:
            values["result_json"] = self._json(result, self.max_result_bytes)
        if error is not None:
            values["error_json"] = self._json(error, self.max_result_bytes)
        db.execute("UPDATE scheduler_task_runs SET " + ",".join(f"{key}=?" for key in values) + " WHERE run_id=?", [*values.values(), run_id])
        self._event_locked(db, run_id, current, target, stage, message, {"result": result} if result is not None else {"error": error} if error is not None else {})
        if target in {RunStatus.FAILED, RunStatus.BLOCKED, RunStatus.INTERRUPTED, RunStatus.MISSED}:
            severity = "error" if target is RunStatus.FAILED else "warning"
            self._notification_locked(db, severity, f"Scheduler run {target.value.lower()}", message or f"Run {run_id} is {target.value.lower()}.", run_id=run_id, schedule_id=row["schedule_id"], payload={"status": target.value})
        return self._run(db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()) or {}

    def transition(self, run_id: str, target: RunStatus, *, stage: str | None = None, message: str | None = None,
                   result: Any = None, error: Any = None) -> dict[str, Any]:
        with self._transaction() as db:
            value = self._transition_locked(db, run_id, target, stage=stage, message=message, result=result, error=error)
            if target in TERMINAL_STATUSES:
                db.execute("DELETE FROM scheduler_resource_locks WHERE run_id=?", (run_id,))
            return value

    def queue(self, run_id: str, *, message: str = "Run is due and queued for execution.") -> dict[str, Any]:
        return self.transition(run_id, RunStatus.QUEUED, stage="QUEUED", message=message)

    def mark_missed(self, run_id: str, *, reason: str) -> dict[str, Any]:
        return self.transition(run_id, RunStatus.MISSED, stage="MISSED", message=reason, error={"reason": reason})

    def due_scheduled_runs(self, now: str) -> list[dict[str, Any]]:
        with self._connect() as db:
            return [self._run(row) or {} for row in db.execute("SELECT * FROM scheduler_task_runs WHERE status='SCHEDULED' AND due_at<=? ORDER BY due_at", (now,)).fetchall()]

    def queued_runs(self, *, limit: int) -> list[dict[str, Any]]:
        with self._connect() as db:
            return [self._run(row) or {} for row in db.execute("SELECT * FROM scheduler_task_runs WHERE status='QUEUED' ORDER BY due_at,created_at LIMIT ?", (limit,)).fetchall()]

    def try_activate(self, run_id: str, *, owner_id: str, lease_seconds: float, resources: Sequence[str]) -> tuple[dict[str, Any] | None, str | None]:
        now = utc_now()
        expires = iso_utc(now + timedelta(seconds=lease_seconds))
        now_text = iso_utc(now)
        resources = tuple(sorted(set(resources)))
        with self._transaction() as db:
            row = db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != RunStatus.QUEUED.value:
                return None, "RUN_NOT_QUEUED"
            db.execute("DELETE FROM scheduler_resource_locks WHERE expires_at<=?", (now_text,))
            for resource in resources:
                lock = db.execute("SELECT run_id FROM scheduler_resource_locks WHERE resource_key=?", (resource,)).fetchone()
                if lock:
                    return None, f"RESOURCE_LOCKED:{resource}"
            for resource in resources:
                db.execute("INSERT INTO scheduler_resource_locks VALUES(?,?,?,?,?,?)", (resource, run_id, owner_id, now_text, now_text, expires))
            db.execute("""UPDATE scheduler_task_runs SET status='ACTIVE',started_at=?,heartbeat_at=?,lease_owner=?,lease_expires_at=?,stage=?,message=?,updated_at=? WHERE run_id=?""",
                       (now_text, now_text, owner_id, expires, "ACTIVE", "Task execution started.", now_text, run_id))
            self._event_locked(db, run_id, RunStatus.QUEUED, RunStatus.ACTIVE, "ACTIVE", "Task execution started.", {"resources": list(resources), "owner_id": owner_id})
            return self._run(db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()), None

    def heartbeat(self, run_id: str, *, owner_id: str, lease_seconds: float, stage: str | None = None, message: str | None = None,
                  progress_current: float | None = None, progress_total: float | None = None) -> bool:
        now = utc_now()
        now_text, expires = iso_utc(now), iso_utc(now + timedelta(seconds=lease_seconds))
        with self._transaction() as db:
            row = db.execute("SELECT status,lease_owner FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row["status"] != RunStatus.ACTIVE.value or row["lease_owner"] != owner_id:
                return False
            db.execute("""UPDATE scheduler_task_runs SET heartbeat_at=?,lease_expires_at=?,stage=COALESCE(?,stage),message=COALESCE(?,message),
                progress_current=COALESCE(?,progress_current),progress_total=COALESCE(?,progress_total),updated_at=? WHERE run_id=?""",
                       (now_text, expires, stage, message, progress_current, progress_total, now_text, run_id))
            db.execute("UPDATE scheduler_resource_locks SET heartbeat_at=?,expires_at=? WHERE run_id=? AND owner_id=?", (now_text, expires, run_id, owner_id))
            if stage or message or progress_current is not None:
                self._event_locked(db, run_id, RunStatus.ACTIVE, RunStatus.ACTIVE, stage or "PROGRESS", message or "Progress updated.", {"progress_current": progress_current, "progress_total": progress_total})
            return True

    def request_cancel(self, run_id: str) -> dict[str, Any]:
        with self._transaction() as db:
            row = db.execute("SELECT status FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            status = RunStatus(str(row["status"]))
            if status in {RunStatus.SCHEDULED, RunStatus.QUEUED}:
                result = self._transition_locked(db, run_id, RunStatus.CANCELLED, stage="CANCELLED", message="Cancelled by operator before execution.")
                db.execute("DELETE FROM scheduler_resource_locks WHERE run_id=?", (run_id,))
                return result
            if status is not RunStatus.ACTIVE:
                raise ValueError("Only scheduled, queued, or active runs may be cancelled.")
            now = iso_utc()
            db.execute("UPDATE scheduler_task_runs SET cancellation_requested=1,updated_at=? WHERE run_id=?", (now, run_id))
            self._event_locked(db, run_id, RunStatus.ACTIVE, RunStatus.ACTIVE, "CANCELLATION_REQUESTED", "Operator requested cooperative cancellation.")
            return self._run(db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()) or {}

    def is_cancellation_requested(self, run_id: str) -> bool:
        with self._connect() as db:
            row = db.execute("SELECT cancellation_requested FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone()
            return bool(row and row["cancellation_requested"])

    def recover_expired(self) -> list[str]:
        now = iso_utc()
        interrupted: list[str] = []
        with self._transaction() as db:
            db.execute("DELETE FROM scheduler_resource_locks WHERE expires_at<=?", (now,))
            rows = db.execute("SELECT run_id FROM scheduler_task_runs WHERE status='ACTIVE' AND lease_expires_at<=?", (now,)).fetchall()
            for row in rows:
                run_id = str(row["run_id"])
                self._transition_locked(db, run_id, RunStatus.INTERRUPTED, stage="LEASE_EXPIRED", message="Run lease expired before a terminal result was persisted.", error={"reason": "LEASE_EXPIRED"})
                db.execute("DELETE FROM scheduler_resource_locks WHERE run_id=?", (run_id,))
                interrupted.append(run_id)
        return interrupted

    def acquire_or_renew_leader(self, owner_id: str, *, lease_seconds: float) -> bool:
        now = utc_now()
        now_text, expires = iso_utc(now), iso_utc(now + timedelta(seconds=lease_seconds))
        with self._transaction() as db:
            row = db.execute("SELECT owner_id,expires_at FROM scheduler_leader_lease WHERE singleton=1").fetchone()
            if row is None:
                db.execute("INSERT INTO scheduler_leader_lease VALUES(1,?,?,?)", (owner_id, now_text, expires))
                return True
            if row["owner_id"] == owner_id or str(row["expires_at"]) <= now_text:
                db.execute("UPDATE scheduler_leader_lease SET owner_id=?,heartbeat_at=?,expires_at=? WHERE singleton=1", (owner_id, now_text, expires))
                return True
            return False

    def leader(self) -> dict[str, Any] | None:
        with self._connect() as db:
            row = db.execute("SELECT owner_id,heartbeat_at,expires_at FROM scheduler_leader_lease WHERE singleton=1").fetchone()
            return dict(row) if row else None

    def release_leader(self, owner_id: str) -> None:
        with self._transaction() as db:
            db.execute("DELETE FROM scheduler_leader_lease WHERE singleton=1 AND owner_id=?", (owner_id,))

    def _notification_locked(self, db: sqlite3.Connection, severity: str, title: str, body: str, *, run_id: str | None = None,
                             schedule_id: str | None = None, payload: Mapping[str, Any] | None = None) -> str:
        notification_id = f"notification-{uuid4().hex}"
        db.execute("INSERT INTO scheduler_notifications VALUES(?,?,?,?,?,?,?,?,?)", (notification_id, iso_utc(), severity, title[:240], body[:8192], run_id, schedule_id, None, self._json(payload or {}, self.max_event_bytes)))
        return notification_id

    def notify(self, severity: str, title: str, body: str, *, run_id: str | None = None, schedule_id: str | None = None,
               payload: Mapping[str, Any] | None = None) -> str:
        if severity not in {"info", "warning", "error"}:
            raise ValueError("Unsupported notification severity.")
        with self._transaction() as db:
            return self._notification_locked(db, severity, title, body, run_id=run_id, schedule_id=schedule_id, payload=payload)

    def list_notifications(self, *, unread_only: bool = False, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        clause = " WHERE read_at IS NULL" if unread_only else ""
        with self._connect() as db:
            total = int(db.execute("SELECT COUNT(*) FROM scheduler_notifications" + clause).fetchone()[0])
            rows = db.execute("SELECT * FROM scheduler_notifications" + clause + " ORDER BY created_at DESC LIMIT ? OFFSET ?", (page_size, (page - 1) * page_size)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = self._loads(item.pop("payload_json"), {})
            items.append(item)
        return {"items": items, "page": page, "page_size": page_size, "total": total, "pages": max(1, (total + page_size - 1) // page_size)}

    def mark_notification_read(self, notification_id: str) -> dict[str, Any]:
        with self._transaction() as db:
            row = db.execute("SELECT * FROM scheduler_notifications WHERE notification_id=?", (notification_id,)).fetchone()
            if row is None:
                raise KeyError(notification_id)
            now = iso_utc()
            db.execute("UPDATE scheduler_notifications SET read_at=COALESCE(read_at,?) WHERE notification_id=?", (now, notification_id))
            item = dict(db.execute("SELECT * FROM scheduler_notifications WHERE notification_id=?", (notification_id,)).fetchone())
            item["payload"] = self._loads(item.pop("payload_json"), {})
            return item

    def mark_all_notifications_read(self) -> int:
        with self._transaction() as db:
            cursor = db.execute("UPDATE scheduler_notifications SET read_at=? WHERE read_at IS NULL", (iso_utc(),))
            return int(cursor.rowcount)

    def list_runs(self, *, status: str | None = None, task_type: str | None = None, schedule_id: str | None = None,
                  page: int = 1, page_size: int = 100) -> dict[str, Any]:
        predicates, params = [], []
        for field, value in (("status", status), ("task_type", task_type), ("schedule_id", schedule_id)):
            if value:
                predicates.append(f"{field}=?")
                params.append(value)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self._connect() as db:
            total = int(db.execute("SELECT COUNT(*) FROM scheduler_task_runs" + where, params).fetchone()[0])
            rows = db.execute("SELECT * FROM scheduler_task_runs" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?", [*params, page_size, (page - 1) * page_size]).fetchall()
        return {"items": [self._run(row) for row in rows], "page": page, "page_size": page_size, "total": total, "pages": max(1, (total + page_size - 1) // page_size)}

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            run = self._run(db.execute("SELECT * FROM scheduler_task_runs WHERE run_id=?", (run_id,)).fetchone())
            if run is None:
                return None
            events = db.execute("SELECT * FROM scheduler_run_events WHERE run_id=? ORDER BY occurred_at", (run_id,)).fetchall()
            locks = db.execute("SELECT * FROM scheduler_resource_locks WHERE run_id=? ORDER BY resource_key", (run_id,)).fetchall()
            notifications = db.execute("SELECT * FROM scheduler_notifications WHERE run_id=? ORDER BY created_at DESC", (run_id,)).fetchall()
            run["events"] = [{**dict(row), "payload": self._loads(row["payload_json"], {})} for row in events]
            for event in run["events"]:
                event.pop("payload_json", None)
            run["resource_locks"] = [dict(row) for row in locks]
            run["notifications"] = [{**dict(row), "payload": self._loads(row["payload_json"], {})} for row in notifications]
            for notification in run["notifications"]:
                notification.pop("payload_json", None)
            return run

    def status_summary(self) -> dict[str, Any]:
        with self._connect() as db:
            counts = {row["status"]: int(row["count"]) for row in db.execute("SELECT status,COUNT(*) count FROM scheduler_task_runs GROUP BY status")}
            active = int(counts.get(RunStatus.ACTIVE.value, 0))
            unread = int(db.execute("SELECT COUNT(*) FROM scheduler_notifications WHERE read_at IS NULL").fetchone()[0])
            next_run = db.execute("SELECT run_id,due_at,task_type FROM scheduler_task_runs WHERE status='SCHEDULED' ORDER BY due_at LIMIT 1").fetchone()
            quick = db.execute("PRAGMA quick_check").fetchone()
        return {"counts": counts, "active_workers": active, "unread_notifications": unread, "next_due": dict(next_run) if next_run else None,
                "database": {"path": str(self.path), "quick_check": str(quick[0]) if quick else "unknown"}, "leader": self.leader()}
