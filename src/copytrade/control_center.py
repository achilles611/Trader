"""Phase C paper-only control-center API and durable operator controls.

This module deliberately consumes the persisted discovery/research/execution
tables.  It adds only operator state, control state, and an audit feed; it
does not alter Phase A discovery or Phase B scoring/reconstruction logic.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker
from src.l3f_provider.shadow_runtime import LaneIIIShadowRuntime
from src.l3g_paper.commissioning import (
    CommissioningLedgerGateError,
    evaluate_commissioning_ledger_gate,
    evaluate_commissioning_post_run_verification,
)
from src.l3g_paper.health import ledger_health_projection, sanitized_paper_health
from src.l3g_paper.ledger import PaperLedger, resolve_ledger_epoch
from src.l3g_paper.ninjatrader_login import NinjaTraderLoginBootstrap, NinjaTraderLoginState
from src.l3g_paper.ninjatrader_transport import PaperExecutionTransport
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout
from src.l3g_paper.slim_status import derive_slim_paper_status, unavailable_slim_status
from src.l3g_paper.verification import LocalLedgerVerificationController
from src.l3h_live.status import fail_closed_status
from src.ops_scheduler.api_models import PreviewRequest, RunNowRequest, ScheduleRequest, ScheduleUpdateRequest, TemplateRequest
from src.ops_scheduler.engine import SchedulerEngine, SchedulerSettings
from src.ops_scheduler.models import ScheduleLifecycle
from src.ops_scheduler.registry import TaskRegistry
from src.ops_scheduler.service import SchedulerService
from src.ops_scheduler.store import OperationsStore
from src.ops_scheduler.tasks import _authentic_observation_freshness, production_task_definitions

from .config import CopyTradeConfig
from .contracts import PHASE_B_RECOMMENDATION_SCHEMA_VERSION
from .control_center_read_model import phase_b_candidate_view
from .discovery import build_discovery_provider, parse_activity_age
from .models import as_utc, iso, jsonable, stable_id, utc_now
from .rate_limit import shared_hyperliquid_info_limiter
from .science_read_model import ScientificReadModel
from .source_acquisition import HyperCoreSourceAcquisition, HyperCoreSourceError, cache_directory, discovery_preset
from .storage import CopyTradeDatabase


CONTROL_RUNNING = "RUNNING"
CONTROL_ENTRIES_PAUSED = "ENTRIES_PAUSED"
CONTROL_EXITING = "EXITING"
CONTROL_PAUSED = "PAUSED"
CONTROL_STATES = {CONTROL_RUNNING, CONTROL_ENTRIES_PAUSED, CONTROL_EXITING, CONTROL_PAUSED}
OPERATOR_STATES = {"new", "approved", "shadow", "active", "muted", "rejected"}
WATCHER_MAX_SUBSCRIPTIONS = 10
NINJATRADER_RUNTIME_LOGGER = logging.getLogger("uvicorn.error")
LEDGER_VERIFICATION_FRESHNESS_SECONDS = 15 * 60
MARKET_OBSERVER_ACTIVE_FRESHNESS_SECONDS = 15.0
LEDGER_VERIFIER_SHUTDOWN_WAIT_SECONDS = 30.0


class UnsafePaperExecutionShutdown(RuntimeError):
    """Normal process teardown was refused while exact execution is unproven."""


def _assert_hot_paper_ledger_path(paper_path: Path, cold_root: Path) -> None:
    resolved_path = paper_path.resolve()
    resolved_cold_root = cold_root.expanduser().resolve()
    try:
        resolved_path.relative_to(resolved_cold_root)
    except ValueError:
        return
    raise RuntimeError(
        f"LANE_III_PAPER active ledger path {resolved_path} may not reside under configured cold storage root {resolved_cold_root}"
    )


def _runtime_git_sha() -> str:
    supplied = os.getenv("BEELZEBUB_GIT_SHA")
    if supplied:
        return supplied
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() if result.returncode == 0 and result.stdout.strip() else "unknown"


def _load(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default


def _dump(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"))


def _config_fingerprint(snapshot: dict[str, Any]) -> str:
    """Match Phase B's immutable configuration fingerprint without importing its pipeline at module load."""
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class WatcherMembershipSupervisor:
    """Own the one optional execution watcher used by the Control Center.

    Membership is intentionally derived from the service on a short local poll:
    active entry targets plus wallets that still need exit monitoring.  A
    replacement waits for the previous watcher task to stop before it starts a
    successor, so public fills are never processed by overlapping watchers.
    """

    def __init__(
        self, watcher_service: Any, watcher_factory: Any, store: "ControlCenterStore", *,
        poll_interval_seconds: float = 1.0, retry_delay_seconds: float = 3.0, stop_timeout_seconds: float = 3.0,
    ) -> None:
        self.watcher_service = watcher_service
        self.watcher_factory = watcher_factory
        self.store = store
        self.poll_interval_seconds = max(0.05, poll_interval_seconds)
        self.retry_delay_seconds = max(0.05, retry_delay_seconds)
        self.stop_timeout_seconds = max(0.05, stop_timeout_seconds)
        self._watcher: Any | None = None
        self._watcher_task: asyncio.Task[Any] | None = None
        self._desired_wallets: tuple[str, ...] = ()
        self._subscribed_wallets: tuple[str, ...] = ()
        self._state = "STARTING"
        self._error = ""
        self._last_membership_change: str | None = None
        self._next_retry_at = 0.0
        self._stopping = False
        self._wake = asyncio.Event()
        self._transition_lock = asyncio.Lock()

    def wake(self) -> None:
        """Request a prompt membership check after an operator/position change."""
        self._wake.set()

    def health(self) -> dict[str, Any]:
        watcher_payload = self._watcher.health.as_dict() if self._watcher is not None else {}
        watcher_state = str(watcher_payload.get("state") or "")
        if self._state in {"IDLE", "STARTING", "DEGRADED", "STOPPED"}:
            state = self._state
        else:
            state = watcher_state or "CONNECTED"
        return {
            **watcher_payload,
            "state": state,
            "supervisor_state": self._state,
            "desired_wallets": list(self._desired_wallets),
            "subscribed_wallets": list(self._subscribed_wallets),
            "desired_target_count": len(self._desired_wallets),
            "subscribed_target_count": len(self._subscribed_wallets),
            "membership_in_sync": self._desired_wallets == self._subscribed_wallets and self._state in {"IDLE", "CONNECTED"},
            "last_membership_change": self._last_membership_change,
            "supervisor_error": self._error,
            "per_target": watcher_payload.get("per_target", {}),
        }

    async def run(self) -> None:
        try:
            while not self._stopping:
                try:
                    desired = tuple(sorted({str(wallet).lower() for wallet in self.watcher_service.monitored_execution_wallets()}))
                    await self._reconcile(desired)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A watcher or transient local-service failure must never
                    # end the FastAPI lifespan task.  Expose it and retry.
                    self._state, self._error = "DEGRADED", str(exc)
                    self._next_retry_at = asyncio.get_running_loop().time() + self.retry_delay_seconds
                    await self._stop_current()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval_seconds)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
        finally:
            await self._stop_current()
            self._state = "STOPPED"

    async def stop(self) -> None:
        self._stopping = True
        self.wake()
        await self._stop_current()
        self._state = "STOPPED"

    async def _reconcile(self, desired: tuple[str, ...]) -> None:
        async with self._transition_lock:
            if desired != self._desired_wallets:
                self._desired_wallets = desired
                self._last_membership_change = iso(utc_now())
            await self._clear_finished_watcher()
            if len(desired) > WATCHER_MAX_SUBSCRIPTIONS:
                detail = (
                    f"Execution watcher requires {len(desired)} subscriptions but supports a maximum of "
                    f"{WATCHER_MAX_SUBSCRIPTIONS}. Reduce the Active cohort or close exit-only sleeves."
                )
                # A stale subset is not a valid subscription set.  Stop it
                # before reporting the fail-safe degraded state.
                await self._stop_current_locked()
                self._state, self._error = "DEGRADED", detail
                if self.store.control_state()["entries_allowed"]:
                    self.store.set_control_state(CONTROL_ENTRIES_PAUSED, by="watcher_supervisor", note=detail)
                return
            if desired == self._subscribed_wallets and self._watcher_task is not None and not self._watcher_task.done():
                self._state, self._error = "CONNECTED", ""
                return
            if not desired:
                await self._stop_current_locked()
                self._state, self._error = "IDLE", ""
                return
            loop = asyncio.get_running_loop()
            if loop.time() < self._next_retry_at and self._watcher_task is None:
                self._state = "DEGRADED"
                return
            await self._stop_current_locked()
            self._state, self._error = "STARTING", ""
            try:
                watcher = self.watcher_factory(self.watcher_service.adapter)
                self._watcher = watcher
                self._watcher_task = asyncio.create_task(
                    watcher.run(
                        list(desired), self.watcher_service.ingest_watched_fills, self.watcher_service.ingest_watched_state,
                        self.watcher_service.ingest_market_update, self._reconcile_snapshot(desired),
                    )
                )
                self._subscribed_wallets = desired
                self._last_membership_change = iso(utc_now())
                await asyncio.sleep(0)
                await self._clear_finished_watcher()
                if self._watcher_task is not None:
                    self._state = "CONNECTED"
            except Exception as exc:
                self._watcher = None
                self._watcher_task = None
                self._subscribed_wallets = ()
                self._state, self._error = "DEGRADED", str(exc)
                self._next_retry_at = loop.time() + self.retry_delay_seconds

    def _reconcile_snapshot(self, wallets: tuple[str, ...]) -> Any:
        async def reconcile() -> dict[str, int]:
            per_wallet = getattr(self.watcher_service, "reconcile_wallet", None)
            if callable(per_wallet):
                result: dict[str, int] = {}
                for wallet in wallets:
                    result[wallet] = await per_wallet(wallet)
                return result
            fallback = self.watcher_service.reconcile_monitored_wallets()
            return await fallback if asyncio.iscoroutine(fallback) else fallback
        return reconcile

    async def _clear_finished_watcher(self) -> None:
        task = self._watcher_task
        if task is None or not task.done():
            return
        try:
            task.result()
            error = "Execution watcher exited unexpectedly."
        except asyncio.CancelledError:
            error = ""
        except Exception as exc:  # watcher failures must leave FastAPI alive
            error = str(exc)
        self._watcher, self._watcher_task, self._subscribed_wallets = None, None, ()
        if not self._stopping:
            self._state = "DEGRADED"
            self._error = error or "Execution watcher stopped unexpectedly."
            self._next_retry_at = asyncio.get_running_loop().time() + self.retry_delay_seconds

    async def _stop_current(self) -> None:
        async with self._transition_lock:
            await self._stop_current_locked()

    async def _stop_current_locked(self) -> None:
        watcher, task = self._watcher, self._watcher_task
        if watcher is not None:
            try:
                watcher.stop()
            except Exception as exc:
                self._error = self._error or f"Execution watcher stop failed: {exc}"
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=self.stop_timeout_seconds)
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        elif task is not None:
            await asyncio.gather(task, return_exceptions=True)
        if self._subscribed_wallets:
            self._last_membership_change = iso(utc_now())
        self._watcher, self._watcher_task, self._subscribed_wallets = None, None, ()


class ControlCenterStore:
    """Small additive schema for durable Phase C operator state and audit data."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS copy_control_center_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    state TEXT NOT NULL, updated_at TEXT NOT NULL,
                    updated_by TEXT NOT NULL DEFAULT 'operator',
                    note TEXT NOT NULL DEFAULT ''
                );
                INSERT OR IGNORE INTO copy_control_center_state(singleton, state, updated_at, updated_by, note)
                    VALUES (1, 'RUNNING', datetime('now'), 'system', 'paper entries enabled by default');
                CREATE TABLE IF NOT EXISTS copy_control_center_activity (
                    event_id TEXT PRIMARY KEY, occurred_at TEXT NOT NULL, category TEXT NOT NULL,
                    severity TEXT NOT NULL, wallet TEXT, symbol TEXT, message TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_control_activity_time
                    ON copy_control_center_activity(occurred_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_control_activity_wallet
                    ON copy_control_center_activity(wallet, occurred_at DESC);
                CREATE TABLE IF NOT EXISTS copy_control_center_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    progress_current INTEGER,
                    progress_total INTEGER,
                    stage TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    cancellation_requested INTEGER NOT NULL DEFAULT 0,
                    configuration_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_control_jobs_type_time
                    ON copy_control_center_jobs(job_type, created_at DESC);
                """
            )

    def control_state(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT state, updated_at, updated_by, note FROM copy_control_center_state WHERE singleton=1").fetchone()
        assert row is not None
        result = dict(row)
        result["entries_allowed"] = result["state"] == CONTROL_RUNNING
        result["paper_only"] = True
        return result

    def set_control_state(self, state: str, *, by: str = "operator", note: str = "") -> dict[str, Any]:
        if state not in CONTROL_STATES:
            raise ValueError(f"Unsupported paper control state: {state}")
        self.initialize()
        now = iso(None)
        with self._connect() as connection:
            connection.execute(
                "UPDATE copy_control_center_state SET state=?, updated_at=?, updated_by=?, note=? WHERE singleton=1",
                (state, now, by, note),
            )
        self.record_activity(
            category="control", severity="warning" if state != CONTROL_RUNNING else "info",
            message=f"Paper control state changed to {state}", payload={"state": state, "by": by, "note": note},
        )
        return self.control_state()

    def record_activity(
        self, *, category: str, severity: str, message: str, wallet: str | None = None,
        symbol: str | None = None, payload: dict[str, Any] | None = None, occurred_at: object | None = None,
    ) -> None:
        self.initialize()
        at = iso(occurred_at)
        event_id = stable_id("control_activity", at, category, severity, wallet or "", symbol or "", message, payload or {})
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_control_center_activity(event_id, occurred_at, category, severity, wallet, symbol, message, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (event_id, at, category, severity, wallet.lower() if wallet else None, symbol, message, _dump(payload or {})),
            )

    def activities(self, *, limit: int = 100, wallet: str | None = None) -> list[dict[str, Any]]:
        self.initialize()
        query = "SELECT * FROM copy_control_center_activity"
        values: list[Any] = []
        if wallet:
            query += " WHERE wallet=?"
            values.append(wallet.lower())
        query += " ORDER BY occurred_at DESC LIMIT ?"
        values.append(max(1, min(int(limit), 500)))
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [{**dict(row), "payload": _load(row["payload_json"], {})} for row in rows]

    def create_job(self, *, job_type: str, configuration: dict[str, Any], job_id: str | None = None) -> dict[str, Any]:
        self.initialize()
        created_at = iso(utc_now())
        identifier = job_id or stable_id("control_center_job", job_type, created_at, configuration)
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_control_center_jobs(job_id, job_type, status, created_at, stage, message, configuration_json)
                   VALUES (?, ?, 'queued', ?, 'queued', 'Discovery job queued.', ?)""",
                (identifier, job_type, created_at, _dump(configuration)),
            )
        return self.get_job(identifier) or {}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM copy_control_center_jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._job_payload(row) if row else None

    def list_jobs(self, *, job_type: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        self.initialize()
        query = "SELECT * FROM copy_control_center_jobs"
        values: list[Any] = []
        if job_type:
            query += " WHERE job_type=?"
            values.append(job_type)
        query += " ORDER BY created_at DESC LIMIT ?"
        values.append(max(1, min(int(limit), 200)))
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._job_payload(row) for row in rows]

    def update_job(
        self, job_id: str, *, status: str | None = None, stage: str | None = None, message: str | None = None,
        progress_current: int | None = None, progress_total: int | None = None, result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None, started: bool = False, finished: bool = False,
    ) -> dict[str, Any]:
        self.initialize()
        assignments, values = [], []
        for column, value in (("status", status), ("stage", stage), ("message", message),
                              ("progress_current", progress_current), ("progress_total", progress_total)):
            if value is not None:
                assignments.append(f"{column}=?")
                values.append(value)
        if result is not None:
            assignments.append("result_json=?")
            values.append(_dump(result))
        if error is not None:
            assignments.append("error_json=?")
            values.append(_dump(error))
        if started:
            assignments.append("started_at=COALESCE(started_at, ?)")
            values.append(iso(utc_now()))
        if finished:
            assignments.append("finished_at=?")
            values.append(iso(utc_now()))
        if not assignments:
            return self.get_job(job_id) or {}
        values.append(job_id)
        with self._connect() as connection:
            connection.execute(f"UPDATE copy_control_center_jobs SET {', '.join(assignments)} WHERE job_id=?", values)
        return self.get_job(job_id) or {}

    def request_job_cancellation(self, job_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("UPDATE copy_control_center_jobs SET cancellation_requested=1 WHERE job_id=?", (job_id,))
        return self.get_job(job_id)

    @staticmethod
    def _job_payload(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["cancellation_requested"] = bool(value["cancellation_requested"])
        value["configuration"] = _load(value.pop("configuration_json"), {})
        value["result"] = _load(value.pop("result_json"), {})
        value["error"] = _load(value.pop("error_json"), {})
        return value

    def entry_block_reason(self, wallet: str, action: str) -> str | None:
        """Return an auditable paper-entry gate reason; exits are never gated here."""
        if action not in {"open", "add"}:
            return None
        state = self.control_state()
        if not state["entries_allowed"]:
            return "paper_entries_paused"
        with self._connect() as connection:
            target = connection.execute("SELECT status FROM copy_targets WHERE wallet=?", (wallet.lower(),)).fetchone()
        status = str(target["status"]) if target else None
        if status == "muted":
            return "wallet_muted"
        if status != "active":
            return "wallet_not_active"
        return None


class CopyControlCenter:
    """Read-model and command service for the Phase C control surface."""

    def __init__(
        self, config: CopyTradeConfig, database: CopyTradeDatabase | None = None, *, execution_service: Any | None = None,
        shadow_adapter: Any | None = None,
    ) -> None:
        self.config = config
        self.database = database or CopyTradeDatabase(config.artifacts.database_path)
        self.database.initialize()
        self.store = ControlCenterStore(config.artifacts.database_path)
        self.store.initialize()
        self.science = ScientificReadModel(config, self.database.path)
        self._execution_service = execution_service
        self._shadow_adapter = shadow_adapter

    def _paper_service(self) -> Any:
        """Return the single service that owns mutable PAPER engine state."""
        if self._execution_service is None:
            # Laziness avoids the module-level service/control-store import
            # cycle while still preventing Control Center fallback commands
            # from creating an independent ad-hoc execution engine.
            from .service import CopyTradeService
            self._execution_service = CopyTradeService(self.config, self.database)
        return self._execution_service

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self.store._connect() as connection:
            yield connection

    def health(self, watcher_health: dict[str, Any] | Any | None = None) -> dict[str, Any]:
        now = utc_now()
        with self._connect() as connection:
            db_ok = bool(connection.execute("SELECT 1").fetchone())
            latest_discovery = connection.execute("SELECT * FROM copy_discovery_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            latest_analysis = connection.execute("SELECT * FROM copy_analysis_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            mark = connection.execute("SELECT MAX(updated_at) AS updated_at FROM copy_virtual_positions WHERE closed_at IS NULL").fetchone()
            fill = connection.execute("SELECT MAX(event_timestamp) AS event_timestamp FROM copy_raw_fills").fetchone()
        last_mark = mark["updated_at"] if mark and mark["updated_at"] else None
        mark_age_ms = (now - as_utc(last_mark)).total_seconds() * 1000 if last_mark else None
        supplied_watcher = watcher_health() if callable(watcher_health) else watcher_health
        watcher = dict(supplied_watcher or {"state": "NOT_ATTACHED", "detail": "Control center is not running a watcher."})
        active_wallets = {target.wallet for target in self.database.list_targets("active")}
        monitored_wallets = active_wallets | {position.target_wallet for position in self.database.list_virtual_positions(open_only=True)}
        watcher["active_entry_target_count"] = len(active_wallets)
        watcher["monitored_target_count"] = len(monitored_wallets)
        watcher["open_sleeve_wallet_count"] = len(monitored_wallets - active_wallets)
        watcher.setdefault("desired_wallets", [])
        watcher.setdefault("subscribed_wallets", list(watcher.get("per_target", {})))
        watcher.setdefault("desired_target_count", len(watcher["desired_wallets"]))
        watcher.setdefault("subscribed_target_count", len(watcher["subscribed_wallets"]))
        watcher.setdefault("membership_in_sync", watcher["desired_wallets"] == watcher["subscribed_wallets"])
        watcher.setdefault("last_membership_change", None)
        watcher.setdefault("supervisor_state", watcher.get("state", "NOT_ATTACHED"))
        analysis = self.config.analysis
        api_limiter = shared_hyperliquid_info_limiter(
            self.config.source.info_url, operating_budget=getattr(analysis, "api_weight_budget_per_minute", 900),
            backoff_initial_seconds=getattr(analysis, "rate_limit_backoff_initial_seconds", 2.0),
            backoff_max_seconds=getattr(analysis, "rate_limit_backoff_max_seconds", 30.0),
            jitter_seconds=getattr(analysis, "rate_limit_jitter_seconds", 0.5),
            coordination_path=self.config.artifacts.database_path,
        )
        return {
            "mode": self.config.mode,
            "paper_only": True,
            "database": {"connected": db_ok, "path": str(self.config.artifacts.database_path)},
            "watcher": watcher,
            "market_data": {"last_mark_at": last_mark, "age_ms": mark_age_ms,
                            "fresh": bool(mark_age_ms is not None and mark_age_ms <= self.config.paper_execution.market_data_max_age_ms)},
            "source": {"last_public_fill_at": fill["event_timestamp"] if fill else None},
            "recovery": self._paper_service().recovery_status(),
            "hyperliquid_api": api_limiter.telemetry(),
            "last_discovery_run": self._run_payload(latest_discovery),
            "last_phase_b_run": self._run_payload(latest_analysis),
            "websocket": {"available": True, "endpoint": "/ws"},
            "kill_switch": {"active": self.config.risk.kill_switch_path.exists(), "path": str(self.config.risk.kill_switch_path)},
            "control": self.store.control_state(),
            # Phase D.0 is a persisted simulator-only read model.  It is
            # intentionally not wired to the mutable Phase-C paper engine.
            "execution": self.execution_health(),
            "timestamp": iso(now),
        }

    @staticmethod
    def _run_payload(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        value = dict(row)
        for key in list(value):
            if key.endswith("_json"):
                value[key[:-5]] = _load(value.pop(key), {} if key == "configuration_json" else [])
        return value

    def overview(self) -> dict[str, Any]:
        counts = self._counts()
        portfolio = self.portfolio_summary()
        return {
            "paper_only": True,
            "counts": counts,
            "funnel": self.funnel(),
            "portfolio": portfolio,
            "control": self.store.control_state(),
            "top_candidates": self.candidates(page_size=8, status="")['items'],
            "active_cohort": self.active_cohort(),
            "recovery": self._paper_service().recovery_status(),
            "recent_activity": self.activity(limit=8),
        }

    def _counts(self) -> dict[str, int]:
        with self._connect() as connection:
            discovered = int(connection.execute("SELECT COUNT(*) FROM copy_discovery_candidates").fetchone()[0])
            statuses = {str(row["status"]): int(row["count"]) for row in connection.execute(
                "SELECT status, COUNT(*) AS count FROM copy_targets GROUP BY status"
            ).fetchall()}
            lifecycle = {str(row["state"]): int(row["count"]) for row in connection.execute(
                """SELECT COALESCE(lifecycle_status, 'new') AS state, COUNT(*) AS count
                   FROM copy_discovery_candidates candidate LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
                   GROUP BY COALESCE(lifecycle_status, 'new')"""
            ).fetchall()}
            stale = int(connection.execute(
                """SELECT COUNT(*) FROM copy_candidate_analyses analysis JOIN copy_candidate_scores score
                   ON score.target_wallet=analysis.wallet AND score.analysis_run_id=analysis.last_run_id AND score.provenance='phase_b'
                   WHERE analysis.lifecycle_status='qualified' AND score.config_fingerprint<>?""",
                (_config_fingerprint(self.config.research_snapshot()),),
            ).fetchone()[0])
            open_positions = int(connection.execute("SELECT COUNT(*) FROM copy_virtual_positions WHERE closed_at IS NULL").fetchone()[0])
        return {
            "total_discovered": discovered, "new": statuses.get("new", 0), "queued": statuses.get("queued", 0),
            "prefilter_rejected": lifecycle.get("prefilter_rejected", 0),
            "analyzed": lifecycle.get("analyzed", 0) + lifecycle.get("qualified", 0),
            "qualified": lifecycle.get("qualified", 0), "shadow": statuses.get("shadow", 0),
            "active": statuses.get("active", 0), "muted": statuses.get("muted", 0), "rejected": statuses.get("rejected", 0),
            "stale_analyses": stale, "open_paper_positions": open_positions,
        }

    def funnel(self) -> list[dict[str, Any]]:
        counts = self._counts()
        # Each value is a directly persisted-state count; unavailable stages are
        # intentionally absent rather than inferred from unrelated totals.
        return [
            {"key": "discovered", "label": "Discovered", "count": counts["total_discovered"], "filter": {}},
            {"key": "prefilter_rejected", "label": "Prefilter rejected", "count": counts["prefilter_rejected"], "filter": {"lifecycle": "prefilter_rejected"}},
            {"key": "analyzed", "label": "Analyzed", "count": counts["analyzed"], "filter": {"lifecycle": "analyzed"}},
            {"key": "qualified", "label": "Qualified", "count": counts["qualified"], "filter": {"lifecycle": "qualified"}},
            {"key": "shadow", "label": "Shadow finalists", "count": counts["shadow"], "filter": {"status": "shadow"}},
            {"key": "active", "label": "Active paper traders", "count": counts["active"], "filter": {"status": "active"}},
        ]

    def candidates(
        self, *, page: int = 1, page_size: int = 50, sort: str = "score", direction: str = "desc",
        search: str = "", status: str = "", lifecycle: str = "", min_score: float | None = None,
        max_score: float | None = None, min_win_rate: float | None = None, max_win_rate: float | None = None,
        min_profit_factor: float | None = None, max_profit_factor: float | None = None, max_drawdown: float | None = None,
        max_follower_drawdown: float | None = None, coverage: str = "", copyability_available: bool | None = None,
        recent_days: int | None = None, current_only: bool = False,
    ) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))
        sortable = {
            "score": "phase_score.total_score", "wallet": "candidate.wallet", "last_active": "candidate.recent_activity_at",
            "campaigns": "json_extract(analysis.summary_json, '$.target_metrics.activity.campaigns')",
            "win_rate": "json_extract(analysis.summary_json, '$.target_metrics.profitability.win_rate')",
            "profit_factor": "json_extract(analysis.summary_json, '$.target_metrics.profitability.profit_factor')",
            "target_pnl": "json_extract(analysis.summary_json, '$.target_metrics.profitability.net_pnl')",
            "follower_pnl": "json_extract(analysis.summary_json, '$.follower.net_pnl')",
            "target_drawdown": "json_extract(analysis.summary_json, '$.target_metrics.risk.max_drawdown_fraction')",
            "follower_drawdown": "json_extract(analysis.summary_json, '$.follower.max_drawdown')",
        }
        order = sortable.get(sort, sortable["score"])
        descending = "ASC" if direction.lower() == "asc" else "DESC"
        clauses: list[str] = []
        values: list[Any] = []
        if search:
            clauses.append("candidate.wallet LIKE ?")
            values.append(f"%{search.lower()}%")
        if status:
            clauses.append("target.status=?")
            values.append(status)
        if lifecycle:
            clauses.append("COALESCE(analysis.lifecycle_status, 'new')=?")
            values.append(lifecycle)
        if min_score is not None:
            clauses.append("COALESCE(phase_score.total_score, -999999)>=?")
            values.append(float(min_score))
        if max_score is not None:
            clauses.append("COALESCE(phase_score.total_score, 999999)<=?")
            values.append(float(max_score))
        if min_win_rate is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profitability.win_rate'), -1)>=?")
            values.append(float(min_win_rate))
        if max_win_rate is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profitability.win_rate'), 999999)<=?")
            values.append(float(max_win_rate))
        if min_profit_factor is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profitability.profit_factor'), -1)>=?")
            values.append(float(min_profit_factor))
        if max_profit_factor is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profitability.profit_factor'), 999999)<=?")
            values.append(float(max_profit_factor))
        if max_drawdown is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.risk.max_drawdown_fraction'), 999999)<=?")
            values.append(float(max_drawdown))
        if max_follower_drawdown is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.follower.max_drawdown'), 999999)<=?")
            values.append(float(max_follower_drawdown))
        if coverage:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.coverage.coverage_state'), 'UNPROVEN')=?")
            values.append(coverage)
        if copyability_available is not None:
            if copyability_available:
                clauses.append("COALESCE(json_extract(analysis.summary_json, '$.copyability.status'), '') NOT IN ('', 'unavailable')")
            else:
                clauses.append("COALESCE(json_extract(analysis.summary_json, '$.copyability.status'), 'unavailable') IN ('', 'unavailable')")
        if recent_days is not None:
            cutoff = iso(utc_now() - timedelta(days=max(0, int(recent_days))))
            clauses.append("candidate.recent_activity_at>=?")
            values.append(cutoff)
        fingerprint = _config_fingerprint(self.config.research_snapshot())
        if current_only:
            clauses.append("phase_score.config_fingerprint=?")
            values.append(fingerprint)
        base = """
            FROM copy_discovery_candidates candidate
            JOIN copy_targets target ON target.wallet=candidate.wallet
            LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
            LEFT JOIN copy_candidate_scores phase_score ON phase_score.target_wallet=candidate.wallet
              AND phase_score.analysis_run_id=analysis.last_run_id AND phase_score.provenance='phase_b'
            LEFT JOIN copy_candidate_scores legacy_score ON legacy_score.target_wallet=candidate.wallet
              AND legacy_score.provenance!='phase_b' AND legacy_score.calculated_at=(SELECT MAX(current_score.calculated_at)
                FROM copy_candidate_scores current_score WHERE current_score.target_wallet=candidate.wallet
                  AND current_score.provenance!='phase_b')
        """
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            total = int(connection.execute("SELECT COUNT(*) " + base + where, values).fetchone()[0])
            rows = connection.execute(
                """SELECT candidate.wallet, candidate.discovered_at, candidate.last_seen_at, candidate.recent_activity_at,
                   candidate.source_count, target.label, target.status AS operator_state,
                   COALESCE(analysis.lifecycle_status, 'new') AS lifecycle_status, analysis.completed_at AS analysis_timestamp,
                   analysis.summary_json, analysis.prefilter_reasons_json,
                   phase_score.total_score AS phase_score_total, phase_score.eligible AS phase_score_eligible,
                   phase_score.component_scores_json AS phase_score_components, phase_score.penalties_json AS phase_score_penalties,
                   phase_score.reasons_json AS phase_score_reasons, phase_score.provenance AS phase_score_provenance,
                   phase_score.analysis_run_id AS phase_score_run_id, phase_score.config_fingerprint AS phase_score_fingerprint,
                   legacy_score.total_score AS legacy_score_total, legacy_score.eligible AS legacy_score_eligible
                """ + base + where + f" ORDER BY {order} {descending} NULLS LAST, candidate.wallet ASC LIMIT ? OFFSET ?",
                [*values, page_size, (page - 1) * page_size],
            ).fetchall()
        items = [self._candidate_row(dict(row), fingerprint) for row in rows]
        return {"items": items, "page": page, "page_size": page_size, "total": total,
                "pages": max(1, (total + page_size - 1) // page_size), "current_config_fingerprint": fingerprint}

    def _candidate_row(self, row: dict[str, Any], fingerprint: str) -> dict[str, Any]:
        summary = _load(row.pop("summary_json", None), {})
        phase_score = self._score_mapping(row, "phase_score")
        legacy_score = self._score_mapping(row, "legacy_score")
        view = phase_b_candidate_view(summary, phase_score, current_config_fingerprint=fingerprint, legacy_score=legacy_score)
        target, follower, coverage = view["target"], view["follower"], view["coverage"]
        canonical = view["score"]
        return {
            "wallet": row["wallet"], "label": row.get("label", ""), "operator_state": row["operator_state"],
            "research_state": row["lifecycle_status"], "score": canonical["total"], "qualified": canonical["eligible"],
            "analysis_timestamp": row.get("analysis_timestamp"), "stale_analysis": bool(phase_score and not canonical["current"]),
            "last_active": row.get("recent_activity_at"), "history_days": view["history_days"],
            "campaigns": view["campaigns"], "win_rate": target.get("win_rate"),
            "profit_factor": target.get("profit_factor"), "expectancy": target.get("expectancy"),
            "target_net_pnl": target.get("net_pnl"), "target_max_drawdown": target.get("max_drawdown"),
            "follower_net_pnl": follower.get("net_pnl"), "follower_max_drawdown": follower.get("max_drawdown"),
            "follower_expectancy": follower.get("expectancy"), "follower_profit_factor": follower.get("profit_factor"),
            "copyability": view["copyability"].get("score", view["copyability"].get("status")),
            "missed_trade_rate": follower.get("missed_trade_rate"), "slippage_robustness": follower.get("slippage_robustness"),
            "walk_forward": view["walk_forward"]["status"], "walk_forward_score": view["walk_forward"]["score"],
            "coverage": coverage.get("coverage_state", coverage.get("status", "UNPROVEN")),
            "concentration": target.get("concentration"), "liquidation_frequency": target.get("liquidation_frequency"),
            "recency_days": view["last_active_recency_days"], "source_count": row.get("source_count", 0),
            "score_reasons": canonical["reasons"], "legacy_compatibility_score": view["legacy_compatibility_score"],
            "prefilter_reasons": _load(row.get("prefilter_reasons_json"), []),
        }

    @staticmethod
    def _score_mapping(row: dict[str, Any], prefix: str) -> dict[str, Any] | None:
        total = row.get(f"{prefix}_total")
        if total is None:
            return None
        return {
            "total_score": total, "eligible": bool(row.get(f"{prefix}_eligible")),
            "component_scores": _load(row.get(f"{prefix}_components"), {}),
            "penalties": _load(row.get(f"{prefix}_penalties"), {}), "reasons": _load(row.get(f"{prefix}_reasons"), []),
            "provenance": row.get(f"{prefix}_provenance"), "analysis_run_id": row.get(f"{prefix}_run_id"),
            "config_fingerprint": row.get(f"{prefix}_fingerprint"),
        }

    def candidate_detail(self, wallet: str) -> dict[str, Any] | None:
        rows = self.candidates(page=1, page_size=200, search=wallet, sort="wallet", direction="asc")["items"]
        row = next((item for item in rows if item["wallet"].lower() == wallet.lower()), None)
        if not row:
            return None
        target = self.database.get_target(wallet)
        analysis = self.database.get_candidate_analysis(wallet)
        summary = analysis.summary if analysis else {}
        canonical_score = self._authoritative_score(wallet, analysis.last_run_id if analysis else None)
        fingerprint = _config_fingerprint(self.config.research_snapshot())
        recommendation = self.database.get_finalist_recommendation(
            analysis.last_run_id if analysis else None, fingerprint, wallet,
        )
        legacy_score = self._latest_legacy_score(wallet)
        view = phase_b_candidate_view(summary, canonical_score, current_config_fingerprint=fingerprint, legacy_score=legacy_score)
        return {
            "identity": {"wallet": wallet.lower(), "label": target.label if target else "", "operator_state": target.status if target else "new",
                         "research_state": analysis.lifecycle_status if analysis else "new", "first_discovered": row.get("discovered_at"),
                         "last_activity": row.get("last_active"), "analysis_timestamp": row.get("analysis_timestamp"),
                         "coverage": view["coverage"], "source_count": row.get("source_count", 0)},
            # Phase A cheap filtering and Phase B evidence-backed gates answer
            # different questions.  Never present a Phase-A reason as though
            # it were a failed Phase-B hard gate.
            "phase_a_prefilter_reasons": row.get("prefilter_reasons", []),
            "phase_b_hard_gates": list((canonical_score or {}).get("hard_gates", [])),
            "score": view["score"],
            "legacy_compatibility_score": view["legacy_compatibility_score"],
            "target_performance": view["target"], "follower_performance": view["follower"], "copyability": view["copyability"],
            "slippage": view["slippage_scenarios"], "latency": view["latency"],
            "walk_forward": view["walk_forward"], "concentration": view["target"]["concentration"],
            "analysis_window": view["analysis_window"], "diversification": view["diversification"],
            "finalist_recommendation": recommendation,
            "table_row": row, "open_paper_positions": self.positions(wallet=wallet), "activity": self.activity(wallet=wallet, limit=50),
        }

    def shadow_finalists(self) -> list[dict[str, Any]]:
        """Render Phase B's persisted diversified recommendations verbatim.

        This is a read model: Phase C never recalculates scores, gates,
        correlation, or exposure penalties when deciding its finalist cohort.
        """
        fingerprint = _config_fingerprint(self.config.research_snapshot())
        candidates = {str(item["wallet"]).lower(): item for item in self.database.list_analysis_candidates(limit=10_000)}
        recommendations = self.database.list_finalist_recommendations(fingerprint, selected_only=True)
        finalists = []
        for recommendation in recommendations:
            if recommendation.get("recommendation_schema_version") != PHASE_B_RECOMMENDATION_SCHEMA_VERSION:
                continue
            wallet = str(recommendation["wallet"]).lower()
            candidate = candidates.get(wallet)
            # A recommendation belongs to an immutable candidate analysis run.
            # Do not present it after the candidate advanced to a newer run.
            if not candidate or candidate.get("analysis_run_id") != recommendation.get("analysis_run_id"):
                continue
            score = self._authoritative_score(wallet, str(recommendation["analysis_run_id"]))
            if not score:
                continue
            summary = candidate.get("analysis_summary", {})
            view = phase_b_candidate_view(summary, score, current_config_fingerprint=fingerprint)
            finalists.append({"rank": recommendation.get("selection_rank"), "wallet": wallet,
                              "score": recommendation.get("final_selection_score"),
                              "target": view["target"], "follower": view["follower"], "copyability": view["copyability"],
                              "data_quality": view["coverage"], "principal_risks": list(score.get("reasons", [])),
                              "diversification": {"diversification_penalty": recommendation.get("diversification_penalty")},
                              "walk_forward": view["walk_forward"], "current_config_fingerprint": fingerprint,
                              "candidate_config_fingerprint": score.get("config_fingerprint"), "stale_for_current_config": False,
                              "selection_rank": recommendation.get("selection_rank"),
                              "selection_score": recommendation.get("final_selection_score"),
                              "finalist_recommendation": recommendation,
                              "selection_reason": "persisted Phase B finalist recommendation"})
        positions = {item["wallet"]: item for item in self._pnl_by_trader()}
        for item in finalists:
            target = self.database.get_target(str(item["wallet"]))
            item["operator_state"] = target.status if target else "new"
            item["paper_pnl"] = positions.get(str(item["wallet"]), {})
        return finalists

    def set_operator_state(self, wallet: str, state: str, *, by: str = "operator", allow_overflow: bool = False) -> dict[str, Any]:
        if state not in OPERATOR_STATES:
            raise ValueError("Operator state must be one of: " + ", ".join(sorted(OPERATOR_STATES)))
        if state == "active":
            return self.activate_wallet(wallet, by=by, allow_overflow=allow_overflow)
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError("Wallet was not found in the candidate universe.")
        return self._apply_operator_state(target.wallet, state, by=by, allow_overflow=allow_overflow)

    def activate_wallet(self, wallet: str, *, by: str = "operator", allow_overflow: bool = False) -> dict[str, Any]:
        """Enter Active only after validating the complete current Phase-B chain.

        This is the single application-level Active transition.  Generic
        status helpers intentionally reject Active so a future CLI, plugin, or
        automation cannot accidentally grant paper-entry eligibility.
        """
        # Keep the authorization reads, status write, and success audit in one
        # short SQLite write transaction.  A separate validation connection
        # would allow a concurrent Phase-B run to replace the candidate's
        # authoritative run after validation and before the Active write.
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            target = connection.execute(
                "SELECT wallet, status FROM copy_targets WHERE wallet=?", (wallet.lower(),)
            ).fetchone()
            if not target:
                raise KeyError("Wallet was not found in the candidate universe.")
            self._validate_activation_authority(str(target["wallet"]), connection=connection)
            before = str(target["status"])
            now = iso(None)
            cursor = connection.execute(
                "UPDATE copy_targets SET status=?, updated_at=? WHERE wallet=?",
                ("active", now, target["wallet"]),
            )
            if not cursor.rowcount:
                raise KeyError("Wallet was not found in the candidate universe.")
            connection.execute(
                "UPDATE copy_discovery_candidates SET discovery_status=? WHERE wallet=?",
                ("active", target["wallet"]),
            )
            message = f"Trader state changed from {before} to active"
            payload = {"from": before, "to": "active", "by": by}
            connection.execute(
                """INSERT OR IGNORE INTO copy_control_center_activity(
                    event_id, occurred_at, category, severity, wallet, symbol, message, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (stable_id("control_activity", now, "operator", "info", target["wallet"], "", message, payload),
                 now, "operator", "info", target["wallet"], None, message, _dump(payload)),
            )
            active_count = int(connection.execute(
                "SELECT COUNT(*) FROM copy_targets WHERE status='active'"
            ).fetchone()[0])
        return {"wallet": str(target["wallet"]).lower(), "operator_state": "active", "previous_state": before,
                "paper_only": True, "active_count_after": active_count, "recommended_max": 7,
                "cohort_over_recommended_size": active_count > 7, "allow_overflow": allow_overflow}

    def _validate_activation_authority(
        self, wallet: str, *, connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        """Validate Phase B's immutable authority before any target mutation."""
        if connection is None:
            analysis = self.database.get_candidate_analysis(wallet)
            lifecycle_status = analysis.lifecycle_status if analysis else None
            run_id = analysis.last_run_id if analysis else None
        else:
            analysis = connection.execute(
                "SELECT lifecycle_status, last_run_id FROM copy_candidate_analyses WHERE wallet=?", (wallet.lower(),)
            ).fetchone()
            lifecycle_status = str(analysis["lifecycle_status"]) if analysis else None
            run_id = str(analysis["last_run_id"]) if analysis and analysis["last_run_id"] else None
        if lifecycle_status != "qualified" or not run_id:
            raise ValueError("Wallet cannot be activated because its Phase B analysis is not currently qualified. No state change was made.")
        fingerprint = _config_fingerprint(self.config.research_snapshot())
        score = self._authoritative_score(wallet, run_id, connection=connection)
        if not score or score.get("provenance") != "phase_b" or score.get("analysis_run_id") != run_id:
            raise ValueError("Wallet cannot be activated because it lacks an authoritative Phase B score for its current analysis run. No state change was made.")
        if not bool(score["eligible"]):
            raise ValueError("Wallet cannot be activated because its canonical Phase B score is not eligible. No state change was made.")
        if score.get("config_fingerprint") != fingerprint:
            raise ValueError("Wallet cannot be activated because its Phase B analysis is stale. No state change was made.")
        if connection is None:
            run = self.database.get_analysis_run(run_id)
        else:
            row = connection.execute("SELECT status FROM copy_analysis_runs WHERE run_id=?", (run_id,)).fetchone()
            run = dict(row) if row else None
        if not run or run.get("status") not in {"completed", "completed_with_errors"}:
            raise ValueError("Wallet cannot be activated because its canonical Phase B analysis run did not complete successfully. No state change was made.")
        if connection is None:
            recommendation = self.database.get_finalist_recommendation(run_id, fingerprint, wallet)
        else:
            row = connection.execute(
                """SELECT * FROM copy_analysis_finalist_recommendations
                   WHERE analysis_run_id=? AND config_fingerprint=? AND wallet=?""",
                (run_id, fingerprint, wallet.lower()),
            ).fetchone()
            recommendation = dict(row) if row else None
            if recommendation:
                recommendation["finalist_eligible"] = bool(recommendation["finalist_eligible"])
                recommendation["finalist_rejection_reasons"] = _load(
                    recommendation.pop("finalist_rejection_reasons_json"), []
                )
        if not recommendation or recommendation.get("analysis_run_id") != run_id:
            raise ValueError("Wallet cannot be activated because it lacks a current persisted Phase B finalist recommendation. No state change was made.")
        if recommendation.get("recommendation_schema_version") != PHASE_B_RECOMMENDATION_SCHEMA_VERSION:
            raise ValueError("Wallet cannot be activated because its persisted Phase B finalist recommendation uses an unsupported contract version. No state change was made.")
        if not recommendation["finalist_eligible"]:
            reasons = ", ".join(str(reason) for reason in recommendation["finalist_rejection_reasons"]) or "not finalist eligible"
            raise ValueError(f"Wallet cannot be activated because Phase B rejected it as a finalist ({reasons}). No state change was made.")
        # ``finalist_eligible`` is the policy gate; rank is the independent
        # diversification decision.  Active is the paper execution cohort, so
        # it must be selected into the persisted diversified finalist set too.
        if recommendation.get("selection_rank") is None:
            raise ValueError("Wallet cannot be activated because Phase B did not select it into the current diversified finalist cohort. No state change was made.")
        return recommendation

    def _apply_operator_state(self, wallet: str, state: str, *, by: str, allow_overflow: bool) -> dict[str, Any]:
        """Persist a validated non-Active operator transition and its audit event."""
        if state == "active":
            raise ValueError("Direct Active transition is prohibited; use activate_wallet so Phase-B authority is validated.")
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError("Wallet was not found in the candidate universe.")
        before = target.status
        if not self.database.set_target_status(wallet, state):
            raise KeyError("Wallet was not found in the candidate universe.")
        self.store.record_activity(category="operator", severity="info", wallet=wallet,
            message=f"Trader state changed from {before} to {state}", payload={"from": before, "to": state, "by": by})
        active_count = len(self.database.list_targets("active"))
        return {"wallet": wallet.lower(), "operator_state": state, "previous_state": before, "paper_only": True,
                "active_count_after": active_count, "recommended_max": 7,
                "cohort_over_recommended_size": active_count > 7, "allow_overflow": allow_overflow}

    def _authoritative_score(
        self, wallet: str, run_id: str | None, *, connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        if not run_id:
            return None
        if connection is None:
            with self._connect() as query_connection:
                row = query_connection.execute(
                    """SELECT total_score, eligible, component_scores_json, penalties_json, reasons_json, source_quality,
                       provenance, analysis_run_id, config_fingerprint, confidence_score, hard_gates_json, score_version
                       FROM copy_candidate_scores WHERE target_wallet=? AND analysis_run_id=? AND provenance='phase_b'
                       LIMIT 1""",
                    (wallet.lower(), run_id),
                ).fetchone()
        else:
            row = connection.execute(
                """SELECT total_score, eligible, component_scores_json, penalties_json, reasons_json, source_quality,
                   provenance, analysis_run_id, config_fingerprint, confidence_score, hard_gates_json, score_version
                   FROM copy_candidate_scores WHERE target_wallet=? AND analysis_run_id=? AND provenance='phase_b' LIMIT 1""",
                (wallet.lower(), run_id),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        return {"total_score": item["total_score"], "eligible": bool(item["eligible"]),
                "component_scores": _load(item["component_scores_json"], {}), "penalties": _load(item["penalties_json"], {}),
                "reasons": _load(item["reasons_json"], []), "source_quality": float(item["source_quality"]),
                "provenance": item["provenance"], "analysis_run_id": item["analysis_run_id"],
                "config_fingerprint": item["config_fingerprint"], "confidence_score": float(item["confidence_score"] or 0),
                "hard_gates": _load(item["hard_gates_json"], []),
                "score_version": item["score_version"]}

    def _latest_legacy_score(self, wallet: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT total_score, eligible FROM copy_candidate_scores WHERE target_wallet=? AND provenance!='phase_b'
                   ORDER BY calculated_at DESC LIMIT 1""", (wallet.lower(),)
            ).fetchone()
        return {"total_score": row["total_score"], "eligible": bool(row["eligible"])} if row else None

    def active_cohort(self) -> dict[str, Any]:
        active = [target for target in self.database.list_targets("active")]
        pnl = {row["wallet"]: row for row in self._pnl_by_trader()}
        members = []
        for target in active:
            detail = self.candidate_detail(target.wallet)
            row = detail["table_row"] if detail else {}
            members.append({"wallet": target.wallet, "label": target.label, "score": row.get("score"), "open_pnl": pnl.get(target.wallet, {}).get("open_pnl", 0.0),
                            "total_pnl": pnl.get(target.wallet, {}).get("total_pnl", 0.0), "drawdown": pnl.get(target.wallet, {}).get("max_drawdown", 0.0),
                            "allocation_policy": "Dynamic 5/10/20", "operator_state": "active", "research_state": row.get("research_state")})
        return {"target_size": "5–7", "count": len(members), "members": members, "paper_only": True}

    def positions(self, *, wallet: str | None = None, symbol: str | None = None, direction: str | None = None) -> list[dict[str, Any]]:
        positions = self.database.list_virtual_positions(open_only=True)
        now = utc_now()
        result = []
        for position in positions:
            if wallet and position.target_wallet != wallet.lower():
                continue
            if symbol and position.symbol.upper() != symbol.upper():
                continue
            if direction and position.direction.lower() != direction.lower():
                continue
            age = max(0.0, (now - position.opened_at).total_seconds())
            mark_age = max(0.0, (now - position.updated_at).total_seconds() * 1000)
            result.append({
                "paper": True, "sleeve_id": position.sleeve_id, "target_wallet": position.target_wallet, "symbol": position.symbol,
                "direction": position.direction, "quantity": position.quantity, "entry_price": position.entry_price, "current_mark": position.current_mark,
                "target_entry_price": position.target_entry_price, "allocated_capital": position.allocated_capital,
                "remaining_capital": position.remaining_capital,
                "allocation_bucket": getattr(position, "sizing_bucket", "unknown_legacy"),
                "allocation_fraction": getattr(position, "sizing_allocation_fraction", None),
                "unrealized_pnl": position.unrealized_pnl, "realized_pnl": position.realized_pnl, "fees": position.entry_fee + position.exit_fee,
                "opened_at": iso(position.opened_at), "age_seconds": age, "campaign_id": position.campaign_id,
                "mark_fresh": mark_age <= self.config.paper_execution.market_data_max_age_ms, "mark_age_ms": mark_age,
                "max_drawdown": position.max_drawdown,
            })
        return result

    def portfolio_summary(self) -> dict[str, Any]:
        snapshot = self.database.latest_portfolio_snapshot()
        open_positions = self.database.list_virtual_positions(open_only=True)
        all_positions = self.database.list_virtual_positions()
        committed = sum(position.remaining_capital for position in open_positions)
        open_pnl = sum(position.unrealized_pnl for position in open_positions)
        fees = sum(position.entry_fee + position.exit_fee for position in all_positions)
        realized_total = sum(position.realized_pnl - position.entry_fee for position in all_positions)
        today = utc_now().date()
        realized_today = sum(position.realized_pnl - position.entry_fee for position in all_positions if position.closed_at and position.closed_at.date() == today)
        equity = float(snapshot["equity"]) if snapshot else self.config.capital.initial_capital + realized_total + open_pnl
        cash = float(snapshot["cash"]) if snapshot else self.config.capital.initial_capital - committed + realized_total
        curve = self._portfolio_curve()
        current_dd = float(snapshot["drawdown_fraction"]) if snapshot else 0.0
        max_dd = max([float(point.get("drawdown_fraction") or 0.0) for point in curve] + [current_dd])
        return {
            "paper_only": True, "equity": equity, "cash": cash, "committed_capital": committed, "open_pnl": open_pnl,
            "realized_pnl_today": realized_today, "realized_pnl_total": realized_total, "fees": fees,
            "current_drawdown": current_dd, "max_drawdown": max_dd, "active_wallets": len({item.target_wallet for item in open_positions}),
            "open_positions": len(open_positions), "equity_curve": curve, "drawdown_curve": [{"timestamp": point["timestamp"], "value": point.get("drawdown_fraction", 0.0)} for point in curve],
            "pnl_by_trader": self._pnl_by_trader(), "pnl_by_symbol": self._pnl_by_symbol(), "pnl_by_bucket": self._pnl_by_bucket(),
        }

    def _portfolio_curve(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM copy_portfolio_snapshots ORDER BY timestamp DESC LIMIT 500").fetchall()
        return [dict(row) for row in reversed(rows)]

    def _pnl_by_trader(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT target_wallet, SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END) AS open_pnl,
                   SUM(realized_pnl-entry_fee) AS realized_pnl, SUM(entry_fee+exit_fee) AS fees,
                   SUM(CASE WHEN closed_at IS NULL THEN remaining_capital ELSE 0 END) AS capital_usage,
                   MAX(max_drawdown) AS max_drawdown FROM copy_virtual_positions GROUP BY target_wallet ORDER BY (SUM(realized_pnl-entry_fee)+SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END)) DESC"""
            ).fetchall()
        return [{**dict(row), "total_pnl": float(row["open_pnl"] or 0) + float(row["realized_pnl"] or 0)} for row in rows]

    def _pnl_by_symbol(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT symbol, SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END) AS open_pnl,
                   SUM(realized_pnl-entry_fee) AS realized_pnl, SUM(CASE WHEN closed_at IS NULL THEN remaining_capital ELSE 0 END) AS exposure,
                   SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS position_count FROM copy_virtual_positions GROUP BY symbol ORDER BY exposure DESC"""
            ).fetchall()
        return [{**dict(row), "total_pnl": float(row["open_pnl"] or 0) + float(row["realized_pnl"] or 0)} for row in rows]

    def _pnl_by_bucket(self) -> list[dict[str, Any]]:
        values: dict[str, dict[str, float]] = {}
        for position in self.database.list_virtual_positions():
            bucket = getattr(position, "sizing_bucket", "unknown_legacy")
            row = values.setdefault(bucket, {"bucket": bucket, "open_pnl": 0.0, "realized_pnl": 0.0, "capital_usage": 0.0, "position_count": 0.0})
            row["position_count"] += 1
            row["realized_pnl"] += position.realized_pnl - position.entry_fee
            if position.is_open:
                row["open_pnl"] += position.unrealized_pnl
                row["capital_usage"] += position.remaining_capital
        return [{**item, "total_pnl": item["open_pnl"] + item["realized_pnl"]} for item in values.values()]

    def risk_panel(self) -> dict[str, Any]:
        portfolio = self.portfolio_summary()
        capital = max(self.config.capital.initial_capital, 1e-12)
        symbol = portfolio["pnl_by_symbol"]
        top_symbol = max([float(row.get("exposure") or 0.0) for row in symbol], default=0.0)
        return {"paper_only": True, "kill_switch": self.config.risk.kill_switch_path.exists(), "entry_control": self.store.control_state(),
                "limits": [
                    {"label": "Capital committed", "current": portfolio["committed_capital"] / capital, "limit": self.config.risk.max_total_committed_fraction},
                    {"label": "Largest symbol concentration", "current": top_symbol / capital, "limit": self.config.risk.max_capital_per_symbol_fraction},
                    {"label": "Portfolio drawdown", "current": portfolio["current_drawdown"], "limit": self.config.risk.max_copy_drawdown_fraction},
                    {"label": "Daily realized loss", "current": max(0.0, -portfolio["realized_pnl_today"]) / capital, "limit": self.config.risk.daily_loss_stop_fraction},
                ]}

    def execution_health(self) -> dict[str, Any]:
        """Versioned Phase-D visibility from ledger state, never live transport."""
        result = self.database.execution_read_model()
        shadow = self.config.shadow_observation
        if shadow.enabled:
            from .shadow import SHADOW_EXECUTION_DOMAIN, shadow_execution_account_id
            account_id = shadow.account_id.lower()
            result["shadow"] = self.database.shadow_read_model(
                configured=True, venue=shadow.venue.lower(), account_id=account_id,
                execution_domain=SHADOW_EXECUTION_DOMAIN,
                execution_account_id=shadow_execution_account_id(shadow.venue, account_id),
            )
        return result

    def refresh_shadow_observation(self) -> dict[str, Any]:
        """Perform one remote read-only D.4 refresh and persist its evidence."""
        shadow = self.config.shadow_observation
        if not shadow.enabled:
            return self.database.shadow_read_model(configured=False)
        from .shadow import HyperliquidReadOnlyShadowAdapter, ShadowObservationService
        adapter = self._shadow_adapter or HyperliquidReadOnlyShadowAdapter(self.config.source)
        return ShadowObservationService(self.database, adapter, shadow).refresh()

    def activity(self, *, limit: int = 100, wallet: str | None = None) -> list[dict[str, Any]]:
        manual = self.store.activities(limit=limit, wallet=wallet)
        with self._connect() as connection:
            filters, values = [], []
            if wallet:
                filters.append("target_wallet=?")
                values.append(wallet.lower())
            where = " WHERE " + " AND ".join(filters) if filters else ""
            attempts = connection.execute(
                "SELECT * FROM copy_execution_attempts" + where + " ORDER BY decided_at DESC LIMIT ?", [*values, limit]
            ).fetchall()
        generated = []
        for attempt in attempts:
            item = dict(attempt)
            verb = "filled" if item["status"] == "filled" else item["status"]
            generated.append({"event_id": item["attempt_id"], "occurred_at": item["decided_at"], "category": "execution",
                              "severity": "info" if item["status"] == "filled" else "warning", "wallet": item["target_wallet"], "symbol": item["symbol"],
                              "message": f"Paper {item['action']} {verb}: {item['symbol']}", "payload": {"reason": item["reason"], "paper": True}})
        return sorted([*manual, *generated], key=lambda item: str(item["occurred_at"]), reverse=True)[:limit]

    def pause_entries(self) -> dict[str, Any]:
        return self.store.set_control_state(CONTROL_ENTRIES_PAUSED, note="New PAPER entries paused; exits remain enabled.")

    def resume_entries(self) -> dict[str, Any]:
        return self.store.set_control_state(CONTROL_RUNNING, note="New PAPER entries resumed.")

    def close_all_paper_positions(self, *, pause_after: bool = False) -> dict[str, Any]:
        """Delegate control closing to the service-owned serialized engine."""
        return self._paper_service().close_all_paper_positions(pause_after=pause_after)

    def exit_and_pause(self) -> dict[str, Any]:
        return self.close_all_paper_positions(pause_after=True)


class CandidateDiscoveryOrchestrator:
    """Run official-source acquisition around, never inside, frozen Phase A."""

    def __init__(self, service: Any, store: ControlCenterStore, source: HyperCoreSourceAcquisition) -> None:
        self.service = service
        self.store = store
        self.source = source

    def run(self, job_id: str) -> None:
        job = self.store.get_job(job_id)
        if not job:
            return
        configuration = dict(job.get("configuration") or {})
        try:
            self.store.update_job(job_id, status="acquiring", stage="source_resolution", message="Resolving recent official HyperCore UTC-hour source objects.", started=True)
            objects = self.source.resolve_hourly_objects(int(configuration["source_hour_count"]))
            if self._cancelled(job_id):
                return
            preflight = self.source.preflight(objects)
            self.store.update_job(
                job_id, status="acquiring", stage="preflight", message=(
                    f"Preflight complete: {preflight['objects_planned']} source hours; "
                    f"{preflight['objects_cached']} cached; {preflight['bytes_to_download']} bytes to download."
                ), progress_current=preflight["objects_cached"], progress_total=len(objects), result={"source_plan": preflight},
            )
            if self._cancelled(job_id):
                return
            protected_paths = set(preflight["protected_paths"])
            cached_identifiers = set(preflight["cached_source_identifiers"])
            self.store.update_job(job_id, status="acquiring", stage="acquisition", message="Acquiring preflighted official HyperCore source objects.", progress_current=0, progress_total=len(objects))
            cached_paths, provenance = [], []
            bytes_acquired, bytes_reused = 0, 0
            for index, source_object in enumerate(objects, 1):
                if self._cancelled(job_id):
                    return
                path, metadata = self.source.acquire(source_object, protected_paths=protected_paths)
                cached_paths.append(str(path))
                provenance.append(metadata)
                if source_object.identifier in cached_identifiers:
                    bytes_reused += source_object.size
                else:
                    bytes_acquired += source_object.size
                self.store.update_job(job_id, status="acquiring", stage="acquisition", message=f"Acquired {index} of {len(objects)} official hourly source objects.", progress_current=index, progress_total=len(objects))
            if self._cancelled(job_id):
                return
            self.store.update_job(job_id, status="parsing", stage="parsing", message="Preparing cached HyperCore fills for frozen Phase A discovery.")
            # Phase A remains the only parser/normalizer and the only writer of
            # discovery evidence.  Cached objects take the identical local-file
            # path used by the pre-existing reproducible CLI command.
            provider = build_discovery_provider("hypercore-file", cached_paths)
            self.store.update_job(job_id, status="discovering", stage="discovering", message="Running frozen Phase A candidate discovery.")
            summary = self.service.discover_candidates(
                provider, limit=int(configuration["candidate_limit"]), min_activity=int(configuration["min_activity"]),
                refresh=False, max_activity_age=parse_activity_age(str(configuration["max_activity_age"])),
                configuration={
                    "source": "official_hypercore_requester_pays_cache",
                    "source_transport": "aws_s3_requester_pays",
                    "official_source_identifier": "s3://hl-mainnet-node-data/node_fills_by_block/",
                    "source_hour_count": int(configuration["source_hour_count"]),
                    "preset": configuration["preset"], "objects": provenance,
                },
            )
            result = {
                "discovery_run_id": summary.run_id, "status": summary.status, "wallets_observed": summary.wallets_seen,
                "eligible_wallets": summary.eligible_wallets, "registered_candidates": summary.new_wallets + summary.existing_wallets_refreshed,
                "new_candidates": summary.new_wallets, "existing_refreshed": summary.existing_wallets_refreshed,
                "filtered": summary.filtered_wallets, "deferred_by_limit": summary.limit_deferred_wallets,
                "invalid": len(summary.errors), "source_objects": len(objects), "hourly_objects": len(objects),
                "source_first_hour": objects[0].data_hour_start, "source_last_hour": objects[-1].data_hour_start,
                "bytes_acquired": bytes_acquired, "bytes_reused_from_cache": bytes_reused,
                "source_plan": preflight, "source_metadata": provenance,
            }
            status = "completed_with_warnings" if summary.errors else "completed"
            message = "Candidate discovery completed with warnings." if summary.errors else "Candidate discovery completed."
            self.store.update_job(job_id, status=status, stage="completed", message=message, result=result, finished=True)
            self.store.record_activity(category="discovery", severity="warning" if summary.errors else "info", message=message,
                                       payload={"job_id": job_id, **result})
        except HyperCoreSourceError as exc:
            current = self.store.get_job(job_id) or {}
            self._fail(job_id, str(exc), str(current.get("stage") or "source_access"))
        except Exception as exc:  # Phase A errors stay auditable without taking FastAPI down.
            self._fail(job_id, str(exc), "discovery")

    def _cancelled(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if not job or not job.get("cancellation_requested"):
            return False
        self.store.update_job(job_id, status="cancelled", stage="cancelled", message="Discovery cancelled before frozen Phase A processing started.", finished=True)
        return True

    def _fail(self, job_id: str, message: str, stage: str) -> None:
        self.store.update_job(job_id, status="failed", stage=stage, message=message, error={"message": message}, finished=True)
        self.store.record_activity(category="discovery", severity="warning", message="Candidate discovery failed.", payload={"job_id": job_id, "error": message, "stage": stage})


def discovery_job_configuration(body: dict[str, Any] | None) -> dict[str, Any]:
    """Accept small bounded operator overrides; never source keys, URLs, or paths."""
    body = body or {}
    preset = discovery_preset(str(body.get("preset") or "standard"))
    limit = int(body.get("candidate_limit", preset["candidate_limit"]))
    min_activity = int(body.get("min_activity", preset["min_activity"]))
    window_hours = float(body.get("window_hours", preset["hourly_object_count"]))
    max_activity_age = str(body.get("max_activity_age", preset["max_activity_age"]))
    if not 1 <= limit <= 5_000:
        raise ValueError("Candidate limit must be between 1 and 5000.")
    if not 1 <= min_activity <= 100:
        raise ValueError("Minimum activity must be between 1 and 100.")
    if not 0 < window_hours <= 24:
        raise ValueError("Source window must be greater than zero and no more than 24 hours.")
    parse_activity_age(max_activity_age)
    return {"preset": preset["preset"], "candidate_limit": limit, "min_activity": min_activity,
            "max_activity_age": max_activity_age,
            "source_hour_count": max(1, int(window_hours) if window_hours.is_integer() else int(window_hours) + 1),
            "window_hours": window_hours}


def create_control_center_app(
    config: CopyTradeConfig, database: CopyTradeDatabase | None = None, watcher_health: dict[str, Any] | Any | None = None,
    *, watcher_service: Any | None = None, watcher_factory: Any | None = None,
    watcher_poll_interval_seconds: float = 1.0, watcher_retry_delay_seconds: float = 3.0,
    watcher_stop_timeout_seconds: float = 3.0, discovery_source: HyperCoreSourceAcquisition | None = None,
    ninjatrader_listener_factory: Callable[[], NinjaTraderListenerWorker] | None = None,
    lane_iii_shadow_factory: Callable[[], LaneIIIShadowRuntime] | None = None,
    lane_iii_paper_factory: Callable[[PaperLedger], LaneIIIPaperRuntime] | None = None,
    paper_execution_transport_factory: Callable[[PaperLedger, Callable[[dict[str, object]], None], Callable[[str], None]], PaperExecutionTransport] | None = None,
    paper_ledger_factory: Callable[[Path], PaperLedger] | None = None,
    ninjatrader_login_bootstrap_factory: Callable[[], NinjaTraderLoginBootstrap] | None = None,
) -> Any:
    """Create the local FastAPI Phase C application; no live-trading routes exist."""
    try:
        from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
        from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:  # pragma: no cover - dependency guidance
        raise RuntimeError("copy-control-center requires fastapi and uvicorn; install requirements.txt.") from exc

    # ``from __future__ import annotations`` stores endpoint annotations as
    # strings.  FastAPI resolves those in module globals, while this optional
    # dependency is intentionally imported only when creating the app.
    # Publish the injected type so the /ws parameter is recognized as a
    # WebSocket instead of being interpreted as an HTTP query parameter.
    globals()["WebSocket"] = WebSocket

    # Import lazily: CopyTradeService itself owns Phase C control-state setup.
    from .service import CopyTradeService
    execution_service = watcher_service or CopyTradeService(config, database)
    center = CopyControlCenter(config, execution_service.database, execution_service=execution_service)
    watcher_runtime: dict[str, Any] = {}
    ninjatrader_runtime: dict[str, Any] = {}
    paper_ledger_shutdown_receipt: dict[str, object] | None = None
    job_runtime: dict[str, asyncio.Task[Any]] = {}
    source = discovery_source or HyperCoreSourceAcquisition(cache_directory(config.artifacts.database_path))
    discovery_orchestrator = CandidateDiscoveryOrchestrator(execution_service, center.store, source)
    configured_paper_path = os.getenv("BEELZEBUB_L3G_PAPER_LEDGER")
    paper_path = (
        Path(configured_paper_path).expanduser().resolve()
        if configured_paper_path
        else Path(config.artifacts.database_path).resolve().with_name("lane_iii_paper.sqlite3")
    )
    configured_paper_epoch = os.getenv("BEELZEBUB_L3G_PAPER_LEDGER_EPOCH")
    derived_audit_root = paper_path.parent.parent / "audit" if paper_path.parent.name.lower() == "hot" else paper_path.parent / "audit"
    audit_root = Path(os.getenv("BEELZEBUB_LEDGER_AUDIT_ROOT") or derived_audit_root).resolve()
    ledger_verifier = LocalLedgerVerificationController(paper_path, audit_root)
    runtime_binding = {
        "ledger": str(paper_path),
        "audit": str(audit_root),
        "control_center": "127.0.0.1:8090",
        "python": sys.executable,
        "pid": os.getpid(),
        "git_sha": _runtime_git_sha(),
    }

    def live_watcher_health() -> dict[str, Any] | None:
        supervisor = watcher_runtime.get("supervisor")
        if supervisor is not None:
            return supervisor.health()
        if callable(watcher_health):
            return watcher_health()
        return watcher_health

    def refresh_watcher_membership() -> None:
        supervisor = watcher_runtime.get("supervisor")
        if supervisor is not None:
            supervisor.wake()

    def unobserved_account_balances() -> dict[str, dict[str, object]]:
        """Stable display shape before the read-only listener receives a value."""
        return {
            "Sim101": {
                "alias": "Sim101", "account_class": "LOCAL_SIMULATION",
                "cash_value": None, "cash_value_observed_at": None,
                "net_liquidation": None, "net_liquidation_observed_at": None,
                "realized_pnl": None, "realized_pnl_observed_at": None,
                "unrealized_pnl": None, "unrealized_pnl_observed_at": None,
            },
            "Lucid25kflex01": {
                "alias": "Lucid25kflex01", "account_class": "PROVIDER_EVALUATION",
                "cash_value": None, "cash_value_observed_at": None,
                "net_liquidation": None, "net_liquidation_observed_at": None,
                "realized_pnl": None, "realized_pnl_observed_at": None,
                "unrealized_pnl": None, "unrealized_pnl_observed_at": None,
            },
        }

    def ninja_listener_health() -> dict[str, object]:
        listener = ninjatrader_runtime.get("listener")
        if listener is None:
            return {
                "state": "UNSTARTED",
                "host": "127.0.0.1",
                "port": 48135,
                "error": None,
                "start_attempts": 0,
                "accepted_observations": 0,
                "last_observation_at": None,
                "observation_types": {},
                "market_observer_state": "NOT_ACTIVE",
                "market_observer_active": False,
                "market_observer_level_one_received": False,
                "market_observer_depth_received": False,
                "last_level_one_at": None,
                "last_depth_at": None,
                "market_observer_freshness": {
                    "timestamp": None,
                    "threshold_seconds": MARKET_OBSERVER_ACTIVE_FRESHNESS_SECONDS,
                    "age_seconds": None,
                    "fresh": False,
                    "reason": "MISSING_OBSERVATION_TIMESTAMP",
                },
                "account_balances": unobserved_account_balances(),
                "authority": "OBSERVE_ONLY",
            }
        value = listener.status().as_dict()
        value.setdefault("account_balances", unobserved_account_balances())
        freshness = _authentic_observation_freshness(
            {"last_observation_at": value.get("last_level_one_at")},
            MARKET_OBSERVER_ACTIVE_FRESHNESS_SECONDS,
        )
        received = value.get("market_observer_level_one_received") is True
        value.update({
            "market_observer_active": freshness["fresh"],
            "market_observer_state": "ACTIVE" if freshness["fresh"] else "STALE" if received else "NOT_ACTIVE",
            "market_observer_freshness": freshness,
        })
        return value

    def ninja_login_health() -> dict[str, object]:
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is None:
            return {
                "schema": "lane-iii-phase-g-ninjatrader-login-bootstrap-v1",
                "state": "UNSTARTED",
                "attempt_count": 0,
                "ninjatrader_process_detected": False,
                "login_window_detected": False,
                "control_center_detected": False,
                "lucid_connection_state": "UNKNOWN",
                "failure_category": None,
            }
        return bootstrap.status()

    def lane_iii_shadow_health() -> dict[str, object]:
        shadow = ninjatrader_runtime.get("shadow")
        if shadow is None:
            return {
                "schema": "lane-iii-live-shadow-v1",
                "mode": "LANE_III_SHADOW",
                "state": "UNSTARTED",
                "authority": {
                    "observation": "OBSERVE_ONLY",
                    "interpretation": "SHADOW_ONLY",
                    "decision": "SHADOW_ONLY",
                    "execution": "DENIED",
                    "live_capital": "DENIED",
                },
            }
        return shadow.status()

    def lane_iii_paper_health() -> dict[str, object]:
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            observer = ninja_listener_health()
            return {
                "schema": "lane-iii-phase-g-paper-runtime-status-v1",
                "mode": "PAPER_SIM101",
                "display_mode": "EXPERIMENTAL PAPER",
                "state": "UNSTARTED",
                "paper_execution": "DISARMED",
                "scientific_lane_iii": "INCOMPLETE / BLOCKED ON SEQUENCING",
                "scientific_eligibility": False,
                "paper_account": "Sim101",
                "account_class": "LOCAL_SIMULATION",
                "market_instrument": "MNQ SEP26",
                "maximum_quantity": 1,
                "live_capital": "DENIED",
                "account_balances": observer["account_balances"],
                "market_observer": observer,
                "ledger_verification": ledger_verifier.status(),
                "ledger_shutdown": paper_ledger_shutdown_receipt,
            }
        status = paper.status()
        verification = ledger_verifier.status()
        closure = status.get("last_commissioning_closure")
        if isinstance(closure, Mapping):
            status["commissioning_post_run_verification"] = evaluate_commissioning_post_run_verification(
                closure,
                verification,
                checkpoint_matches_report=ledger_verifier.checkpoint_matches_report(verification),
            )
        status["market_observer"] = ninja_listener_health()
        status["account_balances"] = status["market_observer"]["account_balances"]
        status["ledger_verification"] = verification
        status["ledger_shutdown"] = paper_ledger_shutdown_receipt
        raw_ledger = status.get("ledger")
        if isinstance(raw_ledger, Mapping):
            status["ledger"] = ledger_health_projection(
                raw_ledger,
                verification,
                operational_session=(
                    status.get("operational_paper_session")
                    if isinstance(status.get("operational_paper_session"), Mapping)
                    else None
                ),
            )
        return status

    def lane_iii_live_health() -> dict[str, object]:
        """Expose only the L3H fail-closed shell until a local capability is verified.

        Deliberately do not construct a live runtime here: an HTTP status read
        must not create a key, capability, ledger, listener, or order path.
        """
        status_path = os.environ.get("BEELZEBUB_L3H_GATEWAY_STATUS_PATH")
        authorization_path = os.environ.get("BEELZEBUB_L3H3_STATUS_PATH")
        if not authorization_path and status_path:
            authorization_path = str(Path(status_path).with_name("l3h3-live-authorization-status.json"))
        return fail_closed_status(
            mechanical_status_path=None if not status_path else Path(status_path),
            authorization_status_path=None if not authorization_path else Path(authorization_path),
        )

    async def quiesce_ledger_verifier_for_shutdown() -> dict[str, object]:
        """Release a read-only verifier snapshot before the writer truncates WAL.

        The verifier intentionally owns a consistent read transaction for an
        entire scan.  A controlled writer shutdown must request its bounded
        cancellation and wait for its reader to close rather than treating a
        checkpoint ``busy`` result as a mysterious ledger failure.
        """
        initial = ledger_verifier.status()
        result: dict[str, object] = {
            "was_running": initial.get("status") == "IN_PROGRESS",
            "initial_verification_id": initial.get("verification_id"),
            "cancellation_requested": False,
            "completed": initial.get("status") != "IN_PROGRESS",
        }
        if initial.get("status") != "IN_PROGRESS":
            result["final_status"] = initial.get("status")
            return result
        try:
            ledger_verifier.cancel()
            result["cancellation_requested"] = True
        except Exception as error:
            result["error"] = f"{type(error).__name__}: {error}"
            result["final_status"] = ledger_verifier.status().get("status")
            return result
        deadline = asyncio.get_running_loop().time() + LEDGER_VERIFIER_SHUTDOWN_WAIT_SECONDS
        while True:
            current = ledger_verifier.status()
            if current.get("status") != "IN_PROGRESS":
                result["completed"] = True
                result["final_status"] = current.get("status")
                result["final_verification_id"] = current.get("verification_id")
                return result
            if asyncio.get_running_loop().time() >= deadline:
                result["completed"] = False
                result["final_status"] = current.get("status")
                result["error"] = "Verifier reader did not stop before the controlled-shutdown deadline."
                return result
            await asyncio.sleep(0.1)

    scheduler_path = config.scheduler.database_path or Path(config.artifacts.database_path).resolve().with_name("beelzebub_operations.sqlite3")
    scheduler_store = OperationsStore(scheduler_path, max_result_bytes=config.scheduler.max_result_bytes, max_event_bytes=config.scheduler.max_event_bytes)
    scheduler_store.initialize()
    scheduler_settings = SchedulerSettings(
        enabled=config.scheduler.enabled,
        default_timezone=config.scheduler.default_timezone,
        poll_interval_seconds=config.scheduler.poll_interval_seconds,
        leader_lease_seconds=config.scheduler.leader_lease_seconds,
        run_lease_seconds=config.scheduler.run_lease_seconds,
        heartbeat_seconds=config.scheduler.heartbeat_seconds,
        cancellation_grace_seconds=config.scheduler.cancellation_grace_seconds,
        max_concurrent_runs=config.scheduler.max_concurrent_runs,
        maximum_catch_up_runs=config.scheduler.maximum_catch_up_runs,
        default_max_lateness_seconds=config.scheduler.default_max_lateness_seconds,
    )
    scheduler_engine = SchedulerEngine(
        scheduler_store,
        TaskRegistry(production_task_definitions(), include_commissioning_probes=config.scheduler.commissioning_probes_enabled),
        settings=scheduler_settings,
        dependencies={
            "control_center_health": lambda: center.health(live_watcher_health),
            "watcher_health": live_watcher_health,
            "ninja_listener_health": ninja_listener_health,
            "lane_iii_paper_health": lane_iii_paper_health,
            "ledger_verification_controller": lambda: ledger_verifier,
            "scheduler_status": lambda: scheduler_engine.status(),
            "database_paths": lambda: {"application": config.artifacts.database_path, "scheduler": scheduler_path},
            "scientific_worker": lambda: getattr(execution_service, "scientific_worker", None) or getattr(center._paper_service(), "scientific_worker", None),
            "audit_export_directory": Path(__file__).resolve().parents[2] / "logs" / "scheduler-audits",
        },
    )
    scheduler_service = SchedulerService(scheduler_store, scheduler_engine.registry, scheduler_engine)

    @asynccontextmanager
    async def lifespan(_: Any) -> Any:
        # The Control Center application owns this worker for its entire
        # lifespan. It is deliberately outside routes, views, and websocket
        # connections so refreshes/remounts cannot create another listener.
        nonlocal paper_ledger_shutdown_receipt
        if ninjatrader_runtime.get("active"):
            raise RuntimeError("NINJATRADER_OBSERVER duplicate FastAPI lifespan refused")
        ninjatrader_runtime["active"] = True
        listener: NinjaTraderListenerWorker | None = None
        paper_transport: PaperExecutionTransport | None = None
        paper_runtime: LaneIIIPaperRuntime | None = None
        paper_ledger: PaperLedger | None = None
        login_bootstrap: NinjaTraderLoginBootstrap | None = None
        verifier_shutdown: dict[str, object] | None = None
        runtime_watchdog_shutdown: dict[str, object] = {
            "required": False,
            "completed": True,
            "reason": None,
        }
        watchdog_shutdown_incomplete = False

        async def retain_execution_transport_for_watchdog() -> dict[str, object]:
            """Keep the signed AddOn connected through a bounded failsafe window."""
            if paper_runtime is None:
                return {"required": False, "completed": True, "reason": "RUNTIME_UNAVAILABLE"}
            current = paper_runtime.watchdog_shutdown_status()
            if current.get("flat_confirmed") is True:
                # A physical flat state received through the transport's
                # ledger-outage fallback is useful safety truth, but it is
                # not durable closing evidence. Never promote it into a
                # clean controlled-shutdown receipt after the ledger recovers.
                return {
                    **current,
                    "completed": current.get("durable_confirmation") is not False,
                }
            if current.get("required") is not True:
                return {**current, "completed": False}
            deadline = time.monotonic() + max(0.0, float(current.get("remaining_seconds") or 0.0))
            while True:
                current = paper_runtime.watchdog_shutdown_status()
                if current.get("flat_confirmed") is True:
                    return {
                        **current,
                        "completed": current.get("durable_confirmation") is not False,
                    }
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # The grace exceeds the AddOn watchdog interval.  Never
                    # claim a confirmed flat state merely because the
                    # independently owned action was requested.
                    return {**current, "completed": False}
                await asyncio.sleep(min(0.1, remaining))
        try:
            shadow = lane_iii_shadow_factory() if lane_iii_shadow_factory is not None else LaneIIIShadowRuntime()
            if type(shadow) is not LaneIIIShadowRuntime:
                raise RuntimeError("LANE_III_SHADOW factory must return the exact shadow runtime")
            ninjatrader_runtime["shadow"] = shadow
            app.state.lane_iii_shadow = shadow
            _assert_hot_paper_ledger_path(paper_path, config.storage.cold_root)
            if paper_ledger_factory is None and not paper_path.exists and configured_paper_path and resolve_ledger_epoch(paper_path, configured_paper_epoch) == "UNSPECIFIED":
                raise RuntimeError(
                    "New production paper ledger requires BEELZEBUB_L3G_PAPER_LEDGER_EPOCH or an epoch-N directory."
                )
            paper_ledger = paper_ledger_factory(paper_path) if paper_ledger_factory is not None else PaperLedger(paper_path, epoch_id=configured_paper_epoch)
            if type(paper_ledger) is not PaperLedger:
                raise RuntimeError("LANE_III_PAPER ledger factory must return the exact durable ledger")
            paper_runtime = lane_iii_paper_factory(paper_ledger) if lane_iii_paper_factory is not None else LaneIIIPaperRuntime(paper_ledger)
            if type(paper_runtime) is not LaneIIIPaperRuntime:
                raise RuntimeError("LANE_III_PAPER factory must return the exact paper runtime")
            paper_runtime.bind_runtime_identity(runtime_binding)
            paper_transport = (
                paper_execution_transport_factory(paper_ledger, paper_runtime.on_execution_message, paper_runtime.on_execution_bridge_state)
                if paper_execution_transport_factory is not None
                else PaperExecutionTransport(
                    paper_ledger,
                    on_message=paper_runtime.on_execution_message,
                    on_bridge_state=paper_runtime.on_execution_bridge_state,
                )
            )
            if type(paper_transport) is not PaperExecutionTransport:
                raise RuntimeError("LANE_III_PAPER transport factory must return the exact signed transport")
            paper_runtime.bind_transport(paper_transport)
            paper_runtime.start()
            paper_transport.start()
            fanout = ObservationFanout(
                shadow_observation=shadow.ingest,
                shadow_transport=shadow.on_transport_state,
                shadow_rejection=shadow.record_raw_rejection,
                shadow_duplicate=shadow.record_raw_duplicate,
                paper_observation=paper_runtime.ingest,
                paper_transport=paper_runtime.on_observation_transport_state,
                paper_rejection=paper_runtime.on_observation_rejection,
                paper_duplicate=paper_runtime.on_observation_duplicate,
                record_failure=paper_runtime.record_sink_failure,
            )
            ninjatrader_runtime["paper_ledger"] = paper_ledger
            ninjatrader_runtime["ledger_verifier"] = ledger_verifier
            ninjatrader_runtime["paper"] = paper_runtime
            ninjatrader_runtime["paper_transport"] = paper_transport
            ninjatrader_runtime["fanout"] = fanout
            app.state.lane_iii_paper = paper_runtime
            app.state.runtime_binding = runtime_binding
            NINJATRADER_RUNTIME_LOGGER.info(
                "BEELZEBUB_RUNTIME_BINDING ledger=%s audit=%s control_center=%s python=%s pid=%s git_sha=%s",
                runtime_binding["ledger"], runtime_binding["audit"], runtime_binding["control_center"],
                runtime_binding["python"], runtime_binding["pid"], runtime_binding["git_sha"],
            )
            app.state.lane_iii_paper_transport = paper_transport
            listener = (
                ninjatrader_listener_factory()
                if ninjatrader_listener_factory is not None
                else NinjaTraderListenerWorker(logger=NINJATRADER_RUNTIME_LOGGER)
            )
            listener.set_observation_sinks(
                on_observation=fanout.on_observation,
                on_local_bridge_state=fanout.on_transport_state,
                on_rejection=fanout.on_rejection,
                on_duplicate=fanout.on_duplicate,
            )
            ninjatrader_runtime["listener"] = listener
            app.state.ninjatrader_observer = listener
            listener_status = listener.start()
            if listener_status.state != "LISTENING":
                raise RuntimeError(
                    f"NINJATRADER_OBSERVER startup failed at {listener_status.host}:{listener_status.port}: "
                    f"{listener_status.error or listener_status.state}"
                )
            # Listener ownership is established before desktop bootstrap so a
            # newly started NinjaTrader process cannot build an offline market
            # callback queue or miss the signed execution handshake.
            if ninjatrader_login_bootstrap_factory is not None:
                login_bootstrap = ninjatrader_login_bootstrap_factory()
            elif ninjatrader_listener_factory is None:
                login_bootstrap = NinjaTraderLoginBootstrap()
            if login_bootstrap is not None:
                if type(login_bootstrap) is not NinjaTraderLoginBootstrap:
                    raise RuntimeError("NINJATRADER_LOGIN factory must return the exact sealed bootstrap")
                ninjatrader_runtime["login_bootstrap"] = login_bootstrap
                app.state.ninjatrader_login_bootstrap = login_bootstrap
                login_bootstrap.start()
            # A thread cannot be safely resumed after a process restart.  Preserve
            # its durable record and make the interruption explicit to operators.
            for job in center.store.list_jobs(job_type="candidate_discovery", limit=200):
                if job["status"] in {"queued", "acquiring", "parsing", "discovering"}:
                    center.store.update_job(job["job_id"], status="failed", stage="interrupted", finished=True,
                                            message="Control Center restarted before this discovery job completed.",
                                            error={"message": "Control Center restarted before this discovery job completed."})
            if watcher_service is not None:
                if watcher_factory is None:
                    from .hyperliquid import HyperliquidWatcher
                    factory = HyperliquidWatcher
                else:
                    factory = watcher_factory
                supervisor = WatcherMembershipSupervisor(
                    watcher_service, factory, center.store,
                    poll_interval_seconds=watcher_poll_interval_seconds,
                    retry_delay_seconds=watcher_retry_delay_seconds,
                    stop_timeout_seconds=watcher_stop_timeout_seconds,
                )
                watcher_runtime["supervisor"] = supervisor
                watcher_runtime["task"] = asyncio.create_task(supervisor.run())
            # The scheduler is lifespan-owned: routes, page refreshes, and
            # websocket clients only inspect this exact durable engine.
            await scheduler_engine.start()
            app.state.scheduler_engine = scheduler_engine
            app.state.scheduler_service = scheduler_service
            yield
        finally:
            supervisor = watcher_runtime.get("supervisor")
            task = watcher_runtime.get("task")
            try:
                await scheduler_engine.stop()
                if login_bootstrap is not None:
                    login_bootstrap.stop()
                if listener is not None:
                    listener.stop()
            finally:
                try:
                    if paper_runtime is not None:
                        try:
                            runtime_watchdog_shutdown = paper_runtime.stop()
                            runtime_watchdog_shutdown = await retain_execution_transport_for_watchdog()
                            watchdog_shutdown_incomplete = runtime_watchdog_shutdown.get("completed") is not True
                        except Exception as error:
                            # If Python cannot even obtain the independent
                            # watchdog result, it has no basis to tear down
                            # the signed callback path. Treat status failure
                            # exactly like an unresolved watchdog.
                            watchdog_shutdown_incomplete = True
                            runtime_watchdog_shutdown = {
                                "required": True,
                                "completed": False,
                                "flat_confirmed": False,
                                "reason": "WATCHDOG_SHUTDOWN_STATUS_UNAVAILABLE",
                                "error": f"{type(error).__name__}: {error}",
                            }
                            NINJATRADER_RUNTIME_LOGGER.exception(
                                "Could not confirm L3G independent AddOn watchdog shutdown state."
                            )
                        if watchdog_shutdown_incomplete:
                            NINJATRADER_RUNTIME_LOGGER.error(
                                "L3G independent AddOn watchdog grace elapsed without flat confirmation: %s",
                                runtime_watchdog_shutdown,
                            )
                finally:
                    if watchdog_shutdown_incomplete:
                        # Do not complete a normal FastAPI lifespan teardown
                        # while exact execution lacks a correlated, durable
                        # flat/order proof.  In particular, merely skipping
                        # transport.stop() and returning would orphan daemon
                        # threads and let the host kill the only watchdog
                        # path. Preserve singleton ownership and refuse the
                        # normal shutdown instead.
                        verifier_shutdown = {
                            "was_running": None,
                            "completed": False,
                            "skipped": True,
                            "reason": "INDEPENDENT_WATCHDOG_UNRESOLVED",
                        }
                        paper_ledger_shutdown_receipt = {
                            "schema": "l3g-ledger-controlled-shutdown-v1",
                            "closed_at": None,
                            "clean_shutdown": False,
                            "admission_sealed": False,
                            "writer_stopped": False,
                            "checkpoint": None,
                            "error": (
                                "WATCHDOG_FLAT_UNCONFIRMED_OR_NON_DURABLE: normal process shutdown "
                                "was refused; manual intervention is required."
                            ),
                            "transport_stop_skipped": True,
                            "ledger_close_deferred": True,
                            "lifecycle_shutdown_aborted": True,
                            "manual_intervention_required": True,
                            "verifier_shutdown": verifier_shutdown,
                            "runtime_watchdog_shutdown": runtime_watchdog_shutdown,
                        }
                        ninjatrader_runtime["unsafe_shutdown"] = paper_ledger_shutdown_receipt
                        NINJATRADER_RUNTIME_LOGGER.critical(
                            "L3G paper shutdown refused: %s", paper_ledger_shutdown_receipt,
                        )
                        raise UnsafePaperExecutionShutdown(
                            "L3G exact execution watchdog has no durable correlated flat confirmation."
                        )
                    try:
                        # A process-local shutdown must not sever the signed
                        # AddOn callback path while its independent watchdog
                        # is still the only available flatting authority.
                        # Leave both transport and writer running and publish
                        # an explicit failed/deferred receipt instead.
                        if paper_transport is not None:
                            paper_transport.stop()
                    finally:
                        try:
                            if supervisor is not None:
                                await supervisor.stop()
                        finally:
                            try:
                                if task is not None:
                                    await asyncio.gather(task, return_exceptions=True)
                            finally:
                                try:
                                    verifier_shutdown = await quiesce_ledger_verifier_for_shutdown()
                                except Exception as error:
                                    verifier_shutdown = {
                                        "was_running": None,
                                        "completed": False,
                                        "error": f"{type(error).__name__}: {error}",
                                    }
                                    NINJATRADER_RUNTIME_LOGGER.exception(
                                        "Could not quiesce L3G verifier before ledger shutdown."
                                    )
                                if verifier_shutdown.get("completed") is not True:
                                    paper_ledger_shutdown_receipt = {
                                        "schema": "l3g-ledger-controlled-shutdown-v1",
                                        "closed_at": None,
                                        "clean_shutdown": False,
                                        "admission_sealed": False,
                                        "writer_stopped": False,
                                        "checkpoint": None,
                                        "error": (
                                            "VERIFIER_READER_NOT_QUIESCED: controlled WAL checkpoint "
                                            "was refused; manual intervention is required."
                                        ),
                                        "transport_stop_skipped": False,
                                        "ledger_close_deferred": True,
                                        "lifecycle_shutdown_aborted": True,
                                        "manual_intervention_required": True,
                                        "verifier_shutdown": verifier_shutdown,
                                        "runtime_watchdog_shutdown": runtime_watchdog_shutdown,
                                    }
                                    ninjatrader_runtime["unsafe_shutdown"] = paper_ledger_shutdown_receipt
                                    NINJATRADER_RUNTIME_LOGGER.critical(
                                        "L3G ledger close refused because verifier did not quiesce: %s",
                                        paper_ledger_shutdown_receipt,
                                    )
                                    raise UnsafePaperExecutionShutdown(
                                        "L3G ledger verifier reader did not quiesce before controlled shutdown."
                                    )
                                watcher_runtime.clear()
                                ninjatrader_runtime.clear()
                                app.state.ninjatrader_observer = None
                                app.state.lane_iii_shadow = None
                                app.state.lane_iii_paper = None
                                app.state.lane_iii_paper_transport = None
                                app.state.ninjatrader_login_bootstrap = None
                                app.state.scheduler_engine = None
                                app.state.scheduler_service = scheduler_service
                                if paper_ledger is not None:
                                    try:
                                        paper_ledger_shutdown_receipt = {
                                            **paper_ledger.close(),
                                            "verifier_shutdown": verifier_shutdown,
                                            "runtime_watchdog_shutdown": runtime_watchdog_shutdown,
                                        }
                                    except Exception as error:
                                        shutdown_status = paper_ledger.shutdown_status()
                                        paper_ledger_shutdown_receipt = {
                                            **(
                                                {
                                                    "schema": "l3g-ledger-controlled-shutdown-v1",
                                                    "clean_shutdown": False,
                                                    "error": f"{type(error).__name__}: {error}",
                                                }
                                                if shutdown_status is None else shutdown_status
                                            ),
                                            "verifier_shutdown": verifier_shutdown,
                                            "runtime_watchdog_shutdown": runtime_watchdog_shutdown,
                                        }
                                        NINJATRADER_RUNTIME_LOGGER.exception(
                                            "L3G paper ledger controlled shutdown failed: %s",
                                            paper_ledger_shutdown_receipt,
                                        )
                                        raise
                                    NINJATRADER_RUNTIME_LOGGER.info(
                                        "L3G paper ledger controlled shutdown: %s",
                                        paper_ledger_shutdown_receipt,
                                    )

    app = FastAPI(title="Trader Copy Control Center", version="1.0", docs_url=None, redoc_url=None,
                  lifespan=lifespan)
    app.state.ninjatrader_observer = None
    app.state.lane_iii_shadow = None
    app.state.lane_iii_paper = None
    app.state.lane_iii_paper_transport = None
    app.state.ninjatrader_login_bootstrap = None
    app.state.ledger_verifier = ledger_verifier
    app.state.scheduler_engine = None
    app.state.scheduler_service = scheduler_service

    @app.exception_handler(sqlite3.Error)
    async def database_unavailable(_: Any, __: sqlite3.Error) -> Any:
        return JSONResponse(status_code=503, content={"detail": "Control-center database is temporarily unavailable."})

    def required_wallet(wallet: str) -> str:
        if not wallet.startswith("0x") or len(wallet) != 42:
            raise HTTPException(status_code=400, detail="Invalid wallet address.")
        return wallet.lower()

    def ledger_verification_status() -> dict[str, Any]:
        return ledger_verifier.status()

    def require_commissioning_ledger_verification(
        commissioning_id: str, runtime_snapshot: Mapping[str, object],
        *,
        launch_auto_on_failure: bool = True,
        enforce_observer: bool = True,
    ) -> dict[str, object]:
        """Evaluate one immutable verified-anchor/live-tail snapshot at an ordered ledger fence."""
        observer = ninja_listener_health()
        if enforce_observer and observer.get("market_observer_active") is not True:
            raise HTTPException(
                status_code=409,
                detail=(
                    "MARKET_OBSERVER_NOT_ACTIVE: market-data connectivity may exist, but "
                    "BeelzebubReadOnlyMarketObserver has produced no QUOTE/TRADE events."
                ),
            )
        status = ledger_verification_status()
        ledger = ninjatrader_runtime.get("paper_ledger")
        verified = status.get("verified_through_sequence")
        full_sequence = status.get("last_full_verified_sequence")
        tail: Mapping[str, object] = {}
        if type(ledger) is PaperLedger and type(verified) is int and type(full_sequence) is int:
            try:
                tail = ledger.commissioning_tail_snapshot(
                    verified, last_full_verified_sequence=full_sequence
                )
            except (RuntimeError, ValueError) as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"COMMISSIONING_LEDGER_TAIL_UNCLASSIFIED: {exc}",
                ) from exc
        try:
            return evaluate_commissioning_ledger_gate(
                status,
                tail,
                {**dict(runtime_snapshot), "commissioning_id": commissioning_id},
                checkpoint_matches_report=ledger_verifier.checkpoint_matches_report(status),
                freshness_seconds=LEDGER_VERIFICATION_FRESHNESS_SECONDS,
            )
        except CommissioningLedgerGateError as exc:
            launch_detail = ""
            if exc.launch_auto and launch_auto_on_failure:
                launched = ledger_verifier.start("auto")
                launch_detail = f" Auto verification launched ({launched.get('verification_id', 'local run')})."
            raise HTTPException(status_code=409, detail=f"{exc.code}: {exc}.{launch_detail}") from exc

    def paper_commissioning_rehearsal(paper: LaneIIIPaperRuntime) -> dict[str, object]:
        """Run the same authority-free readiness proof for Full and Slim views."""
        result = paper.commissioning_rehearsal(
            lambda commissioning_id, runtime_snapshot: require_commissioning_ledger_verification(
                commissioning_id, runtime_snapshot,
                launch_auto_on_failure=False,
                enforce_observer=False,
            )
        )
        observer = ninja_listener_health()
        observer_active = observer.get("market_observer_state") == "ACTIVE"
        runtime_observer = result.get("observer")
        result["observer"] = {
            **(dict(runtime_observer) if isinstance(runtime_observer, Mapping) else {}),
            "status": observer.get("market_observer_state", "NOT_ACTIVE"),
            "listener_state": observer.get("state", "UNSTARTED"),
            "last_level_one_at": observer.get("last_level_one_at"),
            "last_depth_at": observer.get("last_depth_at"),
            "freshness": observer.get("market_observer_freshness"),
            "operator_guidance": None if observer_active else (
                "Open the MNQ SEP26 chart, attach BeelzebubReadOnlyMarketObserver, and wait for ACTIVE."
            ),
        }
        reasons = [str(value) for value in result.get("blocking_reasons", [])]
        if not observer_active:
            reasons.append("MARKET_OBSERVER_NOT_ACTIVE")
        result["blocking_reasons"] = list(dict.fromkeys(reasons))
        result["result"] = "READY" if not result["blocking_reasons"] else "BLOCKED"
        hash_payload = dict(result)
        hash_payload.pop("snapshot_hash", None)
        result["snapshot_hash"] = hashlib.sha256(
            json.dumps(hash_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        return result

    def require_operational_paper_ledger_verification(
        operation_id: str,
        runtime_snapshot: Mapping[str, object],
        *,
        launch_auto_on_failure: bool = True,
        enforce_observer: bool = True,
    ) -> dict[str, object]:
        """Bind an operational paper session to a fresh Full Sim101 proof.

        This is a validation-only bridge.  It deliberately does not reserve or
        consume any commissioning credential.  Unlike the historic
        commissioning route, an operational session never accepts a merely
        incremental result that inherits an older Full proof: the Full scan
        itself must be the latest successful verifier run before new continuous
        paper authority can begin.
        """
        status = ledger_verification_status()
        full_is_latest_pass = (
            status.get("status") == "PASS"
            and status.get("verification_mode") == "full"
            and status.get("verification_id") == status.get("last_full_verification_id")
        )
        if not full_is_latest_pass:
            launch_detail = ""
            if launch_auto_on_failure and status.get("status") != "IN_PROGRESS":
                launched = ledger_verifier.start("full")
                launch_detail = f" Full verification launched ({launched.get('verification_id', 'local run')})."
            raise HTTPException(
                status_code=409,
                detail=(
                    "OPERATIONAL_FULL_LEDGER_VERIFICATION_REQUIRED: "
                    "A successful Full local ledger verification must be the latest proof before paper start."
                    + launch_detail
                ),
            )
        return require_commissioning_ledger_verification(
            operation_id,
            runtime_snapshot,
            launch_auto_on_failure=launch_auto_on_failure,
            enforce_observer=enforce_observer,
        )

    def _ledger_schedule() -> dict[str, Any] | None:
        schedules = scheduler_service.schedules(task_type="lane_iii.ledger_verification", page=1, page_size=10).get("items", [])
        return schedules[0] if schedules else None

    def ledger_verification_schedule_payload(schedule: Mapping[str, Any] | None = None) -> dict[str, Any]:
        current = dict(schedule or _ledger_schedule() or {})
        if not current:
            return {"enabled": False, "frequency": "DISABLED", "local_time": "03:00", "weekday": 0, "mode": "auto", "timezone": scheduler_settings.default_timezone, "schedule_id": None}
        specification = dict(current.get("trigger_specification") or {})
        configuration = dict(current.get("task_configuration") or {})
        frequency = "DAILY" if current.get("trigger_kind") == "DAILY" else "WEEKLY"
        weekdays = specification.get("weekdays") or [0]
        return {
            "enabled": current.get("lifecycle") == ScheduleLifecycle.ENABLED.value,
            "frequency": frequency if current.get("lifecycle") == ScheduleLifecycle.ENABLED.value else "DISABLED",
            "local_time": specification.get("local_time", "03:00"),
            "weekday": int(weekdays[0]),
            "mode": configuration.get("mode", "auto"),
            "timezone": current.get("timezone", scheduler_settings.default_timezone),
            "schedule_id": current.get("schedule_id"),
            "revision": current.get("revision"),
            "next_due_at": current.get("next_due_at"),
            "last_result_status": current.get("last_result_status"),
            "missed_run_policy": current.get("missed_run_policy", "SKIP"),
        }

    def update_ledger_verification_schedule(body: Mapping[str, Any]) -> dict[str, Any]:
        allowed = {"enabled", "frequency", "local_time", "weekday", "mode"}
        unexpected = sorted(set(body) - allowed)
        if unexpected:
            raise ValueError("Unsupported ledger verification schedule fields: " + ", ".join(unexpected))
        enabled = bool(body.get("enabled", True))
        frequency = str(body.get("frequency") or "DAILY").upper()
        if not enabled or frequency == "DISABLED":
            schedule = _ledger_schedule()
            if schedule is not None and schedule.get("lifecycle") != ScheduleLifecycle.PAUSED.value:
                schedule = scheduler_service.set_lifecycle(str(schedule["schedule_id"]), ScheduleLifecycle.PAUSED)
            return ledger_verification_schedule_payload(schedule)
        if frequency not in {"DAILY", "WEEKLY"}:
            raise ValueError("Ledger verification frequency must be DAILY or WEEKLY.")
        local_time = str(body.get("local_time") or "03:00")
        weekday = body.get("weekday", 0)
        if type(weekday) is not int or not 0 <= weekday <= 6:
            raise ValueError("Weekly ledger verification weekday must be 0 (Monday) through 6 (Sunday).")
        mode = str(body.get("mode") or "auto").lower()
        if mode not in {"auto", "incremental", "full"}:
            raise ValueError("Ledger verification mode must be auto, incremental, or full.")
        trigger_kind = "DAILY" if frequency == "DAILY" else "WEEKDAYS"
        trigger_specification: dict[str, Any] = {"local_time": local_time}
        if frequency == "WEEKLY":
            trigger_specification["weekdays"] = [weekday]
        payload = {
            "name": "Lane III ledger verification",
            "description": "Starts a detached local read-only ledger verifier. Missed runs are skipped; no AI/API loop executes the scan.",
            "task_type": "lane_iii.ledger_verification",
            "task_configuration": {"mode": mode},
            "trigger_kind": trigger_kind,
            "trigger_specification": trigger_specification,
            "timezone": scheduler_settings.default_timezone,
            "missed_run_policy": "SKIP",
            "max_lateness_seconds": scheduler_settings.default_max_lateness_seconds,
            "retry_policy": {"max_attempts": 1},
            "lifecycle": "ENABLED",
        }
        existing = _ledger_schedule()
        if existing is None:
            schedule = scheduler_service.create_schedule(payload)
        else:
            schedule = scheduler_service.update_schedule(str(existing["schedule_id"]), {**payload, "current_revision": int(existing["revision"])})
            if schedule.get("lifecycle") != ScheduleLifecycle.ENABLED.value:
                schedule = scheduler_service.set_lifecycle(str(schedule["schedule_id"]), ScheduleLifecycle.ENABLED)
        return ledger_verification_schedule_payload(schedule)

    @app.get("/api/health")
    async def api_health() -> dict[str, Any]:
        return {
            **center.health(live_watcher_health),
            "science": center.science.health(),
            "ninjatrader_login": ninja_login_health(),
            "ninjatrader_observer": ninja_listener_health(),
            "lane_iii_shadow": lane_iii_shadow_health(),
            "lane_iii_paper": sanitized_paper_health(lane_iii_paper_health()),
            "lane_iii_live": lane_iii_live_health(),
            "scheduler": scheduler_service.status(),
            "runtime_binding": runtime_binding,
        }

    def scheduler_error(exc: Exception) -> None:
        if isinstance(exc, KeyError):
            raise HTTPException(status_code=404, detail="Scheduler record was not found.") from exc
        if isinstance(exc, RuntimeError) and str(exc) == "STALE_REVISION":
            raise HTTPException(status_code=409, detail="Schedule revision is stale; refresh before editing.") from exc
        if isinstance(exc, ValueError):
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        raise exc

    @app.get("/api/scheduler/status")
    async def api_scheduler_status() -> dict[str, Any]:
        return scheduler_service.status()

    @app.get("/api/scheduler/catalog")
    async def api_scheduler_catalog() -> dict[str, Any]:
        return scheduler_service.catalog()

    @app.post("/api/scheduler/preview")
    async def api_scheduler_preview(payload: PreviewRequest) -> dict[str, Any]:
        try:
            return scheduler_service.preview(payload.model_dump())
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.get("/api/scheduler/schedules")
    async def api_scheduler_schedules(lifecycle: str | None = None, task_type: str | None = None,
                                      page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
        return scheduler_service.schedules(lifecycle=lifecycle, task_type=task_type, page=page, page_size=page_size)

    @app.post("/api/scheduler/schedules")
    async def api_scheduler_create_schedule(payload: ScheduleRequest) -> dict[str, Any]:
        try:
            return scheduler_service.create_schedule(payload.model_dump())
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.get("/api/scheduler/schedules/{schedule_id}")
    async def api_scheduler_schedule(schedule_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.schedule(schedule_id)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.put("/api/scheduler/schedules/{schedule_id}")
    async def api_scheduler_update_schedule(schedule_id: str, payload: ScheduleUpdateRequest) -> dict[str, Any]:
        try:
            return scheduler_service.update_schedule(schedule_id, payload.model_dump())
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/schedules/{schedule_id}/pause")
    async def api_scheduler_pause(schedule_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.set_lifecycle(schedule_id, ScheduleLifecycle.PAUSED)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/schedules/{schedule_id}/resume")
    async def api_scheduler_resume(schedule_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.set_lifecycle(schedule_id, ScheduleLifecycle.ENABLED)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/schedules/{schedule_id}/archive")
    async def api_scheduler_archive(schedule_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.set_lifecycle(schedule_id, ScheduleLifecycle.ARCHIVED)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/schedules/{schedule_id}/run-now")
    async def api_scheduler_run_now(schedule_id: str, payload: RunNowRequest | None = None) -> dict[str, Any]:
        try:
            return scheduler_service.run_now(schedule_id, operator_request_id=payload.operator_request_id if payload else None)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/templates/{template_id}/instantiate")
    async def api_scheduler_instantiate(template_id: str, payload: TemplateRequest | None = None) -> dict[str, Any]:
        try:
            return scheduler_service.instantiate_template(template_id, name=payload.name if payload else None)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.get("/api/scheduler/runs")
    async def api_scheduler_runs(status: str | None = None, task_type: str | None = None, schedule_id: str | None = None,
                                 page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
        return scheduler_service.runs(status=status, task_type=task_type, schedule_id=schedule_id, page=page, page_size=page_size)

    @app.get("/api/scheduler/runs/{run_id}")
    async def api_scheduler_run(run_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.run(run_id)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/runs/{run_id}/cancel")
    async def api_scheduler_cancel(run_id: str) -> dict[str, Any]:
        try:
            return await scheduler_service.cancel(run_id)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/runs/{run_id}/retry")
    async def api_scheduler_retry(run_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.retry(run_id)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.get("/api/scheduler/notifications")
    async def api_scheduler_notifications(unread_only: bool = False, page: int = Query(1, ge=1),
                                          page_size: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
        return scheduler_service.notifications(unread_only=unread_only, page=page, page_size=page_size)

    @app.post("/api/scheduler/notifications/{notification_id}/read")
    async def api_scheduler_mark_notification(notification_id: str) -> dict[str, Any]:
        try:
            return scheduler_service.mark_notification_read(notification_id)
        except Exception as exc:
            scheduler_error(exc)
            raise AssertionError("unreachable")

    @app.post("/api/scheduler/notifications/read-all")
    async def api_scheduler_mark_all_notifications() -> dict[str, Any]:
        return scheduler_service.mark_all_notifications_read()

    @app.get("/api/lane-iii/shadow")
    async def api_lane_iii_shadow() -> dict[str, object]:
        return lane_iii_shadow_health()

    @app.get("/api/lane-iii/shadow/audit")
    async def api_lane_iii_shadow_audit(limit: int = Query(100, ge=1, le=512)) -> dict[str, object]:
        shadow = ninjatrader_runtime.get("shadow")
        if shadow is None:
            return {"mode": "LANE_III_SHADOW", "items": []}
        return {"mode": "LANE_III_SHADOW", "items": shadow.audit_records(limit)}

    @app.get("/api/lane-iii/paper")
    async def api_lane_iii_paper() -> dict[str, object]:
        return lane_iii_paper_health()

    @app.get("/api/lane-iii/paper/slim-status")
    async def api_lane_iii_paper_slim_status() -> dict[str, object]:
        """Compact, read-only rendering state; it cannot acquire paper authority."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            return unavailable_slim_status()
        runtime_status = paper.status()
        verification = ledger_verifier.status()
        raw_ledger = runtime_status.get("ledger")
        if isinstance(raw_ledger, Mapping):
            runtime_status["ledger"] = ledger_health_projection(
                raw_ledger,
                verification,
                operational_session=(
                    runtime_status.get("operational_paper_session")
                    if isinstance(runtime_status.get("operational_paper_session"), Mapping)
                    else None
                ),
            )
        observer = ninja_listener_health()
        return derive_slim_paper_status(
            runtime_status,
            verification,
            observer,
            paper.operational_paper_readiness(
                lambda operation_id, runtime_snapshot: require_operational_paper_ledger_verification(
                    operation_id, runtime_snapshot,
                    launch_auto_on_failure=False,
                    enforce_observer=False,
                )
            ),
            verification_freshness_seconds=LEDGER_VERIFICATION_FRESHNESS_SECONDS,
        )

    @app.get("/api/lane-iii/live")
    async def api_lane_iii_live() -> dict[str, object]:
        """Read-only L3H status. No live order-producing route is registered."""
        return lane_iii_live_health()

    @app.get("/api/accounts/balances")
    async def api_account_balances() -> dict[str, object]:
        """Expose the two NinjaTrader AccountItem CashValue readings, read-only."""
        return {
            "accounts": ninja_listener_health()["account_balances"],
            "authority": "OBSERVE_ONLY",
        }

    @app.get("/api/lane-iii/paper/ledger-verification")
    async def api_lane_iii_paper_ledger_verification() -> dict[str, Any]:
        return ledger_verification_status()

    @app.post("/api/lane-iii/paper/ledger-verification")
    async def api_start_lane_iii_paper_ledger_verification(body: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        if ninjatrader_runtime.get("paper_ledger") is None:
            raise HTTPException(status_code=503, detail="Lane III paper ledger is unavailable.")
        payload = body or {}
        if not isinstance(payload, dict) or set(payload) - {"mode"}:
            raise HTTPException(status_code=400, detail="Ledger verification accepts only an optional mode.")
        mode = str(payload.get("mode") or "auto").lower()
        if mode not in {"auto", "incremental", "full"}:
            raise HTTPException(status_code=400, detail="Verification mode must be auto, incremental, or full.")
        return ledger_verifier.start(mode)

    @app.post("/api/lane-iii/paper/ledger-verification/cancel")
    async def api_cancel_lane_iii_paper_ledger_verification() -> dict[str, Any]:
        return ledger_verifier.cancel()

    @app.get("/api/lane-iii/paper/ledger-verification/schedule")
    async def api_lane_iii_paper_ledger_verification_schedule() -> dict[str, Any]:
        return ledger_verification_schedule_payload()

    @app.post("/api/lane-iii/paper/ledger-verification/schedule")
    async def api_update_lane_iii_paper_ledger_verification_schedule(body: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Ledger verification schedule body must be an object.")
        try:
            return update_ledger_verification_schedule(body)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/lane-iii/paper/audit")
    async def api_lane_iii_paper_audit(
        limit: int = Query(100, ge=1, le=512),
        session_kind: str | None = None,
        session_family: str | None = None,
        trade_date: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, object]:
        ledger = ninjatrader_runtime.get("paper_ledger")
        if ledger is None:
            return {"mode": "PAPER_SIM101", "items": []}
        return {
            "mode": "PAPER_SIM101",
            "filters": {"session_kind": session_kind, "session_family": session_family, "trade_date": trade_date, "session_id": session_id},
            "items": ledger.recent(limit, session_kind=session_kind, session_family=session_family, trade_date=trade_date, session_id=session_id),
        }

    @app.post("/api/lane-iii/paper/arm")
    async def api_lane_iii_paper_arm() -> dict[str, object]:
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            # A BeezConsole-only restart can outlast the bounded desktop UI
            # probe even though NinjaTrader remains authenticated.  Never
            # trust the process probe alone in that case: accept only the
            # stronger, fresh signed Sim101 execution handshake and complete
            # position/order reconciliation already enforced by paper.arm().
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"),
                transport.get("reconciled"), transport.get("account"),
                transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        return paper.arm()

    @app.post("/api/lane-iii/paper/commission-entry")
    async def api_lane_iii_paper_commission_entry(body: dict[str, Any] | None = Body(default=None)) -> dict[str, object]:
        """Run the closed Sim101 commissioning entry only through paper runtime gates."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"),
                transport.get("reconciled"), transport.get("account"),
                transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Commissioning entry requires its active lifecycle credential.")
        commissioning_id = body.get("commissioning_id")
        commissioning_token = body.get("commissioning_token")
        if not isinstance(commissioning_id, str) or not isinstance(commissioning_token, str):
            raise HTTPException(status_code=400, detail="Commissioning entry lifecycle credential is invalid.")
        return paper.commission_entry(commissioning_id, commissioning_token)

    @app.post("/api/lane-iii/paper/commissioning-arm")
    async def api_lane_iii_paper_commissioning_arm() -> dict[str, object]:
        """Atomically reserve the sole commissioning entry before paper arms."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"),
                transport.get("reconciled"), transport.get("account"),
                transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        return paper.commissioning_arm(require_commissioning_ledger_verification)

    @app.post("/api/lane-iii/paper/commissioning-rehearsal")
    async def api_lane_iii_paper_commissioning_rehearsal() -> dict[str, object]:
        """Run production commissioning validators without acquiring authority."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        return paper_commissioning_rehearsal(paper)

    @app.post("/api/lane-iii/paper/commissioning-start")
    async def api_lane_iii_paper_commissioning_start(
        body: dict[str, Any] | None = Body(default=None),
    ) -> dict[str, object]:
        """Canonical one-request production commissioning authority path."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        if not isinstance(body, dict) or set(body) != {"request_id"} or not isinstance(body.get("request_id"), str):
            raise HTTPException(status_code=400, detail="Commissioning start accepts only a request_id.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"),
                transport.get("reconciled"), transport.get("account"),
                transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        try:
            return paper.commissioning_start(
                str(body["request_id"]), require_commissioning_ledger_verification,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/lane-iii/paper/operational-start")
    async def api_lane_iii_paper_operational_start(
        body: dict[str, Any] | None = Body(default=None),
    ) -> dict[str, object]:
        """Start the continuous, paper-only Sim101 operator lifecycle."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        if not isinstance(body, dict) or set(body) != {"request_id"} or not isinstance(body.get("request_id"), str):
            raise HTTPException(status_code=400, detail="Operational paper start accepts only a request_id.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"), transport.get("reconciled"),
                transport.get("account"), transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        try:
            return paper.operational_paper_start(
                str(body["request_id"]), require_operational_paper_ledger_verification,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/lane-iii/paper/commission-exit")
    async def api_lane_iii_paper_commission_exit() -> dict[str, object]:
        """Close only an active explicit commissioning position through normal paper gates."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        bootstrap = ninjatrader_runtime.get("login_bootstrap")
        if bootstrap is not None and bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED:
            transport = paper.status().get("transport")
            if not isinstance(transport, Mapping) or (
                transport.get("state"), transport.get("authenticated_client"),
                transport.get("reconciled"), transport.get("account"),
                transport.get("account_class"), transport.get("instrument"),
            ) != ("AUTHENTICATED", True, True, "Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
                raise HTTPException(status_code=409, detail="NinjaTrader desktop authentication is not operational.")
        return paper.commission_exit()

    @app.post("/api/lane-iii/paper/pause")
    async def api_lane_iii_paper_pause() -> dict[str, object]:
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        return paper.pause_entries()

    @app.post("/api/lane-iii/paper/resume")
    async def api_lane_iii_paper_resume() -> dict[str, object]:
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        return paper.resume_entries()

    @app.post("/api/lane-iii/paper/flatten-and-disarm")
    async def api_lane_iii_paper_flatten_and_disarm() -> dict[str, object]:
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        return paper.flatten_and_disarm()

    @app.post("/api/lane-iii/paper/entry-profile")
    async def api_lane_iii_paper_entry_profile(
        body: dict[str, Any] | None = Body(default=None),
    ) -> dict[str, object]:
        """Select the backend-owned Sim101 entry profile; never execution authority."""
        paper = ninjatrader_runtime.get("paper")
        if paper is None:
            raise HTTPException(status_code=503, detail="Lane III paper runtime is unavailable.")
        if (
            not isinstance(body, dict)
            or set(body) != {"operator_command_id", "profile"}
            or not isinstance(body.get("operator_command_id"), str)
            or not isinstance(body.get("profile"), str)
        ):
            raise HTTPException(
                status_code=400,
                detail="Entry-profile selection accepts only operator_command_id and profile.",
            )
        try:
            return paper.select_entry_profile(
                str(body["operator_command_id"]), str(body["profile"]),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/science/health")
    async def api_science_health() -> dict[str, Any]:
        return center.science.health()

    @app.get("/api/science/ecosystem")
    async def api_science_ecosystem() -> dict[str, Any]:
        return center.science.ecosystem()

    @app.get("/api/science/automated")
    async def api_science_automated() -> dict[str, Any]:
        return center.science.automated()

    @app.get("/api/science/data-ignition")
    async def api_science_data_ignition() -> dict[str, Any]:
        return center.science.data_ignition()

    @app.post("/api/science/pause")
    async def api_science_pause(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return center.science.pause_automated_worker(str((payload or {}).get("reason") or "operator requested scientific pause"))

    @app.post("/api/science/resume")
    async def api_science_resume() -> dict[str, Any]:
        return center.science.resume_automated_worker()

    @app.get("/api/wallet-sensors")
    async def api_wallet_sensors() -> dict[str, Any]:
        return center.science.wallet_sensors()

    @app.get("/api/hypotheses")
    async def api_hypotheses(state: str | None = None) -> dict[str, Any]:
        return center.science.hypotheses(state)

    @app.get("/api/experiments")
    async def api_experiments(kind: str | None = None) -> dict[str, Any]:
        return center.science.experiments(kind)

    @app.get("/api/indicators")
    async def api_indicators() -> dict[str, Any]:
        return center.science.indicators()

    @app.get("/api/models")
    async def api_models() -> dict[str, Any]:
        return center.science.models()

    @app.get("/api/confidence")
    async def api_confidence() -> dict[str, Any]:
        return center.science.confidence()

    @app.get("/api/decisions")
    async def api_decisions() -> dict[str, Any]:
        return center.science.decisions()

    @app.get("/api/graveyard")
    async def api_graveyard(search: str = "") -> dict[str, Any]:
        return center.science.graveyard(search)

    @app.get("/api/storage")
    async def api_storage() -> dict[str, Any]:
        return center.science.storage()

    @app.get("/api/overview")
    async def api_overview() -> dict[str, Any]:
        return center.overview()

    @app.get("/api/discovery/status")
    async def api_discovery_status() -> dict[str, Any]:
        overview = center.overview()
        health = center.health(live_watcher_health)
        jobs = center.store.list_jobs(job_type="candidate_discovery", limit=1)
        return {
            "paper_only": True, "source": source.source_status(), "candidate_universe_count": overview["counts"]["total_discovered"],
            "last_successful_discovery": health["last_discovery_run"], "current_job": jobs[0] if jobs else None,
            "presets": {name: {"window_hours": value["hourly_object_count"], "hourly_objects": value["hourly_object_count"], "candidate_limit": value["candidate_limit"],
                               "min_activity": value["min_activity"], "max_activity_age": value["max_activity_age"]} for name, value in {
                                   "quick": discovery_preset("quick"), "standard": discovery_preset("standard"), "deep": discovery_preset("deep"),
                               }.items()},
        }

    @app.get("/api/discovery/source")
    async def api_discovery_source() -> dict[str, Any]:
        return {"paper_only": True, **source.source_status()}

    @app.post("/api/discovery/source/test")
    async def api_discovery_source_test() -> dict[str, Any]:
        return {"paper_only": True, **source.source_status(test_access=True)}

    @app.post("/api/discovery/jobs")
    async def api_discovery_job(body: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Discovery job body must be an object.")
        allowed = {"preset", "candidate_limit", "min_activity", "max_activity_age", "window_hours"}
        unexpected = sorted(set(body) - allowed)
        if unexpected:
            raise HTTPException(status_code=400, detail="Unsupported discovery job fields: " + ", ".join(unexpected))
        try:
            configuration = discovery_job_configuration(body)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if source.source_status().get("connection_state") != "READY":
            raise HTTPException(status_code=409, detail="Test official requester-pays source access successfully before starting discovery.")
        existing = center.store.list_jobs(job_type="candidate_discovery", limit=20)
        if any(item["status"] in {"queued", "acquiring", "parsing", "discovering"} for item in existing):
            raise HTTPException(status_code=409, detail="A candidate discovery job is already running.")
        job = center.store.create_job(job_type="candidate_discovery", configuration=configuration)
        job_runtime[job["job_id"]] = asyncio.create_task(asyncio.to_thread(discovery_orchestrator.run, job["job_id"]))
        return job

    @app.get("/api/discovery/jobs")
    async def api_discovery_jobs() -> dict[str, Any]:
        return {"items": center.store.list_jobs(job_type="candidate_discovery"), "paper_only": True}

    @app.get("/api/discovery/jobs/{job_id}")
    async def api_discovery_job_detail(job_id: str) -> dict[str, Any]:
        job = center.store.get_job(job_id)
        if not job or job["job_type"] != "candidate_discovery":
            raise HTTPException(status_code=404, detail="Discovery job not found.")
        return job

    @app.post("/api/discovery/jobs/{job_id}/cancel")
    async def api_discovery_job_cancel(job_id: str) -> dict[str, Any]:
        job = center.store.get_job(job_id)
        if not job or job["job_type"] != "candidate_discovery":
            raise HTTPException(status_code=404, detail="Discovery job not found.")
        if job["status"] in {"completed", "completed_with_warnings", "failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="This discovery job has already finished.")
        result = center.store.request_job_cancellation(job_id)
        assert result is not None
        return result

    @app.get("/api/candidates")
    async def api_candidates(
        page: int = 1, page_size: int = 50, sort: str = "score", direction: str = "desc", search: str = "",
        status: str = "", lifecycle: str = "", min_score: float | None = None, max_score: float | None = None,
        min_win_rate: float | None = None, max_win_rate: float | None = None, min_profit_factor: float | None = None,
        max_profit_factor: float | None = None, max_drawdown: float | None = None, max_follower_drawdown: float | None = None,
        coverage: str = "", copyability_available: bool | None = None, recent_days: int | None = None, current_only: bool = False,
    ) -> dict[str, Any]:
        return center.candidates(page=page, page_size=page_size, sort=sort, direction=direction, search=search, status=status,
                                 lifecycle=lifecycle, min_score=min_score, max_score=max_score, min_win_rate=min_win_rate,
                                 max_win_rate=max_win_rate, min_profit_factor=min_profit_factor, max_profit_factor=max_profit_factor,
                                 max_drawdown=max_drawdown, max_follower_drawdown=max_follower_drawdown, coverage=coverage,
                                 copyability_available=copyability_available, recent_days=recent_days, current_only=current_only)

    @app.get("/api/candidates/{wallet}")
    async def api_candidate(wallet: str) -> dict[str, Any]:
        detail = center.candidate_detail(required_wallet(wallet))
        if not detail:
            raise HTTPException(status_code=404, detail="Candidate not found.")
        return detail

    @app.post("/api/candidates/{wallet}/operator-state")
    async def api_operator_state(wallet: str, body: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
        if not isinstance(body, dict) or not isinstance(body.get("state"), str) or not body["state"].strip():
            raise HTTPException(status_code=400, detail="Request body requires a non-empty operator state.")
        state = body["state"].strip()
        if state not in OPERATOR_STATES:
            raise HTTPException(status_code=400, detail="Unsupported operator state.")
        try:
            result = center.set_operator_state(required_wallet(wallet), state, allow_overflow=bool(body.get("allow_overflow", False)))
            refresh_watcher_membership()
            return result
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/shadow-finalists")
    async def api_shadow_finalists() -> dict[str, Any]:
        return {"items": center.shadow_finalists(), "paper_only": True}

    @app.get("/api/active-cohort")
    async def api_active() -> dict[str, Any]:
        return center.active_cohort()

    @app.get("/api/portfolio")
    async def api_portfolio() -> dict[str, Any]:
        return center.portfolio_summary()

    @app.get("/api/positions")
    async def api_positions(wallet: str | None = None, symbol: str | None = None, direction: str | None = None) -> dict[str, Any]:
        return {"items": center.positions(wallet=wallet, symbol=symbol, direction=direction), "paper_only": True}

    @app.get("/api/activity")
    async def api_activity(limit: int = Query(100, ge=1, le=500), wallet: str | None = None) -> dict[str, Any]:
        return {"items": center.activity(limit=limit, wallet=wallet)}

    @app.get("/api/system")
    async def api_system() -> dict[str, Any]:
        return {
            "health": center.health(live_watcher_health),
            "risk": center.risk_panel(),
            "source": source.source_status(),
            "ninjatrader_observer": ninja_listener_health(),
            "lane_iii_shadow": lane_iii_shadow_health(),
            "lane_iii_paper": sanitized_paper_health(lane_iii_paper_health()),
            "runtime_binding": runtime_binding,
            "paper_only": True,
        }

    @app.get("/api/runtime-binding")
    async def api_runtime_binding() -> dict[str, Any]:
        """Expose non-secret deployment bindings without granting authority."""
        return dict(runtime_binding)

    @app.get("/api/execution")
    async def api_execution() -> dict[str, Any]:
        return center.execution_health()

    @app.post("/api/execution/shadow/refresh")
    async def api_refresh_shadow_observation() -> dict[str, Any]:
        # This endpoint appends local evidence after public venue reads only;
        # it has no execution adapter, write, signing, or credential path.
        return center.refresh_shadow_observation()

    @app.get("/api/recovery")
    async def api_recovery(wallet: str | None = None) -> dict[str, Any]:
        return center._paper_service().recovery_status(wallet)

    @app.post("/api/recovery/{wallet}/safe-rebaseline")
    async def api_safe_rebaseline(wallet: str) -> dict[str, Any]:
        return await center._paper_service().safe_rebaseline_recovery(wallet)

    @app.get("/api/controls")
    async def api_controls() -> dict[str, Any]:
        return center.store.control_state()

    @app.post("/api/controls/pause-entries")
    async def api_pause_entries() -> dict[str, Any]:
        return center.pause_entries()

    @app.post("/api/controls/resume-entries")
    async def api_resume_entries() -> dict[str, Any]:
        return center.resume_entries()

    @app.post("/api/controls/close-all-paper-positions")
    async def api_close_all() -> dict[str, Any]:
        result = center.close_all_paper_positions()
        refresh_watcher_membership()
        return result

    @app.post("/api/controls/exit-and-pause")
    async def api_exit_pause() -> dict[str, Any]:
        result = center.exit_and_pause()
        refresh_watcher_membership()
        return result

    @app.websocket("/ws")
    async def ws_updates(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            previous: dict[str, str] = {}
            while True:
                events = {
                    "control_state": center.store.control_state(),
                    "portfolio_update": center.portfolio_summary(),
                    "position_update": {"items": center.positions(), "paper_only": True},
                    "watcher_health": center.health(live_watcher_health)["watcher"],
                    "account_balances": ninja_listener_health()["account_balances"],
                    "activity": {"items": center.activity(limit=20)},
                    "scheduler_status": scheduler_service.status(),
                    "scheduler_schedule_update": scheduler_service.schedules(page=1, page_size=50),
                    "scheduler_run_update": scheduler_service.runs(page=1, page_size=50),
                    "scheduler_notification": scheduler_service.notifications(unread_only=True, page=1, page_size=50),
                }
                latest_job = center.store.list_jobs(job_type="candidate_discovery", limit=1)
                if latest_job:
                    events["discovery_job_update"] = latest_job[0]
                for name, payload in events.items():
                    signature = _dump(payload)
                    if previous.get(name) != signature:
                        await websocket.send_json({"type": name, "data": payload, "paper_only": True})
                        previous[name] = signature
                # Waiting only on changed outbound payloads leaves an idle,
                # disconnected browser task alive forever. Poll the inbound
                # ASGI channel so disconnects are observed and application
                # shutdown can reach the listener-owning lifespan promptly.
                try:
                    await asyncio.wait_for(websocket.receive_text(), timeout=1)
                except TimeoutError:
                    pass
        except WebSocketDisconnect:
            return

    frontend_dist = Path(__file__).resolve().parents[2] / "control-center-ui" / "dist"
    if frontend_dist.exists():
        assets = frontend_dist / "assets"
        if assets.exists():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/{path:path}", response_class=FileResponse)
        async def frontend(path: str) -> Any:
            # The SPA shell must never masquerade as an API success.  Routes
            # above own every registered API handler; an unmatched API GET is
            # a backend failure and stays structured JSON for all clients.
            if path == "api" or path.startswith("api/"):
                return JSONResponse(
                    status_code=404,
                    content={
                        "detail": "API endpoint not found.",
                        "code": "API_ENDPOINT_NOT_FOUND",
                        "path": f"/{path}",
                    },
                )
            root = frontend_dist.resolve()
            requested = (root / path).resolve() if path else root / "index.html"
            # Only explicitly built files under dist may be served.  Every SPA
            # route falls back to index.html, including traversal-like paths.
            if root not in requested.parents or not requested.is_file():
                requested = root / "index.html"
            # Never retain an old SPA shell across a local backend restart.
            # The hashed JS/CSS assets can remain safely cacheable.
            headers = {"Cache-Control": "no-store, max-age=0"} if requested.name == "index.html" else None
            return FileResponse(requested, headers=headers)
    else:
        @app.get("/", response_class=HTMLResponse)
        async def frontend_missing() -> str:
            return "<h1>Copy Control Center</h1><p>Frontend build not found. Run <code>npm run build</code> in control-center-ui.</p>"

    return app


def serve_control_center(
    config: CopyTradeConfig, database: CopyTradeDatabase | None = None, *, host: str | None = None, port: int | None = None,
    with_watcher: bool = False, service: Any | None = None,
) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("copy-control-center requires uvicorn; install requirements.txt.") from exc
    if with_watcher and service is None:
        from .service import CopyTradeService
        service = CopyTradeService(config, database)
    uvicorn.run(create_control_center_app(config, database, watcher_service=service if with_watcher else None),
                host=host or config.artifacts.dashboard_host, port=port or config.artifacts.dashboard_port)
