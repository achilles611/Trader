"""Fail-closed NinjaTrader desktop and read-only observer maintenance.

This module owns no trading authority.  It can discover, start, and request a
graceful close of the one fixed NinjaTrader desktop process.  Runtime, broker,
AddOn, chart, observer, and ledger truth are supplied by existing read-only
status projections.  No method can arm a runtime, authenticate an account,
send an execution command, cancel an order, or force-terminate a process.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import threading
import time
from typing import Callable, Mapping, Protocol
from uuid import uuid4

from .ninjatrader_login import PowerShellNinjaTraderLoginAdapter


MAINTENANCE_SCHEMA = "lane-iii-ninjatrader-observer-maintenance-v1"
ACTION_TOKEN_HEADER = "X-Beelzebub-Maintenance-Action"
ACTION_TOKEN_VALUE = "ninjatrader-observer-repair-v1"
AUTH_TOKEN_HEADER = "X-Beelzebub-Maintenance-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_INSTRUMENT = re.compile(r"^MNQ [A-Z]{3}[0-9]{2}$")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_text(value: object, maximum: int = 300) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    return text[:maximum]


@dataclass(frozen=True)
class DesktopProbe:
    process_detected: bool = False
    login_window_detected: bool = False
    control_center_detected: bool = False
    failure_category: str | None = None


class NinjaTraderDesktopAdapter(Protocol):
    def probe(self) -> DesktopProbe: ...
    def configure_instrument(self, instrument: str) -> bool: ...
    def start(self) -> bool: ...
    def request_graceful_shutdown(self) -> bool: ...


class PowerShellNinjaTraderDesktopAdapter:
    """Fixed-action Windows boundary; request data never becomes a command."""

    def __init__(
        self,
        script_path: str | Path | None = None,
        *,
        instrument_config_path: str | Path | None = None,
        command_timeout_seconds: float = 15.0,
    ) -> None:
        root = Path(__file__).resolve().parents[2]
        self.script_path = Path(script_path or root / "tools" / "ninjatrader_autologin.ps1").resolve()
        documents = Path(os.environ.get("USERPROFILE") or Path.home()) / "Documents" / "NinjaTrader 8"
        self.instrument_config_path = Path(
            instrument_config_path or documents / "beelzebub-observer.local.config"
        ).resolve()
        self.command_timeout_seconds = command_timeout_seconds

    def _run(self, action: str) -> dict[str, object]:
        if action not in {"probe", "start", "close-gracefully"} or not self.script_path.is_file():
            return {"ok": False, "failure_category": "AUTOMATION_PROCESS_FAILED"}
        try:
            completed = subprocess.run(
                [
                    "powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive",
                    "-ExecutionPolicy", "Bypass", "-File", str(self.script_path), "-Action", action,
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.command_timeout_seconds,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                env=PowerShellNinjaTraderLoginAdapter._windows_powershell_environment(),
            )
        except (OSError, subprocess.SubprocessError):
            return {"ok": False, "failure_category": "AUTOMATION_PROCESS_FAILED"}
        lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        if completed.returncode != 0 or len(lines) != 1:
            return {"ok": False, "failure_category": "AUTOMATION_PROCESS_FAILED"}
        try:
            payload = json.loads(lines[0])
        except json.JSONDecodeError:
            return {"ok": False, "failure_category": "POWERSHELL_HELPER_INVALID_RESPONSE"}
        return payload if isinstance(payload, dict) else {
            "ok": False, "failure_category": "POWERSHELL_HELPER_INVALID_RESPONSE",
        }

    def probe(self) -> DesktopProbe:
        payload = self._run("probe")
        failure = payload.get("failure_category")
        return DesktopProbe(
            process_detected=payload.get("process_detected") is True,
            login_window_detected=payload.get("login_window_detected") is True,
            control_center_detected=payload.get("control_center_detected") is True,
            failure_category=None if failure is None else _safe_text(failure, 80),
        )

    def configure_instrument(self, instrument: str) -> bool:
        if not _INSTRUMENT.fullmatch(instrument):
            return False
        try:
            self.instrument_config_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.instrument_config_path.with_suffix(
                self.instrument_config_path.suffix + f".{os.getpid()}.tmp"
            )
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(f"L3F_NT_MARKET_INSTRUMENT={instrument}\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.instrument_config_path)
            return True
        except (FileExistsError, OSError):
            try:
                temporary.unlink()
            except (NameError, OSError):
                pass
            return False

    def start(self) -> bool:
        return self._run("start").get("ok") is True

    def request_graceful_shutdown(self) -> bool:
        return self._run("close-gracefully").get("ok") is True


class MaintenanceStage(StrEnum):
    IDLE = "IDLE"
    CHECKING = "CHECKING"
    VERIFYING_RESTART_LEDGER = "VERIFYING_RESTART_LEDGER"
    GATE_CHECK = "GATE_CHECK"
    GRACEFUL_SHUTDOWN = "GRACEFUL_SHUTDOWN"
    LAUNCHING = "LAUNCHING"
    WAITING_FOR_OPERATOR_LOGIN = "WAITING_FOR_OPERATOR_LOGIN"
    WAITING_FOR_ADDON = "WAITING_FOR_ADDON"
    VERIFYING_CHART = "VERIFYING_CHART"
    WAITING_FOR_MANUAL_CHART_ATTACHMENT = "WAITING_FOR_MANUAL_CHART_ATTACHMENT"
    VERIFYING_MARKET_DATA = "VERIFYING_MARKET_DATA"
    RECONCILING = "RECONCILING"
    FINAL_LEDGER_VERIFICATION = "FINAL_LEDGER_VERIFICATION"
    READY = "READY"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class MaintenanceAuditError(RuntimeError):
    """The durable maintenance journal could not accept a transition."""


_ACTIVE_STAGES = frozenset({
    MaintenanceStage.CHECKING, MaintenanceStage.VERIFYING_RESTART_LEDGER,
    MaintenanceStage.GATE_CHECK, MaintenanceStage.GRACEFUL_SHUTDOWN,
    MaintenanceStage.LAUNCHING, MaintenanceStage.WAITING_FOR_OPERATOR_LOGIN,
    MaintenanceStage.WAITING_FOR_ADDON, MaintenanceStage.VERIFYING_CHART,
    MaintenanceStage.WAITING_FOR_MANUAL_CHART_ATTACHMENT,
    MaintenanceStage.VERIFYING_MARKET_DATA, MaintenanceStage.RECONCILING,
    MaintenanceStage.FINAL_LEDGER_VERIFICATION,
})


@dataclass(frozen=True)
class MaintenanceTimeouts:
    process_start_seconds: float = 45.0
    graceful_shutdown_seconds: float = 45.0
    operator_login_seconds: float = 900.0
    addon_seconds: float = 120.0
    chart_seconds: float = 900.0
    market_data_seconds: float = 60.0
    reconciliation_seconds: float = 60.0
    ledger_seconds: float = 120.0
    poll_seconds: float = 0.5

    def __post_init__(self) -> None:
        values = tuple(self.__dict__.values())
        if any(not isinstance(value, (int, float)) or value <= 0 for value in values):
            raise ValueError("Maintenance timeouts must be positive.")


class NinjaTraderMaintenanceService:
    """One idempotent desktop/observer repair workflow per local runtime."""

    def __init__(
        self,
        *,
        paper_status: Callable[[], Mapping[str, object]],
        live_status: Callable[[], Mapping[str, object]],
        ledger_status: Callable[[], Mapping[str, object]],
        start_ledger_verification: Callable[[], Mapping[str, object]],
        desktop: NinjaTraderDesktopAdapter | None = None,
        audit_path: str | Path | None = None,
        timeouts: MaintenanceTimeouts = MaintenanceTimeouts(),
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], bool] | None = None,
    ) -> None:
        self._paper_status = paper_status
        self._live_status = live_status
        self._ledger_status = ledger_status
        self._start_ledger_verification = start_ledger_verification
        self._desktop = desktop or PowerShellNinjaTraderDesktopAdapter()
        root = Path(__file__).resolve().parents[2]
        self._audit_path = Path(audit_path or root / "logs" / "ninjatrader-maintenance-audit.jsonl").resolve()
        self._timeouts = timeouts
        self._clock = clock
        self._stop = threading.Event()
        self._wait = wait or self._stop.wait
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._operation_id: str | None = None
        self._request_id: str | None = None
        self._stage = MaintenanceStage.IDLE
        self._stage_started_at = _utc_now()
        self._operation_started_at: str | None = None
        self._blockers: list[str] = []
        self._manual_action: str | None = None
        self._diagnostics: list[dict[str, str]] = []
        self._launch_count = 0
        self._graceful_shutdown_count = 0
        self._forced_shutdown_count = 0
        self._baseline_command_count = 0
        self._task_command_count = 0
        self._ledger_result: dict[str, object] = {}
        self._last_probe = DesktopProbe()
        self._last_probe_at = float("-inf")
        self._audit_healthy = True
        self._action_token = secrets.token_urlsafe(32)

    @property
    def audit_path(self) -> Path:
        return self._audit_path

    @property
    def action_token(self) -> str:
        return self._action_token

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            if thread is not None and thread.is_alive():
                self._diagnostics.append({"at": _utc_now(), "message": "maintenance_stop_timeout"})
            elif self._stage in _ACTIVE_STAGES:
                try:
                    self._transition(MaintenanceStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                except MaintenanceAuditError:
                    self._stage = MaintenanceStage.BLOCKED
                    self._stage_started_at = _utc_now()
                    self._blockers = ["MAINTENANCE_AUDIT_UNAVAILABLE"]

    def wait(self, timeout_seconds: float | None = None) -> MaintenanceStage:
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            return self._stage

    def _audit(self, event: str) -> None:
        with self._lock:
            record = {
                "schema": MAINTENANCE_SCHEMA,
                "recorded_at": _utc_now(),
                "event": event,
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": self._stage.value,
                "blockers": list(self._blockers),
                "manual_action": self._manual_action,
                "launch_count": self._launch_count,
                "graceful_shutdown_count": self._graceful_shutdown_count,
                "forced_shutdown_count": self._forced_shutdown_count,
                "execution_command_baseline": self._baseline_command_count,
                "execution_commands_sent_by_task": self._task_command_count,
                "ledger_verification_id": self._ledger_result.get("verification_id"),
                "ledger_verified_through_sequence": self._ledger_result.get("verified_through_sequence"),
                "ledger_verified_tip_hash": self._ledger_result.get("tip_hash"),
            }
        try:
            self._audit_path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            with self._audit_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as error:
            with self._lock:
                self._audit_healthy = False
                self._diagnostics.append({"at": _utc_now(), "message": f"audit_write_failed:{type(error).__name__}"})
            raise MaintenanceAuditError("MAINTENANCE_AUDIT_UNAVAILABLE") from error

    def _transition(
        self,
        stage: MaintenanceStage,
        blockers: list[str] | None = None,
        manual_action: str | None = None,
    ) -> None:
        with self._lock:
            self._stage = stage
            self._stage_started_at = _utc_now()
            self._blockers = list(blockers or [])
            self._manual_action = manual_action
        self._audit("STAGE_TRANSITION")

    def _probe(self, *, force: bool = False) -> DesktopProbe:
        with self._lock:
            if not force and self._clock() - self._last_probe_at < 1.0:
                return self._last_probe
        probe = self._desktop.probe()
        with self._lock:
            self._last_probe = probe
            self._last_probe_at = self._clock()
        return probe

    @staticmethod
    def _transport(paper: Mapping[str, object]) -> Mapping[str, object]:
        value = paper.get("transport")
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _observer(paper: Mapping[str, object]) -> Mapping[str, object]:
        value = paper.get("market_observer")
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _attachment(observer: Mapping[str, object]) -> Mapping[str, object]:
        value = observer.get("observer_attachment")
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    def _configured_instrument(paper: Mapping[str, object]) -> str:
        value = paper.get("market_instrument")
        return str(value) if isinstance(value, str) else "UNRESOLVED"

    @staticmethod
    def _command_count(paper: Mapping[str, object]) -> int:
        value = paper.get("last_command")
        if isinstance(value, Mapping) and type(value.get("command_sequence")) is int:
            return int(value["command_sequence"])
        return 0

    @classmethod
    def _addon_ready(cls, paper: Mapping[str, object]) -> bool:
        transport = cls._transport(paper)
        provenance = transport.get("addon_provenance")
        return (
            transport.get("state") == "AUTHENTICATED"
            and transport.get("authenticated_client") is True
            and isinstance(provenance, Mapping)
            and provenance.get("status") == "MATCH"
        )

    @classmethod
    def _chart_ready(cls, paper: Mapping[str, object]) -> bool:
        configured = cls._configured_instrument(paper)
        attachment = cls._attachment(cls._observer(paper))
        return (
            attachment.get("chart_found") is True
            and attachment.get("observer_attached") is True
            and attachment.get("instrument") == configured
            and attachment.get("configured_instrument") == configured
        )

    @classmethod
    def _market_fresh(cls, paper: Mapping[str, object]) -> bool:
        observer = cls._observer(paper)
        freshness = observer.get("market_observer_freshness")
        return (
            observer.get("market_observer_active") is True
            and isinstance(freshness, Mapping)
            and freshness.get("fresh") is True
        )

    @staticmethod
    def _ledger_gate(report: Mapping[str, object], paper: Mapping[str, object]) -> list[str]:
        ledger = paper.get("ledger")
        ledger = ledger if isinstance(ledger, Mapping) else {}
        operational = ledger.get("operational_ledger")
        operational = operational if isinstance(operational, Mapping) else {}
        verified = report.get("verified_through_sequence")
        tip = operational.get("tail_tip_sequence", report.get("captured_tip_sequence"))
        failures: list[str] = []
        if report.get("status") != "PASS": failures.append("LEDGER_VERIFICATION_NOT_PASS")
        if report.get("chain_valid") is not True: failures.append("LEDGER_CHAIN_INVALID_OR_UNKNOWN")
        if report.get("checkpoint_valid") is not True: failures.append("LEDGER_CHECKPOINT_INVALID_OR_UNKNOWN")
        if type(verified) is not int or type(tip) is not int or verified != tip:
            failures.append("LEDGER_UNVERIFIED_TAIL")
        if ledger.get("unverified_tail_rows") not in {0, None}:
            failures.append("LEDGER_UNVERIFIED_TAIL")
        return list(dict.fromkeys(failures))

    @classmethod
    def restart_gate_failures(
        cls,
        paper: Mapping[str, object],
        live: Mapping[str, object],
        ledger_report: Mapping[str, object],
    ) -> list[str]:
        failures: list[str] = []
        exact = (
            (paper.get("state") == "READY_DISARMED", "RUNTIME_NOT_READY_DISARMED"),
            (paper.get("paper_execution") == "DISARMED", "PAPER_RUNTIME_NOT_DISARMED"),
            (paper.get("session_armed_state") == "DISARMED", "SESSION_AUTHORITY_NOT_DISARMED"),
            (paper.get("live_capital") == "DENIED", "PAPER_LIVE_AUTHORITY_NOT_DENIED"),
            (live.get("live_authority") == "DISARMED", "LIVE_AUTHORITY_NOT_DISARMED"),
            (live.get("live_capital") == "DENIED", "LIVE_CAPITAL_NOT_DENIED"),
            (paper.get("account_class") == "LOCAL_SIMULATION", "ACCOUNT_NOT_LOCAL_SIMULATION"),
            (paper.get("current_position") == "FLAT", "POSITION_NOT_FLAT"),
            (paper.get("current_quantity") == 0, "POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("broker_snapshot_position") == "FLAT", "BROKER_POSITION_NOT_FLAT"),
            (paper.get("broker_snapshot_position_quantity") == 0, "BROKER_POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("position_snapshot_complete") is True, "POSITION_SNAPSHOT_INCOMPLETE"),
            (paper.get("order_snapshot_complete") is True, "ORDER_SNAPSHOT_INCOMPLETE"),
            (paper.get("working_owned_orders") == 0, "WORKING_OWNED_ORDERS_PRESENT"),
            (paper.get("working_entry_orders") == 0, "WORKING_ENTRY_ORDERS_PRESENT"),
            (paper.get("unresolved_command") is False, "UNRESOLVED_COMMAND_STATE"),
            (paper.get("unresolved_native_order") is False, "UNRESOLVED_NATIVE_ORDER_STATE"),
            (paper.get("unresolved_execution") is False, "UNRESOLVED_EXECUTION_STATE"),
            (paper.get("entry_owner") == "NONE", "UNRESOLVED_OWNERSHIP_STATE"),
            (paper.get("reconciliation_current") is True, "RECONCILIATION_NOT_CURRENT"),
        )
        failures.extend(code for passed, code in exact if not passed)
        commissioning = paper.get("commissioning_lifecycle")
        if not isinstance(commissioning, Mapping) or commissioning.get("active") is not False:
            failures.append("COMMISSIONING_OWNERSHIP_NOT_CLOSED")
        operational = paper.get("operational_paper_session")
        if isinstance(operational, Mapping) and operational.get("active") is True:
            failures.append("OPERATIONAL_PAPER_SESSION_ACTIVE")
        transport = cls._transport(paper)
        provenance = transport.get("addon_provenance")
        if transport.get("state") != "AUTHENTICATED" or transport.get("authenticated_client") is not True:
            failures.append("EXECUTION_ADDON_NOT_AUTHENTICATED")
        if not isinstance(provenance, Mapping) or provenance.get("status") != "MATCH":
            failures.append("ADDON_PROVENANCE_MISMATCH")
        if transport.get("reconciled") is not True:
            failures.append("EXECUTION_TRANSPORT_NOT_RECONCILED")
        failures.extend(cls._ledger_gate(ledger_report, paper))
        return list(dict.fromkeys(failures))

    @classmethod
    def _healthy(cls, probe: DesktopProbe, paper: Mapping[str, object]) -> bool:
        return (
            probe.process_detected and probe.control_center_detected
            and cls._addon_ready(paper) and cls._chart_ready(paper)
            and cls._market_fresh(paper) and paper.get("reconciliation_current") is True
            and paper.get("state") == "READY_DISARMED"
            and paper.get("paper_execution") == "DISARMED"
            and paper.get("live_capital") == "DENIED"
            and paper.get("account_class") == "LOCAL_SIMULATION"
            and paper.get("current_position") == "FLAT"
            and paper.get("current_quantity") == 0
            and paper.get("position_snapshot_complete") is True
            and paper.get("order_snapshot_complete") is True
            and paper.get("working_owned_orders") == 0
            and paper.get("working_entry_orders") == 0
            and paper.get("unresolved_command") is False
            and paper.get("unresolved_native_order") is False
            and paper.get("unresolved_execution") is False
            and paper.get("entry_owner") == "NONE"
        )

    def _button(self, probe: DesktopProbe, paper: Mapping[str, object]) -> dict[str, object]:
        with self._lock:
            stage = self._stage
        if stage in _ACTIVE_STAGES:
            return {"label": self._progress_label(stage), "enabled": False, "tone": "progress"}
        if self._healthy(probe, paper):
            return {"label": "NinjaTrader Ready", "enabled": False, "tone": "ready"}
        if probe.process_detected:
            return {"label": "Restart + Repair Observer", "enabled": True, "tone": "warning"}
        return {"label": "Open NinjaTrader + Attach Observer", "enabled": True, "tone": "primary"}

    @staticmethod
    def _progress_label(stage: MaintenanceStage) -> str:
        labels = {
            MaintenanceStage.CHECKING: "Checking NinjaTrader…",
            MaintenanceStage.VERIFYING_RESTART_LEDGER: "Verifying restart gates…",
            MaintenanceStage.GATE_CHECK: "Checking restart safety…",
            MaintenanceStage.GRACEFUL_SHUTDOWN: "Closing NinjaTrader gracefully…",
            MaintenanceStage.LAUNCHING: "Opening NinjaTrader…",
            MaintenanceStage.WAITING_FOR_OPERATOR_LOGIN: "Waiting for operator login…",
            MaintenanceStage.WAITING_FOR_ADDON: "Waiting for authenticated AddOn…",
            MaintenanceStage.VERIFYING_CHART: "Locating MNQ chart…",
            MaintenanceStage.WAITING_FOR_MANUAL_CHART_ATTACHMENT: "Waiting for observer attachment…",
            MaintenanceStage.VERIFYING_MARKET_DATA: "Verifying market data…",
            MaintenanceStage.RECONCILING: "Reconciling Sim101…",
            MaintenanceStage.FINAL_LEDGER_VERIFICATION: "Verifying final ledger…",
        }
        return labels.get(stage, "NinjaTrader maintenance in progress…")

    def status(self) -> dict[str, object]:
        try:
            paper = self._paper_status()
            if not isinstance(paper, Mapping):
                paper = {}
        except Exception as error:
            paper = {}
            with self._lock:
                self._diagnostics.append({"at": _utc_now(), "message": f"paper_status_failed:{type(error).__name__}"})
        probe = self._probe()
        transport = self._transport(paper)
        provenance = transport.get("addon_provenance")
        provenance = provenance if isinstance(provenance, Mapping) else {}
        observer = self._observer(paper)
        attachment = self._attachment(observer)
        freshness = observer.get("market_observer_freshness")
        freshness = freshness if isinstance(freshness, Mapping) else {}
        command_count = self._command_count(paper)
        with self._lock:
            self._task_command_count = max(0, command_count - self._baseline_command_count) if self._operation_id else 0
            stage = self._stage
            result = {
                "schema": MAINTENANCE_SCHEMA,
                "action_token": self._action_token,
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": stage.value,
                "stage_started_at": self._stage_started_at,
                "operation_started_at": self._operation_started_at,
                "in_progress": stage in _ACTIVE_STAGES,
                "button": self._button(probe, paper),
                "process": {
                    "state": "RUNNING" if probe.process_detected else "ABSENT",
                    "login_window_detected": probe.login_window_detected,
                    "control_center_detected": probe.control_center_detected,
                    "failure_category": probe.failure_category,
                },
                "addon": {
                    "state": transport.get("state", "UNSTARTED"),
                    "authenticated": transport.get("authenticated_client") is True,
                    "provenance": provenance.get("status", "UNVERIFIED"),
                    "protocol_version": provenance.get("protocol_version"),
                },
                "configured_instrument": self._configured_instrument(paper),
                "chart": {
                    "state": attachment.get("state", "UNKNOWN"),
                    "found": attachment.get("chart_found") is True,
                    "created": False,
                    "instrument": attachment.get("instrument"),
                },
                "observer": {
                    "state": observer.get("market_observer_state", "NOT_ACTIVE"),
                    "attached": attachment.get("observer_attached") is True,
                    "instrument": attachment.get("instrument"),
                    "last_attachment_at": attachment.get("observed_at"),
                    "market_data_fresh": freshness.get("fresh") is True,
                    "freshness_reason": freshness.get("reason", "MISSING_OBSERVATION_TIMESTAMP"),
                    "last_level_one_at": observer.get("last_level_one_at"),
                    "last_depth_at": observer.get("last_depth_at"),
                },
                "readiness": "READY" if self._healthy(probe, paper) else "BLOCKED" if stage in {MaintenanceStage.BLOCKED, MaintenanceStage.FAILED} else "NOT_READY",
                "blockers": list(self._blockers),
                "manual_action": self._manual_action,
                "actions": {
                    "launches": self._launch_count,
                    "graceful_shutdowns": self._graceful_shutdown_count,
                    "forced_shutdowns": self._forced_shutdown_count,
                    "execution_command_baseline": self._baseline_command_count,
                    "execution_commands_sent_by_task": self._task_command_count,
                    "current_execution_command_count": command_count,
                    "orders_submitted_by_task": 0,
                    "orders_cancelled_by_task": 0,
                },
                "ledger": dict(self._ledger_result),
                "diagnostics": list(self._diagnostics[-20:]),
                "audit": {"schema": MAINTENANCE_SCHEMA, "durable": self._audit_healthy},
                "authority": "OBSERVE_AND_DESKTOP_MAINTENANCE_ONLY",
            }
        return result

    def start(self, request_id: str) -> dict[str, object]:
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid NinjaTrader maintenance request ID.")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            if self._request_id == request_id and self._operation_id is not None:
                return self.status()
            self._stop = threading.Event()
            self._wait = self._stop.wait
            self._operation_id = f"ntm-{uuid4().hex}"
            self._request_id = request_id
            self._operation_started_at = _utc_now()
            self._stage = MaintenanceStage.CHECKING
            self._stage_started_at = self._operation_started_at
            self._blockers = []
            self._manual_action = None
            self._diagnostics = []
            self._audit_healthy = True
            self._launch_count = 0
            self._graceful_shutdown_count = 0
            self._forced_shutdown_count = 0
            paper = self._paper_status()
            self._baseline_command_count = self._command_count(paper)
            self._task_command_count = 0
            self._ledger_result = {}
            try:
                self._audit("OPERATION_STARTED")
            except MaintenanceAuditError:
                self._stage = MaintenanceStage.BLOCKED
                self._stage_started_at = _utc_now()
                self._blockers = ["MAINTENANCE_AUDIT_UNAVAILABLE"]
                return self.status()
            self._thread = threading.Thread(target=self._run, name="NinjaTraderMaintenance", daemon=True)
            self._thread.start()
        return self.status()

    def _wait_for(self, predicate: Callable[[], bool], timeout: float) -> bool:
        deadline = self._clock() + timeout
        while not self._stop.is_set() and self._clock() < deadline:
            if predicate():
                return True
            self._wait(min(self._timeouts.poll_seconds, max(0.001, deadline - self._clock())))
        return False

    def _verify_ledger(self, stage: MaintenanceStage) -> tuple[bool, Mapping[str, object]]:
        self._transition(stage)
        started = self._start_ledger_verification()
        report = started
        verification_id = started.get("verification_id") if isinstance(started, Mapping) else None

        def completed() -> bool:
            nonlocal report
            report = self._ledger_status()
            if verification_id and report.get("verification_id") != verification_id and report.get("status") == "IN_PROGRESS":
                return False
            return report.get("status") != "IN_PROGRESS"

        if started.get("status") == "IN_PROGRESS" and not self._wait_for(completed, self._timeouts.ledger_seconds):
            self._transition(MaintenanceStage.BLOCKED, ["LEDGER_VERIFICATION_TIMEOUT"])
            return False, report
        report = self._ledger_status()
        paper = self._paper_status()
        failures = self._ledger_gate(report, paper)
        if not failures:
            first_ledger = paper.get("ledger")
            first_ledger = first_ledger if isinstance(first_ledger, Mapping) else {}
            first_operational = first_ledger.get("operational_ledger")
            first_operational = first_operational if isinstance(first_operational, Mapping) else {}
            first_tip = first_operational.get("tail_tip_sequence", report.get("captured_tip_sequence"))
            stability_seconds = min(
                1.0,
                max(self._timeouts.poll_seconds, self._timeouts.ledger_seconds / 4.0),
            )
            self._wait(stability_seconds)
            stable_paper = self._paper_status()
            stable_ledger = stable_paper.get("ledger")
            stable_ledger = stable_ledger if isinstance(stable_ledger, Mapping) else {}
            stable_operational = stable_ledger.get("operational_ledger")
            stable_operational = stable_operational if isinstance(stable_operational, Mapping) else {}
            stable_tip = stable_operational.get("tail_tip_sequence", report.get("captured_tip_sequence"))
            failures.extend(self._ledger_gate(report, stable_paper))
            if type(first_tip) is not int or first_tip != stable_tip:
                failures.append("LEDGER_TIP_NOT_STABLE")
            paper = stable_paper
        with self._lock:
            self._ledger_result = {
                "status": report.get("status"),
                "verification_id": report.get("verification_id"),
                "verification_mode": report.get("verification_mode"),
                "verified_through_sequence": report.get("verified_through_sequence"),
                "tip_hash": report.get("tip_hash"),
                "chain_valid": report.get("chain_valid"),
                "checkpoint_valid": report.get("checkpoint_valid"),
                "unverified_tail_rows": (
                    paper.get("ledger", {}).get("unverified_tail_rows")
                    if isinstance(paper.get("ledger"), Mapping)
                    else None
                ),
            }
        if failures:
            self._transition(MaintenanceStage.BLOCKED, list(dict.fromkeys(failures)))
            return False, report
        return True, report

    def _run(self) -> None:
        try:
            probe = self._probe(force=True)
            if probe.failure_category:
                self._transition(MaintenanceStage.BLOCKED, [probe.failure_category])
                return
            paper = self._paper_status()
            if self._healthy(probe, paper):
                ok, _ = self._verify_ledger(MaintenanceStage.FINAL_LEDGER_VERIFICATION)
                if ok:
                    self._finish_ready()
                return
            instrument = self._configured_instrument(paper)
            if not self._desktop.configure_instrument(instrument):
                self._transition(MaintenanceStage.BLOCKED, ["CONFIGURED_INSTRUMENT_UNRESOLVED_OR_UNWRITABLE"])
                return
            if probe.process_detected:
                ok, report = self._verify_ledger(MaintenanceStage.VERIFYING_RESTART_LEDGER)
                if not ok:
                    return
                self._transition(MaintenanceStage.GATE_CHECK)
                paper = self._paper_status()
                live = self._live_status()
                failures = self.restart_gate_failures(paper, live, report)
                if failures:
                    self._transition(MaintenanceStage.BLOCKED, failures)
                    return
                self._transition(MaintenanceStage.GRACEFUL_SHUTDOWN)
                self._graceful_shutdown_count += 1
                self._audit("GRACEFUL_SHUTDOWN_REQUESTED")
                if not self._desktop.request_graceful_shutdown():
                    self._transition(MaintenanceStage.BLOCKED, ["GRACEFUL_SHUTDOWN_REFUSED"])
                    return
                if not self._wait_for(lambda: not self._probe(force=True).process_detected, self._timeouts.graceful_shutdown_seconds):
                    self._transition(
                        MaintenanceStage.BLOCKED,
                        ["GRACEFUL_SHUTDOWN_TIMEOUT_REQUIRES_OPERATOR_CONFIRMATION"],
                        "Close NinjaTrader manually, or separately authorize a forced termination; no force kill was attempted.",
                    )
                    return
            self._transition(MaintenanceStage.LAUNCHING)
            if not self._desktop.start():
                self._transition(MaintenanceStage.FAILED, ["NINJATRADER_START_FAILED"])
                return
            self._launch_count += 1
            self._audit("NINJATRADER_LAUNCH_REQUESTED")
            if not self._wait_for(lambda: self._probe(force=True).process_detected, self._timeouts.process_start_seconds):
                self._transition(MaintenanceStage.FAILED, ["NINJATRADER_PROCESS_START_TIMEOUT"])
                return
            self._transition(
                MaintenanceStage.WAITING_FOR_OPERATOR_LOGIN,
                [],
                "Complete NinjaTrader login manually; Beelzebub will not enter or store credentials.",
            )

            def control_center_ready() -> bool:
                value = self._probe(force=True)
                if not value.process_detected:
                    return False
                return value.control_center_detected

            if not self._wait_for(control_center_ready, self._timeouts.operator_login_seconds):
                self._transition(
                    MaintenanceStage.BLOCKED,
                    ["WAITING_FOR_OPERATOR_LOGIN"],
                    "Complete NinjaTrader login manually; Beelzebub will not enter or store credentials.",
                )
                return
            self._transition(MaintenanceStage.WAITING_FOR_ADDON)
            if not self._wait_for(lambda: self._addon_ready(self._paper_status()), self._timeouts.addon_seconds):
                paper = self._paper_status()
                transport = self._transport(paper)
                provenance = transport.get("addon_provenance")
                blocker = "ADDON_PROVENANCE_MISMATCH" if isinstance(provenance, Mapping) and provenance.get("status") not in {None, "MATCH"} else "AUTHENTICATED_ADDON_TIMEOUT"
                self._transition(MaintenanceStage.BLOCKED, [blocker])
                return
            self._transition(MaintenanceStage.VERIFYING_CHART)
            if not self._chart_ready(self._paper_status()):
                self._transition(
                    MaintenanceStage.WAITING_FOR_MANUAL_CHART_ATTACHMENT,
                    [],
                    f"Open or select the {instrument} chart and add BeelzebubReadOnlyMarketObserver once; the workflow will verify it automatically.",
                )
                if not self._wait_for(lambda: self._chart_ready(self._paper_status()), self._timeouts.chart_seconds):
                    paper = self._paper_status()
                    attachment = self._attachment(self._observer(paper))
                    blocker = "WRONG_CHART_INSTRUMENT" if attachment.get("chart_found") is True and attachment.get("instrument") not in {None, instrument} else "OBSERVER_ATTACHMENT_NOT_VERIFIED"
                    self._transition(MaintenanceStage.BLOCKED, [blocker], self._manual_action)
                    return
            self._transition(MaintenanceStage.VERIFYING_MARKET_DATA)
            if not self._wait_for(lambda: self._market_fresh(self._paper_status()), self._timeouts.market_data_seconds):
                self._transition(MaintenanceStage.BLOCKED, ["MARKET_DATA_FRESHNESS_TIMEOUT"])
                return
            self._transition(MaintenanceStage.RECONCILING)
            if not self._wait_for(self._final_reconciliation_ready, self._timeouts.reconciliation_seconds):
                self._transition(MaintenanceStage.BLOCKED, ["SIM101_RECONCILIATION_TIMEOUT"])
                return
            ok, _ = self._verify_ledger(MaintenanceStage.FINAL_LEDGER_VERIFICATION)
            if ok:
                self._finish_ready()
        except MaintenanceAuditError:
            with self._lock:
                self._stage = MaintenanceStage.BLOCKED
                self._stage_started_at = _utc_now()
                self._blockers = ["MAINTENANCE_AUDIT_UNAVAILABLE"]
                self._manual_action = None
        except Exception as error:
            with self._lock:
                self._diagnostics.append({
                    "at": _utc_now(),
                    "message": f"{type(error).__name__}:{_safe_text(error)}",
                })
            try:
                self._transition(MaintenanceStage.FAILED, ["MAINTENANCE_INTERNAL_FAILURE"])
            except MaintenanceAuditError:
                with self._lock:
                    self._stage = MaintenanceStage.BLOCKED
                    self._stage_started_at = _utc_now()
                    self._blockers = ["MAINTENANCE_AUDIT_UNAVAILABLE"]

    def _final_reconciliation_ready(self) -> bool:
        paper = self._paper_status()
        current_commands = self._command_count(paper)
        with self._lock:
            self._task_command_count = max(0, current_commands - self._baseline_command_count)
        if self._task_command_count:
            return False
        transport = self._transport(paper)
        return (
            paper.get("state") == "READY_DISARMED"
            and paper.get("paper_execution") == "DISARMED"
            and paper.get("live_capital") == "DENIED"
            and paper.get("account_class") == "LOCAL_SIMULATION"
            and paper.get("current_position") == "FLAT"
            and paper.get("current_quantity") == 0
            and paper.get("position_snapshot_complete") is True
            and paper.get("order_snapshot_complete") is True
            and paper.get("working_owned_orders") == 0
            and paper.get("working_entry_orders") == 0
            and paper.get("unresolved_command") is False
            and paper.get("unresolved_native_order") is False
            and paper.get("unresolved_execution") is False
            and paper.get("entry_owner") == "NONE"
            and paper.get("reconciliation_current") is True
            and transport.get("reconciled") is True
        )

    def _finish_ready(self) -> None:
        paper = self._paper_status()
        current = self._command_count(paper)
        with self._lock:
            self._task_command_count = max(0, current - self._baseline_command_count)
        if self._task_command_count:
            self._transition(MaintenanceStage.BLOCKED, ["UNEXPECTED_EXECUTION_COMMAND_DURING_MAINTENANCE"])
            return
        if not self._final_reconciliation_ready():
            self._transition(MaintenanceStage.BLOCKED, ["FINAL_READY_DISARMED_PROOF_FAILED"])
            return
        if not self._healthy(self._probe(force=True), paper):
            self._transition(MaintenanceStage.BLOCKED, ["FINAL_OBSERVER_READINESS_PROOF_FAILED"])
            return
        self._transition(MaintenanceStage.READY)
