"""One-click, backend-owned startup for persistent Sim101 paper operation."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Callable, Mapping
from uuid import uuid4


PAPER_AUTOSTART_SCHEMA = "lane-iii-paper-autostart-v1"
PAPER_AUTOSTART_ACTION_HEADER = "X-Beelzebub-Paper-Autostart-Action"
PAPER_AUTOSTART_ACTION_VALUE = "sim101-paper-autostart-v1"
PAPER_AUTOSTART_TOKEN_HEADER = "X-Beelzebub-Paper-Autostart-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PaperAutoStartStage(StrEnum):
    IDLE = "IDLE"
    ENSURING_NINJATRADER = "ENSURING_NINJATRADER"
    VERIFYING_FULL_LEDGER = "VERIFYING_FULL_LEDGER"
    STARTING_PAPER = "STARTING_PAPER"
    RUNNING = "RUNNING"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_ACTIVE = frozenset({
    PaperAutoStartStage.ENSURING_NINJATRADER,
    PaperAutoStartStage.VERIFYING_FULL_LEDGER,
    PaperAutoStartStage.STARTING_PAPER,
})


class PaperAutoStartService:
    """Compose existing guarded startup operations without browser authority.

    The service never restarts NinjaTrader. It may launch an absent desktop via
    the normal maintenance startup mode, waits for authenticated observer and
    Sim101 reconciliation, obtains a current Full ledger proof, and only then
    invokes the canonical persistent operational-start method.
    """

    def __init__(
        self,
        *,
        paper_status: Callable[[], Mapping[str, object]],
        ensure_ninjatrader: Callable[[str], Mapping[str, object]],
        ninjatrader_status: Callable[[], Mapping[str, object]],
        start_full_verification: Callable[[], Mapping[str, object]],
        ledger_status: Callable[[], Mapping[str, object]],
        start_operational_paper: Callable[[str], Mapping[str, object]],
        audit_path: str | Path,
        startup_timeout_seconds: float = 360.0,
        ledger_timeout_seconds: float = 180.0,
        poll_seconds: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], bool] | None = None,
    ) -> None:
        if min(startup_timeout_seconds, ledger_timeout_seconds, poll_seconds) <= 0:
            raise ValueError("Paper auto-start timeouts must be positive.")
        self._paper_status = paper_status
        self._ensure_ninjatrader = ensure_ninjatrader
        self._ninjatrader_status = ninjatrader_status
        self._start_full_verification = start_full_verification
        self._ledger_status = ledger_status
        self._start_operational_paper = start_operational_paper
        self._audit_path = Path(audit_path).resolve()
        self._startup_timeout_seconds = startup_timeout_seconds
        self._ledger_timeout_seconds = ledger_timeout_seconds
        self._poll_seconds = poll_seconds
        self._clock = clock
        self._stop = threading.Event()
        self._custom_wait = wait
        self._wait = wait or self._stop.wait
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._operation_id: str | None = None
        self._request_id: str | None = None
        self._stage = PaperAutoStartStage.IDLE
        self._stage_started_at = _utc_now()
        self._started_at: str | None = None
        self._blockers: list[str] = []
        self._maintenance: dict[str, object] = {}
        self._ledger: dict[str, object] = {}
        self._result: dict[str, object] = {}
        self._diagnostics: list[dict[str, str]] = []
        self._audit_healthy = True
        self._action_token = secrets.token_urlsafe(32)

    @property
    def action_token(self) -> str:
        return self._action_token

    @staticmethod
    def _operational_active(paper: Mapping[str, object]) -> bool:
        session = paper.get("operational_paper_session")
        return isinstance(session, Mapping) and session.get("active") is True

    @staticmethod
    def _base_start_failures(paper: Mapping[str, object]) -> list[str]:
        checks = (
            (paper.get("paper_execution") == "DISARMED", "PAPER_RUNTIME_NOT_DISARMED"),
            (paper.get("session_armed_state") == "DISARMED", "SESSION_AUTHORITY_NOT_DISARMED"),
            (paper.get("live_capital") == "DENIED", "LIVE_CAPITAL_NOT_DENIED"),
            (paper.get("account_class") == "LOCAL_SIMULATION", "ACCOUNT_NOT_LOCAL_SIMULATION"),
            (paper.get("market_instrument") == "MNQ SEP26", "INSTRUMENT_NOT_MNQ_SEP26"),
            (paper.get("current_position") == "FLAT", "POSITION_NOT_FLAT"),
            (paper.get("current_quantity") == 0, "POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("working_owned_orders") == 0, "WORKING_OWNED_ORDERS_PRESENT"),
            (paper.get("working_entry_orders") == 0, "WORKING_ENTRY_ORDERS_PRESENT"),
            (paper.get("unresolved_command") is False, "UNRESOLVED_COMMAND_STATE"),
            (paper.get("unresolved_native_order") is False, "UNRESOLVED_NATIVE_ORDER_STATE"),
            (paper.get("unresolved_execution") is False, "UNRESOLVED_EXECUTION_STATE"),
            (paper.get("entry_owner") == "NONE", "UNRESOLVED_OWNERSHIP_STATE"),
        )
        return [code for passed, code in checks if not passed]

    @classmethod
    def _ready_failures(cls, paper: Mapping[str, object], maintenance: Mapping[str, object]) -> list[str]:
        failures = cls._base_start_failures(paper)
        checks = (
            (paper.get("state") == "READY_DISARMED", "RUNTIME_NOT_READY_DISARMED"),
            (paper.get("broker_snapshot_position") == "FLAT", "BROKER_POSITION_NOT_FLAT"),
            (paper.get("broker_snapshot_position_quantity") == 0, "BROKER_POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("position_snapshot_complete") is True, "POSITION_SNAPSHOT_INCOMPLETE"),
            (paper.get("order_snapshot_complete") is True, "ORDER_SNAPSHOT_INCOMPLETE"),
            (paper.get("reconciliation_current") is True, "RECONCILIATION_NOT_CURRENT"),
            (maintenance.get("stage") == "READY", "NINJATRADER_NOT_READY"),
        )
        failures.extend(code for passed, code in checks if not passed)
        return list(dict.fromkeys(failures))

    def _button(self, paper: Mapping[str, object]) -> dict[str, object]:
        if self._operational_active(paper):
            return {"label": "Paper Trading Running", "enabled": False, "tone": "ready"}
        if self._stage in _ACTIVE:
            labels = {
                PaperAutoStartStage.ENSURING_NINJATRADER: "Starting NinjaTrader…",
                PaperAutoStartStage.VERIFYING_FULL_LEDGER: "Verifying ledger…",
                PaperAutoStartStage.STARTING_PAPER: "Starting paper trading…",
            }
            return {"label": labels[self._stage], "enabled": False, "tone": "progress"}
        failures = self._base_start_failures(paper)
        return {"label": "Start Paper Trading", "enabled": not failures, "tone": "primary" if not failures else "blocked"}

    def status(self) -> dict[str, object]:
        try:
            paper = self._paper_status()
            paper = paper if isinstance(paper, Mapping) else {}
        except Exception:
            paper = {}
        with self._lock:
            if self._operational_active(paper) and self._stage not in _ACTIVE:
                stage = PaperAutoStartStage.RUNNING
            elif self._stage == PaperAutoStartStage.RUNNING:
                stage = PaperAutoStartStage.IDLE
            else:
                stage = self._stage
            return {
                "schema": PAPER_AUTOSTART_SCHEMA,
                "action_token": self._action_token,
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": stage.value,
                "stage_started_at": self._stage_started_at,
                "started_at": self._started_at,
                "in_progress": stage in _ACTIVE,
                "button": self._button(paper),
                "blockers": list(self._blockers),
                "maintenance": dict(self._maintenance),
                "ledger": dict(self._ledger),
                "result": dict(self._result),
                "diagnostics": list(self._diagnostics[-20:]),
                "audit": {"schema": PAPER_AUTOSTART_SCHEMA, "durable": self._audit_healthy},
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
            }

    def _audit(self, event: str) -> None:
        with self._lock:
            record = {
                "schema": PAPER_AUTOSTART_SCHEMA,
                "event": event,
                "recorded_at": _utc_now(),
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": self._stage.value,
                "blockers": list(self._blockers),
                "maintenance_stage": self._maintenance.get("stage"),
                "ledger_verification_id": self._ledger.get("verification_id"),
                "ledger_verified_through_sequence": self._ledger.get("verified_through_sequence"),
                "operational_started": self._result.get("started") is True,
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
            }
            path = self._audit_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            encoded = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, encoded.encode("utf-8"))
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            with self._lock:
                self._audit_healthy = False
            raise RuntimeError("PAPER_AUTOSTART_AUDIT_UNAVAILABLE") from error

    def _transition(self, stage: PaperAutoStartStage, blockers: list[str] | None = None) -> None:
        with self._lock:
            self._stage = stage
            self._stage_started_at = _utc_now()
            self._blockers = list(blockers or [])
        self._audit("STAGE_TRANSITION")

    def start(self, request_id: str) -> dict[str, object]:
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid paper auto-start request ID.")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            if self._request_id == request_id and self._operation_id is not None:
                return self.status()
            self._stop = threading.Event()
            self._wait = self._custom_wait or self._stop.wait
            self._operation_id = f"paper-auto-{uuid4().hex}"
            self._request_id = request_id
            self._started_at = _utc_now()
            self._stage = PaperAutoStartStage.ENSURING_NINJATRADER
            self._stage_started_at = self._started_at
            self._blockers = []
            self._maintenance = {}
            self._ledger = {}
            self._result = {}
            self._diagnostics = []
            self._audit_healthy = True
            try:
                self._audit("OPERATION_STARTED")
            except RuntimeError:
                self._stage = PaperAutoStartStage.BLOCKED
                self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
                return self.status()
            try:
                paper = self._paper_status()
                failures = self._base_start_failures(paper if isinstance(paper, Mapping) else {})
            except Exception:
                failures = ["PAPER_RUNTIME_STATUS_UNAVAILABLE"]
            if failures:
                self._stage = PaperAutoStartStage.BLOCKED
                self._stage_started_at = _utc_now()
                self._blockers = failures
                try:
                    self._audit("STAGE_TRANSITION")
                except RuntimeError:
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
                return self.status()
            self._thread = threading.Thread(target=self._run, name="PaperAutoStart", daemon=True)
            self._thread.start()
        return self.status()

    def _wait_for(self, predicate: Callable[[], bool], timeout: float) -> bool:
        deadline = self._clock() + timeout
        while not self._stop.is_set() and self._clock() < deadline:
            if predicate():
                return True
            self._wait(min(self._poll_seconds, max(0.001, deadline - self._clock())))
        return False

    def wait(self, timeout_seconds: float | None = None) -> PaperAutoStartStage:
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            return self._stage

    def _run(self) -> None:
        try:
            assert self._request_id is not None
            self._transition(PaperAutoStartStage.ENSURING_NINJATRADER)
            self._ensure_ninjatrader(self._request_id)

            def maintenance_done() -> bool:
                value = self._ninjatrader_status()
                with self._lock:
                    self._maintenance = dict(value) if isinstance(value, Mapping) else {}
                return self._maintenance.get("in_progress") is not True

            if not self._wait_for(maintenance_done, self._startup_timeout_seconds):
                self._transition(PaperAutoStartStage.BLOCKED, ["NINJATRADER_STARTUP_TIMEOUT"])
                return
            if self._maintenance.get("stage") != "READY":
                blockers = self._maintenance.get("blockers")
                failures = [str(value) for value in blockers] if isinstance(blockers, list) else []
                self._transition(PaperAutoStartStage.BLOCKED, failures or ["NINJATRADER_STARTUP_BLOCKED"])
                return
            paper = self._paper_status()
            failures = self._ready_failures(paper, self._maintenance)
            if failures:
                self._transition(PaperAutoStartStage.BLOCKED, failures)
                return

            self._transition(PaperAutoStartStage.VERIFYING_FULL_LEDGER)
            started = self._start_full_verification()
            verification_id = started.get("verification_id") if isinstance(started, Mapping) else None

            def ledger_done() -> bool:
                value = self._ledger_status()
                with self._lock:
                    self._ledger = dict(value) if isinstance(value, Mapping) else {}
                if verification_id and self._ledger.get("verification_id") != verification_id:
                    return False
                return self._ledger.get("status") != "IN_PROGRESS"

            if not self._wait_for(ledger_done, self._ledger_timeout_seconds):
                self._transition(PaperAutoStartStage.BLOCKED, ["FULL_LEDGER_VERIFICATION_TIMEOUT"])
                return
            # The verifier controller retains an already-running scan.  If it
            # was incremental, wait for that read-only scan to finish and then
            # launch the Full proof required by this workflow.  Never cancel or
            # relabel the pre-existing verification.
            if self._ledger.get("verification_mode") != "full":
                started = self._start_full_verification()
                verification_id = started.get("verification_id") if isinstance(started, Mapping) else None
                if not self._wait_for(ledger_done, self._ledger_timeout_seconds):
                    self._transition(PaperAutoStartStage.BLOCKED, ["FULL_LEDGER_VERIFICATION_TIMEOUT"])
                    return
            paper = self._paper_status()
            failures = []
            if self._ledger.get("status") != "PASS" or self._ledger.get("verification_mode") != "full":
                failures.append("FULL_LEDGER_VERIFICATION_NOT_PASS")
            if self._ledger.get("chain_valid") is not True or self._ledger.get("checkpoint_valid") is not True:
                failures.append("LEDGER_CHAIN_OR_CHECKPOINT_INVALID")
            if (
                type(self._ledger.get("captured_tip_sequence")) is not int
                or self._ledger.get("verified_through_sequence") != self._ledger.get("captured_tip_sequence")
            ):
                failures.append("FULL_LEDGER_CAPTURED_TIP_NOT_VERIFIED")
            failures.extend(self._ready_failures(paper, self._maintenance))
            if failures:
                self._transition(PaperAutoStartStage.BLOCKED, list(dict.fromkeys(failures)))
                return

            self._transition(PaperAutoStartStage.STARTING_PAPER)
            result = self._start_operational_paper(self._request_id)
            with self._lock:
                self._result = dict(result) if isinstance(result, Mapping) else {}
            paper = self._paper_status()
            if self._result.get("started") is not True and not self._operational_active(paper):
                reasons = self._result.get("reason_codes")
                failures = [str(value) for value in reasons] if isinstance(reasons, list) else []
                self._transition(PaperAutoStartStage.BLOCKED, failures or ["OPERATIONAL_PAPER_START_REFUSED"])
                return
            self._transition(PaperAutoStartStage.RUNNING)
        except RuntimeError as error:
            blocker = str(error) if str(error).isupper() else "PAPER_AUTOSTART_INTERNAL_FAILURE"
            try:
                self._transition(PaperAutoStartStage.BLOCKED, [blocker])
            except RuntimeError:
                with self._lock:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
        except Exception as error:
            with self._lock:
                self._diagnostics.append({"at": _utc_now(), "message": type(error).__name__})
            try:
                self._transition(PaperAutoStartStage.FAILED, ["PAPER_AUTOSTART_INTERNAL_FAILURE"])
            except RuntimeError:
                with self._lock:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            if self._stage in _ACTIVE:
                try:
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                except RuntimeError:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
