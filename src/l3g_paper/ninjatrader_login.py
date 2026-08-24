"""Bounded, credential-redacted NinjaTrader desktop login bootstrap.

The production adapter delegates Windows UI Automation and DPAPI access to a
local PowerShell helper.  No credential value crosses the process command line,
environment, stdout, stderr, logger, API model, or repository boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Callable, Protocol


class NinjaTraderLoginState(str, Enum):
    UNSTARTED = "UNSTARTED"
    STARTING_NINJATRADER = "STARTING_NINJATRADER"
    WAITING_FOR_LOGIN = "WAITING_FOR_LOGIN"
    SUBMITTING_LOGIN = "SUBMITTING_LOGIN"
    WAITING_FOR_CONTROL_CENTER = "WAITING_FOR_CONTROL_CENTER"
    WAITING_FOR_LUCID_CONNECTION = "WAITING_FOR_LUCID_CONNECTION"
    AUTHENTICATED = "AUTHENTICATED"
    BLOCKED = "BLOCKED"
    FAULTED = "FAULTED"


_ALLOWED_FAILURES = frozenset({
    "AMBIGUOUS_LOGIN_WINDOW",
    "AUTOMATION_PROCESS_FAILED",
    "CONTROL_CENTER_NOT_IDENTIFIED",
    "CORRUPT_DPAPI_SECRET",
    "INVALID_CREDENTIALS",
    "LOGIN_AUTOMATION_TIMEOUT",
    "LOGIN_WINDOW_NOT_IDENTIFIED",
    "LUCID_CONNECTION_TIMEOUT",
    "MFA_OR_CHALLENGE_PRESENT",
    "MISSING_LOCAL_SECRET",
    "MULTIPLE_NINJATRADER_PROCESSES",
    "NINJATRADER_PROCESS_EXITED",
    "NINJATRADER_START_FAILED",
    "POWERSHELL_HELPER_INVALID_RESPONSE",
    "UNEXPECTED_LOGIN_UI",
})
_TERMINAL_PROBE_FAILURES = frozenset({
    "AMBIGUOUS_LOGIN_WINDOW",
    "MFA_OR_CHALLENGE_PRESENT",
    "MULTIPLE_NINJATRADER_PROCESSES",
    "UNEXPECTED_LOGIN_UI",
})
_FAULT_PROBE_FAILURES = frozenset({"AUTOMATION_PROCESS_FAILED", "POWERSHELL_HELPER_INVALID_RESPONSE"})


@dataclass(frozen=True)
class NinjaTraderLoginProbe:
    process_detected: bool = False
    login_window_detected: bool = False
    control_center_detected: bool = False
    lucid_connection_state: str = "UNKNOWN"
    failure_category: str | None = None

    def __post_init__(self) -> None:
        if self.lucid_connection_state not in {"CONNECTED", "DISCONNECTED", "UNKNOWN"}:
            raise ValueError("Invalid sanitized Lucid connection state.")
        if self.failure_category is not None and self.failure_category not in _ALLOWED_FAILURES:
            raise ValueError("Invalid sanitized NinjaTrader failure category.")


class NinjaTraderLoginAdapter(Protocol):
    def probe(self) -> NinjaTraderLoginProbe: ...
    def start_ninjatrader(self) -> bool: ...
    def submit_login(self) -> str: ...
    def connect_lucid(self) -> bool: ...


class PowerShellNinjaTraderLoginAdapter:
    """Narrow subprocess boundary for the Windows-only UI/DPAPI helper."""

    def __init__(self, script_path: str | Path | None = None, *, command_timeout_seconds: float = 15.0) -> None:
        default = Path(__file__).resolve().parents[2] / "tools" / "ninjatrader_autologin.ps1"
        self.script_path = Path(script_path or default).resolve()
        self.command_timeout_seconds = command_timeout_seconds

    @staticmethod
    def _windows_powershell_environment() -> dict[str, str]:
        """Return a credential-free Windows PowerShell 5.1 environment.

        Codex can run under PowerShell 7 and publish its module directories in
        PSModulePath.  Windows PowerShell 5.1 cannot load those modules, so the
        desktop helper receives only native Windows PowerShell module roots and
        a narrow allowlist of ordinary process variables.
        """
        source = os.environ
        system_root = source.get("SystemRoot") or source.get("WINDIR") or r"C:\Windows"
        user_profile = source.get("USERPROFILE") or str(Path.home())
        program_files = source.get("ProgramFiles") or r"C:\Program Files"
        allowed = (
            "SystemRoot", "WINDIR", "USERPROFILE", "ProgramFiles", "ProgramData",
            "LOCALAPPDATA", "APPDATA", "TEMP", "TMP", "PATH", "PATHEXT", "ComSpec",
        )
        environment = {name: source[name] for name in allowed if source.get(name)}
        environment.update({
            "SystemRoot": system_root,
            "WINDIR": system_root,
            "USERPROFILE": user_profile,
            "ProgramFiles": program_files,
            "PSModulePath": ";".join((
                str(Path(user_profile) / "Documents" / "WindowsPowerShell" / "Modules"),
                str(Path(program_files) / "WindowsPowerShell" / "Modules"),
                str(Path(system_root) / "System32" / "WindowsPowerShell" / "v1.0" / "Modules"),
            )),
        })
        return environment

    def _run(self, action: str) -> dict[str, object]:
        if action not in {"probe", "start", "submit-login", "connect-lucid"} or not self.script_path.is_file():
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
                env=self._windows_powershell_environment(),
            )
        except (OSError, subprocess.SubprocessError):
            return {"ok": False, "failure_category": "AUTOMATION_PROCESS_FAILED"}
        if completed.returncode != 0:
            return {"ok": False, "failure_category": "AUTOMATION_PROCESS_FAILED"}
        lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
        if len(lines) != 1:
            return {"ok": False, "failure_category": "POWERSHELL_HELPER_INVALID_RESPONSE"}
        try:
            payload = json.loads(lines[0])
        except (TypeError, json.JSONDecodeError):
            return {"ok": False, "failure_category": "POWERSHELL_HELPER_INVALID_RESPONSE"}
        if not isinstance(payload, dict):
            return {"ok": False, "failure_category": "POWERSHELL_HELPER_INVALID_RESPONSE"}
        return payload

    @staticmethod
    def _failure(payload: dict[str, object]) -> str | None:
        value = payload.get("failure_category")
        return str(value) if value in _ALLOWED_FAILURES else None

    def probe(self) -> NinjaTraderLoginProbe:
        payload = self._run("probe")
        failure = self._failure(payload)
        if payload.get("ok") is not True and failure is None:
            failure = "POWERSHELL_HELPER_INVALID_RESPONSE"
        try:
            return NinjaTraderLoginProbe(
                process_detected=payload.get("process_detected") is True,
                login_window_detected=payload.get("login_window_detected") is True,
                control_center_detected=payload.get("control_center_detected") is True,
                lucid_connection_state=str(payload.get("lucid_connection_state", "UNKNOWN")),
                failure_category=failure,
            )
        except ValueError:
            return NinjaTraderLoginProbe(failure_category="POWERSHELL_HELPER_INVALID_RESPONSE")

    def start_ninjatrader(self) -> bool:
        return self._run("start").get("ok") is True

    def submit_login(self) -> str:
        payload = self._run("submit-login")
        if payload.get("ok") is True and payload.get("result") == "SUBMITTED":
            return "SUBMITTED"
        return self._failure(payload) or "POWERSHELL_HELPER_INVALID_RESPONSE"

    def connect_lucid(self) -> bool:
        return self._run("connect-lucid").get("ok") is True


class NinjaTraderLoginBootstrap:
    """One bounded, non-authoritative desktop-authentication state machine."""

    def __init__(
        self,
        adapter: NinjaTraderLoginAdapter | None = None,
        *,
        maximum_attempts: int = 2,
        minimum_attempt_delay_seconds: float = 15.0,
        authentication_timeout_seconds: float = 90.0,
        poll_interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] | None = None,
    ) -> None:
        if maximum_attempts != 2 or minimum_attempt_delay_seconds < 15 or authentication_timeout_seconds > 90:
            raise ValueError("NinjaTrader login limits may not exceed the sealed attempt contract.")
        if authentication_timeout_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError("NinjaTrader login timing must be positive.")
        self._adapter = adapter or PowerShellNinjaTraderLoginAdapter()
        self._maximum_attempts = maximum_attempts
        self._minimum_attempt_delay_seconds = minimum_attempt_delay_seconds
        self._authentication_timeout_seconds = authentication_timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wait = wait or self._stop.wait
        self._thread: threading.Thread | None = None
        self._state = NinjaTraderLoginState.UNSTARTED
        self._attempt_count = 0
        self._started_at: float | None = None
        self._process_detected = False
        self._login_window_detected = False
        self._control_center_detected = False
        self._lucid_connection_state = "UNKNOWN"
        self._failure_category: str | None = None

    @property
    def state(self) -> NinjaTraderLoginState:
        with self._lock:
            return self._state

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._state is not NinjaTraderLoginState.UNSTARTED:
                return
            self._started_at = self._clock()
            self._state = NinjaTraderLoginState.WAITING_FOR_LOGIN
            self._thread = threading.Thread(target=self._run, name="NinjaTraderLoginBootstrap", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=20.0)

    def wait(self, timeout: float | None = None) -> NinjaTraderLoginState:
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
        return self.state

    def _update_probe(self, probe: NinjaTraderLoginProbe) -> None:
        with self._lock:
            self._process_detected = probe.process_detected
            self._login_window_detected = probe.login_window_detected
            self._control_center_detected = probe.control_center_detected
            self._lucid_connection_state = probe.lucid_connection_state

    def _transition(self, state: NinjaTraderLoginState, failure: str | None = None) -> None:
        with self._lock:
            self._state = state
            self._failure_category = failure

    def _blocked(self, failure: str) -> None:
        self._transition(NinjaTraderLoginState.BLOCKED, failure if failure in _ALLOWED_FAILURES else "UNEXPECTED_LOGIN_UI")

    def _run(self) -> None:
        assert self._started_at is not None
        deadline = self._started_at + self._authentication_timeout_seconds
        process_started = False
        process_seen = False
        lucid_connect_requested = False
        last_attempt_at = float("-inf")
        while not self._stop.is_set() and self._clock() < deadline:
            probe = self._adapter.probe()
            self._update_probe(probe)
            if probe.failure_category in _FAULT_PROBE_FAILURES:
                self._transition(NinjaTraderLoginState.FAULTED, probe.failure_category)
                return
            if probe.failure_category in _TERMINAL_PROBE_FAILURES:
                # After the exact Log In control is invoked, NinjaTrader 8.1.6.3
                # briefly retains the Welcome window while removing its login
                # controls.  No further input is permitted in this state; wait
                # only for the already-bounded Control Center timeout.  The
                # identical unexpected shell before a submission still blocks.
                if probe.failure_category == "UNEXPECTED_LOGIN_UI" and self._attempt_count > 0:
                    self._transition(NinjaTraderLoginState.WAITING_FOR_CONTROL_CENTER)
                    self._wait(self._poll_interval_seconds)
                    continue
                self._blocked(probe.failure_category)
                return
            if not probe.process_detected:
                if process_seen:
                    self._blocked("NINJATRADER_PROCESS_EXITED")
                    return
                if process_started:
                    self._transition(NinjaTraderLoginState.STARTING_NINJATRADER)
                    self._wait(self._poll_interval_seconds)
                    continue
                self._transition(NinjaTraderLoginState.STARTING_NINJATRADER)
                if not self._adapter.start_ninjatrader():
                    self._transition(NinjaTraderLoginState.FAULTED, "NINJATRADER_START_FAILED")
                    return
                process_started = True
                self._wait(self._poll_interval_seconds)
                continue
            process_seen = True
            if probe.control_center_detected:
                if probe.lucid_connection_state == "CONNECTED":
                    self._transition(NinjaTraderLoginState.AUTHENTICATED)
                    return
                self._transition(NinjaTraderLoginState.WAITING_FOR_LUCID_CONNECTION)
                if not lucid_connect_requested:
                    if not self._adapter.connect_lucid():
                        self._blocked("CONTROL_CENTER_NOT_IDENTIFIED")
                        return
                    lucid_connect_requested = True
                self._wait(self._poll_interval_seconds)
                continue
            if probe.login_window_detected:
                if probe.failure_category == "INVALID_CREDENTIALS" and self._attempt_count >= self._maximum_attempts:
                    self._blocked("INVALID_CREDENTIALS")
                    return
                delay = self._minimum_attempt_delay_seconds - (self._clock() - last_attempt_at)
                if delay > 0:
                    self._transition(NinjaTraderLoginState.WAITING_FOR_LOGIN)
                    self._wait(min(delay, self._poll_interval_seconds))
                    continue
                if self._attempt_count >= self._maximum_attempts:
                    self._blocked("INVALID_CREDENTIALS")
                    return
                self._transition(NinjaTraderLoginState.SUBMITTING_LOGIN)
                with self._lock:
                    self._attempt_count += 1
                result = self._adapter.submit_login()
                last_attempt_at = self._clock()
                if result != "SUBMITTED":
                    if result == "INVALID_CREDENTIALS" and self._attempt_count < self._maximum_attempts:
                        self._transition(NinjaTraderLoginState.WAITING_FOR_LOGIN)
                        continue
                    self._blocked(result)
                    return
                self._transition(NinjaTraderLoginState.WAITING_FOR_CONTROL_CENTER)
                self._wait(self._poll_interval_seconds)
                continue
            self._transition(
                NinjaTraderLoginState.WAITING_FOR_CONTROL_CENTER
                if self._attempt_count else NinjaTraderLoginState.WAITING_FOR_LOGIN
            )
            self._wait(self._poll_interval_seconds)
        if not self._stop.is_set():
            failure = "LUCID_CONNECTION_TIMEOUT" if self._control_center_detected else "LOGIN_AUTOMATION_TIMEOUT"
            self._blocked(failure)

    def status(self) -> dict[str, object]:
        with self._lock:
            elapsed = 0.0 if self._started_at is None else max(0.0, self._clock() - self._started_at)
            return {
                "schema": "lane-iii-phase-g-ninjatrader-login-bootstrap-v1",
                "state": self._state.value,
                "attempt_count": self._attempt_count,
                "maximum_attempts": self._maximum_attempts,
                "elapsed_seconds": round(elapsed, 3),
                "authentication_timeout_seconds": self._authentication_timeout_seconds,
                "ninjatrader_process_detected": self._process_detected,
                "login_window_detected": self._login_window_detected,
                "control_center_detected": self._control_center_detected,
                "lucid_connection_state": self._lucid_connection_state,
                "failure_category": self._failure_category,
            }
