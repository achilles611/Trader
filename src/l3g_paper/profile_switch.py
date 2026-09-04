"""Fail-closed, process-bound profile switching for Sim101 paper operation.

The active process prepares a fresh ledger/audit run, flattens and disarms the
current profile, then delegates restart to a detached local supervisor.  The
supervisor will not launch the target profile unless the old process publishes
a complete controlled-ledger-shutdown receipt.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from .contracts import PAPER_PROFILE_CATALOG, PaperProfileDefinition, resolve_paper_profile


PROFILE_SWITCH_SCHEMA = "lane-iii-paper-profile-switch-v1"
PROFILE_SWITCH_ACTION_HEADER = "X-Beelzebub-Profile-Switch-Action"
PROFILE_SWITCH_ACTION_VALUE = "sim101-profile-switch-v1"
PROFILE_SWITCH_TOKEN_HEADER = "X-Beelzebub-Profile-Switch-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_ACTIVE_STAGES = frozenset({"PREPARING", "STOPPING_CURRENT", "AWAITING_FLAT", "SHUTDOWN_REQUESTED", "STARTING_TARGET", "AUTOSTARTING_TARGET"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(payload: Mapping[str, object]) -> bytes:
    return json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    encoded = json.dumps(dict(payload), sort_keys=True, indent=2) + "\n"
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("PROFILE_SWITCH_STATE_INVALID")
    return value


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, _canonical(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def profile_catalog() -> list[dict[str, object]]:
    return [profile.payload() for profile in PAPER_PROFILE_CATALOG]


def exact_flat_shutdown_ready(status: Mapping[str, object]) -> bool:
    ledger = status.get("ledger")
    ledger = ledger if isinstance(ledger, Mapping) else {}
    return (
        status.get("state") == "READY_DISARMED"
        and status.get("paper_execution") == "DISARMED"
        and status.get("session_armed_state") == "DISARMED"
        and status.get("current_position") == "FLAT"
        and status.get("current_quantity") == 0
        and status.get("broker_snapshot_position") == "FLAT"
        and status.get("broker_snapshot_position_quantity") == 0
        and status.get("working_owned_orders") == 0
        and status.get("working_entry_orders") == 0
        and status.get("unresolved_command") is False
        and status.get("unresolved_native_order") is False
        and status.get("unresolved_execution") is False
        and status.get("entry_owner") == "NONE"
        and status.get("operational_paper_session") is None
        and status.get("reconciliation_current") is True
        and ledger.get("deferred_queue_depth") == 0
        and ledger.get("deferred_pending_queue_depth") == 0
        and ledger.get("deferred_inflight_queue_depth") == 0
        and ledger.get("deferred_pending_barrier_count") == 0
        and ledger.get("deferred_writer_error") is None
    )


class PaperProfileSwitchService:
    """Prepare and initiate exactly one authenticated profile handoff."""

    def __init__(
        self,
        *,
        current_profile: PaperProfileDefinition,
        paper_status: Callable[[], Mapping[str, object]],
        flatten_and_disarm: Callable[[], Mapping[str, object]],
        verifier_status: Callable[[], Mapping[str, object]],
        request_shutdown: Callable[[], None],
        runtime_root: str | Path,
        project_root: str | Path,
        python_executable: str | Path,
        git_sha: str,
        parent_pid: int | None = None,
        launch_supervisor: Callable[[Path, int], None] | None = None,
        wait: Callable[[float], bool] | None = None,
        stop_timeout_seconds: float = 180.0,
        poll_seconds: float = 0.25,
    ) -> None:
        if stop_timeout_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Profile-switch timeouts must be positive.")
        self.current_profile = current_profile
        self._paper_status = paper_status
        self._flatten_and_disarm = flatten_and_disarm
        self._verifier_status = verifier_status
        self._request_shutdown = request_shutdown
        self.runtime_root = Path(runtime_root).resolve()
        self.project_root = Path(project_root).resolve()
        self.python_executable = Path(python_executable).resolve()
        self.git_sha = git_sha
        self.parent_pid = os.getpid() if parent_pid is None else parent_pid
        self._launch_supervisor = launch_supervisor or self._default_launch_supervisor
        self._wait = wait or time.sleep
        self._stop_timeout_seconds = stop_timeout_seconds
        self._poll_seconds = poll_seconds
        self._token = uuid4().hex + uuid4().hex
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._state: dict[str, object] = {
            "schema": PROFILE_SWITCH_SCHEMA,
            "operation_id": None,
            "request_id": None,
            "stage": "IDLE",
            "in_progress": False,
            "current_profile": current_profile.selection_key,
            "target_profile": None,
            "blockers": [],
            "updated_at": _utc_now(),
        }
        self._state_path: Path | None = None
        inherited = os.getenv("BEELZEBUB_PROFILE_SWITCH_OPERATION")
        if inherited:
            candidate = self.runtime_root / "profile-switch" / "operations" / inherited / "state.json"
            if candidate.is_file():
                try:
                    self._state = _read_json(candidate)
                    self._state_path = candidate
                except (OSError, ValueError, RuntimeError):
                    pass

    @property
    def action_token(self) -> str:
        return self._token

    @property
    def audit_path(self) -> Path:
        return self.runtime_root / "profile-switch" / "profile-switch-audit.jsonl"

    def status(self) -> dict[str, object]:
        with self._lock:
            state = dict(self._state)
        return {
            **state,
            "active_profile": self.current_profile.selection_key,
            "runtime_root": str(self.runtime_root),
            "action_token": self._token,
            "profiles": profile_catalog(),
            "authority": "PAPER_SIM101_PROFILE_SELECTION_ONLY",
            "live_capital": "DENIED",
        }

    def _record(self, event: str, **updates: object) -> None:
        with self._lock:
            self._state.update(updates)
            self._state["updated_at"] = _utc_now()
            self._state["in_progress"] = self._state.get("stage") in _ACTIVE_STAGES
            state = dict(self._state)
            state_path = self._state_path
        if state_path is not None:
            _atomic_json(state_path, state)
        _append_jsonl(self.audit_path, {
            "schema": PROFILE_SWITCH_SCHEMA,
            "event": event,
            "recorded_at": _utc_now(),
            **{key: state.get(key) for key in ("operation_id", "request_id", "stage", "current_profile", "target_profile", "blockers")},
        })

    def _prepare_manifest(self, request_id: str, target: PaperProfileDefinition) -> Path:
        operation_id = f"profile-switch-{uuid4().hex}"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{timestamp}-{uuid4().hex[:12]}"
        run_root = self.runtime_root / "profiles" / target.selection_key.lower() / "runs" / run_id
        operation_root = self.runtime_root / "profile-switch" / "operations" / operation_id
        run_root.mkdir(parents=True, exist_ok=False)
        operation_root.mkdir(parents=True, exist_ok=False)
        ledger_path = run_root / "hot" / "lane_iii_paper.sqlite3"
        audit_root = run_root / "audit"
        ledger_path.parent.mkdir(parents=True, exist_ok=False)
        audit_root.mkdir(parents=True, exist_ok=False)
        epoch = f"L3G-PAPER-EPOCH-{target.selection_key}-{timestamp}-{uuid4().hex[:12]}"
        manifest: dict[str, object] = {
            "schema": PROFILE_SWITCH_SCHEMA,
            "operation_id": operation_id,
            "request_id": request_id,
            "created_at": _utc_now(),
            "parent_pid": self.parent_pid,
            "project_root": str(self.project_root),
            "python_executable": str(self.python_executable),
            "git_sha": self.git_sha,
            "current_profile": self.current_profile.selection_key,
            "target_profile": target.selection_key,
            "paper_policy_hash": target.policy.configuration_hash,
            "risk_profile_hash": target.risk.configuration_hash,
            "ledger_path": str(ledger_path),
            "ledger_epoch": epoch,
            "audit_root": str(audit_root),
            "runtime_root": str(self.runtime_root),
            "paper_only": True,
            "live_capital": "DENIED",
        }
        manifest["manifest_sha256"] = hashlib.sha256(_canonical(manifest)).hexdigest()
        manifest_path = operation_root / "manifest.json"
        descriptor = os.open(manifest_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8") + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._state_path = operation_root / "state.json"
        self._state = {
            "schema": PROFILE_SWITCH_SCHEMA,
            "operation_id": operation_id,
            "request_id": request_id,
            "stage": "PREPARING",
            "in_progress": True,
            "current_profile": self.current_profile.selection_key,
            "target_profile": target.selection_key,
            "manifest_path": str(manifest_path),
            "ledger_path": str(ledger_path),
            "ledger_epoch": epoch,
            "audit_root": str(audit_root),
            "blockers": [],
            "updated_at": _utc_now(),
        }
        _atomic_json(self._state_path, self._state)
        self._record("OPERATION_PREPARED")
        return manifest_path

    def start(self, request_id: str, target_profile: str) -> dict[str, object]:
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid profile-switch request ID.")
        target = resolve_paper_profile(target_profile)
        if target.selection_key == self.current_profile.selection_key:
            raise ValueError("The selected profile is already active.")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            if self._state.get("request_id") == request_id and self._state.get("operation_id"):
                return self.status()
            paper = self._paper_status()
            if (
                paper.get("entry_profile_version") != self.current_profile.policy.entry_profile_version
                or paper.get("live_capital") != "DENIED"
                or paper.get("paper_account") != "Sim101"
                or paper.get("account_class") != "LOCAL_SIMULATION"
                or paper.get("market_instrument") != "MNQ SEP26"
            ):
                raise RuntimeError("PROFILE_SWITCH_CURRENT_RUNTIME_IDENTITY_MISMATCH")
            manifest_path = self._prepare_manifest(request_id, target)
            self._thread = threading.Thread(
                target=self._run,
                args=(manifest_path,),
                name="PaperProfileSwitch",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def _run(self, manifest_path: Path) -> None:
        try:
            self._record("STOP_REQUESTED", stage="STOPPING_CURRENT", blockers=[])
            status = self._paper_status()
            if not exact_flat_shutdown_ready(status):
                self._flatten_and_disarm()
                self._record("AWAITING_FLAT", stage="AWAITING_FLAT")
            deadline = time.monotonic() + self._stop_timeout_seconds
            while time.monotonic() < deadline:
                status = self._paper_status()
                if exact_flat_shutdown_ready(status):
                    break
                self._wait(self._poll_seconds)
            else:
                self._record("SWITCH_BLOCKED", stage="BLOCKED_SAFE", blockers=["CURRENT_PROFILE_DID_NOT_REACH_EXACT_FLAT_SHUTDOWN_BOUNDARY"])
                return
            closing_verifier = self._verifier_status()
            self._launch_supervisor(manifest_path, self.parent_pid)
            self._record(
                "SHUTDOWN_REQUESTED",
                stage="SHUTDOWN_REQUESTED",
                closing_verifier_status=closing_verifier.get("status"),
            )
            self._request_shutdown()
        except Exception as error:
            self._record(
                "SWITCH_FAILED",
                stage="BLOCKED_SAFE",
                blockers=[str(error) if str(error).isupper() else f"PROFILE_SWITCH_{type(error).__name__.upper()}"],
            )

    def _default_launch_supervisor(self, manifest_path: Path, parent_pid: int) -> None:
        command = [
            str(self.python_executable), "-m", "src.l3g_paper.profile_switch",
            "--supervise", str(manifest_path), "--parent-pid", str(parent_pid),
        ]
        options: dict[str, object] = {
            "cwd": str(self.project_root),
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "close_fds": True,
        }
        if os.name == "nt":
            options["creationflags"] = (
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "DETACHED_PROCESS", 0)
                | getattr(subprocess, "CREATE_NO_WINDOW", 0)
            )
        else:  # pragma: no cover - production host is Windows
            options["start_new_session"] = True
        subprocess.Popen(command, **options)  # noqa: S603 - fixed project-local executable and module

    def record_shutdown_receipt(self, receipt: Mapping[str, object]) -> None:
        if self._state_path is None or self._state.get("operation_id") is None:
            return
        clean = (
            receipt.get("clean_shutdown") is True
            and receipt.get("admission_sealed") is True
            and receipt.get("writer_stopped") is True
            and isinstance(receipt.get("checkpoint"), Mapping)
            and receipt["checkpoint"].get("complete") is True  # type: ignore[index]
            and receipt.get("expected_tip_sequence") == receipt.get("durable_tip_sequence")
            and receipt.get("expected_tip_hash") == receipt.get("durable_tip_hash")
            and isinstance(receipt.get("verifier_shutdown"), Mapping)
            and receipt["verifier_shutdown"].get("completed") is True  # type: ignore[index]
            and isinstance(receipt.get("runtime_watchdog_shutdown"), Mapping)
            and receipt["runtime_watchdog_shutdown"].get("completed") is True  # type: ignore[index]
        )
        self._record(
            "CURRENT_PROFILE_SHUTDOWN_RECORDED",
            stage="CURRENT_PROFILE_CLOSED" if clean else "BLOCKED_SAFE",
            blockers=[] if clean else ["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"],
            shutdown_receipt=dict(receipt),
        )


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _manifest(path: Path) -> dict[str, object]:
    value = _read_json(path)
    supplied = value.pop("manifest_sha256", None)
    actual = hashlib.sha256(_canonical(value)).hexdigest()
    if supplied != actual or value.get("schema") != PROFILE_SWITCH_SCHEMA:
        raise RuntimeError("PROFILE_SWITCH_MANIFEST_INTEGRITY_FAILED")
    target = resolve_paper_profile(str(value.get("target_profile") or ""))
    if (
        value.get("paper_policy_hash") != target.policy.configuration_hash
        or value.get("risk_profile_hash") != target.risk.configuration_hash
        or value.get("paper_only") is not True
        or value.get("live_capital") != "DENIED"
    ):
        raise RuntimeError("PROFILE_SWITCH_MANIFEST_PROFILE_MISMATCH")
    value["manifest_sha256"] = supplied
    return value


def _state_update(path: Path, event: str, **updates: object) -> dict[str, object]:
    state = _read_json(path)
    state.update(updates)
    state["updated_at"] = _utc_now()
    state["in_progress"] = state.get("stage") in _ACTIVE_STAGES
    _atomic_json(path, state)
    operation_root = path.parent
    _append_jsonl(operation_root / "supervisor-audit.jsonl", {
        "schema": PROFILE_SWITCH_SCHEMA,
        "event": event,
        "recorded_at": _utc_now(),
        "operation_id": state.get("operation_id"),
        "stage": state.get("stage"),
        "blockers": state.get("blockers"),
    })
    return state


def _http_json(url: str, *, method: str = "GET", headers: Mapping[str, str] | None = None, body: Mapping[str, object] | None = None, timeout: float = 15.0) -> dict[str, object]:
    encoded = None if body is None else _canonical(body)
    request = Request(url, data=encoded, method=method, headers={"Content-Type": "application/json", **dict(headers or {})})
    with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed loopback endpoint
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("PROFILE_SWITCH_HTTP_RESPONSE_INVALID")
    return value


def supervise(manifest_path: Path, parent_pid: int, *, timeout_seconds: float = 600.0) -> int:
    manifest = _manifest(manifest_path)
    state_path = manifest_path.with_name("state.json")
    deadline = time.monotonic() + timeout_seconds
    while _pid_exists(parent_pid) and time.monotonic() < deadline:
        time.sleep(0.25)
    if _pid_exists(parent_pid):
        _state_update(state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE", blockers=["CURRENT_PROCESS_DID_NOT_EXIT"])
        return 2
    state = _read_json(state_path)
    receipt = state.get("shutdown_receipt")
    if state.get("stage") != "CURRENT_PROFILE_CLOSED" or not isinstance(receipt, Mapping):
        _state_update(state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE", blockers=["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"])
        return 3
    ledger_path = Path(str(manifest["ledger_path"])).resolve()
    audit_root = Path(str(manifest["audit_root"])).resolve()
    project_root = Path(str(manifest["project_root"])).resolve()
    python = Path(str(manifest["python_executable"])).resolve()
    runtime_root = Path(str(manifest["runtime_root"])).resolve()
    paths_are_scoped = (
        runtime_root in ledger_path.parents
        and runtime_root in audit_root.parents
        and runtime_root in manifest_path.resolve().parents
    )
    try:
        checkout_sha = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        checkout_sha = "UNRESOLVED"
    if (
        not paths_are_scoped
        or ledger_path.exists()
        or not python.is_file()
        or not (project_root / "main.py").is_file()
        or checkout_sha != manifest.get("git_sha")
    ):
        _state_update(state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE", blockers=["TARGET_RUNTIME_PATH_VALIDATION_FAILED"])
        return 4
    _state_update(state_path, "TARGET_STARTING", stage="STARTING_TARGET", blockers=[])
    environment = dict(os.environ)
    environment.update({
        "BEELZEBUB_L3G_PAPER_LEDGER": str(ledger_path),
        "BEELZEBUB_LEDGER_AUDIT_ROOT": str(audit_root),
        "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": str(manifest["ledger_epoch"]),
        "BEELZEBUB_L3G_PAPER_PROFILE": str(manifest["target_profile"]),
        "BEELZEBUB_PROFILE_SWITCH_ROOT": str(runtime_root),
        "BEELZEBUB_PROFILE_SWITCH_OPERATION": str(manifest["operation_id"]),
        "BEELZEBUB_GIT_SHA": str(manifest["git_sha"]),
    })
    backend_log = manifest_path.with_name("target-backend.log")
    with backend_log.open("ab") as stream:
        process = subprocess.Popen(
            [str(python), str(project_root / "main.py"), "copy-control-center", "--with-watcher"],
            cwd=project_root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            close_fds=True,
            creationflags=(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0,
            start_new_session=os.name != "nt",
        )
    base = "http://127.0.0.1:8090"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            _state_update(state_path, "TARGET_FAILED", stage="BLOCKED_SAFE", blockers=["TARGET_PROCESS_EXITED_DURING_STARTUP"])
            return 5
        try:
            binding = _http_json(base + "/api/runtime-binding")
            if (
                binding.get("ledger") == str(ledger_path)
                and binding.get("audit") == str(audit_root)
                and binding.get("git_sha") == manifest.get("git_sha")
                and binding.get("entry_profile_version") == manifest.get("target_profile")
                and binding.get("paper_policy_hash") == manifest.get("paper_policy_hash")
                and binding.get("risk_profile_hash") == manifest.get("risk_profile_hash")
            ):
                break
        except (OSError, ValueError, RuntimeError, HTTPError, URLError):
            pass
        time.sleep(0.5)
    else:
        _state_update(state_path, "TARGET_BLOCKED", stage="BLOCKED_SAFE", blockers=["TARGET_RUNTIME_BINDING_TIMEOUT"])
        return 6
    _state_update(state_path, "TARGET_AUTOSTARTING", stage="AUTOSTARTING_TARGET")
    try:
        auto = _http_json(base + "/api/lane-iii/paper/auto-start")
        token = auto.get("action_token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("TARGET_AUTOSTART_TOKEN_UNAVAILABLE")
        _http_json(
            base + "/api/lane-iii/paper/auto-start",
            method="POST",
            headers={
                "X-Beelzebub-Paper-Autostart-Action": "sim101-paper-autostart-v1",
                "X-Beelzebub-Paper-Autostart-Token": token,
            },
            body={"request_id": f"profile-switch-{manifest['operation_id']}"},
        )
        while time.monotonic() < deadline:
            auto = _http_json(base + "/api/lane-iii/paper/auto-start")
            if auto.get("stage") == "RUNNING":
                _state_update(state_path, "PROFILE_SWITCH_COMPLETED", stage="RUNNING", blockers=[], target_runtime_binding=binding, target_autostart=auto)
                return 0
            if auto.get("stage") in {"BLOCKED", "FAILED", "CANCELLED"}:
                blockers = auto.get("blockers")
                values = [str(value) for value in blockers] if isinstance(blockers, list) else ["TARGET_AUTOSTART_BLOCKED"]
                _state_update(state_path, "TARGET_BLOCKED", stage="BLOCKED_SAFE", blockers=values, target_runtime_binding=binding, target_autostart=auto)
                return 7
            time.sleep(0.5)
    except (OSError, ValueError, RuntimeError, HTTPError, URLError) as error:
        _state_update(state_path, "TARGET_BLOCKED", stage="BLOCKED_SAFE", blockers=[str(error) if str(error).isupper() else "TARGET_AUTOSTART_FAILED"])
        return 8
    _state_update(state_path, "TARGET_BLOCKED", stage="BLOCKED_SAFE", blockers=["TARGET_AUTOSTART_TIMEOUT"])
    return 9


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Supervise one prepared Beelzebub paper profile handoff.")
    parser.add_argument("--supervise", type=Path, required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    options = parser.parse_args(argv)
    return supervise(options.supervise.resolve(), options.parent_pid)


if __name__ == "__main__":
    raise SystemExit(main())
