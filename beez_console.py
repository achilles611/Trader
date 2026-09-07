"""Start the local paper-only copy-trading control center in Brave.

This stays deliberately thin: it locates the project-local virtual
environment, starts the authoritative ``copy-control-center`` command when
needed, and opens the existing local UI.  It never selects a global Python or
changes any trading/operator state.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import BinaryIO, Callable, Iterator, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.l3g_paper.contracts import resolve_paper_profile
from src.l3g_paper.profile_switch import (
    PROFILE_SWITCH_SCHEMA,
    _manifest as validated_profile_switch_manifest,
    _validated_selection as validated_profile_selection,
    remembered_profile_selection,
)


HOST = "127.0.0.1"
PORT = 8090
URL = f"http://{HOST}:{PORT}"
# Startup is intentionally lightweight: runtime accessibility checks and
# NinjaTrader authentication may take time, but historical ledger verification
# is a detached, explicit local operation.  Keep this above the separate
# 90-second NinjaTrader authentication budget.
STARTUP_TIMEOUT_SECONDS = 120.0
DEFAULT_PROFILE_SWITCH_ROOT = r"N:\Beelzebub\runtime"
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_LEDGER_IDENTITY = re.compile(r"^l3g-ledger-[0-9a-f]{32}$")
_PROFILE_SWITCH_OPERATION = re.compile(r"^profile-switch-[0-9a-f]{32}$")
_SOURCE_BEFORE_OPERATION = re.compile(r"^source-before-(profile-switch-[0-9a-f]{32})$")
_TERMINAL_HANDOFF_STAGES = frozenset({"RUNNING", "BLOCKED_SAFE"})
_ADOPTABLE_TARGET_STAGES = frozenset({
    "STARTING_TARGET", "TARGET_PROCESS_CREATED", "AUTOSTARTING_TARGET",
    "TARGET_ACTIVE_FLAT_BLOCKED", "RUNNING_SELECTION_PERSISTENCE_FAILED",
    "BLOCKED_SAFE",
})
_PAPER_AUTHORITY = {
    "mode": "PAPER_SIM101",
    "paper_account": "Sim101",
    "account_class": "LOCAL_SIMULATION",
    "market_instrument": "MNQ SEP26",
    "maximum_quantity": 1,
    "live_capital": "DENIED",
}


@dataclass(frozen=True)
class LaunchBinding:
    """Exact paper runtime identity accepted by this launcher invocation."""

    runtime_root: Path
    ledger_path: Path
    audit_root: Path
    ledger_epoch: str
    profile: str
    git_sha: str
    python: Path
    entry_profile: str
    entry_profile_version: str
    paper_policy_hash: str
    risk_profile_hash: str
    source: str
    ledger_identity: str | None = None
    operation_id: str | None = None
    expected_pid: int | None = None
    expected_parent_pid: int | None = None
    expected_launcher_pid: int | None = None

    def expected_runtime_binding(self) -> dict[str, object]:
        expected: dict[str, object] = {
            "ledger": str(self.ledger_path),
            "audit": str(self.audit_root),
            "control_center": f"{HOST}:{PORT}",
            "python": str(self.python),
            "git_sha": self.git_sha,
            "entry_profile": self.entry_profile,
            "entry_profile_version": self.entry_profile_version,
            "paper_policy_hash": self.paper_policy_hash,
            "risk_profile_hash": self.risk_profile_hash,
            "ledger_epoch": self.ledger_epoch,
            "profile_selection_source": (
                "REMEMBERED_ESTABLISHED_RUN"
                if self.source == "REMEMBERED_ESTABLISHED_RUN"
                else "EXPLICIT_OR_DEFAULT"
            ),
        }
        if self.ledger_identity is not None:
            expected["ledger_identity"] = self.ledger_identity
        if self.expected_pid is not None:
            expected["pid"] = self.expected_pid
        if self.expected_parent_pid is not None:
            expected["parent_pid"] = self.expected_parent_pid
        return expected


def project_root(*, frozen: bool | None = None, executable: str | None = None, source_file: str | None = None) -> Path:
    """Return the executable directory once packaged, otherwise this source directory."""
    packaged = getattr(sys, "frozen", False) if frozen is None else frozen
    if packaged:
        # Keep a packaged Windows executable path lexical when this behavior is
        # tested from a non-Windows CI runner; resolving it there incorrectly
        # prefixes the Linux workspace, and POSIX ``Path`` does not parse its
        # backslashes as separators. Native Windows paths need no resolve.
        executable_path = executable or sys.executable
        if "\\" in executable_path:
            return Path(executable_path.rsplit("\\", 1)[0].rstrip("\\"))
        return Path(executable_path).parent
    return Path(source_file or __file__).resolve().parent


def validate_project_root(root: Path) -> tuple[Path, Path]:
    """Require the executable to live beside the project-local runtime."""
    # requirements.lock is generated and the Lane III runtime is tested with
    # Python 3.12. The legacy .venv in this checkout is Python 3.10 and cannot
    # import stdlib StrEnum, so it is not a valid Beelzebub runtime.
    python = root / ".venv312" / "Scripts" / "python.exe"
    entrypoint = root / "main.py"
    if not entrypoint.is_file() or not python.is_file():
        raise RuntimeError(
            "BeezConsole must be located in the Trader project root beside main.py and .venv312\\Scripts\\python.exe."
        )
    return python, entrypoint


def checkout_git_sha(root: Path) -> str:
    """Resolve the checkout identity locally instead of trusting inherited state."""
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("BeezConsole could not resolve the Trader checkout Git identity.") from exc
    value = result.stdout.strip()
    if result.returncode != 0 or _GIT_SHA.fullmatch(value) is None:
        raise RuntimeError("BeezConsole could not resolve an exact Trader checkout Git identity.")
    return value


def profile_switch_root(environment: Mapping[str, str] | None = None) -> Path:
    env = os.environ if environment is None else environment
    configured = env.get("BEELZEBUB_PROFILE_SWITCH_ROOT")
    return Path(configured or DEFAULT_PROFILE_SWITCH_ROOT).expanduser().resolve()


def _maintenance_arguments(argv: Sequence[str]) -> dict[str, str] | None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--maintenance-ledger-path")
    parser.add_argument("--maintenance-audit-root")
    parser.add_argument("--maintenance-ledger-epoch")
    parser.add_argument("--maintenance-paper-profile")
    try:
        options = parser.parse_args(list(argv))
    except SystemExit as exc:
        raise RuntimeError("BeezConsole received invalid maintenance launch arguments.") from exc
    values = {
        "ledger_path": options.maintenance_ledger_path,
        "audit_root": options.maintenance_audit_root,
        "ledger_epoch": options.maintenance_ledger_epoch,
        "profile": options.maintenance_paper_profile,
    }
    supplied = [isinstance(value, str) and bool(value.strip()) for value in values.values()]
    if any(supplied) and not all(supplied):
        raise RuntimeError(
            "Explicit maintenance launch requires ledger path, audit root, ledger epoch, and paper profile together."
        )
    return {key: str(value).strip() for key, value in values.items()} if all(supplied) else None


def _remembered_selection_without_ledger(runtime_root: Path, *, git_sha: str) -> dict[str, object] | None:
    """Resolve the durable selector without opening a possibly live SQLite file."""
    try:
        selection = validated_profile_selection(runtime_root, required=True)
    except RuntimeError as exc:
        if str(exc) == "PROFILE_SELECTION_STATE_MISSING":
            return None
        raise
    assert selection is not None
    established = selection.get("established")
    if not isinstance(established, Mapping):
        return None
    remembered = dict(established)
    if remembered.get("git_sha") != git_sha:
        raise RuntimeError("PROFILE_SELECTION_CHECKOUT_MISMATCH")
    ledger_path = Path(str(remembered["ledger_path"])).resolve()
    audit_root = Path(str(remembered["audit_root"])).resolve()
    if not ledger_path.is_file() or not audit_root.is_dir():
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_EVIDENCE_MISSING")
    return remembered


def resolve_launch_binding(
    root: Path,
    *,
    argv: Sequence[str] = (),
    environment: Mapping[str, str] | None = None,
    validate_remembered_evidence: bool = True,
) -> LaunchBinding:
    """Resolve an explicit maintenance binding or the proven remembered run."""
    python, _ = validate_project_root(root)
    git_sha = checkout_git_sha(root)
    runtime_root = profile_switch_root(environment)
    maintenance = _maintenance_arguments(argv)
    if maintenance is None:
        try:
            selected = _remembered_selection_without_ledger(runtime_root, git_sha=git_sha)
            remembered = (
                remembered_profile_selection(runtime_root, git_sha=git_sha)
                if validate_remembered_evidence and selected is not None
                else selected
            )
        except RuntimeError as exc:
            raise RuntimeError(f"BeezConsole remembered profile validation failed: {exc}") from exc
        if remembered is None:
            raise RuntimeError(
                "BeezConsole has no validated established paper profile selection. "
                "Use the complete maintenance-only start command to establish an initial run."
            )
        if selected != remembered:
            raise RuntimeError(
                "BeezConsole remembered profile selection changed during validation; retry after it is terminal."
            )
        profile_key = str(remembered["profile"])
        ledger_path = Path(str(remembered["ledger_path"])).resolve()
        audit_root = Path(str(remembered["audit_root"])).resolve()
        ledger_epoch = str(remembered["ledger_epoch"])
        ledger_identity = str(remembered["ledger_identity"])
        operation_id = str(remembered["operation_id"])
        source = "REMEMBERED_ESTABLISHED_RUN"
    else:
        profile_key = maintenance["profile"]
        raw_ledger_path = Path(maintenance["ledger_path"]).expanduser()
        raw_audit_root = Path(maintenance["audit_root"]).expanduser()
        if not raw_ledger_path.is_absolute() or not raw_audit_root.is_absolute():
            raise RuntimeError("Explicit maintenance ledger and audit paths must be absolute.")
        ledger_path = raw_ledger_path.resolve()
        audit_root = raw_audit_root.resolve()
        ledger_epoch = maintenance["ledger_epoch"]
        if not ledger_epoch.startswith("L3G-PAPER-EPOCH-"):
            raise RuntimeError("Explicit maintenance ledger epoch must begin with L3G-PAPER-EPOCH-.")
        ledger_identity = None
        operation_id = None
        source = "EXPLICIT_MAINTENANCE"
    try:
        profile = resolve_paper_profile(profile_key)
    except ValueError as exc:
        raise RuntimeError(f"BeezConsole paper profile is invalid: {profile_key}") from exc
    return LaunchBinding(
        runtime_root=runtime_root,
        ledger_path=ledger_path,
        audit_root=audit_root,
        ledger_epoch=ledger_epoch,
        profile=profile.selection_key,
        git_sha=git_sha,
        python=python.resolve(),
        entry_profile=profile.policy.entry_profile,
        entry_profile_version=profile.policy.entry_profile_version,
        paper_policy_hash=profile.policy.configuration_hash,
        risk_profile_hash=profile.risk.configuration_hash,
        source=source,
        ledger_identity=ledger_identity,
        operation_id=operation_id,
    )


def _validated_operation(
    binding: LaunchBinding,
    operation_id: str,
    *,
    require_current_git_sha: bool = True,
) -> tuple[dict[str, object], dict[str, object]]:
    operation_root = binding.runtime_root / "profile-switch" / "operations" / operation_id
    manifest_path = operation_root / "manifest.json"
    state_path = operation_root / "state.json"
    try:
        manifest = validated_profile_switch_manifest(manifest_path)
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("BeezConsole cannot validate the durable profile handoff projection.") from exc
    project = binding.python.parents[2]
    manifest_git_sha = manifest.get("git_sha")
    if (
        not isinstance(state, dict)
        or manifest.get("operation_id") != operation_id
        or not isinstance(manifest_git_sha, str)
        or _GIT_SHA.fullmatch(manifest_git_sha) is None
        or (require_current_git_sha and manifest_git_sha != binding.git_sha)
        or Path(str(manifest.get("runtime_root"))).resolve() != binding.runtime_root
        or Path(str(manifest.get("project_root"))).resolve() != project
        or Path(str(manifest.get("python_executable"))).resolve() != binding.python
        or state.get("schema") != PROFILE_SWITCH_SCHEMA
        or state.get("operation_id") != operation_id
        or state.get("current_profile") != manifest.get("current_profile")
        or state.get("target_profile") != manifest.get("target_profile")
        or Path(str(state.get("manifest_path"))).resolve() != manifest_path.resolve()
    ):
        raise RuntimeError("BeezConsole cannot validate the durable profile handoff projection.")
    return manifest, state


def assert_remembered_launch_is_not_competing(binding: LaunchBinding) -> None:
    """Re-read the selector and refuse any launch owned by an active handoff."""
    try:
        selection = validated_profile_selection(binding.runtime_root)
    except RuntimeError as exc:
        raise RuntimeError("BeezConsole cannot validate the durable profile selection.") from exc
    if selection is None:
        if binding.source == "REMEMBERED_ESTABLISHED_RUN":
            raise RuntimeError("BeezConsole cannot validate the durable profile selection.")
        return

    established = selection.get("established")
    if binding.source == "REMEMBERED_ESTABLISHED_RUN":
        if not isinstance(established, Mapping) or binding.operation_id is None:
            raise RuntimeError("BeezConsole cannot validate the established profile projection.")
        expected_established = {
            "profile": binding.profile,
            "operation_id": binding.operation_id,
            "ledger_path": str(binding.ledger_path),
            "ledger_identity": binding.ledger_identity,
            "ledger_epoch": binding.ledger_epoch,
            "audit_root": str(binding.audit_root),
            "git_sha": binding.git_sha,
        }
        if any(established.get(key) != value for key, value in expected_established.items()):
            raise RuntimeError("BeezConsole established profile projection changed before launch.")
        source_match = _SOURCE_BEFORE_OPERATION.fullmatch(binding.operation_id)
        established_operation = source_match.group(1) if source_match is not None else binding.operation_id
        manifest, established_state = _validated_operation(binding, established_operation)
        if source_match is None:
            if (
                _PROFILE_SWITCH_OPERATION.fullmatch(binding.operation_id) is None
                or manifest.get("target_profile") != binding.profile
                or Path(str(manifest.get("ledger_path"))).resolve() != binding.ledger_path
                or Path(str(manifest.get("audit_root"))).resolve() != binding.audit_root
                or manifest.get("ledger_epoch") != binding.ledger_epoch
                or established_state.get("stage") != "RUNNING"
            ):
                raise RuntimeError("BeezConsole cannot validate the established profile operation.")
        elif manifest.get("current_profile") != binding.profile:
            raise RuntimeError("BeezConsole cannot validate the source profile operation.")

    requested = selection.get("requested")
    if requested is None:
        if binding.source == "REMEMBERED_ESTABLISHED_RUN" and _SOURCE_BEFORE_OPERATION.fullmatch(
            str(binding.operation_id)
        ):
            raise RuntimeError("BeezConsole cannot prove the source profile handoff is terminal.")
        return
    if not isinstance(requested, Mapping):
        raise RuntimeError("BeezConsole cannot validate the requested profile handoff.")
    requested_operation = str(requested.get("operation_id") or "")
    manifest, state = _validated_operation(
        binding,
        requested_operation,
        # An explicit maintenance start is the fail-closed upgrade path after
        # a checkout change. Its only allowance is to inspect a self-consistent
        # terminal handoff from the prior commit. The stage check below still
        # refuses every active operation, while remembered launches continue
        # to require the current commit through the default above.
        require_current_git_sha=binding.source != "EXPLICIT_MAINTENANCE",
    )
    if (
        manifest.get("request_id") != requested.get("request_id")
        or manifest.get("target_profile") != requested.get("profile")
        or manifest.get("created_at") != requested.get("requested_at")
    ):
        raise RuntimeError("BeezConsole cannot validate the requested profile handoff.")
    stage = state.get("stage")
    if stage not in _TERMINAL_HANDOFF_STAGES:
        raise RuntimeError(
            f"BeezConsole will not compete with profile handoff {requested_operation} at stage {stage}."
        )
    if binding.source == "REMEMBERED_ESTABLISHED_RUN":
        source_match = _SOURCE_BEFORE_OPERATION.fullmatch(str(binding.operation_id))
        expected_requested_operation = source_match.group(1) if source_match is not None else binding.operation_id
        if stage == "RUNNING" and requested_operation != expected_requested_operation:
            raise RuntimeError("BeezConsole requested and established profile operations do not agree.")
        if stage == "RUNNING" and source_match is not None:
            raise RuntimeError("BeezConsole source selection is stale after the target reached RUNNING.")


def adopt_exact_requested_target_listener(
    root: Path,
    established_binding: LaunchBinding,
    actual: Mapping[str, object],
) -> LaunchBinding:
    """Adopt, but never launch, an exact target whose switch projection lags."""
    selection = validated_profile_selection(established_binding.runtime_root, required=True)
    if not isinstance(selection, Mapping):
        raise RuntimeError("BeezConsole cannot validate the requested target selection.")
    requested = selection.get("requested")
    if not isinstance(requested, Mapping):
        raise RuntimeError("BeezConsole has no requested target listener to adopt.")
    operation_id = str(requested.get("operation_id") or "")
    manifest, state = _validated_operation(established_binding, operation_id)
    stage = state.get("stage")
    if stage not in _ADOPTABLE_TARGET_STAGES:
        raise RuntimeError(
            f"BeezConsole cannot adopt requested target {operation_id} at stage {stage}."
        )
    if (
        manifest.get("request_id") != requested.get("request_id")
        or manifest.get("target_profile") != requested.get("profile")
        or manifest.get("created_at") != requested.get("requested_at")
    ):
        raise RuntimeError("BeezConsole requested target identity is inconsistent.")
    target_pid = state.get("target_pid")
    target_runtime_pid = state.get("target_runtime_pid")
    if (
        type(target_pid) is not int
        or target_pid <= 0
        or type(target_runtime_pid) is not int
        or target_runtime_pid <= 0
        or actual.get("pid") != target_runtime_pid
        or not _runtime_binding_matches_launcher(actual, target_pid)
    ):
        raise RuntimeError("BeezConsole requested target process identity is inconsistent.")
    target_runtime_binding = state.get("target_runtime_binding")
    if (
        not isinstance(target_runtime_binding, Mapping)
        or dict(target_runtime_binding) != dict(actual)
    ):
        raise RuntimeError("BeezConsole requested target runtime binding is inconsistent.")
    target_parent_pid = actual.get("parent_pid")
    if type(target_parent_pid) is not int or target_parent_pid <= 0:
        raise RuntimeError("BeezConsole requested target parent process identity is inconsistent.")
    profile = resolve_paper_profile(str(manifest["target_profile"]))
    ledger_identity = actual.get("ledger_identity")
    if _LEDGER_IDENTITY.fullmatch(str(ledger_identity or "")) is None:
        raise RuntimeError("BeezConsole requested target ledger identity is unavailable.")
    target = LaunchBinding(
        runtime_root=Path(str(manifest["runtime_root"])).resolve(),
        ledger_path=Path(str(manifest["ledger_path"])).resolve(),
        audit_root=Path(str(manifest["audit_root"])).resolve(),
        ledger_epoch=str(manifest["ledger_epoch"]),
        profile=profile.selection_key,
        git_sha=str(manifest["git_sha"]),
        python=Path(str(manifest["python_executable"])).resolve(),
        entry_profile=profile.policy.entry_profile,
        entry_profile_version=profile.policy.entry_profile_version,
        paper_policy_hash=profile.policy.configuration_hash,
        risk_profile_hash=profile.risk.configuration_hash,
        source="REQUESTED_TARGET_EXISTING_LISTENER",
        ledger_identity=str(ledger_identity),
        operation_id=operation_id,
        expected_pid=target_runtime_pid,
        expected_parent_pid=target_parent_pid,
        expected_launcher_pid=target_pid,
    )
    if target.python != validate_project_root(root)[0].resolve():
        raise RuntimeError("BeezConsole requested target Python binding is inconsistent.")
    validate_runtime_binding(actual, target)
    paper = fetch_paper_status()
    if paper is None:
        raise RuntimeError("BeezConsole requested target paper authority is unavailable.")
    validate_paper_authority(paper)
    return target


def binding_for_existing_listener(
    root: Path, binding: LaunchBinding,
) -> LaunchBinding:
    """Use the established listener or narrowly adopt its exact requested target."""
    selection_error: RuntimeError | None = None
    try:
        assert_remembered_launch_is_not_competing(binding)
    except RuntimeError as error:
        selection_error = error
    actual = fetch_runtime_binding()
    if actual is not None:
        try:
            validate_runtime_binding(actual, binding)
        except RuntimeError:
            # A terminal handoff can leave the durable established selector on
            # the source even though the one existing listener is already the
            # exact manifest-bound target. Adopt only after the full requested
            # manifest, runtime binding, and Sim101 authority checks below.
            return adopt_exact_requested_target_listener(root, binding, actual)
        actual_pid = actual.get("pid")
        actual_parent_pid = actual.get("parent_pid")
        if (
            type(actual_pid) is not int
            or actual_pid <= 0
            or type(actual_parent_pid) is not int
            or actual_parent_pid <= 0
        ):
            raise RuntimeError("BeezConsole existing listener process identity is unavailable.")
        binding = replace(
            binding,
            expected_pid=actual_pid,
            expected_parent_pid=actual_parent_pid,
        )
    if selection_error is not None:
        raise selection_error
    return binding


@contextmanager
def direct_launch_reservation(runtime_root: Path) -> Iterator[None]:
    """Serialize direct launchers without using a durable stale-owner marker."""
    lock_path = runtime_root / "profile-switch" / "beezconsole-direct-launch.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        _lock_file(handle)
        try:
            yield
        finally:
            _unlock_file(handle)


def _lock_file(handle: BinaryIO) -> None:
    handle.seek(0)
    if handle.read(1) == b"":
        handle.write(b"\0")
        handle.flush()
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover - production host is Windows
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise RuntimeError("Another BeezConsole launcher already owns the direct-start reservation.") from exc


def _unlock_file(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:  # pragma: no cover - production host is Windows
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def server_environment(
    binding: LaunchBinding,
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Create a clean child environment bound to this launch decision."""
    child = dict(os.environ if environment is None else environment)
    for key in tuple(child):
        normalized = key.upper()
        if normalized.startswith("BEELZEBUB_") or normalized.startswith("COPYTRADE_"):
            child.pop(key, None)
    child["BEELZEBUB_PROFILE_SWITCH_ROOT"] = str(binding.runtime_root)
    child["BEELZEBUB_GIT_SHA"] = binding.git_sha
    if binding.source == "EXPLICIT_MAINTENANCE":
        child.update({
            "BEELZEBUB_L3G_PAPER_LEDGER": str(binding.ledger_path),
            "BEELZEBUB_LEDGER_AUDIT_ROOT": str(binding.audit_root),
            "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": binding.ledger_epoch,
            "BEELZEBUB_L3G_PAPER_PROFILE": binding.profile,
        })
    return child


def port_is_open(host: str = HOST, port: int = PORT, *, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def fetch_runtime_binding(url: str = URL, *, timeout: float = 0.75) -> dict[str, object] | None:
    """Read the typed backend identity; arbitrary HTTP success is not readiness."""
    return _fetch_json(f"{url}/api/runtime-binding", timeout=timeout)


def fetch_paper_status(url: str = URL, *, timeout: float = 0.75) -> dict[str, object] | None:
    """Read the exact Lane III paper authority exposed by the adopted backend."""
    return _fetch_json(f"{url}/api/lane-iii/paper", timeout=timeout)


def _fetch_json(url: str, *, timeout: float) -> dict[str, object] | None:
    try:
        request = Request(url, headers={"User-Agent": "BeezConsole"})
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed localhost URL
            if int(response.status) != 200:
                return None
            content_type = response.headers.get_content_type()
            if content_type != "application/json":
                return None
            value = json.loads(response.read().decode("utf-8"))
            return value if isinstance(value, dict) else None
    except (HTTPError, URLError, OSError, UnicodeError, ValueError):
        return None


def _paths_match(left: object, right: str) -> bool:
    if not isinstance(left, str) or not left:
        return False
    try:
        return Path(left).resolve() == Path(right).resolve()
    except OSError:
        return False


def validate_runtime_binding(actual: Mapping[str, object], expected: LaunchBinding) -> None:
    """Require the existing/warmed service to be the exact intended paper runtime."""
    required = expected.expected_runtime_binding()
    mismatches: list[str] = []
    for key, value in required.items():
        if key == "profile_selection_source":
            continue
        matches = (
            _paths_match(actual.get(key), value)
            if key in {"ledger", "audit", "python"}
            else actual.get(key) == value
        )
        if not matches:
            mismatches.append(key)
    if expected.source == "REMEMBERED_ESTABLISHED_RUN":
        allowed_sources = {"REMEMBERED_ESTABLISHED_RUN", "PROFILE_SWITCH_MANIFEST"}
        if _SOURCE_BEFORE_OPERATION.fullmatch(str(expected.operation_id)) is not None:
            allowed_sources.add("EXPLICIT_OR_DEFAULT")
    elif expected.source == "REQUESTED_TARGET_EXISTING_LISTENER":
        allowed_sources = {"PROFILE_SWITCH_MANIFEST"}
    else:
        allowed_sources = {"EXPLICIT_OR_DEFAULT"}
    if actual.get("profile_selection_source") not in allowed_sources:
        mismatches.append("profile_selection_source")
    if (
        (type(actual.get("pid")) is not int or int(actual["pid"]) <= 0)
        and "pid" not in mismatches
    ):
        mismatches.append("pid")
    if (
        (type(actual.get("parent_pid")) is not int or int(actual["parent_pid"]) <= 0)
        and "parent_pid" not in mismatches
    ):
        mismatches.append("parent_pid")
    if (
        expected.expected_launcher_pid is not None
        and not _runtime_binding_matches_launcher(actual, expected.expected_launcher_pid)
    ):
        mismatches.append("launcher_pid")
    if _LEDGER_IDENTITY.fullmatch(str(actual.get("ledger_identity") or "")) is None:
        if "ledger_identity" not in mismatches:
            mismatches.append("ledger_identity")
    if mismatches:
        raise RuntimeError(
            "The service on 127.0.0.1:8090 does not match the validated BeezConsole runtime binding "
            f"({', '.join(mismatches)}). Refusing to use or replace it."
        )


def _runtime_binding_matches_launcher(
    actual: Mapping[str, object], launcher_pid: int,
) -> bool:
    """Accept a direct interpreter or the Windows py.exe shim it spawned."""
    runtime_pid = actual.get("pid")
    parent_pid = actual.get("parent_pid")
    return (
        type(launcher_pid) is int
        and launcher_pid > 0
        and type(runtime_pid) is int
        and runtime_pid > 0
        and type(parent_pid) is int
        and parent_pid > 0
        and (runtime_pid == launcher_pid or parent_pid == launcher_pid)
    )


def validate_paper_authority(actual: Mapping[str, object]) -> None:
    """Refuse a backend that is not the sealed one-contract Sim101 runtime."""
    mismatches: list[str] = []
    for key, value in _PAPER_AUTHORITY.items():
        actual_value = actual.get(key)
        if key == "maximum_quantity":
            matches = type(actual_value) is int and actual_value == value
        else:
            matches = actual_value == value
        if not matches:
            mismatches.append(key)
    if mismatches:
        raise RuntimeError(
            "The service on 127.0.0.1:8090 is not the exact Sim101 / LOCAL_SIMULATION / "
            f"MNQ SEP26 / quantity 1 / live-denied paper authority ({', '.join(mismatches)})."
        )


def brave_candidates(
    environment: Mapping[str, str] | None = None, finder: Callable[[str], str | None] = shutil.which,
) -> tuple[Path, ...]:
    """Return Brave locations in the documented order, without choosing another browser."""
    env = os.environ if environment is None else environment
    program_files = env.get("PROGRAMFILES", r"C:\\Program Files")
    program_files_x86 = env.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")
    local_app_data = env.get("LOCALAPPDATA", "")
    candidates = (
        env.get("BRAVE_PATH"),
        finder("brave.exe"),
        finder("brave"),
        os.path.join(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(program_files_x86, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(local_app_data, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
    )
    return tuple(Path(candidate) for candidate in candidates if candidate)


def brave_path(
    *, environment: Mapping[str, str] | None = None, finder: Callable[[str], str | None] = shutil.which,
    exists: Callable[[Path], bool] | None = None,
) -> Path | None:
    exists = exists or Path.is_file
    for candidate in brave_candidates(environment, finder):
        if exists(candidate):
            return candidate
    return None


def _creation_flags() -> int:
    return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)


def start_server(root: Path, binding: LaunchBinding) -> subprocess.Popen[bytes]:
    """Start the application with only the resolved binding in its environment."""
    # This second durable read is intentionally adjacent to process creation;
    # a profile request may have been recorded after the initial UI launch.
    assert_remembered_launch_is_not_competing(binding)
    python, entrypoint = validate_project_root(root)
    log_path = root / "logs" / "beez-console-server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        return subprocess.Popen(
            [str(python), str(entrypoint), "copy-control-center", "--with-watcher"],
            cwd=root,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=_creation_flags(),
            env=server_environment(binding),
        )


def wait_for_server(
    process: subprocess.Popen[bytes] | None,
    binding: LaunchBinding,
    *,
    timeout_seconds: float = STARTUP_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    pinned_pid = binding.expected_pid
    pinned_parent_pid = binding.expected_parent_pid
    pinned_launcher_pid = binding.expected_launcher_pid
    if process is not None:
        if pinned_launcher_pid is not None and pinned_launcher_pid != process.pid:
            raise RuntimeError("BeezConsole launch process identity conflicts with its runtime binding.")
        pinned_launcher_pid = process.pid
    pinned_runtime_binding: dict[str, object] | None = None

    def pin_or_validate_runtime_binding(actual: Mapping[str, object]) -> None:
        nonlocal pinned_parent_pid, pinned_pid, pinned_runtime_binding
        candidate_pid = actual.get("pid")
        candidate_parent_pid = actual.get("parent_pid")
        if type(candidate_pid) is not int or candidate_pid <= 0:
            raise RuntimeError("The control-center server did not publish a valid process identity.")
        if type(candidate_parent_pid) is not int or candidate_parent_pid <= 0:
            raise RuntimeError("The control-center server did not publish a valid parent process identity.")
        if pinned_pid is None:
            pinned_pid = candidate_pid
        if pinned_parent_pid is None:
            pinned_parent_pid = candidate_parent_pid
        expected = replace(
            binding,
            expected_pid=pinned_pid,
            expected_parent_pid=pinned_parent_pid,
            expected_launcher_pid=pinned_launcher_pid,
        )
        validate_runtime_binding(actual, expected)
        candidate = dict(actual)
        if pinned_runtime_binding is None:
            pinned_runtime_binding = candidate
            return
        if candidate != pinned_runtime_binding:
            changed = sorted(
                key
                for key in set(candidate) | set(pinned_runtime_binding)
                if candidate.get(key) != pinned_runtime_binding.get(key)
            )
            raise RuntimeError(
                "The control-center runtime binding changed during startup "
                f"({', '.join(changed)}). Refusing to adopt it."
            )

    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError("The control-center server stopped during startup. See logs\\beez-console-server.log.")
        runtime_binding = fetch_runtime_binding()
        if runtime_binding is not None:
            pin_or_validate_runtime_binding(runtime_binding)
            paper = fetch_paper_status()
            if paper is not None:
                validate_paper_authority(paper)
                confirmed_binding = fetch_runtime_binding()
                if confirmed_binding is not None:
                    pin_or_validate_runtime_binding(confirmed_binding)
                    if process is not None and process.poll() is not None:
                        raise RuntimeError(
                            "The control-center server stopped during startup. "
                            "See logs\\beez-console-server.log."
                        )
                    return
        if process is not None and process.poll() is not None:
            raise RuntimeError("The control-center server stopped during startup. See logs\\beez-console-server.log.")
        time.sleep(0.25)
    raise RuntimeError(
        f"The control-center server did not publish the validated runtime binding within {timeout_seconds:g} seconds. "
        "See logs\\beez-console-server.log."
    )


def _path_is_within(candidate: str, root: Path) -> bool:
    try:
        Path(candidate.strip().strip('"')).resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def _sanitized_frozen_child_environment(
    environment: Mapping[str, str] | None = None,
    *,
    bundle_root: str | Path | None = None,
) -> dict[str, str]:
    """Remove PyInstaller-bundle PATH entries before launching project Python."""
    child = dict(os.environ if environment is None else environment)
    raw_bundle_root = getattr(sys, "_MEIPASS", None) if bundle_root is None else bundle_root
    if not raw_bundle_root:
        return child
    resolved_bundle_root = Path(raw_bundle_root).resolve()
    for key in tuple(child):
        if key.upper() != "PATH":
            continue
        entries = child[key].split(os.pathsep)
        child[key] = os.pathsep.join(
            entry for entry in entries
            if not entry or not _path_is_within(entry, resolved_bundle_root)
        )
    return child


def _reset_frozen_windows_dll_directory() -> None:
    """Restore the standard Windows DLL search path for an external child."""
    if os.name != "nt":
        return
    if ctypes.windll.kernel32.SetDllDirectoryW(None) == 0:
        raise RuntimeError("BeezConsole could not restore the Windows DLL search path for project Python.")


def delegate_frozen_launcher(root: Path, argv: Sequence[str]) -> subprocess.Popen[bytes]:
    """Make the checkout source authoritative even when a packaged wrapper is stale."""
    python, _ = validate_project_root(root)
    launcher_source = root / "beez_console.py"
    if not launcher_source.is_file():
        raise RuntimeError("BeezConsole cannot find the authoritative checkout launcher source.")
    # PyInstaller sets SetDllDirectoryW(sys._MEIPASS), and Windows propagates
    # that search path to children. Project Python is an external program, so
    # restore the standard loader path and remove any hook-added bundle PATH
    # entries before creating it. This wrapper exits immediately afterward and
    # therefore does not need to restore the frozen loader environment.
    _reset_frozen_windows_dll_directory()
    child_environment = _sanitized_frozen_child_environment()
    return subprocess.Popen(
        [str(python), str(launcher_source), *argv],
        cwd=root,
        env=child_environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=_creation_flags(),
    )


def open_brave(brave: Path, root: Path) -> None:
    # Brave may retain the SPA shell from an earlier local backend.  A unique
    # document URL makes a launcher restart load the current hashed bundle;
    # assets themselves remain cacheable and content-addressed.
    launch_url = f"{URL}/?launch={time.time_ns()}"
    subprocess.Popen([str(brave), "--new-window", launch_url], cwd=root, creationflags=_creation_flags())


def show_error(message: str) -> None:
    if os.name == "nt":
        ctypes.windll.user32.MessageBoxW(None, message, "BeezConsole startup failed", 0x10)
    else:  # pragma: no cover - the package is Windows-only
        print(message, file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        root = project_root()
        validate_project_root(root)
        launch_arguments = () if argv is None else argv
        if getattr(sys, "frozen", False):
            # The executable is a UI wrapper. Runtime selection and startup are
            # always decided by the current checkout source and its exact Git
            # identity, never by PyInstaller-bundled strategy contracts.
            delegate_frozen_launcher(root, launch_arguments)
            return 0

        listener_open = port_is_open()
        binding = resolve_launch_binding(
            root,
            argv=launch_arguments,
            validate_remembered_evidence=False,
        )
        if listener_open:
            binding = binding_for_existing_listener(root, binding)
            wait_for_server(None, binding)
        else:
            # Serialize direct launchers, then repeat both the port and durable
            # selector checks while holding the reservation. The handoff
            # supervisor remains independently authoritative through its
            # requested operation state and target-launch claim.
            with direct_launch_reservation(binding.runtime_root):
                if port_is_open():
                    binding = resolve_launch_binding(
                        root,
                        argv=launch_arguments,
                        validate_remembered_evidence=False,
                    )
                    binding = binding_for_existing_listener(root, binding)
                    wait_for_server(None, binding)
                else:
                    binding = resolve_launch_binding(
                        root,
                        argv=launch_arguments,
                        validate_remembered_evidence=True,
                    )
                    assert_remembered_launch_is_not_competing(binding)
                    process = start_server(root, binding)
                    wait_for_server(process, binding)
        brave = brave_path()
        if brave is None:
            raise RuntimeError("Brave Browser was not found. Install Brave or set BRAVE_PATH to the brave.exe path.")
        open_brave(brave, root)
        return 0
    except (OSError, RuntimeError) as exc:
        show_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
