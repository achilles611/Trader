"""Fail-closed, process-bound profile switching for Sim101 paper operation.

The active process prepares a fresh ledger/audit run, flattens and disarms the
current profile, then delegates restart to a detached local supervisor.  The
supervisor will not launch the target profile unless the old process publishes
a complete controlled-ledger-shutdown receipt.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from typing import BinaryIO, Callable, Iterator, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from .contracts import (
    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
    PAPER_PROFILE_CATALOG,
    PaperProfileDefinition,
    resolve_paper_profile,
)
from .ledger import (
    RISK_CONTINUITY_ANCHOR_SCHEMA,
    read_risk_continuity_guard,
    risk_continuity_anchor_path,
    risk_continuity_guard_path,
)
from .risk_continuity import (
    read_risk_continuity_artifact,
    write_risk_continuity_artifact,
)
from .ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    LocalPaperSecretProvider,
    sign_payload,
    verify_signature,
)
from .perpetual_startup_seed import (
    read_perpetual_startup_seed_artifact,
    read_perpetual_startup_seed_proof,
    write_perpetual_startup_seed_proof,
)
from .verification import LocalLedgerVerificationController, VerificationPaths


PROFILE_SWITCH_SCHEMA = "lane-iii-paper-profile-switch-v1"
PROFILE_SELECTION_SCHEMA = "lane-iii-paper-profile-selection-v3"
PROFILE_SWITCH_ACTION_HEADER = "X-Beelzebub-Profile-Switch-Action"
PROFILE_SWITCH_ACTION_VALUE = "sim101-profile-switch-v1"
PROFILE_SWITCH_TOKEN_HEADER = "X-Beelzebub-Profile-Switch-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_RECORD_HASH = re.compile(r"^[0-9a-f]{64}$")
_PROFILE_SWITCH_OPERATION = re.compile(r"^profile-switch-[0-9a-f]{32}$")
_SOURCE_BEFORE_OPERATION = re.compile(r"^source-before-(profile-switch-[0-9a-f]{32})$")
_ACTIVE_STAGES = frozenset({
    "PREPARING", "STOPPING_CURRENT", "AWAITING_FLAT", "SHUTDOWN_REQUESTED",
    "VERIFYING_STARTUP_SEED", "STARTING_TARGET", "TARGET_PROCESS_CREATED",
    "AUTOSTARTING_TARGET", "TARGET_ACTIVE_FLAT_BLOCKED",
    "TARGET_CLEANUP_UNPROVEN",
})
_TERMINAL_STAGES = frozenset({"RUNNING", "BLOCKED_SAFE", "RUNNING_SELECTION_PERSISTENCE_FAILED"})


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


def _write_exclusive_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), sort_keys=True, indent=2) + "\n"
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _selection_path(runtime_root: Path) -> Path:
    return runtime_root / "profile-switch" / "profile-selection.json"


def assert_risk_guard_outside_runtime_root(
    runtime_root: str | Path, ledger_path: str | Path,
) -> Path:
    """Reject selector layouts whose rollback domain also contains the guard."""
    root = Path(runtime_root).resolve()
    ledger = Path(ledger_path).resolve()
    guard = risk_continuity_guard_path(ledger)
    if root in ledger.parents and (guard == root or root in guard.parents):
        raise RuntimeError("PROFILE_SWITCH_RISK_GUARD_LAYOUT_UNSAFE")
    return guard


def _validated_selection(runtime_root: Path, *, required: bool = False) -> dict[str, object] | None:
    path = _selection_path(runtime_root)
    if not path.is_file():
        if required:
            raise RuntimeError("PROFILE_SELECTION_STATE_MISSING")
        return None
    try:
        value = _read_json(path)
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PROFILE_SELECTION_STATE_INVALID") from exc
    if set(value) != {"schema", "requested", "established", "updated_at"} or value.get("schema") != PROFILE_SELECTION_SCHEMA:
        raise RuntimeError("PROFILE_SELECTION_STATE_INVALID")
    requested = value.get("requested")
    if requested is not None:
        if not isinstance(requested, Mapping) or set(requested) != {
            "profile", "request_id", "operation_id", "requested_at",
        }:
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID")
        try:
            requested_profile = resolve_paper_profile(str(requested.get("profile"))).selection_key
        except ValueError as exc:
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID") from exc
        if (
            requested.get("profile") != requested_profile
            or not isinstance(requested.get("request_id"), str)
            or not _REQUEST_ID.fullmatch(str(requested.get("request_id")))
            or not isinstance(requested.get("operation_id"), str)
            or _PROFILE_SWITCH_OPERATION.fullmatch(str(requested.get("operation_id"))) is None
            or not isinstance(requested.get("requested_at"), str)
        ):
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID")
    established = value.get("established")
    if established is not None:
        required_fields = {
            "profile", "operation_id", "ledger_path", "ledger_identity", "ledger_epoch",
            "risk_anchor_path", "risk_guard_path", "audit_root", "git_sha", "established_at",
        }
        if not isinstance(established, Mapping) or set(established) != required_fields:
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID")
        try:
            established_profile = resolve_paper_profile(str(established.get("profile"))).selection_key
            ledger_path = Path(str(established.get("ledger_path"))).resolve()
            risk_anchor = Path(str(established.get("risk_anchor_path"))).resolve()
            risk_guard = Path(str(established.get("risk_guard_path"))).resolve()
            audit_root = Path(str(established.get("audit_root"))).resolve()
        except (OSError, ValueError) as exc:
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID") from exc
        if (
            established.get("profile") != established_profile
            or runtime_root not in ledger_path.parents
            or risk_anchor != risk_continuity_anchor_path(ledger_path)
            or risk_guard != risk_continuity_guard_path(ledger_path)
            or (runtime_root in ledger_path.parents and runtime_root in risk_guard.parents)
            or runtime_root not in risk_anchor.parents
            or runtime_root not in audit_root.parents
            or not isinstance(established.get("ledger_epoch"), str)
            or not str(established.get("ledger_epoch")).startswith("L3G-PAPER-EPOCH-")
            or not isinstance(established.get("ledger_identity"), str)
            or re.fullmatch(r"l3g-ledger-[0-9a-f]{32}", str(established.get("ledger_identity"))) is None
            or not isinstance(established.get("git_sha"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", str(established.get("git_sha")))
            or not isinstance(established.get("operation_id"), str)
            or not isinstance(established.get("established_at"), str)
        ):
            raise RuntimeError("PROFILE_SELECTION_STATE_INVALID")
    return {
        "schema": PROFILE_SELECTION_SCHEMA,
        "requested": None if requested is None else dict(requested),
        "established": None if established is None else dict(established),
        "updated_at": value.get("updated_at"),
    }


def _validated_established_ledger_metadata(
    ledger_path: Path, established: Mapping[str, object],
) -> dict[str, str]:
    """Read and bind a remembered ledger without letting SQLite create it."""
    try:
        if ledger_path.stat().st_size <= 0:
            raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_LEDGER_INVALID")
        # mode=ro preserves the no-create gate while still reading a durable
        # WAL left by an unclean process exit. SQLite immutable mode would
        # ignore that WAL and could falsely reject a valid remembered run.
        connection = sqlite3.connect(ledger_path.as_uri() + "?mode=ro", uri=True)
        try:
            connection.execute("PRAGMA query_only=ON")
            rows = connection.execute(
                "SELECT metadata_key, metadata_value FROM lane_iii_paper_ledger_metadata"
            ).fetchall()
            placeholders = ",".join("?" for _ in range(7))
            latest_risk = connection.execute(
                "SELECT ledger_sequence, record_hash FROM lane_iii_paper_audit "
                f"WHERE kind IN ({placeholders}) ORDER BY ledger_sequence DESC LIMIT 1",
                (
                    "EXECUTION", "EXECUTION_REALIZED_PNL", "RISK_EVENT_CONTINUITY_IMPORTED",
                    "RISK_EVENT_ENTRY_ACCOUNTED", "RISK_EVENT_EXIT_ACCOUNTED",
                    "RISK_EVENT_AUTHORITY_LOCKOUT", "RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED",
                ),
            ).fetchone()
        finally:
            connection.close()
    except RuntimeError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_LEDGER_INVALID") from exc
    metadata = {str(key): str(value) for key, value in rows}
    profile = resolve_paper_profile(str(established["profile"]))
    expected = {
        "ledger_uuid": str(established["ledger_identity"]),
        "ledger_epoch": str(established["ledger_epoch"]),
        "paper_policy_hash": profile.policy.configuration_hash,
        "risk_profile_hash": profile.risk.configuration_hash,
        "entry_profile": profile.policy.entry_profile,
        "entry_profile_version": profile.policy.entry_profile_version,
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_LEDGER_IDENTITY_MISMATCH")
    if not metadata.get("schema_version") or not metadata.get("created_at"):
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_LEDGER_INVALID")
    anchor_path = risk_continuity_anchor_path(ledger_path)
    try:
        anchor = _read_json(anchor_path)
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_RISK_ANCHOR_INVALID") from exc
    sequence = anchor.get("risk_boundary_sequence")
    record_hash = anchor.get("risk_boundary_hash")
    latest_sequence = 0 if latest_risk is None else int(latest_risk[0])
    latest_hash = None if latest_risk is None else str(latest_risk[1])
    if (
        set(anchor) != {
            "schema", "ledger_path", "ledger_identity", "ledger_epoch",
            "risk_boundary_sequence", "risk_boundary_hash", "guard_path",
            "guard_hash", "updated_at",
        }
        or anchor.get("schema") != RISK_CONTINUITY_ANCHOR_SCHEMA
        or anchor.get("ledger_path") != str(ledger_path)
        or anchor.get("ledger_identity") != established.get("ledger_identity")
        or anchor.get("ledger_epoch") != established.get("ledger_epoch")
        or type(sequence) is not int
        or sequence < 0
        or sequence != latest_sequence
        or record_hash != latest_hash
        or anchor.get("guard_path") != str(risk_continuity_guard_path(ledger_path))
        or not isinstance(anchor.get("guard_hash"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(anchor.get("guard_hash"))) is None
        or not isinstance(anchor.get("updated_at"), str)
    ):
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_RISK_ANCHOR_MISMATCH")
    try:
        guard = read_risk_continuity_guard(ledger_path)
    except RuntimeError as exc:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_RISK_GUARD_INVALID") from exc
    if (
        Path(str(established["risk_guard_path"])).resolve()
        != risk_continuity_guard_path(ledger_path)
        or guard.get("ledger_path") != str(ledger_path)
        or guard.get("ledger_identity") != established.get("ledger_identity")
        or guard.get("ledger_epoch") != established.get("ledger_epoch")
        or guard.get("risk_boundary_sequence") != latest_sequence
        or guard.get("risk_boundary_hash") != latest_hash
        or guard.get("guard_hash") != anchor.get("guard_hash")
    ):
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_RISK_GUARD_MISMATCH")
    return metadata


def remembered_profile_selection(runtime_root: str | Path, *, git_sha: str) -> dict[str, object] | None:
    """Return only the last successfully established run for ordinary launch."""
    root = Path(runtime_root).resolve()
    selection = _validated_selection(root)
    if selection is None or selection.get("established") is None:
        return None
    established = dict(selection["established"])  # type: ignore[arg-type]
    if established.get("git_sha") != git_sha:
        raise RuntimeError("PROFILE_SELECTION_CHECKOUT_MISMATCH")
    ledger_path = Path(str(established["ledger_path"])).resolve()
    audit_root = Path(str(established["audit_root"])).resolve()
    if not ledger_path.is_file() or not audit_root.is_dir():
        # PaperLedger creates a missing database by design. A remembered run is
        # different: lost evidence must fail before construction can turn it
        # into a fresh empty allowance.
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_EVIDENCE_MISSING")
    _validated_established_ledger_metadata(ledger_path, established)
    operation_id = str(established["operation_id"])
    source_match = _SOURCE_BEFORE_OPERATION.fullmatch(operation_id)
    if _PROFILE_SWITCH_OPERATION.fullmatch(operation_id) is not None:
        manifest_operation_id = operation_id
        target_established = True
    elif source_match is not None:
        manifest_operation_id = str(source_match.group(1))
        target_established = False
    else:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_OPERATION_INVALID")
    operation_root = root / "profile-switch" / "operations" / manifest_operation_id
    try:
        manifest = _manifest(operation_root / "manifest.json")
        state = _read_json(operation_root / "state.json")
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_OPERATION_INVALID") from exc
    common_mismatch = (
        manifest.get("operation_id") != manifest_operation_id
        or manifest.get("git_sha") != established.get("git_sha")
        or state.get("schema") != PROFILE_SWITCH_SCHEMA
        or state.get("operation_id") != manifest_operation_id
        or state.get("current_profile") != manifest.get("current_profile")
        or state.get("target_profile") != manifest.get("target_profile")
    )
    if target_established:
        operation_mismatch = (
            manifest.get("target_profile") != established.get("profile")
            or Path(str(manifest.get("ledger_path"))).resolve() != ledger_path
            or manifest.get("ledger_epoch") != established.get("ledger_epoch")
            or Path(str(manifest.get("audit_root"))).resolve() != audit_root
            or state.get("stage") != "RUNNING"
            or Path(str(state.get("ledger_path"))).resolve() != ledger_path
            or state.get("ledger_epoch") != established.get("ledger_epoch")
            or Path(str(state.get("audit_root"))).resolve() != audit_root
        )
    else:
        operation_mismatch = manifest.get("current_profile") != established.get("profile")
    if common_mismatch or operation_mismatch:
        raise RuntimeError("PROFILE_SELECTION_ESTABLISHED_OPERATION_INVALID")
    return established


def _record_requested_selection(
    runtime_root: Path,
    manifest: Mapping[str, object],
    *,
    current_runtime_binding: Mapping[str, object] | None = None,
) -> None:
    selection = _validated_selection(runtime_root) or {
        "schema": PROFILE_SELECTION_SCHEMA, "requested": None, "established": None,
        "updated_at": _utc_now(),
    }
    requested_at = str(manifest["created_at"])
    if selection.get("established") is None and current_runtime_binding is not None:
        current_ledger = Path(str(current_runtime_binding.get("ledger") or "")).resolve()
        current_audit = Path(str(current_runtime_binding.get("audit") or "")).resolve()
        current_anchor = risk_continuity_anchor_path(current_ledger)
        current_guard = assert_risk_guard_outside_runtime_root(runtime_root, current_ledger)
        if (
            current_runtime_binding.get("entry_profile_version") != manifest.get("current_profile")
            or current_runtime_binding.get("git_sha") != manifest.get("git_sha")
            or runtime_root not in current_ledger.parents
            or runtime_root not in current_audit.parents
            or not isinstance(current_runtime_binding.get("ledger_epoch"), str)
            or re.fullmatch(
                r"l3g-ledger-[0-9a-f]{32}",
                str(current_runtime_binding.get("ledger_identity") or ""),
            ) is None
            or not current_anchor.is_file()
            or not current_guard.is_file()
        ):
            raise RuntimeError("PROFILE_SELECTION_CURRENT_RUN_BINDING_MISMATCH")
        selection["established"] = {
            "profile": manifest["current_profile"],
            "operation_id": "source-before-" + str(manifest["operation_id"]),
            "ledger_path": str(current_ledger),
            "ledger_identity": current_runtime_binding["ledger_identity"],
            "ledger_epoch": current_runtime_binding["ledger_epoch"],
            "risk_anchor_path": str(current_anchor),
            "risk_guard_path": str(current_guard),
            "audit_root": str(current_audit),
            "git_sha": manifest["git_sha"],
            "established_at": requested_at,
        }
    selection.update({
        "requested": {
            "profile": manifest["target_profile"],
            "request_id": manifest["request_id"],
            "operation_id": manifest["operation_id"],
            "requested_at": requested_at,
        },
        "updated_at": requested_at,
    })
    _atomic_json(_selection_path(runtime_root), selection)


def _record_established_selection(
    runtime_root: Path, manifest: Mapping[str, object], binding: Mapping[str, object],
) -> None:
    selection = _validated_selection(runtime_root, required=True)
    assert selection is not None
    requested = selection.get("requested")
    if not isinstance(requested, Mapping) or (
        requested.get("operation_id") != manifest.get("operation_id")
        or requested.get("profile") != manifest.get("target_profile")
    ):
        raise RuntimeError("PROFILE_SELECTION_REQUEST_BINDING_MISMATCH")
    ledger_identity = binding.get("ledger_identity")
    if re.fullmatch(r"l3g-ledger-[0-9a-f]{32}", str(ledger_identity or "")) is None:
        raise RuntimeError("PROFILE_SELECTION_TARGET_LEDGER_IDENTITY_UNAVAILABLE")
    target_anchor = risk_continuity_anchor_path(str(manifest["ledger_path"]))
    target_guard = risk_continuity_guard_path(str(manifest["ledger_path"]))
    if not target_anchor.is_file() or not target_guard.is_file():
        raise RuntimeError("PROFILE_SELECTION_TARGET_RISK_ANCHOR_UNAVAILABLE")
    established_at = _utc_now()
    selection.update({
        "established": {
            "profile": manifest["target_profile"],
            "operation_id": manifest["operation_id"],
            "ledger_path": manifest["ledger_path"],
            "ledger_identity": ledger_identity,
            "ledger_epoch": manifest["ledger_epoch"],
            "risk_anchor_path": str(target_anchor),
            "risk_guard_path": str(target_guard),
            "audit_root": manifest["audit_root"],
            "git_sha": manifest["git_sha"],
            "established_at": established_at,
        },
        "updated_at": established_at,
    })
    _atomic_json(_selection_path(runtime_root), selection)


def profile_catalog() -> list[dict[str, object]]:
    return [profile.payload() for profile in PAPER_PROFILE_CATALOG]


def exact_flat_shutdown_ready(status: Mapping[str, object]) -> bool:
    ledger = status.get("ledger")
    ledger = ledger if isinstance(ledger, Mapping) else {}
    continuity = status.get("risk_continuity")
    continuity = continuity if isinstance(continuity, Mapping) else {}
    authority_lockout = continuity.get("authority_lockout")
    authority_lockout = authority_lockout if isinstance(authority_lockout, Mapping) else {}
    risk = status.get("risk")
    risk = risk if isinstance(risk, Mapping) else {}
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
        and authority_lockout.get("locked_out") is risk.get("locked_out")
        and authority_lockout.get("lockout_reason") == risk.get("lockout_reason")
        and authority_lockout.get("lockout_trade_date") == risk.get("lockout_trade_date")
    )


def _valid_shutdown_tip(sequence: object, record_hash: object) -> bool:
    return (
        type(sequence) is int
        and sequence >= 0
        and (
            (sequence == 0 and record_hash is None)
            or (sequence > 0 and isinstance(record_hash, str) and _RECORD_HASH.fullmatch(record_hash) is not None)
        )
    )


def _valid_risk_boundary(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {
            "path", "ledger_identity", "ledger_epoch", "risk_boundary_sequence", "risk_boundary_hash",
        }
        and isinstance(value.get("path"), str) and bool(value.get("path"))
        and isinstance(value.get("ledger_identity"), str) and bool(value.get("ledger_identity"))
        and isinstance(value.get("ledger_epoch"), str) and bool(value.get("ledger_epoch"))
        and _valid_shutdown_tip(value.get("risk_boundary_sequence"), value.get("risk_boundary_hash"))
    )


def _clean_shutdown_receipt(receipt: Mapping[str, object]) -> bool:
    expected_sequence = receipt.get("expected_tip_sequence")
    expected_hash = receipt.get("expected_tip_hash")
    durable_sequence = receipt.get("durable_tip_sequence")
    durable_hash = receipt.get("durable_tip_hash")
    return (
        receipt.get("schema") == "l3g-ledger-controlled-shutdown-v1"
        and receipt.get("clean_shutdown") is True
        and receipt.get("admission_sealed") is True
        and receipt.get("writer_stopped") is True
        and isinstance(receipt.get("checkpoint"), Mapping)
        and receipt["checkpoint"].get("complete") is True  # type: ignore[index]
        and _valid_shutdown_tip(expected_sequence, expected_hash)
        and _valid_shutdown_tip(durable_sequence, durable_hash)
        and expected_sequence == durable_sequence
        and expected_hash == durable_hash
        and _valid_risk_boundary(receipt.get("risk_continuity_boundary"))
        and isinstance(receipt.get("verifier_shutdown"), Mapping)
        and receipt["verifier_shutdown"].get("completed") is True  # type: ignore[index]
        and isinstance(receipt.get("runtime_watchdog_shutdown"), Mapping)
        and receipt["runtime_watchdog_shutdown"].get("completed") is True  # type: ignore[index]
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
        export_perpetual_startup_seed: (
            Callable[[str, Path], Mapping[str, object]] | None
        ) = None,
        request_shutdown: Callable[[], None],
        runtime_root: str | Path,
        project_root: str | Path,
        python_executable: str | Path,
        git_sha: str,
        parent_pid: int | None = None,
        launch_supervisor: Callable[[Path, int], None] | None = None,
        current_runtime_binding: Mapping[str, object] | None = None,
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
        self._export_perpetual_startup_seed = export_perpetual_startup_seed
        self._request_shutdown = request_shutdown
        self.runtime_root = Path(runtime_root).resolve()
        self.project_root = Path(project_root).resolve()
        self.python_executable = Path(python_executable).resolve()
        self.git_sha = git_sha
        self.parent_pid = os.getpid() if parent_pid is None else parent_pid
        self._launch_supervisor = launch_supervisor or self._default_launch_supervisor
        self._current_runtime_binding = (
            None if current_runtime_binding is None else dict(current_runtime_binding)
        )
        if self._current_runtime_binding is not None:
            assert_risk_guard_outside_runtime_root(
                self.runtime_root,
                str(self._current_runtime_binding.get("ledger") or ""),
            )
        self._runtime_binding_finalized = bool(
            self._current_runtime_binding is not None
            and re.fullmatch(
                r"l3g-ledger-[0-9a-f]{32}",
                str(self._current_runtime_binding.get("ledger_identity") or ""),
            )
        )
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
        if not inherited:
            try:
                selection = _validated_selection(self.runtime_root)
                requested = None if selection is None else selection.get("requested")
                if isinstance(requested, Mapping):
                    inherited = str(requested.get("operation_id") or "") or None
            except RuntimeError:
                self._state.update({
                    "stage": "BLOCKED_SAFE",
                    "blockers": ["PROFILE_SELECTION_STATE_INVALID"],
                    "updated_at": _utc_now(),
                })
        if inherited:
            candidate = self.runtime_root / "profile-switch" / "operations" / inherited / "state.json"
            self._state_path = candidate
            if candidate.is_file():
                try:
                    self._state = _read_json(candidate)
                except (OSError, ValueError, RuntimeError):
                    self._state.update({
                        "operation_id": inherited,
                        "stage": "BLOCKED_SAFE",
                        "blockers": ["PROFILE_SWITCH_STATE_INVALID"],
                        "updated_at": _utc_now(),
                    })
            else:
                self._state.update({
                    "operation_id": inherited,
                    "stage": "BLOCKED_SAFE",
                    "in_progress": False,
                    "blockers": ["PROFILE_SWITCH_STATE_INVALID"],
                    "updated_at": _utc_now(),
                })

    def bind_current_runtime_binding(self, binding: Mapping[str, object]) -> None:
        """Install the post-ledger startup identity before accepting a switch."""
        resolved = dict(binding)
        assert_risk_guard_outside_runtime_root(
            self.runtime_root, str(resolved.get("ledger") or ""),
        )
        if re.fullmatch(
            r"l3g-ledger-[0-9a-f]{32}", str(resolved.get("ledger_identity") or ""),
        ) is None:
            raise RuntimeError("PROFILE_SWITCH_RUNTIME_BINDING_INVALID")
        with self._lock:
            if self._runtime_binding_finalized:
                if resolved == self._current_runtime_binding:
                    return
                raise RuntimeError("PROFILE_SWITCH_RUNTIME_BINDING_TOO_LATE")
            self._current_runtime_binding = resolved
            self._runtime_binding_finalized = True

    @property
    def action_token(self) -> str:
        return self._token

    @property
    def audit_path(self) -> Path:
        return self.runtime_root / "profile-switch" / "profile-switch-audit.jsonl"

    def status(self) -> dict[str, object]:
        with self._lock:
            if self._state_path is not None:
                try:
                    if not self._state_path.is_file():
                        raise RuntimeError("PROFILE_SWITCH_STATE_INVALID")
                    disk = _read_json(self._state_path)
                    if (
                        disk.get("schema") != PROFILE_SWITCH_SCHEMA
                        or (
                            self._state.get("operation_id") is not None
                            and disk.get("operation_id") != self._state.get("operation_id")
                        )
                    ):
                        raise RuntimeError("PROFILE_SWITCH_STATE_INVALID")
                    self._state = disk
                except (OSError, ValueError, RuntimeError):
                    self._state.update({
                        "stage": "BLOCKED_SAFE",
                        "in_progress": False,
                        "blockers": ["PROFILE_SWITCH_STATE_INVALID"],
                        "updated_at": _utc_now(),
                    })
            state = dict(self._state)
            try:
                selection = _validated_selection(self.runtime_root)
            except RuntimeError:
                selection = {
                    "schema": PROFILE_SELECTION_SCHEMA,
                    "requested": None,
                    "established": None,
                    "status": "INVALID",
                }
        return {
            **state,
            "active_profile": self.current_profile.selection_key,
            "runtime_root": str(self.runtime_root),
            "action_token": self._token,
            "profiles": profile_catalog(),
            "selection": selection,
            "authority": "PAPER_SIM101_PROFILE_SELECTION_ONLY",
            "live_capital": "DENIED",
        }

    def _record(self, event: str, **updates: object) -> None:
        with self._lock:
            if self._state_path is not None and self._state_path.is_file():
                try:
                    disk = _read_json(self._state_path)
                    if disk.get("stage") in _TERMINAL_STAGES and updates.get("stage") != disk.get("stage"):
                        self._state = disk
                        updates = {}
                        event = "LATE_ORIGIN_UPDATE_IGNORED"
                except (OSError, UnicodeError, ValueError, RuntimeError):
                    pass
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
        seed_required = (
            self.current_profile.selection_key
            == "BEELZEBUB_FIVE_MINUTE_BIAS_V1"
            and target.selection_key == "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"
        )
        current_binding = self._current_runtime_binding or {}
        if seed_required and (
            self._export_perpetual_startup_seed is None
            or not isinstance(current_binding.get("ledger"), str)
            or not isinstance(current_binding.get("audit"), str)
            or not isinstance(current_binding.get("ledger_identity"), str)
            or not isinstance(current_binding.get("ledger_epoch"), str)
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_BINDING_UNAVAILABLE")
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
            "source_ledger_path": current_binding.get("ledger"),
            "source_ledger_identity": current_binding.get("ledger_identity"),
            "source_ledger_epoch": current_binding.get("ledger_epoch"),
            "source_audit_root": current_binding.get("audit"),
            "perpetual_startup_seed_required": seed_required,
            "perpetual_startup_seed_path": (
                str(operation_root / "perpetual-startup-seed.json")
                if seed_required else None
            ),
            "perpetual_startup_seed_proof_path": (
                str(operation_root / "perpetual-startup-seed-proof.json")
                if seed_required else None
            ),
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
        try:
            _record_requested_selection(
                self.runtime_root, manifest,
                current_runtime_binding=self._current_runtime_binding,
            )
        except Exception as error:
            blocker = (
                str(error)
                if str(error).isupper()
                else "PROFILE_SELECTION_REQUEST_PERSISTENCE_FAILED"
            )
            self._state.update({
                "stage": "BLOCKED_SAFE",
                "in_progress": False,
                "blockers": [blocker],
                "updated_at": _utc_now(),
            })
            _atomic_json(self._state_path, self._state)
            _append_jsonl(operation_root / "preparation-audit.jsonl", {
                "schema": PROFILE_SWITCH_SCHEMA,
                "event": "OPERATION_PREPARATION_BLOCKED",
                "recorded_at": _utc_now(),
                "operation_id": operation_id,
                "stage": "BLOCKED_SAFE",
                "blockers": [blocker],
            })
            raise RuntimeError(blocker) from error
        self._record("OPERATION_PREPARED")
        return manifest_path

    def start(self, request_id: str, target_profile: str) -> dict[str, object]:
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid profile-switch request ID.")
        target = resolve_paper_profile(target_profile)
        with self._lock:
            current = self.status()
            exact_replay = (
                self._state.get("request_id") == request_id
                and self._state.get("target_profile") == target.selection_key
                and self._state.get("operation_id") is not None
            )
            if exact_replay:
                return current
            if (
                self._state.get("operation_id") is not None
                and self._state.get("blockers") == ["PROFILE_SWITCH_STATE_INVALID"]
            ):
                raise RuntimeError("PROFILE_SWITCH_STATE_INVALID")
            if target.selection_key == self.current_profile.selection_key:
                raise ValueError("The selected profile is already active.")
            if (
                (self._thread is not None and self._thread.is_alive())
                or self._state.get("stage") in _ACTIVE_STAGES
                or self._state.get("in_progress") is True
            ):
                raise RuntimeError("PROFILE_SWITCH_ALREADY_IN_PROGRESS")
            if self._state.get("request_id") == request_id and self._state.get("operation_id"):
                raise ValueError("Profile-switch request ID is already bound to another target.")
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
            status = self._paper_status()
            continuity_path = manifest_path.with_name("risk-continuity.json")
            artifact = write_risk_continuity_artifact(
                continuity_path,
                operation_id=str(self._state["operation_id"]),
                source_profile=self.current_profile.selection_key,
                target_profile=str(self._state["target_profile"]),
                snapshot=status.get("risk_continuity"),
            )
            self._record(
                "RISK_CONTINUITY_SEALED",
                risk_continuity_path=str(continuity_path),
                risk_continuity_artifact_sha256=artifact["artifact_sha256"],
            )
            manifest = _manifest(manifest_path)
            if manifest.get("perpetual_startup_seed_required") is True:
                exporter = self._export_perpetual_startup_seed
                seed_path = Path(
                    str(manifest.get("perpetual_startup_seed_path") or ""),
                ).resolve()
                if exporter is None or seed_path.parent != manifest_path.parent:
                    raise RuntimeError(
                        "PERPETUAL_STARTUP_SEED_EXPORT_BINDING_INVALID",
                    )
                seed = exporter(str(manifest["operation_id"]), seed_path)
                sealed_seed = read_perpetual_startup_seed_artifact(
                    seed_path,
                    operation_id=str(manifest["operation_id"]),
                    expected_at=_utc_now(),
                )
                if seed.get("artifact_sha256") != sealed_seed.get(
                    "artifact_sha256",
                ):
                    raise RuntimeError(
                        "PERPETUAL_STARTUP_SEED_EXPORT_RESULT_MISMATCH",
                    )
                self._record(
                    "PERPETUAL_STARTUP_SEED_SEALED",
                    perpetual_startup_seed_path=str(seed_path),
                    perpetual_startup_seed_artifact_sha256=sealed_seed.get(
                        "artifact_sha256",
                    ),
                )
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
        with self._lock:
            if self._state_path is not None and self._state_path.is_file():
                try:
                    self._state = _read_json(self._state_path)
                except (OSError, UnicodeError, ValueError, RuntimeError):
                    return
            if (
                self._state_path is None
                or self._state.get("operation_id") is None
                or self._state.get("stage") != "SHUTDOWN_REQUESTED"
                or self._state.get("current_profile") != self.current_profile.selection_key
            ):
                return
        clean = _clean_shutdown_receipt(receipt)
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
    try:
        current = resolve_paper_profile(str(value.get("current_profile") or ""))
        target = resolve_paper_profile(str(value.get("target_profile") or ""))
    except ValueError as exc:
        raise RuntimeError("PROFILE_SWITCH_MANIFEST_PROFILE_MISMATCH") from exc
    if (
        value.get("current_profile") != current.selection_key
        or value.get("target_profile") != target.selection_key
        or current.selection_key == target.selection_key
        or value.get("paper_policy_hash") != target.policy.configuration_hash
        or value.get("risk_profile_hash") != target.risk.configuration_hash
        or value.get("paper_only") is not True
        or value.get("live_capital") != "DENIED"
    ):
        raise RuntimeError("PROFILE_SWITCH_MANIFEST_PROFILE_MISMATCH")
    seed_required = (
        current.selection_key == "BEELZEBUB_FIVE_MINUTE_BIAS_V1"
        and target.selection_key == "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"
    )
    declared_seed_requirement = value.get("perpetual_startup_seed_required")
    if seed_required:
        if declared_seed_requirement is not True:
            raise RuntimeError("PROFILE_SWITCH_MANIFEST_PROFILE_MISMATCH")
    elif declared_seed_requirement not in {None, False}:
        # Manifests sealed before the perpetual profile existed did not carry
        # this field. Preserve their validity without allowing a V1 -> V2
        # operation to bypass the mandatory authenticated startup seed.
        raise RuntimeError("PROFILE_SWITCH_MANIFEST_PROFILE_MISMATCH")
    if seed_required:
        operation_root = path.parent.resolve()
        try:
            seed_path = Path(
                str(value["perpetual_startup_seed_path"]),
            ).resolve()
            proof_path = Path(
                str(value["perpetual_startup_seed_proof_path"]),
            ).resolve()
            source_path = Path(str(value["source_ledger_path"])).resolve()
            source_audit = Path(str(value["source_audit_root"])).resolve()
        except (KeyError, OSError, ValueError) as exc:
            raise RuntimeError("PROFILE_SWITCH_MANIFEST_SEED_BINDING_INVALID") from exc
        if (
            seed_path.parent != operation_root
            or proof_path.parent != operation_root
            or not source_path.is_absolute()
            or not source_audit.is_absolute()
            or re.fullmatch(
                r"l3g-ledger-[0-9a-f]{32}",
                str(value.get("source_ledger_identity") or ""),
            ) is None
            or not str(value.get("source_ledger_epoch") or "").startswith(
                "L3G-PAPER-EPOCH-",
            )
        ):
            raise RuntimeError("PROFILE_SWITCH_MANIFEST_SEED_BINDING_INVALID")
    value["manifest_sha256"] = supplied
    return value


def validated_profile_switch_manifest(path: Path) -> dict[str, object]:
    """Read one integrity-checked profile-switch manifest."""
    return _manifest(path)


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


def _target_binding_matches(
    binding: Mapping[str, object], manifest: Mapping[str, object],
    continuity: Mapping[str, object],
    *,
    target_pid: int,
    seed_artifact: Mapping[str, object] | None = None,
    seed_proof: Mapping[str, object] | None = None,
) -> bool:
    runtime_pid = binding.get("pid")
    runtime_parent_pid = binding.get("parent_pid")
    process_tree_matches = (
        type(target_pid) is int
        and target_pid > 0
        and type(runtime_pid) is int
        and runtime_pid > 0
        and type(runtime_parent_pid) is int
        and runtime_parent_pid > 0
        and (runtime_pid == target_pid or runtime_parent_pid == target_pid)
    )
    seed_matches = (
        manifest.get("perpetual_startup_seed_required") is not True
        or (
            seed_artifact is not None
            and seed_proof is not None
            and binding.get("perpetual_startup_seed_operation_id")
            == manifest.get("operation_id")
            and binding.get("perpetual_startup_seed_artifact_sha256")
            == seed_artifact.get("artifact_sha256")
            and binding.get("perpetual_startup_seed_proof_sha256")
            == seed_proof.get("proof_sha256")
            and binding.get("profile_switch_manifest_sha256")
            == manifest.get("manifest_sha256")
        )
    )
    return seed_matches and process_tree_matches and (
        binding.get("ledger") == str(manifest["ledger_path"])
        and binding.get("audit") == str(manifest["audit_root"])
        and binding.get("git_sha") == manifest.get("git_sha")
        and binding.get("entry_profile_version") == manifest.get("target_profile")
        and binding.get("paper_policy_hash") == manifest.get("paper_policy_hash")
        and binding.get("risk_profile_hash") == manifest.get("risk_profile_hash")
        and binding.get("ledger_epoch") == manifest.get("ledger_epoch")
        and re.fullmatch(
            r"l3g-ledger-[0-9a-f]{32}", str(binding.get("ledger_identity") or ""),
        ) is not None
        and binding.get("risk_continuity_artifact_sha256") == continuity.get("artifact_sha256")
    )


def _control_port_released(host: str = "127.0.0.1", port: int = 8090) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            if os.name == "nt":
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            probe.bind((host, port))
    except OSError:
        return False
    return True


@contextmanager
def _control_port_lease(
    host: str = "127.0.0.1", port: int = 8090,
) -> Iterator[socket.socket]:
    """Hold exclusive ownership of the control endpoint across finalization."""
    lease = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if os.name == "nt":
            lease.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:  # pragma: no cover - production host is Windows
            lease.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        lease.bind((host, port))
    except OSError as exc:
        lease.close()
        raise RuntimeError("PROFILE_SWITCH_CONTROL_PORT_LEASE_UNAVAILABLE") from exc
    try:
        yield lease
    finally:
        lease.close()


def _lock_operation_file(handle: BinaryIO) -> None:
    handle.seek(0)
    if handle.read(1) == b"":
        handle.write(b"\0")
        handle.flush()
        os.fsync(handle.fileno())
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover - production host is Windows
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise RuntimeError("PROFILE_SWITCH_OPERATION_LOCKED") from exc


def _unlock_operation_file(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:  # pragma: no cover - production host is Windows
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def profile_switch_operation_lock(manifest_path: Path) -> Iterator[None]:
    """Serialize one supervisor/finalizer pair with an OS-owned lock."""
    lock_path = manifest_path.resolve().with_name("operation.lock")
    with lock_path.open("a+b") as handle:
        _lock_operation_file(handle)
        try:
            yield
        finally:
            _unlock_operation_file(handle)


def _operation_locked(function: Callable[..., int]) -> Callable[..., int]:
    """Keep the existing supervisor body while holding its operation lock."""
    @wraps(function)
    def locked(manifest_path: Path, *args: object, **kwargs: object) -> int:
        with profile_switch_operation_lock(manifest_path):
            return function(manifest_path, *args, **kwargs)

    # ``functools.wraps`` intentionally does not copy keyword defaults, while
    # the supervisor's bounded-budget test and operator diagnostics inspect
    # them directly.
    locked.__kwdefaults__ = function.__kwdefaults__
    return locked


def _target_paper_identity_matches(
    paper: Mapping[str, object], manifest: Mapping[str, object],
) -> bool:
    """Require the exact paper-only authority selected by this handoff."""
    return bool(
        paper.get("entry_profile_version") == manifest.get("target_profile")
        and paper.get("mode") == "PAPER_SIM101"
        and paper.get("paper_account") == "Sim101"
        and paper.get("account_class") == "LOCAL_SIMULATION"
        and paper.get("market_instrument") == "MNQ SEP26"
        and type(paper.get("maximum_quantity")) is int
        and paper.get("maximum_quantity") == 1
        and paper.get("live_capital") == "DENIED"
    )


def _target_operational_active(
    paper: Mapping[str, object], manifest: Mapping[str, object],
) -> bool:
    session = paper.get("operational_paper_session")
    return bool(
        _target_paper_identity_matches(paper, manifest)
        and isinstance(session, Mapping)
        and session.get("active") is True
        and session.get("stopping") is not True
    )


def _target_perpetual_position_proven(
    paper: Mapping[str, object], manifest: Mapping[str, object],
) -> bool:
    """Prove one reconciled MNQ position and its sole protective order."""
    if manifest.get("target_profile") != FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION:
        return _target_operational_active(paper, manifest)
    position = paper.get("current_position")
    requirement = paper.get("position_requirement")
    source = requirement.get("source_signal") if isinstance(requirement, Mapping) else None
    reasons = requirement.get("blocking_reasons") if isinstance(requirement, Mapping) else None
    expected_state = position if position in {"LONG", "SHORT"} else None
    return bool(
        _target_operational_active(paper, manifest)
        and expected_state is not None
        and paper.get("state") == expected_state
        and paper.get("paper_execution") == "POSITIONED"
        and type(paper.get("current_quantity")) is int
        and paper.get("current_quantity") == 1
        and type(paper.get("current_position_quantity")) is int
        and paper.get("current_position_quantity") == 1
        and paper.get("broker_snapshot_position") == expected_state
        and type(paper.get("broker_snapshot_position_quantity")) is int
        and paper.get("broker_snapshot_position_quantity") == 1
        and type(paper.get("working_owned_orders")) is int
        and paper.get("working_owned_orders") == 1
        and type(paper.get("working_entry_orders")) is int
        and paper.get("working_entry_orders") == 0
        and paper.get("protective_stop_state") == "WORKING"
        and paper.get("foreign_activity") is False
        and paper.get("position_snapshot_complete") is True
        and paper.get("order_snapshot_complete") is True
        and paper.get("reconciliation_current") is True
        and paper.get("unresolved_command") is False
        and paper.get("unresolved_native_order") is False
        and paper.get("unresolved_execution") is False
        and isinstance(requirement, Mapping)
        and requirement.get("required") is True
        and requirement.get("state") == "POSITIONED"
        and requirement.get("actual_position") == expected_state
        and type(requirement.get("actual_quantity")) is int
        and requirement.get("actual_quantity") == 1
        and requirement.get("desired_position") == expected_state
        and requirement.get("primary_blocker") is None
        and isinstance(reasons, (list, tuple))
        and not reasons
        and isinstance(source, Mapping)
        and source.get("direction") == expected_state
        and isinstance(source.get("candle_close_utc"), str)
        and bool(str(source.get("candle_close_utc")).strip())
        and isinstance(source.get("signal_hash"), str)
        and bool(str(source.get("signal_hash")).strip())
        and type(source.get("ledger_sequence")) is int
        and source.get("ledger_sequence", 0) > 0
        and isinstance(source.get("record_hash"), str)
        and bool(str(source.get("record_hash")).strip())
        and source.get("ledger_verified") is True
    )


def _target_perpetual_flat_blocked(
    paper: Mapping[str, object], manifest: Mapping[str, object],
) -> bool:
    if manifest.get("target_profile") != FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION:
        return False
    requirement = paper.get("position_requirement")
    reasons = requirement.get("blocking_reasons") if isinstance(requirement, Mapping) else None
    blocker = requirement.get("primary_blocker") if isinstance(requirement, Mapping) else None
    return bool(
        _target_operational_active(paper, manifest)
        and paper.get("state") == "PAPER_RUNNING"
        and paper.get("paper_execution") == "RUNNING"
        and paper.get("current_position") == "FLAT"
        and type(paper.get("current_quantity")) is int
        and paper.get("current_quantity") == 0
        and type(paper.get("current_position_quantity")) is int
        and paper.get("current_position_quantity") == 0
        and paper.get("broker_snapshot_position") == "FLAT"
        and type(paper.get("broker_snapshot_position_quantity")) is int
        and paper.get("broker_snapshot_position_quantity") == 0
        and isinstance(requirement, Mapping)
        and requirement.get("required") is True
        and requirement.get("state") == "BLOCKED_FLAT"
        and requirement.get("actual_position") == "FLAT"
        and type(requirement.get("actual_quantity")) is int
        and requirement.get("actual_quantity") == 0
        and isinstance(blocker, str)
        and bool(blocker)
        and isinstance(reasons, (list, tuple))
        and blocker in reasons
    )


def _target_position_blocker(paper: Mapping[str, object]) -> str:
    requirement = paper.get("position_requirement")
    if isinstance(requirement, Mapping):
        blocker = requirement.get("primary_blocker")
        if isinstance(blocker, str) and blocker:
            return blocker
    reason = paper.get("lockout_or_fault_reason")
    if isinstance(reason, str) and reason:
        return reason
    return "PERPETUAL_POSITION_NOT_PROVEN"


def _target_non_operational_flat_proven(
    paper: Mapping[str, object], manifest: Mapping[str, object],
) -> bool:
    """Prove that terminating a bound target cannot strand native exposure."""
    session = paper.get("operational_paper_session")
    return bool(
        _target_paper_identity_matches(paper, manifest)
        and (session is None or (
            isinstance(session, Mapping) and session.get("active") is False
        ))
        and paper.get("current_position") == "FLAT"
        and type(paper.get("current_quantity")) is int
        and paper.get("current_quantity") == 0
        and paper.get("broker_snapshot_position") == "FLAT"
        and type(paper.get("broker_snapshot_position_quantity")) is int
        and paper.get("broker_snapshot_position_quantity") == 0
        and type(paper.get("working_owned_orders")) is int
        and paper.get("working_owned_orders") == 0
        and type(paper.get("working_entry_orders")) is int
        and paper.get("working_entry_orders") == 0
        and paper.get("foreign_activity") is False
        and paper.get("position_snapshot_complete") is True
        and paper.get("order_snapshot_complete") is True
        and paper.get("reconciliation_current") is True
        and paper.get("unresolved_command") is False
        and paper.get("unresolved_native_order") is False
        and paper.get("unresolved_execution") is False
    )


def _cleanup_spawned_target(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float,
    poll_seconds: float,
    pid_probe: Callable[[int], bool],
    port_probe: Callable[[], bool],
    wait: Callable[[float], object],
    monotonic: Callable[[], float],
    target_runtime_pid: int | None = None,
) -> dict[str, object]:
    """Stop one spawned target and prove its launcher, runtime, and port released."""
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Target cleanup timeouts must be positive.")
    deadline = monotonic() + timeout_seconds
    terminate_attempted = False
    kill_attempted = False
    errors: list[str] = []

    def handle_exited() -> bool:
        try:
            return process.poll() is not None
        except (AttributeError, OSError, subprocess.SubprocessError) as error:
            errors.append(type(error).__name__)
            return False

    if not handle_exited():
        terminate_attempted = True
        try:
            process.terminate()
        except (AttributeError, OSError, subprocess.SubprocessError) as error:
            errors.append(type(error).__name__)
        try:
            process.wait(timeout=max(0.001, timeout_seconds / 2.0))
        except subprocess.TimeoutExpired:
            pass
        except (AttributeError, OSError, subprocess.SubprocessError) as error:
            errors.append(type(error).__name__)
    if not handle_exited():
        kill_attempted = True
        try:
            process.kill()
        except (AttributeError, OSError, subprocess.SubprocessError) as error:
            errors.append(type(error).__name__)
        remaining = max(0.001, deadline - monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            pass
        except (AttributeError, OSError, subprocess.SubprocessError) as error:
            errors.append(type(error).__name__)

    handle_is_exited = False
    pid_absent = False
    # A Windows py.exe launcher can exit while its not-yet-bound Python child
    # keeps starting.  Without the published runtime PID, a dead launcher and a
    # momentarily free port do not prove that the target process tree is gone.
    runtime_pid_absent = False
    port_released = False
    while True:
        handle_is_exited = handle_exited()
        try:
            pid_absent = not pid_probe(process.pid)
        except Exception as error:  # noqa: BLE001 - failed proof must remain unproven
            errors.append(type(error).__name__)
            pid_absent = False
        if target_runtime_pid is not None:
            try:
                runtime_pid_absent = not pid_probe(target_runtime_pid)
            except Exception as error:  # noqa: BLE001 - failed proof must remain unproven
                errors.append(type(error).__name__)
                runtime_pid_absent = False
        try:
            port_released = port_probe()
        except Exception as error:  # noqa: BLE001 - failed proof must remain unproven
            errors.append(type(error).__name__)
            port_released = False
        if handle_is_exited and pid_absent and runtime_pid_absent and port_released:
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        wait(min(poll_seconds, max(0.001, remaining)))
    process_dead = handle_is_exited and pid_absent and runtime_pid_absent
    return {
        "target_pid": process.pid,
        "terminate_attempted": terminate_attempted,
        "kill_attempted": kill_attempted,
        "process_handle_exited": handle_is_exited,
        "target_pid_absent": pid_absent,
        "target_runtime_pid": target_runtime_pid,
        "target_runtime_pid_absent": runtime_pid_absent,
        "process_dead": process_dead,
        "control_port_released": port_released,
        "cleanup_proven": process_dead and port_released,
        "errors": list(dict.fromkeys(errors)),
    }


def finalize_stale_target_cleanup(
    runtime_root: str | Path,
    operation_id: str,
    *,
    pid_probe: Callable[[int], bool] = _pid_exists,
    native_probe: Callable[..., Mapping[str, object]] | None = None,
    native_proof_validator: Callable[..., Mapping[str, object]] | None = None,
    timeout_seconds: float = 30.0,
    _test_control_endpoint: tuple[str, int] | None = None,
    _test_attestation_key: bytes | None = None,
) -> dict[str, object]:
    """Explicitly finalize one stale target only with fresh native flat proof.

    The finalizer is deliberately offline and command-incapable.  It preserves
    every ledger byte, holds the control port and operation lock throughout the
    transition, and accepts only two fresh signed AddOn reconciliations showing
    an exact flat Sim101 account with no working or foreign orders.
    """
    if _PROFILE_SWITCH_OPERATION.fullmatch(operation_id) is None:
        raise RuntimeError("PROFILE_SWITCH_CLEANUP_OPERATION_INVALID")
    if timeout_seconds <= 0:
        raise ValueError("Cleanup finalization timeout must be positive.")
    if _test_control_endpoint is None:
        control_host, control_port = "127.0.0.1", 8090
    else:
        control_host, control_port = _test_control_endpoint
        if (
            control_host != "127.0.0.1"
            or type(control_port) is not int
            or not 0 <= control_port <= 65535
        ):
            raise ValueError("Cleanup test control endpoint is invalid.")
    root = Path(runtime_root).resolve()
    operation_root = root / "profile-switch" / "operations" / operation_id
    manifest_path = operation_root / "manifest.json"
    state_path = operation_root / "state.json"
    claim_path = operation_root / "target-launch-claim.json"
    selection_path = _selection_path(root)

    def read_bytes(path: Path, code: str) -> bytes:
        try:
            value = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(code) from exc
        if not value:
            raise RuntimeError(code)
        return value

    def prove_absent(pids: tuple[int, ...]) -> None:
        try:
            results = [pid_probe(pid) for pid in pids]
        except Exception as exc:  # noqa: BLE001 - a failed probe is no proof
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_PID_PROBE_FAILED") from exc
        if any(result is not False for result in results):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_PROCESS_STILL_ACTIVE")

    def attestation_key() -> bytes:
        candidate = _test_attestation_key
        if candidate is None:
            try:
                candidate = LocalPaperSecretProvider().load_key()
            except RuntimeError as exc:
                raise RuntimeError(
                    "PROFILE_SWITCH_CLEANUP_ATTESTATION_KEY_UNAVAILABLE"
                ) from exc
        if type(candidate) is not bytes or len(candidate) < 32:
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_ATTESTATION_KEY_INVALID")
        return candidate

    with profile_switch_operation_lock(manifest_path):
        manifest_bytes = read_bytes(
            manifest_path, "PROFILE_SWITCH_CLEANUP_MANIFEST_UNREADABLE",
        )
        manifest = _manifest(manifest_path)
        if (
            manifest.get("operation_id") != operation_id
            or Path(str(manifest.get("runtime_root"))).resolve() != root
            or manifest_path.resolve()
            != (root / "profile-switch" / "operations" / operation_id / "manifest.json").resolve()
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_MANIFEST_BINDING_INVALID")

        selection_bytes = read_bytes(
            selection_path, "PROFILE_SWITCH_CLEANUP_SELECTION_UNREADABLE",
        )
        selection = _validated_selection(root, required=True)
        assert selection is not None
        requested = selection.get("requested")
        if not isinstance(requested, Mapping) or dict(requested) != {
            "profile": manifest.get("target_profile"),
            "request_id": manifest.get("request_id"),
            "operation_id": operation_id,
            "requested_at": manifest.get("created_at"),
        }:
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_SELECTION_BINDING_INVALID")

        state_bytes = read_bytes(
            state_path, "PROFILE_SWITCH_CLEANUP_STATE_UNREADABLE",
        )
        state = _read_json(state_path)
        claim_bytes = read_bytes(
            claim_path, "PROFILE_SWITCH_CLEANUP_CLAIM_UNREADABLE",
        )
        claim = _read_json(claim_path)
        if (
            set(claim) != {
                "schema", "operation_id", "manifest_sha256", "claimed_at",
                "supervisor_pid",
            }
            or claim.get("schema") != PROFILE_SWITCH_SCHEMA
            or claim.get("operation_id") != operation_id
            or claim.get("manifest_sha256") != manifest.get("manifest_sha256")
            or not isinstance(claim.get("claimed_at"), str)
            or not str(claim.get("claimed_at")).strip()
            or type(claim.get("supervisor_pid")) is not int
            or int(claim["supervisor_pid"]) <= 0
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_CLAIM_BINDING_INVALID")

        cleanup = state.get("target_cleanup")
        binding = state.get("target_runtime_binding")
        paper = state.get("target_paper_status")
        blockers = state.get("blockers")
        target_pid = state.get("target_pid")
        target_runtime_pid = state.get("target_runtime_pid")
        supervisor_pid = claim.get("supervisor_pid")
        source_parent_pid = manifest.get("parent_pid")
        if (
            state.get("schema") != PROFILE_SWITCH_SCHEMA
            or state.get("operation_id") != operation_id
            or state.get("request_id") != manifest.get("request_id")
            or state.get("current_profile") != manifest.get("current_profile")
            or state.get("target_profile") != manifest.get("target_profile")
            or Path(str(state.get("manifest_path"))).resolve() != manifest_path.resolve()
            or not isinstance(cleanup, Mapping)
            or not isinstance(binding, Mapping)
            or not isinstance(paper, Mapping)
            or not isinstance(blockers, list)
            or not all(isinstance(item, str) and item for item in blockers)
            or type(target_pid) is not int
            or target_pid <= 0
            or type(target_runtime_pid) is not int
            or target_runtime_pid <= 0
            or type(supervisor_pid) is not int
            or supervisor_pid <= 0
            or supervisor_pid in {target_pid, target_runtime_pid}
            or type(source_parent_pid) is not int
            or source_parent_pid <= 0
            or source_parent_pid in {supervisor_pid, target_pid, target_runtime_pid}
            or cleanup.get("target_pid") != target_pid
            or cleanup.get("target_runtime_pid") != target_runtime_pid
            or binding.get("pid") != target_runtime_pid
            or type(binding.get("parent_pid")) is not int
            or int(binding["parent_pid"]) <= 0
            or int(binding["parent_pid"]) not in {target_pid, supervisor_pid}
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_STATE_BINDING_INVALID")

        transport = paper.get("transport")
        if not isinstance(transport, Mapping):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_PROVENANCE_INVALID")
        build_fingerprint = transport.get("addon_build_fingerprint")
        build_timestamp = transport.get("addon_build_timestamp")
        source_fingerprint = transport.get("expected_addon_source_fingerprint")
        if (
            transport.get("addon_provenance_valid") is not True
            or transport.get("addon_protocol_version") != ADDON_PROTOCOL_VERSION
            or transport.get("addon_source_fingerprint") != source_fingerprint
            or not isinstance(source_fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", source_fingerprint) is None
            or not isinstance(build_fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", build_fingerprint) is None
            or not isinstance(build_timestamp, str)
            or not build_timestamp.strip()
            or type(transport.get("commands_sent")) is not int
            or transport.get("commands_sent") != 0
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_PROVENANCE_INVALID")

        continuity = read_risk_continuity_artifact(
            manifest_path.with_name("risk-continuity.json"),
            operation_id=operation_id,
            target_profile=str(manifest.get("target_profile")),
        )
        seed_artifact: Mapping[str, object] | None = None
        seed_proof: Mapping[str, object] | None = None
        if manifest.get("perpetual_startup_seed_required") is True:
            seed_path = Path(
                str(manifest.get("perpetual_startup_seed_path") or ""),
            ).resolve()
            seed_proof_path = Path(
                str(manifest.get("perpetual_startup_seed_proof_path") or ""),
            ).resolve()
            if (
                seed_path.parent != operation_root.resolve()
                or seed_proof_path.parent != operation_root.resolve()
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_SEED_BINDING_INVALID")
            seed_artifact = read_perpetual_startup_seed_artifact(
                seed_path, operation_id=operation_id,
            )
            seed_proof = read_perpetual_startup_seed_proof(
                seed_proof_path, artifact=seed_artifact,
                operation_id=operation_id,
            )
            if (
                state.get("perpetual_startup_seed_artifact_sha256")
                != seed_artifact.get("artifact_sha256")
                or state.get("perpetual_startup_seed_proof_sha256")
                != seed_proof.get("proof_sha256")
                or seed_proof.get("manifest_sha256")
                != manifest.get("manifest_sha256")
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_SEED_BINDING_INVALID")
        if not _target_binding_matches(
            binding,
            manifest,
            continuity,
            target_pid=target_pid,
            seed_artifact=seed_artifact,
            seed_proof=seed_proof,
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_RUNTIME_BINDING_INVALID")

        all_pids = tuple(sorted({
            source_parent_pid,
            supervisor_pid,
            target_pid,
            target_runtime_pid,
            int(binding["parent_pid"]),
        }))

        if native_probe is None or native_proof_validator is None:
            from .ninjatrader_reconciliation_probe import (
                probe_flat_sim101_reconciliation,
                validate_flat_sim101_reconciliation_proof,
            )

            native_probe = native_probe or probe_flat_sim101_reconciliation
            native_proof_validator = (
                native_proof_validator
                or validate_flat_sim101_reconciliation_proof
            )

        removable_cleanup_blockers = {
            "TARGET_PROCESS_EXIT_UNPROVEN",
            "TARGET_CONTROL_PORT_RELEASE_UNPROVEN",
            "TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN",
            "TARGET_CLEANUP_UNPROVEN",
        }
        late = cleanup.get("late_revalidation")
        if state.get("stage") == "BLOCKED_SAFE" and isinstance(late, Mapping):
            proof_path = Path(str(late.get("native_proof_path") or "")).resolve()
            proof_file = read_bytes(
                proof_path, "PROFILE_SWITCH_CLEANUP_NATIVE_PROOF_UNREADABLE",
            )
            try:
                stored_proof = json.loads(proof_file.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_NATIVE_PROOF_INVALID") from exc
            validated_stored_proof = native_proof_validator(
                stored_proof,
                expected_policy_hash=str(manifest["paper_policy_hash"]),
                expected_risk_hash=str(manifest["risk_profile_hash"]),
                expected_at=(
                    stored_proof.get("completed_at")
                    if isinstance(stored_proof, Mapping) else None
                ),
            )
            prior_attempt = cleanup.get("prior_attempt")
            cleanup_attestation = late.get("cleanup_attestation")
            if not isinstance(prior_attempt, Mapping) or not isinstance(
                cleanup_attestation, Mapping,
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_NATIVE_PROOF_INVALID")
            terminal_late_core = dict(late)
            terminal_late_core.pop("cleanup_attestation", None)
            terminal_cleanup_core = dict(cleanup)
            terminal_cleanup_core["late_revalidation"] = terminal_late_core
            terminal_state_core = dict(state)
            terminal_state_core["target_cleanup"] = terminal_cleanup_core
            terminal_state_core_sha256 = hashlib.sha256(
                _canonical(terminal_state_core),
            ).hexdigest()
            expected_attestation = {
                "schema": "lane-iii-profile-switch-late-cleanup-attestation-v1",
                "operation_id": operation_id,
                "request_id": manifest.get("request_id"),
                "manifest_sha256": manifest.get("manifest_sha256"),
                "source_state_sha256": late.get("source_state_sha256"),
                "selection_sha256": hashlib.sha256(selection_bytes).hexdigest(),
                "claim_sha256": hashlib.sha256(claim_bytes).hexdigest(),
                "prior_attempt_sha256": hashlib.sha256(
                    _canonical(prior_attempt),
                ).hexdigest(),
                "terminal_state_core_sha256": terminal_state_core_sha256,
                "probed_absent_pids": list(all_pids),
                "pid_probe_rounds": 2,
                "control_host": control_host,
                "control_port_number": control_port,
                "native_proof_path": str(proof_path),
                "native_proof_hash": late.get("native_proof_hash"),
                "native_proof_file_sha256": late.get("native_proof_file_sha256"),
                "revalidated_at": late.get("revalidated_at"),
                "final_cleanup": {
                    "target_pid_absent": True,
                    "target_runtime_pid_absent": True,
                    "process_dead": True,
                    "control_port_released": True,
                    "native_exposure_absent": True,
                    "cleanup_proven": True,
                    "safe_terminal_proven": True,
                },
                "retained_blockers": list(blockers),
            }
            unsigned_attestation = dict(cleanup_attestation)
            supplied_attestation_signature = unsigned_attestation.pop(
                "signature", None,
            )
            signed_attestation = dict(unsigned_attestation)
            signed_attestation["signature"] = supplied_attestation_signature
            expected_late_fields = {
                "schema", "revalidated_at", "source_state_sha256",
                "selection_sha256", "claim_sha256", "manifest_sha256",
                "probed_absent_pids", "pid_probe_rounds", "control_port",
                "control_host", "control_port_number",
                "control_port_lease_held_through_commit", "native_proof_path",
                "native_proof_hash", "native_proof_file_sha256",
                "native_session_count", "native_commands_sent",
                "native_exposure_absent", "cleanup_attestation",
            }
            expected_cleanup = {
                **dict(prior_attempt),
                "target_pid_absent": True,
                "target_runtime_pid_absent": True,
                "process_dead": True,
                "control_port_released": True,
                "native_exposure_absent": True,
                "cleanup_proven": True,
                "safe_terminal_proven": True,
                "prior_attempt": dict(prior_attempt),
                "late_revalidation": dict(late),
            }
            if (
                proof_path.parent != operation_root.resolve()
                or not proof_path.name.startswith("late-cleanup-native-proof-")
                or proof_path.suffix != ".json"
                or not isinstance(validated_stored_proof, Mapping)
                or validated_stored_proof.get("proof_hash")
                != late.get("native_proof_hash")
                or hashlib.sha256(proof_file).hexdigest()
                != late.get("native_proof_file_sha256")
                or cleanup.get("cleanup_proven") is not True
                or cleanup.get("safe_terminal_proven") is not True
                or cleanup.get("native_exposure_absent") is not True
                or state.get("in_progress") is not False
                or late.get("schema")
                != "lane-iii-profile-switch-late-cleanup-v1"
                or late.get("manifest_sha256") != manifest.get("manifest_sha256")
                or late.get("selection_sha256")
                != hashlib.sha256(selection_bytes).hexdigest()
                or late.get("claim_sha256")
                != hashlib.sha256(claim_bytes).hexdigest()
                or not isinstance(late.get("source_state_sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", str(late.get("source_state_sha256"))) is None
                or late.get("probed_absent_pids") != list(all_pids)
                or late.get("pid_probe_rounds") != 2
                or late.get("control_port_lease_held_through_commit") is not True
                or not isinstance(late.get("control_host"), str)
                or not isinstance(late.get("control_port_number"), int)
                or late.get("control_port")
                != f"{late.get('control_host')}:{late.get('control_port_number')}"
                or late.get("native_session_count") != 2
                or late.get("native_commands_sent") != 0
                or late.get("native_exposure_absent") is not True
                or state.get("target_cleanup_revalidated_at")
                != late.get("revalidated_at")
                or set(late) != expected_late_fields
                or dict(cleanup) != expected_cleanup
                or any(item in removable_cleanup_blockers for item in blockers)
                or unsigned_attestation != expected_attestation
                or not isinstance(supplied_attestation_signature, str)
                or not verify_signature(attestation_key(), signed_attestation)
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_NATIVE_PROOF_INVALID")
            return state
        if (
            state.get("stage") != "TARGET_CLEANUP_UNPROVEN"
            or state.get("in_progress") is not True
            or "TARGET_CLEANUP_UNPROVEN" not in blockers
            or cleanup.get("cleanup_proven") is not False
            or cleanup.get("safe_terminal_proven") is not False
            or cleanup.get("process_handle_exited") is not True
            or paper.get("state") != "READY_DISARMED"
            or paper.get("paper_execution") != "DISARMED"
            or paper.get("session_armed_state") != "DISARMED"
            or paper.get("entry_owner") != "NONE"
            or paper.get("protective_stop_state") != "NONE"
            or not _target_non_operational_flat_proven(paper, manifest)
        ):
            raise RuntimeError("PROFILE_SWITCH_CLEANUP_NOT_ELIGIBLE")

        with _control_port_lease(control_host, control_port):
            prove_absent(all_pids)
            native_proof = native_probe(
                paper_policy_hash=str(manifest["paper_policy_hash"]),
                risk_profile_hash=str(manifest["risk_profile_hash"]),
                timeout_seconds=timeout_seconds,
            )
            validated_native_proof = native_proof_validator(
                native_proof,
                expected_policy_hash=str(manifest["paper_policy_hash"]),
                expected_risk_hash=str(manifest["risk_profile_hash"]),
                expected_at=_utc_now(),
            )
            if (
                not isinstance(validated_native_proof, Mapping)
                or validated_native_proof.get("status") != "PASS"
                or validated_native_proof.get("commands_sent") != 0
                or validated_native_proof.get("session_count") != 2
                or not isinstance(validated_native_proof.get("proof_hash"), str)
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_NATIVE_PROOF_INVALID")

            proof_path = operation_root / (
                "late-cleanup-native-proof-" + uuid4().hex + ".json"
            )
            _write_exclusive_json(proof_path, validated_native_proof)
            proof_file_sha256 = hashlib.sha256(proof_path.read_bytes()).hexdigest()

            # Revalidate every mutable input and every PID after the bounded
            # native observation, while the operation and control-port leases
            # are still held. Any change leaves the original state untouched.
            if (
                read_bytes(manifest_path, "PROFILE_SWITCH_CLEANUP_MANIFEST_UNREADABLE")
                != manifest_bytes
                or read_bytes(selection_path, "PROFILE_SWITCH_CLEANUP_SELECTION_UNREADABLE")
                != selection_bytes
                or read_bytes(state_path, "PROFILE_SWITCH_CLEANUP_STATE_UNREADABLE")
                != state_bytes
                or read_bytes(claim_path, "PROFILE_SWITCH_CLEANUP_CLAIM_UNREADABLE")
                != claim_bytes
                or _manifest(manifest_path) != manifest
                or _validated_selection(root, required=True) != selection
                or _read_json(state_path) != state
                or _read_json(claim_path) != claim
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_INPUT_CHANGED")
            prove_absent(all_pids)

            # The PID probe is an injected boundary in tests and an external
            # process observation in production. Recheck every captured input
            # after the final probe round and build the replacement from that
            # captured value; never reread/merge a potentially changed file.
            if (
                read_bytes(manifest_path, "PROFILE_SWITCH_CLEANUP_MANIFEST_UNREADABLE")
                != manifest_bytes
                or read_bytes(selection_path, "PROFILE_SWITCH_CLEANUP_SELECTION_UNREADABLE")
                != selection_bytes
                or read_bytes(state_path, "PROFILE_SWITCH_CLEANUP_STATE_UNREADABLE")
                != state_bytes
                or read_bytes(claim_path, "PROFILE_SWITCH_CLEANUP_CLAIM_UNREADABLE")
                != claim_bytes
            ):
                raise RuntimeError("PROFILE_SWITCH_CLEANUP_INPUT_CHANGED")

            revalidated_at = _utc_now()
            prior_cleanup = dict(cleanup)
            retained = [
                item for item in blockers
                if item not in removable_cleanup_blockers
            ]
            late_revalidation: dict[str, object] = {
                "schema": "lane-iii-profile-switch-late-cleanup-v1",
                "revalidated_at": revalidated_at,
                "source_state_sha256": hashlib.sha256(state_bytes).hexdigest(),
                "selection_sha256": hashlib.sha256(selection_bytes).hexdigest(),
                "claim_sha256": hashlib.sha256(claim_bytes).hexdigest(),
                "manifest_sha256": manifest.get("manifest_sha256"),
                "probed_absent_pids": list(all_pids),
                "pid_probe_rounds": 2,
                "control_port": f"{control_host}:{control_port}",
                "control_host": control_host,
                "control_port_number": control_port,
                "control_port_lease_held_through_commit": True,
                "native_proof_path": str(proof_path),
                "native_proof_hash": validated_native_proof["proof_hash"],
                "native_proof_file_sha256": proof_file_sha256,
                "native_session_count": 2,
                "native_commands_sent": 0,
                "native_exposure_absent": True,
            }
            recovered_cleanup = {
                **prior_cleanup,
                "target_pid_absent": True,
                "target_runtime_pid_absent": True,
                "process_dead": True,
                "control_port_released": True,
                "native_exposure_absent": True,
                "cleanup_proven": True,
                "safe_terminal_proven": True,
                "prior_attempt": prior_cleanup,
                "late_revalidation": late_revalidation,
            }
            recovered_state = dict(state)
            recovered_state.update({
                "stage": "BLOCKED_SAFE",
                "blockers": retained,
                "target_cleanup": recovered_cleanup,
                "target_cleanup_revalidated_at": revalidated_at,
                "updated_at": _utc_now(),
                "in_progress": False,
            })
            terminal_state_core_sha256 = hashlib.sha256(
                _canonical(recovered_state),
            ).hexdigest()
            cleanup_attestation: dict[str, object] = {
                "schema": "lane-iii-profile-switch-late-cleanup-attestation-v1",
                "operation_id": operation_id,
                "request_id": manifest.get("request_id"),
                "manifest_sha256": manifest.get("manifest_sha256"),
                "source_state_sha256": late_revalidation["source_state_sha256"],
                "selection_sha256": late_revalidation["selection_sha256"],
                "claim_sha256": late_revalidation["claim_sha256"],
                "prior_attempt_sha256": hashlib.sha256(
                    _canonical(prior_cleanup),
                ).hexdigest(),
                "terminal_state_core_sha256": terminal_state_core_sha256,
                "probed_absent_pids": list(all_pids),
                "pid_probe_rounds": 2,
                "control_host": control_host,
                "control_port_number": control_port,
                "native_proof_path": str(proof_path),
                "native_proof_hash": validated_native_proof["proof_hash"],
                "native_proof_file_sha256": proof_file_sha256,
                "revalidated_at": revalidated_at,
                "final_cleanup": {
                    "target_pid_absent": True,
                    "target_runtime_pid_absent": True,
                    "process_dead": True,
                    "control_port_released": True,
                    "native_exposure_absent": True,
                    "cleanup_proven": True,
                    "safe_terminal_proven": True,
                },
                "retained_blockers": retained,
            }
            cleanup_attestation["signature"] = sign_payload(
                attestation_key(), cleanup_attestation,
            )
            # The core hash above intentionally excludes this nested
            # attestation to avoid a circular digest. The HMAC binds that exact
            # full-state core, and then the attestation is the only insertion.
            late_revalidation["cleanup_attestation"] = cleanup_attestation
            _atomic_json(state_path, recovered_state)
            _append_jsonl(operation_root / "supervisor-audit.jsonl", {
                "schema": PROFILE_SWITCH_SCHEMA,
                "event": "TARGET_CLEANUP_LATE_REVALIDATED",
                "recorded_at": _utc_now(),
                "operation_id": recovered_state.get("operation_id"),
                "stage": recovered_state.get("stage"),
                "blockers": recovered_state.get("blockers"),
            })
            return recovered_state


def _claim_target_launch(manifest_path: Path, manifest: Mapping[str, object]) -> bool:
    claim_path = manifest_path.with_name("target-launch-claim.json")
    claim = {
        "schema": PROFILE_SWITCH_SCHEMA,
        "operation_id": manifest["operation_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "claimed_at": _utc_now(),
        "supervisor_pid": os.getpid(),
    }
    try:
        descriptor = os.open(claim_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return False
    try:
        os.write(descriptor, json.dumps(claim, sort_keys=True, indent=2).encode("utf-8") + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


def _launch_child(
    command: list[str], *, cwd: Path, environment: Mapping[str, str], output: object,
) -> subprocess.Popen[bytes]:
    """Use the production Windows child-process flags for a fixed command."""
    return subprocess.Popen(
        command,
        cwd=cwd,
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=subprocess.STDOUT,
        close_fds=True,
        creationflags=(
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        ) if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )


def _windows_profile_switch_notice(message: str) -> None:
    """Use the existing detached supervisor as a one-shot failure surface."""
    if os.name != "nt":  # pragma: no cover - production launcher is Windows-only
        return
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(None, message, "BeezConsole profile switch blocked", 0x10)
    except (AttributeError, OSError):
        # The durable state/audit remain authoritative if the desktop cannot
        # display a notification (for example, a non-interactive session).
        return


def _exact_verification_report(
    paths: VerificationPaths, verification_id: str,
) -> dict[str, object]:
    """Read only the terminal artifact produced by one verifier process."""
    candidates = sorted(
        paths.reports.glob(f"*-{verification_id}.json"), reverse=True,
    )
    if paths.latest.exists():
        candidates.append(paths.latest)
    for candidate in candidates:
        try:
            report = _read_json(candidate)
        except (OSError, UnicodeError, ValueError, RuntimeError):
            continue
        if report.get("verification_id") == verification_id:
            return report
    raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_VERIFIER_NO_REPORT")


def _bounded_full_verification(
    ledger_path: Path,
    audit_root: Path,
    *,
    timeout_seconds: float,
    python_executable: str | Path | None = None,
    popen: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen,
    cancellation_grace_seconds: float = 5.0,
) -> dict[str, object]:
    """Run one process-owned full scan and refuse an unbounded handoff wait."""
    if timeout_seconds <= 0 or cancellation_grace_seconds <= 0:
        raise ValueError("Verifier timeouts must be positive.")
    ledger = Path(ledger_path).resolve()
    paths = VerificationPaths(Path(audit_root).resolve())
    interpreter = str(python_executable or sys.executable)
    verification_id = f"lv-{uuid4().hex}"
    command = [
        interpreter, "-m", "src.l3g_paper.verification", "run",
        "--ledger", str(ledger), "--audit-root", str(paths.root),
        "--mode", "full", "--verification-id", verification_id,
    ]
    paths.reports.mkdir(parents=True, exist_ok=True)
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    with paths.log(verification_id).open("ab") as output:
        process = popen(
            command,
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            close_fds=True,
            creationflags=flags,
            start_new_session=os.name != "nt",
        )
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as timeout:
            cancel_path = paths.cancel(verification_id)
            try:
                descriptor = os.open(
                    cancel_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
                )
            except FileExistsError:
                pass
            else:
                with os.fdopen(descriptor, "wb") as marker:
                    marker.write(b"cancel requested by profile-switch supervisor\n")
                    marker.flush()
                    os.fsync(marker.fileno())
            try:
                process.wait(timeout=cancellation_grace_seconds)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=cancellation_grace_seconds)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=cancellation_grace_seconds)
                # Publish an exact interrupted artifact and release only the
                # dead verifier's operational lock. Existing ledger evidence
                # and verifier history remain untouched.
                LocalLedgerVerificationController(
                    ledger, paths.root, python_executable=interpreter,
                ).status()
            raise RuntimeError(
                "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_TIMEOUT",
            ) from timeout
    return _exact_verification_report(paths, verification_id)


@_operation_locked
def supervise(
    manifest_path: Path,
    parent_pid: int,
    *,
    timeout_seconds: float = 1200.0,
    poll_seconds: float = 0.25,
    pid_probe: Callable[[int], bool] = _pid_exists,
    port_probe: Callable[[], bool] = _control_port_released,
    wait: Callable[[float], object] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    http_json: Callable[..., dict[str, object]] = _http_json,
    launch_child: Callable[..., subprocess.Popen[bytes]] = _launch_child,
    notify_operator: Callable[[str], None] | None = None,
    api_base: str = "http://127.0.0.1:8090",
    seed_verification_timeout_seconds: float = 600.0,
    target_cleanup_timeout_seconds: float | None = None,
    verify_source: (
        Callable[[Path, Path], Mapping[str, object]] | None
    ) = None,
) -> int:
    manifest = _manifest(manifest_path)
    state_path = manifest_path.with_name("state.json")
    try:
        inherited_state = _read_json(state_path)
    except (OSError, UnicodeError, ValueError, RuntimeError):
        inherited_state = None
    if (
        isinstance(inherited_state, Mapping)
        and inherited_state.get("schema") == PROFILE_SWITCH_SCHEMA
        and inherited_state.get("operation_id") == manifest.get("operation_id")
        and inherited_state.get("stage") == "TARGET_CLEANUP_UNPROVEN"
    ):
        # Only the explicit offline finalizer may add the fresh native proof
        # needed to close this state. A replayed supervisor must leave both the
        # projection and its audit bytes untouched.
        return 12

    def notify(blockers: list[str]) -> None:
        if notify_operator is None:
            return
        message = (
            f"Requested profile {manifest.get('target_profile')} did not become a proven running selection.\n\n"
            + "Blocker: " + ", ".join(blockers)
            + "\n\nNo automatic retry or fallback profile was started."
        )
        try:
            notify_operator(message)
        except Exception:  # noqa: BLE001 - notification cannot change the durable outcome
            return

    if type(manifest.get("parent_pid")) is not int or manifest.get("parent_pid") != parent_pid:
        blockers = ["PROFILE_SWITCH_PARENT_PID_MISMATCH"]
        _state_update(
            state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
            blockers=blockers,
        )
        notify(blockers)
        return 2
    if (
        timeout_seconds <= 0
        or poll_seconds <= 0
        or seed_verification_timeout_seconds <= 0
        or (
            target_cleanup_timeout_seconds is not None
            and target_cleanup_timeout_seconds <= 0
        )
    ):
        raise ValueError("Supervisor timeouts must be positive.")
    target_cleanup_timeout = (
        min(10.0, timeout_seconds)
        if target_cleanup_timeout_seconds is None
        else target_cleanup_timeout_seconds
    )
    deadline = monotonic() + timeout_seconds
    state: dict[str, object] = {}
    receipt: Mapping[str, object] | None = None
    process_released = False
    resources_released = False
    while monotonic() < deadline:
        try:
            state = _read_json(state_path)
        except (OSError, UnicodeError, ValueError, RuntimeError):
            # The projection itself is evidence.  If it is unreadable, do not
            # replace it in an attempt to report a cleaner failure; retain the
            # damaged bytes and append the independently readable blocker.
            _append_jsonl(manifest_path.with_name("supervisor-audit.jsonl"), {
                "schema": PROFILE_SWITCH_SCHEMA,
                "event": "SUPERVISOR_BLOCKED",
                "recorded_at": _utc_now(),
                "operation_id": manifest["operation_id"],
                "stage": "BLOCKED_SAFE",
                "blockers": ["PROFILE_SWITCH_STATE_INVALID"],
            })
            notify(["PROFILE_SWITCH_STATE_INVALID"])
            return 3
        if (
            state.get("schema") != PROFILE_SWITCH_SCHEMA
            or state.get("operation_id") != manifest.get("operation_id")
            or state.get("current_profile") != manifest.get("current_profile")
            or state.get("target_profile") != manifest.get("target_profile")
            or Path(str(state.get("manifest_path"))).resolve() != manifest_path.resolve()
        ):
            blockers = ["PROFILE_SWITCH_STATE_BINDING_MISMATCH"]
            _state_update(
                state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
                blockers=blockers,
            )
            notify(blockers)
            return 3
        if state.get("stage") == "RUNNING":
            return 0
        if state.get("stage") in {"BLOCKED_SAFE", "RUNNING_SELECTION_PERSISTENCE_FAILED"}:
            return 10
        candidate = state.get("shutdown_receipt")
        receipt = candidate if isinstance(candidate, Mapping) and _clean_shutdown_receipt(candidate) else None
        process_released = not pid_probe(parent_pid)
        resources_released = port_probe()
        if state.get("stage") == "CURRENT_PROFILE_CLOSED" and receipt is not None and process_released and resources_released:
            break
        wait(min(poll_seconds, max(0.001, deadline - monotonic())))
    else:
        blockers: list[str] = []
        if receipt is None or state.get("stage") != "CURRENT_PROFILE_CLOSED":
            blockers.append("CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN")
        if not process_released:
            blockers.append("CURRENT_PROCESS_DID_NOT_EXIT")
        if not resources_released:
            blockers.append("CURRENT_PROFILE_RESOURCES_NOT_RELEASED")
        _state_update(state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE", blockers=blockers)
        notify(blockers)
        return 3
    assert receipt is not None
    continuity_path = manifest_path.with_name("risk-continuity.json")
    try:
        continuity = read_risk_continuity_artifact(
            continuity_path,
            operation_id=str(manifest["operation_id"]),
            target_profile=str(manifest["target_profile"]),
        )
    except RuntimeError as error:
        blockers = [str(error)]
        _state_update(
            state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
            blockers=blockers,
        )
        notify(blockers)
        return 4
    if state.get("risk_continuity_artifact_sha256") != continuity.get("artifact_sha256"):
        blockers = ["RISK_CONTINUITY_STATE_BINDING_MISMATCH"]
        _state_update(
            state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
            blockers=blockers,
        )
        notify(blockers)
        return 4
    if continuity.get("source_profile") != manifest.get("current_profile"):
        blockers = ["RISK_CONTINUITY_SOURCE_PROFILE_MISMATCH"]
        _state_update(
            state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
            blockers=blockers,
        )
        notify(blockers)
        return 4
    source_snapshot = continuity.get("snapshot")
    source_ledger = source_snapshot.get("source_ledger") if isinstance(source_snapshot, Mapping) else None
    source_boundary = (
        {key: value for key, value in source_ledger.items() if key != "coverage_complete"}
        if isinstance(source_ledger, Mapping) else None
    )
    if source_boundary != receipt.get("risk_continuity_boundary"):
        blockers = ["RISK_CONTINUITY_SOURCE_BOUNDARY_MISMATCH"]
        _state_update(
            state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE",
            blockers=blockers,
        )
        notify(blockers)
        return 4
    seed_artifact: Mapping[str, object] | None = None
    seed_proof: Mapping[str, object] | None = None
    if manifest.get("perpetual_startup_seed_required") is True:
        try:
            seed_path = Path(
                str(manifest.get("perpetual_startup_seed_path") or ""),
            ).resolve()
            proof_path = Path(
                str(manifest.get("perpetual_startup_seed_proof_path") or ""),
            ).resolve()
            source_path = Path(
                str(manifest.get("source_ledger_path") or ""),
            ).resolve()
            source_audit = Path(
                str(manifest.get("source_audit_root") or ""),
            ).resolve()
            if (
                seed_path.parent != manifest_path.parent
                or proof_path.parent != manifest_path.parent
                or proof_path.exists()
                or not source_path.is_file()
                or not source_audit.is_dir()
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_PATH_INVALID")
            seed_artifact = read_perpetual_startup_seed_artifact(
                seed_path,
                operation_id=str(manifest["operation_id"]),
                expected_at=_utc_now(),
            )
            seed_core = seed_artifact["core"]
            seed_source = seed_core["source_ledger"]  # type: ignore[index]
            if (
                seed_source.get("path") != str(source_path)
                or seed_source.get("ledger_identity")
                != manifest.get("source_ledger_identity")
                or seed_source.get("ledger_epoch")
                != manifest.get("source_ledger_epoch")
                or state.get("perpetual_startup_seed_artifact_sha256")
                != seed_artifact.get("artifact_sha256")
            ):
                raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_BINDING_MISMATCH")
            _state_update(
                state_path,
                "PERPETUAL_STARTUP_SEED_FULL_VERIFICATION_STARTED",
                stage="VERIFYING_STARTUP_SEED",
                blockers=[],
            )
            verification = (
                verify_source(source_path, source_audit)
                if verify_source is not None
                else _bounded_full_verification(
                    source_path,
                    source_audit,
                    timeout_seconds=seed_verification_timeout_seconds,
                )
            )
            # A full scan that crossed into the next UTC five-minute bucket
            # cannot bless an older startup direction.
            seed_artifact = read_perpetual_startup_seed_artifact(
                seed_path,
                operation_id=str(manifest["operation_id"]),
                expected_at=_utc_now(),
            )
            seed_proof = write_perpetual_startup_seed_proof(
                proof_path,
                artifact=seed_artifact,
                manifest_sha256=str(manifest["manifest_sha256"]),
                shutdown_receipt=receipt,
                verification_report=verification,
            )
            seed_proof = read_perpetual_startup_seed_proof(
                proof_path,
                artifact=seed_artifact,
                operation_id=str(manifest["operation_id"]),
                expected_at=_utc_now(),
            )
            _state_update(
                state_path,
                "PERPETUAL_STARTUP_SEED_FULLY_VERIFIED",
                perpetual_startup_seed_proof_path=str(proof_path),
                perpetual_startup_seed_proof_sha256=seed_proof["proof_sha256"],
                perpetual_startup_seed_source_verification_id=(
                    seed_proof["verification"]["verification_id"]  # type: ignore[index]
                ),
            )
        except Exception as error:
            blocker = str(error)
            blockers = [
                blocker
                if blocker and blocker == blocker.upper()
                else "PERPETUAL_STARTUP_SEED_FULL_VERIFICATION_FAILED",
            ]
            _state_update(
                state_path,
                "SUPERVISOR_BLOCKED",
                stage="BLOCKED_SAFE",
                blockers=blockers,
            )
            notify(blockers)
            return 4
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
        blockers = ["TARGET_RUNTIME_PATH_VALIDATION_FAILED"]
        _state_update(state_path, "SUPERVISOR_BLOCKED", stage="BLOCKED_SAFE", blockers=blockers)
        notify(blockers)
        return 4
    if not _claim_target_launch(manifest_path, manifest):
        _append_jsonl(manifest_path.with_name("supervisor-audit.jsonl"), {
            "schema": PROFILE_SWITCH_SCHEMA,
            "event": "DUPLICATE_TARGET_LAUNCH_REFUSED",
            "recorded_at": _utc_now(),
            "operation_id": manifest["operation_id"],
            "stage": _read_json(state_path).get("stage"),
            "blockers": ["TARGET_LAUNCH_ALREADY_CLAIMED"],
        })
        return 10
    _state_update(state_path, "TARGET_LAUNCH_CLAIMED", stage="STARTING_TARGET", blockers=[])
    environment = dict(os.environ)
    for variable in (
        "BEELZEBUB_PERPETUAL_STARTUP_SEED_REQUIRED",
        "BEELZEBUB_PERPETUAL_STARTUP_SEED_PATH",
        "BEELZEBUB_PERPETUAL_STARTUP_SEED_PROOF_PATH",
        "BEELZEBUB_PROFILE_SWITCH_MANIFEST_SHA256",
        "BEELZEBUB_PROFILE_SWITCH_MANIFEST_PATH",
    ):
        environment.pop(variable, None)
    environment.update({
        "BEELZEBUB_L3G_PAPER_LEDGER": str(ledger_path),
        "BEELZEBUB_LEDGER_AUDIT_ROOT": str(audit_root),
        "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": str(manifest["ledger_epoch"]),
        "BEELZEBUB_L3G_PAPER_PROFILE": str(manifest["target_profile"]),
        "BEELZEBUB_PROFILE_SWITCH_ROOT": str(runtime_root),
        "BEELZEBUB_PROFILE_SWITCH_OPERATION": str(manifest["operation_id"]),
        "BEELZEBUB_RISK_CONTINUITY_PATH": str(continuity_path),
        "BEELZEBUB_GIT_SHA": str(manifest["git_sha"]),
    })
    if seed_artifact is not None and seed_proof is not None:
        environment.update({
            "BEELZEBUB_PERPETUAL_STARTUP_SEED_REQUIRED": "1",
            "BEELZEBUB_PERPETUAL_STARTUP_SEED_PATH": str(
                manifest["perpetual_startup_seed_path"],
            ),
            "BEELZEBUB_PERPETUAL_STARTUP_SEED_PROOF_PATH": str(
                manifest["perpetual_startup_seed_proof_path"],
            ),
            "BEELZEBUB_PROFILE_SWITCH_MANIFEST_SHA256": str(
                manifest["manifest_sha256"],
            ),
            "BEELZEBUB_PROFILE_SWITCH_MANIFEST_PATH": str(manifest_path),
        })
    backend_log = manifest_path.with_name("target-backend.log")
    try:
        with backend_log.open("ab") as stream:
            process = launch_child(
                [str(python), str(project_root / "main.py"), "copy-control-center", "--with-watcher"],
                cwd=project_root,
                environment=environment,
                output=stream,
            )
    except (OSError, subprocess.SubprocessError):
        blockers = ["TARGET_PROCESS_CREATION_FAILED"]
        _state_update(state_path, "TARGET_FAILED", stage="BLOCKED_SAFE", blockers=blockers)
        notify(blockers)
        return 5
    _state_update(
        state_path, "TARGET_PROCESS_CREATED", stage="TARGET_PROCESS_CREATED",
        target_pid=process.pid, target_process_created_at=_utc_now(),
    )
    base = api_base

    def block_after_target_cleanup(
        blockers: list[str],
        *,
        safe_exit_code: int,
        safe_state_allowed: bool,
        binding: Mapping[str, object] | None = None,
        auto: Mapping[str, object] | None = None,
        paper: Mapping[str, object] | None = None,
    ) -> int:
        cleanup = _cleanup_spawned_target(
            process,
            timeout_seconds=target_cleanup_timeout,
            poll_seconds=poll_seconds,
            pid_probe=pid_probe,
            port_probe=port_probe,
            wait=wait,
            monotonic=monotonic,
            target_runtime_pid=(
                int(binding["pid"])
                if binding is not None and type(binding.get("pid")) is int
                else None
            ),
        )
        values = list(dict.fromkeys(blockers))
        if cleanup.get("process_dead") is not True:
            values.append("TARGET_PROCESS_EXIT_UNPROVEN")
        if cleanup.get("control_port_released") is not True:
            values.append("TARGET_CONTROL_PORT_RELEASE_UNPROVEN")
        cleanup["native_exposure_absent"] = safe_state_allowed
        cleanup_proven = (
            cleanup.get("cleanup_proven") is True and safe_state_allowed
        )
        cleanup["safe_terminal_proven"] = cleanup_proven
        if not safe_state_allowed:
            values.append("TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN")
        if not cleanup_proven:
            values.append("TARGET_CLEANUP_UNPROVEN")
        updates: dict[str, object] = {
            "stage": "BLOCKED_SAFE" if cleanup_proven else "TARGET_CLEANUP_UNPROVEN",
            "blockers": list(dict.fromkeys(values)),
            "target_pid": process.pid,
            "target_cleanup": cleanup,
        }
        if binding is not None:
            updates["target_runtime_binding"] = dict(binding)
            if type(binding.get("pid")) is int:
                updates["target_runtime_pid"] = binding["pid"]
        if auto is not None:
            updates["target_autostart"] = dict(auto)
        if paper is not None:
            updates["target_paper_status"] = dict(paper)
        _state_update(
            state_path,
            "TARGET_BLOCKED" if cleanup_proven else "TARGET_CLEANUP_UNPROVEN",
            **updates,
        )
        notify(updates["blockers"])  # type: ignore[arg-type]
        return safe_exit_code if cleanup_proven else 12

    # Shutdown, target binding, and auto-start are separately bounded stages.
    # A single shared deadline could expire while the target's own guarded
    # verifier/position-proof workflow was still legally in progress, leaving
    # a false terminal BLOCKED_SAFE projection beside a later successful run.
    deadline = monotonic() + timeout_seconds
    binding: dict[str, object] = {}
    while monotonic() < deadline:
        if process.poll() is not None:
            return block_after_target_cleanup(
                ["TARGET_PROCESS_EXITED_DURING_STARTUP"], safe_exit_code=5,
                safe_state_allowed=True,
            )
        try:
            candidate_binding = http_json(base + "/api/runtime-binding")
            if _target_binding_matches(
                candidate_binding,
                manifest,
                continuity,
                target_pid=process.pid,
                seed_artifact=seed_artifact,
                seed_proof=seed_proof,
            ):
                binding = candidate_binding
                break
        except (OSError, ValueError, RuntimeError, HTTPError, URLError):
            pass
        wait(min(0.5, max(0.001, deadline - monotonic())))
    else:
        return block_after_target_cleanup(
            ["TARGET_RUNTIME_BINDING_TIMEOUT"], safe_exit_code=6,
            safe_state_allowed=True,
        )
    pinned_target_binding = dict(binding)
    _state_update(
        state_path,
        "TARGET_AUTOSTARTING",
        stage="AUTOSTARTING_TARGET",
        target_pid=process.pid,
        target_runtime_pid=binding["pid"],
        target_runtime_binding=binding,
    )
    deadline = monotonic() + timeout_seconds
    post_attempted = False
    last_auto: dict[str, object] = {}
    last_paper: dict[str, object] = {}
    last_error: str | None = None
    active_projection: tuple[str, tuple[str, ...]] | None = None

    def current_target_binding() -> tuple[dict[str, object] | None, str | None]:
        """Read one process-bound identity without accepting a replacement listener."""
        if process.poll() is not None:
            return None, "TARGET_PROCESS_EXITED_DURING_STARTUP"
        try:
            candidate = http_json(base + "/api/runtime-binding")
        except (OSError, ValueError, RuntimeError, HTTPError, URLError):
            if process.poll() is not None:
                return None, "TARGET_PROCESS_EXITED_DURING_STARTUP"
            return None, "TARGET_RUNTIME_BINDING_UNAVAILABLE"
        if not _target_binding_matches(
            candidate,
            manifest,
            continuity,
            target_pid=process.pid,
            seed_artifact=seed_artifact,
            seed_proof=seed_proof,
        ) or dict(candidate) != pinned_target_binding:
            return None, "TARGET_RUNTIME_BINDING_MISMATCH"
        if process.poll() is not None:
            return None, "TARGET_PROCESS_EXITED_DURING_STARTUP"
        return dict(pinned_target_binding), None

    def project_active_target(stage: str, blockers: list[str]) -> None:
        nonlocal active_projection
        projection = (stage, tuple(blockers))
        if projection == active_projection:
            return
        active_projection = projection
        _state_update(
            state_path,
            "TARGET_ACTIVE_FLAT_BLOCKED"
            if stage == "TARGET_ACTIVE_FLAT_BLOCKED"
            else "TARGET_AUTOSTART_OBSERVATION_PENDING",
            stage=stage,
            blockers=blockers,
            target_pid=process.pid,
            target_runtime_pid=binding.get("pid"),
            target_runtime_binding=binding,
            target_autostart=last_auto,
            target_paper_status=last_paper,
        )

    while True:
        cycle_binding, binding_error = current_target_binding()
        if binding_error is not None:
            last_auto = {}
            last_paper = {}
            last_error = binding_error
            now = monotonic()
            if (
                binding_error in {
                    "TARGET_PROCESS_EXITED_DURING_STARTUP",
                    "TARGET_RUNTIME_BINDING_MISMATCH",
                }
                or now >= deadline
            ):
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    # No status response may be consumed after its serving
                    # process identity becomes unavailable or changes.
                    safe_state_allowed=False,
                    binding=binding,
                )
            project_active_target("AUTOSTARTING_TARGET", [binding_error])
            wait(min(0.5, max(0.001, deadline - now)))
            continue
        assert cycle_binding is not None
        binding = cycle_binding

        request_failed = False
        auto_observed = False
        paper_observed = False
        cycle_paper: dict[str, object] = {}
        cycle_auto: dict[str, object] = {}
        try:
            cycle_paper = http_json(base + "/api/lane-iii/paper")
            paper_observed = True
        except (OSError, ValueError, RuntimeError, HTTPError, URLError):
            request_failed = True
            last_error = "TARGET_PAPER_STATUS_UNAVAILABLE"
        try:
            cycle_auto = http_json(base + "/api/lane-iii/paper/auto-start")
            auto_observed = True
        except (OSError, ValueError, RuntimeError, HTTPError, URLError) as error:
            request_failed = True
            message = str(error)
            last_error = (
                message if message and message == message.upper()
                else "TARGET_AUTOSTART_FAILED"
            )

        # Close the status-read sandwich.  Paper and auto-start responses are
        # authoritative only while the same spawned PID still owns the exact
        # runtime binding before and after both reads.
        after_binding, binding_error = current_target_binding()
        if binding_error is not None:
            last_auto = {}
            last_paper = {}
            last_error = binding_error
            now = monotonic()
            if (
                binding_error in {
                    "TARGET_PROCESS_EXITED_DURING_STARTUP",
                    "TARGET_RUNTIME_BINDING_MISMATCH",
                }
                or now >= deadline
            ):
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    safe_state_allowed=False,
                    binding=binding,
                )
            project_active_target("AUTOSTARTING_TARGET", [binding_error])
            wait(min(0.5, max(0.001, deadline - now)))
            continue
        assert after_binding is not None
        binding = after_binding
        last_paper = cycle_paper if paper_observed else {}
        last_auto = cycle_auto if auto_observed else {}
        paper_identity_matches = paper_observed and _target_paper_identity_matches(
            last_paper, manifest,
        )

        if (
            auto_observed
            and paper_identity_matches
            and not post_attempted
            and last_auto.get("stage") != "RUNNING"
        ):
            token = last_auto.get("action_token")
            if not isinstance(token, str) or not token:
                last_error = "TARGET_AUTOSTART_TOKEN_UNAVAILABLE"
            else:
                # Revalidate immediately next to the mutation.  Mark the
                # attempt before sending because a lost response must never
                # produce a blind duplicate POST.
                mutation_binding, binding_error = current_target_binding()
                if binding_error is not None:
                    return block_after_target_cleanup(
                        [binding_error],
                        safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                        safe_state_allowed=False,
                        binding=binding,
                    )
                assert mutation_binding is not None
                binding = mutation_binding
                post_attempted = True
                try:
                    http_json(
                        base + "/api/lane-iii/paper/auto-start",
                        method="POST",
                        headers={
                            "X-Beelzebub-Paper-Autostart-Action": "sim101-paper-autostart-v1",
                            "X-Beelzebub-Paper-Autostart-Token": token,
                        },
                        body={
                            "request_id": f"profile-switch-{manifest['operation_id']}",
                        },
                    )
                except (OSError, ValueError, RuntimeError, HTTPError, URLError) as error:
                    request_failed = True
                    message = str(error)
                    last_error = (
                        message if message and message == message.upper()
                        else "TARGET_AUTOSTART_FAILED"
                    )
                # Never promote or clean up from the pre-action snapshot.  A
                # new sandwich must observe the post-action state first.
                now = monotonic()
                wait(0.5 if now >= deadline else min(0.5, max(0.001, deadline - now)))
                continue

        operational_active = paper_observed and _target_operational_active(
            last_paper, manifest,
        )
        position_proven = paper_observed and _target_perpetual_position_proven(
            last_paper, manifest,
        )
        if (
            auto_observed
            and last_auto.get("stage") == "RUNNING"
            and position_proven
        ):
            persistence_binding, binding_error = current_target_binding()
            if binding_error is not None:
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    safe_state_allowed=False,
                    binding=binding,
                    auto=last_auto,
                    paper=last_paper,
                )
            assert persistence_binding is not None
            binding = persistence_binding
            try:
                _record_established_selection(runtime_root, manifest, binding)
            except RuntimeError as error:
                blockers = [str(error)]
                _state_update(
                    state_path, "PROFILE_SWITCH_SELECTION_PERSISTENCE_FAILED",
                    stage="RUNNING_SELECTION_PERSISTENCE_FAILED",
                    blockers=blockers,
                    target_runtime_binding=binding,
                    target_runtime_pid=binding.get("pid"),
                    target_autostart=last_auto,
                    target_paper_status=last_paper,
                )
                notify(blockers)
                return 11
            final_binding, binding_error = current_target_binding()
            if binding_error is not None:
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    safe_state_allowed=False,
                    binding=binding,
                    auto=last_auto,
                    paper=last_paper,
                )
            assert final_binding is not None
            binding = final_binding
            if process.poll() is not None:
                return block_after_target_cleanup(
                    ["TARGET_PROCESS_EXITED_DURING_STARTUP"],
                    safe_exit_code=5,
                    safe_state_allowed=False,
                    binding=binding,
                    auto=last_auto,
                    paper=last_paper,
                )
            _state_update(
                state_path,
                "PROFILE_SWITCH_COMPLETED",
                stage="RUNNING",
                blockers=[],
                target_runtime_binding=binding,
                target_pid=process.pid,
                target_runtime_pid=binding.get("pid"),
                target_autostart=last_auto,
                target_paper_status=last_paper,
            )
            return 0

        if paper_observed and _target_perpetual_flat_blocked(last_paper, manifest):
            project_active_target(
                "TARGET_ACTIVE_FLAT_BLOCKED",
                [_target_position_blocker(last_paper)],
            )
        elif operational_active:
            blocker = (
                _target_position_blocker(last_paper)
                if manifest.get("target_profile")
                == FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION
                else "TARGET_AUTOSTART_NOT_RUNNING"
            )
            project_active_target("AUTOSTARTING_TARGET", [blocker])
        elif not paper_identity_matches:
            project_active_target(
                "AUTOSTARTING_TARGET", ["TARGET_PAPER_AUTHORITY_MISMATCH"],
            )

        terminal_auto = bool(
            auto_observed
            and last_auto.get("in_progress") is False
            and last_auto.get("stage") in {"BLOCKED", "FAILED", "CANCELLED"}
        )
        non_operational_flat = (
            paper_observed
            and _target_non_operational_flat_proven(last_paper, manifest)
        )
        if terminal_auto and non_operational_flat:
            terminal_binding, binding_error = current_target_binding()
            if binding_error is not None:
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    safe_state_allowed=False,
                    binding=binding,
                    auto=last_auto,
                    paper=last_paper,
                )
            assert terminal_binding is not None
            binding = terminal_binding
            auto_blockers = last_auto.get("blockers")
            blockers = (
                [str(value) for value in auto_blockers]
                if isinstance(auto_blockers, list) and auto_blockers
                else ["TARGET_AUTOSTART_BLOCKED"]
            )
            return block_after_target_cleanup(
                blockers,
                safe_exit_code=7,
                safe_state_allowed=True,
                binding=binding,
                auto=last_auto,
                paper=last_paper,
            )

        now = monotonic()
        if now >= deadline and non_operational_flat:
            timeout_binding, binding_error = current_target_binding()
            if binding_error is not None:
                return block_after_target_cleanup(
                    [binding_error],
                    safe_exit_code=5 if binding_error == "TARGET_PROCESS_EXITED_DURING_STARTUP" else 12,
                    safe_state_allowed=False,
                    binding=binding,
                    auto=last_auto,
                    paper=last_paper,
                )
            assert timeout_binding is not None
            binding = timeout_binding
            blocker = last_error if request_failed and last_error else "TARGET_AUTOSTART_TIMEOUT"
            safe_timeout = bool(
                not post_attempted
                and auto_observed
                and last_auto.get("stage") == "IDLE"
                and last_auto.get("in_progress") is False
            )
            return block_after_target_cleanup(
                [blocker],
                safe_exit_code=8 if request_failed else 9,
                safe_state_allowed=safe_timeout,
                binding=binding,
                auto=last_auto,
                paper=last_paper,
            )
        if now >= deadline and not operational_active:
            blocker = last_error or (
                "TARGET_PAPER_AUTHORITY_MISMATCH"
                if not paper_identity_matches
                else "TARGET_NON_OPERATIONAL_FLAT_PROOF_UNAVAILABLE"
            )
            project_active_target("AUTOSTARTING_TARGET", [blocker])

        delay = 0.5 if now >= deadline else min(0.5, max(0.001, deadline - now))
        wait(delay)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Supervise one prepared Beelzebub paper profile handoff.")
    parser.add_argument("--supervise", type=Path, required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    options = parser.parse_args(argv)
    return supervise(
        options.supervise.resolve(), options.parent_pid,
        notify_operator=_windows_profile_switch_notice,
    )


if __name__ == "__main__":
    raise SystemExit(main())
