"""Local, process-owned verification for the Lane III paper ledger.

This module deliberately has no HTTP, model, broker, NinjaTrader, or order
dependencies.  A controller may launch this module in a separate Python
process, but the verifier itself has read-only SQLite authority and may write
only its own checkpoint, lock, cancellation marker, and JSON artifacts.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, Mapping
from uuid import uuid4

from src.lane_iii.contracts import canonical_hash

from .contracts import PAPER_RECORD_SCHEMA


VERIFIER_VERSION = "l3g-local-ledger-verifier-v1"
REPORT_SCHEMA = "beelzebub-ledger-verification-v1"
CHECKPOINT_SCHEMA = "beelzebub-ledger-verification-checkpoint-v1"
_MODES = frozenset({"auto", "incremental", "full"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _atomic_json(path: Path, value: Mapping[str, Any], *, replace: bool = True) -> None:
    """Atomically publish verifier state without partially readable JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(dict(value), stream, indent=2, sort_keys=True, ensure_ascii=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if not replace and path.exists():
            raise FileExistsError(path)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _file_identity(path: Path) -> dict[str, int | None]:
    try:
        stat = path.stat()
    except OSError:
        return {"device": None, "inode": None, "size": None, "mtime_ns": None}
    # device/inode identify a replacement on local NTFS/POSIX volumes.  Size
    # and mtime are diagnostic only; a checkpoint never trusts mtime alone.
    return {
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


@dataclass(frozen=True)
class VerificationPaths:
    root: Path

    @property
    def reports(self) -> Path:
        return self.root / "ledger-verification"

    @property
    def latest(self) -> Path:
        return self.root / "ledger-verification-latest.json"

    @property
    def checkpoint(self) -> Path:
        return self.root / "ledger-verification-checkpoint.json"

    @property
    def current(self) -> Path:
        return self.root / "ledger-verification-current.json"

    @property
    def lock(self) -> Path:
        return self.root / "ledger-verification.lock"

    def cancel(self, verification_id: str) -> Path:
        return self.root / f"ledger-verification-{verification_id}.cancel"

    def history(self, verification_id: str, completed_at: str) -> Path:
        stamp = completed_at.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
        return self.reports / f"{stamp}-{verification_id}.json"

    def log(self, verification_id: str) -> Path:
        return self.reports / f"{verification_id}.log"


class VerificationFailure(RuntimeError):
    def __init__(self, code: str, message: str, *, full_scan_required: bool = True, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.full_scan_required = full_scan_required
        self.detail = dict(detail or {})


class VerificationCancelled(RuntimeError):
    pass


def _ledger_epoch(path: Path, metadata: Mapping[str, str]) -> str:
    stored = str(metadata.get("ledger_epoch") or "")
    if not stored:
        raise VerificationFailure("LEDGER_EPOCH_MISSING", "Ledger epoch metadata is missing.")
    # The write-time metadata seals the expected epoch.  A file moved into a
    # different epoch directory is fail-closed even if its rows still verify.
    for parent in path.parents:
        name = parent.name.lower()
        if name.startswith("epoch-"):
            suffix = name.removeprefix("epoch-")
            expected = f"L3G-PAPER-EPOCH-{suffix}"
            if expected != stored:
                raise VerificationFailure("LEDGER_EPOCH_PATH_MISMATCH", "Ledger path and sealed epoch metadata disagree.")
            break
    return stored


def _row_digest(row: sqlite3.Row) -> str:
    return canonical_hash({
        "ledger_sequence": int(row["ledger_sequence"]),
        "identity": str(row["identity"]),
        "domain": str(row["domain"]),
        "kind": str(row["kind"]),
        "occurred_at": str(row["occurred_at"]),
        "execution_session_id": row["execution_session_id"],
        "payload_json": str(row["payload_json"]),
        "previous_record_hash": row["previous_record_hash"],
        "record_hash": str(row["record_hash"]),
    })


def _sentinel_sequences(tip: int) -> tuple[int, ...]:
    """Bounded prefix witnesses used with file identity and the chain tip.

    They do not replace a forensic full chain scan.  They make an incremental
    checkpoint reject common in-place historical mutation/replacement without
    walking the old tail. They complement, but never replace, a full scan.
    """
    if tip < 1:
        return ()
    count = min(257, tip)
    if count == 1:
        return (1,)
    values = {1, tip}
    for index in range(1, count - 1):
        values.add(1 + ((tip - 1) * index) // (count - 1))
    return tuple(sorted(values))


class LocalLedgerVerifier:
    """One deterministic verification run against one read-only SQLite image."""

    def __init__(self, ledger_path: str | Path, audit_root: str | Path, *, requested_mode: str, verification_id: str | None = None) -> None:
        self.ledger_path = Path(ledger_path).expanduser().resolve()
        self.paths = VerificationPaths(Path(audit_root).expanduser().resolve())
        self.requested_mode = self._mode(requested_mode)
        self.verification_id = verification_id or f"lv-{uuid4().hex}"
        self.started_at = _utc_now()
        self._cancel_path = self.paths.cancel(self.verification_id)

    @staticmethod
    def _mode(value: str) -> str:
        normalized = str(value).strip().lower()
        if normalized not in _MODES:
            raise ValueError("Verification mode must be auto, incremental, or full.")
        return normalized

    def _connect(self) -> sqlite3.Connection:
        if not self.ledger_path.is_file():
            raise VerificationFailure("LEDGER_UNAVAILABLE", f"Ledger image is unavailable: {self.ledger_path}")
        try:
            connection = sqlite3.connect(f"{self.ledger_path.as_uri()}?mode=ro", uri=True, timeout=5.0)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            # The open read transaction gives this verifier one coherent WAL
            # snapshot while the paper writer continues append-only activity.
            connection.execute("BEGIN")
            return connection
        except sqlite3.Error as exc:
            raise VerificationFailure("SQLITE_ACCESS_ERROR", f"Read-only ledger access failed: {exc}") from exc

    def _metadata(self, connection: sqlite3.Connection) -> dict[str, str]:
        try:
            rows = connection.execute(
                "SELECT metadata_key, metadata_value FROM lane_iii_paper_ledger_metadata"
            ).fetchall()
        except sqlite3.Error as exc:
            raise VerificationFailure("LEDGER_IDENTITY_MISSING", "Ledger identity metadata table is unavailable.") from exc
        metadata = {str(row["metadata_key"]): str(row["metadata_value"]) for row in rows}
        required = {"ledger_uuid", "ledger_epoch", "schema_version"}
        missing = sorted(required - set(metadata))
        if missing:
            raise VerificationFailure("LEDGER_IDENTITY_MISSING", "Ledger identity metadata is incomplete.", detail={"missing": missing})
        return metadata

    def _schema(self, connection: sqlite3.Connection) -> None:
        required = {
            "ledger_sequence", "identity", "domain", "kind", "occurred_at", "execution_session_id",
            "payload_json", "previous_record_hash", "record_hash",
        }
        try:
            columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(lane_iii_paper_audit)").fetchall()}
        except sqlite3.Error as exc:
            raise VerificationFailure("SCHEMA_INVALID", "Paper audit schema is unreadable.") from exc
        missing = sorted(required - columns)
        if missing:
            raise VerificationFailure("SCHEMA_INVALID", "Paper audit schema is incompatible.", detail={"missing": missing})

    def _quick_check(self, connection: sqlite3.Connection) -> str:
        try:
            values = [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
        except sqlite3.Error as exc:
            raise VerificationFailure("QUICK_CHECK_ERROR", f"SQLite quick_check could not complete: {exc}") from exc
        if values != ["ok"]:
            raise VerificationFailure("QUICK_CHECK_FAILED", "SQLite quick_check did not return ok.", detail={"results": values[:20]})
        return "ok"

    def _tip(self, connection: sqlite3.Connection) -> tuple[int, str | None]:
        row = connection.execute(
            "SELECT ledger_sequence, record_hash FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT 1"
        ).fetchone()
        return (0, None) if row is None else (int(row["ledger_sequence"]), str(row["record_hash"]))

    def _checkpoint(self) -> dict[str, Any] | None:
        data = _safe_read_json(self.paths.checkpoint)
        return data if data and data.get("schema") == CHECKPOINT_SCHEMA else None

    def _validate_checkpoint(
        self, connection: sqlite3.Connection, checkpoint: Mapping[str, Any], metadata: Mapping[str, str], tip_sequence: int,
    ) -> tuple[int, str | None]:
        if checkpoint.get("ledger_path") != str(self.ledger_path):
            raise VerificationFailure("CHECKPOINT_PATH_MISMATCH", "Checkpoint belongs to a different ledger path.")
        if checkpoint.get("ledger_identity") != metadata["ledger_uuid"]:
            raise VerificationFailure("LEDGER_REPLACED", "Checkpoint ledger identity does not match this image.")
        if checkpoint.get("ledger_epoch") != _ledger_epoch(self.ledger_path, metadata):
            raise VerificationFailure("CHECKPOINT_EPOCH_MISMATCH", "Checkpoint epoch does not match this ledger.")
        if checkpoint.get("schema_version") != metadata["schema_version"]:
            raise VerificationFailure("CHECKPOINT_SCHEMA_MISMATCH", "Checkpoint schema does not match this ledger.")
        checkpoint_file = checkpoint.get("file_identity")
        current_file = _file_identity(self.ledger_path)
        if not isinstance(checkpoint_file, Mapping) or any(
            checkpoint_file.get(key) != current_file.get(key) for key in ("device", "inode")
        ):
            raise VerificationFailure("LEDGER_REPLACED", "Checkpoint file identity does not match this image.")
        sequence = checkpoint.get("last_verified_sequence")
        record_hash = checkpoint.get("last_verified_hash")
        if type(sequence) is not int or sequence < 0:
            raise VerificationFailure("CHECKPOINT_INVALID", "Checkpoint sequence is invalid.")
        if sequence > tip_sequence:
            raise VerificationFailure("CHECKPOINT_BEYOND_TIP", "Checkpoint points beyond the current ledger tip.")
        if sequence == 0:
            if record_hash is not None:
                raise VerificationFailure("CHECKPOINT_INVALID", "Empty-ledger checkpoint has a record hash.")
        else:
            row = connection.execute(
                "SELECT record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?", (sequence,)
            ).fetchone()
            if row is None or str(row["record_hash"]) != record_hash:
                raise VerificationFailure("CHECKPOINT_HASH_MISMATCH", "Checkpoint hash no longer matches its ledger record.")
        witnesses = checkpoint.get("prefix_sentinels")
        if not isinstance(witnesses, list):
            raise VerificationFailure("CHECKPOINT_INVALID", "Checkpoint has no trusted prefix witnesses.")
        for witness in witnesses:
            if not isinstance(witness, Mapping) or type(witness.get("sequence")) is not int or not isinstance(witness.get("digest"), str):
                raise VerificationFailure("CHECKPOINT_INVALID", "Checkpoint prefix witness is malformed.")
            row = connection.execute(
                "SELECT ledger_sequence, identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence=?", (witness["sequence"],)
            ).fetchone()
            if row is None or _row_digest(row) != witness["digest"]:
                raise VerificationFailure("HISTORICAL_MUTATION_DETECTED", "A trusted pre-checkpoint ledger witness diverged.")
        return sequence, record_hash if isinstance(record_hash, str) else None

    def _check_cancelled(self) -> None:
        if self._cancel_path.exists():
            raise VerificationCancelled("Verification cancellation was requested locally.")

    def _scan_chain(
        self, connection: sqlite3.Connection, *, start_sequence: int, previous_hash: str | None,
    ) -> tuple[int, str | None, int, int]:
        expected_sequence = start_sequence
        rows_scanned = 0
        bytes_scanned = 0
        cursor = connection.execute(
            "SELECT ledger_sequence, identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash "
            "FROM lane_iii_paper_audit WHERE ledger_sequence >= ? ORDER BY ledger_sequence",
            (start_sequence,),
        )
        while True:
            batch = cursor.fetchmany(4096)
            if not batch:
                break
            self._check_cancelled()
            for row in batch:
                sequence = int(row["ledger_sequence"])
                if sequence != expected_sequence:
                    raise VerificationFailure("SEQUENCE_DIVERGENCE", "Ledger sequence is not contiguous.", detail={"expected": expected_sequence, "observed": sequence})
                try:
                    record = json.loads(str(row["payload_json"]))
                except json.JSONDecodeError as exc:
                    raise VerificationFailure("MALFORMED_RECORD", "Ledger payload is not valid JSON.", detail={"sequence": sequence}) from exc
                if not isinstance(record, dict):
                    raise VerificationFailure("MALFORMED_RECORD", "Ledger payload must be a JSON object.", detail={"sequence": sequence})
                observed_hash = record.pop("record_hash", None)
                if (
                    record.get("schema") != PAPER_RECORD_SCHEMA
                    or record.get("identity") != row["identity"]
                    or record.get("previous_record_hash") != previous_hash
                    or record.get("kind") != row["kind"]
                    or record.get("occurred_at") != row["occurred_at"]
                    or record.get("execution_session_id") != row["execution_session_id"]
                    or row["previous_record_hash"] != previous_hash
                    or observed_hash != row["record_hash"]
                    or observed_hash != canonical_hash(record)
                ):
                    raise VerificationFailure(
                        "CHAIN_INVALID", "Ledger hash-chain or record invariant failed.", detail={"sequence": sequence, "identity": str(row["identity"])},
                    )
                previous_hash = str(observed_hash)
                expected_sequence += 1
                rows_scanned += 1
                bytes_scanned += len(str(row["payload_json"]).encode("utf-8"))
        return expected_sequence - 1, previous_hash, rows_scanned, bytes_scanned

    def _witnesses(self, connection: sqlite3.Connection, tip_sequence: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for sequence in _sentinel_sequences(tip_sequence):
            row = connection.execute(
                "SELECT ledger_sequence, identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence=?", (sequence,),
            ).fetchone()
            if row is None:
                raise VerificationFailure("SEQUENCE_DIVERGENCE", "Ledger witness record is unavailable.", detail={"sequence": sequence})
            result.append({"sequence": sequence, "digest": _row_digest(row)})
        return result

    def _checkpoint_payload(
        self, *, metadata: Mapping[str, str], verified_sequence: int, tip_hash: str | None, connection: sqlite3.Connection,
    ) -> dict[str, Any]:
        completed = _utc_now()
        return {
            "schema": CHECKPOINT_SCHEMA,
            "ledger_path": str(self.ledger_path),
            "ledger_identity": metadata["ledger_uuid"],
            "ledger_epoch": _ledger_epoch(self.ledger_path, metadata),
            "schema_version": metadata["schema_version"],
            "last_verified_sequence": verified_sequence,
            "last_verified_hash": tip_hash,
            "verified_at": completed,
            "verifier_version": VERIFIER_VERSION,
            "file_identity": _file_identity(self.ledger_path),
            "current_chain_tip_sequence": verified_sequence,
            "current_chain_tip_hash": tip_hash,
            "prefix_sentinels": self._witnesses(connection, verified_sequence),
        }

    def _base_report(self, *, status: str, verification_mode: str | None = None) -> dict[str, Any]:
        return {
            "schema": REPORT_SCHEMA,
            "verification_id": self.verification_id,
            "status": status,
            "verification_mode": verification_mode or self.requested_mode,
            "requested_mode": self.requested_mode,
            "started_at": self.started_at,
            "completed_at": None,
            "duration_seconds": 0.0,
            "ledger_path": str(self.ledger_path),
            "ledger_epoch": None,
            "quick_check": "not_run",
            "chain_valid": False,
            "checkpoint_valid": False,
            "checkpoint_start_sequence": 0,
            "verified_through_sequence": 0,
            "tip_hash": None,
            "rows_scanned": 0,
            "bytes_scanned": 0,
            "full_scan_required": False,
            "verifier_version": VERIFIER_VERSION,
            "errors": [],
        }

    def _complete(self, report: dict[str, Any]) -> dict[str, Any]:
        completed = _utc_now()
        report["completed_at"] = completed
        report["duration_seconds"] = round(max(0.0, (_parse_utc(completed) - _parse_utc(self.started_at)).total_seconds()), 6)
        return report

    def _publish_terminal(self, report: dict[str, Any]) -> None:
        completed = str(report["completed_at"])
        history = self.paths.history(self.verification_id, completed)
        _atomic_json(history, report, replace=False)
        # "latest" always refers to a fully written terminal artifact.
        _atomic_json(self.paths.latest, report)
        try:
            self.paths.current.unlink()
        except FileNotFoundError:
            pass

    def run(self, *, adopt_lock: bool = False) -> dict[str, Any]:
        self.paths.root.mkdir(parents=True, exist_ok=True)
        if adopt_lock:
            lock = _safe_read_json(self.paths.lock)
            if not lock or lock.get("verification_id") != self.verification_id:
                raise RuntimeError("Verifier launcher lock could not be adopted.")
            _atomic_json(self.paths.lock, {**lock, "pid": os.getpid(), "state": "RUNNING", "started_at": self.started_at})
        else:
            _create_lock(self.paths, self.verification_id, state="RUNNING")
        report = self._base_report(status="IN_PROGRESS")
        _atomic_json(self.paths.current, report)
        connection: sqlite3.Connection | None = None
        try:
            connection = self._connect()
            self._schema(connection)
            metadata = self._metadata(connection)
            epoch = _ledger_epoch(self.ledger_path, metadata)
            report["ledger_epoch"] = epoch
            report["quick_check"] = self._quick_check(connection)
            tip_sequence, _ = self._tip(connection)
            checkpoint = self._checkpoint()
            actual_mode = self.requested_mode
            if actual_mode == "auto":
                actual_mode = "incremental" if checkpoint is not None else "full"
            report["verification_mode"] = actual_mode
            start_sequence, previous_hash = 1, None
            if actual_mode == "incremental":
                if checkpoint is None:
                    raise VerificationFailure("CHECKPOINT_MISSING", "Incremental verification requires a trusted checkpoint.")
                checkpoint_sequence, checkpoint_hash = self._validate_checkpoint(connection, checkpoint, metadata, tip_sequence)
                report["checkpoint_valid"] = True
                report["checkpoint_start_sequence"] = checkpoint_sequence
                start_sequence, previous_hash = checkpoint_sequence + 1, checkpoint_hash
            verified, tip_hash, rows_scanned, bytes_scanned = self._scan_chain(
                connection, start_sequence=start_sequence, previous_hash=previous_hash,
            )
            if verified != tip_sequence:
                raise VerificationFailure("TIP_DIVERGENCE", "Ledger snapshot tip changed during verification.")
            if actual_mode == "full":
                report["checkpoint_valid"] = True
            report.update({
                "status": "PASS",
                "chain_valid": True,
                "verified_through_sequence": verified,
                "tip_hash": tip_hash,
                "rows_scanned": rows_scanned,
                "bytes_scanned": bytes_scanned,
                "full_scan_required": False,
            })
            checkpoint_payload = self._checkpoint_payload(
                metadata=metadata, verified_sequence=verified, tip_hash=tip_hash, connection=connection,
            )
            # A checkpoint is advanced only after every verifier invariant has
            # passed.  Atomic replacement prevents a partial trusted state.
            _atomic_json(self.paths.checkpoint, checkpoint_payload)
        except VerificationCancelled as exc:
            report.update({"status": "CANCELLED", "full_scan_required": False, "errors": [{"code": "CANCELLED", "message": str(exc)}]})
        except VerificationFailure as exc:
            report.update({
                "status": "FAIL", "chain_valid": False, "full_scan_required": exc.full_scan_required,
                "errors": [{"code": exc.code, "message": str(exc), **exc.detail}],
            })
        except Exception as exc:  # Never leave ambiguous successful state.
            report.update({
                "status": "FAIL", "chain_valid": False, "full_scan_required": True,
                "errors": [{"code": "VERIFIER_INTERNAL_ERROR", "message": str(exc)}],
            })
        finally:
            if connection is not None:
                connection.close()
            report = self._complete(report)
            self._publish_terminal(report)
            _release_lock(self.paths, self.verification_id)
            try:
                self._cancel_path.unlink()
            except FileNotFoundError:
                pass
        return report


def _create_lock(paths: VerificationPaths, verification_id: str, *, state: str) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    payload = {"verification_id": verification_id, "pid": os.getpid(), "state": state, "started_at": _utc_now()}
    try:
        descriptor = os.open(paths.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise VerificationFailure("VERIFICATION_ALREADY_RUNNING", "A local ledger verification is already running.", full_scan_required=False) from exc
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(payload, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _release_lock(paths: VerificationPaths, verification_id: str) -> None:
    lock = _safe_read_json(paths.lock)
    if lock is None or lock.get("verification_id") == verification_id:
        try:
            paths.lock.unlink()
        except FileNotFoundError:
            pass


def _pid_is_running(pid: object) -> bool:
    if type(pid) is not int or pid <= 0:
        return False
    if os.name == "nt":
        # os.kill(pid, 0) is not a reliable liveness probe on every Windows
        # configuration. Query the process handle directly so crashed detached
        # verifiers are turned into an explicit INTERRUPTED artifact.
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if not handle:
                return False
            try:
                code = ctypes.c_ulong()
                if not ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                    return False
                return code.value == 259  # STILL_ACTIVE
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class LocalLedgerVerificationController:
    """Launch/inspect a verifier without ever executing its scan in the API."""

    def __init__(self, ledger_path: str | Path, audit_root: str | Path, *, python_executable: str | None = None) -> None:
        self.ledger_path = Path(ledger_path).expanduser().resolve()
        self.paths = VerificationPaths(Path(audit_root).expanduser().resolve())
        self.python_executable = python_executable or sys.executable
        # Keep detached child handles only long enough to reap them when this
        # controller remains alive. The child itself owns the durable lock and
        # artifacts, so a new controller/browser never owns its lifetime.
        self._children: dict[str, Any] = {}

    def _recover_abandoned(self) -> None:
        lock = _safe_read_json(self.paths.lock)
        if not lock or _pid_is_running(lock.get("pid")):
            return
        current = _safe_read_json(self.paths.current)
        verification_id = str(lock.get("verification_id") or (current or {}).get("verification_id") or f"lv-{uuid4().hex}")
        if current and current.get("status") == "IN_PROGRESS":
            report = dict(current)
            report.update({
                "verification_id": verification_id,
                "status": "INTERRUPTED",
                "completed_at": _utc_now(),
                "full_scan_required": False,
                "errors": [{"code": "VERIFIER_INTERRUPTED", "message": "The local verifier process ended before it wrote a terminal result."}],
            })
            started = str(report.get("started_at") or report["completed_at"])
            report["duration_seconds"] = round(max(0.0, (_parse_utc(str(report["completed_at"])) - _parse_utc(started)).total_seconds()), 6)
            history = self.paths.history(verification_id, str(report["completed_at"]))
            _atomic_json(history, report, replace=False)
            _atomic_json(self.paths.latest, report)
            try:
                self.paths.current.unlink()
            except FileNotFoundError:
                pass
        _release_lock(self.paths, verification_id)

    def status(self) -> dict[str, Any]:
        for verification_id, child in tuple(self._children.items()):
            if child.poll() is not None:
                try:
                    child.wait(timeout=0)
                except subprocess.TimeoutExpired:
                    pass
                self._children.pop(verification_id, None)
        self._recover_abandoned()
        current = _safe_read_json(self.paths.current)
        if current and current.get("status") == "IN_PROGRESS":
            result = dict(current)
            try:
                result["elapsed_seconds"] = round(max(0.0, (datetime.now(timezone.utc) - _parse_utc(str(current["started_at"]))).total_seconds()), 3)
            except (KeyError, ValueError):
                result["elapsed_seconds"] = None
            lock = _safe_read_json(self.paths.lock) or {}
            result["process_id"] = lock.get("pid")
            return result
        latest = _safe_read_json(self.paths.latest)
        if latest:
            return latest
        return {
            "schema": REPORT_SCHEMA,
            "status": "UNVERIFIED",
            "verification_mode": None,
            "requested_mode": None,
            "ledger_path": str(self.ledger_path),
            "full_scan_required": True,
            "chain_valid": False,
            "checkpoint_valid": False,
            "errors": [{"code": "NO_VERIFICATION_ARTIFACT", "message": "No completed local ledger verification exists."}],
        }

    def start(self, requested_mode: str = "auto") -> dict[str, Any]:
        mode = LocalLedgerVerifier._mode(requested_mode)
        existing = self.status()
        if existing.get("status") == "IN_PROGRESS":
            return existing
        verification_id = f"lv-{uuid4().hex}"
        try:
            _create_lock(self.paths, verification_id, state="LAUNCHING")
        except VerificationFailure:
            return self.status()
        started = _utc_now()
        initial = LocalLedgerVerifier(self.ledger_path, self.paths.root, requested_mode=mode, verification_id=verification_id)._base_report(status="IN_PROGRESS")
        initial["started_at"] = started
        _atomic_json(self.paths.current, initial)
        command = [
            self.python_executable, "-m", "src.l3g_paper.verification", "run",
            "--ledger", str(self.ledger_path), "--audit-root", str(self.paths.root), "--mode", mode,
            "--verification-id", verification_id, "--adopt-lock",
        ]
        flags = 0
        if os.name == "nt":
            flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        try:
            self.paths.reports.mkdir(parents=True, exist_ok=True)
            with self.paths.log(verification_id).open("a", encoding="utf-8", newline="\n") as log:
                process = subprocess.Popen(
                    command, cwd=str(Path(__file__).resolve().parents[2]), stdin=subprocess.DEVNULL,
                    stdout=log, stderr=log, close_fds=True, creationflags=flags,
                )
            lock = _safe_read_json(self.paths.lock) or {}
            _atomic_json(self.paths.lock, {**lock, "verification_id": verification_id, "pid": process.pid, "state": "LAUNCHING", "started_at": started})
            self._children[verification_id] = process
        except Exception:
            _release_lock(self.paths, verification_id)
            try:
                self.paths.current.unlink()
            except FileNotFoundError:
                pass
            raise
        return self.status()

    def cancel(self) -> dict[str, Any]:
        status = self.status()
        if status.get("status") != "IN_PROGRESS":
            return status
        verification_id = status.get("verification_id")
        if not isinstance(verification_id, str):
            raise RuntimeError("Running verifier has no verification ID.")
        self.paths.cancel(verification_id).write_text("cancel requested\n", encoding="utf-8")
        return {**status, "cancellation_requested": True}

    def checkpoint_matches_report(self, report: Mapping[str, Any]) -> bool:
        """Cheap local identity check for commissioning; it never scans rows."""
        checkpoint = _safe_read_json(self.paths.checkpoint)
        if not checkpoint or checkpoint.get("schema") != CHECKPOINT_SCHEMA:
            return False
        if checkpoint.get("ledger_path") != str(self.ledger_path):
            return False
        if checkpoint.get("last_verified_sequence") != report.get("verified_through_sequence"):
            return False
        if checkpoint.get("last_verified_hash") != report.get("tip_hash"):
            return False
        saved_identity = checkpoint.get("file_identity")
        current_identity = _file_identity(self.ledger_path)
        return isinstance(saved_identity, Mapping) and all(
            saved_identity.get(key) == current_identity.get(key) for key in ("device", "inode")
        )


def run_local_verification(
    ledger_path: str | Path, audit_root: str | Path, *, requested_mode: str = "auto", verification_id: str | None = None,
    adopt_lock: bool = False,
) -> dict[str, Any]:
    return LocalLedgerVerifier(
        ledger_path, audit_root, requested_mode=requested_mode, verification_id=verification_id,
    ).run(adopt_lock=adopt_lock)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run local read-only Lane III paper-ledger verification.")
    command = parser.add_subparsers(dest="command", required=True)
    run = command.add_parser("run", help="run one verifier process")
    run.add_argument("--ledger", required=True)
    run.add_argument("--audit-root", required=True)
    run.add_argument("--mode", choices=sorted(_MODES), default="auto")
    run.add_argument("--verification-id")
    run.add_argument("--adopt-lock", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    options = _parser().parse_args(argv)
    report = run_local_verification(
        options.ledger, options.audit_root, requested_mode=options.mode,
        verification_id=options.verification_id, adopt_lock=bool(options.adopt_lock),
    )
    # The process remains silent by default; callers consume the artifact.
    return 0 if report.get("status") == "PASS" else 1


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess integration
    raise SystemExit(main())
