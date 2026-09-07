"""Validated, paper-only risk continuity carried across process and profile epochs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Mapping

from src.lane_iii.contracts import normalized_utc

from .sessions import PaperSessionKind, context_from_identity


RISK_CONTINUITY_SNAPSHOT_SCHEMA = "lane-iii-paper-risk-continuity-snapshot-v2"
RISK_CONTINUITY_ARTIFACT_SCHEMA = "lane-iii-paper-risk-continuity-artifact-v2"
_ACCOUNT_IDENTITY = ("Sim101", "LOCAL_SIMULATION", "MNQ SEP26")
_RECORD_HASH = re.compile(r"^[0-9a-f]{64}$")


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _decimal(value: object, field: str) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"RISK_CONTINUITY_{field}_INVALID") from exc
    if not parsed.is_finite():
        raise RuntimeError(f"RISK_CONTINUITY_{field}_INVALID")
    return str(parsed)


def _count(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise RuntimeError(f"RISK_CONTINUITY_{field}_INVALID")
    return value


def _utc(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"RISK_CONTINUITY_{field}_INVALID")
    try:
        return normalized_utc(value, f"Risk continuity {field.lower().replace('_', ' ')}")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"RISK_CONTINUITY_{field}_INVALID") from exc


def validate_risk_continuity_snapshot(value: object, *, require_flat: bool = False) -> dict[str, object]:
    """Return one canonical validated snapshot; caller-defined authority is refused."""
    if not isinstance(value, Mapping):
        raise RuntimeError("RISK_CONTINUITY_SNAPSHOT_INVALID")
    required = {
        "schema", "generated_at", "account_name", "account_class", "instrument",
        "source_profile", "source_ledger", "trade_dates", "profile_trade_dates", "sessions",
        "entry_execution_ids", "exit_execution_ids", "authority_lockout",
    }
    if set(value) != required or value.get("schema") != RISK_CONTINUITY_SNAPSHOT_SCHEMA:
        raise RuntimeError("RISK_CONTINUITY_SNAPSHOT_INVALID")
    if (
        value.get("account_name"), value.get("account_class"), value.get("instrument")
    ) != _ACCOUNT_IDENTITY:
        raise RuntimeError("RISK_CONTINUITY_ACCOUNT_IDENTITY_MISMATCH")
    generated_at = _utc(value.get("generated_at"), "GENERATED_AT")
    source_profile = value.get("source_profile")
    if not isinstance(source_profile, str) or not source_profile:
        raise RuntimeError("RISK_CONTINUITY_SOURCE_PROFILE_INVALID")
    try:
        from .contracts import (
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
            resolve_paper_profile,
        )
        source_profile = resolve_paper_profile(source_profile).selection_key
    except ValueError as exc:
        raise RuntimeError("RISK_CONTINUITY_SOURCE_PROFILE_INVALID") from exc

    raw_source_ledger = value.get("source_ledger")
    if not isinstance(raw_source_ledger, Mapping) or set(raw_source_ledger) != {
        "path", "ledger_identity", "ledger_epoch", "risk_boundary_sequence",
        "risk_boundary_hash", "coverage_complete",
    }:
        raise RuntimeError("RISK_CONTINUITY_SOURCE_LEDGER_INVALID")
    ledger_path = raw_source_ledger.get("path")
    ledger_identity = raw_source_ledger.get("ledger_identity")
    ledger_epoch = raw_source_ledger.get("ledger_epoch")
    boundary_sequence = raw_source_ledger.get("risk_boundary_sequence")
    boundary_hash = raw_source_ledger.get("risk_boundary_hash")
    if (
        not isinstance(ledger_path, str) or not ledger_path
        or not isinstance(ledger_identity, str) or not ledger_identity
        or not isinstance(ledger_epoch, str) or not ledger_epoch
        or type(boundary_sequence) is not int or boundary_sequence < 0
        or not (
            (boundary_sequence == 0 and boundary_hash is None)
            or (
                boundary_sequence > 0 and isinstance(boundary_hash, str)
                and _RECORD_HASH.fullmatch(boundary_hash) is not None
            )
        )
        or type(raw_source_ledger.get("coverage_complete")) is not bool
    ):
        raise RuntimeError("RISK_CONTINUITY_SOURCE_LEDGER_INVALID")
    if require_flat and raw_source_ledger.get("coverage_complete") is not True:
        raise RuntimeError("RISK_CONTINUITY_SOURCE_LEDGER_COVERAGE_INCOMPLETE")
    source_ledger = dict(raw_source_ledger)

    raw_lockout = value.get("authority_lockout")
    if not isinstance(raw_lockout, Mapping) or set(raw_lockout) != {
        "locked_out", "lockout_reason", "lockout_trade_date",
    }:
        raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID")
    locked_out = raw_lockout.get("locked_out")
    lockout_reason = raw_lockout.get("lockout_reason")
    lockout_trade_date = raw_lockout.get("lockout_trade_date")
    if type(locked_out) is not bool:
        raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID")
    if locked_out:
        if not isinstance(lockout_reason, str) or not lockout_reason:
            raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID")
        if lockout_trade_date is not None:
            try:
                date.fromisoformat(str(lockout_trade_date))
            except ValueError as exc:
                raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID") from exc
            lockout_trade_date = str(lockout_trade_date)
    elif lockout_reason is not None or lockout_trade_date is not None:
        raise RuntimeError("RISK_CONTINUITY_AUTHORITY_LOCKOUT_INVALID")
    authority_lockout = {
        "locked_out": locked_out,
        "lockout_reason": lockout_reason,
        "lockout_trade_date": lockout_trade_date,
    }

    raw_trade_dates = value.get("trade_dates")
    if not isinstance(raw_trade_dates, list):
        raise RuntimeError("RISK_CONTINUITY_TRADE_DATES_INVALID")
    trade_dates: list[dict[str, object]] = []
    trade_date_keys: set[str] = set()
    for raw in raw_trade_dates:
        if not isinstance(raw, Mapping) or set(raw) != {
            "trade_date", "realized_pnl", "unrealized_pnl", "entry_count",
        }:
            raise RuntimeError("RISK_CONTINUITY_TRADE_DATE_INVALID")
        trade_date = str(raw.get("trade_date"))
        try:
            date.fromisoformat(trade_date)
        except ValueError as exc:
            raise RuntimeError("RISK_CONTINUITY_TRADE_DATE_INVALID") from exc
        if trade_date in trade_date_keys:
            raise RuntimeError("RISK_CONTINUITY_TRADE_DATE_INVALID")
        realized = _decimal(raw.get("realized_pnl"), "REALIZED_PNL")
        unrealized = _decimal(raw.get("unrealized_pnl"), "UNREALIZED_PNL")
        if require_flat and Decimal(unrealized) != 0:
            raise RuntimeError("RISK_CONTINUITY_NONFLAT_UNREALIZED_PNL")
        trade_date_keys.add(trade_date)
        trade_dates.append({
            "trade_date": trade_date,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "entry_count": _count(raw.get("entry_count"), "ENTRY_COUNT"),
        })

    raw_profile_trade_dates = value.get("profile_trade_dates")
    if not isinstance(raw_profile_trade_dates, list):
        raise RuntimeError("RISK_CONTINUITY_PROFILE_TRADE_DATES_INVALID")
    profile_trade_dates: list[dict[str, object]] = []
    profile_trade_date_keys: set[tuple[str, str]] = set()
    for raw in raw_profile_trade_dates:
        if not isinstance(raw, Mapping) or set(raw) != {
            "profile", "trade_date", "entry_count", "consecutive_losses",
        }:
            raise RuntimeError("RISK_CONTINUITY_PROFILE_TRADE_DATE_INVALID")
        try:
            profile = resolve_paper_profile(str(raw.get("profile"))).selection_key
            trade_date = str(raw.get("trade_date"))
            date.fromisoformat(trade_date)
        except ValueError as exc:
            raise RuntimeError("RISK_CONTINUITY_PROFILE_TRADE_DATE_INVALID") from exc
        key = (trade_date, profile)
        if key in profile_trade_date_keys:
            raise RuntimeError("RISK_CONTINUITY_PROFILE_TRADE_DATE_INVALID")
        profile_trade_date_keys.add(key)
        profile_trade_dates.append({
            "profile": profile,
            "trade_date": trade_date,
            "entry_count": _count(raw.get("entry_count"), "PROFILE_ENTRY_COUNT"),
            "consecutive_losses": _count(raw.get("consecutive_losses"), "PROFILE_CONSECUTIVE_LOSSES"),
        })

    raw_sessions = value.get("sessions")
    if not isinstance(raw_sessions, list):
        raise RuntimeError("RISK_CONTINUITY_SESSIONS_INVALID")
    sessions: list[dict[str, object]] = []
    session_keys: set[tuple[str, str]] = set()
    session_totals: dict[str, tuple[int, Decimal]] = {}
    profile_session_totals: dict[tuple[str, str], int] = {}
    for raw in raw_sessions:
        if not isinstance(raw, Mapping) or set(raw) != {
            "profile", "session_kind", "session_family", "session_id", "trade_date",
            "session_profile_hash", "session_generation", "entry_count", "realized_pnl",
        }:
            raise RuntimeError("RISK_CONTINUITY_SESSION_INVALID")
        try:
            profile = resolve_paper_profile(str(raw.get("profile"))).selection_key
            kind = PaperSessionKind(str(raw.get("session_kind")))
            generation = raw.get("session_generation")
            if type(generation) is not int or generation < 0:
                raise ValueError
            context = context_from_identity(
                kind, str(raw.get("session_id")), str(raw.get("trade_date")),
                str(raw.get("session_profile_hash")), generation,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("RISK_CONTINUITY_SESSION_IDENTITY_INVALID") from exc
        key = (context.session_id, profile)
        allowed_perpetual_off_session = (
            context.session_kind is PaperSessionKind.OFF_SESSION
            and profile == FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION
            and context.trade_date != "1970-01-01"
            and context.session_id
            == f"MNQU6:OFF_SESSION:{context.trade_date}"
        )
        if (
            (
                context.session_kind is PaperSessionKind.OFF_SESSION
                and not allowed_perpetual_off_session
            )
            or raw.get("session_family") != context.session_family.value
            or key in session_keys
        ):
            raise RuntimeError("RISK_CONTINUITY_SESSION_IDENTITY_INVALID")
        session_keys.add(key)
        entry_count = _count(raw.get("entry_count"), "SESSION_ENTRY_COUNT")
        realized_pnl = _decimal(raw.get("realized_pnl"), "SESSION_REALIZED_PNL")
        prior_count, prior_pnl = session_totals.get(context.trade_date, (0, Decimal("0")))
        session_totals[context.trade_date] = (prior_count + entry_count, prior_pnl + Decimal(realized_pnl))
        profile_key = (context.trade_date, profile)
        profile_session_totals[profile_key] = profile_session_totals.get(profile_key, 0) + entry_count
        sessions.append({
            "profile": profile,
            "session_kind": context.session_kind.value,
            "session_family": context.session_family.value,
            "session_id": context.session_id,
            "trade_date": context.trade_date,
            "session_profile_hash": context.session_profile_hash,
            "session_generation": context.session_generation,
            "entry_count": entry_count,
            "realized_pnl": realized_pnl,
        })

    if require_flat:
        for trade_date_state in trade_dates:
            bucket_key = str(trade_date_state["trade_date"])
            session_count, session_pnl = session_totals.pop(bucket_key, (0, Decimal("0")))
            if session_count != trade_date_state["entry_count"] or session_pnl != Decimal(str(trade_date_state["realized_pnl"])):
                raise RuntimeError("RISK_CONTINUITY_SESSION_TOTAL_MISMATCH")
        if session_totals:
            raise RuntimeError("RISK_CONTINUITY_SESSION_TOTAL_MISMATCH")
        for profile_state in profile_trade_dates:
            profile_key = (str(profile_state["trade_date"]), str(profile_state["profile"]))
            if profile_session_totals.pop(profile_key, 0) != profile_state["entry_count"]:
                raise RuntimeError("RISK_CONTINUITY_PROFILE_SESSION_TOTAL_MISMATCH")
        if profile_session_totals:
            raise RuntimeError("RISK_CONTINUITY_PROFILE_SESSION_TOTAL_MISMATCH")

    execution_lists: dict[str, list[str]] = {}
    for field in ("entry_execution_ids", "exit_execution_ids"):
        raw_ids = value.get(field)
        if not isinstance(raw_ids, list) or not all(isinstance(item, str) and item for item in raw_ids):
            raise RuntimeError("RISK_CONTINUITY_EXECUTION_IDS_INVALID")
        if len(raw_ids) != len(set(raw_ids)):
            raise RuntimeError("RISK_CONTINUITY_EXECUTION_IDS_INVALID")
        execution_lists[field] = sorted(raw_ids)

    if set(execution_lists["entry_execution_ids"]) & set(execution_lists["exit_execution_ids"]):
        raise RuntimeError("RISK_CONTINUITY_EXECUTION_ROLE_CONFLICT")

    if require_flat:
        total_entries = sum(int(item["entry_count"]) for item in trade_dates)
        if (
            len(execution_lists["entry_execution_ids"]) != total_entries
            or len(execution_lists["exit_execution_ids"]) != total_entries
        ):
            raise RuntimeError("RISK_CONTINUITY_EXECUTION_TOTAL_MISMATCH")

    return {
        "schema": RISK_CONTINUITY_SNAPSHOT_SCHEMA,
        "generated_at": generated_at,
        "account_name": _ACCOUNT_IDENTITY[0],
        "account_class": _ACCOUNT_IDENTITY[1],
        "instrument": _ACCOUNT_IDENTITY[2],
        "source_profile": source_profile,
        "source_ledger": source_ledger,
        "authority_lockout": authority_lockout,
        "trade_dates": sorted(trade_dates, key=lambda item: str(item["trade_date"])),
        "profile_trade_dates": sorted(
            profile_trade_dates, key=lambda item: (str(item["trade_date"]), str(item["profile"])),
        ),
        "sessions": sorted(
            sessions,
            key=lambda item: (str(item["session_id"]), str(item["profile"]), int(item["session_generation"])),
        ),
        **execution_lists,
    }


def write_risk_continuity_artifact(
    path: Path,
    *,
    operation_id: str,
    source_profile: str,
    target_profile: str,
    snapshot: object,
) -> dict[str, object]:
    canonical_snapshot = validate_risk_continuity_snapshot(snapshot, require_flat=True)
    if canonical_snapshot["source_profile"] != source_profile:
        raise RuntimeError("RISK_CONTINUITY_SOURCE_PROFILE_MISMATCH")
    artifact: dict[str, object] = {
        "schema": RISK_CONTINUITY_ARTIFACT_SCHEMA,
        "operation_id": operation_id,
        "source_profile": source_profile,
        "target_profile": target_profile,
        "created_at": str(canonical_snapshot["generated_at"]),
        "snapshot": canonical_snapshot,
    }
    artifact["artifact_sha256"] = sha256(_canonical(artifact)).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, json.dumps(artifact, sort_keys=True, indent=2).encode("utf-8") + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return artifact


def read_risk_continuity_artifact(
    path: Path,
    *,
    operation_id: str | None = None,
    target_profile: str | None = None,
) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("RISK_CONTINUITY_ARTIFACT_UNREADABLE") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("RISK_CONTINUITY_ARTIFACT_INVALID")
    supplied = raw.pop("artifact_sha256", None)
    if (
        set(raw) != {"schema", "operation_id", "source_profile", "target_profile", "created_at", "snapshot"}
        or raw.get("schema") != RISK_CONTINUITY_ARTIFACT_SCHEMA
        or supplied != sha256(_canonical(raw)).hexdigest()
    ):
        raise RuntimeError("RISK_CONTINUITY_ARTIFACT_INTEGRITY_FAILED")
    if operation_id is not None and raw.get("operation_id") != operation_id:
        raise RuntimeError("RISK_CONTINUITY_OPERATION_MISMATCH")
    if target_profile is not None and raw.get("target_profile") != target_profile:
        raise RuntimeError("RISK_CONTINUITY_TARGET_PROFILE_MISMATCH")
    snapshot = validate_risk_continuity_snapshot(raw.get("snapshot"), require_flat=True)
    created_at = _utc(raw.get("created_at"), "CREATED_AT")
    if created_at != snapshot.get("generated_at"):
        raise RuntimeError("RISK_CONTINUITY_ARTIFACT_TIME_MISMATCH")
    if snapshot.get("source_profile") != raw.get("source_profile"):
        raise RuntimeError("RISK_CONTINUITY_SOURCE_PROFILE_MISMATCH")
    return {**raw, "created_at": created_at, "snapshot": snapshot, "artifact_sha256": supplied}
