"""The narrow production task catalog for operations scheduling."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
from typing import Any, Mapping

from src.l3g_paper.sessions import PaperSessionKind, PaperSessionResolver

from .models import AuthorityClassification, TaskBlocked, TaskInvariantFailure, TaskOutcome, sanitized, utc_now
from .registry import TaskDefinition


def _mapping(configuration: Mapping[str, Any], allowed: set[str], required: set[str] = set()) -> dict[str, Any]:
    data = dict(configuration or {})
    unknown = set(data) - allowed
    missing = required - set(data)
    if unknown:
        raise ValueError(f"Unsupported task configuration fields: {', '.join(sorted(unknown))}.")
    if missing:
        raise ValueError(f"Missing task configuration fields: {', '.join(sorted(missing))}.")
    return data


def _text(value: object, name: str, *, maximum: int = 8_192) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be non-empty text no longer than {maximum} characters.")
    return value.strip()


def validate_reminder(configuration: Mapping[str, Any]) -> dict[str, Any]:
    data = _mapping(configuration, {"title", "message", "severity"}, {"title", "message"})
    severity = str(data.get("severity") or "info").lower()
    if severity not in {"info", "warning"}:
        raise ValueError("Reminder severity must be info or warning.")
    return {"title": _text(data["title"], "title", maximum=240), "message": _text(data["message"], "message"), "severity": severity}


def validate_empty(configuration: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(configuration, set())


def validate_science(configuration: Mapping[str, Any]) -> dict[str, Any]:
    data = _mapping(configuration, {"max_items"}, {"max_items"})
    amount = data["max_items"]
    if type(amount) is not int or not 1 <= amount <= 1024:
        raise ValueError("max_items must be an integer from 1 through 1024.")
    return {"max_items": amount}


def validate_session(configuration: Mapping[str, Any], *, freshness: bool = False) -> dict[str, Any]:
    data = _mapping(configuration, {"session", "freshness_threshold_seconds"} if freshness else {"session"}, {"session"})
    session = str(data["session"]).upper()
    if session not in {"ASIA", "NEW_YORK"}:
        raise ValueError("session must be ASIA or NEW_YORK.")
    result = {"session": session}
    if freshness:
        threshold = data.get("freshness_threshold_seconds", 15)
        if type(threshold) not in {int, float} or not 1 <= float(threshold) <= 3600:
            raise ValueError("freshness_threshold_seconds must be between 1 and 3600.")
        result["freshness_threshold_seconds"] = float(threshold)
    return result


def _dependency(context: Any, name: str, default: Any = None) -> Any:
    value = context.dependencies.get(name, default)
    return value() if callable(value) else value


def operator_reminder(context: Any) -> TaskOutcome:
    config = context.configuration
    context.progress(1, 1, "NOTIFY", "Creating durable operator reminder.")
    context.notify(config["severity"], config["title"], config["message"])
    return TaskOutcome(result={"notification": "persisted", "title": config["title"], "severity": config["severity"]}, message="Operator reminder persisted.")


def health_snapshot(context: Any) -> TaskOutcome:
    context.progress(0, 1, "OBSERVE", "Collecting existing runtime health.")
    result = {
        "captured_at": utc_now().isoformat(),
        "control_center": _dependency(context, "control_center_health", {}),
        "watcher": _dependency(context, "watcher_health", {}),
        "ninjatrader_listener": _dependency(context, "ninja_listener_health", {}),
        "lane_iii": _dependency(context, "lane_iii_paper_health", {}),
        "scheduler": _dependency(context, "scheduler_status", {}),
    }
    context.progress(1, 1, "OBSERVE", "Health snapshot captured from existing runtimes.")
    return TaskOutcome(result=sanitized(result), message="Read-only health snapshot captured.")


def database_quick_check(context: Any) -> TaskOutcome:
    paths = _dependency(context, "database_paths", {})
    if not isinstance(paths, Mapping) or not paths:
        raise TaskBlocked("Application database paths are unavailable.")
    results: dict[str, str] = {}
    for index, (name, raw) in enumerate(paths.items(), start=1):
        path = Path(str(raw))
        if not path.exists():
            raise TaskBlocked(f"Configured {name} database is unavailable.")
        with sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True) as connection:
            row = connection.execute("PRAGMA quick_check").fetchone()
        check = str(row[0]) if row else "unknown"
        results[str(name)] = check
        context.progress(index, len(paths), "VERIFY_DATABASE", f"Quick check completed for {name}.")
        if check.lower() != "ok":
            raise TaskInvariantFailure(f"Database quick_check failed for {name}: {check}")
    return TaskOutcome(result={"quick_check": results}, message="Configured database quick checks passed.")


def science_run_once(context: Any) -> TaskOutcome:
    worker = _dependency(context, "scientific_worker")
    if worker is None or not callable(getattr(worker, "run_once", None)):
        raise TaskBlocked("The existing durable scientific worker is unavailable.")
    context.progress(0, 1, "SCIENTIFIC_WORK", "Invoking the existing durable scientific worker once.")
    result = worker.run_once(max_items=context.configuration["max_items"])
    context.progress(1, 1, "SCIENTIFIC_WORK", "Existing scientific worker completed one bounded pass.")
    return TaskOutcome(result=sanitized(result), message="Scientific worker run-once completed.", retryable=True)


def _session_context(session: str) -> tuple[dict[str, Any], str | None]:
    resolver = PaperSessionResolver()
    now = utc_now()
    desired = PaperSessionKind.ASIA_GLOBEX if session == "ASIA" else PaperSessionKind.NEW_YORK_RTH
    resolution = resolver.resolve(now)
    if resolution.context.session_kind is desired:
        return resolution.context.payload(), resolution.reason_code
    cursor = now
    for _ in range(12):
        candidate = resolver.next_valid_session(cursor)
        if candidate is None:
            break
        cursor = candidate.boundary_at("session_end")
        if candidate.session_kind is desired:
            return candidate.payload(), None
    return {}, "SESSION_CALENDAR_UNAVAILABLE"


def _authentic_observation_freshness(
    listener: Mapping[str, Any],
    threshold_seconds: float,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    raw = listener.get("last_observation_at")
    result: dict[str, Any] = {
        "timestamp": raw,
        "threshold_seconds": threshold_seconds,
        "age_seconds": None,
        "fresh": False,
        "reason": None,
    }
    if not isinstance(raw, str) or not raw.strip():
        result["reason"] = "MISSING_OBSERVATION_TIMESTAMP"
        return result
    try:
        observed_at = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        result["reason"] = "INVALID_OBSERVATION_TIMESTAMP"
        return result
    if observed_at.tzinfo is None:
        result["reason"] = "INVALID_OBSERVATION_TIMESTAMP"
        return result
    current = now or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age_seconds = (current.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds()
    result["age_seconds"] = age_seconds
    if age_seconds < 0:
        result["reason"] = "FUTURE_OBSERVATION_TIMESTAMP"
        return result
    if age_seconds > threshold_seconds:
        result["reason"] = "STALE_OBSERVATION_TIMESTAMP"
        return result
    result["fresh"] = True
    result["reason"] = "FRESH"
    return result


def session_readiness(context: Any) -> TaskOutcome:
    config = context.configuration
    requested = config["session"]
    session, calendar_reason = _session_context(requested)
    listener = _dependency(context, "ninja_listener_health", {}) or {}
    runtime = _dependency(context, "lane_iii_paper_health", {}) or {}
    observation_freshness = _authentic_observation_freshness(listener, config["freshness_threshold_seconds"])
    checks: dict[str, Any] = {
        "requested_session": requested,
        "session_resolution": session,
        "calendar_reason": calendar_reason,
        "listener_listening": listener.get("state") == "LISTENING",
        "listener_loopback": str(listener.get("host", "")) == "127.0.0.1",
        "runtime_available": runtime.get("state") not in {None, "UNSTARTED"},
        "sim101": runtime.get("paper_account") == "Sim101",
        "flat": runtime.get("current_position") == "FLAT" and float(runtime.get("current_quantity") or 0) == 0,
        "zero_working_orders": int(runtime.get("working_owned_orders") or 0) == 0,
        "disarmed": str(runtime.get("session_armed_state") or runtime.get("paper_execution") or "DISARMED") == "DISARMED",
        "live_authority_denied": runtime.get("live_capital", "DENIED") == "DENIED",
        "authentic_observation_fresh": observation_freshness["fresh"],
        "mnq_metadata_present": bool(runtime.get("market_instrument")),
    }
    context.progress(1, 1, "READINESS", "Read-only session readiness checks completed.")
    unmet = [name for name, value in checks.items() if name not in {"requested_session", "session_resolution", "calendar_reason"} and not value]
    if calendar_reason or not session:
        unmet.append(calendar_reason or "SESSION_CALENDAR_UNAVAILABLE")
    if unmet:
        return TaskOutcome(status="BLOCKED", result={"checks": checks, "observation_freshness": observation_freshness, "unmet": unmet}, message="Session readiness is blocked; manual arming remains unavailable to the scheduler.")
    return TaskOutcome(result={"checks": checks, "observation_freshness": observation_freshness}, message="Session is ready for operator review; scheduler did not arm it.")


def session_close_audit(context: Any) -> TaskOutcome:
    requested = context.configuration["session"]
    runtime = _dependency(context, "lane_iii_paper_health", {}) or {}
    listener = _dependency(context, "ninja_listener_health", {}) or {}
    checks = {
        "requested_session": requested,
        "runtime_available": runtime.get("state") not in {None, "UNSTARTED"},
        "listener_available": listener.get("state") == "LISTENING",
        "disarmed": str(runtime.get("session_armed_state") or runtime.get("paper_execution") or "DISARMED") == "DISARMED",
        "sim101_flat": runtime.get("current_position") == "FLAT" and float(runtime.get("current_quantity") or 0) == 0,
        "zero_working_orders": int(runtime.get("working_owned_orders") or 0) == 0,
        "live_authority_denied": runtime.get("live_capital", "DENIED") == "DENIED",
        "trade_date": runtime.get("trade_date"),
        "session_status": runtime.get("current_session"),
    }
    context.progress(1, 1, "CLOSE_AUDIT", "Read-only close audit completed.")
    unavailable = [name for name in ("runtime_available", "listener_available", "trade_date") if not checks[name]]
    violations = [name for name in ("disarmed", "sim101_flat", "zero_working_orders", "live_authority_denied") if not checks[name]]
    if unavailable:
        return TaskOutcome(status="BLOCKED", result={"checks": checks, "unavailable": unavailable}, message="Close audit evidence is unavailable.")
    if violations:
        return TaskOutcome(status="FAILED", result={"checks": checks, "violations": violations}, message="Close audit found a proved invariant violation.")
    return TaskOutcome(result={"checks": checks}, message="Read-only session close audit passed.")


def session_audit_export(context: Any) -> TaskOutcome:
    root = _dependency(context, "audit_export_directory")
    if root is None:
        raise TaskBlocked("Fixed audit export directory is unavailable.")
    directory = Path(str(root)).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    payload = sanitized({
        "schema": "beelzebub-scheduler-audit-v1",
        "run": context.run_snapshot(),
        "session": context.configuration["session"],
        "listener": _dependency(context, "ninja_listener_health", {}),
        "lane_iii": _dependency(context, "lane_iii_paper_health", {}),
        "scheduler": _dependency(context, "scheduler_status", {}),
        "exported_at": utc_now().isoformat(),
    })
    target = directory / f"{context.run_id}.json"
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    context.progress(1, 1, "EXPORT", "Sanitized read-only audit bundle exported.")
    return TaskOutcome(result={"audit_export_path": str(target)}, message="Sanitized audit bundle exported to the project-managed directory.")


def production_task_definitions() -> tuple[TaskDefinition, ...]:
    return (
        TaskDefinition("operator.reminder", "Operator reminder", "Creates a durable local reminder.", "Operations", AuthorityClassification.OPERATOR_NOTIFICATION, validate_reminder, operator_reminder, 30),
        TaskDefinition("system.health_snapshot", "System health snapshot", "Captures existing runtime health without starting components.", "System", AuthorityClassification.READ_ONLY, validate_empty, health_snapshot, 30),
        TaskDefinition("system.database_quick_check", "Database quick check", "Runs SQLite quick_check only against fixed configured databases.", "System", AuthorityClassification.READ_ONLY, validate_empty, database_quick_check, 60),
        TaskDefinition("science.run_once", "Scientific worker run once", "Invokes the existing durable scientific worker once.", "Science", AuthorityClassification.SCIENTIFIC_WRITE, validate_science, science_run_once, 300, retryable=True, required_resource_keys=("scientific_worker",)),
        TaskDefinition("lane_iii.session_readiness", "Lane III session readiness", "Read-only readiness evaluation; it cannot arm any session.", "Lane III", AuthorityClassification.READ_ONLY, lambda item: validate_session(item, freshness=True), session_readiness, 60),
        TaskDefinition("lane_iii.session_close_audit", "Lane III close audit", "Read-only post-session audit; it never corrects state.", "Lane III", AuthorityClassification.READ_ONLY, validate_session, session_close_audit, 60),
        TaskDefinition("lane_iii.session_audit_export", "Lane III audit export", "Exports a sanitized audit bundle to a fixed local directory.", "Lane III", AuthorityClassification.LOCAL_AUDIT_WRITE, validate_session, session_audit_export, 60),
    )
