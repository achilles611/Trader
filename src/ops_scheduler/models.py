"""Shared scheduler types and strict boundary helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
from typing import Any, Mapping


class RunStatus(StrEnum):
    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"
    MISSED = "MISSED"
    INTERRUPTED = "INTERRUPTED"


class ScheduleLifecycle(StrEnum):
    ENABLED = "ENABLED"
    PAUSED = "PAUSED"
    ARCHIVED = "ARCHIVED"


class TriggerKind(StrEnum):
    ONCE = "ONCE"
    DAILY = "DAILY"
    WEEKDAYS = "WEEKDAYS"
    INTERVAL = "INTERVAL"
    SESSION_RELATIVE = "SESSION_RELATIVE"


class MissedRunPolicy(StrEnum):
    SKIP = "SKIP"
    RUN_ONCE_ON_RESTART = "RUN_ONCE_ON_RESTART"
    BOUNDED_CATCH_UP = "BOUNDED_CATCH_UP"


class RunOrigin(StrEnum):
    SCHEDULE = "SCHEDULE"
    MANUAL = "MANUAL"
    RETRY = "RETRY"
    COMMISSIONING = "COMMISSIONING"


class EngineState(StrEnum):
    LEADER = "LEADER"
    STANDBY = "STANDBY"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"


class AuthorityClassification(StrEnum):
    READ_ONLY = "READ_ONLY"
    LOCAL_AUDIT_WRITE = "LOCAL_AUDIT_WRITE"
    SCIENTIFIC_WRITE = "SCIENTIFIC_WRITE"
    OPERATOR_NOTIFICATION = "OPERATOR_NOTIFICATION"


TERMINAL_STATUSES = frozenset({
    RunStatus.COMPLETE, RunStatus.FAILED, RunStatus.BLOCKED, RunStatus.CANCELLED,
    RunStatus.MISSED, RunStatus.INTERRUPTED,
})
ALLOWED_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = {
    RunStatus.SCHEDULED: frozenset({RunStatus.QUEUED, RunStatus.MISSED, RunStatus.CANCELLED}),
    RunStatus.QUEUED: frozenset({RunStatus.ACTIVE, RunStatus.CANCELLED, RunStatus.BLOCKED}),
    RunStatus.ACTIVE: frozenset({RunStatus.COMPLETE, RunStatus.FAILED, RunStatus.BLOCKED, RunStatus.CANCELLED, RunStatus.INTERRUPTED}),
}
SENSITIVE_KEY_PARTS = frozenset({"password", "secret", "token", "private_key", "api_key", "credential", "authorization"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    moment = value or utc_now()
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("Scheduler timestamps must be timezone-aware.")
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Scheduler timestamps must include a timezone.")
    return parsed.astimezone(timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_key(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def assert_safe_payload(value: Any, *, path: str = "configuration") -> None:
    """Reject credential-like fields recursively before persistence or execution."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            text = str(key).lower()
            if any(marker in text for marker in SENSITIVE_KEY_PARTS):
                raise ValueError(f"{path} contains a sensitive-looking field.")
            assert_safe_payload(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            assert_safe_payload(item, path=f"{path}[{index}]")


def sanitized(value: Any, *, maximum_bytes: int = 262_144) -> Any:
    """Return a bounded redacted JSON-compatible value for audit storage."""
    def clean(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {
                str(key): "[REDACTED]" if any(marker in str(key).lower() for marker in SENSITIVE_KEY_PARTS) else clean(child)
                for key, child in item.items()
            }
        if isinstance(item, (list, tuple, set)):
            return [clean(child) for child in item]
        if isinstance(item, (str, int, float, bool)) or item is None:
            return item
        return str(item)
    result = clean(value)
    encoded = canonical_json(result).encode("utf-8")
    if len(encoded) <= maximum_bytes:
        return result
    return {"truncated": True, "original_bytes": len(encoded), "preview": encoded[:maximum_bytes].decode("utf-8", "replace")}


@dataclass(frozen=True)
class TaskOutcome:
    status: RunStatus = RunStatus.COMPLETE
    result: Mapping[str, Any] | None = None
    message: str = "Task completed."
    retryable: bool = False


class TaskBlocked(RuntimeError):
    """A valid scheduler task whose required external gate is unavailable."""


class TaskInvariantFailure(RuntimeError):
    """A task found proved evidence that violates a read-only invariant."""
