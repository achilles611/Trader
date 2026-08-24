"""Backend-only occurrence resolution, including DST and L3G session adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.l3g_paper.sessions import PaperSessionKind, PaperSessionResolver

from .models import TriggerKind, iso_utc, parse_utc


@dataclass(frozen=True)
class ResolvedOccurrence:
    due_at: datetime | None
    timezone: str
    local_time: str | None
    reason: str | None = None
    session: Mapping[str, object] | None = None

    def payload(self) -> dict[str, object]:
        return {
            "due_at": iso_utc(self.due_at) if self.due_at else None,
            "local_time": self.local_time,
            "timezone": self.timezone,
            "reason": self.reason,
            "session": dict(self.session or {}),
        }


def _zone(name: object) -> ZoneInfo:
    try:
        return ZoneInfo(str(name or "America/Denver"))
    except ZoneInfoNotFoundError as exc:
        raise ValueError("Scheduler timezone is unavailable.") from exc


def _local_datetime(day: datetime, raw_time: str, zone: ZoneInfo) -> ResolvedOccurrence:
    try:
        clock = time.fromisoformat(raw_time)
    except (TypeError, ValueError) as exc:
        raise ValueError("Trigger local_time must be HH:MM or HH:MM:SS.") from exc
    naive = datetime.combine(day.date(), clock)
    first = naive.replace(tzinfo=zone, fold=0)
    second = naive.replace(tzinfo=zone, fold=1)
    # A local time is valid only when an instant round-trips to that exact wall time.
    valid_first = first.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == naive
    valid_second = second.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) == naive
    if not valid_first and not valid_second:
        return ResolvedOccurrence(None, zone.key, naive.isoformat(), "DST_NONEXISTENT_LOCAL_TIME")
    # Deterministic fold policy: the earlier UTC instant (fold 0) is chosen.
    chosen = first if valid_first else second
    return ResolvedOccurrence(chosen.astimezone(timezone.utc), zone.key, chosen.isoformat(), "DST_AMBIGUOUS_FOLD_0" if valid_first and valid_second and first.utcoffset() != second.utcoffset() else None)


def validate_trigger(kind: str, specification: Mapping[str, Any], *, default_timezone: str = "America/Denver", production: bool = True) -> dict[str, Any]:
    try:
        trigger = TriggerKind(str(kind).upper())
    except ValueError as exc:
        raise ValueError("Unsupported scheduler trigger kind.") from exc
    spec = dict(specification or {})
    if trigger is TriggerKind.ONCE:
        if not isinstance(spec.get("local_datetime"), str):
            raise ValueError("ONCE requires local_datetime.")
        _zone(spec.get("timezone") or default_timezone)
        try:
            datetime.fromisoformat(str(spec["local_datetime"]))
        except ValueError as exc:
            raise ValueError("ONCE local_datetime must be ISO local date/time.") from exc
    elif trigger in {TriggerKind.DAILY, TriggerKind.WEEKDAYS}:
        if not isinstance(spec.get("local_time"), str):
            raise ValueError(f"{trigger.value} requires local_time.")
        _local_datetime(datetime.now(timezone.utc), str(spec["local_time"]), _zone(spec.get("timezone") or default_timezone))
        if trigger is TriggerKind.WEEKDAYS:
            days = spec.get("weekdays")
            if not isinstance(days, list) or not days or any(type(day) is not int or not 0 <= day <= 6 for day in days):
                raise ValueError("WEEKDAYS requires numeric weekdays from 0 (Monday) through 6 (Sunday).")
            spec["weekdays"] = sorted(set(days))
    elif trigger is TriggerKind.INTERVAL:
        parse_utc(str(spec.get("anchor_at_utc") or ""))
        seconds = spec.get("interval_seconds")
        if type(seconds) not in {int, float} or int(seconds) != seconds or int(seconds) < (60 if production else 1):
            raise ValueError("INTERVAL requires interval_seconds of at least one minute.")
        spec["interval_seconds"] = int(seconds)
    else:
        if str(spec.get("session") or "").upper() not in {"ASIA", "NEW_YORK"}:
            raise ValueError("SESSION_RELATIVE requires session ASIA or NEW_YORK.")
        if str(spec.get("event") or "").upper() not in {"OPEN", "CLOSE"}:
            raise ValueError("SESSION_RELATIVE requires event OPEN or CLOSE.")
        offset = spec.get("offset_minutes", 0)
        if type(offset) not in {int, float} or int(offset) != offset or abs(int(offset)) > 24 * 60:
            raise ValueError("SESSION_RELATIVE offset_minutes must be an integer within one day.")
        spec["session"] = str(spec["session"]).upper()
        spec["event"] = str(spec["event"]).upper()
        spec["offset_minutes"] = int(offset)
    spec.setdefault("timezone", default_timezone)
    return spec


def _session_occurrences(spec: Mapping[str, Any], after: datetime, count: int, resolver: PaperSessionResolver) -> Iterable[ResolvedOccurrence]:
    wanted = PaperSessionKind.ASIA_GLOBEX if str(spec["session"]) == "ASIA" else PaperSessionKind.NEW_YORK_RTH
    cursor = after
    produced = 0
    while produced < count:
        context = None
        # The public resolver is the only source of session/profile/holiday facts.
        for _ in range(16):
            candidate = resolver.next_valid_session(cursor)
            if candidate is None:
                yield ResolvedOccurrence(None, "America/New_York", None, "SESSION_CALENDAR_UNVERIFIED")
                return
            cursor = candidate.boundary_at("session_end") + timedelta(seconds=1)
            if candidate.session_kind is wanted:
                context = candidate
                break
        if context is None:
            yield ResolvedOccurrence(None, "America/New_York", None, "SESSION_NOT_RESOLVABLE")
            return
        boundary = "observation_start" if str(spec["event"]) == "OPEN" else "session_end"
        due = context.boundary_at(boundary) + timedelta(minutes=int(spec.get("offset_minutes", 0)))
        if due <= after:
            continue
        payload = context.payload()
        yield ResolvedOccurrence(due, str(payload["timezone"]), due.astimezone(ZoneInfo(str(payload["timezone"]))).isoformat(), None, payload)
        produced += 1


def resolve_occurrences(kind: str, specification: Mapping[str, Any], *, after: datetime, count: int = 5,
                        default_timezone: str = "America/Denver", resolver: PaperSessionResolver | None = None) -> list[ResolvedOccurrence]:
    """Return backend-resolved future occurrences; no frontend clock is authoritative."""
    spec = validate_trigger(kind, specification, default_timezone=default_timezone)
    trigger = TriggerKind(str(kind).upper())
    after = parse_utc(after)
    if count < 1:
        return []
    if trigger is TriggerKind.SESSION_RELATIVE:
        return list(_session_occurrences(spec, after, count, resolver or PaperSessionResolver()))
    if trigger is TriggerKind.INTERVAL:
        anchor = parse_utc(str(spec["anchor_at_utc"]))
        step = timedelta(seconds=int(spec["interval_seconds"]))
        elapsed = max(0, int((after - anchor).total_seconds() // step.total_seconds()) + 1)
        first = anchor + elapsed * step
        return [ResolvedOccurrence(first + index * step, "UTC", (first + index * step).isoformat(), None) for index in range(count)]
    zone = _zone(spec.get("timezone") or default_timezone)
    if trigger is TriggerKind.ONCE:
        raw = datetime.fromisoformat(str(spec["local_datetime"]))
        if raw.tzinfo is not None:
            raise ValueError("ONCE local_datetime must not include an offset; provide timezone separately.")
        occurrence = _local_datetime(raw, raw.time().isoformat(), zone)
        if occurrence.due_at is None:
            return [occurrence] if raw.replace(tzinfo=zone) > after.astimezone(zone) else []
        return [occurrence] if occurrence.due_at > after else []
    local_after = after.astimezone(zone)
    results: list[ResolvedOccurrence] = []
    day = local_after.replace(hour=0, minute=0, second=0, microsecond=0)
    for _ in range(366 * 3):
        if trigger is TriggerKind.WEEKDAYS and day.weekday() not in set(spec["weekdays"]):
            day += timedelta(days=1)
            continue
        occurrence = _local_datetime(day, str(spec["local_time"]), zone)
        if occurrence.due_at is None:
            # Preserve DST gaps in previews so the operator sees why a run will be missed.
            if day.date() >= local_after.date():
                results.append(occurrence)
        elif occurrence.due_at > after:
            results.append(occurrence)
        if len(results) >= count:
            return results
        day += timedelta(days=1)
    return results


def first_occurrence(kind: str, specification: Mapping[str, Any], *, created_at: datetime, default_timezone: str) -> ResolvedOccurrence | None:
    """Resolve a concrete first occurrence, including a past ONCE run for auditable missed handling."""
    if TriggerKind(str(kind).upper()) is TriggerKind.ONCE:
        spec = validate_trigger(kind, specification, default_timezone=default_timezone)
        zone = _zone(spec.get("timezone") or default_timezone)
        raw = datetime.fromisoformat(str(spec["local_datetime"]))
        return _local_datetime(raw, raw.time().isoformat(), zone)
    entries = resolve_occurrences(kind, specification, after=created_at - timedelta(microseconds=1), count=1,
                                  default_timezone=default_timezone)
    return entries[0] if entries else None
