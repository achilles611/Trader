"""Immutable timezone-aware session regimes for Sim101 paper operation.

This module deliberately sits outside of :mod:`src.lane_iii`.  Lane III
remains a frozen scientific/observation boundary; these profiles are a paper
execution envelope which must never be mistaken for a market-data contract.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping
from dateutil import tz

from src.lane_iii.contracts import canonical_hash


NEW_YORK_TIMEZONE = "America/New_York"
LONDON_TIMEZONE = "Europe/London"
PAPER_SESSION_CONTRACT = "MNQU6"
_NY = tz.gettz(NEW_YORK_TIMEZONE)
if _NY is None:  # Never substitute a fixed offset for a market timezone.
    raise RuntimeError("America/New_York timezone data is unavailable.")
_LONDON = tz.gettz(LONDON_TIMEZONE)
if _LONDON is None:  # Never substitute a fixed offset for London civil time.
    raise RuntimeError("Europe/London timezone data is unavailable.")


class PaperSessionKind(StrEnum):
    # Asia is a single first-class paper session.  The alias preserves only
    # source compatibility for callers which imported the former enum name;
    # every serialized identity is now exactly ASIA.
    ASIA = "ASIA"
    ASIA_GLOBEX = "ASIA"
    LONDON = "LONDON"
    NEW_YORK_RTH = "NEW_YORK_RTH"
    NY_AFTER = "NY_AFTER"
    OFF_SESSION = "OFF_SESSION"


class PaperSessionFamily(StrEnum):
    NEW_YORK = "NEW_YORK"
    EUROPE = "EUROPE"
    ASIA = "ASIA"
    OFF_SESSION = "OFF_SESSION"


def session_family(kind: PaperSessionKind) -> PaperSessionFamily:
    """Return the sealed accounting family for one exact paper session."""
    if kind in {PaperSessionKind.NEW_YORK_RTH, PaperSessionKind.NY_AFTER}:
        return PaperSessionFamily.NEW_YORK
    if kind is PaperSessionKind.LONDON:
        return PaperSessionFamily.EUROPE
    if kind is PaperSessionKind.ASIA:
        return PaperSessionFamily.ASIA
    if kind is PaperSessionKind.OFF_SESSION:
        return PaperSessionFamily.OFF_SESSION
    raise ValueError("Unknown paper session kind.")


class PaperCalendarState(StrEnum):
    NORMAL = "NORMAL"
    HOLIDAY_OVERRIDE_VERIFIED = "HOLIDAY_OVERRIDE_VERIFIED"
    HOLIDAY_OVERRIDE_REQUIRED = "HOLIDAY_OVERRIDE_REQUIRED"
    CLOSED = "CLOSED"


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    """Return a calendar date without depending on a host holiday package."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        following = date(year + 1, 1, 1)
    else:
        following = date(year, month + 1, 1)
    candidate = following - timedelta(days=1)
    return candidate - timedelta(days=(candidate.weekday() - weekday) % 7)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    actual = date(year, month, day)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _gregorian_easter(year: int) -> date:
    """Meeus/Jones/Butcher Gregorian Easter calculation."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    return date(year, (h + l - 7 * m + 114) // 31, (h + l - 7 * m + 114) % 31 + 1)


def _conservative_holiday_fence_dates(year: int) -> frozenset[date]:
    """Known US market-holiday candidates, deliberately not a CME timetable.

    Any date returned here requires an operator's explicit verified override.
    The extra early-close candidates intentionally favour a paper-session
    denial over assuming regular CME hours.
    """
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    fixed = {
        _observed_fixed_holiday(year, 1, 1),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _observed_fixed_holiday(year, 12, 25),
        date(year, 12, 24),       # possible Christmas Eve early close
        date(year, 12, 31),       # possible New Year's Eve early close
    }
    variable = {
        _nth_weekday(year, 1, 0, 3),   # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),   # Presidents Day
        _gregorian_easter(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),     # Memorial Day
        _nth_weekday(year, 9, 0, 1),   # Labor Day
        thanksgiving,
        thanksgiving + timedelta(days=1),  # possible day-after early close
    }
    return frozenset(fixed | variable)


def _requires_holiday_override(trade_date: date) -> bool:
    # Include adjacent years because a New Year's observed date can fall on
    # December 31 of the prior calendar year.
    return any(trade_date in _conservative_holiday_fence_dates(year) for year in {
        trade_date.year - 1, trade_date.year, trade_date.year + 1,
    })


def _clock(value: str) -> time:
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Session clock must be HH:MM.") from exc
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise ValueError("Session clock must be an exact minute in local time.")
    return parsed


@dataclass(frozen=True)
class PaperSessionProfile:
    """A sealed market regime expressed in its canonical IANA timezone."""

    session_kind: PaperSessionKind
    timezone: str
    observation_start: str
    entry_start: str
    entry_cutoff: str
    hard_flat_deadline: str
    session_end: str
    valid_start_weekdays: tuple[int, ...]

    def __post_init__(self) -> None:
        expected_timezone = LONDON_TIMEZONE if self.session_kind is PaperSessionKind.LONDON else NEW_YORK_TIMEZONE
        if self.timezone != expected_timezone:
            raise ValueError("Paper session timezone does not match its compiled definition.")
        for value in (self.observation_start, self.entry_start, self.entry_cutoff, self.hard_flat_deadline, self.session_end):
            _clock(value)
        if self.session_kind is PaperSessionKind.OFF_SESSION:
            if self.valid_start_weekdays:
                raise ValueError("OFF_SESSION cannot have a market schedule.")
        elif not self.valid_start_weekdays or any(type(day) is not int or day < 0 or day > 6 for day in self.valid_start_weekdays):
            raise ValueError("A market session requires explicit valid start weekdays.")

    @property
    def profile_hash(self) -> str:
        payload = asdict(self)
        payload["session_kind"] = self.session_kind.value
        return canonical_hash(payload)

    def payload(self) -> dict[str, object]:
        return {
            "session_kind": self.session_kind.value,
            "timezone": self.timezone,
            "observation_start": self.observation_start,
            "entry_start": self.entry_start,
            "entry_cutoff": self.entry_cutoff,
            "hard_flat_deadline": self.hard_flat_deadline,
            "session_end": self.session_end,
            "valid_start_weekdays": list(self.valid_start_weekdays),
            "session_profile_hash": self.profile_hash,
        }


ASIA_PROFILE = PaperSessionProfile(
    PaperSessionKind.ASIA, NEW_YORK_TIMEZONE,
    "18:00", "18:05", "01:30", "01:58", "02:00", (6, 0, 1, 2, 3),
)
# Compatibility symbol only; its identity remains ASIA rather than a
# separately serialized pre-/post-Globex regime.
ASIA_GLOBEX_PROFILE = ASIA_PROFILE
LONDON_PROFILE = PaperSessionProfile(
    PaperSessionKind.LONDON, LONDON_TIMEZONE,
    "08:00", "08:00", "11:30", "11:30", "11:30", (0, 1, 2, 3, 4),
)
NEW_YORK_RTH_PROFILE = PaperSessionProfile(
    PaperSessionKind.NEW_YORK_RTH, NEW_YORK_TIMEZONE,
    "09:30", "09:35", "15:30", "15:58", "16:00", (0, 1, 2, 3, 4),
)
NY_AFTER_PROFILE = PaperSessionProfile(
    PaperSessionKind.NY_AFTER, NEW_YORK_TIMEZONE,
    "16:00", "16:05", "17:30", "17:58", "18:00", (0, 1, 2, 3),
)
OFF_SESSION_PROFILE = PaperSessionProfile(
    PaperSessionKind.OFF_SESSION, NEW_YORK_TIMEZONE,
    "00:00", "00:00", "00:00", "00:00", "00:00", (),
)
SESSION_PROFILES: Mapping[PaperSessionKind, PaperSessionProfile] = MappingProxyType({
    PaperSessionKind.ASIA: ASIA_PROFILE,
    PaperSessionKind.LONDON: LONDON_PROFILE,
    PaperSessionKind.NEW_YORK_RTH: NEW_YORK_RTH_PROFILE,
    PaperSessionKind.NY_AFTER: NY_AFTER_PROFILE,
    PaperSessionKind.OFF_SESSION: OFF_SESSION_PROFILE,
})
SESSION_PRECEDENCE: tuple[PaperSessionKind, ...] = (
    PaperSessionKind.ASIA,
    PaperSessionKind.LONDON,
    PaperSessionKind.NEW_YORK_RTH,
    PaperSessionKind.NY_AFTER,
)


def session_catalog() -> tuple[dict[str, object], ...]:
    """Serialize every first-class operational session and its precedence."""
    return tuple({
        **SESSION_PROFILES[kind].payload(),
        "session_family": session_family(kind).value,
        "precedence": index,
    } for index, kind in enumerate(SESSION_PRECEDENCE, start=1))


@dataclass(frozen=True)
class PaperSessionContext:
    """The immutable identity which accompanies every paper-path artifact."""

    session_kind: PaperSessionKind
    session_id: str
    trade_date: str
    timezone: str
    observation_start: str
    entry_start: str
    entry_cutoff: str
    hard_flat_deadline: str
    session_end: str
    session_profile_hash: str
    session_generation: int
    calendar_state: PaperCalendarState = PaperCalendarState.NORMAL

    def __post_init__(self) -> None:
        profile = SESSION_PROFILES[self.session_kind]
        if self.timezone != profile.timezone:
            raise ValueError("Paper session context timezone is immutable.")
        try:
            date.fromisoformat(self.trade_date)
        except ValueError as exc:
            raise ValueError("Paper session trade_date must be ISO calendar date.") from exc
        if type(self.session_generation) is not int or self.session_generation < 0:
            raise ValueError("Paper session generation must be a non-negative integer.")
        if self.session_profile_hash != profile.profile_hash:
            raise ValueError("Paper session profile hash does not match the compiled profile.")
        if self.session_kind is PaperSessionKind.OFF_SESSION:
            if not self.session_id.startswith(f"{PAPER_SESSION_CONTRACT}:OFF_SESSION:"):
                raise ValueError("OFF_SESSION identity is malformed.")
        elif self.session_id != f"{PAPER_SESSION_CONTRACT}:{self.session_kind.value}:{self.trade_date}":
            raise ValueError("Paper market session ID is malformed.")
        if (self.observation_start, self.entry_start, self.entry_cutoff, self.hard_flat_deadline, self.session_end) != (
            profile.observation_start, profile.entry_start, profile.entry_cutoff, profile.hard_flat_deadline, profile.session_end,
        ):
            raise ValueError("Paper session context cannot alter a compiled profile.")

    def payload(self) -> dict[str, object]:
        return {
            "session_kind": self.session_kind.value,
            "session_family": self.session_family.value,
            "session_id": self.session_id,
            "trade_date": self.trade_date,
            "timezone": self.timezone,
            "observation_start": self.observation_start,
            "entry_start": self.entry_start,
            "entry_cutoff": self.entry_cutoff,
            "hard_flat_deadline": self.hard_flat_deadline,
            "session_end": self.session_end,
            "session_profile_hash": self.session_profile_hash,
            "session_generation": self.session_generation,
            "calendar_state": self.calendar_state.value,
        }

    @property
    def session_family(self) -> PaperSessionFamily:
        return session_family(self.session_kind)

    @property
    def entry_authorized_by_calendar(self) -> bool:
        return self.session_kind is not PaperSessionKind.OFF_SESSION and self.calendar_state in {
            PaperCalendarState.NORMAL, PaperCalendarState.HOLIDAY_OVERRIDE_VERIFIED,
        }

    def boundary_at(self, name: str) -> datetime:
        if name not in {"observation_start", "entry_start", "entry_cutoff", "hard_flat_deadline", "session_end"}:
            raise ValueError("Unknown paper session boundary.")
        if self.session_kind is PaperSessionKind.OFF_SESSION:
            raise ValueError("OFF_SESSION has no operational boundary.")
        boundary = _clock(str(getattr(self, name)))
        trading_date = date.fromisoformat(self.trade_date)
        # Asia starts on the evening before its trade date. All of its other
        # boundaries occur on the trade date after midnight.
        if self.session_kind is PaperSessionKind.ASIA and name in {"observation_start", "entry_start"}:
            trading_date -= timedelta(days=1)
        session_zone = _LONDON if self.timezone == LONDON_TIMEZONE else _NY
        return datetime.combine(trading_date, boundary, tzinfo=session_zone).astimezone(timezone.utc)

    def entry_permitted_at(self, moment: datetime) -> bool:
        if not self.entry_authorized_by_calendar:
            return False
        current = _aware_utc(moment)
        return self.boundary_at("entry_start") <= current < self.boundary_at("entry_cutoff")

    def hard_flat_due_at(self, moment: datetime) -> bool:
        return self.session_kind is not PaperSessionKind.OFF_SESSION and _aware_utc(moment) >= self.boundary_at("hard_flat_deadline")


UNSPECIFIED_OFF_SESSION_CONTEXT = PaperSessionContext(
    PaperSessionKind.OFF_SESSION, f"{PAPER_SESSION_CONTRACT}:OFF_SESSION:1970-01-01",
    "1970-01-01", NEW_YORK_TIMEZONE, "00:00", "00:00", "00:00", "00:00", "00:00",
    OFF_SESSION_PROFILE.profile_hash, 0, PaperCalendarState.CLOSED,
)


def context_from_identity(
    session_kind: PaperSessionKind,
    session_id: str,
    trade_date: str,
    session_profile_hash: str,
    session_generation: int,
    *,
    calendar_state: PaperCalendarState = PaperCalendarState.NORMAL,
) -> PaperSessionContext:
    """Rehydrate and validate the compact identity carried by paper records."""
    profile = SESSION_PROFILES[session_kind]
    return PaperSessionContext(
        session_kind, session_id, trade_date, profile.timezone, profile.observation_start,
        profile.entry_start, profile.entry_cutoff, profile.hard_flat_deadline, profile.session_end,
        session_profile_hash, session_generation, calendar_state,
    )


@dataclass(frozen=True)
class PaperSessionResolution:
    context: PaperSessionContext
    entry_authorized: bool
    reason_code: str | None = None


class PaperSessionCalendar:
    """Explicit exception state with a conservative US-holiday fence.

    The built-in fence is not a CME hours feed. It marks holiday and likely
    early-close candidates as unverified, so an operator must record a
    verified override before this paper path can enter during them.
    """

    def __init__(self, overrides: Mapping[str | date, PaperCalendarState] | None = None) -> None:
        values: dict[str, PaperCalendarState] = {}
        for raw_date, state in (overrides or {}).items():
            text = raw_date.isoformat() if isinstance(raw_date, date) else str(raw_date)
            date.fromisoformat(text)
            if type(state) is not PaperCalendarState:
                raise ValueError("Calendar overrides require explicit PaperCalendarState values.")
            values[text] = state
        self._overrides = MappingProxyType(values)

    def state_for(self, trade_date: date) -> PaperCalendarState:
        explicit = self._overrides.get(trade_date.isoformat())
        if explicit is not None:
            return explicit
        if _requires_holiday_override(trade_date):
            return PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED
        return PaperCalendarState.NORMAL


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Paper market event timestamps must be timezone-aware.")
    return value.astimezone(timezone.utc)


def parse_market_event_time(value: str | datetime) -> datetime:
    """Parse one actual event timestamp; naïve/ambiguous local input is denied."""
    if isinstance(value, datetime):
        return _aware_utc(value)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Paper market event timestamp is required.")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Paper market event timestamp is invalid.") from exc
    return _aware_utc(parsed)


class PaperSessionResolver:
    """Classifies event time only, retaining a fail-closed backward-time fence.

    Precedence is deterministic: ASIA, LONDON, NEW_YORK_RTH, then NY_AFTER.
    The V1 windows do not ordinarily overlap, but this order is sealed so a
    future calendar or timezone-rule change cannot silently change identity.
    """

    def __init__(self, calendar: PaperSessionCalendar | None = None) -> None:
        self.calendar = calendar or PaperSessionCalendar()
        self._last_event_at: datetime | None = None

    @staticmethod
    def _off_context(local: datetime, generation: int, state: PaperCalendarState) -> PaperSessionContext:
        return PaperSessionContext(
            PaperSessionKind.OFF_SESSION,
            f"{PAPER_SESSION_CONTRACT}:OFF_SESSION:{local.date().isoformat()}",
            local.date().isoformat(), NEW_YORK_TIMEZONE, "00:00", "00:00", "00:00", "00:00", "00:00",
            OFF_SESSION_PROFILE.profile_hash, generation, state,
        )

    def resolve(self, event_timestamp: str | datetime | None, *, generation: int = 0) -> PaperSessionResolution:
        if event_timestamp is None:
            return PaperSessionResolution(UNSPECIFIED_OFF_SESSION_CONTEXT, False, "EVENT_TIMESTAMP_MISSING")
        try:
            moment = parse_market_event_time(event_timestamp)
        except ValueError:
            return PaperSessionResolution(UNSPECIFIED_OFF_SESSION_CONTEXT, False, "EVENT_TIMESTAMP_INVALID")
        if self._last_event_at is not None and moment < self._last_event_at:
            return PaperSessionResolution(UNSPECIFIED_OFF_SESSION_CONTEXT, False, "EVENT_TIMESTAMP_MOVED_BACKWARD")
        self._last_event_at = moment
        local = moment.astimezone(_NY)
        current = local.timetz().replace(tzinfo=None)
        london_local = moment.astimezone(_LONDON)
        london_current = london_local.timetz().replace(tzinfo=None)

        profile: PaperSessionProfile | None = None
        trade_day: date | None = None
        if current >= _clock("18:00") and local.weekday() in ASIA_PROFILE.valid_start_weekdays:
            profile, trade_day = ASIA_PROFILE, local.date() + timedelta(days=1)
        elif current < _clock("02:00") and (local.date() - timedelta(days=1)).weekday() in ASIA_PROFILE.valid_start_weekdays:
            profile, trade_day = ASIA_PROFILE, local.date()
        elif _clock("08:00") <= london_current < _clock("11:30") and london_local.weekday() in LONDON_PROFILE.valid_start_weekdays:
            profile, trade_day = LONDON_PROFILE, london_local.date()
        elif _clock("09:30") <= current < _clock("16:00") and local.weekday() in NEW_YORK_RTH_PROFILE.valid_start_weekdays:
            profile, trade_day = NEW_YORK_RTH_PROFILE, local.date()
        elif _clock("16:00") <= current < _clock("18:00") and local.weekday() in NY_AFTER_PROFILE.valid_start_weekdays:
            profile, trade_day = NY_AFTER_PROFILE, local.date()
        if profile is None or trade_day is None:
            closed = PaperCalendarState.CLOSED if local.weekday() == 5 else self.calendar.state_for(local.date())
            return PaperSessionResolution(self._off_context(local, generation, closed), False, "OFF_SESSION")
        state = self.calendar.state_for(trade_day)
        context = PaperSessionContext(
            profile.session_kind, f"{PAPER_SESSION_CONTRACT}:{profile.session_kind.value}:{trade_day.isoformat()}",
            trade_day.isoformat(), profile.timezone, profile.observation_start, profile.entry_start,
            profile.entry_cutoff, profile.hard_flat_deadline, profile.session_end, profile.profile_hash,
            generation, state,
        )
        if state is PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED:
            return PaperSessionResolution(context, False, "HOLIDAY_SESSION_UNVERIFIED")
        if state is PaperCalendarState.CLOSED:
            return PaperSessionResolution(context, False, "SESSION_CLOSED")
        return PaperSessionResolution(context, context.entry_permitted_at(moment), None)

    def next_valid_session(self, after: str | datetime, *, generation: int = 0) -> PaperSessionContext | None:
        """Find the next observation start without inferring exceptional hours."""
        try:
            moment = parse_market_event_time(after)
        except ValueError:
            return None
        # Bounded seven-calendar-day scan covers a weekend plus any explicit
        # CLOSED override while keeping status calculation deterministic.
        candidate = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(7 * 24 * 60):
            resolution = self.resolve(candidate, generation=generation)
            if resolution.context.session_kind is not PaperSessionKind.OFF_SESSION:
                start = resolution.context.boundary_at("observation_start")
                if start >= moment and resolution.context.calendar_state in {
                    PaperCalendarState.NORMAL, PaperCalendarState.HOLIDAY_OVERRIDE_VERIFIED,
                }:
                    return resolution.context
            candidate += timedelta(minutes=1)
        return None
