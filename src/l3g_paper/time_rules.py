"""Portable America/New_York conversion, including Windows without tzdata."""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def america_new_york(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timezone-aware input is required.")
    try:
        return value.astimezone(ZoneInfo("America/New_York"))
    except ZoneInfoNotFoundError:
        # US post-2007 DST: second Sunday in March at 07:00 UTC through
        # first Sunday in November at 06:00 UTC. This keeps the commissioning
        # risk clock deterministic on Windows hosts without an IANA database.
        utc = value.astimezone(timezone.utc)
        march = calendar.monthcalendar(utc.year, 3)
        march_sundays = [week[calendar.SUNDAY] for week in march if week[calendar.SUNDAY]]
        november = calendar.monthcalendar(utc.year, 11)
        november_sundays = [week[calendar.SUNDAY] for week in november if week[calendar.SUNDAY]]
        dst_start = datetime(utc.year, 3, march_sundays[1], 7, tzinfo=timezone.utc)
        dst_end = datetime(utc.year, 11, november_sundays[0], 6, tzinfo=timezone.utc)
        offset = -4 if dst_start <= utc < dst_end else -5
        return utc.astimezone(timezone(timedelta(hours=offset), name="America/New_York"))

