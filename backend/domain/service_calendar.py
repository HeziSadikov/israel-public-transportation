from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Set, Any, Optional


@dataclass
class ServiceCalendar:
    """
    Kept for backwards compatibility; now used with dict-like feed objects
    (e.g. GTFSFeed from PostGIS) rather than a concrete gtfs_loader type.
    """
    feed: Any
    # Simple per-date memoization to avoid recomputing active service sets for the
    # same yyyymmdd repeatedly (useful for bulk pattern builds).
    _cache: Dict[str, Set[str]] = None  # type: ignore[assignment]

    def active_service_ids_for_date(self, yyyymmdd: str) -> Set[str]:
        """
        Returns service_ids active on the given service day (YYYYMMDD).

        Behaviour:
        - When calendar.txt is present and non-empty, use calendar/calendar_dates
          rules (precise and often faster by excluding inactive services).
        - When calendar is missing or empty, fall back to all service_ids that
          appear in trips (avoids "no patterns found" when calendar is incomplete).
        """
        if self._cache is None:
            self._cache = {}
        if yyyymmdd in self._cache:
            return self._cache[yyyymmdd].copy()

        # Use real calendar when available (precision + often fewer trips considered).
        calendar = getattr(self.feed, "calendar", None)
        if calendar is not None and len(calendar) > 0:
            active = self._active_from_calendar(yyyymmdd)
            self._cache[yyyymmdd] = set(active)
            return active

        # Fallback for feeds without calendar: all service_ids that appear in trips.
        if hasattr(self.feed, "trips") and self.feed.trips:
            active = {t["service_id"] for t in self.feed.trips if t.get("service_id")}
            self._cache[yyyymmdd] = set(active)
            return active
        self._cache[yyyymmdd] = set()
        return set()

    def _active_from_calendar(self, yyyymmdd: str) -> Set[str]:
        """Compute active service_ids from calendar.txt and calendar_dates.txt."""
        d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        active: Set[str] = set()
        calendar_by_id: Dict[str, Dict] = {}

        for row in getattr(self.feed, "calendar", []) or []:
            calendar_by_id[row["service_id"]] = row

        # Base on calendar.txt
        for service_id, row in calendar_by_id.items():
            start_date = _parse_yyyymmdd(row["start_date"])
            end_date = _parse_yyyymmdd(row["end_date"])
            if not (start_date <= d <= end_date):
                continue
            weekday_name = d.strftime("%A").lower()
            if row.get(weekday_name) == "1":
                active.add(service_id)

        # Apply calendar_dates.txt exceptions
        for row in getattr(self.feed, "calendar_dates", []) or []:
            sd = _parse_yyyymmdd(row["date"])
            if sd != d:
                continue
            sid = row["service_id"]
            exception_type = row.get("exception_type")
            if exception_type == "1":
                active.add(sid)
            elif exception_type == "2" and sid in active:
                active.remove(sid)

        return active


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def parse_gtfs_time_to_seconds(t: str) -> int:
    """
    Parses GTFS time strings including extended 24-27 hour values.
    Returns seconds since 00:00 of the service day.
    """
    if not t:
        return 0
    parts = t.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid GTFS time: {t}")
    h, m, s = map(int, parts)
    # Allow 24-27 hour values for Israel GTFS
    return h * 3600 + m * 60 + s


def default_profile_for_date(yyyymmdd: str) -> str:
    """
    Resolve a baseline service profile from day-of-week.
    """
    d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    dow = d.weekday()  # Mon=0 ... Sun=6
    if dow == 4:
        return "friday"
    if dow == 5:
        return "saturday"
    if dow == 6:
        return "sunday"
    return "weekday"


def date_has_calendar_exception(yyyymmdd: str, feed: Optional[Any] = None) -> bool:
    """
    Return True when calendar_dates has any exception for this date.
    """
    if feed is not None:
        for row in getattr(feed, "calendar_dates", []) or []:
            if str(row.get("date", "")) == str(yyyymmdd):
                return True
        return False
    # DB fallback (PostGIS path).
    try:
        from backend.infra import db_access

        return db_access.has_calendar_exception_for_date(yyyymmdd)
    except Exception:
        return False


def resolve_service_profile(yyyymmdd: str, feed: Optional[Any] = None) -> str:
    """
    Return profile key used for graph cache routing.

    - Normal days -> weekday/friday/saturday/sunday
    - Special/holiday (calendar_dates exception exists) -> special:YYYYMMDD
    """
    base = default_profile_for_date(yyyymmdd)
    if date_has_calendar_exception(yyyymmdd, feed=feed):
        return f"special:{yyyymmdd}"
    return base

