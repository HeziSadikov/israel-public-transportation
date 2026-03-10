from __future__ import annotations

from typing import Dict, List, Any, Optional

from shapely.geometry import shape

from .gtfs_loader import GTFSFeed
from .service_calendar import ServiceCalendar
from .db_access import get_routes_in_polygon


def find_routes_in_polygon(
  feed: Optional[GTFSFeed],
  polygon_geojson: Dict[str, Any],
  yyyymmdd: str,
  start_sec: int,
  end_sec: int,
) -> List[Dict[str, Any]]:
  """
  Returns a list of routes whose shapes intersect the given polygon and have
  at least one trip active on the given service date within the specified
  time window [start_sec, end_sec].

  Implementation now delegates spatial search to PostGIS via db_access,
  using the active feed recorded in feed_versions. The previous SQLite +
  in-memory fallback has been removed.
  """
  poly = shape(polygon_geojson)
  if poly.is_empty:
    return []

  # Keep ServiceCalendar around for potential future date-aware logic; current
  # PostGIS query already applies a service calendar + time-window filter.
  _ = ServiceCalendar  # silence unused-variable warnings

  wkt = poly.wkt
  rows = get_routes_in_polygon(
    polygon_wkt=wkt,
    date_ymd=yyyymmdd,
    start_sec=start_sec,
    end_sec=end_sec,
  )
  return [
    {
      "route_id": r.route_id,
      "direction_id": r.direction_id,
      "route_short_name": r.route_short_name,
      "route_long_name": r.route_long_name,
      "agency_id": r.agency_id,
      "agency_name": r.agency_name,
      "first_time_s": r.first_time_s,
      "last_time_s": r.last_time_s,
    }
    for r in rows
  ]

