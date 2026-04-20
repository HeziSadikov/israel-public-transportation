from __future__ import annotations

import time
from typing import Dict, List, Any, Optional, Tuple

from shapely.geometry import LineString, shape

from backend.infra.config import SHAPES_BY_ID_CACHE, TRIP_TIME_BOUNDS_CACHE
from backend.infra.db_access import get_routes_in_polygon_range
from backend.infra.logging_utils import log
from .service_calendar import ServiceCalendar, parse_gtfs_time_to_seconds


def get_trip_time_bounds(feed: Any) -> Dict[str, Tuple[int, int]]:
  """
  Return per-trip [first_departure_s, last_arrival_s] bounds.
  Results are memoized by feed source_path when available.
  """
  key = str(getattr(feed, "source_path", "") or "")
  if key and key in TRIP_TIME_BOUNDS_CACHE:
    return TRIP_TIME_BOUNDS_CACHE[key]

  bounds: Dict[str, Tuple[int, int]] = {}
  for row in getattr(feed, "stop_times", []) or []:
    trip_id = str(row.get("trip_id", "") or "")
    if not trip_id:
      continue
    dep = str(row.get("departure_time") or row.get("arrival_time") or "")
    arr = str(row.get("arrival_time") or row.get("departure_time") or "")
    if not dep or not arr:
      continue
    dep_s = parse_gtfs_time_to_seconds(dep)
    arr_s = parse_gtfs_time_to_seconds(arr)
    if trip_id not in bounds:
      bounds[trip_id] = (dep_s, arr_s)
    else:
      lo, hi = bounds[trip_id]
      bounds[trip_id] = (min(lo, dep_s), max(hi, arr_s))

  if key:
    TRIP_TIME_BOUNDS_CACHE[key] = bounds
  return bounds


def _get_shapes_by_id(feed: Any) -> Dict[str, List[Dict[str, Any]]]:
  """
  Group shapes by shape_id and sort by shape_pt_sequence.
  Results are memoized by feed source_path when available.
  """
  key = str(getattr(feed, "source_path", "") or "")
  if key and key in SHAPES_BY_ID_CACHE:
    return SHAPES_BY_ID_CACHE[key]

  grouped: Dict[str, List[Dict[str, Any]]] = {}
  for row in getattr(feed, "shapes", []) or []:
    sid = str(row.get("shape_id", "") or "")
    if not sid:
      continue
    grouped.setdefault(sid, []).append(row)

  for sid, rows in grouped.items():
    rows.sort(key=lambda r: int(str(r.get("shape_pt_sequence", "0") or "0")))
    grouped[sid] = rows

  if key:
    SHAPES_BY_ID_CACHE[key] = grouped
  return grouped


def _trip_overlaps_window(
  trip_bounds: Tuple[int, int],
  start_sec: int,
  end_sec: int,
) -> bool:
  lo, hi = trip_bounds
  return not (hi < start_sec or lo > end_sec)


def _in_memory_find_routes(
  feed: Any,
  polygon_geojson: Dict[str, Any],
  start_date_ymd: str,
  start_sec: int,
  end_date_ymd: str,
  end_sec: int,
) -> List[Dict[str, Any]]:
  poly = shape(polygon_geojson)
  if poly.is_empty:
    return []

  calendar_rows = getattr(feed, "calendar", []) or []
  calendar_date_rows = getattr(feed, "calendar_dates", []) or []
  if not calendar_rows and calendar_date_rows:
    # Compatibility behavior for fixture-only feeds with calendar_dates entries
    # but no calendar table rows: only explicit "added" services are active.
    def _active_from_calendar_dates(day_ymd: str) -> set[str]:
      out: set[str] = set()
      for row in calendar_date_rows:
        if str(row.get("date", "")) != str(day_ymd):
          continue
        if str(row.get("exception_type", "")) == "1":
          sid = str(row.get("service_id", "") or "")
          if sid:
            out.add(sid)
      return out

    active = _active_from_calendar_dates(start_date_ymd)
    if end_date_ymd != start_date_ymd:
      active = active.union(_active_from_calendar_dates(end_date_ymd))
  else:
    active = ServiceCalendar(feed).active_service_ids_for_date(start_date_ymd)
    if end_date_ymd != start_date_ymd:
      active = active.union(ServiceCalendar(feed).active_service_ids_for_date(end_date_ymd))
  if not active:
    return []

  trip_bounds = get_trip_time_bounds(feed)
  shapes_by_id = _get_shapes_by_id(feed)
  routes_by_id = {
    str(r.get("route_id", "")): r
    for r in (getattr(feed, "routes", []) or [])
    if r.get("route_id")
  }
  agencies_by_id = {
    str(a.get("agency_id", "")): a
    for a in (getattr(feed, "agencies", []) or [])
    if a.get("agency_id")
  }

  grouped: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
  for trip in getattr(feed, "trips", []) or []:
    service_id = str(trip.get("service_id", "") or "")
    if service_id not in active:
      continue
    trip_id = str(trip.get("trip_id", "") or "")
    if not trip_id or trip_id not in trip_bounds:
      continue
    bounds = trip_bounds[trip_id]
    if not _trip_overlaps_window(bounds, start_sec, end_sec):
      continue

    shape_id = str(trip.get("shape_id", "") or "")
    shape_rows = shapes_by_id.get(shape_id) if shape_id else None
    if not shape_rows or len(shape_rows) < 2:
      continue
    coords = []
    for row in shape_rows:
      lon = row.get("shape_pt_lon")
      lat = row.get("shape_pt_lat")
      if lon is None or lat is None:
        continue
      coords.append((float(lon), float(lat)))
    if len(coords) < 2:
      continue
    if not LineString(coords).intersects(poly):
      continue

    route_id = str(trip.get("route_id", "") or "")
    if not route_id:
      continue
    direction_id = None if trip.get("direction_id") in (None, "") else str(trip.get("direction_id"))
    key = (route_id, direction_id)
    route = routes_by_id.get(route_id, {})
    agency = agencies_by_id.get(str(route.get("agency_id", "") or ""), {})
    if key not in grouped:
      grouped[key] = {
        "route_id": route_id,
        "direction_id": direction_id,
        "route_short_name": route.get("route_short_name"),
        "route_long_name": route.get("route_long_name"),
        "agency_id": route.get("agency_id"),
        "agency_name": agency.get("agency_name"),
        "first_time_s": bounds[0],
        "last_time_s": bounds[1],
        "trip_count": 1,
        "last_stop_name": None,
      }
    else:
      row = grouped[key]
      row["first_time_s"] = min(int(row["first_time_s"]), bounds[0])
      row["last_time_s"] = max(int(row["last_time_s"]), bounds[1])
      row["trip_count"] = int(row["trip_count"]) + 1

  return list(grouped.values())


def find_routes_in_polygon(
  feed: Optional[Any],
  polygon_geojson: Dict[str, Any],
  start_date_ymd: str,
  start_sec: int,
  end_date_ymd: str,
  end_sec: int,
) -> List[Dict[str, Any]]:
  """
  Returns a list of routes whose shapes intersect the given polygon and have
  at least one trip active in the given datetime range.

  Implementation now delegates spatial search to PostGIS via db_access,
  using the active feed recorded in feed_versions. The previous SQLite +
  in-memory fallback has been removed.
  """
  if feed is not None:
    # Compatibility mode for unit tests and in-memory feed fixtures.
    return _in_memory_find_routes(
      feed=feed,
      polygon_geojson=polygon_geojson,
      start_date_ymd=start_date_ymd,
      start_sec=start_sec,
      end_date_ymd=end_date_ymd,
      end_sec=end_sec,
    )

  poly = shape(polygon_geojson)
  if poly.is_empty:
    return []

  # Keep ServiceCalendar around for potential future date-aware logic; current
  # PostGIS query already applies a service calendar + time-window filter.
  _ = ServiceCalendar  # silence unused-variable warnings

  wkt = poly.wkt
  t0 = time.perf_counter()
  log(
    "area/routes",
    f"phase=domain_pg_call_start wkt_chars={len(wkt)} "
    f"start_date={start_date_ymd} end_date={end_date_ymd} "
    f"start_sec={start_sec} end_sec={end_sec}",
  )
  rows = get_routes_in_polygon_range(
    polygon_wkt=wkt,
    start_date_ymd=start_date_ymd,
    start_sec=start_sec,
    end_date_ymd=end_date_ymd,
    end_sec=end_sec,
  )
  elapsed_ms = int((time.perf_counter() - t0) * 1000)
  log(
    "area/routes",
    f"phase=domain_pg_call_done elapsed_ms={elapsed_ms} route_rows={len(rows)}",
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
      "trip_count": r.trip_count,
      "last_stop_name": r.last_stop_name,
    }
    for r in rows
  ]

