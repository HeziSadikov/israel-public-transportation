from __future__ import annotations

from typing import Dict, List, Tuple, Any, Set

from shapely.geometry import shape, LineString
from shapely.prepared import prep

from .gtfs_loader import GTFSFeed
from .service_calendar import ServiceCalendar, parse_gtfs_time_to_seconds
from .config import TRIP_TIME_BOUNDS_CACHE, SHAPES_BY_ID_CACHE
from .sqlite_db import (
  get_trip_time_bounds_from_db,
  get_area_search_candidates,
  get_shape_ids_in_bbox,
  stream_trip_time_bounds,
)


def _feed_cache_key(feed: GTFSFeed) -> str:
  return getattr(feed, "version_id", "default")


def get_trip_time_bounds(feed: GTFSFeed) -> Dict[str, Tuple[int, int]]:
  """
  Returns a mapping trip_id -> (first_time_s, last_time_s) for the GTFS feed.

  Prefers the materialized trip_time_bounds table (fast). If missing or empty,
  falls back to scanning stop_times once and caches in TRIP_TIME_BOUNDS_CACHE.
  """
  key = _feed_cache_key(feed)
  cached = TRIP_TIME_BOUNDS_CACHE.get(key)
  if cached is not None:
    return cached

  bounds = get_trip_time_bounds_from_db()
  if bounds:
    TRIP_TIME_BOUNDS_CACHE[key] = bounds
    return bounds

  # Fallback: stream stop_times in chunks (old DBs without trip_time_bounds/dep_sec)
  bounds = stream_trip_time_bounds(parse_gtfs_time_to_seconds)
  TRIP_TIME_BOUNDS_CACHE[key] = bounds
  return bounds


def _get_shapes_by_id(feed: GTFSFeed) -> Dict[str, List[Dict]]:
  """Build or return cached shape_id -> sorted list of shape_pt rows."""
  key = _feed_cache_key(feed)
  cached = SHAPES_BY_ID_CACHE.get(key)
  if cached is not None:
    return cached
  out: Dict[str, List[Dict]] = {}
  for row in feed.shapes:
    sid = row.get("shape_id")
    if not sid:
      continue
    out.setdefault(sid, []).append(row)
  for sid, pts in out.items():
    pts.sort(key=lambda r: int(r.get("shape_pt_sequence", 0)))
  SHAPES_BY_ID_CACHE[key] = out
  return out


def find_routes_in_polygon(
  feed: GTFSFeed,
  polygon_geojson: Dict[str, Any],
  yyyymmdd: str,
  start_sec: int,
  end_sec: int,
) -> List[Dict[str, Any]]:
  """
  Returns a list of routes whose shapes intersect the given polygon and have
  at least one trip active on the given service date within the specified
  time window [start_sec, end_sec].

  Fast path (when trip_time_bounds + shape_bbox exist): filter candidates in
  SQL, then only build LineStrings and intersects for shape_ids in polygon bbox.
  Uses prepared polygon for repeated intersects. Falls back to Python iteration
  for older DBs.
  """
  poly = shape(polygon_geojson)
  if poly.is_empty:
    return []

  svc_cal = ServiceCalendar(feed)
  active_services = svc_cal.active_service_ids_for_date(yyyymmdd)
  if not active_services:
    return []

  shapes_by_id = _get_shapes_by_id(feed)
  routes_by_id: Dict[str, Dict] = {r["route_id"]: r for r in feed.routes}
  agencies_by_id: Dict[str, Dict] = {a.get("agency_id"): a for a in getattr(feed, "agencies", [])}
  poly_bounds = poly.bounds
  poly_prepared = prep(poly)

  # Build shape_id -> list of (route_id, direction_id, lo, hi)
  trip_index: Dict[str, List[Tuple[str, str, int, int]]] = {}

  # Fast path: candidates from SQL (active trips in time window)
  candidates_sql = get_area_search_candidates(list(active_services), start_sec, end_sec)
  if candidates_sql:
    for shape_id, route_id, direction_id, lo, hi in candidates_sql:
      trip_index.setdefault(shape_id, []).append((route_id, direction_id, lo, hi))

  if not trip_index:
    # Fallback: build trip_index in Python (old DB without trip_time_bounds)
    trip_time_bounds = get_trip_time_bounds(feed)
    for trip in feed.trips:
      if trip.get("service_id") not in active_services:
        continue
      trip_id = trip.get("trip_id")
      shape_id = trip.get("shape_id")
      route_id = trip.get("route_id")
      if not trip_id or not shape_id or not route_id:
        continue
      tb = trip_time_bounds.get(trip_id)
      if not tb:
        continue
      lo, hi = tb
      if hi < start_sec or lo > end_sec:
        continue
      direction_id = trip.get("direction_id") or ""
      trip_index.setdefault(shape_id, []).append((route_id, direction_id, lo, hi))

  if not trip_index:
    return []

  # Spatial prefilter: restrict to shape_ids whose bbox overlaps polygon (if we have shape_bbox)
  bbox_shape_ids: Set[str] = set(get_shape_ids_in_bbox(
    poly_bounds[0], poly_bounds[1], poly_bounds[2], poly_bounds[3]
  ))
  if bbox_shape_ids:
    candidate_shape_ids = [sid for sid in trip_index if sid in bbox_shape_ids]
  else:
    candidate_shape_ids = list(trip_index.keys())

  results: Dict[Tuple[str, str], Dict[str, Any]] = {}

  for shape_id in candidate_shape_ids:
    trip_infos = trip_index[shape_id]
    pts = shapes_by_id.get(shape_id)
    if not pts or len(pts) < 2:
      continue

    # Bbox pre-filter (when shape_bbox wasn't used, or double-check)
    try:
      lons = [float(p["shape_pt_lon"]) for p in pts]
      lats = [float(p["shape_pt_lat"]) for p in pts]
    except (KeyError, ValueError):
      continue
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    if (
      max_lon < poly_bounds[0]
      or min_lon > poly_bounds[2]
      or max_lat < poly_bounds[1]
      or min_lat > poly_bounds[3]
    ):
      continue

    line = LineString(list(zip(lons, lats)))
    if not poly_prepared.intersects(line):
      continue

    for route_id, direction_id, lo, hi in trip_infos:
      key = (route_id, direction_id)
      meta = routes_by_id.get(route_id, {})
      agency_meta = agencies_by_id.get(meta.get("agency_id")) if meta else None
      existing = results.get(key)
      if existing is None:
        results[key] = {
          "route_id": route_id,
          "direction_id": direction_id or None,
          "route_short_name": meta.get("route_short_name"),
          "route_long_name": meta.get("route_long_name"),
          "agency_id": meta.get("agency_id"),
          "agency_name": agency_meta.get("agency_name") if agency_meta else None,
          "first_time_s": lo,
          "last_time_s": hi,
        }
      else:
        if lo < existing["first_time_s"]:
          existing["first_time_s"] = lo
        if hi > existing["last_time_s"]:
          existing["last_time_s"] = hi

  return list(results.values())

