"""
Stops in map bounds and routes serving a stop in a time window.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .gtfs_loader import GTFSFeed
from .service_calendar import ServiceCalendar, parse_gtfs_time_to_seconds
from .sqlite_db import get_conn


def get_stops_in_bounds(
    feed: GTFSFeed,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Returns stops whose lat/lon fall inside the given bounding box.
    """
    out: List[Dict[str, Any]] = []
    for s in feed.stops:
        if len(out) >= limit:
            break
        try:
            lat = float(s.get("stop_lat", 0))
            lon = float(s.get("stop_lon", 0))
        except (TypeError, ValueError):
            continue
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            out.append({
                "stop_id": s.get("stop_id"),
                "stop_name": s.get("stop_name"),
                "stop_code": s.get("stop_code"),
                "stop_lat": lat,
                "stop_lon": lon,
            })
    return out


def get_routes_serving_stop(
    feed: GTFSFeed,
    stop_id: str,
    yyyymmdd: str,
    start_sec: int,
    end_sec: int,
    max_results: int = 100,
) -> List[Dict[str, Any]]:
    """
    Returns routes that serve the given stop on the given date with at least
    one trip calling at that stop between start_sec and end_sec (seconds since
    midnight).
    """
    calendar = ServiceCalendar(feed)
    active_services = calendar.active_service_ids_for_date(yyyymmdd)
    if not active_services:
        return []

    trips_by_id = {t["trip_id"]: t for t in feed.trips}
    routes_by_id = {r["route_id"]: r for r in feed.routes}
    agencies_by_id = {a.get("agency_id"): a for a in getattr(feed, "agencies", [])}

    # trip_id -> (route_id, direction_id) for trips that call at stop in window
    seen: Dict[tuple, Dict[str, Any]] = {}

    conn = get_conn()
    # Scan only stop_times rows for this stop_id and join with trips in Python.
    cur = conn.execute(
        """
        SELECT trip_id,
               departure_time,
               arrival_time
        FROM stop_times
        WHERE stop_id = ?
        """,
        (stop_id,),
    )

    for st in cur.fetchall():
        st = dict(st)
        t_str = (st.get("departure_time") or st.get("arrival_time") or "").strip()
        if not t_str:
            continue
        try:
            t_sec = parse_gtfs_time_to_seconds(t_str)
        except Exception:
            continue
        if t_sec < start_sec or t_sec > end_sec:
            continue

        trip_id = st.get("trip_id")
        if not trip_id:
            continue
        trip = trips_by_id.get(trip_id)
        if not trip or trip.get("service_id") not in active_services:
            continue

        route_id = trip.get("route_id")
        if not route_id:
            continue
        direction_id = trip.get("direction_id") or ""
        key = (route_id, direction_id)

        if key in seen:
            entry = seen[key]
            if t_sec < entry["first_time_sec"]:
                entry["first_time_sec"] = t_sec
            if t_sec > entry["last_time_sec"]:
                entry["last_time_sec"] = t_sec
        else:
            meta = routes_by_id.get(route_id, {})
            agency_meta = agencies_by_id.get(meta.get("agency_id")) if meta else None
            seen[key] = {
                "route_id": route_id,
                "direction_id": direction_id or None,
                "route_short_name": meta.get("route_short_name"),
                "route_long_name": meta.get("route_long_name"),
                "agency_id": meta.get("agency_id"),
                "agency_name": agency_meta.get("agency_name") if agency_meta else None,
                "first_time_sec": t_sec,
                "last_time_sec": t_sec,
            }

    def fmt_time(sec: int) -> str:
        h = sec // 3600
        m = (sec % 3600) // 60
        return f"{h:02d}:{m:02d}"

    results = []
    for v in seen.values():
        v["first_time"] = fmt_time(v["first_time_sec"])
        v["last_time"] = fmt_time(v["last_time_sec"])
        del v["first_time_sec"]
        del v["last_time_sec"]
        results.append(v)
        if len(results) >= max_results:
            break

    results.sort(key=lambda x: (x.get("route_short_name") or x["route_id"], x.get("direction_id") or ""))
    return results
