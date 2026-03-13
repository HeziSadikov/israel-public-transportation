from __future__ import annotations

"""
PostgreSQL/PostGIS data access layer for Israel GTFS Detour Router.

This module hides raw SQL behind a small set of functions used by:
  - area_search (routes in polygon for a date/time window)
  - graph_builder / detour_graph (patterns, stops, shapes, trip time bounds)

It assumes the schema defined in backend/db_postgis_schema.sql and that
feed_versions.active indicates the current GTFS feed.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import os
import hashlib
import json

import psycopg2
from psycopg2.extras import DictCursor


# In containers and most environments we prefer an explicit DATABASE_URL.
# For backward compatibility with the existing Windows setup, fall back to
# the previous localhost DSN if the env var is not set.
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs")


def _get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=DictCursor)


def get_active_feed_id(conn=None) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM feed_versions WHERE active = TRUE ORDER BY fetched_at DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No active feed in feed_versions")
            # Support both DictCursor (row["id"]) and regular cursor (row[0]).
            try:
                return int(row["id"])  # type: ignore[index]
            except Exception:
                return int(row[0])
    finally:
        if close:
            conn.close()


@dataclass
class RouteInAreaRow:
    route_id: str
    direction_id: Optional[int]
    route_short_name: Optional[str]
    route_long_name: Optional[str]
    agency_id: Optional[str]
    agency_name: Optional[str]
    first_time_s: Optional[int]
    last_time_s: Optional[int]


def get_routes_in_polygon(
    polygon_wkt: str,
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    conn=None,
) -> List[RouteInAreaRow]:
    """
    Find routes whose shapes intersect the given polygon on a given date/time window.

    polygon_wkt: WKT string in SRID 4326; this keeps app.py / area_search free of SQL.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH active_services AS (
                    SELECT c.service_id
                    FROM calendar c
                    WHERE c.feed_id = %s
                      AND %s BETWEEN c.start_date AND c.end_date
                      AND CASE EXTRACT(DOW FROM to_timestamp(%s::text, 'YYYYMMDD'))
                            WHEN 0 THEN c.sunday
                            WHEN 1 THEN c.monday
                            WHEN 2 THEN c.tuesday
                            WHEN 3 THEN c.wednesday
                            WHEN 4 THEN c.thursday
                            WHEN 5 THEN c.friday
                            WHEN 6 THEN c.saturday
                          END = 1
                    UNION
                    SELECT cd.service_id
                    FROM calendar_dates cd
                    WHERE cd.feed_id = %s
                      AND cd.date = %s
                      AND cd.exception_type = 1
                ),
                trips_in_window AS (
                    SELECT t.feed_id, t.route_id, t.direction_id, t.trip_id
                    FROM trips t
                    JOIN active_services s
                      ON s.service_id = t.service_id
                    JOIN trip_time_bounds b
                      ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                    WHERE t.feed_id = %s
                      AND b.last_sec >= %s
                      AND b.first_sec <= %s
                ),
                routes_geom AS (
                    SELECT DISTINCT
                        tiw.route_id,
                        tiw.direction_id,
                        r.short_name AS route_short_name,
                        r.long_name AS route_long_name,
                        r.agency_id,
                        MIN(b.first_sec) AS first_time_s,
                        MAX(b.last_sec) AS last_time_s
                    FROM trips_in_window tiw
                    JOIN trips t
                      ON t.feed_id = tiw.feed_id AND t.trip_id = tiw.trip_id
                    JOIN routes r
                      ON r.feed_id = tiw.feed_id AND r.route_id = tiw.route_id
                    JOIN trip_time_bounds b
                      ON b.feed_id = tiw.feed_id AND b.trip_id = tiw.trip_id
                    JOIN shapes_lines sl
                      ON sl.feed_id = tiw.feed_id AND sl.shape_id = t.shape_id
                    WHERE ST_Intersects(
                        sl.geom,
                        ST_GeomFromText(%s, 4326)
                    )
                    GROUP BY tiw.route_id, tiw.direction_id,
                             r.short_name, r.long_name, r.agency_id
                )
                SELECT
                    rg.route_id,
                    rg.direction_id,
                    rg.route_short_name,
                    rg.route_long_name,
                    rg.agency_id,
                    a.name AS agency_name,
                    rg.first_time_s,
                    rg.last_time_s
                FROM routes_geom rg
                LEFT JOIN agencies a
                  ON a.feed_id = %s AND a.agency_id = rg.agency_id
                ORDER BY rg.route_short_name, rg.route_id, rg.direction_id
                """,
                (
                    feed_id,
                    int(date_ymd),
                    date_ymd,
                    feed_id,
                    int(date_ymd),
                    feed_id,
                    start_sec,
                    end_sec,
                    polygon_wkt,
                    feed_id,
                ),
            )
            rows = []
            for r in cur.fetchall():
                rows.append(
                    RouteInAreaRow(
                        route_id=r["route_id"],
                        direction_id=r["direction_id"],
                        route_short_name=r["route_short_name"],
                        route_long_name=r["route_long_name"],
                        agency_id=r["agency_id"],
                        agency_name=r["agency_name"],
                        first_time_s=r["first_time_s"],
                        last_time_s=r["last_time_s"],
                    )
                )
            return rows
    finally:
        if close:
            conn.close()


@dataclass
class PatternMeta:
    pattern_id: str
    route_id: str
    direction_id: Optional[int]
    repr_trip_id: str
    repr_shape_id: Optional[str]
    stop_ids: List[str]
    frequency: int
    used_shape: bool


def get_pattern_for_route(
    route_id: str,
    direction_id: Optional[str],
    date_ymd: str,
    conn=None,
) -> Optional[PatternMeta]:
    """
    For now this returns the most frequent pattern row for the active feed and route/direction.
    The date_ymd is accepted for future date-aware filtering but currently unused.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.pattern_id,
                    p.route_id,
                    p.direction_id,
                    p.repr_trip_id,
                    p.repr_shape_id,
                    p.stop_ids,
                    p.frequency,
                    p.used_shape
                FROM patterns p
                WHERE p.feed_id = %s
                  AND p.route_id = %s
                  AND (%s IS NULL OR p.direction_id = %s::int)
                ORDER BY p.frequency DESC NULLS LAST
                LIMIT 1
                """,
                (feed_id, route_id, direction_id, direction_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            return PatternMeta(
                pattern_id=row["pattern_id"],
                route_id=row["route_id"],
                direction_id=row["direction_id"],
                repr_trip_id=row["repr_trip_id"],
                repr_shape_id=row["repr_shape_id"],
                stop_ids=list(row["stop_ids"] or []),
                frequency=int(row["frequency"] or 0),
                used_shape=bool(row["used_shape"]),
            )
    finally:
        if close:
            conn.close()


@dataclass
class StopMeta:
    stop_id: str
    name: Optional[str]
    lat: float
    lon: float


def get_pattern_stops(pattern_id: str, conn=None) -> List[StopMeta]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ps.seq, ps.stop_id, s.name, s.lat, s.lon
                FROM pattern_stops ps
                JOIN stops s
                  ON s.feed_id = ps.feed_id AND s.stop_id = ps.stop_id
                WHERE ps.feed_id = %s AND ps.pattern_id = %s
                ORDER BY ps.seq
                """,
                (feed_id, pattern_id),
            )
            return [
                StopMeta(
                    stop_id=row["stop_id"],
                    name=row["name"],
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                )
                for row in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()


def get_shape_line(shape_id: str, conn=None) -> Optional[Any]:
    """
    Return the LineString geometry for the given shape_id from shapes_lines.
    Returns a Shapely LineString or None if not found.
    """
    from shapely import wkt

    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ST_AsText(geom) AS wkt
                FROM shapes_lines
                WHERE feed_id = %s AND shape_id = %s
                """,
                (feed_id, shape_id),
            )
            row = cur.fetchone()
            if not row or not row["wkt"]:
                return None
            return wkt.loads(str(row["wkt"]))
    finally:
        if close:
            conn.close()


def get_stop_times_for_trip(trip_id: str, conn=None) -> List[Dict[str, Any]]:
    """
    Return ordered stop_times for a single trip from PostGIS.
    Each row has stop_id, stop_sequence, arrival_time, departure_time, shape_dist_traveled.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT stop_id, stop_sequence, arrival_time, departure_time, shape_dist_traveled
                FROM stop_times
                WHERE feed_id = %s AND trip_id = %s
                ORDER BY stop_sequence
                """,
                (feed_id, trip_id),
            )
            return [
                {
                    "stop_id": r["stop_id"],
                    "stop_sequence": r["stop_sequence"],
                    "arrival_time": r["arrival_time"],
                    "departure_time": r["departure_time"],
                    "shape_dist_traveled": r["shape_dist_traveled"],
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()

def get_stops_in_bounds_pg(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Return stops in the active feed whose lat/lon fall inside the given bounds.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT stop_id, name, lat, lon
                FROM stops
                WHERE feed_id = %s
                  AND lat BETWEEN %s AND %s
                  AND lon BETWEEN %s AND %s
                LIMIT %s
                """,
                (feed_id, min_lat, max_lat, min_lon, max_lon, limit),
            )
            return [
                {
                    "stop_id": r["stop_id"],
                    "stop_name": r["name"],
                    "stop_code": None,
                    "stop_lat": float(r["lat"]),
                    "stop_lon": float(r["lon"]),
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()


def get_trip_time_bounds_pg(conn=None) -> Dict[str, Tuple[int, int]]:
    """
    Return trip_id -> (first_sec, last_sec) from PostGIS trip_time_bounds for the active feed.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trip_id, first_sec, last_sec
                FROM trip_time_bounds
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            return {
                r["trip_id"]: (int(r["first_sec"]), int(r["last_sec"]))
                for r in cur.fetchall()
            }
    finally:
        if close:
            conn.close()


def _get_active_service_ids_for_date_pg(yyyymmdd: str, conn) -> List[str]:
    """
    Compute active service_ids for a given date from PostGIS calendar + calendar_dates.
    Mirrors ServiceCalendar._active_from_calendar but uses SQL rows directly.
    """
    from datetime import datetime as _dt

    d = _dt.strptime(yyyymmdd, "%Y%m%d").date()
    weekday_name = d.strftime("%A").lower()

    # Base calendar
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT service_id,
                   monday, tuesday, wednesday, thursday, friday, saturday, sunday,
                   start_date, end_date
            FROM calendar
            WHERE %s BETWEEN start_date AND end_date
              AND feed_id = %s
            """,
            (int(yyyymmdd), get_active_feed_id(conn)),
        )
        calendar_rows = [dict(r) for r in cur.fetchall()]

    active: set[str] = set()
    for row in calendar_rows:
        if str(row.get(weekday_name) or 0) == "1":
            active.add(row["service_id"])

    # Exceptions
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT service_id, exception_type
            FROM calendar_dates
            WHERE feed_id = %s AND date = %s
            """,
            (get_active_feed_id(conn), int(yyyymmdd)),
        )
        for r in cur.fetchall():
            sid = r["service_id"]
            et = str(r["exception_type"])
            if et == "1":
                active.add(sid)
            elif et == "2" and sid in active:
                active.remove(sid)
    return list(active)


def get_routes_serving_stop_pg(
    stop_id: str,
    yyyymmdd: str,
    start_sec: int,
    end_sec: int,
    max_results: int = 100,
) -> List[Dict[str, Any]]:
    """
    PostGIS-backed version of get_routes_serving_stop:
    returns routes that serve the given stop in the time window on the given date.
    """
    from backend.service_calendar import parse_gtfs_time_to_seconds

    close = False
    conn = _get_conn()
    close = True
    try:
        feed_id = get_active_feed_id(conn)
        active_services = set(_get_active_service_ids_for_date_pg(yyyymmdd, conn))
        if not active_services:
            return []

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  st.trip_id,
                  st.departure_time,
                  st.arrival_time,
                  t.route_id,
                  t.direction_id,
                  t.service_id
                FROM stop_times st
                JOIN trips t
                  ON t.feed_id = st.feed_id AND t.trip_id = st.trip_id
                WHERE st.feed_id = %s
                  AND st.stop_id = %s
                """,
                (feed_id, stop_id),
            )
            rows = [dict(r) for r in cur.fetchall()]

        seen: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}

        for st in rows:
            service_id = st.get("service_id")
            if service_id not in active_services:
                continue
            t_str = (st.get("departure_time") or st.get("arrival_time") or "").strip()
            if not t_str:
                continue
            try:
                t_sec = parse_gtfs_time_to_seconds(t_str)
            except Exception:
                continue
            if t_sec < start_sec or t_sec > end_sec:
                continue

            route_id = st.get("route_id")
            if not route_id:
                continue
            dir_val = st.get("direction_id")
            direction_id = None if dir_val is None else str(dir_val)
            key = (route_id, direction_id)

            if key in seen:
                entry = seen[key]
                if t_sec < entry["first_time_sec"]:
                    entry["first_time_sec"] = t_sec
                if t_sec > entry["last_time_sec"]:
                    entry["last_time_sec"] = t_sec
            else:
                seen[key] = {
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "first_time_sec": t_sec,
                    "last_time_sec": t_sec,
                }

        if not seen:
            return []

        # Fetch route/agency metadata for the seen routes.
        route_ids = [k[0] for k in seen.keys()]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  r.route_id,
                  r.short_name,
                  r.long_name,
                  r.agency_id,
                  a.name AS agency_name
                FROM routes r
                LEFT JOIN agencies a
                  ON a.feed_id = r.feed_id AND a.agency_id = r.agency_id
                WHERE r.feed_id = %s
                  AND r.route_id = ANY(%s)
                """,
                (feed_id, route_ids),
            )
            meta_rows = {r["route_id"]: dict(r) for r in cur.fetchall()}

        def _fmt_time(sec: int) -> str:
            h = sec // 3600
            m = (sec % 3600) // 60
            return f"{h:02d}:{m:02d}"

        results: List[Dict[str, Any]] = []
        for (route_id, direction_id), entry in seen.items():
            m = meta_rows.get(route_id, {})
            first_sec = entry["first_time_sec"]
            last_sec = entry["last_time_sec"]
            results.append(
                {
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "route_short_name": m.get("short_name"),
                    "route_long_name": m.get("long_name"),
                    "agency_id": m.get("agency_id"),
                    "agency_name": m.get("agency_name"),
                    "first_time": _fmt_time(first_sec),
                    "last_time": _fmt_time(last_sec),
                }
            )
            if len(results) >= max_results:
                break

        results.sort(
            key=lambda x: (
                x.get("route_short_name") or x["route_id"],
                x.get("direction_id") or "",
            )
        )
        return results
    finally:
        if close:
            conn.close()


def compute_route_signature(
    route_id: str,
    direction_id: Optional[str],
    conn=None,
) -> str:
    """
    Compute a stable hash for (route_id, direction_id) for the active feed
    based on trips + stop_times + shapes.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            # Trips for this route/direction
            cur.execute(
                """
                SELECT trip_id, service_id, shape_id
                FROM trips
                WHERE feed_id = %s
                  AND route_id = %s
                  AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
                ORDER BY trip_id
                """,
                (feed_id, route_id, int(direction_id) if direction_id is not None else None),
            )
            trips = [dict(r) for r in cur.fetchall()]
            trip_ids = [t["trip_id"] for t in trips]

            # Stop sequences
            if trip_ids:
                cur.execute(
                    """
                    SELECT trip_id, stop_id, stop_sequence
                    FROM stop_times
                    WHERE feed_id = %s
                      AND trip_id = ANY(%s)
                    ORDER BY trip_id, stop_sequence
                    """,
                    (feed_id, trip_ids),
                )
                stop_times = [dict(r) for r in cur.fetchall()]
            else:
                stop_times = []

            # Shapes used
            shape_ids = sorted({t["shape_id"] for t in trips if t.get("shape_id")})
            if shape_ids:
                cur.execute(
                    """
                    SELECT shape_id, seq, lat, lon
                    FROM shapes
                    WHERE feed_id = %s
                      AND shape_id = ANY(%s)
                    ORDER BY shape_id, seq
                    """,
                    (feed_id, shape_ids),
                )
                shapes = [dict(r) for r in cur.fetchall()]
            else:
                shapes = []

        payload = {
            "trips": trips,
            "stop_times": stop_times,
            "shapes": shapes,
        }
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()
    finally:
        if close:
            conn.close()


def get_route_signature(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    conn=None,
) -> Optional[str]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sig_hash
                FROM route_signatures
                WHERE feed_id = %s
                  AND route_id = %s
                  AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
                """,
                (feed_id, route_id, int(direction_id) if direction_id is not None else None),
            )
            row = cur.fetchone()
            return row["sig_hash"] if row else None
    finally:
        if close:
            conn.close()


def upsert_route_signature(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    sig_hash: str,
    conn=None,
) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO route_signatures (feed_id, route_id, direction_id, sig_hash)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (feed_id, route_id, direction_id)
                DO UPDATE SET sig_hash = EXCLUDED.sig_hash
                """,
                (feed_id, route_id, int(direction_id) if direction_id is not None else None, sig_hash),
            )
            conn.commit()
    finally:
        if close:
            conn.close()


def get_cached_route_graph_pg(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    date_ymd: str,
    pretty_osm: bool,
    route_sig_hash: str,
    conn=None,
) -> Optional[bytes]:
    """
    Return a cached pickled graph entry from PostGIS route_graph_cache if present.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT graph_blob
                FROM route_graph_cache
                WHERE feed_id = %s
                  AND route_id = %s
                  AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
                  AND date_ymd = %s
                  AND pretty_osm = %s
                  AND route_sig_hash = %s
                """,
                (
                    feed_id,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    int(date_ymd),
                    pretty_osm,
                    route_sig_hash,
                ),
            )
            row = cur.fetchone()
            if not row:
                return None
            return bytes(row["graph_blob"])
    finally:
        if close:
            conn.close()


def save_route_graph_pg(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    date_ymd: str,
    pretty_osm: bool,
    route_sig_hash: str,
    graph_blob: bytes,
    conn=None,
) -> None:
    """
    Save or update a cached pickled graph entry in PostGIS route_graph_cache.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO route_graph_cache (
                  feed_id,
                  route_id,
                  direction_id,
                  date_ymd,
                  pretty_osm,
                  route_sig_hash,
                  graph_blob
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (feed_id, route_id, direction_id, date_ymd, pretty_osm)
                DO UPDATE SET
                  route_sig_hash = EXCLUDED.route_sig_hash,
                  graph_blob     = EXCLUDED.graph_blob,
                  created_at     = NOW()
                """,
                (
                    feed_id,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    int(date_ymd),
                    pretty_osm,
                    route_sig_hash,
                    psycopg2.Binary(graph_blob),
                ),
            )
            conn.commit()
    finally:
        if close:
            conn.close()



def search_routes_pg(q: str, limit: int) -> List[Dict[str, Any]]:
    """
    Search routes in the active feed by id/short_name/long_name using PostGIS.
    Mirrors the old SQLite-based semantics.
    """
    q = (q or "").strip().lower()
    if not q:
        return []
    close = False
    conn = _get_conn()
    close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            if q.isdigit():
                cur.execute(
                    """
                    SELECT
                      r.route_id,
                      r.short_name,
                      r.long_name,
                      r.agency_id,
                      r.route_type,
                      a.name AS agency_name
                    FROM routes r
                    LEFT JOIN agencies a
                      ON a.feed_id = r.feed_id AND a.agency_id = r.agency_id
                    WHERE r.feed_id = %s
                      AND (LOWER(r.route_id) = %s OR LOWER(r.short_name) = %s)
                    LIMIT %s
                    """,
                    (feed_id, q, q, limit),
                )
            else:
                like = f"%{q}%"
                cur.execute(
                    """
                    SELECT
                      r.route_id,
                      r.short_name,
                      r.long_name,
                      r.agency_id,
                      r.route_type,
                      a.name AS agency_name
                    FROM routes r
                    LEFT JOIN agencies a
                      ON a.feed_id = r.feed_id AND a.agency_id = r.agency_id
                    WHERE r.feed_id = %s
                      AND (
                        LOWER(COALESCE(r.route_id, ''))    LIKE %s OR
                        LOWER(COALESCE(r.short_name, ''))  LIKE %s OR
                        LOWER(COALESCE(r.long_name, ''))   LIKE %s
                      )
                    LIMIT %s
                    """,
                    (feed_id, like, like, like, limit),
                )
            return [
                {
                    "route_id": r["route_id"],
                    "route_short_name": r["short_name"],
                    "route_long_name": r["long_name"],
                    "agency_id": r["agency_id"],
                    "route_type": r["route_type"],
                    "agency_name": r["agency_name"],
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()

