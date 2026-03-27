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

# Optional[str] for direction_id in tuple keys (route_id, direction_id)
RouteDirKey = Tuple[str, Optional[str]]

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
    active_trip_count: Optional[int] = None
    selection_source: Optional[str] = None


def _to_int_direction(direction_id: Optional[str]) -> Optional[int]:
    if direction_id is None:
        return None
    try:
        return int(direction_id)
    except Exception:
        return None


def get_top_patterns_for_routes(
    route_ids: List[str],
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    k_per_route_dir: int = 2,
    direction_filter_by_route: Optional[Dict[str, Optional[str]]] = None,
    include_fallback: bool = True,
    conn=None,
) -> Dict[RouteDirKey, List[PatternMeta]]:
    """
    Return Top-K patterns per (route_id, direction_id) for the active feed.

    Primary ranking uses strict service-day + time-window activity:
      1) active_trip_count DESC
      2) frequency DESC
      3) pattern_id ASC

    If no strict match exists for a route/direction and include_fallback=True,
    fallback picks by frequency ordering from all patterns on that route/direction.
    """
    if not route_ids:
        return {}
    k = max(1, int(k_per_route_dir or 1))
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        wanted_routes = set(route_ids)
        dir_filter_int: Dict[str, Optional[int]] = {}
        if direction_filter_by_route:
            for rid, did in direction_filter_by_route.items():
                dir_filter_int[rid] = _to_int_direction(did)

        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dow AS (
                    SELECT EXTRACT(DOW FROM to_date(%s::text, 'YYYYMMDD'))::int AS d
                ),
                calendar_services AS (
                    SELECT c.service_id
                    FROM calendar c, dow
                    WHERE c.feed_id = %s
                      AND %s BETWEEN c.start_date AND c.end_date
                      AND (
                        (d = 0 AND c.sunday = 1)
                        OR (d = 1 AND c.monday = 1)
                        OR (d = 2 AND c.tuesday = 1)
                        OR (d = 3 AND c.wednesday = 1)
                        OR (d = 4 AND c.thursday = 1)
                        OR (d = 5 AND c.friday = 1)
                        OR (d = 6 AND c.saturday = 1)
                      )
                ),
                add_services AS (
                    SELECT service_id
                    FROM calendar_dates
                    WHERE feed_id = %s
                      AND date = %s
                      AND exception_type = 1
                ),
                remove_services AS (
                    SELECT service_id
                    FROM calendar_dates
                    WHERE feed_id = %s
                      AND date = %s
                      AND exception_type = 2
                ),
                active_services AS (
                    (SELECT service_id FROM calendar_services
                     UNION
                     SELECT service_id FROM add_services)
                    EXCEPT
                    SELECT service_id FROM remove_services
                ),
                trips_in_window AS (
                    SELECT t.trip_id, t.route_id, t.direction_id
                    FROM trips t
                    JOIN active_services a
                      ON a.service_id = t.service_id
                    JOIN trip_time_bounds b
                      ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                    WHERE t.feed_id = %s
                      AND t.route_id = ANY(%s)
                      AND b.last_sec >= %s
                      AND b.first_sec <= %s
                ),
                trip_stop_chains AS (
                    SELECT
                        tw.trip_id,
                        tw.route_id,
                        tw.direction_id,
                        ARRAY_AGG(st.stop_id ORDER BY st.stop_sequence)::text[] AS stop_ids
                    FROM trips_in_window tw
                    JOIN stop_times st
                      ON st.feed_id = %s AND st.trip_id = tw.trip_id
                    GROUP BY tw.trip_id, tw.route_id, tw.direction_id
                ),
                strict_counts AS (
                    SELECT
                        p.pattern_id,
                        COUNT(*)::int AS active_trip_count
                    FROM patterns p
                    JOIN trip_stop_chains tsc
                      ON tsc.route_id = p.route_id
                     AND COALESCE(tsc.direction_id, -1) = COALESCE(p.direction_id, -1)
                     AND tsc.stop_ids = p.stop_ids
                    WHERE p.feed_id = %s
                      AND p.route_id = ANY(%s)
                    GROUP BY p.pattern_id
                )
                SELECT
                    p.pattern_id,
                    p.route_id,
                    p.direction_id,
                    p.repr_trip_id,
                    p.repr_shape_id,
                    p.stop_ids,
                    p.frequency,
                    p.used_shape,
                    COALESCE(sc.active_trip_count, 0) AS active_trip_count
                FROM patterns p
                LEFT JOIN strict_counts sc
                  ON sc.pattern_id = p.pattern_id
                WHERE p.feed_id = %s
                  AND p.route_id = ANY(%s)
                ORDER BY
                    p.route_id,
                    p.direction_id NULLS FIRST,
                    COALESCE(sc.active_trip_count, 0) DESC,
                    p.frequency DESC NULLS LAST,
                    p.pattern_id ASC
                """,
                (
                    date_ymd,
                    feed_id,
                    int(date_ymd),
                    feed_id,
                    int(date_ymd),
                    feed_id,
                    int(date_ymd),
                    feed_id,
                    route_ids,
                    start_sec,
                    end_sec,
                    feed_id,
                    feed_id,
                    route_ids,
                    feed_id,
                    route_ids,
                ),
            )
            rows = cur.fetchall()

        by_key_all: Dict[RouteDirKey, List[PatternMeta]] = {}
        by_key_strict: Dict[RouteDirKey, List[PatternMeta]] = {}
        for row in rows:
            rid = row["route_id"]
            if rid not in wanted_routes:
                continue
            did_str = None if row["direction_id"] is None else str(row["direction_id"])
            if rid in dir_filter_int:
                wanted_did = dir_filter_int[rid]
                if wanted_did is not None and row["direction_id"] != wanted_did:
                    continue
            key: RouteDirKey = (rid, did_str)
            meta = PatternMeta(
                pattern_id=row["pattern_id"],
                route_id=rid,
                direction_id=row["direction_id"],
                repr_trip_id=row["repr_trip_id"],
                repr_shape_id=row["repr_shape_id"],
                stop_ids=list(row["stop_ids"] or []),
                frequency=int(row["frequency"] or 0),
                used_shape=bool(row["used_shape"]),
                active_trip_count=int(row["active_trip_count"] or 0),
            )
            by_key_all.setdefault(key, []).append(meta)
            if (meta.active_trip_count or 0) > 0:
                strict_meta = PatternMeta(
                    pattern_id=meta.pattern_id,
                    route_id=meta.route_id,
                    direction_id=meta.direction_id,
                    repr_trip_id=meta.repr_trip_id,
                    repr_shape_id=meta.repr_shape_id,
                    stop_ids=meta.stop_ids,
                    frequency=meta.frequency,
                    used_shape=meta.used_shape,
                    active_trip_count=meta.active_trip_count,
                    selection_source="strict",
                )
                by_key_strict.setdefault(key, []).append(strict_meta)

        selected: Dict[RouteDirKey, List[PatternMeta]] = {}
        keys = sorted(set(by_key_all.keys()) | set(by_key_strict.keys()))
        for key in keys:
            strict_list = by_key_strict.get(key, [])
            if strict_list:
                selected[key] = strict_list[:k]
                continue
            if include_fallback:
                fallback = by_key_all.get(key, [])[:k]
                for m in fallback:
                    m.selection_source = "fallback"
                if fallback:
                    selected[key] = fallback
        return selected
    finally:
        if close:
            conn.close()


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
    # For explicit direction, delegate to Top-K selector (K=1) with broad window.
    if direction_id is not None:
        picked = get_top_patterns_for_routes(
            route_ids=[route_id],
            date_ymd=date_ymd,
            start_sec=0,
            end_sec=27 * 3600,
            k_per_route_dir=1,
            direction_filter_by_route={route_id: direction_id},
            include_fallback=True,
            conn=conn,
        )
        key: RouteDirKey = (route_id, direction_id)
        one = picked.get(key) or []
        if one:
            return one[0]

    # Compatibility path for direction=None (and as a final fallback).
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
                active_trip_count=None,
                selection_source="fallback",
            )
    finally:
        if close:
            conn.close()


def get_patterns_for_feed(feed_id: int, conn=None) -> Dict[RouteDirKey, PatternMeta]:
    """Return all patterns for the feed keyed by (route_id, direction_id). One query."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pattern_id, route_id, direction_id, repr_trip_id, repr_shape_id,
                       stop_ids, frequency, used_shape
                FROM patterns
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            out: Dict[RouteDirKey, PatternMeta] = {}
            for row in cur.fetchall():
                key: RouteDirKey = (
                    row["route_id"],
                    None if row["direction_id"] is None else str(row["direction_id"]),
                )
                out[key] = PatternMeta(
                    pattern_id=row["pattern_id"],
                    route_id=row["route_id"],
                    direction_id=row["direction_id"],
                    repr_trip_id=row["repr_trip_id"],
                    repr_shape_id=row["repr_shape_id"],
                    stop_ids=list(row["stop_ids"] or []),
                    frequency=int(row["frequency"] or 0),
                    used_shape=bool(row["used_shape"]),
                )
            return out
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


def get_pattern_stops_bulk(
    feed_id: int,
    pattern_ids: List[str],
    conn=None,
) -> Dict[str, List[StopMeta]]:
    """Load pattern_stops for many pattern_ids in one query. Returns pattern_id -> ordered list of StopMeta."""
    if not pattern_ids:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ps.pattern_id, ps.seq, ps.stop_id, s.name, s.lat, s.lon
                FROM pattern_stops ps
                JOIN stops s ON s.feed_id = ps.feed_id AND s.stop_id = ps.stop_id
                WHERE ps.feed_id = %s AND ps.pattern_id = ANY(%s)
                ORDER BY ps.pattern_id, ps.seq
                """,
                (feed_id, pattern_ids),
            )
            out: Dict[str, List[StopMeta]] = {}
            for row in cur.fetchall():
                pid = row["pattern_id"]
                out.setdefault(pid, []).append(
                    StopMeta(
                        stop_id=row["stop_id"],
                        name=row["name"],
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                    )
                )
            return out
    finally:
        if close:
            conn.close()


def get_pattern_nodes_bulk(
    pattern_ids: List[str],
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Load precomputed pattern-stop occurrence nodes from PostGIS.

    Returns rows suitable for assembling the local detour nx.DiGraph with:
    - node_id, pattern_id, stop_id, stop_sequence
    - lat/lon
    - out_heading_deg and frequency (used for transfer compatibility + A* weighting)
    """
    if not pattern_ids:
        return []
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
                    node_id,
                    pattern_id,
                    route_id,
                    direction_id,
                    stop_id,
                    stop_sequence,
                    lat,
                    lon,
                    out_heading_deg,
                    frequency
                FROM pattern_nodes
                WHERE feed_id = %s
                  AND pattern_id = ANY(%s)
                """,
                (feed_id, pattern_ids),
            )
            return [
                {
                    "node_id": r["node_id"],
                    "pattern_id": r["pattern_id"],
                    "route_id": r["route_id"],
                    "direction_id": None if r.get("direction_id") is None else str(r.get("direction_id")),
                    "stop_id": r["stop_id"],
                    "stop_sequence": int(r["stop_sequence"]),
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "out_heading_deg": None if r.get("out_heading_deg") is None else float(r.get("out_heading_deg")),
                    "frequency": None if r.get("frequency") is None else int(r.get("frequency")),
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()


def get_pattern_edges_bulk(
    pattern_ids: List[str],
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Load precomputed directed ride edges for pattern_ids from PostGIS.

    Edge geometries are returned as Shapely LineStrings so the caller can:
    - build edge_geometries for blocked-edge intersection + GeoJSON
    - set A* weights: travel_time_s + distance_m
    """
    if not pattern_ids:
        return []
    # Local import: shapely is only required when precomputed ride-network is used.
    from shapely import wkt as _wkt

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
                    pattern_id,
                    from_node_id,
                    to_node_id,
                    from_stop_id,
                    to_stop_id,
                    travel_time_s,
                    distance_m,
                    ST_AsText(geom) AS wkt
                FROM pattern_edges
                WHERE feed_id = %s
                  AND pattern_id = ANY(%s)
                """,
                (feed_id, pattern_ids),
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                line = None
                wkt_txt = r.get("wkt")
                if wkt_txt:
                    try:
                        line = _wkt.loads(str(wkt_txt))
                    except Exception:
                        line = None
                out.append(
                    {
                        "pattern_id": r["pattern_id"],
                        "from_node_id": r["from_node_id"],
                        "to_node_id": r["to_node_id"],
                        "from_stop_id": r["from_stop_id"],
                        "to_stop_id": r["to_stop_id"],
                        "travel_time_s": None if r.get("travel_time_s") is None else float(r.get("travel_time_s")),
                        "distance_m": None if r.get("distance_m") is None else float(r.get("distance_m")),
                        "linestring": line,
                    }
                )
            return out
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


def get_shape_lines_bulk(
    feed_id: int,
    shape_ids: List[str],
    conn=None,
) -> Dict[str, Any]:
    """Load LineString geometry for many shape_ids in one query. Returns shape_id -> LineString or None."""
    if not shape_ids:
        return {}
    from shapely import wkt
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT shape_id, ST_AsText(geom) AS wkt
                FROM shapes_lines
                WHERE feed_id = %s AND shape_id = ANY(%s)
                """,
                (feed_id, shape_ids),
            )
            out: Dict[str, Any] = {}
            for row in cur.fetchall():
                sid = row["shape_id"]
                if row.get("wkt"):
                    try:
                        out[sid] = wkt.loads(str(row["wkt"]))
                    except Exception:
                        out[sid] = None
                else:
                    out[sid] = None
            return out
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


def get_stop_times_bulk(
    feed_id: int,
    trip_ids: List[str],
    conn=None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Load stop_times for many trip_ids in one query. Returns trip_id -> ordered list of rows."""
    if not trip_ids:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time, shape_dist_traveled
                FROM stop_times
                WHERE feed_id = %s AND trip_id = ANY(%s)
                ORDER BY trip_id, stop_sequence
                """,
                (feed_id, trip_ids),
            )
            out: Dict[str, List[Dict[str, Any]]] = {}
            for row in cur.fetchall():
                tid = row["trip_id"]
                out.setdefault(tid, []).append({
                    "stop_id": row["stop_id"],
                    "stop_sequence": row["stop_sequence"],
                    "arrival_time": row["arrival_time"],
                    "departure_time": row["departure_time"],
                    "shape_dist_traveled": row["shape_dist_traveled"],
                })
            return out
    finally:
        if close:
            conn.close()


def get_all_stop_times_for_feed(
    feed_id: int,
    conn=None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Load all stop_times for the feed in one query. Returns trip_id -> ordered list of rows. Use for bulk pattern building."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time, shape_dist_traveled
                FROM stop_times
                WHERE feed_id = %s
                ORDER BY trip_id, stop_sequence
                """,
                (feed_id,),
            )
            out: Dict[str, List[Dict[str, Any]]] = {}
            for row in cur.fetchall():
                tid = row["trip_id"]
                out.setdefault(tid, []).append({
                    "stop_id": row["stop_id"],
                    "stop_sequence": row["stop_sequence"],
                    "arrival_time": row["arrival_time"],
                    "departure_time": row["departure_time"],
                    "shape_dist_traveled": row["shape_dist_traveled"],
                })
            return out
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
    Routes that serve the given stop in the time window on the given date.
    Implemented as a single SQL query (SQL-heavy): active services, time parsing,
    and aggregation done in Postgres.
    """
    date_ymd = int(yyyymmdd)
    close = False
    conn = _get_conn()
    close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dow AS (
                    SELECT EXTRACT(DOW FROM to_date(%s::text, 'YYYYMMDD'))::int AS d
                ),
                calendar_services AS (
                    SELECT c.service_id
                    FROM calendar c, dow
                    WHERE c.feed_id = %s
                      AND %s BETWEEN c.start_date AND c.end_date
                      AND (
                        (d = 0 AND c.sunday = 1)
                        OR (d = 1 AND c.monday = 1)
                        OR (d = 2 AND c.tuesday = 1)
                        OR (d = 3 AND c.wednesday = 1)
                        OR (d = 4 AND c.thursday = 1)
                        OR (d = 5 AND c.friday = 1)
                        OR (d = 6 AND c.saturday = 1)
                      )
                ),
                add_services AS (
                    SELECT service_id FROM calendar_dates
                    WHERE feed_id = %s AND date = %s AND exception_type = 1
                ),
                remove_services AS (
                    SELECT service_id FROM calendar_dates
                    WHERE feed_id = %s AND date = %s AND exception_type = 2
                ),
                active_services AS (
                    (SELECT service_id FROM calendar_services
                     UNION
                     SELECT service_id FROM add_services)
                    EXCEPT
                    SELECT service_id FROM remove_services
                ),
                stop_sec AS (
                    SELECT
                        st.trip_id,
                        t.route_id,
                        t.direction_id,
                        (split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 1)::int * 3600
                         + split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 2)::int * 60
                         + coalesce(nullif(trim(split_part(trim(coalesce(st.departure_time, st.arrival_time, '0:0:0')), ':', 3)), ''), '0')::int
                        ) AS sec
                    FROM stop_times st
                    JOIN trips t ON t.feed_id = st.feed_id AND t.trip_id = st.trip_id
                    JOIN active_services a ON a.service_id = t.service_id
                    WHERE st.feed_id = %s
                      AND st.stop_id = %s
                      AND length(trim(coalesce(st.departure_time, st.arrival_time, ''))) >= 5
                ),
                in_window AS (
                    SELECT route_id, direction_id, min(sec) AS first_sec, max(sec) AS last_sec
                    FROM stop_sec
                    WHERE sec BETWEEN %s AND %s
                    GROUP BY route_id, direction_id
                )
                SELECT
                    i.route_id,
                    i.direction_id,
                    i.first_sec,
                    i.last_sec,
                    r.short_name,
                    r.long_name,
                    r.agency_id,
                    a.name AS agency_name
                FROM in_window i
                JOIN routes r ON r.feed_id = %s AND r.route_id = i.route_id
                LEFT JOIN agencies a ON a.feed_id = r.feed_id AND a.agency_id = r.agency_id
                ORDER BY r.short_name, i.route_id, i.direction_id
                LIMIT %s
                """,
                (
                    yyyymmdd,
                    feed_id,
                    date_ymd,
                    feed_id,
                    date_ymd,
                    feed_id,
                    date_ymd,
                    feed_id,
                    stop_id,
                    start_sec,
                    end_sec,
                    feed_id,
                    max_results,
                ),
            )
            rows = cur.fetchall()
        results = []
        for r in rows:
            dir_val = r["direction_id"]
            direction_id = None if dir_val is None else str(dir_val)
            first_sec = int(r["first_sec"])
            last_sec = int(r["last_sec"])
            results.append(
                {
                    "route_id": r["route_id"],
                    "direction_id": direction_id,
                    "route_short_name": r["short_name"],
                    "route_long_name": r["long_name"],
                    "agency_id": r["agency_id"],
                    "agency_name": r["agency_name"],
                    "first_time": f"{first_sec // 3600:02d}:{(first_sec % 3600) // 60:02d}",
                    "last_time": f"{last_sec // 3600:02d}:{(last_sec % 3600) // 60:02d}",
                }
            )
        return results
    finally:
        if close:
            conn.close()


def has_calendar_exception_for_date(
    yyyymmdd: str,
    conn=None,
) -> bool:
    """
    Return True when calendar_dates contains at least one exception row
    for the active feed and service date.
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
                SELECT 1
                FROM calendar_dates
                WHERE feed_id = %s
                  AND date = %s
                LIMIT 1
                """,
                (feed_id, int(yyyymmdd)),
            )
            return cur.fetchone() is not None
    finally:
        if close:
            conn.close()


def get_route_direction_pairs(feed_id: int, conn=None) -> List[RouteDirKey]:
    """
    Return all distinct (route_id, direction_id) for a feed.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT route_id, direction_id
                FROM trips
                WHERE feed_id = %s
                ORDER BY route_id, direction_id
                """,
                (feed_id,),
            )
            out: List[RouteDirKey] = []
            for row in cur.fetchall():
                out.append(
                    (
                        row["route_id"],
                        None if row["direction_id"] is None else str(row["direction_id"]),
                    )
                )
            return out
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


def compute_route_signatures_bulk(feed_id: int, conn=None) -> Dict[RouteDirKey, str]:
    """
    Compute signature for every (route_id, direction_id) in the feed in 3 queries.
    Returns (route_id, direction_id) -> sig_hash. Memory-heavy for large feeds.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trip_id, service_id, shape_id, route_id, direction_id FROM trips WHERE feed_id = %s",
                (feed_id,),
            )
            trips_rows = cur.fetchall()
        # Group trips by (route_id, direction_id)
        by_key: Dict[RouteDirKey, List[Dict]] = {}
        for r in trips_rows:
            key: RouteDirKey = (
                r["route_id"],
                None if r["direction_id"] is None else str(r["direction_id"]),
            )
            by_key.setdefault(key, []).append({
                "trip_id": r["trip_id"],
                "service_id": r["service_id"],
                "shape_id": r["shape_id"],
            })
        for k, lst in by_key.items():
            lst.sort(key=lambda x: x["trip_id"])

        trip_ids = [r["trip_id"] for r in trips_rows]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trip_id, stop_id, stop_sequence
                FROM stop_times
                WHERE feed_id = %s AND trip_id = ANY(%s)
                ORDER BY trip_id, stop_sequence
                """,
                (feed_id, trip_ids),
            )
            stop_times_rows = cur.fetchall()
        stop_times_by_trip: Dict[str, List[Dict]] = {}
        for row in stop_times_rows:
            stop_times_by_trip.setdefault(row["trip_id"], []).append(dict(row))

        shape_ids = sorted({r["shape_id"] for r in trips_rows if r.get("shape_id")})
        shapes_rows: List[Dict] = []
        if shape_ids:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT shape_id, seq, lat, lon
                    FROM shapes
                    WHERE feed_id = %s AND shape_id = ANY(%s)
                    ORDER BY shape_id, seq
                    """,
                    (feed_id, shape_ids),
                )
                shapes_rows = [dict(r) for r in cur.fetchall()]
        shapes_by_id: Dict[str, List[Dict]] = {}
        for s in shapes_rows:
            shapes_by_id.setdefault(s["shape_id"], []).append(s)

        out: Dict[RouteDirKey, str] = {}
        for key, trips in by_key.items():
            trip_ids_k = [t["trip_id"] for t in trips]
            stop_times = []
            for tid in trip_ids_k:
                stop_times.extend(stop_times_by_trip.get(tid, []))
            shape_ids_k = sorted({t["shape_id"] for t in trips if t.get("shape_id")})
            shapes = []
            for sid in shape_ids_k:
                shapes.extend(shapes_by_id.get(sid, []))
            payload = {"trips": trips, "stop_times": stop_times, "shapes": shapes}
            raw = json.dumps(payload, sort_keys=True).encode("utf-8")
            out[key] = hashlib.sha256(raw).hexdigest()
        return out
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
    pretty_osm: bool,
    route_sig_hash: str,
    conn=None,
) -> Optional[bytes]:
    """
    Return a cached pickled graph entry from PostGIS route_graph_cache if present.

    The cache is keyed by (feed_id, route_id, direction_id, pretty_osm) and guarded
    by route_sig_hash. date_ymd is treated as metadata only.
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
                  AND pretty_osm = %s
                  AND route_sig_hash = %s
                """,
                (
                    feed_id,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
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


def get_cached_graphs_bulk(
    feed_id: int,
    pretty_osm: bool,
    conn=None,
) -> Dict[RouteDirKey, Tuple[str, bytes]]:
    """
    Load all cached graph blobs for (feed_id, pretty_osm).
    Returns (route_id, direction_id) -> (sig_hash, blob).
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT route_id, direction_id, route_sig_hash, graph_blob
                FROM route_graph_cache
                WHERE feed_id = %s AND pretty_osm = %s
                """,
                (feed_id, pretty_osm),
            )
            out: Dict[RouteDirKey, Tuple[str, bytes]] = {}
            for row in cur.fetchall():
                key: RouteDirKey = (
                    row["route_id"],
                    None if row["direction_id"] is None else str(row["direction_id"]),
                )
                out[key] = (row["route_sig_hash"], bytes(row["graph_blob"]))
            return out
    finally:
        if close:
            conn.close()


def save_route_graph_pg(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    pretty_osm: bool,
    route_sig_hash: str,
    graph_blob: bytes,
    date_ymd: Optional[str] = None,
    conn=None,
) -> None:
    """
    Save or update a cached pickled graph entry in PostGIS route_graph_cache.

    The logical key is (feed_id, route_id, direction_id, pretty_osm); date_ymd is
    stored as metadata only when provided.
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
                ON CONFLICT (feed_id, route_id, direction_id, pretty_osm)
                DO UPDATE SET
                  date_ymd       = EXCLUDED.date_ymd,
                  route_sig_hash = EXCLUDED.route_sig_hash,
                  graph_blob     = EXCLUDED.graph_blob,
                  created_at     = NOW()
                """,
                (
                    feed_id,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    int(date_ymd) if date_ymd is not None else None,
                    pretty_osm,
                    route_sig_hash,
                    psycopg2.Binary(graph_blob),
                ),
            )
            conn.commit()
    finally:
        if close:
            conn.close()



def get_blocked_edge_keys_pg(
    edge_geometries: Dict[Tuple[str, str], Any],
    blockage_geojson: Dict[str, Any],
    conn=None,
) -> Optional[set]:
    """
    Return set of (node_u, node_v) keys whose edge geometry intersects the blockage polygon.
    Uses PostGIS for intersection; buffer ~110 m applied to polygon.
    Returns None on error so caller can fall back to Python STRtree.
    """
    if not edge_geometries:
        return set()
    try:
        from shapely.geometry import shape as _shape

        geom = _shape(blockage_geojson)
        if geom.is_empty:
            return set()
        geom = geom.buffer(1e-3)
        if geom.is_empty:
            return set()
        polygon_wkt = geom.wkt
    except Exception:
        return None
    keys = list(edge_geometries.keys())
    wkts = []
    for k in keys:
        eg = edge_geometries.get(k)
        if eg is not None and getattr(eg, "linestring", None) is not None:
            wkts.append(eg.linestring.wkt)
        else:
            wkts.append("LINESTRING EMPTY")
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH edges AS (
                    SELECT ordinality AS idx, ST_GeomFromText(wkt, 4326) AS geom
                    FROM unnest(%s::text[]) WITH ORDINALITY AS t(wkt)
                    WHERE wkt IS NOT NULL AND wkt != '' AND wkt NOT LIKE '%%EMPTY%%'
                ),
                poly AS (
                    SELECT ST_Buffer(ST_GeomFromText(%s, 4326)::geography, 110)::geometry AS g
                )
                SELECT e.idx
                FROM edges e, poly p
                WHERE ST_Intersects(e.geom, p.g)
                """,
                (wkts, polygon_wkt),
            )
            indices = {int(row["idx"]) for row in cur.fetchall()}
    except Exception:
        if close:
            conn.close()
        return None
    if close:
        conn.close()
    return {keys[i - 1] for i in indices if 1 <= i <= len(keys)}


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

