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

import psycopg2
from psycopg2.extras import DictCursor


# NOTE: On this Windows setup, reading DATABASE_URL from the environment caused
# a UnicodeDecodeError inside psycopg2 due to non-UTF-8 bytes in the DSN.
# To keep things simple and robust here, we use an explicit ASCII DSN string.
DB_URL = "postgresql://postgres@localhost:5432/israel_gtfs"


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


