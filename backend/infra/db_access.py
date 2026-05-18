from __future__ import annotations

"""
PostgreSQL/PostGIS data access layer for Israel GTFS Detour Router.

This module hides raw SQL behind a small set of functions used by:
  - area_search (routes in polygon for a date/time window)
  - graph_builder / detour_graph (patterns, stops, shapes, trip time bounds)

It assumes the schema defined in backend/sql/schema/db_postgis_schema.sql and that
feed_versions.active indicates the current GTFS feed.
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# Optional[str] for direction_id in tuple keys (route_id, direction_id)
RouteDirKey = Tuple[str, Optional[str]]

# route_signatures / route_graph_cache PK columns cannot store SQL NULL for direction_id.
_DIRECTION_ID_NULL_DB = -1


def direction_id_db_value(direction_id: Optional[str]) -> int:
    """Map logical direction_id to DB column value (use -1 when GTFS direction is absent)."""
    if direction_id is None or direction_id == "":
        return _DIRECTION_ID_NULL_DB
    return int(direction_id)


def direction_id_from_db(value: Optional[int]) -> Optional[str]:
    if value is None or int(value) == _DIRECTION_ID_NULL_DB:
        return None
    return str(int(value))

import os
import hashlib
import json
import time

import psycopg2
from psycopg2.extras import DictCursor, Json

from backend.infra.config import LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
from backend.infra.logging_utils import log


# In containers and most environments we prefer an explicit DATABASE_URL.
# For backward compatibility with the existing Windows setup, fall back to
# the previous localhost DSN if the env var is not set.
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs")
STOP_ROUTES_STATEMENT_TIMEOUT_MS = int(os.getenv("STOP_ROUTES_STATEMENT_TIMEOUT_MS", "90000"))
# Area polygon search can be heavy on full national feeds. 0 = no PostgreSQL statement limit.
# Set AREA_ROUTES_STATEMENT_TIMEOUT_MS to e.g. 180000 to cap long queries (milliseconds).
AREA_ROUTES_STATEMENT_TIMEOUT_MS = int(os.getenv("AREA_ROUTES_STATEMENT_TIMEOUT_MS", "0"))
ROUTES_TILE_STATEMENT_TIMEOUT_MS = int(os.getenv("ROUTES_TILE_STATEMENT_TIMEOUT_MS", "15000"))


class StopRoutesQueryTimeoutError(RuntimeError):
    pass


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
                # Recovery path for partially migrated/loaded DBs where active was never flipped.
                cur.execute("SELECT id FROM feed_versions ORDER BY fetched_at DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    try:
                        chosen = int(row["id"])  # type: ignore[index]
                    except Exception:
                        chosen = int(row[0])
                    log("db/feed", f"no active feed flag set; using latest feed_id={chosen}")
                    return chosen
                raise RuntimeError("No active feed in feed_versions")
            # Support both DictCursor (row["id"]) and regular cursor (row[0]).
            try:
                return int(row["id"])  # type: ignore[index]
            except Exception:
                return int(row[0])
    finally:
        if close:
            conn.close()


def get_active_feed_calendar_span(conn=None) -> Tuple[Optional[int], Optional[int]]:
    """
    MIN(start_date) and MAX(end_date) across calendar rows for the active feed.
    Returns (None, None) if there is no calendar data (e.g. calendar_dates-only feeds).
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
                SELECT MIN(start_date) AS mn, MAX(end_date) AS mx
                FROM calendar
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            row = cur.fetchone()
            if not row or row["mn"] is None or row["mx"] is None:
                return (None, None)
            return (int(row["mn"]), int(row["mx"]))
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
    trip_count: Optional[int]
    last_stop_name: Optional[str] = None
    time_match_confidence: Optional[str] = None
    time_match_note: Optional[str] = None


def get_routes_in_polygon(
    polygon_wkt: str,
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    time_semantics_mode: str = "legacy_trip_overlap",
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
        log(
            "area/routes",
            f"phase=pg_single_day_begin feed_id={feed_id} date={date_ymd} "
            f"time_window_sec={start_sec}-{end_sec} wkt_chars={len(polygon_wkt)} "
            f"statement_timeout_ms={AREA_ROUTES_STATEMENT_TIMEOUT_MS} "
            f"time_semantics_mode={time_semantics_mode}",
        )
        if time_semantics_mode != "legacy_trip_overlap":
            return _get_routes_in_polygon_pass_through_single_day(
                conn=conn,
                feed_id=feed_id,
                polygon_wkt=polygon_wkt,
                date_ymd=date_ymd,
                start_sec=start_sec,
                end_sec=end_sec,
                time_semantics_mode=time_semantics_mode,
            )
        with conn.cursor() as cur:
            cur.execute(
                "SET LOCAL statement_timeout = %s", (AREA_ROUTES_STATEMENT_TIMEOUT_MS,)
            )
            t_exec = time.perf_counter()
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
                shapes_in_area AS MATERIALIZED (
                    SELECT sl.feed_id, sl.shape_id
                    FROM shapes_lines sl
                    WHERE sl.feed_id = %s
                      AND ST_Intersects(sl.geom, ST_GeomFromText(%s, 4326))
                ),
                shape_hit_trips AS MATERIALIZED (
                    SELECT DISTINCT t.feed_id, t.trip_id, t.route_id, t.direction_id, t.service_id
                    FROM shapes_in_area sia
                    JOIN trips t
                      ON t.feed_id = sia.feed_id
                      AND t.shape_id = sia.shape_id
                      AND t.shape_id IS NOT NULL
                    WHERE t.feed_id = %s
                ),
                trips_in_window AS MATERIALIZED (
                    SELECT sht.feed_id, sht.trip_id, sht.route_id, sht.direction_id
                    FROM shape_hit_trips sht
                    JOIN active_services s ON s.service_id = sht.service_id
                    JOIN trip_time_bounds b ON b.feed_id = sht.feed_id AND b.trip_id = sht.trip_id
                    WHERE sht.feed_id = %s
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
                        MAX(b.last_sec) AS last_time_s,
                        COUNT(DISTINCT tiw.trip_id)::int AS trip_count
                    FROM trips_in_window tiw
                    JOIN trip_time_bounds b
                      ON b.feed_id = tiw.feed_id AND b.trip_id = tiw.trip_id
                    JOIN routes r
                      ON r.feed_id = tiw.feed_id AND r.route_id = tiw.route_id
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
                    rg.last_time_s,
                    rg.trip_count,
                    NULL::text AS last_stop_name
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
                    polygon_wkt,
                    feed_id,
                    feed_id,
                    start_sec,
                    end_sec,
                    feed_id,
                ),
            )
            exec_ms = int((time.perf_counter() - t_exec) * 1000)
            log("area/routes", f"phase=pg_single_day_execute_done elapsed_ms={exec_ms}")
            t_fetch = time.perf_counter()
            raw_rows = cur.fetchall()
            fetch_ms = int((time.perf_counter() - t_fetch) * 1000)
            log(
                "area/routes",
                f"phase=pg_single_day_fetch_done rows={len(raw_rows)} elapsed_ms={fetch_ms}",
            )
            rows = []
            for r in raw_rows:
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
                        trip_count=r["trip_count"],
                        last_stop_name=r.get("last_stop_name"),
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


def get_detour_patterns_for_routes(
    route_ids: List[str],
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    aoi_geojson: Dict[str, Any],
    k_per_route_dir: int = 6,
    direction_filter_by_route: Optional[Dict[str, Optional[str]]] = None,
    min_overlap_m: float = 0.0,
    conn=None,
) -> Dict[RouteDirKey, List[PatternMeta]]:
    """
    Return AOI-ranked Top-K patterns per (route_id, direction_id) using pattern_detour_index.

    This is detour-specific and intentionally does not fall back to legacy frequency-only
    selectors; callers should decide whether missing results are terminal.
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
                WITH aoi AS (
                    SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS geom
                ),
                dow AS (
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
                    COALESCE(sc.active_trip_count, 0) AS active_trip_count,
                    ST_Length(
                        ST_Intersection(pdi.corridor_geom, aoi.geom)::geography
                    ) AS overlap_m,
                    CASE
                        WHEN pdi.length_m > 0
                        THEN ST_Length(ST_Intersection(pdi.corridor_geom, aoi.geom)::geography) / pdi.length_m
                        ELSE 0.0
                    END AS overlap_ratio
                FROM pattern_detour_index pdi
                JOIN patterns p
                  ON p.feed_id = pdi.feed_id AND p.pattern_id = pdi.pattern_id
                LEFT JOIN strict_counts sc
                  ON sc.pattern_id = p.pattern_id
                CROSS JOIN aoi
                WHERE pdi.feed_id = %s
                  AND p.route_id = ANY(%s)
                  AND pdi.corridor_geom IS NOT NULL
                  AND ST_Intersects(pdi.corridor_geom, aoi.geom)
                ORDER BY
                    p.route_id,
                    p.direction_id NULLS FIRST,
                    overlap_m DESC,
                    overlap_ratio DESC,
                    COALESCE(sc.active_trip_count, 0) DESC,
                    p.frequency DESC NULLS LAST,
                    p.pattern_id ASC
                """,
                (
                    json.dumps(aoi_geojson),
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

        by_key: Dict[RouteDirKey, List[PatternMeta]] = {}
        for row in rows:
            rid = row["route_id"]
            if rid not in wanted_routes:
                continue
            did_str = None if row["direction_id"] is None else str(row["direction_id"])
            if rid in dir_filter_int:
                wanted_did = dir_filter_int[rid]
                if wanted_did is not None and row["direction_id"] != wanted_did:
                    continue
            overlap_m = float(row.get("overlap_m") or 0.0)
            if overlap_m < float(min_overlap_m):
                continue
            key: RouteDirKey = (rid, did_str)
            by_key.setdefault(key, []).append(
                PatternMeta(
                    pattern_id=row["pattern_id"],
                    route_id=rid,
                    direction_id=row["direction_id"],
                    repr_trip_id=row["repr_trip_id"],
                    repr_shape_id=row["repr_shape_id"],
                    stop_ids=list(row["stop_ids"] or []),
                    frequency=int(row["frequency"] or 0),
                    used_shape=bool(row["used_shape"]),
                    active_trip_count=int(row["active_trip_count"] or 0),
                    selection_source="detour_spatial",
                )
            )

        selected: Dict[RouteDirKey, List[PatternMeta]] = {}
        for key in sorted(by_key.keys()):
            metas = by_key.get(key) or []
            if metas:
                selected[key] = metas[:k]
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
                    str(row["route_id"]),
                    None if row["direction_id"] is None else str(row["direction_id"]),
                )
                out[key] = PatternMeta(
                    pattern_id=str(row["pattern_id"]) if row["pattern_id"] is not None else "",
                    route_id=str(row["route_id"]),
                    direction_id=row["direction_id"],
                    repr_trip_id=str(row["repr_trip_id"]) if row["repr_trip_id"] is not None else "",
                    repr_shape_id=(
                        str(row["repr_shape_id"]) if row["repr_shape_id"] is not None else None
                    ),
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
                pid = str(row["pattern_id"]) if row["pattern_id"] is not None else ""
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
                sid = str(row["shape_id"]) if row["shape_id"] is not None else ""
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
                tid = str(row["trip_id"]) if row["trip_id"] is not None else ""
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
                tid = str(row["trip_id"]) if row["trip_id"] is not None else ""
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


def search_stops_pg(
    q: str,
    limit: int = 20,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Search active-feed stops by stop name, stop code, or stop_id.
    Ranking: code/id prefix first, then name prefix, then contains.
    """
    query = (q or "").strip()
    if not query:
        return []
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        prefix = f"{query}%"
        contains = f"%{query}%"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_schema = current_schema()
                    AND table_name = 'stops'
                    AND column_name = 'stop_code'
                ) AS has_stop_code
                """
            )
            has_stop_code = bool((cur.fetchone() or {}).get("has_stop_code"))

            if has_stop_code:
                cur.execute(
                    """
                    SELECT stop_id, name, stop_code, lat, lon
                    FROM stops
                    WHERE feed_id = %s
                      AND (
                        name ILIKE %s
                        OR COALESCE(stop_code, '') ILIKE %s
                        OR stop_id ILIKE %s
                      )
                    ORDER BY
                      CASE
                        WHEN COALESCE(stop_code, '') ILIKE %s OR stop_id ILIKE %s THEN 0
                        WHEN name ILIKE %s THEN 1
                        ELSE 2
                      END,
                      name ASC
                    LIMIT %s
                    """,
                    (feed_id, contains, contains, contains, prefix, prefix, prefix, max(1, int(limit))),
                )
            else:
                cur.execute(
                    """
                    SELECT stop_id, name, NULL::text AS stop_code, lat, lon
                    FROM stops
                    WHERE feed_id = %s
                      AND (
                        name ILIKE %s
                        OR stop_id ILIKE %s
                      )
                    ORDER BY
                      CASE
                        WHEN stop_id ILIKE %s THEN 0
                        WHEN name ILIKE %s THEN 1
                        ELSE 2
                      END,
                      name ASC
                    LIMIT %s
                    """,
                    (feed_id, contains, contains, prefix, prefix, max(1, int(limit))),
                )
            return [
                {
                    "stop_id": r["stop_id"],
                    "stop_name": r["name"],
                    "stop_code": r.get("stop_code"),
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
            # Guardrail: avoid long-running stop query hanging the API call.
            # Helpful DB indexes for large feeds:
            #   stop_times(feed_id, stop_id, trip_id)
            #   trips(feed_id, trip_id, service_id, route_id, direction_id)
            cur.execute("SET LOCAL statement_timeout = %s", (STOP_ROUTES_STATEMENT_TIMEOUT_MS,))
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
                stop_filtered AS (
                    SELECT
                        st.trip_id,
                        trim(coalesce(st.departure_time, st.arrival_time, '')) AS dep_time_raw
                    FROM stop_times st
                    WHERE st.feed_id = %s
                      AND st.stop_id = %s
                      AND length(trim(coalesce(st.departure_time, st.arrival_time, ''))) >= 5
                ),
                stop_sec AS (
                    SELECT
                        sf.trip_id,
                        t.route_id,
                        t.direction_id,
                        (split_part(sf.dep_time_raw, ':', 1)::int * 3600
                         + split_part(sf.dep_time_raw, ':', 2)::int * 60
                         + coalesce(nullif(trim(split_part(sf.dep_time_raw, ':', 3)), ''), '0')::int
                        ) AS sec
                    FROM stop_filtered sf
                    JOIN trips t ON t.feed_id = %s AND t.trip_id = sf.trip_id
                    JOIN active_services a ON a.service_id = t.service_id
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
                    feed_id,
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
    except psycopg2.errors.QueryCanceled as e:
        raise StopRoutesQueryTimeoutError(
            f"Stop routes query timed out after {STOP_ROUTES_STATEMENT_TIMEOUT_MS}ms"
        ) from e
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


def compute_route_signatures_bulk(
    feed_id: int,
    conn=None,
    progress_every: int = 0,
) -> Dict[RouteDirKey, str]:
    """
    Compute signature for every (route_id, direction_id) in the feed in 3 queries.
    Returns (route_id, direction_id) -> sig_hash. Memory-heavy for large feeds.
    """
    from backend.infra.logging_utils import log

    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        t0 = time.monotonic()
        log("precompute_graphs", f"signatures: trips query start (feed_id={feed_id})")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trip_id, service_id, shape_id, route_id, direction_id FROM trips WHERE feed_id = %s",
                (feed_id,),
            )
            trips_rows = cur.fetchall()
        log(
            "precompute_graphs",
            f"signatures: trips query done rows={len(trips_rows)} elapsed={time.monotonic() - t0:.2f}s",
        )
        # Group trips by (route_id, direction_id)
        by_key: Dict[RouteDirKey, List[Dict]] = {}
        for r in trips_rows:
            key: RouteDirKey = (
                str(r["route_id"]),
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
        t1 = time.monotonic()
        log("precompute_graphs", f"signatures: stop_times query start trips={len(trip_ids)}")
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
        log(
            "precompute_graphs",
            f"signatures: stop_times query done rows={len(stop_times_rows)} elapsed={time.monotonic() - t1:.2f}s",
        )
        stop_times_by_trip: Dict[str, List[Dict]] = {}
        for row in stop_times_rows:
            tid = str(row["trip_id"]) if row["trip_id"] is not None else ""
            stop_times_by_trip.setdefault(tid, []).append(dict(row))

        shape_ids = sorted({r["shape_id"] for r in trips_rows if r.get("shape_id")})
        shapes_rows: List[Dict] = []
        if shape_ids:
            t2 = time.monotonic()
            log("precompute_graphs", f"signatures: shapes query start shape_ids={len(shape_ids)}")
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
            log(
                "precompute_graphs",
                f"signatures: shapes query done rows={len(shapes_rows)} elapsed={time.monotonic() - t2:.2f}s",
            )
        shapes_by_id: Dict[str, List[Dict]] = {}
        for s in shapes_rows:
            sid = str(s["shape_id"]) if s["shape_id"] is not None else ""
            shapes_by_id.setdefault(sid, []).append(s)

        out: Dict[RouteDirKey, str] = {}
        total_keys = len(by_key)
        if total_keys:
            log("precompute_graphs", f"signatures: hashing {total_keys} route-direction keys")
        hash_t0 = time.monotonic()
        for i, (key, trips) in enumerate(by_key.items(), start=1):
            trip_ids_k = [t["trip_id"] for t in trips]
            stop_times = []
            for tid in trip_ids_k:
                stop_times.extend(
                    stop_times_by_trip.get(str(tid) if tid is not None else "", [])
                )
            shape_ids_k = sorted({t["shape_id"] for t in trips if t.get("shape_id")})
            shapes = []
            for sid in shape_ids_k:
                shapes.extend(
                    shapes_by_id.get(str(sid) if sid is not None else "", [])
                )
            payload = {"trips": trips, "stop_times": stop_times, "shapes": shapes}
            raw = json.dumps(payload, sort_keys=True).encode("utf-8")
            out[key] = hashlib.sha256(raw).hexdigest()
            if progress_every > 0 and (i % progress_every == 0 or i == total_keys):
                log(
                    "precompute_graphs",
                    f"signatures: hashed {i}/{total_keys} elapsed={time.monotonic() - hash_t0:.2f}s",
                )
        log(
            "precompute_graphs",
            f"signatures: done total={len(out)} elapsed={time.monotonic() - t0:.2f}s",
        )
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
                  AND direction_id = %s
                """,
                (feed_id, route_id, direction_id_db_value(direction_id)),
            )
            row = cur.fetchone()
            if not row:
                return None
            try:
                return row["sig_hash"]
            except (TypeError, KeyError):
                return row[0]
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
                (feed_id, route_id, direction_id_db_value(direction_id), sig_hash),
            )
            if close:
                conn.commit()
    finally:
        if close:
            conn.close()


def get_feed_checksums(feed_id: int, conn=None) -> Tuple[Optional[str], Optional[str]]:
    """Return (checksum, patterns_built_checksum) for feed_versions.id = feed_id."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT checksum, patterns_built_checksum
                FROM feed_versions
                WHERE id = %s
                """,
                (feed_id,),
            )
            row = cur.fetchone()
            if not row:
                return None, None
            return row.get("checksum"), row.get("patterns_built_checksum")
    finally:
        if close:
            conn.close()


def get_route_signatures_bulk(
    feed_id: int,
    conn=None,
) -> Dict[RouteDirKey, str]:
    """Load route_signatures.sig_hash for every row in the feed (one query)."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT route_id, direction_id, sig_hash
                FROM route_signatures
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            out: Dict[RouteDirKey, str] = {}
            for row in cur.fetchall():
                try:
                    route_id = str(row["route_id"])
                    dir_val = row["direction_id"]
                    sig_hash = row["sig_hash"]
                except (TypeError, KeyError):
                    route_id = str(row[0])
                    dir_val = row[1]
                    sig_hash = row[2]
                key: RouteDirKey = (route_id, direction_id_from_db(dir_val))
                out[key] = sig_hash
            return out
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
                    direction_id_db_value(direction_id),
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
                    str(row["route_id"]),
                    direction_id_from_db(row["direction_id"]),
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
    *,
    commit: bool = True,
) -> None:
    """
    Save or update a cached pickled graph entry in PostGIS route_graph_cache.

    The logical key is (feed_id, route_id, direction_id, pretty_osm); date_ymd is
    stored as metadata only when provided.

    If ``commit`` is False, the INSERT runs in the current transaction; the caller
    must ``commit`` (or rely on autocommit). Own connections always commit before close.
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
                    direction_id_db_value(direction_id),
                    int(date_ymd) if date_ymd is not None else None,
                    pretty_osm,
                    route_sig_hash,
                    psycopg2.Binary(graph_blob),
                ),
            )
        if close or commit:
            conn.commit()
    finally:
        if close:
            conn.close()


def get_cached_route_preview_pg(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    profile_key: str,
    pretty_osm: bool,
    route_sig_hash: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Return cached preview payload for route/profile when signature matches.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pattern_id, preview_blob
                FROM route_preview_cache
                WHERE feed_id = %s
                  AND route_id = %s
                  AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
                  AND profile_key = %s
                  AND pretty_osm = %s
                  AND route_sig_hash = %s
                """,
                (
                    feed_id,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    profile_key,
                    pretty_osm,
                    route_sig_hash,
                ),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "pattern_id": row["pattern_id"],
                "preview_blob": bytes(row["preview_blob"]),
            }
    finally:
        if close:
            conn.close()


def get_cached_previews_bulk(
    feed_id: int,
    profile_key: str,
    pretty_osm: bool,
    conn=None,
) -> Dict[RouteDirKey, Tuple[str, str, bytes]]:
    """
    Load preview cache rows for (feed_id, profile_key, pretty_osm).
    Returns (route_id, direction_id) -> (route_sig_hash, pattern_id, preview_blob)
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT route_id, direction_id, route_sig_hash, pattern_id, preview_blob
                FROM route_preview_cache
                WHERE feed_id = %s
                  AND profile_key = %s
                  AND pretty_osm = %s
                """,
                (feed_id, profile_key, pretty_osm),
            )
            out: Dict[RouteDirKey, Tuple[str, str, bytes]] = {}
            for row in cur.fetchall():
                key: RouteDirKey = (
                    row["route_id"],
                    None if row["direction_id"] is None else str(row["direction_id"]),
                )
                out[key] = (
                    row["route_sig_hash"],
                    str(row["pattern_id"] or ""),
                    bytes(row["preview_blob"]),
                )
            return out
    finally:
        if close:
            conn.close()


def save_route_preview_pg(
    feed_id: int,
    route_id: str,
    direction_id: Optional[str],
    profile_key: str,
    pretty_osm: bool,
    route_sig_hash: str,
    pattern_id: str,
    preview_blob: bytes,
    conn=None,
    *,
    commit: bool = True,
) -> None:
    """
    Save/update cached preview payload keyed by route+profile.

    If ``commit`` is False, the caller must commit. Own connections always commit before close.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO route_preview_cache (
                  feed_id,
                  route_id,
                  direction_id,
                  profile_key,
                  pretty_osm,
                  route_sig_hash,
                  pattern_id,
                  preview_blob
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (feed_id, route_id, direction_id, profile_key, pretty_osm)
                DO UPDATE SET
                  route_sig_hash = EXCLUDED.route_sig_hash,
                  pattern_id     = EXCLUDED.pattern_id,
                  preview_blob   = EXCLUDED.preview_blob,
                  created_at     = NOW()
                """,
                (
                    feed_id,
                    route_id,
                    direction_id_db_value(direction_id),
                    profile_key,
                    pretty_osm,
                    route_sig_hash,
                    pattern_id,
                    psycopg2.Binary(preview_blob),
                ),
            )
        if close or commit:
            conn.commit()
    finally:
        if close:
            conn.close()



def hash_geojson_canonical(geojson: Dict[str, Any]) -> str:
    """Stable hash used for detour-by-area cache keys."""
    raw = json.dumps(
        geojson, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_street_override_key(
    scope: str,
    route_id: str,
    direction_id: Optional[str],
    blockage_hash: str,
    entry_stop_id: str,
    exit_stop_id: str,
    route_sig_hash: str,
) -> str:
    """Deterministic key for detour_street_override (scope: point | by_area)."""
    payload = {
        "scope": scope,
        "route_id": route_id,
        "direction_id": str(direction_id) if direction_id is not None else "",
        "blockage_hash": blockage_hash,
        "entry_stop_id": entry_stop_id,
        "exit_stop_id": exit_stop_id,
        "route_sig_hash": route_sig_hash or "",
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    return hashlib.sha256(raw).hexdigest()


def get_detour_street_override_pg(
    feed_id: int,
    override_key: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Load stored street override blob."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT road_geojson, turn_by_turn_json
                FROM detour_street_override
                WHERE feed_id = %s AND override_key = %s
                """,
                (feed_id, override_key),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "road_geojson": json.loads(str(row["road_geojson"])),
                "turn_by_turn": json.loads(str(row["turn_by_turn_json"])),
            }
    finally:
        if close:
            conn.close()


def save_detour_street_override_pg(
    feed_id: int,
    override_key: str,
    scope: str,
    route_id: str,
    direction_id: Optional[str],
    blockage_hash: str,
    entry_stop_id: str,
    exit_stop_id: str,
    route_sig_hash: str,
    road_geojson: Dict[str, Any],
    turn_by_turn: List[Any],
    conn=None,
    *,
    commit: bool = True,
) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    dir_sql: Optional[int] = None
    if direction_id is not None and str(direction_id).strip() != "":
        try:
            dir_sql = int(str(direction_id).strip())
        except ValueError:
            dir_sql = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO detour_street_override (
                  feed_id,
                  override_key,
                  scope,
                  route_id,
                  direction_id,
                  blockage_hash,
                  entry_stop_id,
                  exit_stop_id,
                  route_sig_hash,
                  road_geojson,
                  turn_by_turn_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (feed_id, override_key)
                DO UPDATE SET
                  road_geojson = EXCLUDED.road_geojson,
                  turn_by_turn_json = EXCLUDED.turn_by_turn_json,
                  created_at = NOW()
                """,
                (
                    feed_id,
                    override_key,
                    scope,
                    route_id,
                    dir_sql,
                    blockage_hash,
                    entry_stop_id,
                    exit_stop_id,
                    route_sig_hash or "",
                    json.dumps(road_geojson, separators=(",", ":"), ensure_ascii=False),
                    json.dumps(turn_by_turn, separators=(",", ":"), ensure_ascii=False),
                ),
            )
        if close or commit:
            conn.commit()
    finally:
        if close:
            conn.close()


def get_cached_detour_by_area_pg(
    feed_id: int,
    mode: str,
    route_id: str,
    direction_id: Optional[str],
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    transfer_radius_m: float,
    use_osm_detour: bool,
    policy_profile: str,
    blockage_hash: str,
    route_sig_hash: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Load cached /detours/by-area route result when key and route signature match."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result_json
                FROM detour_by_area_cache
                WHERE feed_id = %s
                  AND mode = %s
                  AND route_id = %s
                  AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
                  AND date_ymd = %s
                  AND start_sec = %s
                  AND end_sec = %s
                  AND transfer_radius_m = %s
                  AND use_osm_detour = %s
                  AND policy_profile = %s
                  AND blockage_hash = %s
                  AND route_sig_hash = %s
                """,
                (
                    feed_id,
                    mode,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    int(date_ymd),
                    int(start_sec),
                    int(end_sec),
                    float(transfer_radius_m),
                    bool(use_osm_detour),
                    policy_profile,
                    blockage_hash,
                    route_sig_hash,
                ),
            )
            row = cur.fetchone()
            if not row:
                return None
            return json.loads(str(row["result_json"]))
    finally:
        if close:
            conn.close()


def save_detour_by_area_pg(
    feed_id: int,
    mode: str,
    route_id: str,
    direction_id: Optional[str],
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    transfer_radius_m: float,
    use_osm_detour: bool,
    policy_profile: str,
    blockage_hash: str,
    route_sig_hash: str,
    result_json: Dict[str, Any],
    conn=None,
    *,
    commit: bool = True,
) -> None:
    """Save/update cached /detours/by-area route result."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO detour_by_area_cache (
                  feed_id,
                  mode,
                  route_id,
                  direction_id,
                  date_ymd,
                  start_sec,
                  end_sec,
                  transfer_radius_m,
                  use_osm_detour,
                  policy_profile,
                  blockage_hash,
                  route_sig_hash,
                  result_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                  feed_id,
                  mode,
                  route_id,
                  direction_id,
                  date_ymd,
                  start_sec,
                  end_sec,
                  transfer_radius_m,
                  use_osm_detour,
                  policy_profile,
                  blockage_hash
                )
                DO UPDATE SET
                  route_sig_hash = EXCLUDED.route_sig_hash,
                  result_json    = EXCLUDED.result_json,
                  created_at     = NOW()
                """,
                (
                    feed_id,
                    mode,
                    route_id,
                    int(direction_id) if direction_id is not None else None,
                    int(date_ymd),
                    int(start_sec),
                    int(end_sec),
                    float(transfer_radius_m),
                    bool(use_osm_detour),
                    policy_profile,
                    blockage_hash,
                    route_sig_hash,
                    json.dumps(result_json, separators=(",", ":"), ensure_ascii=False),
                ),
            )
        if close or commit:
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


def search_routes_pg(
    q: str,
    limit: int,
    date_ymd: Optional[str] = None,
    start_sec: Optional[int] = None,
    end_sec: Optional[int] = None,
) -> List[Dict[str, Any]]:
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
        use_window = (
            bool(date_ymd)
            and len(str(date_ymd)) == 8
            and str(date_ymd).isdigit()
            and start_sec is not None
            and end_sec is not None
        )
        window_sql = ""
        window_params: List[Any] = []
        if use_window:
            window_sql = """
                ,
                dow AS (
                    SELECT EXTRACT(DOW FROM to_timestamp(%s::text, 'YYYYMMDD'))::int AS d
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
                    SELECT t.route_id, COUNT(DISTINCT t.trip_id)::int AS trip_count
                    FROM trips t
                    JOIN active_services s
                      ON s.service_id = t.service_id
                    JOIN trip_time_bounds b
                      ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                    WHERE t.feed_id = %s
                      AND b.last_sec >= %s
                      AND b.first_sec <= %s
                    GROUP BY t.route_id
                )
            """
            window_params = [
                int(str(date_ymd)),
                feed_id,
                int(str(date_ymd)),
                feed_id,
                int(str(date_ymd)),
                feed_id,
                int(str(date_ymd)),
                feed_id,
                int(start_sec),
                int(end_sec),
            ]
        with conn.cursor() as cur:
            if q.isdigit():
                cur.execute(
                    f"""
                    WITH route_last_stop AS (
                      SELECT DISTINCT ON (t.route_id)
                        t.route_id,
                        s.name AS last_stop_name
                      FROM trips t
                      JOIN (
                        SELECT feed_id, trip_id, MAX(stop_sequence) AS max_seq
                        FROM stop_times
                        GROUP BY feed_id, trip_id
                      ) mx ON mx.feed_id = t.feed_id AND mx.trip_id = t.trip_id
                      JOIN stop_times st
                        ON st.feed_id = mx.feed_id AND st.trip_id = mx.trip_id AND st.stop_sequence = mx.max_seq
                      JOIN stops s ON s.feed_id = st.feed_id AND s.stop_id = st.stop_id
                      WHERE t.feed_id = %s
                      ORDER BY t.route_id, t.trip_id
                    ),
                    filtered_routes AS (
                      SELECT
                        r.route_id,
                        r.short_name,
                        r.long_name,
                        r.agency_id,
                        r.route_type
                      FROM routes r
                      WHERE r.feed_id = %s
                        AND (LOWER(r.route_id) = %s OR LOWER(r.short_name) = %s)
                      LIMIT %s
                    )
                    {window_sql}
                    SELECT
                      fr.route_id,
                      fr.short_name,
                      fr.long_name,
                      fr.agency_id,
                      fr.route_type,
                      a.name AS agency_name,
                      {"COALESCE(tw.trip_count, 0)::int AS trip_count" if use_window else "NULL::int AS trip_count"},
                      rls.last_stop_name
                    FROM filtered_routes fr
                    LEFT JOIN route_last_stop rls ON rls.route_id = fr.route_id
                    LEFT JOIN agencies a
                      ON a.feed_id = %s AND a.agency_id = fr.agency_id
                    {"LEFT JOIN trips_in_window tw ON tw.route_id = fr.route_id" if use_window else ""}
                    """,
                    [feed_id, feed_id, q, q, limit, *window_params, feed_id],
                )
            else:
                like = f"%{q}%"
                cur.execute(
                    f"""
                    WITH route_last_stop AS (
                      SELECT DISTINCT ON (t.route_id)
                        t.route_id,
                        s.name AS last_stop_name
                      FROM trips t
                      JOIN (
                        SELECT feed_id, trip_id, MAX(stop_sequence) AS max_seq
                        FROM stop_times
                        GROUP BY feed_id, trip_id
                      ) mx ON mx.feed_id = t.feed_id AND mx.trip_id = t.trip_id
                      JOIN stop_times st
                        ON st.feed_id = mx.feed_id AND st.trip_id = mx.trip_id AND st.stop_sequence = mx.max_seq
                      JOIN stops s ON s.feed_id = st.feed_id AND s.stop_id = st.stop_id
                      WHERE t.feed_id = %s
                      ORDER BY t.route_id, t.trip_id
                    ),
                    filtered_routes AS (
                      SELECT
                        r.route_id,
                        r.short_name,
                        r.long_name,
                        r.agency_id,
                        r.route_type
                      FROM routes r
                      WHERE r.feed_id = %s
                        AND (
                          LOWER(COALESCE(r.route_id, ''))    LIKE %s OR
                          LOWER(COALESCE(r.short_name, ''))  LIKE %s OR
                          LOWER(COALESCE(r.long_name, ''))   LIKE %s
                        )
                      LIMIT %s
                    )
                    {window_sql}
                    SELECT
                      fr.route_id,
                      fr.short_name,
                      fr.long_name,
                      fr.agency_id,
                      fr.route_type,
                      a.name AS agency_name,
                      {"COALESCE(tw.trip_count, 0)::int AS trip_count" if use_window else "NULL::int AS trip_count"},
                      rls.last_stop_name
                    FROM filtered_routes fr
                    LEFT JOIN route_last_stop rls ON rls.route_id = fr.route_id
                    LEFT JOIN agencies a
                      ON a.feed_id = %s AND a.agency_id = fr.agency_id
                    {"LEFT JOIN trips_in_window tw ON tw.route_id = fr.route_id" if use_window else ""}
                    """,
                    [feed_id, feed_id, like, like, like, limit, *window_params, feed_id],
                )
            return [
                {
                    "route_id": r["route_id"],
                    "route_short_name": r["short_name"],
                    "route_long_name": r["long_name"],
                    "agency_id": r["agency_id"],
                    "route_type": r["route_type"],
                    "agency_name": r["agency_name"],
                    "trip_count": r["trip_count"],
                    "last_stop_name": r.get("last_stop_name"),
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()


def _get_routes_in_polygon_pass_through_single_day(
    conn,
    feed_id: int,
    polygon_wkt: str,
    date_ymd: str,
    start_sec: int,
    end_sec: int,
    time_semantics_mode: str,
) -> List[RouteInAreaRow]:
    confidence_expr = "'approx'::text" if time_semantics_mode == "pass_through_stop_proxy" else (
        "CASE WHEN bool_and(wh.has_anchor_pair AND wh.has_shape_pair) "
        "THEN 'high'::text ELSE 'unknown'::text END"
    )
    note_expr = "NULL::text" if time_semantics_mode == "pass_through_stop_proxy" else (
        "CASE WHEN bool_and(wh.has_anchor_pair AND wh.has_shape_pair) THEN NULL::text ELSE "
        "'Estimated from nearest bracketing stops near polygon; some trips had missing anchors or shape distances.'::text END"
    )
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = %s", (AREA_ROUTES_STATEMENT_TIMEOUT_MS,))
        cur.execute(
            f"""
            WITH poly AS (
                SELECT ST_GeomFromText(%s, 4326) AS geom
            ),
            active_services AS (
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
                EXCEPT
                SELECT cd.service_id
                FROM calendar_dates cd
                WHERE cd.feed_id = %s
                  AND cd.date = %s
                  AND cd.exception_type = 2
            ),
            shapes_in_area AS MATERIALIZED (
                SELECT sl.feed_id, sl.shape_id
                FROM shapes_lines sl
                JOIN poly p ON TRUE
                WHERE sl.feed_id = %s
                  AND ST_Intersects(sl.geom, p.geom)
            ),
            candidate_trips AS MATERIALIZED (
                SELECT DISTINCT t.feed_id, t.trip_id, t.route_id, t.direction_id
                FROM shapes_in_area sia
                JOIN trips t
                  ON t.feed_id = sia.feed_id
                 AND t.shape_id = sia.shape_id
                 AND t.shape_id IS NOT NULL
                JOIN active_services s ON s.service_id = t.service_id
                JOIN trip_time_bounds b
                  ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                WHERE t.feed_id = %s
                  AND b.last_sec >= %s
                  AND b.first_sec <= %s
            ),
            trip_stops AS MATERIALIZED (
                SELECT
                    ct.trip_id,
                    ct.route_id,
                    ct.direction_id,
                    st.stop_sequence,
                    (
                      split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 1)::int * 3600
                      + split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 2)::int * 60
                      + coalesce(nullif(trim(split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 3)), ''), '0')::int
                    ) AS sec,
                    st.shape_dist_traveled AS shape_dist,
                    ST_Distance(
                        ST_SetSRID(ST_MakePoint(sp.lon, sp.lat), 4326),
                        p.geom
                    ) AS dist_m
                FROM candidate_trips ct
                JOIN stop_times st
                  ON st.feed_id = ct.feed_id
                 AND st.trip_id = ct.trip_id
                JOIN stops sp
                  ON sp.feed_id = st.feed_id
                 AND sp.stop_id = st.stop_id
                JOIN poly p ON TRUE
                WHERE st.feed_id = %s
                  AND length(trim(coalesce(st.departure_time, st.arrival_time, ''))) >= 5
            ),
            pivot AS MATERIALIZED (
                SELECT DISTINCT ON (ts.trip_id)
                    ts.trip_id,
                    ts.route_id,
                    ts.direction_id,
                    ts.stop_sequence AS pivot_seq
                FROM trip_stops ts
                ORDER BY ts.trip_id, ts.dist_m ASC, ts.stop_sequence ASC
            ),
            anchor_seq AS MATERIALIZED (
                SELECT
                    p.trip_id,
                    p.route_id,
                    p.direction_id,
                    MAX(CASE WHEN ts.stop_sequence <= p.pivot_seq THEN ts.stop_sequence END) AS a_seq,
                    MIN(CASE WHEN ts.stop_sequence >= p.pivot_seq THEN ts.stop_sequence END) AS b_seq
                FROM pivot p
                JOIN trip_stops ts
                  ON ts.trip_id = p.trip_id
                GROUP BY p.trip_id, p.route_id, p.direction_id
            ),
            anchors AS MATERIALIZED (
                SELECT
                    s.trip_id,
                    s.route_id,
                    s.direction_id,
                    s.a_seq,
                    a.sec AS a_sec,
                    a.dist_m AS a_dist,
                    a.shape_dist AS a_shape,
                    s.b_seq,
                    b.sec AS b_sec,
                    b.dist_m AS b_dist,
                    b.shape_dist AS b_shape
                FROM anchor_seq s
                LEFT JOIN trip_stops a
                  ON a.trip_id = s.trip_id
                 AND a.stop_sequence = s.a_seq
                LEFT JOIN trip_stops b
                  ON b.trip_id = s.trip_id
                 AND b.stop_sequence = s.b_seq
            ),
            trip_pass AS MATERIALIZED (
                SELECT
                    an.trip_id,
                    an.route_id,
                    an.direction_id,
                    (
                        CASE
                            WHEN an.a_seq IS NULL AND an.b_seq IS NULL THEN NULL
                            WHEN an.a_seq IS NULL THEN an.b_sec
                            WHEN an.b_seq IS NULL THEN an.a_sec
                            WHEN an.b_seq = an.a_seq THEN an.a_sec
                            WHEN an.a_sec IS NULL OR an.b_sec IS NULL THEN NULL
                            WHEN an.b_sec <= an.a_sec THEN NULL
                            WHEN coalesce(an.a_dist, 0) + coalesce(an.b_dist, 0) > 0
                                THEN round(
                                    an.a_sec
                                    + (an.b_sec - an.a_sec)
                                      * (coalesce(an.a_dist, 0) / (coalesce(an.a_dist, 0) + coalesce(an.b_dist, 0)))
                                )::int
                            ELSE ((an.a_sec + an.b_sec) / 2)::int
                        END
                    ) AS pass_sec,
                    (an.a_seq IS NOT NULL AND an.b_seq IS NOT NULL AND an.b_seq > an.a_seq) AS has_anchor_pair,
                    (an.a_shape IS NOT NULL AND an.b_shape IS NOT NULL) AS has_shape_pair
                FROM anchors an
            ),
            window_hits AS MATERIALIZED (
                SELECT
                    tp.trip_id,
                    tp.route_id,
                    tp.direction_id,
                    tp.pass_sec,
                    tp.has_anchor_pair,
                    tp.has_shape_pair
                FROM trip_pass tp
                WHERE tp.pass_sec IS NOT NULL
                  AND tp.pass_sec BETWEEN %s AND %s
            ),
            routes_geom AS (
                SELECT
                    wh.route_id,
                    wh.direction_id,
                    r.short_name AS route_short_name,
                    r.long_name AS route_long_name,
                    r.agency_id,
                    MIN(wh.pass_sec) AS first_time_s,
                    MAX(wh.pass_sec) AS last_time_s,
                    COUNT(DISTINCT wh.trip_id)::int AS trip_count,
                    {confidence_expr} AS time_match_confidence,
                    {note_expr} AS time_match_note
                FROM window_hits wh
                JOIN routes r
                  ON r.feed_id = %s AND r.route_id = wh.route_id
                GROUP BY wh.route_id, wh.direction_id, r.short_name, r.long_name, r.agency_id
            )
            SELECT
                rg.route_id,
                rg.direction_id,
                rg.route_short_name,
                rg.route_long_name,
                rg.agency_id,
                a.name AS agency_name,
                rg.first_time_s,
                rg.last_time_s,
                rg.trip_count,
                NULL::text AS last_stop_name,
                rg.time_match_confidence,
                rg.time_match_note
            FROM routes_geom rg
            LEFT JOIN agencies a
              ON a.feed_id = %s AND a.agency_id = rg.agency_id
            ORDER BY rg.route_short_name, rg.route_id, rg.direction_id
            """,
            (
                polygon_wkt,
                feed_id,
                int(date_ymd),
                date_ymd,
                feed_id,
                int(date_ymd),
                feed_id,
                int(date_ymd),
                feed_id,
                feed_id,
                start_sec,
                end_sec,
                feed_id,
                start_sec,
                end_sec,
                feed_id,
                feed_id,
            ),
        )
        return [
            RouteInAreaRow(
                route_id=r["route_id"],
                direction_id=r["direction_id"],
                route_short_name=r["route_short_name"],
                route_long_name=r["route_long_name"],
                agency_id=r["agency_id"],
                agency_name=r["agency_name"],
                first_time_s=r["first_time_s"],
                last_time_s=r["last_time_s"],
                trip_count=r["trip_count"],
                last_stop_name=r.get("last_stop_name"),
                time_match_confidence=r.get("time_match_confidence"),
                time_match_note=r.get("time_match_note"),
            )
            for r in cur.fetchall()
        ]


def _get_routes_in_polygon_pass_through_multi_day(
    conn,
    feed_id: int,
    polygon_wkt: str,
    start_date_ymd: str,
    start_sec: int,
    end_date_ymd: str,
    end_sec: int,
    time_semantics_mode: str,
) -> List[RouteInAreaRow]:
    confidence_expr = "'approx'::text" if time_semantics_mode == "pass_through_stop_proxy" else (
        "CASE WHEN bool_and(wh.has_anchor_pair AND wh.has_shape_pair) "
        "THEN 'high'::text ELSE 'unknown'::text END"
    )
    note_expr = "NULL::text" if time_semantics_mode == "pass_through_stop_proxy" else (
        "CASE WHEN bool_and(wh.has_anchor_pair AND wh.has_shape_pair) THEN NULL::text ELSE "
        "'Estimated from nearest bracketing stops near polygon; some trips had missing anchors or shape distances.'::text END"
    )
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = %s", (AREA_ROUTES_STATEMENT_TIMEOUT_MS,))
        cur.execute(
            f"""
            WITH poly AS (
                SELECT ST_GeomFromText(%s, 4326) AS geom
            ),
            bounds AS (
                SELECT
                    to_date(%s::text, 'YYYYMMDD') AS start_date,
                    to_date(%s::text, 'YYYYMMDD') AS end_date,
                    %s::int AS start_sec,
                    %s::int AS end_sec
            ),
            day_windows AS (
                SELECT
                    to_char(gs::date, 'YYYYMMDD')::int AS date_ymd,
                    EXTRACT(DOW FROM gs::date)::int AS dow,
                    CASE WHEN gs::date = b.start_date THEN b.start_sec ELSE 0 END AS day_start_sec,
                    CASE WHEN gs::date = b.end_date THEN b.end_sec ELSE 27 * 3600 END AS day_end_sec
                FROM bounds b
                JOIN LATERAL generate_series(b.start_date, b.end_date, interval '1 day') gs ON TRUE
            ),
            calendar_services AS (
                SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, c.service_id
                FROM day_windows dw
                JOIN calendar c ON c.feed_id = %s
                  AND dw.date_ymd BETWEEN c.start_date AND c.end_date
                  AND (
                    (dw.dow = 0 AND c.sunday = 1)
                    OR (dw.dow = 1 AND c.monday = 1)
                    OR (dw.dow = 2 AND c.tuesday = 1)
                    OR (dw.dow = 3 AND c.wednesday = 1)
                    OR (dw.dow = 4 AND c.thursday = 1)
                    OR (dw.dow = 5 AND c.friday = 1)
                    OR (dw.dow = 6 AND c.saturday = 1)
                  )
            ),
            add_services AS (
                SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, cd.service_id
                FROM day_windows dw
                JOIN calendar_dates cd ON cd.feed_id = %s
                 AND cd.date = dw.date_ymd
                 AND cd.exception_type = 1
            ),
            remove_services AS (
                SELECT dw.date_ymd, cd.service_id
                FROM day_windows dw
                JOIN calendar_dates cd ON cd.feed_id = %s
                 AND cd.date = dw.date_ymd
                 AND cd.exception_type = 2
            ),
            base_services AS (
                SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM calendar_services
                UNION
                SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM add_services
            ),
            active_services AS (
                SELECT bs.date_ymd, bs.day_start_sec, bs.day_end_sec, bs.service_id
                FROM base_services bs
                LEFT JOIN remove_services r
                  ON r.date_ymd = bs.date_ymd AND r.service_id = bs.service_id
                WHERE r.service_id IS NULL
            ),
            shapes_in_area AS MATERIALIZED (
                SELECT sl.feed_id, sl.shape_id
                FROM shapes_lines sl
                JOIN poly p ON TRUE
                WHERE sl.feed_id = %s
                  AND ST_Intersects(sl.geom, p.geom)
            ),
            candidate_trips AS MATERIALIZED (
                SELECT DISTINCT
                    t.feed_id,
                    t.trip_id,
                    t.route_id,
                    t.direction_id,
                    s.day_start_sec,
                    s.day_end_sec
                FROM shapes_in_area sia
                JOIN trips t
                  ON t.feed_id = sia.feed_id
                 AND t.shape_id = sia.shape_id
                 AND t.shape_id IS NOT NULL
                JOIN active_services s ON s.service_id = t.service_id
                JOIN trip_time_bounds b
                  ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                WHERE t.feed_id = %s
                  AND b.last_sec >= s.day_start_sec
                  AND b.first_sec <= s.day_end_sec
            ),
            trip_stops AS MATERIALIZED (
                SELECT
                    ct.date_ymd,
                    ct.day_start_sec,
                    ct.day_end_sec,
                    ct.trip_id,
                    ct.route_id,
                    ct.direction_id,
                    st.stop_sequence,
                    (
                      split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 1)::int * 3600
                      + split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 2)::int * 60
                      + coalesce(nullif(trim(split_part(trim(coalesce(st.departure_time, st.arrival_time, '')), ':', 3)), ''), '0')::int
                    ) AS sec,
                    st.shape_dist_traveled AS shape_dist,
                    ST_Distance(
                        ST_SetSRID(ST_MakePoint(sp.lon, sp.lat), 4326),
                        p.geom
                    ) AS dist_m
                FROM candidate_trips ct
                JOIN stop_times st
                  ON st.feed_id = ct.feed_id
                 AND st.trip_id = ct.trip_id
                JOIN stops sp
                  ON sp.feed_id = st.feed_id
                 AND sp.stop_id = st.stop_id
                JOIN poly p ON TRUE
                WHERE st.feed_id = %s
                  AND length(trim(coalesce(st.departure_time, st.arrival_time, ''))) >= 5
            ),
            pivot AS MATERIALIZED (
                SELECT DISTINCT ON (ts.date_ymd, ts.trip_id)
                    ts.date_ymd,
                    ts.day_start_sec,
                    ts.day_end_sec,
                    ts.trip_id,
                    ts.route_id,
                    ts.direction_id,
                    ts.stop_sequence AS pivot_seq
                FROM trip_stops ts
                ORDER BY ts.date_ymd, ts.trip_id, ts.dist_m ASC, ts.stop_sequence ASC
            ),
            anchor_seq AS MATERIALIZED (
                SELECT
                    p.date_ymd,
                    p.day_start_sec,
                    p.day_end_sec,
                    p.trip_id,
                    p.route_id,
                    p.direction_id,
                    MAX(CASE WHEN ts.stop_sequence <= p.pivot_seq THEN ts.stop_sequence END) AS a_seq,
                    MIN(CASE WHEN ts.stop_sequence >= p.pivot_seq THEN ts.stop_sequence END) AS b_seq
                FROM pivot p
                JOIN trip_stops ts
                  ON ts.date_ymd = p.date_ymd
                 AND ts.trip_id = p.trip_id
                GROUP BY p.date_ymd, p.day_start_sec, p.day_end_sec, p.trip_id, p.route_id, p.direction_id
            ),
            anchors AS MATERIALIZED (
                SELECT
                    s.date_ymd,
                    s.day_start_sec,
                    s.day_end_sec,
                    s.trip_id,
                    s.route_id,
                    s.direction_id,
                    s.a_seq,
                    a.sec AS a_sec,
                    a.dist_m AS a_dist,
                    a.shape_dist AS a_shape,
                    s.b_seq,
                    b.sec AS b_sec,
                    b.dist_m AS b_dist,
                    b.shape_dist AS b_shape
                FROM anchor_seq s
                LEFT JOIN trip_stops a
                  ON a.date_ymd = s.date_ymd
                 AND a.trip_id = s.trip_id
                 AND a.stop_sequence = s.a_seq
                LEFT JOIN trip_stops b
                  ON b.date_ymd = s.date_ymd
                 AND b.trip_id = s.trip_id
                 AND b.stop_sequence = s.b_seq
            ),
            trip_pass AS MATERIALIZED (
                SELECT
                    an.date_ymd,
                    an.day_start_sec,
                    an.day_end_sec,
                    an.trip_id,
                    an.route_id,
                    an.direction_id,
                    (
                        CASE
                            WHEN an.a_seq IS NULL AND an.b_seq IS NULL THEN NULL
                            WHEN an.a_seq IS NULL THEN an.b_sec
                            WHEN an.b_seq IS NULL THEN an.a_sec
                            WHEN an.b_seq = an.a_seq THEN an.a_sec
                            WHEN an.a_sec IS NULL OR an.b_sec IS NULL THEN NULL
                            WHEN an.b_sec <= an.a_sec THEN NULL
                            WHEN coalesce(an.a_dist, 0) + coalesce(an.b_dist, 0) > 0
                                THEN round(
                                    an.a_sec
                                    + (an.b_sec - an.a_sec)
                                      * (coalesce(an.a_dist, 0) / (coalesce(an.a_dist, 0) + coalesce(an.b_dist, 0)))
                                )::int
                            ELSE ((an.a_sec + an.b_sec) / 2)::int
                        END
                    ) AS pass_sec,
                    (an.a_seq IS NOT NULL AND an.b_seq IS NOT NULL AND an.b_seq > an.a_seq) AS has_anchor_pair,
                    (an.a_shape IS NOT NULL AND an.b_shape IS NOT NULL) AS has_shape_pair
                FROM anchors an
            ),
            window_hits AS MATERIALIZED (
                SELECT
                    tp.trip_id,
                    tp.route_id,
                    tp.direction_id,
                    tp.pass_sec,
                    tp.has_anchor_pair,
                    tp.has_shape_pair
                FROM trip_pass tp
                WHERE tp.pass_sec IS NOT NULL
                  AND tp.pass_sec BETWEEN tp.day_start_sec AND tp.day_end_sec
            ),
            routes_geom AS (
                SELECT
                    wh.route_id,
                    wh.direction_id,
                    r.short_name AS route_short_name,
                    r.long_name AS route_long_name,
                    r.agency_id,
                    MIN(wh.pass_sec) AS first_time_s,
                    MAX(wh.pass_sec) AS last_time_s,
                    COUNT(DISTINCT wh.trip_id)::int AS trip_count,
                    {confidence_expr} AS time_match_confidence,
                    {note_expr} AS time_match_note
                FROM window_hits wh
                JOIN routes r
                  ON r.feed_id = %s AND r.route_id = wh.route_id
                GROUP BY wh.route_id, wh.direction_id, r.short_name, r.long_name, r.agency_id
            )
            SELECT
                rg.route_id,
                rg.direction_id,
                rg.route_short_name,
                rg.route_long_name,
                rg.agency_id,
                a.name AS agency_name,
                rg.first_time_s,
                rg.last_time_s,
                rg.trip_count,
                NULL::text AS last_stop_name,
                rg.time_match_confidence,
                rg.time_match_note
            FROM routes_geom rg
            LEFT JOIN agencies a
              ON a.feed_id = %s AND a.agency_id = rg.agency_id
            ORDER BY rg.route_short_name, rg.route_id, rg.direction_id
            """,
            (
                polygon_wkt,
                start_date_ymd,
                end_date_ymd,
                start_sec,
                end_sec,
                feed_id,
                feed_id,
                feed_id,
                feed_id,
                feed_id,
                feed_id,
                feed_id,
            ),
        )
        return [
            RouteInAreaRow(
                route_id=r["route_id"],
                direction_id=r["direction_id"],
                route_short_name=r["route_short_name"],
                route_long_name=r["route_long_name"],
                agency_id=r["agency_id"],
                agency_name=r["agency_name"],
                first_time_s=r["first_time_s"],
                last_time_s=r["last_time_s"],
                trip_count=r["trip_count"],
                last_stop_name=r.get("last_stop_name"),
                time_match_confidence=r.get("time_match_confidence"),
                time_match_note=r.get("time_match_note"),
            )
            for r in cur.fetchall()
        ]


def get_routes_in_polygon_range(
    polygon_wkt: str,
    start_date_ymd: str,
    start_sec: int,
    end_date_ymd: str,
    end_sec: int,
    time_semantics_mode: str = "legacy_trip_overlap",
    conn=None,
) -> List[RouteInAreaRow]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        # UI and most clients send the same start/end calendar day. The multi-day query below
        # builds per-day rows (generate_series + calendar joins) and can be orders of magnitude
        # slower on full national feeds, effectively hanging /area/routes. Single-day path matches
        # the legacy get_routes_in_polygon implementation.
        sd = (start_date_ymd or "").strip()
        ed = (end_date_ymd or "").strip()
        if sd and sd == ed:
            log(
                "area/routes",
                f"phase=db_branch branch=single_day_sql date={sd} wkt_chars={len(polygon_wkt)} "
                f"statement_timeout_ms={AREA_ROUTES_STATEMENT_TIMEOUT_MS}",
            )
            return get_routes_in_polygon(
                polygon_wkt,
                sd,
                start_sec,
                end_sec,
                time_semantics_mode=time_semantics_mode,
                conn=conn,
            )

        feed_id = get_active_feed_id(conn)
        log(
            "area/routes",
            f"phase=pg_multi_day_begin feed_id={feed_id} start_date={sd} end_date={ed} "
            f"time_window_sec={start_sec}-{end_sec} wkt_chars={len(polygon_wkt)} "
            f"statement_timeout_ms={AREA_ROUTES_STATEMENT_TIMEOUT_MS} "
            f"time_semantics_mode={time_semantics_mode}",
        )
        if time_semantics_mode != "legacy_trip_overlap":
            return _get_routes_in_polygon_pass_through_multi_day(
                conn=conn,
                feed_id=feed_id,
                polygon_wkt=polygon_wkt,
                start_date_ymd=start_date_ymd,
                start_sec=start_sec,
                end_date_ymd=end_date_ymd,
                end_sec=end_sec,
                time_semantics_mode=time_semantics_mode,
            )
        with conn.cursor() as cur:
            cur.execute(
                "SET LOCAL statement_timeout = %s", (AREA_ROUTES_STATEMENT_TIMEOUT_MS,)
            )
            t_exec = time.perf_counter()
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        to_date(%s::text, 'YYYYMMDD') AS start_date,
                        to_date(%s::text, 'YYYYMMDD') AS end_date,
                        %s::int AS start_sec,
                        %s::int AS end_sec
                ),
                day_windows AS (
                    SELECT
                        to_char(gs::date, 'YYYYMMDD')::int AS date_ymd,
                        EXTRACT(DOW FROM gs::date)::int AS dow,
                        CASE WHEN gs::date = b.start_date THEN b.start_sec ELSE 0 END AS day_start_sec,
                        CASE WHEN gs::date = b.end_date THEN b.end_sec ELSE 27 * 3600 END AS day_end_sec
                    FROM bounds b
                    JOIN LATERAL generate_series(b.start_date, b.end_date, interval '1 day') gs ON TRUE
                ),
                calendar_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, c.service_id
                    FROM day_windows dw
                    JOIN calendar c ON c.feed_id = %s
                     AND dw.date_ymd BETWEEN c.start_date AND c.end_date
                     AND (
                        (dw.dow = 0 AND c.sunday = 1)
                        OR (dw.dow = 1 AND c.monday = 1)
                        OR (dw.dow = 2 AND c.tuesday = 1)
                        OR (dw.dow = 3 AND c.wednesday = 1)
                        OR (dw.dow = 4 AND c.thursday = 1)
                        OR (dw.dow = 5 AND c.friday = 1)
                        OR (dw.dow = 6 AND c.saturday = 1)
                     )
                ),
                add_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 1
                ),
                remove_services AS (
                    SELECT dw.date_ymd, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 2
                ),
                base_services AS (
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM calendar_services
                    UNION
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM add_services
                ),
                active_services AS (
                    SELECT bs.date_ymd, bs.day_start_sec, bs.day_end_sec, bs.service_id
                    FROM base_services bs
                    LEFT JOIN remove_services r
                      ON r.date_ymd = bs.date_ymd AND r.service_id = bs.service_id
                    WHERE r.service_id IS NULL
                ),
                shapes_in_area AS MATERIALIZED (
                    SELECT sl.feed_id, sl.shape_id
                    FROM shapes_lines sl
                    WHERE sl.feed_id = %s
                      AND ST_Intersects(sl.geom, ST_GeomFromText(%s, 4326))
                ),
                shape_hit_trips AS MATERIALIZED (
                    SELECT DISTINCT t.feed_id, t.trip_id, t.route_id, t.direction_id, t.service_id
                    FROM shapes_in_area sia
                    JOIN trips t
                      ON t.feed_id = sia.feed_id
                      AND t.shape_id = sia.shape_id
                      AND t.shape_id IS NOT NULL
                    WHERE t.feed_id = %s
                ),
                trips_in_window AS MATERIALIZED (
                    SELECT sht.feed_id, sht.trip_id, sht.route_id, sht.direction_id
                    FROM shape_hit_trips sht
                    JOIN active_services s ON s.service_id = sht.service_id
                    JOIN trip_time_bounds b ON b.feed_id = sht.feed_id AND b.trip_id = sht.trip_id
                    WHERE sht.feed_id = %s
                      AND b.last_sec >= s.day_start_sec
                      AND b.first_sec <= s.day_end_sec
                ),
                routes_geom AS (
                    SELECT DISTINCT
                        tiw.route_id,
                        tiw.direction_id,
                        r.short_name AS route_short_name,
                        r.long_name AS route_long_name,
                        r.agency_id,
                        MIN(b.first_sec) AS first_time_s,
                        MAX(b.last_sec) AS last_time_s,
                        COUNT(DISTINCT tiw.trip_id)::int AS trip_count
                    FROM trips_in_window tiw
                    JOIN trip_time_bounds b
                      ON b.feed_id = tiw.feed_id AND b.trip_id = tiw.trip_id
                    JOIN routes r
                      ON r.feed_id = tiw.feed_id AND r.route_id = tiw.route_id
                    GROUP BY tiw.route_id, tiw.direction_id, r.short_name, r.long_name, r.agency_id
                )
                SELECT
                    rg.route_id,
                    rg.direction_id,
                    rg.route_short_name,
                    rg.route_long_name,
                    rg.agency_id,
                    a.name AS agency_name,
                    rg.first_time_s,
                    rg.last_time_s,
                    rg.trip_count,
                    NULL::text AS last_stop_name
                FROM routes_geom rg
                LEFT JOIN agencies a
                  ON a.feed_id = %s AND a.agency_id = rg.agency_id
                ORDER BY rg.route_short_name, rg.route_id, rg.direction_id
                """,
                (
                    start_date_ymd,
                    end_date_ymd,
                    start_sec,
                    end_sec,
                    feed_id,
                    feed_id,
                    feed_id,
                    feed_id,
                    polygon_wkt,
                    feed_id,
                    feed_id,
                    feed_id,
                ),
            )
            exec_ms = int((time.perf_counter() - t_exec) * 1000)
            log("area/routes", f"phase=pg_multi_day_execute_done elapsed_ms={exec_ms}")
            t_fetch = time.perf_counter()
            raw_rows = cur.fetchall()
            fetch_ms = int((time.perf_counter() - t_fetch) * 1000)
            log(
                "area/routes",
                f"phase=pg_multi_day_fetch_done rows={len(raw_rows)} elapsed_ms={fetch_ms}",
            )
            return [
                RouteInAreaRow(
                    route_id=r["route_id"],
                    direction_id=r["direction_id"],
                    route_short_name=r["route_short_name"],
                    route_long_name=r["route_long_name"],
                    agency_id=r["agency_id"],
                    agency_name=r["agency_name"],
                    first_time_s=r["first_time_s"],
                    last_time_s=r["last_time_s"],
                    trip_count=r["trip_count"],
                    last_stop_name=r.get("last_stop_name"),
                )
                for r in raw_rows
            ]
    finally:
        if close:
            conn.close()


def get_routes_vector_tile_mvt(
    z: int,
    x: int,
    y: int,
    scope: str = "all",
    render_mode: str = "balanced",
    start_date_ymd: Optional[str] = None,
    start_sec: Optional[int] = None,
    end_date_ymd: Optional[str] = None,
    end_sec: Optional[int] = None,
    conn=None,
) -> bytes:
    """
    Return Mapbox Vector Tile bytes for route geometries clipped to one tile.

    scope='all' returns all route-direction shapes in the active feed.
    scope='time_window' filters to route-direction pairs active in the supplied window.
    """
    if scope not in ("all", "time_window"):
        raise ValueError("scope must be 'all' or 'time_window'")
    if render_mode not in ("always_visible", "balanced"):
        raise ValueError("render_mode must be 'always_visible' or 'balanced'")
    if scope == "time_window":
        if (
            start_date_ymd is None
            or start_sec is None
            or end_date_ymd is None
            or end_sec is None
        ):
            raise ValueError(
                "time_window scope requires start_date_ymd/start_sec/end_date_ymd/end_sec"
            )

    def _simplify_tolerance_for_zoom(zoom: int, mode: str) -> float:
        # Smoothly decay simplification by zoom to avoid abrupt geometry jumps
        # between adjacent zoom bands.
        if mode == "always_visible":
            if zoom >= 8:
                return 0.0
            z = max(0, min(8, int(zoom)))
            base_tol_deg = 0.0022
            decay = 0.68 ** max(0, z - 2)
            tol = base_tol_deg * decay
            return max(0.00008, tol)
        if zoom >= 13:
            return 0.0
        z = max(0, min(13, int(zoom)))
        base_tol_deg = 0.02
        decay = 0.58 ** max(0, z - 2)
        tol = base_tol_deg * decay
        return max(0.00025, tol)

    simplify_tolerance = _simplify_tolerance_for_zoom(z, render_mode)

    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (ROUTES_TILE_STATEMENT_TIMEOUT_MS,))
            if scope == "all":
                cur.execute(
                    """
                    WITH tile AS (
                        SELECT
                            ST_TileEnvelope(%s, %s, %s) AS geom_3857,
                            ST_Transform(ST_TileEnvelope(%s, %s, %s), 4326) AS geom_4326
                    ),
                    route_shapes AS (
                        SELECT DISTINCT
                            t.route_id,
                            t.direction_id,
                            r.short_name AS route_short_name,
                            r.agency_id,
                            t.shape_id,
                            sl.geom
                        FROM trips t
                        JOIN routes r
                          ON r.feed_id = t.feed_id
                         AND r.route_id = t.route_id
                        JOIN shapes_lines sl
                          ON sl.feed_id = t.feed_id
                         AND sl.shape_id = t.shape_id
                        JOIN tile tb ON sl.geom && tb.geom_4326
                        WHERE t.feed_id = %s
                          AND t.shape_id IS NOT NULL
                          AND ST_Intersects(sl.geom, tb.geom_4326)
                    ),
                    counts AS (
                        SELECT
                            route_id,
                            direction_id,
                            COUNT(*)::int AS shape_count
                        FROM route_shapes
                        GROUP BY route_id, direction_id
                    ),
                    stats AS (
                        SELECT
                            COALESCE(MIN(shape_count), 0)::double precision AS min_shape_count,
                            COALESCE(MAX(shape_count), 0)::double precision AS max_shape_count
                        FROM counts
                    ),
                    prepared AS (
                        SELECT
                            rs.route_id,
                            rs.direction_id,
                            rs.route_short_name,
                            rs.agency_id,
                            c.shape_count,
                            CASE
                                WHEN %s::double precision > 0
                                    THEN ST_SimplifyPreserveTopology(rs.geom, %s::double precision)
                                ELSE rs.geom
                            END AS geom
                        FROM route_shapes rs
                        JOIN counts c
                          ON c.route_id = rs.route_id
                         AND COALESCE(c.direction_id, -1) = COALESCE(rs.direction_id, -1)
                    ),
                    mvtgeom AS (
                        SELECT
                            p.route_id,
                            p.direction_id::text AS direction_id,
                            route_short_name,
                            agency_id,
                            shape_count,
                            CASE
                                WHEN st.max_shape_count <= st.min_shape_count THEN 0.35
                                ELSE LEAST(
                                    1.0,
                                    GREATEST(
                                        0.0,
                                        (
                                            LN(shape_count::double precision + 1.0) - LN(st.min_shape_count + 1.0)
                                        ) / NULLIF(
                                            LN(st.max_shape_count + 1.0) - LN(st.min_shape_count + 1.0),
                                            0.0
                                        )
                                    )
                                )
                            END AS intensity,
                            ST_AsMVTGeom(
                                ST_Transform(p.geom, 3857),
                                tile.geom_3857,
                                4096,
                                64,
                                true
                            ) AS geom
                        FROM prepared p
                        CROSS JOIN stats st
                        CROSS JOIN tile
                        WHERE p.geom IS NOT NULL
                    )
                    SELECT ST_AsMVT(mvtgeom, 'routes', 4096, 'geom') AS tile
                    FROM mvtgeom
                    """,
                    (z, x, y, z, x, y, feed_id, simplify_tolerance, simplify_tolerance),
                )
            else:
                cur.execute(
                    """
                    WITH tile AS (
                        SELECT
                            ST_TileEnvelope(%s, %s, %s) AS geom_3857,
                            ST_Transform(ST_TileEnvelope(%s, %s, %s), 4326) AS geom_4326
                    ),
                    bounds AS (
                        SELECT
                            to_date(%s::text, 'YYYYMMDD') AS start_date,
                            to_date(%s::text, 'YYYYMMDD') AS end_date,
                            %s::int AS start_sec,
                            %s::int AS end_sec
                    ),
                    day_windows AS (
                        SELECT
                            to_char(gs::date, 'YYYYMMDD')::int AS date_ymd,
                            EXTRACT(DOW FROM gs::date)::int AS dow,
                            CASE WHEN gs::date = b.start_date THEN b.start_sec ELSE 0 END AS day_start_sec,
                            CASE WHEN gs::date = b.end_date THEN b.end_sec ELSE 27 * 3600 END AS day_end_sec
                        FROM bounds b
                        JOIN LATERAL generate_series(b.start_date, b.end_date, interval '1 day') gs ON TRUE
                    ),
                    calendar_services AS (
                        SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, c.service_id
                        FROM day_windows dw
                        JOIN calendar c ON c.feed_id = %s
                         AND dw.date_ymd BETWEEN c.start_date AND c.end_date
                         AND (
                            (dw.dow = 0 AND c.sunday = 1)
                            OR (dw.dow = 1 AND c.monday = 1)
                            OR (dw.dow = 2 AND c.tuesday = 1)
                            OR (dw.dow = 3 AND c.wednesday = 1)
                            OR (dw.dow = 4 AND c.thursday = 1)
                            OR (dw.dow = 5 AND c.friday = 1)
                            OR (dw.dow = 6 AND c.saturday = 1)
                         )
                    ),
                    add_services AS (
                        SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, cd.service_id
                        FROM day_windows dw
                        JOIN calendar_dates cd ON cd.feed_id = %s
                         AND cd.date = dw.date_ymd
                         AND cd.exception_type = 1
                    ),
                    remove_services AS (
                        SELECT dw.date_ymd, cd.service_id
                        FROM day_windows dw
                        JOIN calendar_dates cd ON cd.feed_id = %s
                         AND cd.date = dw.date_ymd
                         AND cd.exception_type = 2
                    ),
                    base_services AS (
                        SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM calendar_services
                        UNION
                        SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM add_services
                    ),
                    active_services AS (
                        SELECT bs.date_ymd, bs.day_start_sec, bs.day_end_sec, bs.service_id
                        FROM base_services bs
                        LEFT JOIN remove_services r
                          ON r.date_ymd = bs.date_ymd
                         AND r.service_id = bs.service_id
                        WHERE r.service_id IS NULL
                    ),
                    active_trips AS (
                        SELECT DISTINCT
                            t.trip_id,
                            t.route_id,
                            t.direction_id,
                            t.shape_id
                        FROM trips t
                        JOIN active_services s
                          ON s.service_id = t.service_id
                        JOIN trip_time_bounds b
                          ON b.feed_id = t.feed_id
                         AND b.trip_id = t.trip_id
                        WHERE t.feed_id = %s
                          AND t.shape_id IS NOT NULL
                          AND b.last_sec >= s.day_start_sec
                          AND b.first_sec <= s.day_end_sec
                    ),
                    route_shapes AS (
                        SELECT DISTINCT
                            at.route_id,
                            at.direction_id,
                            r.short_name AS route_short_name,
                            r.agency_id,
                            at.shape_id,
                            sl.geom
                        FROM active_trips at
                        JOIN routes r
                          ON r.feed_id = %s
                         AND r.route_id = at.route_id
                        JOIN shapes_lines sl
                          ON sl.feed_id = %s
                         AND sl.shape_id = at.shape_id
                        JOIN tile tb ON sl.geom && tb.geom_4326
                        WHERE ST_Intersects(sl.geom, tb.geom_4326)
                    ),
                    counts AS (
                        SELECT
                            route_id,
                            direction_id,
                            COUNT(*)::int AS shape_count
                        FROM route_shapes
                        GROUP BY route_id, direction_id
                    ),
                    stats AS (
                        SELECT
                            COALESCE(MIN(shape_count), 0)::double precision AS min_shape_count,
                            COALESCE(MAX(shape_count), 0)::double precision AS max_shape_count
                        FROM counts
                    ),
                    prepared AS (
                        SELECT
                            rs.route_id,
                            rs.direction_id,
                            rs.route_short_name,
                            rs.agency_id,
                            c.shape_count,
                            CASE
                                WHEN %s::double precision > 0
                                    THEN ST_SimplifyPreserveTopology(rs.geom, %s::double precision)
                                ELSE rs.geom
                            END AS geom
                        FROM route_shapes rs
                        JOIN counts c
                          ON c.route_id = rs.route_id
                         AND COALESCE(c.direction_id, -1) = COALESCE(rs.direction_id, -1)
                    ),
                    mvtgeom AS (
                        SELECT
                            p.route_id,
                            p.direction_id::text AS direction_id,
                            route_short_name,
                            agency_id,
                            shape_count,
                            CASE
                                WHEN st.max_shape_count <= st.min_shape_count THEN 0.35
                                ELSE LEAST(
                                    1.0,
                                    GREATEST(
                                        0.0,
                                        (
                                            LN(shape_count::double precision + 1.0) - LN(st.min_shape_count + 1.0)
                                        ) / NULLIF(
                                            LN(st.max_shape_count + 1.0) - LN(st.min_shape_count + 1.0),
                                            0.0
                                        )
                                    )
                                )
                            END AS intensity,
                            ST_AsMVTGeom(
                                ST_Transform(p.geom, 3857),
                                tile.geom_3857,
                                4096,
                                64,
                                true
                            ) AS geom
                        FROM prepared p
                        CROSS JOIN stats st
                        CROSS JOIN tile
                        WHERE p.geom IS NOT NULL
                    )
                    SELECT ST_AsMVT(mvtgeom, 'routes', 4096, 'geom') AS tile
                    FROM mvtgeom
                    """,
                    (
                        z,
                        x,
                        y,
                        z,
                        x,
                        y,
                        start_date_ymd,
                        end_date_ymd,
                        start_sec,
                        end_sec,
                        feed_id,
                        feed_id,
                        feed_id,
                        feed_id,
                        feed_id,
                        feed_id,
                        simplify_tolerance,
                        simplify_tolerance,
                    ),
                )
            row = cur.fetchone()
            tile = row["tile"] if row else None
            return bytes(tile) if tile else b""
    finally:
        if close:
            conn.close()


def get_routes_coverage_tile_mvt(
    z: int,
    x: int,
    y: int,
    conn=None,
) -> bytes:
    """
    Return a low-zoom generalized route coverage tile.

    This layer is intentionally coarse and feed-wide so users keep route
    presence context while zoomed out, then transition to detailed tiles.
    """
    def _coverage_tolerance_for_zoom(zoom: int) -> float:
        if zoom >= 10:
            return 0.0
        zc = max(0, min(10, int(zoom)))
        base_tol_deg = 0.03
        decay = 0.62 ** max(0, zc - 2)
        tol = base_tol_deg * decay
        return max(0.0004, tol)

    simplify_tolerance = _coverage_tolerance_for_zoom(z)
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (ROUTES_TILE_STATEMENT_TIMEOUT_MS,))
            cur.execute(
                """
                WITH tile AS (
                    SELECT
                        ST_TileEnvelope(%s, %s, %s) AS geom_3857,
                        ST_Transform(ST_TileEnvelope(%s, %s, %s), 4326) AS geom_4326
                ),
                collected AS (
                    SELECT ST_UnaryUnion(ST_Collect(sl.geom)) AS geom
                    FROM shapes_lines sl
                    JOIN tile t ON sl.geom && ST_Expand(t.geom_4326, 0.05)
                    WHERE sl.feed_id = %s
                      AND ST_Intersects(sl.geom, t.geom_4326)
                ),
                simplified AS (
                    SELECT
                        CASE
                            WHEN %s::double precision > 0
                                THEN ST_SimplifyPreserveTopology(geom, %s::double precision)
                            ELSE geom
                        END AS geom
                    FROM collected
                    WHERE geom IS NOT NULL
                ),
                mvtgeom AS (
                    SELECT
                        ST_AsMVTGeom(
                            ST_Transform(geom, 3857),
                            tile.geom_3857,
                            4096,
                            256,
                            true
                        ) AS geom,
                        0.22::double precision AS intensity
                    FROM simplified
                    CROSS JOIN tile
                    WHERE geom IS NOT NULL
                )
                SELECT ST_AsMVT(mvtgeom, 'coverage', 4096, 'geom') AS tile
                FROM mvtgeom
                """,
                (z, x, y, z, x, y, feed_id, simplify_tolerance, simplify_tolerance),
            )
            row = cur.fetchone()
            tile = row["tile"] if row else None
            return bytes(tile) if tile else b""
    finally:
        if close:
            conn.close()


def get_routes_serving_stop_pg_range(
    stop_id: str,
    start_date_ymd: str,
    start_sec: int,
    end_date_ymd: str,
    end_sec: int,
    max_results: int = 100,
) -> List[Dict[str, Any]]:
    close = False
    conn = _get_conn()
    close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (STOP_ROUTES_STATEMENT_TIMEOUT_MS,))
            cur.execute(
                """
                WITH bounds AS (
                    SELECT
                        to_date(%s::text, 'YYYYMMDD') AS start_date,
                        to_date(%s::text, 'YYYYMMDD') AS end_date,
                        %s::int AS start_sec,
                        %s::int AS end_sec
                ),
                day_windows AS (
                    SELECT
                        to_char(gs::date, 'YYYYMMDD')::int AS date_ymd,
                        EXTRACT(DOW FROM gs::date)::int AS dow,
                        CASE WHEN gs::date = b.start_date THEN b.start_sec ELSE 0 END AS day_start_sec,
                        CASE WHEN gs::date = b.end_date THEN b.end_sec ELSE 27 * 3600 END AS day_end_sec
                    FROM bounds b
                    JOIN LATERAL generate_series(b.start_date, b.end_date, interval '1 day') gs ON TRUE
                ),
                calendar_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, c.service_id
                    FROM day_windows dw
                    JOIN calendar c ON c.feed_id = %s
                     AND dw.date_ymd BETWEEN c.start_date AND c.end_date
                     AND (
                        (dw.dow = 0 AND c.sunday = 1)
                        OR (dw.dow = 1 AND c.monday = 1)
                        OR (dw.dow = 2 AND c.tuesday = 1)
                        OR (dw.dow = 3 AND c.wednesday = 1)
                        OR (dw.dow = 4 AND c.thursday = 1)
                        OR (dw.dow = 5 AND c.friday = 1)
                        OR (dw.dow = 6 AND c.saturday = 1)
                     )
                ),
                add_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 1
                ),
                remove_services AS (
                    SELECT dw.date_ymd, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 2
                ),
                base_services AS (
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM calendar_services
                    UNION
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM add_services
                ),
                active_services AS (
                    SELECT bs.date_ymd, bs.day_start_sec, bs.day_end_sec, bs.service_id
                    FROM base_services bs
                    LEFT JOIN remove_services r
                      ON r.date_ymd = bs.date_ymd AND r.service_id = bs.service_id
                    WHERE r.service_id IS NULL
                ),
                stop_filtered AS (
                    SELECT
                        st.trip_id,
                        trim(coalesce(st.departure_time, st.arrival_time, '')) AS dep_time_raw
                    FROM stop_times st
                    WHERE st.feed_id = %s
                      AND st.stop_id = %s
                      AND length(trim(coalesce(st.departure_time, st.arrival_time, ''))) >= 5
                ),
                stop_sec AS (
                    SELECT
                        sf.trip_id,
                        t.route_id,
                        t.direction_id,
                        (split_part(sf.dep_time_raw, ':', 1)::int * 3600
                         + split_part(sf.dep_time_raw, ':', 2)::int * 60
                         + coalesce(nullif(trim(split_part(sf.dep_time_raw, ':', 3)), ''), '0')::int
                        ) AS sec
                    FROM stop_filtered sf
                    JOIN trips t ON t.feed_id = %s AND t.trip_id = sf.trip_id
                    JOIN active_services a ON a.service_id = t.service_id
                    WHERE (split_part(sf.dep_time_raw, ':', 1)::int * 3600
                         + split_part(sf.dep_time_raw, ':', 2)::int * 60
                         + coalesce(nullif(trim(split_part(sf.dep_time_raw, ':', 3)), ''), '0')::int
                        ) BETWEEN a.day_start_sec AND a.day_end_sec
                ),
                in_window AS (
                    SELECT route_id, direction_id, min(sec) AS first_sec, max(sec) AS last_sec
                    FROM stop_sec
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
                    start_date_ymd,
                    end_date_ymd,
                    start_sec,
                    end_sec,
                    feed_id,
                    feed_id,
                    feed_id,
                    feed_id,
                    stop_id,
                    feed_id,
                    feed_id,
                    max_results,
                ),
            )
            rows = cur.fetchall()
        return [
            {
                "route_id": r["route_id"],
                "direction_id": None if r["direction_id"] is None else str(r["direction_id"]),
                "route_short_name": r["short_name"],
                "route_long_name": r["long_name"],
                "agency_id": r["agency_id"],
                "agency_name": r["agency_name"],
                "first_time": f"{int(r['first_sec']) // 3600:02d}:{(int(r['first_sec']) % 3600) // 60:02d}",
                "last_time": f"{int(r['last_sec']) // 3600:02d}:{(int(r['last_sec']) % 3600) // 60:02d}",
            }
            for r in rows
        ]
    except psycopg2.errors.QueryCanceled as e:
        raise StopRoutesQueryTimeoutError(
            f"Stop routes query timed out after {STOP_ROUTES_STATEMENT_TIMEOUT_MS}ms"
        ) from e
    finally:
        if close:
            conn.close()


def search_routes_pg_range(
    q: str,
    limit: int,
    start_date_ymd: Optional[str] = None,
    start_sec: Optional[int] = None,
    end_date_ymd: Optional[str] = None,
    end_sec: Optional[int] = None,
) -> List[Dict[str, Any]]:
    q = (q or "").strip().lower()
    if not q:
        return []
    close = False
    conn = _get_conn()
    close = True
    try:
        feed_id = get_active_feed_id(conn)
        use_window = (
            bool(start_date_ymd)
            and bool(end_date_ymd)
            and start_sec is not None
            and end_sec is not None
            and len(str(start_date_ymd)) == 8
            and len(str(end_date_ymd)) == 8
            and str(start_date_ymd).isdigit()
            and str(end_date_ymd).isdigit()
        )
        window_sql = ""
        window_params: List[Any] = []
        if use_window:
            window_sql = """
                ,
                bounds AS (
                    SELECT
                        to_date(%s::text, 'YYYYMMDD') AS start_date,
                        to_date(%s::text, 'YYYYMMDD') AS end_date,
                        %s::int AS start_sec,
                        %s::int AS end_sec
                ),
                day_windows AS (
                    SELECT
                        to_char(gs::date, 'YYYYMMDD')::int AS date_ymd,
                        EXTRACT(DOW FROM gs::date)::int AS dow,
                        CASE WHEN gs::date = b.start_date THEN b.start_sec ELSE 0 END AS day_start_sec,
                        CASE WHEN gs::date = b.end_date THEN b.end_sec ELSE 27 * 3600 END AS day_end_sec
                    FROM bounds b
                    JOIN LATERAL generate_series(b.start_date, b.end_date, interval '1 day') gs ON TRUE
                ),
                calendar_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, c.service_id
                    FROM day_windows dw
                    JOIN calendar c ON c.feed_id = %s
                     AND dw.date_ymd BETWEEN c.start_date AND c.end_date
                     AND (
                        (dw.dow = 0 AND c.sunday = 1)
                        OR (dw.dow = 1 AND c.monday = 1)
                        OR (dw.dow = 2 AND c.tuesday = 1)
                        OR (dw.dow = 3 AND c.wednesday = 1)
                        OR (dw.dow = 4 AND c.thursday = 1)
                        OR (dw.dow = 5 AND c.friday = 1)
                        OR (dw.dow = 6 AND c.saturday = 1)
                     )
                ),
                add_services AS (
                    SELECT dw.date_ymd, dw.day_start_sec, dw.day_end_sec, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 1
                ),
                remove_services AS (
                    SELECT dw.date_ymd, cd.service_id
                    FROM day_windows dw
                    JOIN calendar_dates cd ON cd.feed_id = %s
                     AND cd.date = dw.date_ymd
                     AND cd.exception_type = 2
                ),
                base_services AS (
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM calendar_services
                    UNION
                    SELECT date_ymd, day_start_sec, day_end_sec, service_id FROM add_services
                ),
                active_services AS (
                    SELECT bs.date_ymd, bs.day_start_sec, bs.day_end_sec, bs.service_id
                    FROM base_services bs
                    LEFT JOIN remove_services r
                      ON r.date_ymd = bs.date_ymd AND r.service_id = bs.service_id
                    WHERE r.service_id IS NULL
                ),
                trips_in_window AS (
                    SELECT t.route_id, COUNT(DISTINCT t.trip_id)::int AS trip_count
                    FROM trips t
                    JOIN active_services s ON s.service_id = t.service_id
                    JOIN trip_time_bounds b ON b.feed_id = t.feed_id AND b.trip_id = t.trip_id
                    WHERE t.feed_id = %s
                      AND b.last_sec >= s.day_start_sec
                      AND b.first_sec <= s.day_end_sec
                    GROUP BY t.route_id
                )
            """
            window_params = [
                str(start_date_ymd),
                str(end_date_ymd),
                int(start_sec),
                int(end_sec),
                feed_id,
                feed_id,
                feed_id,
                feed_id,
            ]
        with conn.cursor() as cur:
            if q.isdigit():
                cur.execute(
                    f"""
                    WITH route_last_stop AS (
                      SELECT DISTINCT ON (t.route_id)
                        t.route_id,
                        s.name AS last_stop_name
                      FROM trips t
                      JOIN (
                        SELECT feed_id, trip_id, MAX(stop_sequence) AS max_seq
                        FROM stop_times
                        GROUP BY feed_id, trip_id
                      ) mx ON mx.feed_id = t.feed_id AND mx.trip_id = t.trip_id
                      JOIN stop_times st
                        ON st.feed_id = mx.feed_id AND st.trip_id = mx.trip_id AND st.stop_sequence = mx.max_seq
                      JOIN stops s ON s.feed_id = st.feed_id AND s.stop_id = st.stop_id
                      WHERE t.feed_id = %s
                      ORDER BY t.route_id, t.trip_id
                    ),
                    filtered_routes AS (
                      SELECT
                        r.route_id,
                        r.short_name,
                        r.long_name,
                        r.agency_id,
                        r.route_type
                      FROM routes r
                      WHERE r.feed_id = %s
                        AND (LOWER(r.route_id) = %s OR LOWER(r.short_name) = %s)
                      LIMIT %s
                    )
                    {window_sql}
                    SELECT
                      fr.route_id,
                      fr.short_name,
                      fr.long_name,
                      fr.agency_id,
                      fr.route_type,
                      a.name AS agency_name,
                      {"COALESCE(tw.trip_count, 0)::int AS trip_count" if use_window else "NULL::int AS trip_count"},
                      rls.last_stop_name
                    FROM filtered_routes fr
                    LEFT JOIN route_last_stop rls ON rls.route_id = fr.route_id
                    LEFT JOIN agencies a
                      ON a.feed_id = %s AND a.agency_id = fr.agency_id
                    {"LEFT JOIN trips_in_window tw ON tw.route_id = fr.route_id" if use_window else ""}
                    """,
                    [feed_id, feed_id, q, q, limit, *window_params, feed_id],
                )
            else:
                like = f"%{q}%"
                cur.execute(
                    f"""
                    WITH route_last_stop AS (
                      SELECT DISTINCT ON (t.route_id)
                        t.route_id,
                        s.name AS last_stop_name
                      FROM trips t
                      JOIN (
                        SELECT feed_id, trip_id, MAX(stop_sequence) AS max_seq
                        FROM stop_times
                        GROUP BY feed_id, trip_id
                      ) mx ON mx.feed_id = t.feed_id AND mx.trip_id = t.trip_id
                      JOIN stop_times st
                        ON st.feed_id = mx.feed_id AND st.trip_id = mx.trip_id AND st.stop_sequence = mx.max_seq
                      JOIN stops s ON s.feed_id = st.feed_id AND s.stop_id = st.stop_id
                      WHERE t.feed_id = %s
                      ORDER BY t.route_id, t.trip_id
                    ),
                    filtered_routes AS (
                      SELECT
                        r.route_id,
                        r.short_name,
                        r.long_name,
                        r.agency_id,
                        r.route_type
                      FROM routes r
                      WHERE r.feed_id = %s
                        AND (
                          LOWER(COALESCE(r.route_id, ''))    LIKE %s OR
                          LOWER(COALESCE(r.short_name, ''))  LIKE %s OR
                          LOWER(COALESCE(r.long_name, ''))   LIKE %s
                        )
                      LIMIT %s
                    )
                    {window_sql}
                    SELECT
                      fr.route_id,
                      fr.short_name,
                      fr.long_name,
                      fr.agency_id,
                      fr.route_type,
                      a.name AS agency_name,
                      {"COALESCE(tw.trip_count, 0)::int AS trip_count" if use_window else "NULL::int AS trip_count"},
                      rls.last_stop_name
                    FROM filtered_routes fr
                    LEFT JOIN route_last_stop rls ON rls.route_id = fr.route_id
                    LEFT JOIN agencies a
                      ON a.feed_id = %s AND a.agency_id = fr.agency_id
                    {"LEFT JOIN trips_in_window tw ON tw.route_id = fr.route_id" if use_window else ""}
                    """,
                    [feed_id, feed_id, like, like, like, limit, *window_params, feed_id],
                )
            return [
                {
                    "route_id": r["route_id"],
                    "route_short_name": r["short_name"],
                    "route_long_name": r["long_name"],
                    "agency_id": r["agency_id"],
                    "route_type": r["route_type"],
                    "agency_name": r["agency_name"],
                    "trip_count": r["trip_count"],
                    "last_stop_name": r.get("last_stop_name"),
                }
                for r in cur.fetchall()
            ]
    finally:
        if close:
            conn.close()



# --- Detour v2 -----------------------------------------------------------------


def get_trip_route_shape(trip_id: str, conn=None) -> Optional[Dict[str, Any]]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT route_id, shape_id
                FROM trips
                WHERE feed_id = %s AND trip_id = %s
                LIMIT 1
                """,
                (feed_id, trip_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"route_id": row["route_id"], "shape_id": row["shape_id"]}
    finally:
        if close:
            conn.close()


def get_stop_lonlat_bulk(stop_ids: List[str], conn=None) -> Dict[str, Tuple[float, float]]:
    if not stop_ids:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT stop_id, lon, lat
                FROM stops
                WHERE feed_id = %s AND stop_id = ANY(%s)
                """,
                (feed_id, stop_ids),
            )
            out: Dict[str, Tuple[float, float]] = {}
            for r in cur.fetchall():
                sid = str(r["stop_id"])
                if r.get("lon") is not None and r.get("lat") is not None:
                    out[sid] = (float(r["lon"]), float(r["lat"]))
            return out
    finally:
        if close:
            conn.close()


def osm_segments_intersecting_polygon(feed_id: int, polygon_geojson: Dict[str, Any], conn=None) -> List[Dict[str, Any]]:
    """Return osm_road_segments rows intersecting polygon."""
    del feed_id
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        gj = json.dumps(polygon_geojson)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT osm_way_id, direction
                FROM osm_road_segments
                WHERE ST_Intersects(
                    geom::geography,
                    ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)::geography
                )
                LIMIT 5000
                """,
                (gj,),
            )
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        if close:
            conn.close()


def insert_incident(
    polygon_geojson: Dict[str, Any],
    incident_type: Optional[str],
    description: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str],
    created_by: Optional[str],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        gj = json.dumps(polygon_geojson)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO incidents (polygon_geom, incident_type, description, start_time, end_time, created_by)
                VALUES (
                    ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                    %s, %s, %s::timestamptz, %s::timestamptz, %s
                )
                RETURNING id
                """,
                (gj, incident_type, description, start_time, end_time, created_by),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])
    finally:
        if close:
            conn.close()


def _sanitize_for_jsonb(obj: Any) -> Any:
    """Recursively replace non-finite floats with None before json.dumps.

    PostgreSQL jsonb rejects the bare tokens ``Infinity``, ``-Infinity`` and
    ``NaN`` that Python's json.dumps emits for float('inf') / float('nan').
    """
    import math as _math  # local import so it doesn't pollute module-level namespace

    if isinstance(obj, float):
        return obj if _math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_for_jsonb(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_jsonb(v) for v in obj]
    return obj


def insert_detour_request(
    *,
    feed_id: int,
    trip_id: str,
    route_id: str,
    service_date: str,
    incident_id: Optional[int],
    status: str,
    payload_json: Dict[str, Any],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO detour_requests (feed_id, trip_id, route_id, service_date, incident_id, status, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    feed_id,
                    trip_id,
                    route_id,
                    service_date,
                    incident_id,
                    status,
                    json.dumps(_sanitize_for_jsonb(payload_json), allow_nan=False),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])
    finally:
        if close:
            conn.close()


def insert_detour_candidate(
    *,
    detour_request_id: int,
    candidate_rank: int,
    strategy: Optional[str],
    geometry_json: Optional[Dict[str, Any]],
    road_sequence_json: Any,
    turn_sequence_json: Any,
    travel_time_s: float,
    distance_m: float,
    score: float,
    accepted: bool,
    rejection_reasons_json: Any,
    score_breakdown_json: Dict[str, Any],
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
                INSERT INTO detour_candidates (
                    detour_request_id, candidate_rank, strategy, geometry_json,
                    road_sequence_json, turn_sequence_json, travel_time_s, distance_m,
                    score, accepted, rejection_reasons_json, score_breakdown_json
                )
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    detour_request_id,
                    candidate_rank,
                    strategy,
                    json.dumps(_sanitize_for_jsonb(geometry_json), allow_nan=False) if geometry_json is not None else None,
                    json.dumps(_sanitize_for_jsonb(road_sequence_json), allow_nan=False),
                    json.dumps(_sanitize_for_jsonb(turn_sequence_json), allow_nan=False),
                    _sanitize_for_jsonb(travel_time_s),
                    _sanitize_for_jsonb(distance_m),
                    _sanitize_for_jsonb(score),
                    accepted,
                    json.dumps(_sanitize_for_jsonb(rejection_reasons_json), allow_nan=False),
                    json.dumps(_sanitize_for_jsonb(score_breakdown_json), allow_nan=False),
                ),
            )
            conn.commit()
    finally:
        if close:
            conn.close()


def get_detour_request_full(detour_request_id: int, conn=None) -> Optional[Dict[str, Any]]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, feed_id, trip_id, route_id, service_date, incident_id, status, payload_json, created_at
                FROM detour_requests WHERE id = %s
                """,
                (detour_request_id,),
            )
            req = cur.fetchone()
            if not req:
                return None
            cur.execute(
                """
                SELECT * FROM detour_candidates WHERE detour_request_id = %s ORDER BY candidate_rank
                """,
                (detour_request_id,),
            )
            cands = [dict(r) for r in cur.fetchall()]
            return {"request": dict(req), "candidates": cands}
    finally:
        if close:
            conn.close()


def insert_approved_detour(
    *,
    feed_id: int,
    route_id: str,
    trip_pattern_key: str,
    incident_signature: str,
    geometry_json: Dict[str, Any],
    road_sequence_json: Any,
    turn_sequence_json: Any,
    approved_by: Optional[str],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO approved_detours (
                    feed_id, route_id, trip_pattern_key, incident_signature,
                    geometry_json, road_sequence_json, turn_sequence_json, approved_by
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                RETURNING id
                """,
                (
                    feed_id,
                    route_id,
                    trip_pattern_key,
                    incident_signature,
                    json.dumps(geometry_json),
                    json.dumps(road_sequence_json),
                    json.dumps(turn_sequence_json),
                    approved_by,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])
    finally:
        if close:
            conn.close()


def osm_segment_nodes_intersecting_polygon(polygon_geojson: Dict[str, Any], conn=None) -> List[int]:
    """Collect from_node_id / to_node_id from osm_road_segments intersecting the polygon (for turn bans)."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        gj = json.dumps(polygon_geojson)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT n FROM (
                    SELECT from_node_id AS n
                    FROM osm_road_segments
                    WHERE ST_Intersects(
                        geom::geometry,
                        ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)::geometry
                    )
                    UNION
                    SELECT to_node_id AS n
                    FROM osm_road_segments
                    WHERE ST_Intersects(
                        geom::geometry,
                        ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)::geometry
                    )
                ) q
                WHERE n IS NOT NULL
                """,
                (gj, gj),
            )
            return [int(r["n"]) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        if close:
            conn.close()


def get_bus_edge_evidence_bulk(
    osm_way_ids: List[int],
    conn=None,
) -> Dict[int, Dict[str, Any]]:
    """Return bus_edge_evidence rows keyed by osm_way_id (best row per way if multiple directions)."""
    if not osm_way_ids:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        out: Dict[int, Dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT osm_way_id, direction, approved_detour_count, successful_trace_count,
                       manual_reject_count, confidence_score, last_seen_at
                FROM bus_edge_evidence
                WHERE osm_way_id = ANY(%s)
                """,
                (osm_way_ids,),
            )
            for row in cur.fetchall():
                wid = int(row["osm_way_id"])
                appr = int(row["approved_detour_count"] or 0)
                prev = out.get(wid)
                if prev is None or appr > int(prev.get("approved_detour_count") or 0):
                    out[wid] = {
                        "osm_way_id": wid,
                        "direction": row.get("direction"),
                        "approved_detour_count": appr,
                        "successful_trace_count": int(row["successful_trace_count"] or 0),
                        "manual_reject_count": int(row["manual_reject_count"] or 0),
                        "confidence_score": float(row["confidence_score"] or 0.0),
                    }
        return out
    except Exception:
        return {}
    finally:
        if close:
            conn.close()


def get_bus_turn_evidence_bulk(
    triplets: List[tuple[int, int, int]],
    conn=None,
) -> Dict[tuple[int, int, int], Dict[str, Any]]:
    """Keyed by (from_way_id, via_node_id, to_way_id)."""
    if not triplets:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        out: Dict[tuple[int, int, int], Dict[str, Any]] = {}
        with conn.cursor() as cur:
            # Chunk to avoid huge queries
            chunk_size = 50
            for i in range(0, len(triplets), chunk_size):
                chunk = triplets[i : i + chunk_size]
                parts = []
                flat: List[Any] = []
                for j, (fw, vn, tw) in enumerate(chunk):
                    parts.append(f"(%s::bigint, %s::bigint, %s::bigint)")
                    flat.extend([fw, vn, tw])
                sql = f"""
                    SELECT t.from_way_id, t.via_node_id, t.to_way_id, t.approved_detour_count, t.confidence_score
                    FROM bus_turn_evidence t
                    JOIN (VALUES {", ".join(parts)}) AS v(fw, vn, tw)
                      ON t.from_way_id = v.fw AND t.via_node_id = v.vn AND t.to_way_id = v.tw
                """
                cur.execute(sql, flat)
                for row in cur.fetchall():
                    key = (int(row["from_way_id"]), int(row["via_node_id"]), int(row["to_way_id"]))
                    out[key] = {
                        "approved_detour_count": int(row["approved_detour_count"] or 0),
                        "confidence_score": float(row["confidence_score"] or 0.0),
                    }
        return out
    except Exception:
        return {}
    finally:
        if close:
            conn.close()


def bump_bus_turn_evidence(from_way_id: int, via_node_id: int, to_way_id: int, conn=None) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bus_turn_evidence (from_way_id, via_node_id, to_way_id, approved_detour_count, last_seen_at)
                VALUES (%s, %s, %s, 1, NOW())
                ON CONFLICT (from_way_id, via_node_id, to_way_id) DO UPDATE SET
                    approved_detour_count = bus_turn_evidence.approved_detour_count + 1,
                    confidence_score = (bus_turn_evidence.approved_detour_count + 1)::double precision
                        / ((bus_turn_evidence.approved_detour_count + 1) + COALESCE(bus_turn_evidence.manual_reject_count, 0) + 2.0),
                    last_seen_at = NOW()
                """,
                (from_way_id, via_node_id, to_way_id),
            )
            conn.commit()
    except Exception:
        pass
    finally:
        if close:
            conn.close()


def bump_bus_edge_evidence(osm_way_id: int, direction: Optional[str], conn=None) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        d = direction if direction is not None else ""
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bus_edge_evidence (osm_way_id, direction, approved_detour_count, last_seen_at)
                VALUES (%s, %s, 1, NOW())
                ON CONFLICT (osm_way_id, direction) DO UPDATE SET
                    approved_detour_count = bus_edge_evidence.approved_detour_count + 1,
                    confidence_score = (bus_edge_evidence.approved_detour_count + 1)::double precision
                        / ((bus_edge_evidence.approved_detour_count + 1) + COALESCE(bus_edge_evidence.manual_reject_count, 0) + 2.0),
                    last_seen_at = NOW()
                """,
                (osm_way_id, d),
            )
            conn.commit()
    except Exception:
        pass
    finally:
        if close:
            conn.close()


def get_gtfs_bus_way_evidence_bulk(
    feed_id: int,
    osm_way_ids: List[int],
    conn=None,
) -> Dict[int, Dict[str, Any]]:
    """
    Return feed-scoped GTFS evidence rows keyed by osm_way_id (exact rows in gtfs_bus_way_evidence).

    This is an exact table lookup on (feed_id, osm_way_id); it does not infer bus usage from
    geometry or pattern_edge_match. Empty results usually mean the table is unpopulated for
    this feed, not that buses never use those ways.
    """
    if not osm_way_ids:
        return {}
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        out: Dict[int, Dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT osm_way_id, direction, trip_count, route_count, confidence_score, sample_trip_ids_json
                FROM gtfs_bus_way_evidence
                WHERE feed_id = %s
                  AND osm_way_id = ANY(%s)
                """,
                (feed_id, osm_way_ids),
            )
            for row in cur.fetchall():
                wid = int(row["osm_way_id"])
                conf = float(row["confidence_score"] or 0.0)
                prev = out.get(wid)
                if prev is None or conf > float(prev.get("confidence_score") or 0.0):
                    out[wid] = {
                        "osm_way_id": wid,
                        "direction": row["direction"],
                        "trip_count": int(row["trip_count"] or 0),
                        "route_count": int(row["route_count"] or 0),
                        "confidence_score": conf,
                        "sample_trip_ids_json": row.get("sample_trip_ids_json"),
                    }
        return out
    finally:
        if close:
            conn.close()


def get_detour_evidence_diag_stats(feed_id: int, conn=None) -> Dict[str, Any]:
    """
    Lightweight counts for detour v2 structured logs (not request-time geometry checks).

    - gtfs_evidence_rows_for_feed / gtfs_evidence_rows_all_feeds: rows in gtfs_bus_way_evidence
      (exact osm_way_id + feed_id table used by get_gtfs_bus_way_evidence_bulk).
    - pattern_edge_match_rows_for_feed_version: rows in pattern_edge_match joined to pattern_edge
      for the active feed_version key (physical matcher output; independent of gtfs_bus_way_evidence).
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    out: Dict[str, Any] = {
        "gtfs_evidence_rows_for_feed": 0,
        "gtfs_evidence_rows_all_feeds": 0,
        "pattern_edge_match_rows_for_feed_version": 0,
    }
    try:
        fv = get_active_feed_version_key(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*)::bigint FROM gtfs_bus_way_evidence WHERE feed_id = %s) AS for_feed,
                    (SELECT COUNT(*)::bigint FROM gtfs_bus_way_evidence) AS all_feeds
                """,
                (feed_id,),
            )
            row = cur.fetchone()
            if row:
                out["gtfs_evidence_rows_for_feed"] = int(row["for_feed"] or 0)
                out["gtfs_evidence_rows_all_feeds"] = int(row["all_feeds"] or 0)
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS n
                    FROM pattern_edge_match pem
                    INNER JOIN pattern_edge pe ON pe.pattern_edge_id = pem.pattern_edge_id
                    WHERE pe.feed_version = %s
                    """,
                    (fv,),
                )
                r2 = cur.fetchone()
                if r2:
                    out["pattern_edge_match_rows_for_feed_version"] = int(r2["n"] or 0)
            except Exception:
                pass
        return out
    finally:
        if close:
            conn.close()


def rebuild_shapes_lines_for_feed(cur: Any, feed_id: int) -> None:
    """
    Recompute shapes_lines from the shapes point table for one feed.
    Same INSERT ... ON CONFLICT DO NOTHING as ingest_gtfs_postgis.
    """
    cur.execute(
        """
        INSERT INTO shapes_lines(feed_id, shape_id, geom)
        SELECT
            s.feed_id,
            s.shape_id,
            ST_SetSRID(
                ST_MakeLine(ST_MakePoint(s.lon, s.lat) ORDER BY s.seq),
                4326
            )::geometry(LineString, 4326)
        FROM shapes s
        WHERE s.feed_id = %s
        GROUP BY s.feed_id, s.shape_id
        ON CONFLICT (feed_id, shape_id) DO NOTHING
        """,
        (feed_id,),
    )


def rebuild_gtfs_bus_way_evidence_for_feed(cur: Any, feed_id: int) -> None:
    """
    Replace gtfs_bus_way_evidence for feed_id from trips, shapes_lines, and osm_road_segments
    using a 30 m geography buffer along shape lines. Caller owns the transaction (commit).

    Long GTFS shapes get ST_Subdivide (128 vertices) so bbox + GiST joins to osm_road_segments
    stay selective; otherwise whole-country shape envelopes match almost every segment.
    """
    cur.execute(
        """
        DELETE FROM gtfs_bus_way_evidence
        WHERE feed_id = %s
        """,
        (feed_id,),
    )
    cur.execute(
        """
        WITH shape_fragments AS (
            SELECT sl.feed_id, sl.shape_id, frag.geom AS geom
            FROM shapes_lines sl
            CROSS JOIN LATERAL ST_Subdivide(sl.geom, 128) AS frag(geom)
            WHERE sl.feed_id = %s
        ),
        shapes_near_osm AS (
            SELECT DISTINCT sf.feed_id, sf.shape_id, ors.osm_way_id
            FROM shape_fragments sf
            JOIN osm_road_segments ors
              ON ors.geom && ST_Expand(sf.geom, 0.00035)
             AND ST_DWithin(sf.geom::geography, ors.geom::geography, 30.0)
        ),
        trip_shape_hits AS (
            SELECT
                t.feed_id,
                t.trip_id,
                t.route_id,
                COALESCE(t.direction_id::text, '') AS direction,
                sno.osm_way_id
            FROM trips t
            JOIN shapes_near_osm sno
              ON sno.feed_id = t.feed_id
             AND sno.shape_id = t.shape_id
            WHERE t.feed_id = %s
              AND t.shape_id IS NOT NULL
        ),
        agg AS (
            SELECT
                feed_id,
                osm_way_id,
                direction,
                COUNT(DISTINCT trip_id)::int AS trip_count,
                COUNT(DISTINCT route_id)::int AS route_count,
                TO_JSONB((ARRAY_AGG(DISTINCT trip_id ORDER BY trip_id))[1:8]) AS sample_trip_ids_json
            FROM trip_shape_hits
            GROUP BY feed_id, osm_way_id, direction
        )
        INSERT INTO gtfs_bus_way_evidence (
            feed_id,
            osm_way_id,
            direction,
            trip_count,
            route_count,
            sample_trip_ids_json,
            confidence_score,
            last_computed_at
        )
        SELECT
            a.feed_id,
            a.osm_way_id,
            a.direction,
            a.trip_count,
            a.route_count,
            a.sample_trip_ids_json,
            LEAST(1.0, GREATEST(0.05, LN(1 + a.trip_count) / LN(1 + 25)))::double precision AS confidence_score,
            NOW()
        FROM agg a
        ON CONFLICT (feed_id, osm_way_id, direction) DO UPDATE SET
            trip_count = EXCLUDED.trip_count,
            route_count = EXCLUDED.route_count,
            sample_trip_ids_json = EXCLUDED.sample_trip_ids_json,
            confidence_score = EXCLUDED.confidence_score,
            last_computed_at = NOW()
        """,
        (feed_id, feed_id),
    )


def get_candidate_osm_segments_for_polyline(
    coordinates: List[Tuple[float, float]],
    *,
    max_distance_m: float = 35.0,
    limit: int = 400,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Approximate map-matching by selecting OSM road segments near an input polyline
    and ordering by projected position along that line.
    """
    if len(coordinates) < 2:
        return []
    wkt_coords = ", ".join(f"{float(lon)} {float(lat)}" for lon, lat in coordinates)
    line_wkt = f"LINESTRING({wkt_coords})"
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT ST_SetSRID(ST_GeomFromText(%s), 4326) AS line
                )
                SELECT
                    ors.segment_id,
                    ors.osm_way_id,
                    ors.from_node_id,
                    ors.to_node_id,
                    ors.direction,
                    ors.highway,
                    ors.access,
                    ors.bus,
                    ors.psv,
                    ors.service,
                    ST_Length(ors.geom::geography) AS length_m
                FROM osm_road_segments ors, q
                WHERE ST_DWithin(ors.geom::geography, q.line::geography, %s)
                ORDER BY ST_LineLocatePoint(q.line, ST_LineInterpolatePoint(ors.geom, 0.5))
                LIMIT %s
                """,
                (line_wkt, float(max_distance_m), int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        if close:
            conn.close()


def upsert_gtfs_bus_way_evidence_rows(
    feed_id: int,
    rows: List[Dict[str, Any]],
    conn=None,
) -> None:
    """
    Upsert GTFS->OSM way evidence rows for one feed.
    Each row should include osm_way_id and may include direction, trip_count, route_count,
    sample_trip_ids_json, confidence_score.
    """
    if not rows:
        return
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO gtfs_bus_way_evidence (
                        feed_id, osm_way_id, direction, trip_count, route_count,
                        sample_trip_ids_json, confidence_score, last_computed_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
                    ON CONFLICT (feed_id, osm_way_id, direction) DO UPDATE SET
                        trip_count = EXCLUDED.trip_count,
                        route_count = EXCLUDED.route_count,
                        sample_trip_ids_json = EXCLUDED.sample_trip_ids_json,
                        confidence_score = EXCLUDED.confidence_score,
                        last_computed_at = NOW()
                    """,
                    (
                        int(feed_id),
                        int(r.get("osm_way_id") or 0),
                        str(r.get("direction") or ""),
                        int(r.get("trip_count") or 0),
                        int(r.get("route_count") or 0),
                        json.dumps(r.get("sample_trip_ids_json")),
                        float(r.get("confidence_score") or 0.0),
                    ),
                )
            conn.commit()
    finally:
        if close:
            conn.close()


def insert_bus_edge_constraint(
    *,
    osm_way_id: int,
    direction: Optional[str],
    constraint_type: str,
    severity: float,
    reason_code: Optional[str],
    notes: Optional[str],
    created_by: Optional[str],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bus_edge_constraints (
                    osm_way_id, direction, constraint_type, severity, reason_code, notes, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (osm_way_id, direction, constraint_type, severity, reason_code, notes, created_by),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])
    finally:
        if close:
            conn.close()


def insert_bus_turn_constraint(
    *,
    from_way_id: int,
    via_node_id: int,
    to_way_id: int,
    constraint_type: str,
    severity: float,
    reason_code: Optional[str],
    notes: Optional[str],
    created_by: Optional[str],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bus_turn_constraints (
                    from_way_id, via_node_id, to_way_id, constraint_type, severity, reason_code, notes, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (from_way_id, via_node_id, to_way_id, constraint_type, severity, reason_code, notes, created_by),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"])
    finally:
        if close:
            conn.close()


def _execute_split_ddl(conn, sql_text: str) -> None:
    """Run migration SQL text as one script (supports dollar-quoted DO blocks)."""
    lines: List[str] = []
    for line in sql_text.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        lines.append(line)
    buf = "\n".join(lines)
    with conn.cursor() as cur:
        cur.execute(buf)


def ensure_pattern_physical_layer_schema(conn=None) -> bool:
    """
    CREATE TABLE IF NOT EXISTS for pattern_edge / pattern_edge_match / detour_audit (physical OSM layer).
    Safe when tables already exist.
    """
    if os.getenv("DETOUR_PHYSICAL_SCHEMA_ENSURE", "1").strip().lower() in ("0", "false", "no"):
        return True
    path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "sql", "migrations", "ensure_pattern_physical_layer.sql")
    )
    if not os.path.isfile(path):
        return False
    with open(path, encoding="utf-8") as f:
        sql_text = f.read()
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        _execute_split_ddl(conn, sql_text)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def ensure_pattern_legal_anchor_schema(conn=None) -> bool:
    """
    CREATE TABLE IF NOT EXISTS for pattern_legal_anchor_candidate (precomputed detour anchors).
    Safe when table already exists.
    """
    if os.getenv("LEGAL_ANCHOR_SCHEMA_ENSURE", "1").strip().lower() in ("0", "false", "no"):
        return True
    path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "sql", "migrations", "ensure_pattern_legal_anchor.sql")
    )
    if not os.path.isfile(path):
        return False
    with open(path, encoding="utf-8") as f:
        sql_text = f.read()
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        _execute_split_ddl(conn, sql_text)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def get_pattern_edge_match_summary_for_stop_pair(
    pattern_id: str,
    from_stop_sequence: int,
    to_stop_sequence: int,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Return joined pattern_edge + match_summary row if physical layer is populated."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT checksum FROM feed_versions WHERE id = %s", (feed_id,))
            crow = cur.fetchone()
            checksum = crow.get("checksum") if crow else None
            feed_version = str(checksum) if checksum else str(feed_id)
            cur.execute(
                """
                SELECT s.matched_geom, s.entry_segment_id, s.exit_segment_id,
                       s.confidence, s.coverage_ratio, s.mean_offset_m,
                       s.mean_heading_error_deg, s.is_ambiguous, s.match_version,
                       pe.pattern_edge_id
                FROM pattern_edge pe
                JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
                WHERE pe.feed_version = %s AND pe.pattern_id = %s
                  AND pe.from_stop_sequence = %s AND pe.to_stop_sequence = %s
                LIMIT 1
                """,
                (feed_version, pattern_id, from_stop_sequence, to_stop_sequence),
            )
            row = cur.fetchone()
            if not row:
                return None
            return dict(row)
    except Exception:
        return None
    finally:
        if close:
            conn.close()


def get_pattern_id_for_trip(trip_id: str, conn=None) -> Optional[str]:
    """Resolve pattern_id for a trip: repr_trip_id match first, else route+direction+shape."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pattern_id FROM patterns
                WHERE feed_id = %s AND repr_trip_id = %s
                LIMIT 1
                """,
                (feed_id, trip_id),
            )
            row = cur.fetchone()
            if row and row.get("pattern_id") is not None:
                return str(row["pattern_id"])
            cur.execute(
                """
                SELECT shape_id, route_id, direction_id FROM trips
                WHERE feed_id = %s AND trip_id = %s
                LIMIT 1
                """,
                (feed_id, trip_id),
            )
            trow = cur.fetchone()
            if not trow:
                return None
            shape_id = trow.get("shape_id")
            route_id = trow.get("route_id")
            if not shape_id or not route_id:
                return None
            dir_id = trow.get("direction_id")
            cur.execute(
                """
                SELECT pattern_id FROM patterns
                WHERE feed_id = %s AND route_id = %s AND repr_shape_id = %s
                  AND (direction_id IS NOT DISTINCT FROM %s)
                LIMIT 1
                """,
                (feed_id, str(route_id), str(shape_id), dir_id),
            )
            prow = cur.fetchone()
            if not prow or prow.get("pattern_id") is None:
                return None
            return str(prow["pattern_id"])
    finally:
        if close:
            conn.close()


def get_active_feed_version_key(conn=None) -> str:
    """Same feed_version string as pattern_edge / get_pattern_edge_match_summary_for_stop_pair."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT checksum FROM feed_versions WHERE id = %s", (feed_id,))
            crow = cur.fetchone()
            checksum = crow.get("checksum") if crow else None
            return str(checksum) if checksum else str(feed_id)
    finally:
        if close:
            conn.close()


def fetch_pattern_edge_summaries_for_trip_ordered(
    trip_id: str, conn=None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Ordered stop-pair summaries aligned to trip stop_times (consecutive pairs).
    Missing pairs appear as empty dicts so callers can compute coverage gaps.
    Returns (ordered_rows, pair_count_expected).
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        pattern_id = get_pattern_id_for_trip(trip_id, conn)
        if not pattern_id:
            return [], 0
        stop_rows = get_stop_times_for_trip(trip_id, conn)
        if len(stop_rows) < 2:
            return [], 0
        feed_version = get_active_feed_version_key(conn)
        pairs: List[Tuple[int, int]] = []
        for i in range(len(stop_rows) - 1):
            a = stop_rows[i]
            b = stop_rows[i + 1]
            pairs.append((int(a["stop_sequence"]), int(b["stop_sequence"])))
        pair_count = len(pairs)
        if not pairs:
            return [], 0
        flat: List[Any] = []
        for a, b in pairs:
            flat.extend([a, b])
        placeholders = ",".join(["(%s,%s)"] * len(pairs))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT s.matched_geom, s.entry_segment_id, s.exit_segment_id,
                       s.confidence, s.coverage_ratio, s.mean_offset_m,
                       s.mean_heading_error_deg, s.is_ambiguous, s.match_version,
                       pe.pattern_edge_id, pe.from_stop_sequence, pe.to_stop_sequence
                FROM pattern_edge pe
                JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
                WHERE pe.feed_version = %s AND pe.pattern_id = %s
                  AND (pe.from_stop_sequence, pe.to_stop_sequence) IN ({placeholders})
                """,
                (feed_version, pattern_id, *flat),
            )
            fetched = cur.fetchall()
        by_pair: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for r in fetched or []:
            d = dict(r)
            key = (int(d["from_stop_sequence"]), int(d["to_stop_sequence"]))
            by_pair[key] = d
        ordered: List[Dict[str, Any]] = [by_pair.get(p, {}) for p in pairs]
        return ordered, pair_count
    finally:
        if close:
            conn.close()


def upsert_osm_road_segment(
    *,
    osm_way_id: int,
    from_node_id: int,
    to_node_id: int,
    geom_wkt: str,
    length_m: Optional[float] = None,
    highway: Optional[str] = None,
    conn=None,
) -> int:
    """Insert or return existing segment_id for (way, from_node, to_node)."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT segment_id FROM osm_road_segments
                WHERE osm_way_id = %s AND from_node_id = %s AND to_node_id = %s
                LIMIT 1
                """,
                (osm_way_id, from_node_id, to_node_id),
            )
            ex = cur.fetchone()
            if ex and ex.get("segment_id") is not None:
                sid = int(ex["segment_id"])
                if close:
                    conn.commit()
                return sid
            cur.execute(
                """
                INSERT INTO osm_road_segments
                  (osm_way_id, from_node_id, to_node_id, geom, length_m, highway)
                VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)
                RETURNING segment_id
                """,
                (osm_way_id, from_node_id, to_node_id, geom_wkt, length_m, highway),
            )
            row = cur.fetchone()
        if close:
            conn.commit()
        if not row:
            raise RuntimeError("upsert_osm_road_segment: insert returned no row")
        return int(row["segment_id"])
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def upsert_pattern_edge_row(
    *,
    feed_version: str,
    pattern_id: str,
    route_id: str,
    direction_id: Optional[int],
    from_stop_id: str,
    to_stop_id: str,
    from_stop_sequence: int,
    to_stop_sequence: int,
    representative_trip_id: Optional[str],
    representative_shape_id: Optional[str],
    gtfs_geom_wkt: Optional[str],
    gtfs_length_m: Optional[float],
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            if gtfs_geom_wkt:
                cur.execute(
                    """
                    INSERT INTO pattern_edge (
                      feed_version, pattern_id, route_id, direction_id,
                      from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
                      representative_trip_id, representative_shape_id, gtfs_geom, gtfs_length_m
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_GeomFromText(%s,4326), %s)
                    ON CONFLICT (feed_version, pattern_id, from_stop_sequence, to_stop_sequence)
                    DO UPDATE SET
                      route_id = EXCLUDED.route_id,
                      direction_id = EXCLUDED.direction_id,
                      from_stop_id = EXCLUDED.from_stop_id,
                      to_stop_id = EXCLUDED.to_stop_id,
                      representative_trip_id = EXCLUDED.representative_trip_id,
                      representative_shape_id = EXCLUDED.representative_shape_id,
                      gtfs_geom = EXCLUDED.gtfs_geom,
                      gtfs_length_m = EXCLUDED.gtfs_length_m
                    RETURNING pattern_edge_id
                    """,
                    (
                        feed_version,
                        pattern_id,
                        route_id,
                        direction_id,
                        from_stop_id,
                        to_stop_id,
                        from_stop_sequence,
                        to_stop_sequence,
                        representative_trip_id,
                        representative_shape_id,
                        gtfs_geom_wkt,
                        gtfs_length_m,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO pattern_edge (
                      feed_version, pattern_id, route_id, direction_id,
                      from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
                      representative_trip_id, representative_shape_id, gtfs_geom, gtfs_length_m
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NULL, %s)
                    ON CONFLICT (feed_version, pattern_id, from_stop_sequence, to_stop_sequence)
                    DO UPDATE SET
                      route_id = EXCLUDED.route_id,
                      direction_id = EXCLUDED.direction_id,
                      from_stop_id = EXCLUDED.from_stop_id,
                      to_stop_id = EXCLUDED.to_stop_id,
                      representative_trip_id = EXCLUDED.representative_trip_id,
                      representative_shape_id = EXCLUDED.representative_shape_id,
                      gtfs_length_m = EXCLUDED.gtfs_length_m
                    RETURNING pattern_edge_id
                    """,
                    (
                        feed_version,
                        pattern_id,
                        route_id,
                        direction_id,
                        from_stop_id,
                        to_stop_id,
                        from_stop_sequence,
                        to_stop_sequence,
                        representative_trip_id,
                        representative_shape_id,
                        gtfs_length_m,
                    ),
                )
            row = cur.fetchone()
        if close:
            conn.commit()
        if not row:
            raise RuntimeError("upsert_pattern_edge_row: insert returned no row")
        return int(row["pattern_edge_id"])
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def replace_pattern_edge_matches(
    pattern_edge_id: int,
    rows: List[Dict[str, Any]],
    conn=None,
) -> None:
    """rows: ordinal, segment_id, segment_forward, offset_mean_m, heading_error_deg."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pattern_edge_match WHERE pattern_edge_id = %s", (pattern_edge_id,))
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO pattern_edge_match (
                      pattern_edge_id, ordinal, segment_id, segment_forward,
                      offset_mean_m, heading_error_deg
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        pattern_edge_id,
                        int(r["ordinal"]),
                        int(r["segment_id"]),
                        bool(r.get("segment_forward", True)),
                        r.get("offset_mean_m"),
                        r.get("heading_error_deg"),
                    ),
                )
        if close:
            conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def upsert_pattern_edge_match_summary_row(
    pattern_edge_id: int,
    *,
    matched_geom_wkt: Optional[str],
    entry_segment_id: Optional[int],
    exit_segment_id: Optional[int],
    confidence: Optional[float],
    coverage_ratio: Optional[float],
    mean_offset_m: Optional[float],
    mean_heading_error_deg: Optional[float],
    is_ambiguous: bool,
    match_version: Optional[str],
    conn=None,
) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            if matched_geom_wkt:
                cur.execute(
                    """
                    INSERT INTO pattern_edge_match_summary (
                      pattern_edge_id, matched_geom, entry_segment_id, exit_segment_id,
                      confidence, coverage_ratio, mean_offset_m, mean_heading_error_deg,
                      is_ambiguous, match_version
                    ) VALUES (%s, ST_GeomFromText(%s,4326), %s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pattern_edge_id) DO UPDATE SET
                      matched_geom = EXCLUDED.matched_geom,
                      entry_segment_id = EXCLUDED.entry_segment_id,
                      exit_segment_id = EXCLUDED.exit_segment_id,
                      confidence = EXCLUDED.confidence,
                      coverage_ratio = EXCLUDED.coverage_ratio,
                      mean_offset_m = EXCLUDED.mean_offset_m,
                      mean_heading_error_deg = EXCLUDED.mean_heading_error_deg,
                      is_ambiguous = EXCLUDED.is_ambiguous,
                      match_version = EXCLUDED.match_version
                    """,
                    (
                        pattern_edge_id,
                        matched_geom_wkt,
                        entry_segment_id,
                        exit_segment_id,
                        confidence,
                        coverage_ratio,
                        mean_offset_m,
                        mean_heading_error_deg,
                        is_ambiguous,
                        match_version,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO pattern_edge_match_summary (
                      pattern_edge_id, matched_geom, entry_segment_id, exit_segment_id,
                      confidence, coverage_ratio, mean_offset_m, mean_heading_error_deg,
                      is_ambiguous, match_version
                    ) VALUES (%s, NULL, %s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (pattern_edge_id) DO UPDATE SET
                      matched_geom = EXCLUDED.matched_geom,
                      entry_segment_id = EXCLUDED.entry_segment_id,
                      exit_segment_id = EXCLUDED.exit_segment_id,
                      confidence = EXCLUDED.confidence,
                      coverage_ratio = EXCLUDED.coverage_ratio,
                      mean_offset_m = EXCLUDED.mean_offset_m,
                      mean_heading_error_deg = EXCLUDED.mean_heading_error_deg,
                      is_ambiguous = EXCLUDED.is_ambiguous,
                      match_version = EXCLUDED.match_version
                    """,
                    (
                        pattern_edge_id,
                        entry_segment_id,
                        exit_segment_id,
                        confidence,
                        coverage_ratio,
                        mean_offset_m,
                        mean_heading_error_deg,
                        is_ambiguous,
                        match_version,
                    ),
                )
        if close:
            conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def count_pattern_edge_summaries_with_match_version(
    conn,
    feed_version: str,
    pattern_id: str,
    match_version: str,
) -> int:
    """How many pattern legs have a summary row with the given match_version."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS c
            FROM pattern_edge pe
            JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
            WHERE pe.feed_version = %s AND pe.pattern_id = %s AND s.match_version = %s
            """,
            (feed_version, pattern_id, match_version),
        )
        row = cur.fetchone()
        return int(row["c"]) if row and row.get("c") is not None else 0


def list_pattern_edge_pairs_with_match_version(
    conn,
    feed_version: str,
    pattern_id: str,
    match_version: str,
    *,
    accept_ambiguous: bool = False,
) -> Set[Tuple[int, int]]:
    """
    (from_stop_sequence, to_stop_sequence) pairs that already have the given match_version.

    By default, legs marked is_ambiguous on the summary are NOT counted, so backfill will
    retry them. Pass accept_ambiguous=True to treat ambiguous matches as satisfied (legacy).
    """
    with conn.cursor() as cur:
        base = """
            SELECT pe.from_stop_sequence, pe.to_stop_sequence
            FROM pattern_edge pe
            JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
            WHERE pe.feed_version = %s AND pe.pattern_id = %s AND s.match_version = %s
            """
        if not accept_ambiguous:
            base += " AND s.is_ambiguous = FALSE"
        cur.execute(base, (feed_version, pattern_id, match_version))
        out: Set[Tuple[int, int]] = set()
        for r in cur.fetchall() or []:
            out.add((int(r["from_stop_sequence"]), int(r["to_stop_sequence"])))
        return out


def bulk_upsert_osm_road_segments(
    rows: List[Dict[str, Any]],
    conn,
) -> Dict[Tuple[int, int, int], int]:
    """
    Resolve segment_id for each (osm_way_id, from_node_id, to_node_id). conn must not auto-commit.
    rows: dicts with keys osm_way_id, from_node_id, to_node_id, geom_wkt, length_m, highway.
    """
    from psycopg2.extras import execute_values

    if not rows:
        return {}
    seen: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for r in rows:
        key = (int(r["osm_way_id"]), int(r["from_node_id"]), int(r["to_node_id"]))
        if key not in seen:
            seen[key] = r
    mapping: Dict[Tuple[int, int, int], int] = {}
    keys_list = list(seen.keys())
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT osm_way_id, from_node_id, to_node_id, segment_id
            FROM osm_road_segments
            WHERE (osm_way_id, from_node_id, to_node_id) IN %s
            """,
            (tuple(keys_list),),
        )
        for r in cur.fetchall() or []:
            k = (int(r["osm_way_id"]), int(r["from_node_id"]), int(r["to_node_id"]))
            mapping[k] = int(r["segment_id"])
    missing_keys = [k for k in keys_list if k not in mapping]
    if not missing_keys:
        return mapping
    to_insert = [seen[k] for k in missing_keys]
    tpls = [
        (
            int(r["osm_way_id"]),
            int(r["from_node_id"]),
            int(r["to_node_id"]),
            str(r["geom_wkt"]),
            r.get("length_m"),
            r.get("highway"),
        )
        for r in to_insert
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO osm_road_segments (osm_way_id, from_node_id, to_node_id, geom, length_m, highway)
            VALUES %s
            RETURNING osm_way_id, from_node_id, to_node_id, segment_id
            """,
            tpls,
        )
        for r in cur.fetchall() or []:
            k = (int(r["osm_way_id"]), int(r["from_node_id"]), int(r["to_node_id"]))
            mapping[k] = int(r["segment_id"])
    return mapping


def bulk_upsert_pattern_edge_rows(
    rows: List[Dict[str, Any]],
    conn,
) -> Dict[Tuple[int, int], int]:
    """
    Bulk upsert pattern_edge rows. Each dict: feed_version, pattern_id, route_id, direction_id,
    from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
    representative_trip_id, representative_shape_id, gtfs_geom_wkt, gtfs_length_m.
    Returns (from_stop_sequence, to_stop_sequence) -> pattern_edge_id.
    """
    from psycopg2.extras import execute_values

    if not rows:
        return {}
    out: Dict[Tuple[int, int], int] = {}
    with_geom: List[Dict[str, Any]] = []
    without_geom: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("gtfs_geom_wkt"):
            with_geom.append(r)
        else:
            without_geom.append(r)
    with conn.cursor() as cur:
        if with_geom:
            tpls = [
                (
                    r["feed_version"],
                    r["pattern_id"],
                    r["route_id"],
                    r.get("direction_id"),
                    r["from_stop_id"],
                    r["to_stop_id"],
                    int(r["from_stop_sequence"]),
                    int(r["to_stop_sequence"]),
                    r.get("representative_trip_id"),
                    r.get("representative_shape_id"),
                    str(r["gtfs_geom_wkt"]),
                    r.get("gtfs_length_m"),
                )
                for r in with_geom
            ]
            execute_values(
                cur,
                """
                INSERT INTO pattern_edge (
                  feed_version, pattern_id, route_id, direction_id,
                  from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
                  representative_trip_id, representative_shape_id, gtfs_geom, gtfs_length_m
                ) VALUES %s
                ON CONFLICT (feed_version, pattern_id, from_stop_sequence, to_stop_sequence)
                DO UPDATE SET
                  route_id = EXCLUDED.route_id,
                  direction_id = EXCLUDED.direction_id,
                  from_stop_id = EXCLUDED.from_stop_id,
                  to_stop_id = EXCLUDED.to_stop_id,
                  representative_trip_id = EXCLUDED.representative_trip_id,
                  representative_shape_id = EXCLUDED.representative_shape_id,
                  gtfs_geom = EXCLUDED.gtfs_geom,
                  gtfs_length_m = EXCLUDED.gtfs_length_m
                RETURNING pattern_edge_id, from_stop_sequence, to_stop_sequence
                """,
                tpls,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_GeomFromText(%s,4326), %s)",
            )
            for r in cur.fetchall() or []:
                out[(int(r["from_stop_sequence"]), int(r["to_stop_sequence"]))] = int(r["pattern_edge_id"])
        if without_geom:
            tpls = [
                (
                    r["feed_version"],
                    r["pattern_id"],
                    r["route_id"],
                    r.get("direction_id"),
                    r["from_stop_id"],
                    r["to_stop_id"],
                    int(r["from_stop_sequence"]),
                    int(r["to_stop_sequence"]),
                    r.get("representative_trip_id"),
                    r.get("representative_shape_id"),
                    r.get("gtfs_length_m"),
                )
                for r in without_geom
            ]
            execute_values(
                cur,
                """
                INSERT INTO pattern_edge (
                  feed_version, pattern_id, route_id, direction_id,
                  from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
                  representative_trip_id, representative_shape_id, gtfs_geom, gtfs_length_m
                ) VALUES %s
                ON CONFLICT (feed_version, pattern_id, from_stop_sequence, to_stop_sequence)
                DO UPDATE SET
                  route_id = EXCLUDED.route_id,
                  direction_id = EXCLUDED.direction_id,
                  from_stop_id = EXCLUDED.from_stop_id,
                  to_stop_id = EXCLUDED.to_stop_id,
                  representative_trip_id = EXCLUDED.representative_trip_id,
                  representative_shape_id = EXCLUDED.representative_shape_id,
                  gtfs_length_m = EXCLUDED.gtfs_length_m
                RETURNING pattern_edge_id, from_stop_sequence, to_stop_sequence
                """,
                tpls,
            )
            for r in cur.fetchall() or []:
                out[(int(r["from_stop_sequence"]), int(r["to_stop_sequence"]))] = int(r["pattern_edge_id"])
    return out


def bulk_delete_and_insert_pattern_edge_matches(
    match_rows: List[Tuple[int, Dict[str, Any]]],
    conn,
) -> None:
    """
    For each pattern_edge_id, delete existing matches then insert new rows.
    match_rows: (pattern_edge_id, row_dict with ordinal, segment_id, segment_forward, offset_mean_m, heading_error_deg).
    """
    from psycopg2.extras import execute_values

    if not match_rows:
        return
    by_pe: Dict[int, List[Dict[str, Any]]] = {}
    for peid, row in match_rows:
        by_pe.setdefault(int(peid), []).append(row)
    peids = list(by_pe.keys())
    flat: List[Tuple[Any, ...]] = []
    for peid, rows in by_pe.items():
        for r in rows:
            flat.append(
                (
                    peid,
                    int(r["ordinal"]),
                    int(r["segment_id"]),
                    bool(r.get("segment_forward", True)),
                    r.get("offset_mean_m"),
                    r.get("heading_error_deg"),
                )
            )
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pattern_edge_match WHERE pattern_edge_id = ANY(%s)", (peids,))
        if flat:
            execute_values(
                cur,
                """
                INSERT INTO pattern_edge_match (
                  pattern_edge_id, ordinal, segment_id, segment_forward,
                  offset_mean_m, heading_error_deg
                ) VALUES %s
                """,
                flat,
            )


def bulk_upsert_pattern_edge_match_summaries(
    rows: List[Dict[str, Any]],
    conn,
) -> None:
    """Each dict: pattern_edge_id, matched_geom_wkt, entry_segment_id, exit_segment_id, confidence,
    coverage_ratio, mean_offset_m, mean_heading_error_deg, is_ambiguous, match_version."""
    from psycopg2.extras import execute_values

    if not rows:
        return
    with_wkt: List[Dict[str, Any]] = []
    no_wkt: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("matched_geom_wkt"):
            with_wkt.append(r)
        else:
            no_wkt.append(r)
    with conn.cursor() as cur:
        if with_wkt:
            tpls = [
                (
                    int(r["pattern_edge_id"]),
                    str(r["matched_geom_wkt"]),
                    r.get("entry_segment_id"),
                    r.get("exit_segment_id"),
                    r.get("confidence"),
                    r.get("coverage_ratio"),
                    r.get("mean_offset_m"),
                    r.get("mean_heading_error_deg"),
                    bool(r.get("is_ambiguous", False)),
                    r.get("match_version"),
                )
                for r in with_wkt
            ]
            execute_values(
                cur,
                """
                INSERT INTO pattern_edge_match_summary (
                  pattern_edge_id, matched_geom, entry_segment_id, exit_segment_id,
                  confidence, coverage_ratio, mean_offset_m, mean_heading_error_deg,
                  is_ambiguous, match_version
                ) VALUES %s
                ON CONFLICT (pattern_edge_id) DO UPDATE SET
                  matched_geom = EXCLUDED.matched_geom,
                  entry_segment_id = EXCLUDED.entry_segment_id,
                  exit_segment_id = EXCLUDED.exit_segment_id,
                  confidence = EXCLUDED.confidence,
                  coverage_ratio = EXCLUDED.coverage_ratio,
                  mean_offset_m = EXCLUDED.mean_offset_m,
                  mean_heading_error_deg = EXCLUDED.mean_heading_error_deg,
                  is_ambiguous = EXCLUDED.is_ambiguous,
                  match_version = EXCLUDED.match_version
                """,
                tpls,
                template="(%s, ST_GeomFromText(%s,4326), %s,%s,%s,%s,%s,%s,%s,%s)",
            )
        if no_wkt:
            tpls = [
                (
                    int(r["pattern_edge_id"]),
                    r.get("entry_segment_id"),
                    r.get("exit_segment_id"),
                    r.get("confidence"),
                    r.get("coverage_ratio"),
                    r.get("mean_offset_m"),
                    r.get("mean_heading_error_deg"),
                    bool(r.get("is_ambiguous", False)),
                    r.get("match_version"),
                )
                for r in no_wkt
            ]
            execute_values(
                cur,
                """
                INSERT INTO pattern_edge_match_summary (
                  pattern_edge_id, matched_geom, entry_segment_id, exit_segment_id,
                  confidence, coverage_ratio, mean_offset_m, mean_heading_error_deg,
                  is_ambiguous, match_version
                ) VALUES %s
                ON CONFLICT (pattern_edge_id) DO UPDATE SET
                  matched_geom = EXCLUDED.matched_geom,
                  entry_segment_id = EXCLUDED.entry_segment_id,
                  exit_segment_id = EXCLUDED.exit_segment_id,
                  confidence = EXCLUDED.confidence,
                  coverage_ratio = EXCLUDED.coverage_ratio,
                  mean_offset_m = EXCLUDED.mean_offset_m,
                  mean_heading_error_deg = EXCLUDED.mean_heading_error_deg,
                  is_ambiguous = EXCLUDED.is_ambiguous,
                  match_version = EXCLUDED.match_version
                """,
                tpls,
                template="(%s, NULL, %s,%s,%s,%s,%s,%s,%s,%s)",
            )


def fetch_osm_road_segments_by_way_ids(
    way_ids: Sequence[int],
    conn,
) -> Dict[int, List[Dict[str, Any]]]:
    """Bulk-load all ``osm_road_segments`` rows per ``osm_way_id`` (for trace way-only fallback)."""
    out: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    uniq = sorted({int(w) for w in way_ids if int(w) > 0})
    if not uniq:
        return out
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT segment_id, osm_way_id, from_node_id, to_node_id,
                   heading_start_deg, heading_end_deg, COALESCE(import_source, '') AS import_source,
                   length_m
            FROM osm_road_segments
            WHERE osm_way_id = ANY(%s)
            """,
            (uniq,),
        )
        for r in cur.fetchall() or []:
            out[int(r["osm_way_id"])].append(dict(r))
    return out


def fetch_osm_road_segments_by_way_endpoint_triples(
    triples: Sequence[Tuple[int, int, int]],
    conn,
) -> Dict[Tuple[int, int, int], List[Dict[str, Any]]]:
    """
    Bulk-load candidate ``osm_road_segments`` rows for each directed triple
    (osm_way_id, from_node_id, to_node_id). Multiple rows per key are possible
    when legacy stubs overlap v3 geometry.
    """
    out: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = defaultdict(list)
    uniq = sorted({(int(w), int(f), int(t)) for w, f, t in triples})
    if not uniq:
        return out
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT segment_id, osm_way_id, from_node_id, to_node_id,
                   heading_start_deg, heading_end_deg, COALESCE(import_source, '') AS import_source,
                   length_m
            FROM osm_road_segments
            WHERE (osm_way_id, from_node_id, to_node_id) IN %s
            """,
            (tuple(uniq),),
        )
        for r in cur.fetchall() or []:
            key = (
                int(r["osm_way_id"]),
                int(r["from_node_id"]),
                int(r["to_node_id"]),
            )
            out[key].append(dict(r))
    return out


def count_pattern_osm_segments(
    *,
    feed_id: int,
    pattern_id: str,
    conn,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM pattern_osm_segments
            WHERE feed_id = %s AND pattern_id = %s
            """,
            (int(feed_id), str(pattern_id)),
        )
        row = cur.fetchone()
        return int(row["n"] or 0) if row else 0


def replace_pattern_osm_segments(
    *,
    feed_id: int,
    pattern_id: str,
    rows: Sequence[Dict[str, Any]],
    conn,
) -> None:
    """
    Replace stored path for ``(feed_id, pattern_id)``.

    rows: seq (int, 1-based), segment_id (int), confidence (optional float),
    source (optional str).
    """
    from psycopg2.extras import execute_values

    fid = int(feed_id)
    pid = str(pattern_id)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM pattern_osm_segments WHERE feed_id = %s AND pattern_id = %s",
            (fid, pid),
        )
        if not rows:
            return
        flat = [
            (
                fid,
                pid,
                int(r["seq"]),
                int(r["segment_id"]),
                r.get("confidence"),
                str(r["source"]) if r.get("source") is not None else None,
            )
            for r in rows
        ]
        execute_values(
            cur,
            """
            INSERT INTO pattern_osm_segments (feed_id, pattern_id, seq, segment_id, confidence, source)
            VALUES %s
            """,
            flat,
        )


def fetch_pattern_osm_segments_path(
    *,
    feed_id: int,
    pattern_id: str,
    conn,
) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT seq, segment_id, confidence, source
            FROM pattern_osm_segments
            WHERE feed_id = %s AND pattern_id = %s
            ORDER BY seq ASC
            """,
            (int(feed_id), str(pattern_id)),
        )
        return [dict(r) for r in cur.fetchall() or []]


def resolve_pattern_id_for_trip(trip_id: str, conn) -> Optional[str]:
    """
    Pick a ``patterns.pattern_id`` row for the trip using route, direction, shape, and optional repr_trip_id tie-break.
    """
    feed_id = get_active_feed_id(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT route_id, direction_id, shape_id
            FROM trips
            WHERE feed_id = %s AND trip_id = %s
            LIMIT 1
            """,
            (feed_id, str(trip_id)),
        )
        tr = cur.fetchone()
        if not tr:
            return None
        route_id = str(tr["route_id"])
        dir_id = tr.get("direction_id")
        shape_id = tr.get("shape_id")
        shape_s = str(shape_id) if shape_id is not None else None

        cur.execute(
            """
            SELECT pattern_id
            FROM patterns
            WHERE feed_id = %s
              AND route_id = %s
              AND (direction_id IS NOT DISTINCT FROM %s)
              AND (
                repr_shape_id IS NOT DISTINCT FROM %s
                OR repr_shape_id IS NULL
              )
            ORDER BY
              CASE WHEN repr_trip_id = %s THEN 0 ELSE 1 END,
              CASE WHEN repr_shape_id IS NOT NULL THEN 0 ELSE 1 END,
              pattern_id
            LIMIT 1
            """,
            (feed_id, route_id, dir_id, shape_s, str(trip_id)),
        )
        r1 = cur.fetchone()
        if r1 and r1.get("pattern_id"):
            return str(r1["pattern_id"])

        cur.execute(
            """
            SELECT pattern_id
            FROM patterns
            WHERE feed_id = %s AND route_id = %s
              AND (direction_id IS NOT DISTINCT FROM %s)
            ORDER BY COALESCE(frequency, 0) DESC, pattern_id
            LIMIT 1
            """,
            (feed_id, route_id, dir_id),
        )
        r2 = cur.fetchone()
        return str(r2["pattern_id"]) if r2 and r2.get("pattern_id") else None


def fetch_gtfs_bus_observed_segment_ids(*, feed_id: int, conn) -> Set[int]:
    """Segment ids present in ``gtfs_bus_segment_evidence`` for the feed."""
    out: Set[int] = set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT segment_id FROM gtfs_bus_segment_evidence WHERE feed_id = %s",
            (int(feed_id),),
        )
        for r in cur.fetchall() or []:
            out.add(int(r["segment_id"]))
    return out


def detour_path_intersects_polygon_wkt(
    segment_ids: Sequence[int],
    polygon_wkt: str,
    *,
    conn,
) -> bool:
    """True when any ``osm_road_segments.geom`` on the path intersects the polygon."""
    ids = [int(x) for x in segment_ids if x is not None]
    if not ids:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM osm_road_segments s,
                   (SELECT ST_SetSRID(ST_GeomFromText(%s, 4326), 4326) AS g) AS poly
              WHERE s.segment_id = ANY(%s)
                AND ST_Intersects(s.geom, poly.g)
            )
            """,
            (str(polygon_wkt), ids),
        )
        row = cur.fetchone()
        return bool(row and row.get("exists"))


def fetch_segment_path_geojson_feature(
    segment_ids: Sequence[int],
    *,
    conn,
    properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Merge ordered segment geometries into one GeoJSON LineString/MultiLineString feature."""
    ids = [int(x) for x in segment_ids]
    if not ids:
        return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": []}, "properties": properties or {}}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ST_AsGeoJSON(
              ST_LineMerge(
                ST_Collect(s.geom ORDER BY array_position(%s::bigint[], s.segment_id))
              )
            )::text AS gj
            FROM osm_road_segments s
            WHERE s.segment_id = ANY(%s)
            """,
            (ids, ids),
        )
        row = cur.fetchone()
        gj_raw = row["gj"] if row else None
    try:
        geom = json.loads(gj_raw) if gj_raw else {"type": "LineString", "coordinates": []}
    except Exception:
        geom = {"type": "LineString", "coordinates": []}
    feat: Dict[str, Any] = {"type": "Feature", "geometry": geom, "properties": dict(properties or {})}
    return feat


_PAT_TRIPS_TEMP = "_bus_ev_pat_trips"
_TRIP_KEY_COUNTS_TEMP = "_bus_ev_trip_key_counts"


def _bus_evidence_prepare_session(cur) -> None:
    """Favor hash aggregation / parallel gather for large feed-level GROUP BYs."""
    cur.execute("SET LOCAL work_mem = '512MB'")
    cur.execute("SET LOCAL max_parallel_workers_per_gather = 4")


def _bus_evidence_load_pat_trips_temp(cur, fid: int, *, exact_trip_counts: bool) -> None:
    """
    One trip-count row per pattern (reused for segment + turn aggregation).

    ``exact_trip_counts=False`` (default): aggregate trips once by route/direction/shape,
    then join patterns — avoids a nested loop over all trips per pattern.
    """
    cur.execute(f"DROP TABLE IF EXISTS {_PAT_TRIPS_TEMP}")
    cur.execute(f"DROP TABLE IF EXISTS {_TRIP_KEY_COUNTS_TEMP}")
    if exact_trip_counts:
        log("bus_evidence", f"feed_id={fid} pattern trip counts (exact trips join)")
        cur.execute(
            f"""
            CREATE TEMP TABLE {_PAT_TRIPS_TEMP} ON COMMIT PRESERVE ROWS AS
            SELECT p.pattern_id, COUNT(DISTINCT t.trip_id)::int AS trip_cnt
            FROM patterns p
            INNER JOIN trips t
              ON t.feed_id = p.feed_id
             AND t.route_id = p.route_id
             AND (t.direction_id IS NOT DISTINCT FROM p.direction_id)
             AND (
                  (p.repr_shape_id IS NOT NULL AND (t.shape_id IS NOT DISTINCT FROM p.repr_shape_id))
               OR (p.repr_shape_id IS NULL)
             )
            WHERE p.feed_id = %s
            GROUP BY p.pattern_id
            """,
            (fid,),
        )
    else:
        log(
            "bus_evidence",
            f"feed_id={fid} pattern trip counts (fast: pre-aggregate trips by route/direction/shape)",
        )
        cur.execute(
            f"""
            CREATE TEMP TABLE {_TRIP_KEY_COUNTS_TEMP} ON COMMIT PRESERVE ROWS AS
            SELECT
              route_id,
              direction_id,
              shape_id,
              COUNT(DISTINCT trip_id)::int AS trip_cnt
            FROM trips
            WHERE feed_id = %s
            GROUP BY route_id, direction_id, shape_id
            """,
            (fid,),
        )
        cur.execute(
            f"""
            CREATE INDEX ON {_TRIP_KEY_COUNTS_TEMP} (route_id, direction_id, shape_id)
            """
        )
        cur.execute(
            f"""
            CREATE TEMP TABLE {_PAT_TRIPS_TEMP} ON COMMIT PRESERVE ROWS AS
            SELECT
              p.pattern_id,
              COALESCE(
                CASE
                  WHEN p.repr_shape_id IS NOT NULL THEN ts.trip_cnt
                  ELSE trd.trip_cnt
                END,
                1
              )::int AS trip_cnt
            FROM patterns p
            LEFT JOIN {_TRIP_KEY_COUNTS_TEMP} ts
              ON p.repr_shape_id IS NOT NULL
             AND ts.route_id = p.route_id
             AND (ts.direction_id IS NOT DISTINCT FROM p.direction_id)
             AND (ts.shape_id IS NOT DISTINCT FROM p.repr_shape_id)
            LEFT JOIN (
              SELECT route_id, direction_id, SUM(trip_cnt)::int AS trip_cnt
              FROM {_TRIP_KEY_COUNTS_TEMP}
              GROUP BY route_id, direction_id
            ) trd
              ON p.repr_shape_id IS NULL
             AND trd.route_id = p.route_id
             AND (trd.direction_id IS NOT DISTINCT FROM p.direction_id)
            WHERE p.feed_id = %s
            """,
            (fid,),
        )
    cur.execute(f"CREATE INDEX ON {_PAT_TRIPS_TEMP} (pattern_id)")


def rebuild_gtfs_bus_corridor_evidence(
    *,
    feed_id: int,
    conn,
    commit_between_steps: bool = False,
    exact_trip_counts: bool = False,
) -> Dict[str, int]:
    """
    Replace ``gtfs_bus_segment_evidence`` and ``gtfs_bus_turn_evidence`` for ``feed_id``
    from ``pattern_osm_segments`` + ``patterns`` + ``trips`` join keys.

    Uses a temp pattern→trip table (computed once) and ``LEAD`` for consecutive segments
    instead of a self-join on ~1M+ rows. When ``commit_between_steps`` is true, commits
    after segment evidence so DDL in other sessions is not blocked for the turn step.
    """
    fid = int(feed_id)
    t_all = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*)::bigint AS n FROM pattern_osm_segments WHERE feed_id = %s",
            (fid,),
        )
        row = cur.fetchone()
        pos_rows = int(row["n"] if isinstance(row, dict) else row[0]) if row else 0
    log(
        "bus_evidence",
        f"feed_id={fid} pattern_osm_segments={pos_rows:,} — rebuild starting",
    )

    with conn.cursor() as cur:
        _bus_evidence_prepare_session(cur)
        t0 = time.perf_counter()
        log("bus_evidence", f"feed_id={fid} deleting prior evidence rows")
        cur.execute("DELETE FROM gtfs_bus_turn_evidence WHERE feed_id = %s", (fid,))
        turn_deleted = cur.rowcount
        cur.execute("DELETE FROM gtfs_bus_segment_evidence WHERE feed_id = %s", (fid,))
        seg_deleted = cur.rowcount
        log(
            "bus_evidence",
            f"feed_id={fid} deleted turns={turn_deleted:,} segments={seg_deleted:,} "
            f"elapsed_s={time.perf_counter() - t0:.1f}",
        )

        t0 = time.perf_counter()
        _bus_evidence_load_pat_trips_temp(cur, fid, exact_trip_counts=exact_trip_counts)
        log(
            "bus_evidence",
            f"feed_id={fid} pattern trip counts ready elapsed_s={time.perf_counter() - t0:.1f}",
        )

        t0 = time.perf_counter()
        log(
            "bus_evidence",
            f"feed_id={fid} aggregating segment evidence ({pos_rows:,} pattern_osm rows)",
        )
        cur.execute(
            f"""
            INSERT INTO gtfs_bus_segment_evidence (
              feed_id, segment_id, trip_count, route_count, pattern_count, confidence_score
            )
            SELECT
              %s,
              pos.segment_id,
              SUM(COALESCE(pt.trip_cnt, 1))::int,
              COUNT(DISTINCT p.route_id)::int,
              COUNT(DISTINCT pos.pattern_id)::int,
              AVG(COALESCE(pos.confidence, 0.5))::double precision
            FROM pattern_osm_segments pos
            INNER JOIN patterns p
              ON p.feed_id = pos.feed_id AND p.pattern_id = pos.pattern_id
            LEFT JOIN {_PAT_TRIPS_TEMP} pt ON pt.pattern_id = p.pattern_id
            WHERE pos.feed_id = %s
            GROUP BY pos.segment_id
            """,
            (fid, fid),
        )
        seg_inserted = cur.rowcount
        log(
            "bus_evidence",
            f"feed_id={fid} segment evidence inserted={seg_inserted:,} "
            f"elapsed_s={time.perf_counter() - t0:.1f}",
        )

    if commit_between_steps:
        conn.commit()
        log("bus_evidence", f"feed_id={fid} committed segment evidence")

    with conn.cursor() as cur:
        _bus_evidence_prepare_session(cur)
        t0 = time.perf_counter()
        log(
            "bus_evidence",
            f"feed_id={fid} aggregating turn evidence (window scan)",
        )
        cur.execute(
            f"""
            INSERT INTO gtfs_bus_turn_evidence (
              feed_id, from_segment_id, to_segment_id, trip_count, route_count, pattern_count, confidence_score
            )
            SELECT
              %s,
              chain.from_segment_id,
              chain.to_segment_id,
              SUM(COALESCE(pt.trip_cnt, 1))::int,
              COUNT(DISTINCT chain.pattern_id)::int,
              COUNT(DISTINCT chain.route_id)::int,
              AVG(chain.conf)::double precision
            FROM (
              SELECT
                pos.segment_id AS from_segment_id,
                LEAD(pos.segment_id) OVER (
                  PARTITION BY pos.pattern_id ORDER BY pos.seq
                ) AS to_segment_id,
                pos.pattern_id,
                p.route_id,
                (
                  COALESCE(pos.confidence, 0.5) + COALESCE(
                    LEAD(pos.confidence) OVER (PARTITION BY pos.pattern_id ORDER BY pos.seq),
                    0.5
                  )
                ) / 2.0::double precision AS conf
              FROM pattern_osm_segments pos
              INNER JOIN patterns p
                ON p.feed_id = pos.feed_id AND p.pattern_id = pos.pattern_id
              WHERE pos.feed_id = %s
            ) chain
            LEFT JOIN {_PAT_TRIPS_TEMP} pt ON pt.pattern_id = chain.pattern_id
            WHERE chain.to_segment_id IS NOT NULL
            GROUP BY chain.from_segment_id, chain.to_segment_id
            """,
            (fid, fid),
        )
        turn_inserted = cur.rowcount
        log(
            "bus_evidence",
            f"feed_id={fid} turn evidence inserted={turn_inserted:,} "
            f"elapsed_s={time.perf_counter() - t0:.1f}",
        )
        cur.execute(f"DROP TABLE IF EXISTS {_PAT_TRIPS_TEMP}")
        cur.execute(f"DROP TABLE IF EXISTS {_TRIP_KEY_COUNTS_TEMP}")

    log(
        "bus_evidence",
        f"feed_id={fid} rebuild SQL finished total_elapsed_s={time.perf_counter() - t_all:.1f}",
    )
    return {
        "gtfs_bus_segment_evidence_deleted_approx": int(seg_deleted),
        "gtfs_bus_turn_evidence_deleted_approx": int(turn_deleted),
        "gtfs_bus_segment_evidence_inserted": int(seg_inserted),
        "gtfs_bus_turn_evidence_inserted": int(turn_inserted),
        "pattern_osm_segments_input": int(pos_rows),
    }


def insert_detour_audit_row(
    *,
    detour_id: str,
    request_hash: Optional[str] = None,
    route_id: Optional[str] = None,
    direction_id: Optional[int] = None,
    trip_id: Optional[str] = None,
    entry_segment_id: Optional[int] = None,
    rejoin_segment_id: Optional[int] = None,
    validation_status: Optional[str] = None,
    validation_reason: Optional[str] = None,
    debug_json: Optional[Dict[str, Any]] = None,
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
                INSERT INTO detour_audit (
                  detour_id, request_hash, route_id, direction_id, trip_id,
                  entry_segment_id, rejoin_segment_id,
                  validation_status, validation_reason, debug_json
                ) VALUES (%s::uuid, %s,%s,%s,%s,%s,%s,%s,%s, %s::jsonb)
                """,
                (
                    detour_id,
                    request_hash,
                    route_id,
                    direction_id,
                    trip_id,
                    entry_segment_id,
                    rejoin_segment_id,
                    validation_status,
                    validation_reason,
                    json.dumps(debug_json) if debug_json is not None else None,
                ),
            )
        if close:
            conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def ensure_detour_v2_support_schema(conn=None) -> bool:
    """
    CREATE TABLE IF NOT EXISTS for incident + detour v2 persistence when the DB predates that DDL.
    Called on API startup unless DETOUR_V2_SCHEMA_ENSURE is 0/false/no.
    """
    if os.getenv("DETOUR_V2_SCHEMA_ENSURE", "1").strip().lower() in ("0", "false", "no"):
        return True
    path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "sql", "migrations", "ensure_detour_v2_support.sql")
    )
    if not os.path.isfile(path):
        return False
    with open(path, encoding="utf-8") as f:
        sql_text = f.read()
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        _execute_split_ddl(conn, sql_text)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def fetch_pattern_legal_anchor_candidates(
    feed_version: str,
    pattern_id: str,
    conn=None,
    *,
    anchor_version: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Load precomputed legal anchor rows for a pattern (exit + rejoin roles)."""
    if not feed_version or not pattern_id:
        return []
    ver = (anchor_version or LEGAL_ANCHOR_INDEX_ANCHOR_VERSION).strip() or LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT role, rank_in_role, shape_dist_m, lon, lat, osm_node_id,
                       incoming_way_id, score, trace_meta, anchor_version
                FROM pattern_legal_anchor_candidate
                WHERE feed_version = %s AND pattern_id = %s AND anchor_version = %s
                ORDER BY role, rank_in_role
                """,
                (feed_version, pattern_id, ver),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        if close:
            conn.close()


def replace_pattern_legal_anchor_candidates(
    feed_version: str,
    pattern_id: str,
    rows: List[Dict[str, Any]],
    *,
    anchor_version: str = "1",
    conn=None,
) -> None:
    """Replace all anchor candidates for a feed_version + pattern_id."""
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM pattern_legal_anchor_candidate
                WHERE feed_version = %s AND pattern_id = %s AND anchor_version = %s
                """,
                (feed_version, pattern_id, anchor_version),
            )
            for r in rows:
                tm = r.get("trace_meta")
                tm_adapt: Any
                if isinstance(tm, dict):
                    tm_adapt = Json(tm)
                elif tm is None:
                    tm_adapt = Json({})
                else:
                    tm_adapt = Json({})
                cur.execute(
                    """
                    INSERT INTO pattern_legal_anchor_candidate (
                        feed_version, pattern_id, role, rank_in_role, shape_dist_m,
                        lon, lat, osm_node_id, incoming_way_id, score, trace_meta, anchor_version
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        feed_version,
                        pattern_id,
                        str(r.get("role") or ""),
                        int(r.get("rank_in_role") or 0),
                        float(r.get("shape_dist_m") or 0.0),
                        float(r.get("lon")),
                        float(r.get("lat")),
                        r.get("osm_node_id"),
                        r.get("incoming_way_id"),
                        r.get("score"),
                        tm_adapt,
                        str(r.get("anchor_version") or anchor_version),
                    ),
                )
            if close:
                conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def fetch_osm_road_segments_by_from_nodes(
    node_ids: Sequence[int],
    conn=None,
) -> List[Dict[str, Any]]:
    """Bulk-load osm_road_segments rows for outgoing edges at junction nodes (from_node_id)."""
    ids = sorted({int(x) for x in node_ids if x is not None})
    if not ids:
        return []
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT from_node_id, osm_way_id, heading_start_deg
                FROM osm_road_segments
                WHERE from_node_id = ANY(%s)
                """,
                (ids,),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        if close:
            conn.close()


def fetch_osm_turn_restrictions_by_via_nodes(
    node_ids: Sequence[int],
    conn=None,
) -> List[Dict[str, Any]]:
    """Bulk-load turn restrictions involving via_node_id in the given set."""
    ids = sorted({int(x) for x in node_ids if x is not None})
    if not ids:
        return []
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT from_way_id, via_node_id, to_way_id
                FROM osm_turn_restrictions
                WHERE via_node_id = ANY(%s)
                """,
                (ids,),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        if close:
            conn.close()


def fetch_pattern_legal_anchor_pattern_status(
    feed_version: str,
    pattern_id: str,
    anchor_version: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome, row_count, updated_at
                FROM pattern_legal_anchor_pattern_status
                WHERE feed_version = %s AND pattern_id = %s AND anchor_version = %s
                """,
                (feed_version, pattern_id, anchor_version),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        if close:
            conn.close()


def upsert_pattern_legal_anchor_pattern_status(
    *,
    feed_version: str,
    pattern_id: str,
    anchor_version: str,
    outcome: str,
    row_count: int = 0,
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
                INSERT INTO pattern_legal_anchor_pattern_status (
                    feed_version, pattern_id, anchor_version, outcome, row_count, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (feed_version, pattern_id, anchor_version) DO UPDATE SET
                    outcome = EXCLUDED.outcome,
                    row_count = EXCLUDED.row_count,
                    updated_at = NOW()
                """,
                (feed_version, pattern_id, anchor_version, outcome, int(row_count)),
            )
            if close:
                conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def count_pattern_legal_anchor_candidates(
    feed_version: str,
    pattern_id: str,
    anchor_version: str,
    conn=None,
) -> int:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS c FROM pattern_legal_anchor_candidate
                WHERE feed_version = %s AND pattern_id = %s AND anchor_version = %s
                """,
                (feed_version, pattern_id, anchor_version),
            )
            row = cur.fetchone()
            return int(row["c"] if row and hasattr(row, "keys") else row[0] or 0)
    finally:
        if close:
            conn.close()


def fetch_pattern_trace_valhalla_cache(
    feed_version: str,
    repr_shape_id: str,
    direction: str,
    trace_version: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT edges_json, shape_lonlat_json, total_m
                FROM pattern_trace_valhalla_cache
                WHERE feed_version = %s AND repr_shape_id = %s AND direction = %s AND trace_version = %s
                """,
                (feed_version, repr_shape_id, direction, trace_version),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        if close:
            conn.close()


def upsert_pattern_trace_valhalla_cache(
    *,
    feed_version: str,
    repr_shape_id: str,
    direction: str,
    trace_version: str,
    edges: List[Dict[str, Any]],
    shape_lonlat: Optional[List[Tuple[float, float]]],
    total_m: float,
    conn=None,
) -> None:
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        ej = Json(edges)
        if shape_lonlat is not None:
            sj = Json([[float(a), float(b)] for a, b in shape_lonlat])
        else:
            sj = Json([])
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pattern_trace_valhalla_cache (
                    feed_version, repr_shape_id, direction, trace_version,
                    edges_json, shape_lonlat_json, total_m, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (feed_version, repr_shape_id, direction, trace_version) DO UPDATE SET
                    edges_json = EXCLUDED.edges_json,
                    shape_lonlat_json = EXCLUDED.shape_lonlat_json,
                    total_m = EXCLUDED.total_m,
                    created_at = NOW()
                """,
                (feed_version, repr_shape_id, direction, trace_version, ej, sj, float(total_m)),
            )
            if close:
                conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def compute_pattern_signature(
    feed_id: int,
    pattern_id: str,
    *,
    repr_trip_id: Optional[str] = None,
    repr_shape_id: Optional[str] = None,
    conn=None,
) -> str:
    """
    Stable hash for a pattern's representative trip/shape/stop_times content.
    """
    close = False
    if conn is None:
        conn = _get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            if repr_trip_id is None or repr_shape_id is None:
                cur.execute(
                    """
                    SELECT repr_trip_id, repr_shape_id
                    FROM patterns
                    WHERE feed_id = %s AND pattern_id = %s
                    """,
                    (feed_id, pattern_id),
                )
                prow = cur.fetchone()
                if not prow:
                    return _fingerprint_sha256_empty_pattern(feed_id, pattern_id)
                repr_trip_id = repr_trip_id or prow.get("repr_trip_id")
                repr_shape_id = repr_shape_id or prow.get("repr_shape_id")

            stop_times: List[Dict] = []
            if repr_trip_id:
                cur.execute(
                    """
                    SELECT stop_id, stop_sequence, shape_dist_traveled
                    FROM stop_times
                    WHERE feed_id = %s AND trip_id = %s
                    ORDER BY stop_sequence
                    """,
                    (feed_id, repr_trip_id),
                )
                stop_times = [dict(r) for r in cur.fetchall()]

            shapes: List[Dict] = []
            if repr_shape_id:
                cur.execute(
                    """
                    SELECT shape_id, seq, lat, lon
                    FROM shapes
                    WHERE feed_id = %s AND shape_id = %s
                    ORDER BY seq
                    """,
                    (feed_id, repr_shape_id),
                )
                shapes = [dict(r) for r in cur.fetchall()]

        payload = {
            "pattern_id": str(pattern_id),
            "repr_trip_id": repr_trip_id,
            "repr_shape_id": repr_shape_id,
            "stop_times": stop_times,
            "shapes": shapes,
        }
        raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()
    finally:
        if close:
            conn.close()


def _fingerprint_sha256_empty_pattern(feed_id: int, pattern_id: str) -> str:
    raw = json.dumps(
        {"feed_id": feed_id, "pattern_id": pattern_id, "missing": True},
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def upsert_pattern_signature(
    feed_id: int,
    pattern_id: str,
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
                INSERT INTO pattern_signatures (feed_id, pattern_id, sig_hash)
                VALUES (%s, %s, %s)
                ON CONFLICT (feed_id, pattern_id) DO UPDATE SET sig_hash = EXCLUDED.sig_hash
                """,
                (feed_id, pattern_id, sig_hash),
            )
            if close:
                conn.commit()
    except Exception:
        if close:
            conn.rollback()
        raise
    finally:
        if close:
            conn.close()


def get_pattern_signature(
    feed_id: int,
    pattern_id: str,
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
                SELECT sig_hash FROM pattern_signatures
                WHERE feed_id = %s AND pattern_id = %s
                """,
                (feed_id, pattern_id),
            )
            row = cur.fetchone()
            return row["sig_hash"] if row else None
    finally:
        if close:
            conn.close()
