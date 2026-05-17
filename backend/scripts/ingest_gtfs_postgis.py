"""
GTFS -> Postgres/PostGIS ingest script for Israel GTFS Detour Router.

Usage (example):

    # Defaults: zip at repo root `israel-public-transportation.zip`, DB from DATABASE_URL
    # (or db_access fallback), source-url = MOT GTFS file URL (overridable via env).
    python -m backend.scripts.ingest_gtfs_postgis

    python -m backend.scripts.ingest_gtfs_postgis \\
        --gtfs-zip ./israel-public-transportation.zip \\
        --database-url postgresql://user:password@localhost:5432/israel_gtfs

    # Download from MOT if the zip is missing (no network if file already exists):
    python -m backend.scripts.ingest_gtfs_postgis --fetch

    # Always re-download before ingest:
    python -m backend.scripts.ingest_gtfs_postgis --fetch-always

This script:
  - SHA-256 checksums the zip; skips loading if the active feed already has the same checksum (use --force to override).
  - Creates a new feed_versions row.
  - Loads core GTFS CSVs into Postgres tables (see db_postgis_schema.sql).
  - Builds shapes_lines (LineStrings) and basic derived tables such as
    trip_time_bounds. Pattern and route_shapes precomputation can later be
    expanded, but we keep a minimal, robust first version here.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import execute_values

from backend.infra.config import GTFS_REMOTE_BASE, GTFS_REMOTE_FILENAME, LOCAL_GTFS_ZIP
from backend.infra.db_access import (
    DB_URL,
    rebuild_gtfs_bus_way_evidence_for_feed,
    rebuild_shapes_lines_for_feed,
)
from backend.infra.gtfs_download import download_gtfs_zip, get_remote_gtfs_metadata
from backend.infra.logging_utils import ensure_cli_action_logging, log


def _connect(database_url: str):
    return psycopg2.connect(database_url)


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _create_feed_version(cur, source_url: str | None, checksum: str | None) -> int:
    cur.execute(
        """
        INSERT INTO feed_versions (source_url, checksum, active)
        VALUES (%s, %s, FALSE)
        RETURNING id
        """,
        (source_url, checksum),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> Iterable[Dict[str, str]]:
    try:
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(text)
            for row in reader:
                yield row
    except KeyError:
        return []


# Progress for very large GTFS tables (Israel stop_times is tens of millions of rows).
_STOP_TIMES_CSV_LOG_EVERY = 250_000
# COPY runs faster than execute_values; larger chunks = fewer commits (less WAL sync churn).
_STOP_TIMES_COPY_CHUNK = 200_000


def _stop_times_copy_text_field(value: object) -> str:
    """PostgreSQL COPY TEXT format: tab-separated, \\N for NULL."""
    if value is None:
        return "\\N"
    s = str(value)
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _normalize_stop_times_pickup_dropoff(row: Tuple) -> Tuple:
    """Empty pickup/drop_off from GTFS → NULL for COPY into INT columns."""
    lst = list(row)
    for i in (6, 7):
        if lst[i] == "":
            lst[i] = None
    return tuple(lst)


def _bulk_insert_stop_times_via_copy(
    cur,
    table: str,
    columns: Tuple[str, ...],
    rows: List[Tuple],
    *,
    chunk_size: int = _STOP_TIMES_COPY_CHUNK,
    label: str = "",
) -> None:
    """Load stop_times with COPY FROM STDIN + commit per chunk (fast path vs execute_values)."""
    if not rows:
        return
    total = len(rows)
    t0 = time.perf_counter()
    tag = f" ({label})" if label else ""
    for off in range(0, total, chunk_size):
        chunk = rows[off : off + chunk_size]
        buf = io.StringIO()
        for t in chunk:
            t = _normalize_stop_times_pickup_dropoff(t)
            buf.write("\t".join(_stop_times_copy_text_field(x) for x in t))
            buf.write("\n")
        buf.seek(0)
        cur.copy_from(buf, table, columns=columns, sep="\t", null="\\N")
        cur.connection.commit()
        done = min(off + chunk_size, total)
        if done % 500_000 == 0 or done == total:
            print(
                f"[ingest]   ... {table}{tag} copy-inserted {done}/{total} rows "
                f"(elapsed_s={time.perf_counter() - t0:.1f}) +commit",
                flush=True,
            )


def _bulk_insert(cur, table: str, columns: Tuple[str, ...], rows: Iterable[Tuple]):
    rows = list(rows)
    if not rows:
        return
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES %s"
    execute_values(cur, sql, rows, page_size=1000)


def _bulk_insert_chunked(
    cur,
    table: str,
    columns: Tuple[str, ...],
    rows: List[Tuple],
    *,
    chunk_size: int = 100_000,
    label: str = "",
    commit_each_chunk: bool = False,
) -> None:
    """Large inserts in slices so the DB client stays responsive; logs progress.

    If commit_each_chunk is True, commits after each slice. Use for huge tables
    (e.g. stop_times): one multi-million-row transaction can stall for minutes
    when WAL/checkpoint pressure spikes; smaller commits keep throughput steadier.
    """
    if not rows:
        return
    total = len(rows)
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES %s"
    t0 = time.perf_counter()
    tag = f" ({label})" if label else ""
    for off in range(0, total, chunk_size):
        chunk = rows[off : off + chunk_size]
        execute_values(cur, sql, chunk, page_size=1000)
        if commit_each_chunk:
            cur.connection.commit()
        done = min(off + chunk_size, total)
        if done % 500_000 == 0 or done == total:
            cmt = " +commit" if commit_each_chunk else ""
            print(
                f"[ingest]   ... {table}{tag} inserted {done}/{total} rows "
                f"(elapsed_s={time.perf_counter() - t0:.1f}){cmt}",
                flush=True,
            )


def _log_lock_diagnostics(database_url: str, phase: str) -> None:
    try:
        diag_conn = _connect(database_url)
        try:
            with diag_conn.cursor() as cur:
                cur.execute("SET statement_timeout = '5s'")
                cur.execute(
                    """
                    SELECT pid, state, wait_event_type, wait_event, query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                    ORDER BY state, pid
                    LIMIT 10
                    """
                )
                rows = cur.fetchall() or []
        finally:
            diag_conn.close()
        for r in rows:
            pid, state, wet, we, query = r
            query_txt = (query or "").replace("\n", " ").strip()
            if len(query_txt) > 240:
                query_txt = query_txt[:240] + "..."
            log(
                "ingest",
                (
                    f"phase={phase} lock_diag pid={pid} state={state!r} "
                    f"wait_event_type={wet!r} wait_event={we!r} query={query_txt!r}"
                ),
            )
    except Exception as e:
        log("ingest", f"phase={phase} lock_diag_error={e!s}")


def _log_feed_versions_blockers(database_url: str, phase: str) -> None:
    try:
        diag_conn = _connect(database_url)
        try:
            with diag_conn.cursor() as cur:
                cur.execute("SET statement_timeout = '5s'")
                cur.execute(
                    """
                    SELECT a.pid, a.usename, a.state, a.wait_event_type, a.wait_event, a.query
                    FROM pg_stat_activity a
                    JOIN pg_locks l ON l.pid = a.pid
                    JOIN pg_class c ON c.oid = l.relation
                    WHERE c.relname = 'feed_versions'
                      AND a.datname = current_database()
                    ORDER BY a.state, a.pid
                    LIMIT 10
                    """
                )
                rows = cur.fetchall() or []
        finally:
            diag_conn.close()
        for r in rows:
            pid, usern, state, wet, we, query = r
            query_txt = (query or "").replace("\n", " ").strip()
            if len(query_txt) > 240:
                query_txt = query_txt[:240] + "..."
            log(
                "ingest",
                (
                    f"phase={phase} feed_versions_lock pid={pid} user={usern!r} state={state!r} "
                    f"wait_event_type={wet!r} wait_event={we!r} query={query_txt!r}"
                ),
            )
    except Exception as e:
        log("ingest", f"phase={phase} feed_versions_lock_diag_error={e!s}")


def ingest_gtfs(
    gtfs_zip: Path,
    database_url: str,
    source_url: str | None = None,
    *,
    force: bool = False,
):
    log("ingest", "phase=checksum start")
    t_checksum = time.perf_counter()
    zip_checksum = _sha256_file(gtfs_zip)
    log("ingest", f"phase=checksum done checksum={zip_checksum} elapsed_s={time.perf_counter() - t_checksum:.2f}")
    log("ingest", "phase=db_connect start")
    t_connect = time.perf_counter()
    conn = _connect(database_url)
    log("ingest", f"phase=db_connect done elapsed_s={time.perf_counter() - t_connect:.2f}")
    conn.autocommit = False
    try:
        if not force:
            log("ingest", "phase=checksum_skip_check start")
            t_skip_check = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '8s'")
                cur.execute("SET LOCAL statement_timeout = '20s'")
                try:
                    cur.execute(
                        "SELECT id, checksum FROM feed_versions WHERE active = TRUE LIMIT 1"
                    )
                    row = cur.fetchone()
                except (pg_errors.LockNotAvailable, pg_errors.QueryCanceled) as e:
                    elapsed = time.perf_counter() - t_skip_check
                    conn.rollback()
                    log(
                        "ingest",
                        (
                            "phase=checksum_skip_check error="
                            f"{type(e).__name__} elapsed_s={elapsed:.2f} "
                            "hint=feed_versions may be blocked by another transaction"
                        ),
                    )
                    _log_lock_diagnostics(database_url, "checksum_skip_check")
                    _log_feed_versions_blockers(database_url, "checksum_skip_check")
                    raise
            if row and row[1] and row[1] == zip_checksum:
                conn.rollback()
                print(
                    f"[ingest] Skip: zip unchanged (checksum={zip_checksum}, feed_id={row[0]})",
                    flush=True,
                )
                log(
                    "ingest",
                    f"phase=checksum_skip_check done skip=true feed_id={row[0]} elapsed_s={time.perf_counter() - t_skip_check:.2f}",
                )
                return
            conn.rollback()
            log(
                "ingest",
                f"phase=checksum_skip_check done skip=false elapsed_s={time.perf_counter() - t_skip_check:.2f}",
            )

        with conn, conn.cursor() as cur:
            print(f"[ingest] Starting ingest for {gtfs_zip} into {database_url}")
            feed_id = _create_feed_version(cur, source_url or str(gtfs_zip), zip_checksum)
            print(f"[ingest] Created feed_versions row feed_id={feed_id}")
            log("ingest", f"phase=zip_open start path={gtfs_zip}")

            with zipfile.ZipFile(gtfs_zip, "r") as zf:
                log("ingest", "phase=zip_open done")
                # agencies.txt
                print("[ingest] Loading agencies.txt ...", flush=True)
                t_agencies = time.perf_counter()
                agencies_rows = []
                for row in _read_csv_from_zip(zf, "agency.txt"):
                    agencies_rows.append(
                        (
                            feed_id,
                            row.get("agency_id") or "default",
                            row.get("agency_name"),
                            row.get("agency_url"),
                            row.get("agency_timezone"),
                            row.get("agency_lang"),
                            row.get("agency_phone"),
                        )
                    )
                _bulk_insert(
                    cur,
                    "agencies",
                    (
                        "feed_id",
                        "agency_id",
                        "name",
                        "url",
                        "timezone",
                        "lang",
                        "phone",
                    ),
                    agencies_rows,
                )
                log(
                    "ingest",
                    f"phase=load_agencies done rows={len(agencies_rows)} elapsed_s={time.perf_counter() - t_agencies:.2f}",
                )

                # routes.txt
                print("[ingest] Loading routes.txt ...", flush=True)
                t_routes = time.perf_counter()
                routes_rows = []
                for row in _read_csv_from_zip(zf, "routes.txt"):
                    routes_rows.append(
                        (
                            feed_id,
                            row.get("route_id"),
                            row.get("agency_id"),
                            row.get("route_short_name"),
                            row.get("route_long_name"),
                            int(row.get("route_type") or 3),
                            row.get("route_color"),
                            row.get("route_text_color"),
                        )
                    )
                _bulk_insert(
                    cur,
                    "routes",
                    (
                        "feed_id",
                        "route_id",
                        "agency_id",
                        "short_name",
                        "long_name",
                        "route_type",
                        "route_color",
                        "route_text_color",
                    ),
                    routes_rows,
                )
                log(
                    "ingest",
                    f"phase=load_routes done rows={len(routes_rows)} elapsed_s={time.perf_counter() - t_routes:.2f}",
                )

                # stops.txt
                print("[ingest] Loading stops.txt ...", flush=True)
                t_stops = time.perf_counter()
                stops_rows = []
                for row in _read_csv_from_zip(zf, "stops.txt"):
                    lat = row.get("stop_lat")
                    lon = row.get("stop_lon")
                    try:
                        lat_f = float(lat) if lat is not None else None
                        lon_f = float(lon) if lon is not None else None
                    except ValueError:
                        lat_f, lon_f = None, None
                    # Store WKT and let PostGIS build the geography.
                    wkt = f"POINT({lon_f} {lat_f})" if lat_f is not None and lon_f is not None else None
                    stops_rows.append(
                        (
                            feed_id,
                            row.get("stop_id"),
                            row.get("stop_name"),
                            row.get("stop_desc"),
                            lat_f,
                            lon_f,
                            wkt,
                            row.get("zone_id"),
                            row.get("parent_station"),
                        )
                    )
                if stops_rows:
                    # ST_GeogFromText(NULL) returns NULL, so missing coords are handled naturally.
                    values_template = "(%s,%s,%s,%s,%s,%s,ST_GeogFromText(%s),%s,%s)"
                    execute_values(
                        cur,
                        """
                        INSERT INTO stops
                        (feed_id, stop_id, name, description, lat, lon, geom, zone_id, parent_station)
                        VALUES %s
                        """,
                        stops_rows,
                        template=values_template,
                        page_size=1000,
                    )
                log(
                    "ingest",
                    f"phase=load_stops done rows={len(stops_rows)} elapsed_s={time.perf_counter() - t_stops:.2f}",
                )

                # trips.txt
                print("[ingest] Loading trips.txt ...", flush=True)
                t_trips = time.perf_counter()
                trips_rows = []
                for row in _read_csv_from_zip(zf, "trips.txt"):
                    trips_rows.append(
                        (
                            feed_id,
                            row.get("trip_id"),
                            row.get("route_id"),
                            row.get("service_id"),
                            row.get("direction_id") if row.get("direction_id") not in ("", None) else None,
                            row.get("shape_id"),
                            row.get("trip_headsign"),
                            row.get("block_id"),
                        )
                    )
                _bulk_insert(
                    cur,
                    "trips",
                    (
                        "feed_id",
                        "trip_id",
                        "route_id",
                        "service_id",
                        "direction_id",
                        "shape_id",
                        "headsign",
                        "block_id",
                    ),
                    trips_rows,
                )
                log(
                    "ingest",
                    f"phase=load_trips done rows={len(trips_rows)} elapsed_s={time.perf_counter() - t_trips:.2f}",
                )
                # End the mega-txn before stop_times: millions of rows + indexes in one
                # open transaction often trigger long checkpoint/WAL stalls mid-load.
                cur.connection.commit()
                print(
                    "[ingest] committed feed_versions..trips before stop_times "
                    "(avoids single huge transaction through stop_times)",
                    flush=True,
                )

                # stop_times.txt
                print("[ingest] Loading stop_times.txt ...", flush=True)
                t_stop_times = time.perf_counter()
                stop_time_rows = []
                for row in _read_csv_from_zip(zf, "stop_times.txt"):
                    try:
                        seq = int(row.get("stop_sequence") or 0)
                    except ValueError:
                        seq = 0
                    dist = row.get("shape_dist_traveled")
                    try:
                        dist_f = float(dist) if dist is not None and dist != "" else None
                    except ValueError:
                        dist_f = None
                    stop_time_rows.append(
                        (
                            feed_id,
                            row.get("trip_id"),
                            row.get("arrival_time"),
                            row.get("departure_time"),
                            row.get("stop_id"),
                            seq,
                            row.get("pickup_type"),
                            row.get("drop_off_type"),
                            dist_f,
                        )
                    )
                    n = len(stop_time_rows)
                    if n % _STOP_TIMES_CSV_LOG_EVERY == 0:
                        print(
                            f"[ingest]   ... stop_times.txt parsed {n} rows "
                            f"(elapsed_s={time.perf_counter() - t_stop_times:.1f})",
                            flush=True,
                        )
                print(
                    f"[ingest]   ... stop_times.txt parse complete rows={len(stop_time_rows)}, "
                    "COPY bulk inserting ...",
                    flush=True,
                )
                # COPY + per-chunk commit is much faster than execute_values; synchronous_commit
                # off reduces fsync spikes that otherwise stall inserts for many minutes mid-load.
                cur.execute("SET LOCAL synchronous_commit = OFF")
                _bulk_insert_stop_times_via_copy(
                    cur,
                    "stop_times",
                    (
                        "feed_id",
                        "trip_id",
                        "arrival_time",
                        "departure_time",
                        "stop_id",
                        "stop_sequence",
                        "pickup_type",
                        "drop_off_type",
                        "shape_dist_traveled",
                    ),
                    stop_time_rows,
                    label="bulk",
                )
                log(
                    "ingest",
                    f"phase=load_stop_times done rows={len(stop_time_rows)} elapsed_s={time.perf_counter() - t_stop_times:.2f}",
                )

                # calendar.txt
                print("[ingest] Loading calendar.txt ...", flush=True)
                t_calendar = time.perf_counter()
                cal_rows = []
                for row in _read_csv_from_zip(zf, "calendar.txt"):
                    cal_rows.append(
                        (
                            feed_id,
                            row.get("service_id"),
                            int(row.get("monday") or 0),
                            int(row.get("tuesday") or 0),
                            int(row.get("wednesday") or 0),
                            int(row.get("thursday") or 0),
                            int(row.get("friday") or 0),
                            int(row.get("saturday") or 0),
                            int(row.get("sunday") or 0),
                            int(row.get("start_date") or 0),
                            int(row.get("end_date") or 0),
                        )
                    )
                _bulk_insert(
                    cur,
                    "calendar",
                    (
                        "feed_id",
                        "service_id",
                        "monday",
                        "tuesday",
                        "wednesday",
                        "thursday",
                        "friday",
                        "saturday",
                        "sunday",
                        "start_date",
                        "end_date",
                    ),
                    cal_rows,
                )
                log(
                    "ingest",
                    f"phase=load_calendar done rows={len(cal_rows)} elapsed_s={time.perf_counter() - t_calendar:.2f}",
                )

                # calendar_dates.txt
                print("[ingest] Loading calendar_dates.txt ...", flush=True)
                t_calendar_dates = time.perf_counter()
                cald_rows = []
                for row in _read_csv_from_zip(zf, "calendar_dates.txt"):
                    cald_rows.append(
                        (
                            feed_id,
                            row.get("service_id"),
                            int(row.get("date") or 0),
                            int(row.get("exception_type") or 0),
                        )
                    )
                _bulk_insert(
                    cur,
                    "calendar_dates",
                    ("feed_id", "service_id", "date", "exception_type"),
                    cald_rows,
                )
                log(
                    "ingest",
                    f"phase=load_calendar_dates done rows={len(cald_rows)} elapsed_s={time.perf_counter() - t_calendar_dates:.2f}",
                )

                # shapes.txt -> shapes + shapes_lines
                print("[ingest] Loading shapes.txt ...", flush=True)
                t_shapes = time.perf_counter()
                shapes_points: Dict[Tuple[str, int], Tuple[float, float, float | None]] = {}
                for row in _read_csv_from_zip(zf, "shapes.txt"):
                    shape_id = row.get("shape_id")
                    if not shape_id:
                        continue
                    try:
                        seq = int(row.get("shape_pt_sequence") or 0)
                    except ValueError:
                        continue
                    lat = float(row.get("shape_pt_lat") or 0.0)
                    lon = float(row.get("shape_pt_lon") or 0.0)
                    dist = row.get("shape_dist_traveled")
                    try:
                        dist_f = float(dist) if dist is not None and dist != "" else None
                    except ValueError:
                        dist_f = None
                    shapes_points[(shape_id, seq)] = (lat, lon, dist_f)

                shapes_rows = []
                for (shape_id, seq), (lat, lon, dist_f) in shapes_points.items():
                    shapes_rows.append(
                        (
                            feed_id,
                            shape_id,
                            seq,
                            lat,
                            lon,
                            dist_f,
                        )
                    )
                _bulk_insert(
                    cur,
                    "shapes",
                    (
                        "feed_id",
                        "shape_id",
                        "seq",
                        "lat",
                        "lon",
                        "dist_traveled",
                    ),
                    shapes_rows,
                )
                log(
                    "ingest",
                    f"phase=load_shapes done rows={len(shapes_rows)} elapsed_s={time.perf_counter() - t_shapes:.2f}",
                )

                # Build shapes_lines using PostGIS aggregation
                print("[ingest] Building shapes_lines (this can take a while) ...", flush=True)
                log("ingest", "phase=build_shapes_lines start")
                t_shapes_lines = time.perf_counter()
                rebuild_shapes_lines_for_feed(cur, feed_id)
                log(
                    "ingest",
                    f"phase=build_shapes_lines done elapsed_s={time.perf_counter() - t_shapes_lines:.2f}",
                )

                # Build GTFS->OSM bus-way evidence (feed-scoped) for strict detour-v2 compatibility checks.
                # This is intentionally a conservative spatial association stage:
                # shape line buffered proximity -> OSM road segment way ids.
                print("[ingest] Building gtfs_bus_way_evidence ...", flush=True)
                log("ingest", "phase=build_gtfs_bus_way_evidence start")
                t_gtfs_way_ev = time.perf_counter()
                rebuild_gtfs_bus_way_evidence_for_feed(cur, feed_id)
                log(
                    "ingest",
                    f"phase=build_gtfs_bus_way_evidence done elapsed_s={time.perf_counter() - t_gtfs_way_ev:.2f}",
                )

                # Compute basic trip_time_bounds from stop_times
                print("[ingest] Computing trip_time_bounds ...", flush=True)
                log("ingest", "phase=build_trip_time_bounds start")
                t_trip_bounds = time.perf_counter()
                cur.execute(
                    """
                    INSERT INTO trip_time_bounds(feed_id, trip_id, first_sec, last_sec)
                    SELECT
                        st.feed_id,
                        st.trip_id,
                        MIN(EXTRACT(EPOCH FROM make_interval(hours := split_part(st.arrival_time, ':', 1)::int,
                                                             mins   := split_part(st.arrival_time, ':', 2)::int,
                                                             secs   := COALESCE(NULLIF(split_part(st.arrival_time, ':', 3), '')::int, 0)
                                            )))::int AS first_sec,
                        MAX(EXTRACT(EPOCH FROM make_interval(hours := split_part(st.departure_time, ':', 1)::int,
                                                             mins   := split_part(st.departure_time, ':', 2)::int,
                                                             secs   := COALESCE(NULLIF(split_part(st.departure_time, ':', 3), '')::int, 0)
                                            )))::int AS last_sec
                    FROM stop_times st
                    WHERE st.feed_id = %s
                    GROUP BY st.feed_id, st.trip_id
                    ON CONFLICT (feed_id, trip_id) DO NOTHING
                    """,
                    (feed_id,),
                )
                log(
                    "ingest",
                    f"phase=build_trip_time_bounds done elapsed_s={time.perf_counter() - t_trip_bounds:.2f}",
                )

            # Activate this feed and deactivate others
            print("[ingest] Marking feed active in feed_versions ...", flush=True)
            log("ingest", "phase=activate_feed start")
            t_activate = time.perf_counter()
            cur.execute("UPDATE feed_versions SET active = FALSE WHERE id <> %s", (feed_id,))
            cur.execute("UPDATE feed_versions SET active = TRUE WHERE id = %s", (feed_id,))
            log(
                "ingest",
                f"phase=activate_feed done elapsed_s={time.perf_counter() - t_activate:.2f}",
            )

        log("ingest", "phase=transaction_commit start")
        t_commit = time.perf_counter()
        conn.commit()
        log("ingest", f"phase=transaction_commit done elapsed_s={time.perf_counter() - t_commit:.2f}")
        print(f"[ingest] Completed successfully for feed_id={feed_id}")

        try:
            from backend.infra.pipeline_skip import (
                ensure_pipeline_schema,
                mark_feed_succeeded,
                StageName,
            )

            ensure_pipeline_schema(conn)
            mark_feed_succeeded(conn, feed_id, StageName.INGEST.value, zip_checksum)
            conn.commit()
        except Exception as e:
            log("ingest", f"phase=pipeline_stage_mark warning={e!s}")

        # Bulk INSERT leaves statistics stale; bad plans can make /area/routes and spatial joins slow.
        try:
            t_an = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(
                    "ANALYZE shapes_lines, trips, trip_time_bounds, calendar, calendar_dates, stop_times"
                )
            conn.commit()
            log(
                "ingest",
                f"phase=post_ingest_analyze done elapsed_s={time.perf_counter() - t_an:.2f}",
            )
        except Exception as e:
            conn.rollback()
            log("ingest", f"phase=post_ingest_analyze failed err={e!s}")
    finally:
        conn.close()


def _default_gtfs_source_url() -> str:
    return f"{GTFS_REMOTE_BASE.rstrip('/')}/{GTFS_REMOTE_FILENAME}"


def _meta_sidecar_path(gtfs_zip: Path) -> Path:
    return gtfs_zip.with_name(f"{gtfs_zip.name}.meta.json")


def _read_local_fetch_metadata(gtfs_zip: Path) -> Dict[str, str]:
    p = _meta_sidecar_path(gtfs_zip)
    if not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        print(f"[ingest] Warning: invalid metadata sidecar at {p}; ignoring.", flush=True)
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for k in ("source_url", "etag", "last_modified", "content_length", "saved_at_utc"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    return out


def _write_local_fetch_metadata(gtfs_zip: Path, source_url: str, remote_meta: Dict[str, Optional[str]]) -> None:
    log("ingest", f"phase=metadata_sidecar_write start path={_meta_sidecar_path(gtfs_zip)}")
    p = _meta_sidecar_path(gtfs_zip)
    payload = {
        "source_url": source_url,
        "etag": (remote_meta.get("etag") or "").strip() or None,
        "last_modified": (remote_meta.get("last_modified") or "").strip() or None,
        "content_length": (remote_meta.get("content_length") or "").strip() or None,
        "saved_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)
    log("ingest", f"phase=metadata_sidecar_write done path={p}")


def _should_download_if_newer(
    gtfs_zip: Path,
    source_url: str,
    remote_meta: Dict[str, Optional[str]],
    local_meta: Dict[str, str],
) -> tuple[bool, str]:
    if not gtfs_zip.is_file():
        return True, "zip_missing"
    if not remote_meta:
        return True, "metadata_unavailable"
    if local_meta.get("source_url") and local_meta.get("source_url") != source_url:
        return True, "source_url_changed"
    if not local_meta:
        return True, "no_local_metadata"

    re = (remote_meta.get("etag") or "").strip()
    le = (local_meta.get("etag") or "").strip()
    if re and le:
        return (re != le), ("etag_changed" if re != le else "etag_unchanged")

    rm = (remote_meta.get("last_modified") or "").strip()
    lm = (local_meta.get("last_modified") or "").strip()
    if rm and lm:
        return (rm != lm), ("last_modified_changed" if rm != lm else "last_modified_unchanged")

    rc = (remote_meta.get("content_length") or "").strip()
    lc = (local_meta.get("content_length") or "").strip()
    if rc and lc:
        return (rc != lc), ("content_length_changed" if rc != lc else "content_length_unchanged")

    return True, "metadata_inconclusive"


def main():
    ensure_cli_action_logging()
    log("ingest", "phase=main start")
    ap = argparse.ArgumentParser(
        description="Ingest GTFS zip into Postgres/PostGIS.",
        epilog=(
            f"Defaults: --gtfs-zip {LOCAL_GTFS_ZIP} ; "
            "--database-url from DATABASE_URL env (else db_access fallback); "
            f"--source-url {_default_gtfs_source_url()}"
        ),
    )
    ap.add_argument(
        "--gtfs-zip",
        type=str,
        default=str(LOCAL_GTFS_ZIP),
        help=f"Path to GTFS zip (default: {LOCAL_GTFS_ZIP})",
    )
    ap.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="PostgreSQL URL (default: DATABASE_URL env, else same fallback as backend/db_access.py)",
    )
    ap.add_argument(
        "--source-url",
        type=str,
        default=_default_gtfs_source_url(),
        help="Original GTFS URL for feed_versions metadata (default: MOT israel-public-transportation.zip)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Ingest even if the active feed already has the same zip checksum",
    )
    ap.add_argument(
        "--fetch",
        action="store_true",
        help=(
            "If the zip at --gtfs-zip is missing, download it from --source-url "
            "(default MOT GTFS URL) before ingesting."
        ),
    )
    ap.add_argument(
        "--fetch-always",
        action="store_true",
        help="Always download GTFS to --gtfs-zip from --source-url before ingesting (overwrites).",
    )
    ap.add_argument(
        "--fetch-if-newer",
        action="store_true",
        help=(
            "Download GTFS only when remote metadata differs from local sidecar metadata "
            "(etag/last-modified/content-length), or when metadata is unavailable/inconclusive."
        ),
    )
    args = ap.parse_args()

    fetch_modes = int(bool(args.fetch)) + int(bool(args.fetch_always)) + int(bool(args.fetch_if_newer))
    if fetch_modes > 1:
        ap.error("Use only one of --fetch, --fetch-always, and --fetch-if-newer")

    database_url = args.database_url or DB_URL
    gtfs_path = Path(args.gtfs_zip)

    if args.fetch_always:
        log("ingest", "phase=fetch_always start")
        print(
            f"[ingest] --fetch-always: downloading to {gtfs_path.resolve()} ...",
            flush=True,
        )
        download_gtfs_zip(
            gtfs_path,
            url=args.source_url,
            log_tag="ingest/fetch",
            timeout=600.0,
        )
        try:
            remote_meta = get_remote_gtfs_metadata(args.source_url, timeout=30.0, log_tag="ingest/fetch")
        except Exception:
            remote_meta = {}
        _write_local_fetch_metadata(gtfs_path, args.source_url, remote_meta)
        log("ingest", "phase=fetch_always done")
    elif args.fetch_if_newer:
        log("ingest", "phase=fetch_if_newer start")
        print("[ingest] --fetch-if-newer: probing remote metadata ...", flush=True)
        try:
            remote_meta = get_remote_gtfs_metadata(args.source_url, timeout=30.0, log_tag="ingest/fetch")
        except Exception as e:
            print(f"[ingest] --fetch-if-newer: metadata probe failed ({e!s}); downloading for safety.", flush=True)
            remote_meta = {}
        if remote_meta:
            print(
                "[ingest] --fetch-if-newer: remote metadata "
                f"etag={'yes' if remote_meta.get('etag') else 'no'}, "
                f"last_modified={'yes' if remote_meta.get('last_modified') else 'no'}, "
                f"content_length={'yes' if remote_meta.get('content_length') else 'no'}",
                flush=True,
            )
        else:
            print("[ingest] --fetch-if-newer: remote metadata unavailable", flush=True)
        local_meta = _read_local_fetch_metadata(gtfs_path)
        if local_meta:
            print(
                "[ingest] --fetch-if-newer: local sidecar found "
                f"(etag={'yes' if local_meta.get('etag') else 'no'}, "
                f"last_modified={'yes' if local_meta.get('last_modified') else 'no'}, "
                f"content_length={'yes' if local_meta.get('content_length') else 'no'})",
                flush=True,
            )
        else:
            print("[ingest] --fetch-if-newer: local sidecar missing/empty", flush=True)
        do_download, reason = _should_download_if_newer(gtfs_path, args.source_url, remote_meta, local_meta)
        print(f"[ingest] --fetch-if-newer: decision={reason}", flush=True)
        if do_download:
            print(
                f"[ingest] --fetch-if-newer: downloading to {gtfs_path.resolve()} (reason={reason}) ...",
                flush=True,
            )
            download_gtfs_zip(
                gtfs_path,
                url=args.source_url,
                log_tag="ingest/fetch",
                timeout=600.0,
            )
            if not remote_meta:
                try:
                    remote_meta = get_remote_gtfs_metadata(args.source_url, timeout=30.0, log_tag="ingest/fetch")
                except Exception:
                    remote_meta = {}
            _write_local_fetch_metadata(gtfs_path, args.source_url, remote_meta)
        else:
            print(
                f"[ingest] --fetch-if-newer: skip download (reason={reason}); using {gtfs_path.resolve()}",
                flush=True,
            )
        log("ingest", "phase=fetch_if_newer done")
    elif not gtfs_path.is_file():
        if args.fetch:
            log("ingest", "phase=fetch_missing_only start")
            print(
                f"[ingest] Zip missing; downloading (--fetch) to {gtfs_path.resolve()} ...",
                flush=True,
            )
            download_gtfs_zip(
                gtfs_path,
                url=args.source_url,
                log_tag="ingest/fetch",
                timeout=600.0,
            )
            try:
                remote_meta = get_remote_gtfs_metadata(args.source_url, timeout=30.0, log_tag="ingest/fetch")
            except Exception:
                remote_meta = {}
            _write_local_fetch_metadata(gtfs_path, args.source_url, remote_meta)
            log("ingest", "phase=fetch_missing_only done")
        else:
            ap.error(
                f"GTFS zip not found: {gtfs_path.resolve()} "
                "(use --fetch to download if missing, --fetch-if-newer, or --fetch-always to overwrite)"
            )

    log("ingest", "phase=ingest_gtfs_call start")
    ingest_gtfs(gtfs_path, database_url, source_url=args.source_url, force=args.force)
    log("ingest", "phase=ingest_gtfs_call done")
    log("ingest", "phase=main done")


if __name__ == "__main__":
    main()

