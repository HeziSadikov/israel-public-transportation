"""
GTFS -> Postgres/PostGIS ingest script for Israel GTFS Detour Router.

Usage (example):

    python -m backend.scripts.ingest_gtfs_postgis \\
        --gtfs-zip ./israel-public-transportation.zip \\
        --database-url postgresql://user:password@localhost:5432/israel_gtfs

This script:
  - Creates a new feed_versions row.
  - Loads core GTFS CSVs into Postgres tables (see db_postgis_schema.sql).
  - Builds shapes_lines (LineStrings) and basic derived tables such as
    trip_time_bounds. Pattern and route_shapes precomputation can later be
    expanded, but we keep a minimal, robust first version here.
"""

from __future__ import annotations

import argparse
import csv
import io
import zipfile
from pathlib import Path
from typing import Dict, Iterable, Tuple

import psycopg2
from psycopg2.extras import execute_values


def _connect(database_url: str):
    return psycopg2.connect(database_url)


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


def _bulk_insert(cur, table: str, columns: Tuple[str, ...], rows: Iterable[Tuple]):
    rows = list(rows)
    if not rows:
        return
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES %s"
    execute_values(cur, sql, rows, page_size=1000)


def ingest_gtfs(gtfs_zip: Path, database_url: str, source_url: str | None = None):
    conn = _connect(database_url)
    conn.autocommit = False
    try:
        with conn, conn.cursor() as cur:
            checksum = None  # could compute SHA256 of zip if desired
            feed_id = _create_feed_version(cur, source_url or str(gtfs_zip), checksum)

            with zipfile.ZipFile(gtfs_zip, "r") as zf:
                # agencies.txt
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

                # routes.txt
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

                # stops.txt
                stops_rows = []
                for row in _read_csv_from_zip(zf, "stops.txt"):
                    lat = row.get("stop_lat")
                    lon = row.get("stop_lon")
                    try:
                        lat_f = float(lat) if lat is not None else None
                        lon_f = float(lon) if lon is not None else None
                    except ValueError:
                        lat_f, lon_f = None, None
                    geom_expr = f"ST_SetSRID(ST_MakePoint({lon_f}, {lat_f}), 4326)" if lat_f is not None and lon_f is not None else None
                    stops_rows.append(
                        (
                            feed_id,
                            row.get("stop_id"),
                            row.get("stop_name"),
                            row.get("stop_desc"),
                            lat_f,
                            lon_f,
                            geom_expr,
                            row.get("zone_id"),
                            row.get("parent_station"),
                        )
                    )
                # geometry must be expressed as expressions; use execute_values with template
                if stops_rows:
                    values_template = "(%s,%s,%s,%s,%s,%s,COALESCE(%s::text, NULL)::geography, %s,%s)"
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

                # trips.txt
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

                # stop_times.txt
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
                _bulk_insert(
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
                )

                # calendar.txt
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

                # calendar_dates.txt
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

                # shapes.txt -> shapes + shapes_lines
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

                # Build shapes_lines using PostGIS aggregation
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

                # Compute basic trip_time_bounds from stop_times
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

            # Activate this feed and deactivate others
            cur.execute("UPDATE feed_versions SET active = FALSE WHERE id <> %s", (feed_id,))
            cur.execute("UPDATE feed_versions SET active = TRUE WHERE id = %s", (feed_id,))

        conn.commit()
        print(f"Ingest completed for feed_id={feed_id}")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="Ingest GTFS zip into Postgres/PostGIS.")
    ap.add_argument("--gtfs-zip", type=str, required=True, help="Path to GTFS zip file")
    ap.add_argument(
        "--database-url",
        type=str,
        required=True,
        help="PostgreSQL connection URL, e.g. postgresql://user:pass@localhost:5432/dbname",
    )
    ap.add_argument("--source-url", type=str, default=None, help="Optional original GTFS URL for metadata")
    args = ap.parse_args()

    ingest_gtfs(Path(args.gtfs_zip), args.database_url, source_url=args.source_url)


if __name__ == "__main__":
    main()

