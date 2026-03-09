from __future__ import annotations

"""
Import GTFS from israel-public-transportation.zip into a local SQLite database.

This is the first step toward a “real system” backed by SQLite instead of
re-parsing CSV on every request.

Usage (from project root):

  python -m scripts.import_gtfs_sqlite --db data/gtfs.db

Notes:
- This creates and populates tables that mirror standard GTFS files:
  agency, routes, trips, stops, stop_times, calendar, calendar_dates, shapes.
- Columns are created based on the CSV headers (all TEXT), with some typed
  convenience columns added where useful later.
- Existing data in those tables is dropped before import.
"""

import argparse
import csv
import sqlite3
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_GTFS_ZIP = BASE_DIR / "israel-public-transportation.zip"


GTFS_FILES = [
    "agency.txt",
    "routes.txt",
    "trips.txt",
    "stops.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "shapes.txt",
]


def _open_gtfs_reader(zip_path: Path, name: str) -> Iterable[dict]:
    with zipfile.ZipFile(zip_path) as zf:
        try:
            with zf.open(name, "r") as f:
                text = f.read().decode("utf-8-sig").splitlines()
        except KeyError:
            return []
    reader = csv.DictReader(text)
    return list(reader)


def _ensure_table_for_file(conn: sqlite3.Connection, filename: str, rows: List[dict]) -> None:
    """
    Create a table that mirrors the GTFS file's headers.
    All columns are TEXT for now; we can add indices and typed views later.
    """
    table = filename.replace(".txt", "")
    cur = conn.cursor()

    cur.execute(f'DROP TABLE IF EXISTS "{table}"')

    if not rows:
        conn.commit()
        return

    headers = list(rows[0].keys())
    cols_sql = ", ".join(f'"{h}" TEXT' for h in headers)
    cur.execute(f'CREATE TABLE "{table}" ({cols_sql})')
    placeholders = ", ".join("?" for _ in headers)
    columns_sql = ", ".join([f'"{h}"' for h in headers])
    insert_sql = f'INSERT INTO "{table}" ({columns_sql}) VALUES ({placeholders})'

    batch: List[tuple] = []
    for row in rows:
        batch.append(tuple(row.get(h) for h in headers))
        if len(batch) >= 5000:
            cur.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        cur.executemany(insert_sql, batch)
    conn.commit()


def _parse_gtfs_time_to_seconds(t: str) -> Optional[int]:
    """Parse GTFS time (HH:MM:SS, allow 24-27h) to seconds since midnight. Returns None if invalid."""
    if not t or not isinstance(t, str):
        return None
    t = t.strip()
    if not t:
        return None
    parts = t.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = map(int, parts)
        return h * 3600 + m * 60 + s
    except ValueError:
        return None


def _ensure_stop_times_with_dep_sec(conn: sqlite3.Connection, rows: List[dict]) -> None:
    """Create stop_times with dep_sec (INTEGER) for fast trip time bounds via SQL."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS stop_times")
    if not rows:
        conn.commit()
        return

    headers = [k for k in rows[0].keys() if k != "dep_sec"]
    cols_sql = ", ".join(f'"{h}" TEXT' for h in headers) + ', "dep_sec" INTEGER'
    cur.execute(f'CREATE TABLE stop_times ({cols_sql})')
    placeholders = ", ".join("?" for _ in headers) + ", ?"
    columns_sql = ", ".join([f'"{h}"' for h in headers]) + ', "dep_sec"'
    insert_sql = f'INSERT INTO stop_times ({columns_sql}) VALUES ({placeholders})'

    batch: List[tuple] = []
    for row in rows:
        t_str = (row.get("departure_time") or row.get("arrival_time") or "").strip()
        dep_sec = _parse_gtfs_time_to_seconds(t_str) if t_str else None
        batch.append(tuple(row.get(h) for h in headers) + (dep_sec,))
        if len(batch) >= 5000:
            cur.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        cur.executemany(insert_sql, batch)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stop_times_trip ON stop_times(trip_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stop_times_dep_sec ON stop_times(dep_sec)")
    conn.commit()


def _ensure_trip_time_bounds(conn: sqlite3.Connection) -> None:
    """Materialize trip_id -> (lo_sec, hi_sec) for area search without scanning all stop_times."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS trip_time_bounds")
    cur.execute("""
        CREATE TABLE trip_time_bounds (
            trip_id TEXT PRIMARY KEY,
            lo_sec INTEGER NOT NULL,
            hi_sec INTEGER NOT NULL
        )
    """)
    cur.execute("""
        INSERT INTO trip_time_bounds (trip_id, lo_sec, hi_sec)
        SELECT trip_id, MIN(dep_sec), MAX(dep_sec)
        FROM stop_times
        WHERE dep_sec IS NOT NULL
        GROUP BY trip_id
    """)
    conn.commit()


def _ensure_shape_bbox(conn: sqlite3.Connection) -> None:
    """Precompute per-shape bounding box for spatial prefilter in area search."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS shape_bbox")
    cur.execute("""
        CREATE TABLE shape_bbox (
            shape_id TEXT PRIMARY KEY,
            min_lon REAL NOT NULL,
            min_lat REAL NOT NULL,
            max_lon REAL NOT NULL,
            max_lat REAL NOT NULL
        )
    """)
    cur.execute("""
        INSERT INTO shape_bbox (shape_id, min_lon, min_lat, max_lon, max_lat)
        SELECT
            shape_id,
            MIN(CAST(shape_pt_lon AS REAL)),
            MIN(CAST(shape_pt_lat AS REAL)),
            MAX(CAST(shape_pt_lon AS REAL)),
            MAX(CAST(shape_pt_lat AS REAL))
        FROM shapes
        WHERE shape_id IS NOT NULL AND shape_id != ''
          AND shape_pt_lon IS NOT NULL AND shape_pt_lat IS NOT NULL
        GROUP BY shape_id
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shape_bbox_bounds ON shape_bbox(min_lon, max_lon, min_lat, max_lat)")
    conn.commit()


def import_gtfs_to_sqlite(db_path: Path, zip_path: Path) -> None:
    if not zip_path.exists():
        raise SystemExit(f"GTFS zip not found at {zip_path}")

    print(f"Importing GTFS from {zip_path} into SQLite DB {db_path} ...")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        for fname in GTFS_FILES:
            print(f"  - Loading {fname} ...", flush=True)
            rows = list(_open_gtfs_reader(zip_path, fname))
            if fname == "stop_times.txt":
                _ensure_stop_times_with_dep_sec(conn, rows)
            else:
                _ensure_table_for_file(conn, fname, rows)
        print("  - Building trip_time_bounds and shape_bbox ...", flush=True)
        _ensure_trip_time_bounds(conn)
        _ensure_shape_bbox(conn)
        print("GTFS import to SQLite completed.")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import GTFS zip into a SQLite database.")
    parser.add_argument(
        "--db",
        type=str,
        default=str(BASE_DIR / "data" / "gtfs.db"),
        help="Path to SQLite DB file (default: data/gtfs.db).",
    )
    parser.add_argument(
        "--zip",
        type=str,
        default=str(DEFAULT_GTFS_ZIP),
        help="Path to GTFS zip (default: israel-public-transportation.zip in project root).",
    )
    args = parser.parse_args()

    import_gtfs_to_sqlite(Path(args.db), Path(args.zip))


if __name__ == "__main__":
    main()

