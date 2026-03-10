"""
Legacy SQLite persistence for GTFS and graph cache.

PREFERRED: Use PostgreSQL/PostGIS (see backend/db_access.py and backend/db_postgis_schema.sql).
This module is kept for backward compatibility when DATABASE_URL is not set or
when precomputed graphs were stored in SQLite. Route graph cache (route_graphs_v2, etc.)
is still written here by precompute scripts; /graph/build prefers PostGIS when available.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import BASE_DIR

DB_PATH = BASE_DIR / "data" / "gtfs.db"

_conn: sqlite3.Connection | None = None


def get_conn() -> sqlite3.Connection:
  global _conn
  if _conn is None:
    if not DB_PATH.exists():
      raise FileNotFoundError(f"SQLite DB not found at {DB_PATH}")
    # Allow the same connection object to be used from FastAPI's threadpool
    # workers (read-heavy, occasional writes). Precompute runs in a separate
    # process with its own connection, so this is safe here.
    _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    # Ensure legacy route_graphs table exists for graph caching.
    _conn.execute(
      """
      CREATE TABLE IF NOT EXISTS route_graphs (
        feed_version TEXT NOT NULL,
        route_id     TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        date_ymd     TEXT NOT NULL,
        pretty_osm   INTEGER NOT NULL,
        graph_blob   BLOB NOT NULL,
        PRIMARY KEY (feed_version, route_id, direction_id, date_ymd, pretty_osm)
      )
      """
    )
    # V2 schema: patterns, pattern_stops, route_graphs_v2 (compact representation).
    _conn.execute(
      """
      CREATE TABLE IF NOT EXISTS patterns (
        feed_version    TEXT NOT NULL,
        route_id        TEXT NOT NULL,
        direction_id    TEXT NOT NULL,
        date_ymd        TEXT NOT NULL,
        pattern_id      TEXT NOT NULL,
        frequency       INTEGER NOT NULL,
        repr_trip_id    TEXT,
        repr_shape_id   TEXT,
        used_shape      INTEGER NOT NULL,
        PRIMARY KEY (feed_version, route_id, direction_id, date_ymd, pattern_id)
      )
      """
    )
    _conn.execute(
      """
      CREATE TABLE IF NOT EXISTS pattern_stops (
        feed_version TEXT NOT NULL,
        pattern_id   TEXT NOT NULL,
        seq          INTEGER NOT NULL,
        stop_id      TEXT NOT NULL,
        lat          REAL NOT NULL,
        lon          REAL NOT NULL,
        name         TEXT,
        PRIMARY KEY (feed_version, pattern_id, seq)
      )
      """
    )
    _conn.execute(
      """
      CREATE INDEX IF NOT EXISTS idx_pattern_stops_pid
      ON pattern_stops(feed_version, pattern_id)
      """
    )
    _conn.execute(
      """
      CREATE TABLE IF NOT EXISTS route_graphs_v2 (
        feed_version TEXT NOT NULL,
        route_id     TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        date_ymd     TEXT NOT NULL,
        pattern_id   TEXT NOT NULL,
        pretty_osm   INTEGER NOT NULL,

        stop_count   INTEGER NOT NULL,
        edge_count   INTEGER NOT NULL,

        -- optional packed stop_ids; for now we keep it NULL and rely on pattern_stops
        stop_ids_blob BLOB,

        -- adjacency arrays as packed bytes
        u_idx_blob   BLOB NOT NULL,
        v_idx_blob   BLOB NOT NULL,
        w_s_blob     BLOB NOT NULL,

        -- display polyline for GTFS / OSM geometry
        pattern_polyline_gtfs TEXT,
        pattern_polyline_osm  TEXT,

        created_at TEXT NOT NULL,
        PRIMARY KEY (feed_version, route_id, direction_id, date_ymd, pattern_id, pretty_osm)
      )
      """
    )
    _conn.execute(
      """
      CREATE INDEX IF NOT EXISTS idx_route_graphs_v2_lookup
      ON route_graphs_v2(feed_version, route_id, direction_id, date_ymd, pretty_osm)
      """
    )
    _conn.commit()
  return _conn


def search_routes(q: str, limit: int) -> List[Dict]:
  conn = get_conn()
  q_stripped = q.strip()
  # If the query is purely numeric, prefer exact matches on route_short_name/route_id.
  if q_stripped.isdigit():
    sql = """
      SELECT r.route_id,
             r.route_short_name,
             r.route_long_name,
             r.agency_id,
             r.route_type,
             a.agency_name
      FROM routes r
      LEFT JOIN agency a ON a.agency_id = r.agency_id
      WHERE r.route_short_name = ?
         OR r.route_id = ?
      LIMIT ?
    """
    cur = conn.execute(sql, (q_stripped, q_stripped, limit))
  else:
    like = f"%{q_stripped.lower()}%"
    sql = """
      SELECT r.route_id,
             r.route_short_name,
             r.route_long_name,
             r.agency_id,
             r.route_type,
             a.agency_name
      FROM routes r
      LEFT JOIN agency a ON a.agency_id = r.agency_id
      WHERE LOWER(COALESCE(r.route_id, '')) LIKE ?
         OR LOWER(COALESCE(r.route_short_name, '')) LIKE ?
         OR LOWER(COALESCE(r.route_long_name, '')) LIKE ?
      LIMIT ?
    """
    cur = conn.execute(sql, (like, like, like, limit))
  rows = cur.fetchall()
  return [dict(row) for row in rows]


def stops_in_bounds_sqlite(
  min_lat: float,
  max_lat: float,
  min_lon: float,
  max_lon: float,
  limit: int,
) -> List[Dict]:
  conn = get_conn()
  sql = """
    SELECT stop_id,
           stop_name,
           stop_code,
           CAST(stop_lat AS REAL) AS stop_lat,
           CAST(stop_lon AS REAL) AS stop_lon
    FROM stops
    WHERE stop_lat IS NOT NULL
      AND stop_lon IS NOT NULL
      AND CAST(stop_lat AS REAL) BETWEEN ? AND ?
      AND CAST(stop_lon AS REAL) BETWEEN ? AND ?
    LIMIT ?
  """
  cur = conn.execute(sql, (min_lat, max_lat, min_lon, max_lon, limit))
  rows = cur.fetchall()
  return [dict(row) for row in rows]


def get_route_graph_blob(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pretty_osm: bool,
) -> Optional[bytes]:
  conn = get_conn()
  cur = conn.execute(
    """
    SELECT graph_blob
    FROM route_graphs
    WHERE feed_version = ?
      AND route_id = ?
      AND direction_id = ?
      AND date_ymd = ?
      AND pretty_osm = ?
    """,
    (feed_version, route_id, direction_id, date_ymd, 1 if pretty_osm else 0),
  )
  row = cur.fetchone()
  return row["graph_blob"] if row else None


def save_route_graph_blob(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pretty_osm: bool,
  blob: bytes,
) -> None:
  conn = get_conn()
  conn.execute(
    """
    INSERT OR REPLACE INTO route_graphs
      (feed_version, route_id, direction_id, date_ymd, pretty_osm, graph_blob)
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    (feed_version, route_id, direction_id, date_ymd, 1 if pretty_osm else 0, blob),
  )
  conn.commit()


def save_pattern_record(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pattern_id: str,
  frequency: int,
  repr_trip_id: Optional[str],
  repr_shape_id: Optional[str],
  used_shape: bool,
) -> None:
  conn = get_conn()
  conn.execute(
    """
    INSERT OR REPLACE INTO patterns
      (feed_version, route_id, direction_id, date_ymd, pattern_id,
       frequency, repr_trip_id, repr_shape_id, used_shape)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
      feed_version,
      route_id,
      direction_id or "",
      date_ymd,
      pattern_id,
      frequency,
      repr_trip_id,
      repr_shape_id,
      1 if used_shape else 0,
    ),
  )
  conn.commit()


def save_pattern_stops(
  feed_version: str,
  pattern_id: str,
  stops: List[Dict],
) -> None:
  """
  Upsert ordered stops for a pattern. `stops` is a list of dicts with:
  {seq, stop_id, lat, lon, name}.
  """
  conn = get_conn()
  cur = conn.cursor()
  cur.execute(
    """
    DELETE FROM pattern_stops
    WHERE feed_version = ? AND pattern_id = ?
    """,
    (feed_version, pattern_id),
  )
  cur.executemany(
    """
    INSERT INTO pattern_stops
      (feed_version, pattern_id, seq, stop_id, lat, lon, name)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    [
      (
        feed_version,
        pattern_id,
        s["seq"],
        s["stop_id"],
        s["lat"],
        s["lon"],
        s.get("name"),
      )
      for s in stops
    ],
  )
  conn.commit()


def save_route_graph_v2(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pattern_id: str,
  pretty_osm: bool,
  stop_count: int,
  edge_count: int,
  u_idx_blob: bytes,
  v_idx_blob: bytes,
  w_s_blob: bytes,
  pattern_polyline_gtfs: Optional[str],
  pattern_polyline_osm: Optional[str],
) -> None:
  conn = get_conn()
  conn.execute(
    """
    INSERT OR REPLACE INTO route_graphs_v2
      (feed_version, route_id, direction_id, date_ymd, pattern_id, pretty_osm,
       stop_count, edge_count, stop_ids_blob,
       u_idx_blob, v_idx_blob, w_s_blob,
       pattern_polyline_gtfs, pattern_polyline_osm, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, datetime('now'))
    """,
    (
      feed_version,
      route_id,
      direction_id or "",
      date_ymd,
      pattern_id,
      1 if pretty_osm else 0,
      stop_count,
      edge_count,
      u_idx_blob,
      v_idx_blob,
      w_s_blob,
      pattern_polyline_gtfs,
      pattern_polyline_osm,
    ),
  )
  conn.commit()


def get_route_graph_v2(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pretty_osm: bool,
) -> Optional[Dict]:
  conn = get_conn()
  cur = conn.execute(
    """
    SELECT *
    FROM route_graphs_v2
    WHERE feed_version = ?
      AND route_id = ?
      AND direction_id = ?
      AND date_ymd = ?
      AND pretty_osm = ?
    """,
    (feed_version, route_id, direction_id or "", date_ymd, 1 if pretty_osm else 0),
  )
  row = cur.fetchone()
  return dict(row) if row else None


def get_route_graphs_v2_for_route(
  feed_version: str,
  route_id: str,
  date_ymd: str,
  pretty_osm: bool,
) -> List[Dict]:
  """
  Return all v2 graph rows for a given route/date, regardless of direction_id.
  Used as a wildcard fallback when the caller did not specify direction.
  """
  conn = get_conn()
  cur = conn.execute(
    """
    SELECT *
    FROM route_graphs_v2
    WHERE feed_version = ?
      AND route_id = ?
      AND date_ymd = ?
      AND pretty_osm = ?
    """,
    (feed_version, route_id, date_ymd, 1 if pretty_osm else 0),
  )
  rows = cur.fetchall()
  return [dict(r) for r in rows]


def get_pattern_record(
  feed_version: str,
  route_id: str,
  direction_id: str,
  date_ymd: str,
  pattern_id: str,
) -> Optional[Dict]:
  conn = get_conn()
  cur = conn.execute(
    """
    SELECT *
    FROM patterns
    WHERE feed_version = ?
      AND route_id = ?
      AND direction_id = ?
      AND date_ymd = ?
      AND pattern_id = ?
    """,
    (feed_version, route_id, direction_id or "", date_ymd, pattern_id),
  )
  row = cur.fetchone()
  return dict(row) if row else None


def get_pattern_stops(
  feed_version: str,
  pattern_id: str,
  limit: Optional[int] = None,
) -> List[Dict]:
  conn = get_conn()
  sql = """
    SELECT seq, stop_id, lat, lon, name
    FROM pattern_stops
    WHERE feed_version = ? AND pattern_id = ?
    ORDER BY seq ASC
  """
  params: Tuple = (feed_version, pattern_id)
  if limit is not None:
    sql += " LIMIT ?"
    params = (feed_version, pattern_id, limit)
  cur = conn.execute(sql, params)
  rows = cur.fetchall()
  return [dict(r) for r in rows]


def get_stop_times_for_trip(trip_id: str) -> List[Dict]:
  """
  Return ordered stop_times rows for a single trip_id, using the SQLite DB.
  """
  conn = get_conn()
  cur = conn.execute(
    """
    SELECT *
    FROM stop_times
    WHERE trip_id = ?
    ORDER BY CAST(stop_sequence AS INTEGER)
    """,
    (trip_id,),
  )
  return [dict(row) for row in cur.fetchall()]


def get_trip_time_bounds_from_db() -> Dict[str, Tuple[int, int]]:
  """
  Return trip_id -> (lo_sec, hi_sec). Tries (1) trip_time_bounds table,
  (2) aggregate on stop_times.dep_sec if that column exists. Returns {} if both fail.
  """
  try:
    conn = get_conn()
  except FileNotFoundError:
    return {}
  try:
    cur = conn.execute(
      "SELECT trip_id, lo_sec, hi_sec FROM trip_time_bounds"
    )
    rows = cur.fetchall()
    if rows:
      return {row["trip_id"]: (int(row["lo_sec"]), int(row["hi_sec"])) for row in rows}
  except sqlite3.OperationalError:
    pass
  # Fallback: one aggregate query if stop_times has dep_sec (e.g. new import, table not yet built)
  try:
    cur = conn.execute(
      """
      SELECT trip_id, MIN(CAST(dep_sec AS INTEGER)) AS lo_sec, MAX(CAST(dep_sec AS INTEGER)) AS hi_sec
      FROM stop_times WHERE dep_sec IS NOT NULL AND dep_sec != ''
      GROUP BY trip_id
      """
    )
    return {row["trip_id"]: (int(row["lo_sec"]), int(row["hi_sec"])) for row in cur.fetchall()}
  except sqlite3.OperationalError:
    return {}


def get_area_search_candidates(
  active_service_ids: List[str],
  start_sec: int,
  end_sec: int,
) -> List[Tuple[str, str, str, int, int]]:
  """
  Return (shape_id, route_id, direction_id, lo_sec, hi_sec) for trips that are
  active in the given service set and overlap [start_sec, end_sec].
  Pushes filtering into SQL so we avoid iterating all trips in Python.
  """
  if not active_service_ids:
    return []
  try:
    conn = get_conn()
  except FileNotFoundError:
    return []
  try:
    cur = conn.execute("SELECT 1 FROM trip_time_bounds LIMIT 1")
    cur.fetchone()
  except sqlite3.OperationalError:
    return []
  placeholders = ",".join("?" * len(active_service_ids))
  sql = f"""
    SELECT DISTINCT
      t.shape_id,
      t.route_id,
      COALESCE(t.direction_id, '') AS direction_id,
      b.lo_sec,
      b.hi_sec
    FROM trips t
    JOIN trip_time_bounds b ON b.trip_id = t.trip_id
    WHERE t.service_id IN ({placeholders})
      AND t.shape_id IS NOT NULL AND TRIM(COALESCE(t.shape_id, '')) != ''
      AND t.route_id IS NOT NULL
      AND b.hi_sec >= ? AND b.lo_sec <= ?
  """
  params = list(active_service_ids) + [start_sec, end_sec]
  cur = conn.execute(sql, params)
  return [
    (row["shape_id"], row["route_id"], row["direction_id"], int(row["lo_sec"]), int(row["hi_sec"]))
    for row in cur.fetchall()
  ]


def get_shape_ids_in_bbox(
  min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> List[str]:
  """
  Return shape_ids whose bounding box overlaps the given rectangle.
  Used as spatial prefilter before building LineStrings and calling intersects.
  """
  try:
    conn = get_conn()
  except FileNotFoundError:
    return []
  try:
    cur = conn.execute(
      """
      SELECT shape_id FROM shape_bbox
      WHERE max_lon >= ? AND min_lon <= ?
        AND max_lat >= ? AND min_lat <= ?
      """,
      (min_lon, max_lon, min_lat, max_lat),
    )
  except sqlite3.OperationalError:
    return []
  return [row["shape_id"] for row in cur.fetchall()]


def iter_all_stop_times() -> List[Dict]:
  """
  Return all stop_times rows from SQLite as a list of dicts.
  Avoid in hot paths; prefer get_trip_time_bounds_from_db() or stream_trip_time_bounds().
  """
  conn = get_conn()
  cur = conn.execute("SELECT * FROM stop_times")
  return [dict(row) for row in cur.fetchall()]


# Chunk size for streaming stop_times (avoids loading millions of rows at once)
_STOP_TIMES_FETCHMANY_SIZE = 50_000


def stream_trip_time_bounds(
  parse_time_fn,
) -> Dict[str, Tuple[int, int]]:
  """
  Stream stop_times in chunks and compute trip_id -> (lo_sec, hi_sec) without
  loading the whole table. Use when trip_time_bounds table and dep_sec aggregate
  are unavailable (old DB). parse_time_fn(str) -> int or None for GTFS time.
  """
  try:
    conn = get_conn()
  except FileNotFoundError:
    return {}
  cur = conn.execute(
    "SELECT trip_id, departure_time, arrival_time FROM stop_times"
  )
  bounds: Dict[str, Tuple[int, int]] = {}
  while True:
    rows = cur.fetchmany(_STOP_TIMES_FETCHMANY_SIZE)
    if not rows:
      break
    for row in rows:
      trip_id = row["trip_id"]
      if not trip_id:
        continue
      t_str = (row["departure_time"] or row["arrival_time"] or "").strip()
      if not t_str:
        continue
      try:
        t_sec = parse_time_fn(t_str)
      except Exception:
        continue
      if t_sec is None:
        continue
      if trip_id in bounds:
        lo, hi = bounds[trip_id]
        if t_sec < lo:
          lo = t_sec
        if t_sec > hi:
          hi = t_sec
        bounds[trip_id] = (lo, hi)
      else:
        bounds[trip_id] = (t_sec, t_sec)
  return bounds

