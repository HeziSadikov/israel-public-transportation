"""
Precompute route patterns into Postgres/PostGIS using the existing PatternBuilder.

This script:
  - Loads the active GTFSFeed from the existing SQLite-based loader.
  - Uses PatternBuilder to compute RoutePattern objects per (route_id, direction_id)
    for a given service date.
  - Writes results into the PostGIS-backed `patterns` and `pattern_stops` tables.

Usage:

  python -m backend.scripts.build_patterns_postgis \\
      --database-url postgresql://user:pass@localhost:5432/israel_gtfs \\
      --date 20260308
"""

from __future__ import annotations

import argparse
from typing import Optional

import psycopg2

from backend.gtfs_loader import load_active_feed
from backend.pattern_builder import PatternBuilder, RoutePattern
from backend.db_access import get_active_feed_id, DB_URL as DEFAULT_DB_URL


def _connect(database_url: Optional[str]):
  return psycopg2.connect(database_url or DEFAULT_DB_URL)


def _upsert_patterns_for_feed(cur, feed_id: int, yyyymmdd: str) -> None:
  print(f"[patterns] Loading active GTFS feed from SQLite for date {yyyymmdd} ...", flush=True)
  feed = load_active_feed()
  patterns_builder = PatternBuilder(feed)

  # Clear existing patterns for this feed so we can rebuild deterministically.
  print(f"[patterns] Clearing existing patterns for feed_id={feed_id} ...", flush=True)
  cur.execute("DELETE FROM pattern_stops WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM patterns WHERE feed_id = %s", (feed_id,))

  routes = feed.routes
  print(f"[patterns] Building patterns for {len(routes)} routes ...", flush=True)
  processed_routes = 0
  for r in routes:
    route_id = r.get("route_id")
    if not route_id:
      continue

    # We build patterns separately for each direction_id we observe on trips,
    # plus a None case to catch routes without direction_id set.
    direction_ids = set()
    for t in feed.trips:
      if t.get("route_id") != route_id:
        continue
      d = t.get("direction_id")
      direction_ids.add(d if d not in ("", None) else None)

    if not direction_ids:
      direction_ids = {None}

    for dir_id in direction_ids:
      pats = patterns_builder.build_patterns_for_route(
        route_id=route_id,
        direction_id=dir_id,
        yyyymmdd=yyyymmdd,
        max_trips=None,
      )
      for pid, pat in pats.items():
        _insert_pattern(cur, feed_id, pat)
    processed_routes += 1
    if processed_routes % 50 == 0:
      print(f"[patterns] Processed {processed_routes}/{len(routes)} routes ...", flush=True)


def _insert_pattern(cur, feed_id: int, pat: RoutePattern) -> None:
  """
  Insert a single RoutePattern and its stops into patterns + pattern_stops.
  """
  used_shape = pat.representative_shape_id is not None

  cur.execute(
    """
    INSERT INTO patterns (
      feed_id,
      pattern_id,
      route_id,
      direction_id,
      repr_trip_id,
      repr_shape_id,
      stop_ids,
      frequency,
      used_shape
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (
      feed_id,
      pat.pattern_id,
      pat.route_id,
      int(pat.direction_id) if pat.direction_id not in (None, "") else None,
      pat.representative_trip_id,
      pat.representative_shape_id,
      pat.stop_ids,
      pat.frequency,
      used_shape,
    ),
  )

  # Insert ordered stops into pattern_stops
  stop_rows = []
  for seq, sid in enumerate(pat.stop_ids):
    stop_rows.append((feed_id, pat.pattern_id, seq, sid))

  if stop_rows:
    from psycopg2.extras import execute_values

    execute_values(
      cur,
      """
      INSERT INTO pattern_stops (feed_id, pattern_id, seq, stop_id)
      VALUES %s
      """,
      stop_rows,
      page_size=1000,
    )


def build_patterns(database_url: Optional[str], date_ymd: str) -> None:
  conn = _connect(database_url)
  conn.autocommit = False
  try:
    print(f"[patterns] Starting pattern build for date {date_ymd} using {database_url or 'DEFAULT_DB_URL'}", flush=True)
    with conn:
      with conn.cursor() as cur:
        # Use db_access logic to resolve active feed_id inside Postgres.
        feed_id = get_active_feed_id(conn)
        print(f"[patterns] Active PostGIS feed_id={feed_id}", flush=True)
        _upsert_patterns_for_feed(cur, feed_id, date_ymd)
    conn.commit()
    print("[patterns] Patterns and pattern_stops populated successfully.", flush=True)
  finally:
    conn.close()


def main() -> None:
  ap = argparse.ArgumentParser(description="Precompute route patterns into Postgres/PostGIS.")
  ap.add_argument(
    "--database-url",
    type=str,
    default=None,
    help="PostgreSQL connection URL; overrides DATABASE_URL when set.",
  )
  ap.add_argument(
    "--date",
    type=str,
    required=True,
    help="Service date (YYYYMMDD) used when selecting active trips for patterns.",
  )
  args = ap.parse_args()

  build_patterns(args.database_url, args.date)


if __name__ == "__main__":
  main()

