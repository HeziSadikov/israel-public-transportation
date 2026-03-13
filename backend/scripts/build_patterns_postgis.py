"""
Precompute route patterns into Postgres/PostGIS using the existing PatternBuilder.

This script:
  - Loads GTFS tables for the active feed directly from Postgres/PostGIS.
  - Constructs an in-memory GTFSFeed object from those tables.
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
from psycopg2.extras import DictCursor

from backend.pattern_builder import PatternBuilder, RoutePattern
from backend.db_access import (
  get_active_feed_id,
  DB_URL as DEFAULT_DB_URL,
  compute_route_signature,
  get_route_signature,
  upsert_route_signature,
)


def _connect(database_url: Optional[str]):
  return psycopg2.connect(database_url or DEFAULT_DB_URL, cursor_factory=DictCursor)


def _load_feed_from_postgis(cur, feed_id: int) -> GTFSFeed:
  """
  Load core GTFS tables for the given feed_id from PostGIS and construct a
  GTFSFeed object compatible with PatternBuilder and ServiceCalendar.
  """
  # Agencies
  cur.execute(
    """
    SELECT agency_id, name, url, timezone, lang, phone
    FROM agencies
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  agencies = [
    {
      "agency_id": r["agency_id"],
      "agency_name": r["name"],
      "agency_url": r["url"],
      "agency_timezone": r["timezone"],
      "agency_lang": r["lang"],
      "agency_phone": r["phone"],
    }
    for r in cur.fetchall()
  ]

  # Routes
  cur.execute(
    """
    SELECT route_id, agency_id, short_name, long_name, route_type, route_color, route_text_color
    FROM routes
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  routes = [
    {
      "route_id": r["route_id"],
      "agency_id": r["agency_id"],
      "route_short_name": r["short_name"],
      "route_long_name": r["long_name"],
      "route_type": r["route_type"],
      "route_color": r["route_color"],
      "route_text_color": r["route_text_color"],
    }
    for r in cur.fetchall()
  ]

  # Stops
  cur.execute(
    """
    SELECT stop_id, name, lat, lon, zone_id, parent_station
    FROM stops
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  stops = [
    {
      "stop_id": r["stop_id"],
      "stop_name": r["name"],
      "stop_lat": r["lat"],
      "stop_lon": r["lon"],
      "zone_id": r["zone_id"],
      "parent_station": r["parent_station"],
    }
    for r in cur.fetchall()
  ]

  # Trips
  cur.execute(
    """
    SELECT trip_id, route_id, service_id, direction_id, shape_id, headsign, block_id
    FROM trips
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  trips = []
  for r in cur.fetchall():
    dir_val = r["direction_id"]
    trips.append(
      {
        "trip_id": r["trip_id"],
        "route_id": r["route_id"],
        "service_id": r["service_id"],
        # PatternBuilder expects direction_id as string or None
        "direction_id": None if dir_val is None else str(dir_val),
        "shape_id": r["shape_id"],
        "trip_headsign": r["headsign"],
        "block_id": r["block_id"],
      }
    )

  # Calendar
  cur.execute(
    """
    SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday,
           sunday, start_date, end_date
    FROM calendar
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  calendar = [
    {
      "service_id": r["service_id"],
      # ServiceCalendar expects "0"/"1" strings and YYYYMMDD strings
      "monday": str(r["monday"] or 0),
      "tuesday": str(r["tuesday"] or 0),
      "wednesday": str(r["wednesday"] or 0),
      "thursday": str(r["thursday"] or 0),
      "friday": str(r["friday"] or 0),
      "saturday": str(r["saturday"] or 0),
      "sunday": str(r["sunday"] or 0),
      "start_date": f"{int(r['start_date']):08d}" if r["start_date"] is not None else "00000000",
      "end_date": f"{int(r['end_date']):08d}" if r["end_date"] is not None else "00000000",
    }
    for r in cur.fetchall()
  ]

  # Calendar dates
  cur.execute(
    """
    SELECT service_id, date, exception_type
    FROM calendar_dates
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  calendar_dates = [
    {
      "service_id": r["service_id"],
      "date": f"{int(r['date']):08d}",
      "exception_type": str(r["exception_type"]),
    }
    for r in cur.fetchall()
  ]

  # Shapes
  cur.execute(
    """
    SELECT shape_id, seq, lat, lon, dist_traveled
    FROM shapes
    WHERE feed_id = %s
    ORDER BY shape_id, seq
    """,
    (feed_id,),
  )
  shapes = [
    {
      "shape_id": r["shape_id"],
      "shape_pt_sequence": r["seq"],
      "shape_pt_lat": r["lat"],
      "shape_pt_lon": r["lon"],
      "shape_dist_traveled": r["dist_traveled"],
    }
    for r in cur.fetchall()
  ]

  # stop_times are accessed via db_access.get_stop_times_for_trip in PatternBuilder/GraphBuilder,
  # so we do not load them into GTFSFeed.stop_times here.
  # Build a simple dict-like feed object compatible with PatternBuilder/ServiceCalendar.
  feed = type("FeedStub", (), {})()
  feed.version_id = f"postgis-{feed_id}"
  feed.routes = routes
  feed.agencies = agencies
  feed.trips = trips
  feed.stop_times = []
  feed.stops = stops
  feed.calendar_dates = calendar_dates
  feed.calendar = calendar
  feed.shapes = shapes
  return feed


def _upsert_patterns_for_feed(cur, feed_id: int, old_feed_id: int | None, yyyymmdd: str) -> None:
  print(f"[patterns] Loading active GTFS feed from PostGIS for date {yyyymmdd} ...", flush=True)
  feed = _load_feed_from_postgis(cur, feed_id)
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
      # Compute per-route/direction signature for the new feed.
      sig_new = compute_route_signature(route_id, dir_id)

      reused = False
      if old_feed_id is not None:
        sig_old = get_route_signature(old_feed_id, route_id, dir_id)
        if sig_old is not None and sig_old == sig_new:
          # Route unchanged between feeds: copy existing patterns and pattern_stops
          # from old_feed_id to the new feed.
          print(
            f"[patterns] Reusing patterns for route_id={route_id!r}, direction_id={dir_id!r} "
            f"from old_feed_id={old_feed_id}",
            flush=True,
          )
          # Copy patterns
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
            SELECT
              %s AS feed_id,
              pattern_id,
              route_id,
              direction_id,
              repr_trip_id,
              repr_shape_id,
              stop_ids,
              frequency,
              used_shape
            FROM patterns
            WHERE feed_id = %s
              AND route_id = %s
              AND COALESCE(direction_id, -1) = COALESCE(%s::int, -1)
            """,
            (feed_id, old_feed_id, route_id, int(dir_id) if dir_id is not None else None),
          )
          # Copy pattern_stops for the copied patterns
          cur.execute(
            """
            INSERT INTO pattern_stops (
              feed_id,
              pattern_id,
              seq,
              stop_id
            )
            SELECT
              %s AS feed_id,
              ps.pattern_id,
              ps.seq,
              ps.stop_id
            FROM pattern_stops ps
            JOIN patterns p
              ON p.feed_id = ps.feed_id AND p.pattern_id = ps.pattern_id
            WHERE ps.feed_id = %s
              AND p.feed_id = %s
              AND p.route_id = %s
              AND COALESCE(p.direction_id, -1) = COALESCE(%s::int, -1)
            """,
            (feed_id, old_feed_id, old_feed_id, route_id, int(dir_id) if dir_id is not None else None),
          )
          reused = True

      if not reused:
        # Build fresh patterns for this route/direction.
        pats = patterns_builder.build_patterns_for_route(
          route_id=route_id,
          direction_id=dir_id,
          yyyymmdd=yyyymmdd,
          max_trips=None,
        )
        for pid, pat in pats.items():
          _insert_pattern(cur, feed_id, pat)

      # Record the new signature for this route/direction in the new feed.
      upsert_route_signature(feed_id, route_id, dir_id, sig_new)
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
      # Use db_access logic to resolve active feed_id inside Postgres.
      feed_id = get_active_feed_id(conn)
      # Find the most recent previous feed (if any) to reuse patterns from.
      with conn.cursor() as cur:
        cur.execute(
          """
          SELECT id
          FROM feed_versions
          WHERE id <> %s
          ORDER BY fetched_at DESC
          LIMIT 1
          """,
          (feed_id,),
        )
        row = cur.fetchone()
        old_feed_id = int(row[0]) if row else None
      print(f"[patterns] Active PostGIS feed_id={feed_id}, old_feed_id={old_feed_id}", flush=True)
      with conn.cursor() as cur:
        _upsert_patterns_for_feed(cur, feed_id, old_feed_id, date_ymd)
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

