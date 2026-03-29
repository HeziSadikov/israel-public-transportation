"""
Precompute route patterns into Postgres/PostGIS using the existing PatternBuilder.

This script:
  - Loads GTFS tables for the active feed directly from Postgres/PostGIS.
  - Constructs an in-memory GTFSFeed object from those tables.
  - Uses PatternBuilder to compute RoutePattern objects per (route_id, direction_id)
    for a given service date.
  - Writes results into the PostGIS-backed `patterns` and `pattern_stops` tables.

Usage:

  python -m backend.scripts.build_patterns_postgis

  python -m backend.scripts.build_patterns_postgis \\
      --database-url postgresql://user:pass@localhost:5432/israel_gtfs \\
      --date 20260308
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from psycopg2.extras import DictCursor

from backend.logging_utils import log
from backend.graph_builder import build_graph_for_pattern_from_postgis
from backend.pattern_builder import PatternBuilder, RoutePattern
from backend.db_access import (
  get_active_feed_id,
  DB_URL as DEFAULT_DB_URL,
  compute_route_signature,
  get_route_signature,
  upsert_route_signature,
  get_all_stop_times_for_feed,
  PatternMeta,
  get_pattern_stops_bulk,
  get_stop_times_bulk,
  get_shape_lines_bulk,
)


def _connect(database_url: Optional[str]):
  return psycopg2.connect(database_url or DEFAULT_DB_URL, cursor_factory=DictCursor)


def _ensure_patterns_built_checksum_column(conn) -> None:
  """Idempotent DDL for databases created before patterns_built_checksum was added."""
  prev = conn.autocommit
  conn.autocommit = True
  try:
    with conn.cursor() as cur:
      cur.execute(
        "ALTER TABLE feed_versions ADD COLUMN IF NOT EXISTS patterns_built_checksum TEXT"
      )
  finally:
    conn.autocommit = prev


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

  # For pattern building we don't need full shapes; keep shape_id on trips and let
  # graph building fetch shapes lazily from PostGIS when needed.
  shapes = []

  # stop_times are accessed via db_access.get_stop_times_* helpers in PatternBuilder/GraphBuilder,
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
  log("patterns", f"Loading active GTFS feed from PostGIS for date {yyyymmdd} ...")
  feed = _load_feed_from_postgis(cur, feed_id)
  patterns_builder = PatternBuilder(feed)

  # Load all stop_times for the feed once (one query) so we avoid per-trip DB calls.
  conn = cur.connection
  log("patterns", f"Loading all stop_times for feed_id={feed_id} (one query) ...")
  stop_times_by_trip = get_all_stop_times_for_feed(feed_id, conn)
  log("patterns", f"Loaded stop_times for {len(stop_times_by_trip)} trips.")

  # Clear existing patterns for this feed so we can rebuild deterministically.
  log("patterns", f"Clearing existing patterns for feed_id={feed_id} ...")
  cur.execute("DELETE FROM pattern_stops WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM patterns WHERE feed_id = %s", (feed_id,))
  # Clear precomputed ride network derived tables.
  cur.execute("DELETE FROM pattern_nodes WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM pattern_edges WHERE feed_id = %s", (feed_id,))

  routes = feed.routes
  log("patterns", f"Building patterns for {len(routes)} routes ...")
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
      # Compute per-route/direction signature for the new feed (reuse conn).
      sig_new = compute_route_signature(route_id, dir_id, conn)

      reused = False
      if old_feed_id is not None:
        sig_old = get_route_signature(old_feed_id, route_id, dir_id, conn)
        if sig_old is not None and sig_old == sig_new:
          # Route unchanged between feeds: copy existing patterns and pattern_stops
          # from old_feed_id to the new feed.
          log(
            "patterns",
            f"Reusing patterns for route_id={route_id!r}, direction_id={dir_id!r} from old_feed_id={old_feed_id}",
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
        # Build fresh patterns for this route/direction. If the build date has no
        # service for this route (e.g. weekday vs weekend), try the next 7 days
        # so every route that runs on any day gets a pattern.
        pats = patterns_builder.build_patterns_for_route(
          route_id=route_id,
          direction_id=dir_id,
          yyyymmdd=yyyymmdd,
          max_trips=None,
          stop_times_preloaded=stop_times_by_trip,
        )
        if not pats:
          base = datetime.strptime(yyyymmdd, "%Y%m%d").date()
          for d in range(1, 8):
            try_date = (base + timedelta(days=d)).strftime("%Y%m%d")
            pats = patterns_builder.build_patterns_for_route(
              route_id=route_id,
              direction_id=dir_id,
              yyyymmdd=try_date,
              max_trips=None,
              stop_times_preloaded=stop_times_by_trip,
            )
            if pats:
              break
        if not pats:
          # Still no pattern (e.g. route only runs outside the 8-day window). Use all trips.
          pats = patterns_builder.build_patterns_for_route(
            route_id=route_id,
            direction_id=dir_id,
            yyyymmdd=yyyymmdd,
            max_trips=None,
            use_all_trips=True,
            stop_times_preloaded=stop_times_by_trip,
          )
        for pid, pat in pats.items():
          _insert_pattern(cur, feed_id, pat)

      # Record the new signature for this route/direction in the new feed.
      upsert_route_signature(feed_id, route_id, dir_id, sig_new, conn)
    processed_routes += 1
    if processed_routes % 50 == 0:
      log("patterns", f"Processed {processed_routes}/{len(routes)} routes ...")

  _build_pattern_ride_network(cur=cur, feed_id=feed_id, yyyymmdd=yyyymmdd)


def _build_pattern_ride_network(cur, feed_id: int, yyyymmdd: str) -> None:
  """
  Populate `pattern_nodes` + `pattern_edges` for all patterns in the given feed_id.

  Phase 1 strategy: derive ride nodes/edges from the authoritative `patterns` +
  `pattern_stops` tables by calling `build_graph_for_pattern_from_postgis(...)`
  for each pattern, then inserting the resulting nx graph into PostGIS.
  """
  from psycopg2.extras import execute_values

  log("patterns", f"Building precomputed ride network for feed_id={feed_id} ...")

  # Load all patterns (global list) then bulk load their dependencies.
  cur.execute(
    """
    SELECT
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
    """,
    (feed_id,),
  )
  pattern_rows = cur.fetchall()
  if not pattern_rows:
    log("patterns", f"No patterns found for feed_id={feed_id}; ride network remains empty.")
    return

  pattern_ids: list[str] = [r["pattern_id"] for r in pattern_rows]
  repr_trip_ids: list[str] = sorted({r.get("repr_trip_id") for r in pattern_rows if r.get("repr_trip_id")})
  repr_shape_ids: list[str] = sorted({r.get("repr_shape_id") for r in pattern_rows if r.get("repr_shape_id")})

  conn = cur.connection
  pattern_stops_by_id = get_pattern_stops_bulk(feed_id=feed_id, pattern_ids=pattern_ids, conn=conn)
  stop_times_by_trip = get_stop_times_bulk(feed_id=feed_id, trip_ids=repr_trip_ids, conn=conn)
  shape_lines_by_id = get_shape_lines_bulk(feed_id=feed_id, shape_ids=repr_shape_ids, conn=conn)

  nodes_inserted = 0
  edges_inserted = 0

  # Insert nodes/edges per pattern to keep memory bounded.
  for idx, row in enumerate(pattern_rows):
    pid = row["pattern_id"]

    stops = pattern_stops_by_id.get(pid, [])
    if len(stops) < 2:
      continue

    repr_trip_id = row.get("repr_trip_id")
    repr_shape_id = row.get("repr_shape_id")

    pattern_meta = PatternMeta(
      pattern_id=pid,
      route_id=row["route_id"],
      direction_id=row.get("direction_id"),
      repr_trip_id=repr_trip_id,
      repr_shape_id=repr_shape_id,
      stop_ids=[s.stop_id for s in stops],
      frequency=int(row.get("frequency") or 0),
      used_shape=bool(row.get("used_shape")),
    )

    shape_line = shape_lines_by_id.get(repr_shape_id) if repr_shape_id else None
    stop_times = stop_times_by_trip.get(repr_trip_id) if repr_trip_id else None

    # Build the authoritative local ride graph for this pattern.
    res = build_graph_for_pattern_from_postgis(
      pattern_meta=pattern_meta,
      date_ymd=yyyymmdd,
      pattern_stops=stops,
      shape_line=shape_line,
      stop_times=stop_times,
    )

    # -------------------------
    # Insert pattern_nodes
    # -------------------------
    node_rows = []
    for node_id, nd in res.graph.nodes(data=True):
      lat = float(nd["lat"])
      lon = float(nd["lon"])
      out_heading = nd.get("out_heading_deg")
      out_heading_f = None if out_heading is None else float(out_heading)
      freq = nd.get("frequency")
      freq_i = None if freq is None else int(freq)

      direction_val = nd.get("direction_id")
      direction_int = None
      if direction_val not in (None, ""):
        try:
          direction_int = int(direction_val)
        except (TypeError, ValueError):
          direction_int = None

      node_rows.append(
        (
          feed_id,
          node_id,
          pid,
          nd["route_id"],
          direction_int,
          nd["stop_id"],
          int(nd["stop_sequence"]),
          lat,
          lon,
          out_heading_f,
          freq_i,
          # store WKT and let PostGIS cast to geography in the insert template
          f"POINT({lon} {lat})",
        )
      )

    if node_rows:
      execute_values(
        cur,
        """
        INSERT INTO pattern_nodes (
          feed_id,
          node_id,
          pattern_id,
          route_id,
          direction_id,
          stop_id,
          stop_sequence,
          lat,
          lon,
          out_heading_deg,
          frequency,
          geom
        )
        VALUES %s
        """,
        node_rows,
        page_size=5000,
        template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s,4326)::geography)",
      )
      nodes_inserted += len(node_rows)

    # -------------------------
    # Insert pattern_edges
    # -------------------------
    edge_rows = []
    for u, v, ed in res.graph.edges(data=True):
      eg = res.edge_geometries.get((u, v))
      if eg is None or eg.linestring is None:
        continue

      travel_time_s = ed.get("travel_time_s", ed.get("weight"))
      distance_m = ed.get("distance_m", 0.0)
      edge_rows.append(
        (
          feed_id,
          pid,
          u,
          v,
          eg.from_stop_id,
          eg.to_stop_id,
          float(travel_time_s) if travel_time_s is not None else None,
          float(distance_m) if distance_m is not None else None,
          eg.linestring.wkt,
        )
      )

    if edge_rows:
      execute_values(
        cur,
        """
        INSERT INTO pattern_edges (
          feed_id,
          pattern_id,
          from_node_id,
          to_node_id,
          from_stop_id,
          to_stop_id,
          travel_time_s,
          distance_m,
          geom
        )
        VALUES %s
        """,
        edge_rows,
        page_size=5000,
        template="(%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s,4326))",
      )
      edges_inserted += len(edge_rows)

    if (idx + 1) % 50 == 0:
      log("patterns", f"Ride network build progress: {idx + 1}/{len(pattern_rows)} patterns ...")

  log(
    "patterns",
    f"Ride network build finished for feed_id={feed_id}. nodes={nodes_inserted}, edges={edges_inserted}",
  )


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


def pick_default_pattern_build_date(cur, feed_id: int) -> str:
  """
  Choose a YYYYMMDD inside the feed's published service window so scripts need no --date.
  Prefer UTC today when it falls in [min(start_date), max(end_date)] from calendar;
  otherwise clamp to the nearest bound. Falls back to calendar_dates bounds, then today.
  """
  from datetime import datetime, timezone

  today_s = datetime.now(timezone.utc).strftime("%Y%m%d")
  today_i = int(today_s)

  cur.execute(
    """
    SELECT MIN(start_date)::bigint AS mn, MAX(end_date)::bigint AS mx
    FROM calendar
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  row = cur.fetchone()
  mn = int(row["mn"]) if row and row["mn"] is not None else None
  mx = int(row["mx"]) if row and row["mx"] is not None else None

  if mn is not None and mx is not None:
    if mn <= today_i <= mx:
      return today_s
    if today_i < mn:
      return f"{mn:08d}"
    return f"{mx:08d}"

  cur.execute(
    """
    SELECT MIN(date)::bigint AS mn, MAX(date)::bigint AS mx
    FROM calendar_dates
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  row2 = cur.fetchone()
  mn2 = int(row2["mn"]) if row2 and row2["mn"] is not None else None
  mx2 = int(row2["mx"]) if row2 and row2["mx"] is not None else None

  if mn2 is not None and mx2 is not None:
    if mn2 <= today_i <= mx2:
      return today_s
    if today_i < mn2:
      return f"{mn2:08d}"
    return f"{mx2:08d}"

  return today_s


def build_patterns(
  database_url: Optional[str],
  date_ymd: Optional[str] = None,
  *,
  force: bool = False,
) -> str:
  """
  Populate patterns / pattern_stops (and ride network) for the active feed.
  If date_ymd is None, picks a reference date from calendar coverage (see pick_default_pattern_build_date).
  Skips work when feed_versions.checksum matches patterns_built_checksum (same zip as last pattern build),
  unless force=True.
  Returns the YYYYMMDD reference date (for logging / metadata).
  """
  conn = _connect(database_url)
  conn.autocommit = False
  try:
    _ensure_patterns_built_checksum_column(conn)
    feed_id = get_active_feed_id(conn)

    if not force:
      with conn.cursor() as cur:
        cur.execute(
          """
          SELECT checksum, patterns_built_checksum
          FROM feed_versions
          WHERE id = %s
          """,
          (feed_id,),
        )
        fv = cur.fetchone()
      zip_ck = fv["checksum"] if fv else None
      pat_ck = fv["patterns_built_checksum"] if fv else None
      if zip_ck and pat_ck and zip_ck == pat_ck:
        with conn.cursor() as cur:
          ref_date = (
            date_ymd
            if date_ymd is not None
            else pick_default_pattern_build_date(cur, feed_id)
          )
        conn.rollback()
        log(
          "patterns",
          f"Skip: patterns already built for this feed zip (feed_id={feed_id})",
        )
        return ref_date

    conn.rollback()

    with conn:
      with conn.cursor() as cur:
        if date_ymd is None:
          date_ymd = pick_default_pattern_build_date(cur, feed_id)
          log(
            "patterns",
            f"No --date given; using feed-derived reference date {date_ymd}",
          )
      log(
        "patterns",
        f"Starting pattern build for date {date_ymd} using {database_url or 'DEFAULT_DB_URL'}",
      )
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
      log("patterns", f"Active PostGIS feed_id={feed_id}, old_feed_id={old_feed_id}")
      with conn.cursor() as cur:
        _upsert_patterns_for_feed(cur, feed_id, old_feed_id, date_ymd)
        cur.execute(
          """
          UPDATE feed_versions
          SET patterns_built_checksum = checksum
          WHERE id = %s
          """,
          (feed_id,),
        )
    log("patterns", "Patterns and pattern_stops populated successfully.")
    return date_ymd
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
    default=None,
    help=(
      "Optional service date YYYYMMDD. If omitted, a date inside the feed calendar "
      "window is chosen automatically (UTC today when in range, else clamped)."
    ),
  )
  ap.add_argument(
    "--force",
    action="store_true",
    help="Rebuild patterns even when they already match the active feed zip checksum.",
  )
  args = ap.parse_args()

  build_patterns(args.database_url, args.date, force=args.force)


if __name__ == "__main__":
  main()

