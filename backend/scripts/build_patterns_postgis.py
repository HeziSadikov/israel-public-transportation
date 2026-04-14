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

  # Topology / shape work only — ignore GTFS calendar when selecting trips (not for operational accuracy):
  python -m backend.scripts.build_patterns_postgis --ignore-calendar

When run as a CLI (not via Uvicorn), progress and timestamps go to stderr through
the ``app.action`` logger (see ``ensure_cli_action_logging`` in ``backend.infra.logging_utils``).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import time
from typing import Any, Dict, List, Optional, Set

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import DictCursor

from backend.infra.logging_utils import ensure_cli_action_logging, log

_ROUTE_PROGRESS_EVERY = 25
_RIDE_NETWORK_PROGRESS_EVERY = 25
from backend.domain.graph_builder import build_graph_for_pattern_from_postgis
from backend.domain.pattern_builder import PatternBuilder, RoutePattern
from backend.infra.db_access import (
  get_active_feed_id,
  DB_URL as DEFAULT_DB_URL,
  compute_route_signature,
  get_route_signature,
  upsert_route_signature,
  PatternMeta,
  get_pattern_stops_bulk,
  get_stop_times_bulk,
  get_shape_lines_bulk,
)


def _connect(database_url: Optional[str]):
  return psycopg2.connect(database_url or DEFAULT_DB_URL, cursor_factory=DictCursor)


def _merge_pattern_corridor_wkt(edge_wkts: List[str]) -> Optional[str]:
  """Merge per-edge LineStrings to one corridor geometry WKT for detour indexing."""
  if not edge_wkts:
    return None
  from shapely import wkt as _wkt
  from shapely.ops import linemerge, unary_union

  lines = []
  for txt in edge_wkts:
    if not txt:
      continue
    try:
      geom = _wkt.loads(str(txt))
    except Exception:
      continue
    if geom is None or geom.is_empty:
      continue
    lines.append(geom)
  if not lines:
    return None
  try:
    merged = linemerge(unary_union(lines))
  except Exception:
    merged = lines[0]
  if merged is None or merged.is_empty:
    return None
  return str(merged.wkt)


def _ensure_patterns_built_checksum_column(conn) -> None:
  """Idempotent DDL for databases created before patterns_built_checksum was added."""
  prev = conn.autocommit
  conn.autocommit = True
  try:
    with conn.cursor() as cur:
      cur.execute("SET lock_timeout = '5s'")
      try:
        cur.execute(
          "ALTER TABLE feed_versions ADD COLUMN IF NOT EXISTS patterns_built_checksum TEXT"
        )
        log("patterns", "phase=ensure_patterns_built_checksum_column done")
      except (pg_errors.LockNotAvailable, pg_errors.QueryCanceled) as e:
        # If another session holds DDL-sensitive locks, avoid hanging here.
        # Continue only when the column already exists.
        cur.execute(
          """
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'feed_versions'
            AND column_name = 'patterns_built_checksum'
          LIMIT 1
          """
        )
        exists = cur.fetchone() is not None
        if exists:
          log(
            "patterns",
            (
              "phase=ensure_patterns_built_checksum_column done "
              f"column_exists=true ddl_skipped_reason={type(e).__name__}"
            ),
          )
        else:
          raise
  finally:
    conn.autocommit = prev


def _trip_row_from_db(r: Any) -> dict:
  """Normalize a trips-table row to the dict shape PatternBuilder expects."""
  dir_val = r["direction_id"]
  return {
    "trip_id": r["trip_id"],
    "route_id": r["route_id"],
    "service_id": r["service_id"],
    "direction_id": None if dir_val is None or dir_val == "" else str(dir_val),
    "shape_id": r["shape_id"],
    "trip_headsign": r["headsign"],
    "block_id": r["block_id"],
  }


def _direction_ids_by_route(cur, feed_id: int) -> Dict[str, Set[Optional[str]]]:
  """
  DISTINCT (route_id, direction_id) from trips — small vs full trips fetch.
  Normalizes direction_id like the previous in-Python scan (None/'' -> None).
  """
  cur.execute(
    """
    SELECT DISTINCT route_id, direction_id
    FROM trips
    WHERE feed_id = %s
    """,
    (feed_id,),
  )
  out: Dict[str, Set[Optional[str]]] = {}
  for row in cur.fetchall():
    rid = row["route_id"]
    if not rid:
      continue
    dir_val = row["direction_id"]
    d: Optional[str]
    if dir_val is None or dir_val == "":
      d = None
    else:
      d = str(dir_val)
    out.setdefault(rid, set()).add(d)
  return out


def _trips_for_routes(cur, feed_id: int, route_ids: List[str]) -> List[dict]:
  """Load trips only for the given route_ids (one query per batch)."""
  if not route_ids:
    return []
  cur.execute(
    """
    SELECT trip_id, route_id, service_id, direction_id, shape_id, headsign, block_id
    FROM trips
    WHERE feed_id = %s AND route_id = ANY(%s)
    """,
    (feed_id, route_ids),
  )
  return [_trip_row_from_db(r) for r in cur.fetchall()]


def _load_feed_core_from_postgis(cur, feed_id: int) -> GTFSFeed:
  """
  Load agencies, routes, stops, calendar, calendar_dates — no trips (loaded per route batch).
  GTFSFeed-compatible stub for PatternBuilder and ServiceCalendar.
  """
  log("patterns", f"feed load: querying agencies (feed_id={feed_id}) ...")
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
  log("patterns", f"feed load: loaded {len(agencies)} agencies")

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
  log("patterns", f"feed load: loaded {len(routes)} routes")

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
  log("patterns", f"feed load: loaded {len(stops)} stops")
  log(
    "patterns",
    "feed load: skipping full trips table — trips + stop_times load per route batch",
  )
  trips: List[dict] = []

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
  log("patterns", f"feed load: loaded {len(calendar)} calendar rows")

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
  log("patterns", f"feed load: loaded {len(calendar_dates)} calendar_dates rows")

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


def _upsert_patterns_for_feed(
    cur,
    feed_id: int,
    old_feed_id: int | None,
    yyyymmdd: str,
    *,
    ignore_calendar: bool = False,
    route_batch_size: int = 200,
) -> None:
  route_batch_size = max(1, int(route_batch_size))
  log("patterns", f"Loading active GTFS feed from PostGIS for date {yyyymmdd} ...")
  feed = _load_feed_core_from_postgis(cur, feed_id)
  patterns_builder = PatternBuilder(feed)

  conn = cur.connection
  log("patterns", "Loading distinct (route_id, direction_id) from trips ...")
  direction_map = _direction_ids_by_route(cur, feed_id)
  log("patterns", f"Distinct routes-with-trips={len(direction_map)} (direction keys only, not full trips).")

  # Clear existing patterns for this feed so we can rebuild deterministically.
  log("patterns", f"Clearing existing patterns for feed_id={feed_id} ...")
  cur.execute("DELETE FROM pattern_stops WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM patterns WHERE feed_id = %s", (feed_id,))
  # Clear precomputed ride network derived tables.
  cur.execute("DELETE FROM pattern_nodes WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM pattern_edges WHERE feed_id = %s", (feed_id,))
  cur.execute("DELETE FROM pattern_detour_index WHERE feed_id = %s", (feed_id,))

  routes = feed.routes
  n_routes = len(routes)
  log("patterns", f"Building patterns for {n_routes} routes (batch_size={route_batch_size}) ...")
  log(
    "patterns",
    f"This phase often takes many minutes on a full feed; progress every {_ROUTE_PROGRESS_EVERY} routes.",
  )
  processed_routes = 0
  for batch_start in range(0, n_routes, route_batch_size):
    chunk = routes[batch_start : batch_start + route_batch_size]
    route_ids = [r["route_id"] for r in chunk if r.get("route_id")]
    chunk_trips = _trips_for_routes(cur, feed_id, route_ids)
    trip_ids = [t["trip_id"] for t in chunk_trips]
    stop_times_chunk = get_stop_times_bulk(feed_id, trip_ids, conn)
    feed.trips = chunk_trips
    log(
      "patterns",
      f"route batch [{batch_start}:{batch_start + len(chunk)}/{n_routes}] "
      f"trips={len(chunk_trips)} stop_times_trips={len(stop_times_chunk)}",
    )

    for r in chunk:
      route_id = r.get("route_id")
      if not route_id:
        continue

      direction_ids = direction_map.get(route_id)
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
          if ignore_calendar:
            pats = patterns_builder.build_patterns_for_route(
              route_id=route_id,
              direction_id=dir_id,
              yyyymmdd=yyyymmdd,
              max_trips=None,
              use_all_trips=True,
              stop_times_preloaded=stop_times_chunk,
            )
          else:
            pats = patterns_builder.build_patterns_for_route(
              route_id=route_id,
              direction_id=dir_id,
              yyyymmdd=yyyymmdd,
              max_trips=None,
              stop_times_preloaded=stop_times_chunk,
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
                  stop_times_preloaded=stop_times_chunk,
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
                stop_times_preloaded=stop_times_chunk,
              )
          for pid, pat in pats.items():
            _insert_pattern(cur, feed_id, pat)

        # Record the new signature for this route/direction in the new feed.
        upsert_route_signature(feed_id, route_id, dir_id, sig_new, conn)

      processed_routes += 1
      if processed_routes % _ROUTE_PROGRESS_EVERY == 0:
        log("patterns", f"Processed {processed_routes}/{n_routes} routes ...")

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
    edge_wkts: List[str] = []
    for u, v, ed in res.graph.edges(data=True):
      eg = res.edge_geometries.get((u, v))
      if eg is None or eg.linestring is None:
        continue

      travel_time_s = ed.get("travel_time_s", ed.get("weight"))
      distance_m = ed.get("distance_m", 0.0)
      ls_wkt = eg.linestring.wkt
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
          ls_wkt,
        )
      )
      edge_wkts.append(ls_wkt)

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

    corridor_wkt = _merge_pattern_corridor_wkt(edge_wkts)
    first_stop_id = stops[0].stop_id if stops else None
    last_stop_id = stops[-1].stop_id if stops else None
    cur.execute(
      """
      INSERT INTO pattern_detour_index (
        feed_id,
        pattern_id,
        route_id,
        direction_id,
        first_stop_id,
        last_stop_id,
        stop_count,
        edge_count,
        length_m,
        corridor_geom
      )
      VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        CASE
          WHEN %s IS NULL THEN 0.0
          ELSE ST_Length(ST_GeomFromText(%s, 4326)::geography)
        END,
        CASE
          WHEN %s IS NULL THEN NULL
          ELSE ST_GeomFromText(%s, 4326)
        END
      )
      ON CONFLICT (feed_id, pattern_id) DO UPDATE
      SET
        route_id = EXCLUDED.route_id,
        direction_id = EXCLUDED.direction_id,
        first_stop_id = EXCLUDED.first_stop_id,
        last_stop_id = EXCLUDED.last_stop_id,
        stop_count = EXCLUDED.stop_count,
        edge_count = EXCLUDED.edge_count,
        length_m = EXCLUDED.length_m,
        corridor_geom = EXCLUDED.corridor_geom,
        updated_at = NOW()
      """,
      (
        feed_id,
        pid,
        row["route_id"],
        row.get("direction_id"),
        first_stop_id,
        last_stop_id,
        len(stops),
        len(edge_rows),
        corridor_wkt,
        corridor_wkt,
        corridor_wkt,
        corridor_wkt,
      ),
    )

    if (idx + 1) % _RIDE_NETWORK_PROGRESS_EVERY == 0:
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
  ignore_calendar: bool = False,
  route_batch_size: int = 200,
) -> str:
  """
  Populate patterns / pattern_stops (and ride network) for the active feed.
  If date_ymd is None, picks a reference date from calendar coverage (see pick_default_pattern_build_date).
  Skips work when feed_versions.checksum matches patterns_built_checksum (same zip as last pattern build),
  unless force=True.
  If ignore_calendar=True, pattern selection uses all trips (use_all_trips); date_ymd is still used for
  ride-network graph metadata — not for operational timetable accuracy.
  route_batch_size controls how many routes share one trips + stop_times bulk load from PostGIS.
  Returns the YYYYMMDD reference date (for logging / metadata).
  """
  ensure_cli_action_logging()
  log(
    "patterns",
    "build_patterns_postgis: connecting to PostGIS and resolving active feed (checksum gate) ...",
  )
  log("patterns", "phase=checksum_gate start")
  t_gate = time.perf_counter()
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
        log(
          "patterns",
          f"phase=checksum_gate done skip=true feed_id={feed_id} elapsed_s={time.perf_counter() - t_gate:.2f}",
        )
        return ref_date

    conn.rollback()
    log(
      "patterns",
      f"phase=checksum_gate done skip=false feed_id={feed_id} elapsed_s={time.perf_counter() - t_gate:.2f}",
    )

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
        f"Starting pattern build for date {date_ymd} using {database_url or 'DEFAULT_DB_URL'}"
        f"{' (ignore_calendar=True)' if ignore_calendar else ''}",
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
        _upsert_patterns_for_feed(
          cur,
          feed_id,
          old_feed_id,
          date_ymd,
          ignore_calendar=ignore_calendar,
          route_batch_size=route_batch_size,
        )
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
  ensure_cli_action_logging()
  log("patterns", "build_patterns_postgis CLI starting ...")
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
  ap.add_argument(
    "--ignore-calendar",
    action="store_true",
    help=(
      "Build patterns using all trips per route/direction (skip GTFS service-day filter). "
      "For topology/shape tooling only, not operational schedules."
    ),
  )
  ap.add_argument(
    "--route-batch-size",
    type=int,
    default=200,
    metavar="N",
    help=(
      "Number of routes per batch when loading trips/stop_times from PostGIS "
      "(lower if trip_id = ANY(...) hits size limits). Default: 200."
    ),
  )
  args = ap.parse_args()
  if args.route_batch_size < 1:
    ap.error("--route-batch-size must be >= 1")

  build_patterns(
    args.database_url,
    args.date,
    force=args.force,
    ignore_calendar=args.ignore_calendar,
    route_batch_size=args.route_batch_size,
  )


if __name__ == "__main__":
  main()

