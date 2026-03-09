from __future__ import annotations

"""
Precompute and persist per-route graphs for faster /graph/build.

Usage (from project root):

  python -m scripts.precompute_graphs --date 20260301 --max-routes 200

This will:
- Load the active GTFS feed.
- Scan stop_times.txt **once** for the given service date to build stop sequences
  for all active trips.
- For each route_id, derive its most frequent stop pattern and build the graph once.
- Store the result in the same on-disk cache that /graph/build uses.

Subsequent /graph/build calls for the same (feed_version, route_id, direction_id, date, pretty_osm)
will then be instant (loaded from disk) instead of rebuilding from GTFS.
"""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import hashlib
import multiprocessing
import pickle
from typing import Dict, List, Optional, Tuple

from backend.gtfs_loader import load_active_feed
from backend.graph_builder import GraphBuilder
from backend.osm_pretty import map_match_pattern
from backend.config import GRAPH_CACHE
from backend.service_calendar import ServiceCalendar
from backend.pattern_builder import RoutePattern
from backend.sqlite_db import (
    get_conn,
    save_route_graph_blob,
    save_pattern_record,
    save_pattern_stops,
    save_route_graph_v2,
)
import sqlite3

# Reuse the same cache key + file mapping as the FastAPI app.
from app import _graph_cache_key, _graph_cache_file_path  # type: ignore


def _build_trip_indexes_for_date(date_str: str):
    """
    Scan trips and stop_times **once** for the given service date and return:
    - trips_meta: trip_id -> (route_id, direction_id, service_id, shape_id)
    - route_trips: route_id -> list of trip_ids
    - trip_stops: trip_id -> ordered list of stop_ids
    """
    # Build active_services using calendar and calendar_dates from SQLite.
    conn = get_conn()
    calendar_rows = [dict(r) for r in conn.execute("SELECT * FROM calendar").fetchall()]
    try:
        calendar_dates_rows = [dict(r) for r in conn.execute("SELECT * FROM calendar_dates").fetchall()]
    except sqlite3.OperationalError:
        # Some feeds don't have calendar_dates.txt; treat as empty.
        calendar_dates_rows = []

    class _FeedStub:
        calendar = calendar_rows
        calendar_dates = calendar_dates_rows

    calendar = ServiceCalendar(_FeedStub())  # type: ignore[arg-type]
    active_services = calendar.active_service_ids_for_date(date_str)
    if not active_services:
        return {}, {}, {}

    trips_meta: Dict[str, Dict[str, Optional[str]]] = {}
    route_trips: Dict[str, List[str]] = defaultdict(list)

    # Filter trips by active services using SQLite trips table.
    for t in (dict(r) for r in conn.execute("SELECT * FROM trips")):
        service_id = t.get("service_id")
        if service_id not in active_services:
            continue
        trip_id = t.get("trip_id")
        route_id = t.get("route_id")
        if not trip_id or not route_id:
            continue
        meta = {
            "route_id": route_id,
            "direction_id": t.get("direction_id"),
            "service_id": service_id,
            "shape_id": t.get("shape_id") or None,
        }
        trips_meta[trip_id] = meta
        route_trips[route_id].append(trip_id)

    # Build stop sequences for all active trips in one pass over stop_times table
    trip_stops_raw: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    if trips_meta:
        for st in (dict(r) for r in conn.execute("SELECT trip_id, stop_id, stop_sequence FROM stop_times")):
            trip_id = st.get("trip_id")
            if trip_id not in trips_meta:
                continue
            stop_id = st.get("stop_id")
            seq_str = st.get("stop_sequence")
            if not stop_id or not seq_str:
                continue
            try:
                seq = int(seq_str)
            except ValueError:
                continue
            trip_stops_raw[trip_id].append((seq, stop_id))

    trip_stops: Dict[str, List[str]] = {}
    for trip_id, seq_list in trip_stops_raw.items():
        if not seq_list:
            continue
        seq_list.sort(key=lambda x: x[0])
        trip_stops[trip_id] = [sid for _, sid in seq_list]

    return trips_meta, route_trips, trip_stops


def _build_patterns_for_route_from_indexes(
    route_id: str,
    trips_meta: Dict[str, Dict[str, Optional[str]]],
    route_trips: Dict[str, List[str]],
    trip_stops: Dict[str, List[str]],
) -> Dict[str, RoutePattern]:
    """
    Derive RoutePattern objects for a single route_id using pre-built indexes.
    Mirrors PatternBuilder.build_patterns_for_route but uses precomputed trip_stops.
    """
    trip_ids = route_trips.get(route_id, [])
    if not trip_ids:
        return {}

    patterns: Dict[str, List[Tuple[str, Optional[str], Optional[str]]]] = defaultdict(list)

    for trip_id in trip_ids:
        stops = trip_stops.get(trip_id) or []
        if len(stops) < 2:
            continue
        meta = trips_meta.get(trip_id) or {}
        direction_id = meta.get("direction_id")
        shape_id = meta.get("shape_id")

        pattern_key_str = f"{route_id}|{direction_id or ''}|" + ",".join(stops)
        pattern_id = hashlib.sha256(pattern_key_str.encode("utf-8")).hexdigest()[:16]
        patterns[pattern_id].append((trip_id, direction_id, shape_id))

    result: Dict[str, RoutePattern] = {}
    for pid, trip_infos in patterns.items():
        freq = len(trip_infos)
        trip_ids = [ti[0] for ti in trip_infos]
        direction_ids = [ti[1] for ti in trip_infos]
        shape_ids = [ti[2] for ti in trip_infos if ti[2] is not None]

        rep_trip_id = trip_ids[0]
        rep_dir = Counter(direction_ids).most_common(1)[0][0] if direction_ids else None
        rep_shape = shape_ids[0] if shape_ids else None

        stops = trip_stops.get(rep_trip_id) or []
        if len(stops) < 2:
            continue

        result[pid] = RoutePattern(
            pattern_id=pid,
            route_id=route_id,
            direction_id=rep_dir,
            stop_ids=stops,
            frequency=freq,
            representative_trip_id=rep_trip_id,
            representative_shape_id=rep_shape,
        )

    return result


def _worker_build_graphs_for_routes(
    route_ids: List[str],
    start_index: int,
    total_routes: int,
    date_str: str,
    feed_version: str,
    pretty_osm: bool,
    indexes_blob: bytes,
) -> List[Tuple[str, str, str, int, str, Optional[str], bool, List[Dict], bytes, bytes, bytes, str, Optional[str], int, int]]:
    """
    Top-level worker function so it can be pickled by multiprocessing.
    Builds graphs for a batch of routes and returns (route_id, direction_id, blob)
    for each. The main process writes to SQLite sequentially to avoid lock contention.
    """
    trips_meta, route_trips, trip_stops = pickle.loads(indexes_blob)

    local_feed = load_active_feed()
    graph_builder = GraphBuilder(local_feed)

    # route_id, direction_id, pattern_id, frequency, repr_trip_id, repr_shape_id,
    # used_shape, stops_meta, u_idx_blob, v_idx_blob, w_s_blob,
    # pattern_polyline_gtfs, pattern_polyline_osm, stop_count, edge_count
    results: List[Tuple[str, str, str, int, str, Optional[str], bool, List[Dict], bytes, bytes, bytes, str, Optional[str], int, int]] = []
    for offset, route_id in enumerate(route_ids):
        global_idx = start_index + offset + 1  # 1-based for nicer logs
        print(
            f"[{global_idx}/{total_routes}] Building graph for route {route_id}..."
        )

        patterns = _build_patterns_for_route_from_indexes(
            route_id=route_id,
            trips_meta=trips_meta,
            route_trips=route_trips,
            trip_stops=trip_stops,
        )
        if not patterns:
            continue

        chosen = max(patterns.values(), key=lambda p: p.frequency)

        build_result = graph_builder.build_graph_for_pattern(chosen)

        used_osm = False
        snapped_pattern_geom = None

        if pretty_osm:
            osm_res = map_match_pattern(
                pattern_geom=None,
                edge_geometries=build_result.edge_geometries,
            )
            used_osm = osm_res.used_osm
            snapped_pattern_geom = osm_res.snapped_pattern_geom
            edge_geoms = osm_res.snapped_edges
        else:
            edge_geoms = build_result.edge_geometries

        # Build compact adjacency arrays based on pattern stop order.
        stop_ids = chosen.stop_ids
        stop_index = {sid: idx for idx, sid in enumerate(stop_ids)}
        u_idx: List[int] = []
        v_idx: List[int] = []
        w_s: List[float] = []
        for i in range(len(stop_ids) - 1):
            a = stop_ids[i]
            b = stop_ids[i + 1]
            u = stop_index.get(a)
            v = stop_index.get(b)
            if u is None or v is None:
                continue
            edge_data = build_result.graph.get_edge_data(a, b, default={})
            w = float(edge_data.get("travel_time_s", 0.0))
            u_idx.append(u)
            v_idx.append(v)
            w_s.append(w)

        # Serialize arrays into compact byte blobs using the array module.
        from array import array

        u_arr = array("I", u_idx)
        v_arr = array("I", v_idx)
        w_arr = array("f", w_s)

        u_blob = u_arr.tobytes()
        v_blob = v_arr.tobytes()
        w_blob = w_arr.tobytes()

        # For v2, we no longer construct a full pattern LineString here to avoid
        # large memory allocations for extremely detailed routes. The compact
        # cache relies on adjacency and stop coordinates; the UI can still draw
        # straight segments between stops immediately, and we can add an
        # optional pattern polyline later if needed.
        pattern_polyline_gtfs = ""
        pattern_polyline_osm = None

        stop_count = len(stop_ids)
        edge_count = len(u_idx)

        # Collect stop metadata for pattern_stops table.
        stops_meta: List[Dict] = []
        for seq, sid in enumerate(stop_ids):
            stop_row = graph_builder.stops_by_id.get(sid)
            if not stop_row:
                continue
            try:
                lat = float(stop_row["stop_lat"])
                lon = float(stop_row["stop_lon"])
            except (KeyError, TypeError, ValueError):
                continue
            stops_meta.append(
                {
                    "seq": seq,
                    "stop_id": sid,
                    "lat": lat,
                    "lon": lon,
                    "name": stop_row.get("stop_name"),
                }
            )

        results.append(
            (
                route_id,
                chosen.direction_id or "",
                chosen.pattern_id,
                chosen.frequency,
                chosen.representative_trip_id,
                chosen.representative_shape_id,
                build_result.used_shape,
                stops_meta,
                u_blob,
                v_blob,
                w_blob,
                pattern_polyline_gtfs,
                pattern_polyline_osm,
                stop_count,
                edge_count,
            )
        )

    return results


def precompute_graphs(
    date: Optional[str] = None,
    pretty_osm: bool = False,
    max_routes: Optional[int] = None,
    skip_existing: bool = False,
) -> None:
    feed = load_active_feed()
    feed_version = feed.version_id
    date_str = date or datetime.utcnow().strftime("%Y%m%d")

    print(f"Building trip/stop indexes for service date {date_str} from SQLite (one pass over stop_times table)...")
    trips_meta, route_trips, trip_stops = _build_trip_indexes_for_date(date_str)
    if not trips_meta:
        print("No active services for this date; nothing to precompute.")
        return

    # Only consider routes that actually have active trips.
    active_route_ids = sorted(route_trips.keys())

    # Optionally skip routes that already have a cached graph for this
    # (feed_version, date, pretty_osm) in SQLite.
    if skip_existing:
        conn = get_conn()
        filtered: List[str] = []
        for rid in active_route_ids:
            # First check v2 compact cache.
            cur = conn.execute(
                """
                SELECT 1
                FROM route_graphs_v2
                WHERE feed_version = ?
                  AND route_id = ?
                  AND date_ymd = ?
                  AND pretty_osm = ?
                LIMIT 1
                """,
                (feed_version, rid, date_str, 1 if pretty_osm else 0),
            )
            if cur.fetchone():
                continue
            # Fallback: also treat legacy v1 cache as existing.
            cur = conn.execute(
                """
                SELECT 1
                FROM route_graphs
                WHERE feed_version = ?
                  AND route_id = ?
                  AND date_ymd = ?
                  AND pretty_osm = ?
                LIMIT 1
                """,
                (feed_version, rid, date_str, 1 if pretty_osm else 0),
            )
            if cur.fetchone():
                continue
            filtered.append(rid)
        skipped = len(active_route_ids) - len(filtered)
        active_route_ids = filtered
        if skipped > 0:
            print(f"Skip-existing enabled: {skipped} routes already cached, {len(active_route_ids)} to build.")
    if max_routes is not None:
        active_route_ids = active_route_ids[:max_routes]

    total_routes = len(active_route_ids)
    if total_routes == 0:
        print("No active routes to precompute for this date (after filtering).")
        return

    # Decide worker count automatically based on CPU.
    cpu_count = multiprocessing.cpu_count() or 1
    workers = max(1, min(cpu_count, 4))

    print(f"Precomputing graphs for {total_routes} routes using {workers} worker(s)...")

    # Serialize shared indexes once so they can be sent to worker processes.
    indexes_blob = pickle.dumps((trips_meta, route_trips, trip_stops))

    # Chunk route ids into roughly equal batches per worker.
    chunk_size = max(1, (total_routes + workers - 1) // workers)
    batches: List[Tuple[List[str], int]] = []
    for i in range(0, total_routes, chunk_size):
        batch_ids = active_route_ids[i : i + chunk_size]
        batches.append((batch_ids, i))

    # Workers only build graphs and return (route_id, direction_id, blob).
    # Main process writes to SQLite sequentially so only one writer touches the DB.
    built_total = 0
    write_failures = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _worker_build_graphs_for_routes,
                batch_ids,
                start_index,
                total_routes,
                date_str,
                feed_version,
                pretty_osm,
                indexes_blob,
            )
            for (batch_ids, start_index) in batches
        ]
        for batch_num, f in enumerate(futures, 1):
            batch_results = f.result()
            if batch_results:
                print(f"Writing {len(batch_results)} graphs to SQLite (batch {batch_num}/{len(futures)})...")
            for (
                route_id,
                direction_id,
                pattern_id,
                frequency,
                repr_trip_id,
                repr_shape_id,
                used_shape,
                stops_meta,
                u_blob,
                v_blob,
                w_blob,
                pattern_polyline_gtfs,
                pattern_polyline_osm,
                stop_count,
                edge_count,
            ) in batch_results:
                try:
                    # Persist pattern metadata and stops.
                    save_pattern_record(
                        feed_version=feed_version,
                        route_id=route_id,
                        direction_id=direction_id,
                        date_ymd=date_str,
                        pattern_id=pattern_id,
                        frequency=frequency,
                        repr_trip_id=repr_trip_id,
                        repr_shape_id=repr_shape_id,
                        used_shape=used_shape,
                    )
                    save_pattern_stops(
                        feed_version=feed_version,
                        pattern_id=pattern_id,
                        stops=stops_meta,
                    )
                    # Legacy blob stub (kept for backward compatibility; currently unused).
                    empty_stub = pickle.dumps({})
                    save_route_graph_blob(
                        feed_version=feed_version,
                        route_id=route_id,
                        direction_id=direction_id,
                        date_ymd=date_str,
                        pretty_osm=pretty_osm,
                        blob=empty_stub,
                    )
                    # V2 compact representation.
                    save_route_graph_v2(
                        feed_version=feed_version,
                        route_id=route_id,
                        direction_id=direction_id,
                        date_ymd=date_str,
                        pattern_id=pattern_id,
                        pretty_osm=pretty_osm,
                        stop_count=stop_count,
                        edge_count=edge_count,
                        u_idx_blob=u_blob,
                        v_idx_blob=v_blob,
                        w_s_blob=w_blob,
                        pattern_polyline_gtfs=pattern_polyline_gtfs,
                        pattern_polyline_osm=pattern_polyline_osm,
                    )
                    built_total += 1
                except Exception as e:
                    write_failures += 1
                    print(f"  ! Failed to write graph cache for route {route_id}: {e}")

    print(f"Done. Built and saved {built_total} graphs." + (f" ({write_failures} write failures.)" if write_failures else ""))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Precompute per-route graphs into the on-disk cache used by /graph/build."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Service date as YYYYMMDD (default: today in UTC).",
    )
    parser.add_argument(
        "--pretty-osm",
        action="store_true",
        help="Also precompute OSM-snapped geometry (requires OSRM running).",
    )
    parser.add_argument(
        "--max-routes",
        type=int,
        default=None,
        help="Optional limit on number of routes to precompute (for testing).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip routes that already have cached graphs for this date/pretty_osm.",
    )

    args = parser.parse_args()
    precompute_graphs(
        date=args.date,
        pretty_osm=args.pretty_osm,
        max_routes=args.max_routes,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    main()

