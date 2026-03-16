from __future__ import annotations

"""
Precompute per-route graphs into PostGIS route_graph_cache for faster /graph/build.

Uses bulk DB reads (patterns, cache, signatures, pattern_stops, shapes, stop_times)
and optional parallel graph building to run much faster than per-route queries.

Usage (from project root):

    python -m scripts.precompute_graphs_postgis --date 20260315

Faster (use 4 workers for graph building):

    python -m scripts.precompute_graphs_postgis --date 20260315 --workers 4

Optional:

    python -m scripts.precompute_graphs_postgis --date 20260315 \\
        --database-url postgresql://user:pass@localhost:5432/israel_gtfs

To save progress to a file (so you don't lose messages):

    python -m scripts.precompute_graphs_postgis --date 20260315 2>&1 | tee precompute.log
"""

import argparse
import pickle
import sys
from typing import Optional, Tuple, List, Dict, Any

import psycopg2
from psycopg2.extras import DictCursor

from backend.config import GRAPH_CACHE
from backend.db_access import DB_URL
from backend.graph_builder import build_graph_for_pattern_from_postgis
from backend import db_access as db_access_module


def _connect(database_url: Optional[str]):
    return psycopg2.connect(database_url or DB_URL, cursor_factory=DictCursor)


def _iter_route_directions(conn) -> List[Tuple[str, Optional[str]]]:
    """All (route_id, direction_id) pairs for the active feed."""
    feed_id = db_access_module.get_active_feed_id(conn)
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
        rows = cur.fetchall()
    return [
        (r["route_id"], None if r["direction_id"] is None else str(r["direction_id"]))
        for r in rows
    ]


def _build_one(
    meta: Any,
    date_ymd: str,
    stops: Optional[List],
    shape_line: Optional[Any],
    stop_times: Optional[List],
) -> Tuple[Any, Dict]:
    """Build graph for one pattern; returns (pattern, cache_entry dict)."""
    result = build_graph_for_pattern_from_postgis(
        meta,
        date_ymd,
        pattern_stops=stops,
        shape_line=shape_line,
        stop_times=stop_times,
    )
    cache_entry = {
        "graph": result.graph,
        "edge_geometries": result.edge_geometries,
        "pattern": result.pattern,
        "used_shape": result.used_shape,
        "used_osm_snapping": False,
        "snapped_pattern_geom": None,
        "date": date_ymd,
    }
    return result.pattern, cache_entry


def _build_chunk(
    chunk: List[Tuple[str, Optional[str]]],
    patterns_dict: Dict,
    stops_by_pid: Dict,
    shapes_by_sid: Dict,
    stop_times_by_trip: Dict,
    feed_id: int,
    date_ymd: str,
    sigs_dict: Dict,
    database_url: Optional[str],
) -> int:
    """Build and save graphs for a chunk of (route_id, direction_id). Used by parallel workers."""
    conn_w = _connect(database_url)
    n = 0
    try:
        for (r, d) in chunk:
            meta = patterns_dict.get((r, d))
            if not meta:
                continue
            pid = meta.pattern_id
            stops = stops_by_pid.get(pid)
            shape_line = (
                shapes_by_sid.get(meta.repr_shape_id)
                if meta.repr_shape_id
                else None
            )
            stop_times = stop_times_by_trip.get(meta.repr_trip_id)
            if not stops or len(stops) < 2:
                continue
            _, cache_entry = _build_one(
                meta, date_ymd, stops, shape_line, stop_times or []
            )
            sig_hash = sigs_dict.get((r, d), "")
            try:
                db_access_module.save_route_graph_pg(
                    feed_id=feed_id,
                    route_id=r,
                    direction_id=d,
                    date_ymd=date_ymd,
                    pretty_osm=False,
                    route_sig_hash=sig_hash,
                    graph_blob=pickle.dumps(cache_entry),
                    conn=conn_w,
                )
                n += 1
            except Exception:
                pass
    finally:
        conn_w.close()
    return n


def main():
    # So progress shows immediately when output is redirected (e.g. tee)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(
        description="Precompute per-route graphs into PostGIS route_graph_cache (bulk + optional parallel)."
    )
    ap.add_argument("--database-url", type=str, default=None)
    ap.add_argument("--date", type=str, required=True, help="Service date YYYYMMDD")
    ap.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for graph building (1 = sequential)",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Print progress every N graphs (default 50)",
    )
    args = ap.parse_args()

    conn = _connect(args.database_url)
    try:
        feed_id = db_access_module.get_active_feed_id(conn)
        date_ymd = args.date
        print(f"[precompute_graphs] feed_id={feed_id}, date={date_ymd}", flush=True)

        pairs = _iter_route_directions(conn)
        total = len(pairs)
        print(f"[precompute_graphs] {total} (route_id, direction_id) pairs.", flush=True)

        # 1) All patterns for feed (one query)
        patterns = db_access_module.get_patterns_for_feed(feed_id, conn)
        print(f"[precompute_graphs] Loaded {len(patterns)} patterns.", flush=True)

        # 2) Existing cache for this date (one query)
        cache = db_access_module.get_cached_graphs_bulk(
            feed_id, date_ymd, False, conn
        )
        print(f"[precompute_graphs] Loaded {len(cache)} cached graphs.", flush=True)

        # 3) Signatures for all route/direction in one go (3 queries)
        print("[precompute_graphs] Computing route signatures in bulk ...", flush=True)
        sigs = db_access_module.compute_route_signatures_bulk(feed_id, conn)
        print(f"[precompute_graphs] Computed {len(sigs)} signatures.", flush=True)

        # Pairs that need building: have pattern, (not in cache or sig mismatch)
        to_build: List[Tuple[str, Optional[str]]] = []
        for (r, d) in pairs:
            if (r, d) not in patterns:
                continue
            cached = cache.get((r, d))
            sig = sigs.get((r, d))
            if cached and sig and cached[0] == sig:
                continue
            to_build.append((r, d))

        no_pattern_count = sum(1 for (r, d) in pairs if (r, d) not in patterns)
        reused_count = len(patterns) - len(to_build)
        print(
            f"[precompute_graphs] To build: {len(to_build)}, already cached: {reused_count}, no pattern: {no_pattern_count}",
            flush=True,
        )

        if not to_build:
            print("[precompute_graphs] Nothing to build.", flush=True)
            return

        # 4) Bulk load pattern_stops, shape lines, stop_times for to_build
        pattern_ids = [patterns[(r, d)].pattern_id for (r, d) in to_build]
        shape_ids = [
            patterns[(r, d)].repr_shape_id
            for (r, d) in to_build
            if patterns[(r, d)].repr_shape_id
        ]
        shape_ids = list(dict.fromkeys(shape_ids))
        trip_ids = [patterns[(r, d)].repr_trip_id for (r, d) in to_build]

        print("[precompute_graphs] Bulk loading pattern_stops, shapes, stop_times ...", flush=True)
        stops_by_pid = db_access_module.get_pattern_stops_bulk(feed_id, pattern_ids, conn)
        shapes_by_sid = db_access_module.get_shape_lines_bulk(feed_id, shape_ids, conn)
        stop_times_by_trip = db_access_module.get_stop_times_bulk(feed_id, trip_ids, conn)

        # 5) Build graphs (sequential or parallel)
        workers = max(1, int(args.workers))
        built = 0
        if workers <= 1:
            for i, (r, d) in enumerate(to_build):
                meta = patterns[(r, d)]
                pid = meta.pattern_id
                stops = stops_by_pid.get(pid)
                shape_line = (
                    shapes_by_sid.get(meta.repr_shape_id)
                    if meta.repr_shape_id
                    else None
                )
                stop_times = stop_times_by_trip.get(meta.repr_trip_id)
                if not stops or len(stops) < 2:
                    continue
                _, cache_entry = _build_one(
                    meta, date_ymd, stops, shape_line, stop_times or []
                )
                sig_hash = sigs.get((r, d), "")
                key = f"postgis-{feed_id}|{r}|{d or ''}|{date_ymd}|gtfs"
                GRAPH_CACHE[key] = cache_entry
                try:
                    db_access_module.save_route_graph_pg(
                        feed_id=feed_id,
                        route_id=r,
                        direction_id=d,
                        date_ymd=date_ymd,
                        pretty_osm=False,
                        route_sig_hash=sig_hash,
                        graph_blob=pickle.dumps(cache_entry),
                        conn=conn,
                    )
                except Exception:
                    pass
                built += 1
                progress_every = max(1, int(args.progress_every))
                if built % progress_every == 0 or (i + 1) == len(to_build):
                    print(
                        f"[precompute_graphs] Processed {i + 1}/{len(to_build)} (built={built})",
                        flush=True,
                    )
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed

            chunk_size = (len(to_build) + workers - 1) // workers
            chunks = [
                to_build[i : i + chunk_size]
                for i in range(0, len(to_build), chunk_size)
            ]
            with ProcessPoolExecutor(max_workers=workers) as ex:
                futures = [
                    ex.submit(
                        _build_chunk,
                        ch,
                        patterns,
                        stops_by_pid,
                        shapes_by_sid,
                        stop_times_by_trip,
                        feed_id,
                        date_ymd,
                        sigs,
                        args.database_url or DB_URL,
                    )
                    for ch in chunks
                ]
                completed = 0
                for fut in as_completed(futures):
                    built += fut.result()
                    completed += 1
                    print(
                        f"[precompute_graphs] Chunk {completed}/{len(chunks)} done (built so far: {built})",
                        flush=True,
                    )
            print(f"[precompute_graphs] Parallel build done. Total built: {built}.", flush=True)

        print(
            f"[precompute_graphs] Done. built={built}, reused={reused_count}, no_pattern={no_pattern_count}",
            flush=True,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
