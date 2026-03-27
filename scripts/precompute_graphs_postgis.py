from __future__ import annotations

"""
Precompute per-route graphs into PostGIS route_graph_cache for faster /graph/build.

Uses bulk DB reads (patterns, cache, signatures, pattern_stops, shapes, stop_times)
and optional parallel graph building to run much faster than per-route queries.

Usage (from project root):

    python -m scripts.precompute_graphs_postgis

Faster (use 4 workers for graph building):

    python -m scripts.precompute_graphs_postgis --workers 4

Optional:

    python -m scripts.precompute_graphs_postgis \\
        --profiles weekday,friday,saturday,sunday \\
        --database-url postgresql://user:pass@localhost:5432/israel_gtfs
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
from backend.logging_utils import log
from backend.osm_pretty import map_match_pattern
from backend import db_access as db_access_module


def _connect(database_url: Optional[str]):
    return psycopg2.connect(database_url or DB_URL, cursor_factory=DictCursor)


def _build_preview_payload(cache_entry: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    from shapely.geometry import mapping, Point

    pattern = cache_entry["pattern"]
    graph = cache_entry.get("graph")
    edge_geometries = cache_entry["edge_geometries"]
    snapped_pattern_geom = cache_entry.get("snapped_pattern_geom")
    stop_features: List[Dict[str, Any]] = []
    stops_list: List[Dict[str, Any]] = []
    if graph is not None:
        by_stop: Dict[str, Dict[str, Any]] = {}
        for _nid, node_data in graph.nodes(data=True):
            sid = node_data.get("stop_id")
            if sid is None:
                continue
            key = str(sid)
            if key not in by_stop:
                by_stop[key] = node_data
        for idx, sid in enumerate(pattern.stop_ids):
            d = by_stop.get(str(sid))
            if not d:
                continue
            lat, lon = d.get("lat"), d.get("lon")
            if lat is None or lon is None:
                continue
            stop_name = d.get("stop_name")
            pt = Point(float(lon), float(lat))
            stop_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(pt),
                    "properties": {"stop_id": sid, "name": stop_name},
                }
            )
            stops_list.append(
                {
                    "stop_id": str(sid),
                    "name": stop_name,
                    "lat": float(lat),
                    "lon": float(lon),
                    "sequence": idx,
                }
            )

    edge_features = []
    for (_u, _v), eg in edge_geometries.items():
        edge_features.append(
            {
                "type": "Feature",
                "geometry": mapping(eg.linestring),
                "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
            }
        )
    features = stop_features + edge_features
    if snapped_pattern_geom is not None:
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(snapped_pattern_geom),
                "properties": {"kind": "pattern_snapped"},
            }
        )
    return {"type": "FeatureCollection", "features": features}, stops_list


def _render_progress(done: int, total: int, prefix: str = "") -> None:
    """Simple in-place ASCII progress bar for long loops."""
    if total <= 0:
        return
    width = 30
    frac = max(0.0, min(1.0, done / total))
    filled = int(width * frac)
    bar = "#" * filled + "-" * (width - filled)
    pct = int(frac * 100)
    text = f"{prefix}[{bar}] {pct:3d}% ({done}/{total})"
    # Carriage return, no newline; flushed so it updates in-place.
    print("\r" + text, end="", flush=True)
    if done >= total:
        print("", flush=True)  # move to next line when finished


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
    stops: Optional[List],
    shape_line: Optional[Any],
    stop_times: Optional[List],
) -> Tuple[Any, Dict]:
    """Build graph for one pattern; returns (pattern, cache_entry dict)."""
    result = build_graph_for_pattern_from_postgis(
        meta,
        "",
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
        "date": None,
        "preview_geojson": None,
        "preview_stops": None,
    }
    preview_geojson, preview_stops = _build_preview_payload(cache_entry)
    cache_entry["preview_geojson"] = preview_geojson
    cache_entry["preview_stops"] = preview_stops
    return result.pattern, cache_entry


def _merge_edge_geometries(edge_geometries: Dict[Tuple[str, str], Any]):
    """Heuristic: concatenate edges in insertion order into a single LineString."""
    if not edge_geometries:
        return None
    from shapely.geometry import LineString

    parts = []
    for eg in edge_geometries.values():
        ls = getattr(eg, "linestring", None)
        if ls is not None and len(ls.coords) >= 2:
            parts.append(ls)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    # Simple concatenation of coordinates; good enough for OSRM map-matching input.
    coords = []
    for ls in parts:
        coords.extend(ls.coords)
    try:
        return LineString(coords)
    except Exception:
        return parts[0]


def _build_chunk(
    chunk: List[Tuple[str, Optional[str]]],
    patterns_dict: Dict,
    stops_by_pid: Dict,
    shapes_by_sid: Dict,
    stop_times_by_trip: Dict,
    feed_id: int,
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
            _, cache_entry = _build_one(meta, stops, shape_line, stop_times or [])
            sig_hash = sigs_dict.get((r, d), "")
            try:
                # 1) Save GTFS-only graph (pretty_osm = False).
                db_access_module.save_route_graph_pg(
                    feed_id=feed_id,
                    route_id=r,
                    direction_id=d,
                    pretty_osm=False,
                    route_sig_hash=sig_hash,
                    graph_blob=pickle.dumps(cache_entry),
                    conn=conn_w,
                )
                # 2) Try to precompute OSRM-snapped variant (pretty_osm = True).
                try:
                    pattern_geom = _merge_edge_geometries(
                        cache_entry["edge_geometries"]
                    )
                    osm_res = map_match_pattern(
                        pattern_geom=pattern_geom,
                        edge_geometries=cache_entry["edge_geometries"],
                    )
                    if osm_res.used_osm:
                        pretty_entry = {
                            "graph": cache_entry["graph"],
                            "edge_geometries": osm_res.snapped_edges,
                            "pattern": cache_entry["pattern"],
                            "used_shape": cache_entry["used_shape"],
                            "used_osm_snapping": True,
                            "snapped_pattern_geom": osm_res.snapped_pattern_geom,
                            "date": None,
                            "preview_geojson": None,
                            "preview_stops": None,
                        }
                        preview_geojson, preview_stops = _build_preview_payload(pretty_entry)
                        pretty_entry["preview_geojson"] = preview_geojson
                        pretty_entry["preview_stops"] = preview_stops
                        db_access_module.save_route_graph_pg(
                            feed_id=feed_id,
                            route_id=r,
                            direction_id=d,
                            pretty_osm=True,
                            route_sig_hash=sig_hash,
                            graph_blob=pickle.dumps(pretty_entry),
                            conn=conn_w,
                        )
                except Exception:
                    # OSRM failure should not break precompute.
                    pass
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
    ap.add_argument(
        "--profiles",
        type=str,
        default="weekday,friday,saturday,sunday",
        help="Comma-separated service profiles to prewarm in in-memory cache keys.",
    )
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
    profiles = [p.strip() for p in str(args.profiles).split(",") if p.strip()]
    if not profiles:
        profiles = ["weekday", "friday", "saturday", "sunday"]

    conn = _connect(args.database_url)
    try:
        feed_id = db_access_module.get_active_feed_id(conn)
        log("precompute_graphs", f"feed_id={feed_id}")

        pairs = _iter_route_directions(conn)
        total = len(pairs)
        log("precompute_graphs", f"{total} (route_id, direction_id) pairs.")

        # 1) All patterns for feed (one query)
        patterns = db_access_module.get_patterns_for_feed(feed_id, conn)
        log("precompute_graphs", f"Loaded {len(patterns)} patterns.")

        # 2) Existing cache for this feed (one query, GTFS-only).
        cache = db_access_module.get_cached_graphs_bulk(
            feed_id, False, conn
        )
        log("precompute_graphs", f"Loaded {len(cache)} cached graphs (GTFS).")

        # 3) Signatures for all route/direction in one go (3 queries)
        log("precompute_graphs", "Computing route signatures in bulk ...")
        sigs = db_access_module.compute_route_signatures_bulk(feed_id, conn)
        log("precompute_graphs", f"Computed {len(sigs)} signatures.")

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
        log(
            "precompute_graphs",
            f"To build: {len(to_build)}, already cached: {reused_count}, no_pattern: {no_pattern_count}",
        )

        if not to_build:
            log("precompute_graphs", "Nothing to build.")
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

        log("precompute_graphs", "Bulk loading pattern_stops, shapes, stop_times ...")
        stops_by_pid = db_access_module.get_pattern_stops_bulk(feed_id, pattern_ids, conn)
        shapes_by_sid = db_access_module.get_shape_lines_bulk(feed_id, shape_ids, conn)
        stop_times_by_trip = db_access_module.get_stop_times_bulk(feed_id, trip_ids, conn)

        # 5) Build graphs (sequential or parallel)
        workers = max(1, int(args.workers))
        built = 0
        if workers <= 1:
            total_build = len(to_build)
            for i, (r, d) in enumerate(to_build, start=1):
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
                _, cache_entry = _build_one(meta, stops, shape_line, stop_times or [])
                sig_hash = sigs.get((r, d), "")
                # 1) Save GTFS-only graph.
                for profile_key in profiles:
                    key = f"postgis-{feed_id}|{r}|{d or ''}|profile:{profile_key}|gtfs"
                    GRAPH_CACHE[key] = cache_entry
                try:
                    db_access_module.save_route_graph_pg(
                        feed_id=feed_id,
                        route_id=r,
                        direction_id=d,
                        date_ymd=None,
                        pretty_osm=False,
                        route_sig_hash=sig_hash,
                        graph_blob=pickle.dumps(cache_entry),
                        conn=conn,
                    )
                    # 2) Try to precompute OSRM-snapped graph.
                    try:
                        pattern_geom = _merge_edge_geometries(
                            cache_entry["edge_geometries"]
                        )
                        osm_res = map_match_pattern(
                            pattern_geom=pattern_geom,
                            edge_geometries=cache_entry["edge_geometries"],
                        )
                        if osm_res.used_osm:
                            pretty_entry = {
                                "graph": cache_entry["graph"],
                                "edge_geometries": osm_res.snapped_edges,
                                "pattern": cache_entry["pattern"],
                                "used_shape": cache_entry["used_shape"],
                                "used_osm_snapping": True,
                                "snapped_pattern_geom": osm_res.snapped_pattern_geom,
                                "date": None,
                                "preview_geojson": None,
                                "preview_stops": None,
                            }
                            preview_geojson, preview_stops = _build_preview_payload(pretty_entry)
                            pretty_entry["preview_geojson"] = preview_geojson
                            pretty_entry["preview_stops"] = preview_stops
                            for profile_key in profiles:
                                key_osm = (
                                    f"postgis-{feed_id}|{r}|{d or ''}|profile:{profile_key}|osm"
                                )
                                GRAPH_CACHE[key_osm] = pretty_entry
                            db_access_module.save_route_graph_pg(
                                feed_id=feed_id,
                                route_id=r,
                                direction_id=d,
                                date_ymd=None,
                                pretty_osm=True,
                                route_sig_hash=sig_hash,
                                graph_blob=pickle.dumps(pretty_entry),
                                conn=conn,
                            )
                    except Exception:
                        pass
                except Exception:
                    pass
                built += 1
                progress_every = max(1, int(args.progress_every))
                if built % progress_every == 0 or i == total_build:
                    _render_progress(
                        built,
                        total_build,
                        prefix="[precompute_graphs] Build ",
                    )
                    log(
                        "precompute_graphs",
                        f"Processed {i}/{total_build} (built={built})",
                    )
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed

            total_build = len(to_build)
            chunk_size = (total_build + workers - 1) // workers
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
                        sigs,
                        args.database_url or DB_URL,
                    )
                    for ch in chunks
                ]
                completed = 0
                for fut in as_completed(futures):
                    built += fut.result()
                    completed += 1
                    _render_progress(
                        built,
                        total_build,
                        prefix="[precompute_graphs] Build (parallel) ",
                    )
                    log(
                        "precompute_graphs",
                        f"Chunk {completed}/{len(chunks)} done (built so far: {built})",
                    )
            log("precompute_graphs", f"Parallel build done. Total built: {built}.")

        log(
            "precompute_graphs",
            f"Done. built={built}, skipped_unchanged={reused_count}, no_pattern={no_pattern_count}, profiles={profiles}",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
