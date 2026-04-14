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
import threading
import time
from typing import Optional, Tuple, List, Dict, Any

import psycopg2
from psycopg2 import sql as pg_sql
from psycopg2.extras import DictCursor

from backend.infra.config import GRAPH_CACHE
from backend.infra.db_access import DB_URL
from backend.domain.graph_builder import build_graph_for_pattern_from_postgis
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.adapters.osm_pretty import map_match_pattern
from backend.infra import db_access as db_access_module
from backend.domain.route_preview_payload import build_route_preview_cache_dict
from backend.scripts.build_patterns_postgis import pick_default_pattern_build_date


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


def _build_preview_payload_from_stops_and_shape(
    stops: Optional[List],
    shape_line: Optional[Any],
    snapped_pattern_geom: Optional[Any] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Build route preview GeoJSON from ordered pattern stops + one route polyline (shape or OSRM snap).
    Avoids iterating all graph edge geometries (preview-only; graph blobs unchanged).
    """
    from shapely.geometry import mapping, Point

    stop_features: List[Dict[str, Any]] = []
    stops_list: List[Dict[str, Any]] = []
    if stops:
        for idx, sm in enumerate(stops):
            sid = getattr(sm, "stop_id", None)
            if sid is None:
                continue
            lat, lon = getattr(sm, "lat", None), getattr(sm, "lon", None)
            if lat is None or lon is None:
                continue
            name = getattr(sm, "name", None)
            pt = Point(float(lon), float(lat))
            stop_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(pt),
                    "properties": {"stop_id": sid, "name": name},
                }
            )
            stops_list.append(
                {
                    "stop_id": str(sid),
                    "name": name,
                    "lat": float(lat),
                    "lon": float(lon),
                    "sequence": idx,
                }
            )

    features: List[Dict[str, Any]] = list(stop_features)
    line = snapped_pattern_geom if snapped_pattern_geom is not None else shape_line
    if line is not None and len(getattr(line, "coords", []) or []) >= 2:
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(line),
                "properties": {"kind": "pattern_shape" if snapped_pattern_geom is None else "pattern_snapped"},
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
        (str(r["route_id"]), None if r["direction_id"] is None else str(r["direction_id"]))
        for r in rows
    ]


def _build_one(
    meta: Any,
    stops: Optional[List],
    shape_line: Optional[Any],
    stop_times: Optional[List],
    *,
    fast_preview_geojson: bool = False,
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
    if fast_preview_geojson:
        preview_geojson, preview_stops = _build_preview_payload_from_stops_and_shape(
            stops, shape_line, None
        )
    else:
        preview_geojson, preview_stops = _build_preview_payload(cache_entry)
    cache_entry["preview_geojson"] = preview_geojson
    cache_entry["preview_stops"] = preview_stops
    return result.pattern, cache_entry


def _subset_bulk_for_chunk(
    chunk: List[Tuple[str, Optional[str]]],
    patterns: Dict,
    stops_by_pid: Dict,
    shapes_by_sid: Dict,
    stop_times_by_trip: Dict,
) -> Tuple[Dict, Dict, Dict, Dict]:
    """
    Pass only rows needed for this chunk to worker processes (smaller pickle; fixes Windows spawn issues).
    """
    patterns_sub: Dict = {k: patterns[k] for k in chunk if k in patterns}
    pids = {patterns[k].pattern_id for k in chunk if k in patterns}
    trip_ids = {
        patterns[k].repr_trip_id
        for k in chunk
        if k in patterns and patterns[k].repr_trip_id
    }
    shape_ids = {
        patterns[k].repr_shape_id
        for k in chunk
        if k in patterns and patterns[k].repr_shape_id
    }
    stops_sub = {pid: stops_by_pid[pid] for pid in pids if pid in stops_by_pid}
    for pid in pids:
        if pid not in stops_sub:
            alt = str(pid)
            if alt in stops_by_pid:
                stops_sub[pid] = stops_by_pid[alt]
    shapes_sub = {sid: shapes_by_sid[sid] for sid in shape_ids if sid in shapes_by_sid}
    for sid in shape_ids:
        if sid not in shapes_sub:
            alt = str(sid)
            if alt in shapes_by_sid:
                shapes_sub[sid] = shapes_by_sid[alt]
    st_sub = {tid: stop_times_by_trip[tid] for tid in trip_ids if tid in stop_times_by_trip}
    for tid in trip_ids:
        if tid not in st_sub:
            alt = str(tid)
            if alt in stop_times_by_trip:
                st_sub[tid] = stop_times_by_trip[alt]
    return patterns_sub, stops_sub, shapes_sub, st_sub


def _chunk_batches(seq: List[str], chunk_size: int) -> List[List[str]]:
    """Split seq into chunks; chunk_size <= 0 means one batch (whole seq)."""
    if not seq:
        return []
    if chunk_size <= 0:
        return [seq]
    return [seq[i : i + chunk_size] for i in range(0, len(seq), chunk_size)]


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


def _commit_route_bundle(
    conn,
    *,
    commit_bs: int,
    sp_name: str,
    pending_since_commit: List[int],
) -> None:
    """Finish one route in the current transaction: release savepoint if batching, else commit."""
    if commit_bs <= 1:
        conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute(
            pg_sql.SQL("RELEASE SAVEPOINT {}").format(pg_sql.Identifier(sp_name))
        )
    pending_since_commit[0] += 1
    if pending_since_commit[0] >= commit_bs:
        conn.commit()
        pending_since_commit[0] = 0


def _build_chunk(
    chunk: List[Tuple[str, Optional[str]]],
    patterns_dict: Dict,
    stops_by_pid: Dict,
    shapes_by_sid: Dict,
    stop_times_by_trip: Dict,
    feed_id: int,
    sigs_dict: Dict,
    database_url: Optional[str],
    profiles: List[str],
    date_ymd: str,
    chunk_label: str,
    progress_every: int,
    heartbeat_every_s: int,
    fast_preview_geojson: bool,
    with_pretty_osm: bool,
    commit_batch_size: int,
) -> int:
    """Build and save graphs for a chunk of (route_id, direction_id). Used by parallel workers."""
    conn_w = _connect(database_url)
    n = 0
    total_in_chunk = len(chunk)
    commit_bs = max(1, int(commit_batch_size))
    pending_since_commit: List[int] = [0]
    log(
        "precompute_graphs",
        f"build: chunk {chunk_label} worker started ({total_in_chunk} routes in this chunk)",
    )
    pe = max(1, int(progress_every))
    hb_s = max(0, int(heartbeat_every_s))
    state = {"idx": 0, "saved": 0, "route": None, "stop": False}

    def _heartbeat() -> None:
        if hb_s <= 0:
            return
        while not state["stop"]:
            time.sleep(hb_s)
            if state["stop"]:
                break
            log(
                "precompute_graphs",
                (
                    f"build: chunk {chunk_label} heartbeat "
                    f"position {state['idx']}/{total_in_chunk} "
                    f"saved_ok={state['saved']} current_route={state['route']}"
                ),
            )

    hb_thread = None
    if hb_s > 0:
        hb_thread = threading.Thread(target=_heartbeat, daemon=True)
        hb_thread.start()
    try:
        for idx, (r, d) in enumerate(chunk, start=1):
            sp_name = f"sp_{chunk_label.replace('/', '_')}_{idx}"
            state["idx"] = idx
            state["route"] = (r, d)
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
                meta,
                stops,
                shape_line,
                stop_times or [],
                fast_preview_geojson=fast_preview_geojson,
            )
            sig_hash = sigs_dict.get((r, d), "")
            try:
                if commit_bs > 1:
                    with conn_w.cursor() as cur:
                        cur.execute(
                            pg_sql.SQL("SAVEPOINT {}").format(pg_sql.Identifier(sp_name))
                        )
                # 1) Save GTFS-only graph (pretty_osm = False).
                db_access_module.save_route_graph_pg(
                    feed_id=feed_id,
                    route_id=r,
                    direction_id=d,
                    pretty_osm=False,
                    route_sig_hash=sig_hash,
                    graph_blob=pickle.dumps(cache_entry),
                    date_ymd=date_ymd,
                    conn=conn_w,
                    commit=False,
                )
                gtfs_preview_bytes = pickle.dumps(
                    build_route_preview_cache_dict(
                        cache_entry["pattern"].pattern_id,
                        cache_entry.get("preview_stops") or [],
                        cache_entry.get("preview_geojson"),
                        False,
                        f"postgis-{feed_id}",
                    )
                )
                for profile_key in profiles:
                    db_access_module.save_route_preview_pg(
                        feed_id=feed_id,
                        route_id=r,
                        direction_id=d,
                        profile_key=profile_key,
                        pretty_osm=False,
                        route_sig_hash=sig_hash,
                        pattern_id=str(cache_entry["pattern"].pattern_id),
                        preview_blob=gtfs_preview_bytes,
                        conn=conn_w,
                        commit=False,
                    )
                # 2) Optional OSRM-snapped variant (pretty_osm = True).
                if with_pretty_osm:
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
                            if fast_preview_geojson:
                                preview_geojson, preview_stops = (
                                    _build_preview_payload_from_stops_and_shape(
                                        stops,
                                        shape_line,
                                        osm_res.snapped_pattern_geom,
                                    )
                                )
                            else:
                                preview_geojson, preview_stops = _build_preview_payload(
                                    pretty_entry
                                )
                            pretty_entry["preview_geojson"] = preview_geojson
                            pretty_entry["preview_stops"] = preview_stops
                            db_access_module.save_route_graph_pg(
                                feed_id=feed_id,
                                route_id=r,
                                direction_id=d,
                                pretty_osm=True,
                                route_sig_hash=sig_hash,
                                graph_blob=pickle.dumps(pretty_entry),
                                date_ymd=date_ymd,
                                conn=conn_w,
                                commit=False,
                            )
                            osm_preview_bytes = pickle.dumps(
                                build_route_preview_cache_dict(
                                    pretty_entry["pattern"].pattern_id,
                                    pretty_entry.get("preview_stops") or [],
                                    pretty_entry.get("preview_geojson"),
                                    True,
                                    f"postgis-{feed_id}",
                                )
                            )
                            for profile_key in profiles:
                                db_access_module.save_route_preview_pg(
                                    feed_id=feed_id,
                                    route_id=r,
                                    direction_id=d,
                                    profile_key=profile_key,
                                    pretty_osm=True,
                                    route_sig_hash=sig_hash,
                                    pattern_id=str(pretty_entry["pattern"].pattern_id),
                                    preview_blob=osm_preview_bytes,
                                    conn=conn_w,
                                    commit=False,
                                )
                    except Exception:
                        pass
                _commit_route_bundle(
                    conn_w,
                    commit_bs=commit_bs,
                    sp_name=sp_name,
                    pending_since_commit=pending_since_commit,
                )
                n += 1
                state["saved"] = n
            except Exception as e:
                if commit_bs > 1:
                    try:
                        with conn_w.cursor() as cur:
                            cur.execute(
                                pg_sql.SQL("ROLLBACK TO SAVEPOINT {}").format(
                                    pg_sql.Identifier(sp_name)
                                )
                            )
                    except Exception:
                        try:
                            conn_w.rollback()
                        except Exception:
                            pass
                else:
                    try:
                        conn_w.rollback()
                    except Exception:
                        pass
                if isinstance(e, psycopg2.Error):
                    log(
                        "precompute_graphs",
                        (
                            f"route ({r!r},{d!r}) graph/preview save failed: "
                            f"{type(e).__name__}: {e!s} "
                            f"(pgcode={getattr(e, 'pgcode', None)!r})"
                        ),
                    )
                else:
                    log(
                        "precompute_graphs",
                        f"route ({r!r},{d!r}) graph/preview save failed: {type(e).__name__}: {e!s}",
                    )
            if idx % pe == 0 or idx == total_in_chunk:
                log(
                    "precompute_graphs",
                    (
                        f"build: chunk {chunk_label} position {idx}/{total_in_chunk} "
                        f"saved_ok={n}"
                    ),
                )
        if commit_bs > 1 and pending_since_commit[0] > 0:
            conn_w.commit()
    finally:
        state["stop"] = True
        if hb_thread is not None:
            hb_thread.join(timeout=0.2)
        conn_w.close()
    return n


def main():
    ensure_cli_action_logging()
    log("precompute_graphs", "phase=main start")
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
        help=(
            "Comma-separated service profiles for route_graph_cache and route_preview_cache "
            "(match GRAPH_WARMUP_PROFILES / production UI service calendar)."
        ),
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
    ap.add_argument(
        "--signature-progress-every",
        type=int,
        default=500,
        help="Print progress every N signature hashes during bulk signature step (default 500)",
    )
    ap.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help=(
            "Metadata date_ymd stored with route_graph_cache rows (DB may require NOT NULL). "
            "Default: a date inside the feed calendar window (same logic as pattern build)."
        ),
    )
    ap.add_argument(
        "--bulk-chunk-size",
        type=int,
        default=400,
        help=(
            "Split bulk pattern_stops / shapes / stop_times queries into batches of this many "
            "IDs so logs advance during long DB reads (0 = single query per table)."
        ),
    )
    ap.add_argument(
        "--worker-heartbeat-seconds",
        type=int,
        default=0,
        help=(
            "Per-worker liveness heartbeat interval in seconds during parallel build "
            "(0 = disable; default)."
        ),
    )
    ap.add_argument(
        "--force-signatures",
        action="store_true",
        help=(
            "Always recompute route signatures from trips/stop_times/shapes (slow). "
            "Default: when feed checksum matches patterns_built_checksum, load signatures "
            "from route_signatures (one query) instead of the bulk scan."
        ),
    )
    ap.add_argument(
        "--fast-preview-geojson",
        action="store_true",
        help=(
            "Build route preview GeoJSON from pattern stops + one polyline (faster; less map detail). "
            "Default uses full graph edge geometries for previews (same fidelity as before the fast path)."
        ),
    )
    ap.add_argument(
        "--legacy-preview-geojson",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    ap.add_argument(
        "--with-pretty-osm",
        action="store_true",
        help=(
            "Run OSRM map-match and write pretty_osm=True graph + preview rows (adds significant time). "
            "Default is GTFS-only cache for this run."
        ),
    )
    ap.add_argument(
        "--commit-batch-size",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Commit every N successful routes (per worker in parallel; uses SAVEPOINT per route when N>1). "
            "Default 1 commits after each route."
        ),
    )
    args = ap.parse_args()
    profiles = [p.strip() for p in str(args.profiles).split(",") if p.strip()]
    if not profiles:
        profiles = ["weekday", "friday", "saturday", "sunday"]

    conn = _connect(args.database_url)
    try:
        log("precompute_graphs", "phase=resolve_active_feed start")
        feed_id = db_access_module.get_active_feed_id(conn)
        log("precompute_graphs", f"phase=resolve_active_feed done feed_id={feed_id}")
        log("precompute_graphs", f"feed_id={feed_id}")
        force_signatures = bool(args.force_signatures)
        with conn.cursor() as cur:
            if args.date and str(args.date).strip().isdigit() and len(str(args.date).strip()) == 8:
                reference_date_ymd = str(args.date).strip()
            elif args.date:
                log(
                    "precompute_graphs",
                    f"Ignoring invalid --date {args.date!r}; using feed-derived reference date.",
                )
                reference_date_ymd = pick_default_pattern_build_date(cur, feed_id)
            else:
                reference_date_ymd = pick_default_pattern_build_date(cur, feed_id)
        log("precompute_graphs", f"reference date_ymd (metadata for graph cache)={reference_date_ymd}")

        log("precompute_graphs", "phase=load_route_direction_pairs start")
        pairs = _iter_route_directions(conn)
        log("precompute_graphs", f"phase=load_route_direction_pairs done pairs={len(pairs)}")
        total = len(pairs)
        log("precompute_graphs", f"{total} (route_id, direction_id) pairs.")

        # 1) All patterns for feed (one query)
        log("precompute_graphs", "phase=load_patterns start")
        patterns = db_access_module.get_patterns_for_feed(feed_id, conn)
        log("precompute_graphs", f"phase=load_patterns done patterns={len(patterns)}")
        log("precompute_graphs", f"Loaded {len(patterns)} patterns.")

        # 2) Existing cache for this feed (one query, GTFS-only).
        log("precompute_graphs", "phase=load_cached_graphs start")
        cache = db_access_module.get_cached_graphs_bulk(
            feed_id, False, conn
        )
        log("precompute_graphs", f"phase=load_cached_graphs done cache_rows={len(cache)}")
        log("precompute_graphs", f"Loaded {len(cache)} cached graphs (GTFS).")

        # 3) Route signatures: either load from route_signatures (fast) or full bulk scan.
        sig_t0 = time.monotonic()
        zip_ck, pat_ck = db_access_module.get_feed_checksums(feed_id, conn)
        can_trust_db_sigs = (
            not force_signatures
            and zip_ck is not None
            and pat_ck is not None
            and zip_ck == pat_ck
        )
        sigs: Dict[Tuple[str, Optional[str]], str] = {}
        if can_trust_db_sigs:
            db_sigs = db_access_module.get_route_signatures_bulk(feed_id, conn)
            missing_cov = [
                (r, d)
                for (r, d) in pairs
                if (r, d) in patterns and (r, d) not in db_sigs
            ]
            if not missing_cov:
                sigs = db_sigs
                log(
                    "precompute_graphs",
                    (
                        "Using route_signatures from DB (feed checksum matches "
                        "patterns_built_checksum); skipping bulk trip/stop_times/shapes scan."
                    ),
                )
                log(
                    "precompute_graphs",
                    f"Loaded {len(sigs)} signatures from DB. "
                    f"Signature phase elapsed={time.monotonic() - sig_t0:.2f}s",
                )
            else:
                log(
                    "precompute_graphs",
                    (
                        f"route_signatures missing {len(missing_cov)} (route,direction) rows "
                        "for routes that have patterns; falling back to bulk signature scan."
                    ),
                )

        if not sigs:
            log("precompute_graphs", "Computing route signatures in bulk ...")
            sigs = db_access_module.compute_route_signatures_bulk(
                feed_id,
                conn,
                progress_every=max(0, int(args.signature_progress_every)),
            )
            log("precompute_graphs", f"Computed {len(sigs)} signatures.")
            log(
                "precompute_graphs",
                f"Signature phase elapsed={time.monotonic() - sig_t0:.2f}s",
            )

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
        shape_ids = list(dict.fromkeys(str(s) for s in shape_ids if s))
        trip_ids = [patterns[(r, d)].repr_trip_id for (r, d) in to_build]

        log("precompute_graphs", "Bulk loading pattern_stops, shapes, stop_times ...")
        chunk_sz = max(0, int(args.bulk_chunk_size))
        pattern_ids_uniq = list(dict.fromkeys(str(p) for p in pattern_ids))
        trip_ids_uniq = list(dict.fromkeys(str(t) for t in trip_ids if t))
        n_pat_ids = len(pattern_ids)
        n_pat_unique = len(pattern_ids_uniq)
        n_shape_ids = len(shape_ids)
        n_trip_ids = len(trip_ids)
        n_trip_unique = len(trip_ids_uniq)
        log(
            "precompute_graphs",
            (
                f"bulk_load: request pattern_ids={n_pat_ids} (unique={n_pat_unique}), "
                f"shape_ids={n_shape_ids}, trip_ids={n_trip_ids} (unique={n_trip_unique}), "
                f"chunk_size={chunk_sz or 'all'}"
            ),
        )
        bulk_t0 = time.monotonic()

        pat_batches = _chunk_batches(pattern_ids_uniq, chunk_sz)
        n_pb = len(pat_batches)
        stops_by_pid: Dict[str, Any] = {}
        log(
            "precompute_graphs",
            f"bulk_load: pattern_stops {n_pb} batch(es) (feed_id={feed_id})",
        )
        for bi, batch in enumerate(pat_batches, start=1):
            t0 = time.monotonic()
            log(
                "precompute_graphs",
                f"bulk_load: pattern_stops batch {bi}/{n_pb} (patterns_in_batch={len(batch)}) ...",
            )
            part = db_access_module.get_pattern_stops_bulk(feed_id, batch, conn)
            stops_by_pid.update(part)
            br = sum(len(v) for v in part.values())
            log(
                "precompute_graphs",
                (
                    f"bulk_load: pattern_stops batch {bi}/{n_pb} done "
                    f"stop_rows_in_batch={br} elapsed={time.monotonic() - t0:.2f}s"
                ),
            )
        n_pat_loaded = len(stops_by_pid)
        n_pat_stop_rows = sum(len(v) for v in stops_by_pid.values())
        log(
            "precompute_graphs",
            (
                f"bulk_load: pattern_stops all batches done patterns={n_pat_loaded} "
                f"stop_rows={n_pat_stop_rows}"
            ),
        )

        shape_batches = _chunk_batches(shape_ids, chunk_sz)
        n_sb = len(shape_batches)
        shapes_by_sid: Dict[str, Any] = {}
        log("precompute_graphs", f"bulk_load: shapes_lines {n_sb} batch(es)")
        for bi, batch in enumerate(shape_batches, start=1):
            t0 = time.monotonic()
            log(
                "precompute_graphs",
                f"bulk_load: shapes_lines batch {bi}/{n_sb} (shapes_in_batch={len(batch)}) ...",
            )
            part = db_access_module.get_shape_lines_bulk(feed_id, batch, conn)
            shapes_by_sid.update(part)
            log(
                "precompute_graphs",
                (
                    f"bulk_load: shapes_lines batch {bi}/{n_sb} done "
                    f"elapsed={time.monotonic() - t0:.2f}s"
                ),
            )
        n_shapes_loaded = len(shapes_by_sid)
        log(
            "precompute_graphs",
            f"bulk_load: shapes_lines all batches done shapes={n_shapes_loaded}",
        )

        st_batches = _chunk_batches(trip_ids_uniq, chunk_sz)
        n_tb = len(st_batches)
        stop_times_by_trip: Dict[str, Any] = {}
        log("precompute_graphs", f"bulk_load: stop_times {n_tb} batch(es)")
        for bi, batch in enumerate(st_batches, start=1):
            t0 = time.monotonic()
            log(
                "precompute_graphs",
                f"bulk_load: stop_times batch {bi}/{n_tb} (trips_in_batch={len(batch)}) ...",
            )
            part = db_access_module.get_stop_times_bulk(feed_id, batch, conn)
            stop_times_by_trip.update(part)
            br = sum(len(v) for v in part.values())
            log(
                "precompute_graphs",
                (
                    f"bulk_load: stop_times batch {bi}/{n_tb} done "
                    f"rows_in_batch={br} elapsed={time.monotonic() - t0:.2f}s"
                ),
            )
        n_trips_loaded = len(stop_times_by_trip)
        n_st_rows = sum(len(v) for v in stop_times_by_trip.values())
        log(
            "precompute_graphs",
            (
                f"bulk_load: stop_times all batches done trips={n_trips_loaded} "
                f"stop_time_rows={n_st_rows}"
            ),
        )
        log(
            "precompute_graphs",
            f"bulk_load: done total_elapsed={time.monotonic() - bulk_t0:.2f}s",
        )

        # 5) Build graphs (sequential or parallel)
        log(
            "precompute_graphs",
            (
                f"Graph build options: fast_preview_geojson={bool(args.fast_preview_geojson)}, "
                f"with_pretty_osm={bool(args.with_pretty_osm)}, "
                f"commit_batch_size={max(1, int(args.commit_batch_size))}"
            ),
        )
        workers = max(1, int(args.workers))
        built = 0
        if workers <= 1:
            total_build = len(to_build)
            commit_bs_seq = max(1, int(args.commit_batch_size))
            pending_seq: List[int] = [0]
            fast_pv = bool(args.fast_preview_geojson)
            with_pretty_seq = bool(args.with_pretty_osm)
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
                _, cache_entry = _build_one(
                    meta,
                    stops,
                    shape_line,
                    stop_times or [],
                    fast_preview_geojson=fast_pv,
                )
                sig_hash = sigs.get((r, d), "")
                for profile_key in profiles:
                    key = f"postgis-{feed_id}|{r}|{d or ''}|profile:{profile_key}|gtfs"
                    GRAPH_CACHE[key] = cache_entry
                sp_name_seq = f"sp_seq_{i}"
                try:
                    if commit_bs_seq > 1:
                        with conn.cursor() as cur:
                            cur.execute(
                                pg_sql.SQL("SAVEPOINT {}").format(
                                    pg_sql.Identifier(sp_name_seq)
                                )
                            )
                    db_access_module.save_route_graph_pg(
                        feed_id=feed_id,
                        route_id=r,
                        direction_id=d,
                        pretty_osm=False,
                        route_sig_hash=sig_hash,
                        graph_blob=pickle.dumps(cache_entry),
                        date_ymd=reference_date_ymd,
                        conn=conn,
                        commit=False,
                    )
                    gtfs_preview_bytes = pickle.dumps(
                        build_route_preview_cache_dict(
                            cache_entry["pattern"].pattern_id,
                            cache_entry.get("preview_stops") or [],
                            cache_entry.get("preview_geojson"),
                            False,
                            f"postgis-{feed_id}",
                        )
                    )
                    for profile_key in profiles:
                        db_access_module.save_route_preview_pg(
                            feed_id=feed_id,
                            route_id=r,
                            direction_id=d,
                            profile_key=profile_key,
                            pretty_osm=False,
                            route_sig_hash=sig_hash,
                            pattern_id=str(cache_entry["pattern"].pattern_id),
                            preview_blob=gtfs_preview_bytes,
                            conn=conn,
                            commit=False,
                        )
                    if with_pretty_seq:
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
                                if fast_pv:
                                    preview_geojson, preview_stops = (
                                        _build_preview_payload_from_stops_and_shape(
                                            stops,
                                            shape_line,
                                            osm_res.snapped_pattern_geom,
                                        )
                                    )
                                else:
                                    preview_geojson, preview_stops = _build_preview_payload(
                                        pretty_entry
                                    )
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
                                    pretty_osm=True,
                                    route_sig_hash=sig_hash,
                                    graph_blob=pickle.dumps(pretty_entry),
                                    date_ymd=reference_date_ymd,
                                    conn=conn,
                                    commit=False,
                                )
                                osm_preview_bytes = pickle.dumps(
                                    build_route_preview_cache_dict(
                                        pretty_entry["pattern"].pattern_id,
                                        pretty_entry.get("preview_stops") or [],
                                        pretty_entry.get("preview_geojson"),
                                        True,
                                        f"postgis-{feed_id}",
                                    )
                                )
                                for profile_key in profiles:
                                    db_access_module.save_route_preview_pg(
                                        feed_id=feed_id,
                                        route_id=r,
                                        direction_id=d,
                                        profile_key=profile_key,
                                        pretty_osm=True,
                                        route_sig_hash=sig_hash,
                                        pattern_id=str(pretty_entry["pattern"].pattern_id),
                                        preview_blob=osm_preview_bytes,
                                        conn=conn,
                                        commit=False,
                                    )
                        except Exception:
                            pass
                    _commit_route_bundle(
                        conn,
                        commit_bs=commit_bs_seq,
                        sp_name=sp_name_seq,
                        pending_since_commit=pending_seq,
                    )
                except Exception:
                    if commit_bs_seq > 1:
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    pg_sql.SQL("ROLLBACK TO SAVEPOINT {}").format(
                                        pg_sql.Identifier(sp_name_seq)
                                    )
                                )
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                    else:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    continue
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
            if commit_bs_seq > 1 and pending_seq[0] > 0:
                conn.commit()
        else:
            from concurrent.futures import ProcessPoolExecutor, as_completed

            total_build = len(to_build)
            chunk_size = (total_build + workers - 1) // workers
            chunks = [
                to_build[i : i + chunk_size]
                for i in range(0, len(to_build), chunk_size)
            ]
            pe = max(1, int(args.progress_every))
            log(
                "precompute_graphs",
                (
                    f"Parallel graph build: {workers} workers, {total_build} routes, "
                    f"{len(chunks)} chunk(s) (~{chunk_size} routes/chunk). "
                    f"Each worker logs every {pe} routes and heartbeats every "
                    f"{max(0, int(args.worker_heartbeat_seconds))}s; this can take a long time."
                ),
            )
            with ProcessPoolExecutor(max_workers=workers) as ex:
                futures = []
                for ci, ch in enumerate(chunks, start=1):
                    psub, ssub, shsub, stsub = _subset_bulk_for_chunk(
                        ch,
                        patterns,
                        stops_by_pid,
                        shapes_by_sid,
                        stop_times_by_trip,
                    )
                    label = f"{ci}/{len(chunks)}"
                    futures.append(
                        ex.submit(
                            _build_chunk,
                            ch,
                            psub,
                            ssub,
                            shsub,
                            stsub,
                            feed_id,
                            sigs,
                            args.database_url or DB_URL,
                            profiles,
                            reference_date_ymd,
                            label,
                            pe,
                            max(0, int(args.worker_heartbeat_seconds)),
                            bool(args.fast_preview_geojson),
                            bool(args.with_pretty_osm),
                            max(1, int(args.commit_batch_size)),
                        )
                    )
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
        log("precompute_graphs", "phase=main done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
