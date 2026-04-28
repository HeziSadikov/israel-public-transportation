"""
Precompute pattern_legal_anchor_candidate rows via Valhalla trace_attributes + legal filter.

  python -m backend.scripts.precompute_legal_anchors --limit 20
  python -m backend.scripts.precompute_legal_anchors --workers 4 --valhalla-url http://127.0.0.1:8002

Requires DATABASE_URL, VALHALLA_URL (or --valhalla-url), and ensure_pattern_legal_anchor_schema applied.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString

from backend.adapters.osm_detour import match_route_attributes_detailed, valhalla_health
from backend.domain.detour_physical.edge_matcher import densify_linestring
from backend.domain.detour_physical.legal_anchor_index import (
    build_exit_candidate_records,
    build_legal_anchor_osm_caches,
    build_rejoin_candidates_from_reverse_trace,
    collect_end_osm_node_ids_from_edges,
    merge_and_rank_records,
)
from backend.infra import db_access as db
from backend.infra.config import LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
from backend.infra.logging_utils import ensure_cli_action_logging, log

ANCHOR_VERSION = LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
TRACE_CACHE_VERSION = ANCHOR_VERSION

# Shared across worker threads (Valhalla HTTP cap).
_valhalla_sem: threading.BoundedSemaphore | None = None
_tls = threading.local()
_pool_conns: List[Any] = []
_pool_conns_lock = threading.Lock()


def _pool_init() -> None:
    c = db._get_conn()
    _tls.conn = c
    with _pool_conns_lock:
        _pool_conns.append(c)


def _pool_shutdown() -> None:
    with _pool_conns_lock:
        for c in _pool_conns:
            try:
                c.close()
            except Exception:
                pass
        _pool_conns.clear()


def _worker_conn() -> Any:
    c = getattr(_tls, "conn", None)
    if c is None:
        c = db._get_conn()
        _tls.conn = c
    return c


def _list_patterns(
    conn,
    *,
    limit: Optional[int],
    pattern_id: Optional[str],
    sql_prefilter_shapes: bool,
) -> List[Tuple[str, str, Optional[str]]]:
    """Return (pattern_id, repr_trip_id, repr_shape_id) for active feed."""
    feed_id = db.get_active_feed_id(conn)
    shape_clause = ""
    if sql_prefilter_shapes:
        shape_clause = """
          AND p.repr_shape_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM shapes_lines sl
            WHERE sl.feed_id = p.feed_id AND sl.shape_id::text = p.repr_shape_id
          )
        """
    with conn.cursor() as cur:
        if pattern_id:
            cur.execute(
                f"""
                SELECT p.pattern_id, p.repr_trip_id, p.repr_shape_id
                FROM patterns p
                WHERE p.feed_id = %s AND p.pattern_id = %s
                {shape_clause}
                LIMIT 1
                """,
                (feed_id, pattern_id),
            )
        elif limit is not None:
            cur.execute(
                f"""
                SELECT p.pattern_id, p.repr_trip_id, p.repr_shape_id
                FROM patterns p
                WHERE p.feed_id = %s
                {shape_clause}
                ORDER BY p.pattern_id
                LIMIT %s
                """,
                (feed_id, int(limit)),
            )
        else:
            cur.execute(
                f"""
                SELECT p.pattern_id, p.repr_trip_id, p.repr_shape_id
                FROM patterns p
                WHERE p.feed_id = %s
                {shape_clause}
                ORDER BY p.pattern_id
                """,
                (feed_id,),
            )
        return [
            (str(r["pattern_id"]), str(r["repr_trip_id"]) if r["repr_trip_id"] else "", r.get("repr_shape_id"))
            for r in cur.fetchall()
        ]


def _trace_mem_key(feed_version: str, repr_shape_id: str, direction: str) -> Tuple[str, str, str, str]:
    return (feed_version, str(repr_shape_id), direction, TRACE_CACHE_VERSION)


def _load_trace_from_db_cache(
    conn: Any,
    feed_version: str,
    repr_shape_id: str,
    direction: str,
) -> Optional[Dict[str, Any]]:
    row = db.fetch_pattern_trace_valhalla_cache(
        feed_version, str(repr_shape_id), direction, TRACE_CACHE_VERSION, conn=conn
    )
    if not row:
        return None
    edges = row.get("edges_json")
    if isinstance(edges, str):
        try:
            edges = json.loads(edges)
        except Exception:
            return None
    elif isinstance(edges, (bytes, bytearray)):
        try:
            edges = json.loads(edges.decode("utf-8"))
        except Exception:
            return None
    if not isinstance(edges, list):
        return None
    sl = row.get("shape_lonlat_json")
    shape_lonlat: Optional[List[Tuple[float, float]]] = None
    if sl is not None:
        if isinstance(sl, str):
            try:
                sl = json.loads(sl)
            except Exception:
                sl = None
        if isinstance(sl, list):
            shape_lonlat = []
            for pair in sl:
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    shape_lonlat.append((float(pair[0]), float(pair[1])))
    total_m = float(row["total_m"] or 0.0)
    return {"edges": edges, "shape_lonlat": shape_lonlat, "total_m": total_m}


def _save_trace_to_db_cache(
    conn: Any,
    feed_version: str,
    repr_shape_id: str,
    direction: str,
    edges: List[Dict[str, Any]],
    shape_lonlat: Optional[List[Tuple[float, float]]],
    total_m: float,
) -> None:
    db.upsert_pattern_trace_valhalla_cache(
        feed_version=feed_version,
        repr_shape_id=str(repr_shape_id),
        direction=direction,
        trace_version=TRACE_CACHE_VERSION,
        edges=edges,
        shape_lonlat=shape_lonlat,
        total_m=total_m,
        conn=conn,
    )


def _run_trace_attributes(
    pts: List[Tuple[float, float]],
    *,
    valhalla_base_url: Optional[str],
    timeout_s: float = 45.0,
) -> Optional[Dict[str, Any]]:
    if _valhalla_sem is not None:
        with _valhalla_sem:
            return match_route_attributes_detailed(
                pts, costing="bus", timeout_s=timeout_s, base_url=valhalla_base_url
            )
    return match_route_attributes_detailed(pts, costing="bus", timeout_s=timeout_s, base_url=valhalla_base_url)


MemKey = Tuple[str, str, str, str]


def _resolve_trace_detail(
    *,
    mem_key: MemKey,
    conn: Any,
    feed_version: str,
    repr_shape_id: str,
    direction: str,
    pts: List[Tuple[float, float]],
    trace_mem: Dict[MemKey, Dict[str, Any]],
    trace_mem_lock: threading.Lock,
    trace_flight_locks: Dict[MemKey, threading.Lock],
    use_trace_db_cache: bool,
    valhalla_base_url: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Return cached or freshly traced detail; single-flight per mem_key across worker threads."""
    with trace_mem_lock:
        cached = trace_mem.get(mem_key)
        if cached is not None:
            return cached
        if mem_key not in trace_flight_locks:
            trace_flight_locks[mem_key] = threading.Lock()
        flight = trace_flight_locks[mem_key]
    with flight:
        with trace_mem_lock:
            cached = trace_mem.get(mem_key)
            if cached is not None:
                return cached
        if use_trace_db_cache:
            detail = _load_trace_from_db_cache(conn, feed_version, str(repr_shape_id), direction)
            if detail is not None:
                with trace_mem_lock:
                    trace_mem[mem_key] = detail
                return detail
        detail = _run_trace_attributes(pts, valhalla_base_url=valhalla_base_url)
        if detail:
            with trace_mem_lock:
                trace_mem[mem_key] = detail
            if use_trace_db_cache:
                edges = detail.get("edges") or []
                sl = detail.get("shape_lonlat")
                tm = sum(float(e.get("length") or 0.0) for e in edges) * 1000.0
                try:
                    _save_trace_to_db_cache(
                        conn,
                        feed_version,
                        str(repr_shape_id),
                        direction,
                        list(edges),
                        list(sl) if sl else None,
                        tm,
                    )
                except Exception:
                    pass
        return detail


def _process_one_pattern(
    conn: Any,
    *,
    feed_id: int,
    feed_version: str,
    pattern_id: str,
    repr_trip_id: str,
    repr_shape_id: Optional[str],
    shape_map: Dict[str, LineString],
    trace_mem: Dict[MemKey, Dict[str, Any]],
    trace_mem_lock: threading.Lock,
    trace_flight_locks: Dict[MemKey, threading.Lock],
    force: bool,
    valhalla_base_url: Optional[str],
    use_trace_db_cache: bool,
    timings_out: Optional[Dict[str, float]] = None,
) -> str:
    t0 = time.perf_counter()
    acc: Dict[str, float] = {}

    def _mark(name: str) -> None:
        nonlocal t0
        now = time.perf_counter()
        acc[name] = now - t0
        t0 = now

    if not force:
        st = db.fetch_pattern_legal_anchor_pattern_status(
            feed_version, pattern_id, ANCHOR_VERSION, conn=conn
        )
        if st:
            log(
                "precompute-legal-anchors",
                f"pattern_id={pattern_id} skipped_cached_status outcome={st.get('outcome')}",
            )
            return "skipped_cached_status"
        n_existing = db.count_pattern_legal_anchor_candidates(
            feed_version, pattern_id, ANCHOR_VERSION, conn=conn
        )
        if n_existing > 0:
            log("precompute-legal-anchors", f"pattern_id={pattern_id} skipped_existing_rows n={n_existing}")
            return "skipped_existing_rows"

    if not repr_shape_id:
        db.upsert_pattern_legal_anchor_pattern_status(
            feed_version=feed_version,
            pattern_id=pattern_id,
            anchor_version=ANCHOR_VERSION,
            outcome="no_shape",
            row_count=0,
            conn=conn,
        )
        conn.commit()
        return "no_shape"

    line = shape_map.get(str(repr_shape_id))
    if line is None or line.is_empty:
        db.upsert_pattern_legal_anchor_pattern_status(
            feed_version=feed_version,
            pattern_id=pattern_id,
            anchor_version=ANCHOR_VERSION,
            outcome="no_shape",
            row_count=0,
            conn=conn,
        )
        conn.commit()
        return "no_shape"
    _mark("shape_lookup")

    pts = densify_linestring(line, 15.0)
    if len(pts) < 2:
        db.upsert_pattern_legal_anchor_pattern_status(
            feed_version=feed_version,
            pattern_id=pattern_id,
            anchor_version=ANCHOR_VERSION,
            outcome="too_few_points",
            row_count=0,
            conn=conn,
        )
        conn.commit()
        return "too_few_points"

    mem_key_f = _trace_mem_key(feed_version, str(repr_shape_id), "forward")
    mem_key_r = _trace_mem_key(feed_version, str(repr_shape_id), "reverse")

    detail = _resolve_trace_detail(
        mem_key=mem_key_f,
        conn=conn,
        feed_version=feed_version,
        repr_shape_id=str(repr_shape_id),
        direction="forward",
        pts=pts,
        trace_mem=trace_mem,
        trace_mem_lock=trace_mem_lock,
        trace_flight_locks=trace_flight_locks,
        use_trace_db_cache=use_trace_db_cache,
        valhalla_base_url=valhalla_base_url,
    )
    _mark("forward_trace")

    if not detail:
        db.upsert_pattern_legal_anchor_pattern_status(
            feed_version=feed_version,
            pattern_id=pattern_id,
            anchor_version=ANCHOR_VERSION,
            outcome="trace_failed",
            row_count=0,
            conn=conn,
        )
        conn.commit()
        return "trace_failed"
    edges = detail.get("edges") or []
    shape_lonlat = detail.get("shape_lonlat")
    if not shape_lonlat:
        shape_lonlat = pts
    total_m = sum(float(e.get("length") or 0.0) for e in edges) * 1000.0

    pts_rev = list(reversed(pts))
    detail_r = _resolve_trace_detail(
        mem_key=mem_key_r,
        conn=conn,
        feed_version=feed_version,
        repr_shape_id=str(repr_shape_id),
        direction="reverse",
        pts=pts_rev,
        trace_mem=trace_mem,
        trace_mem_lock=trace_mem_lock,
        trace_flight_locks=trace_flight_locks,
        use_trace_db_cache=use_trace_db_cache,
        valhalla_base_url=valhalla_base_url,
    )
    _mark("reverse_trace")

    edges_r: List[Dict[str, Any]] = []
    sl_r: Optional[List[Tuple[float, float]]] = None
    if detail_r and detail_r.get("edges"):
        edges_r = detail_r["edges"]
        sl_r = detail_r.get("shape_lonlat") or pts_rev

    node_ids = collect_end_osm_node_ids_from_edges(edges) | collect_end_osm_node_ids_from_edges(edges_r)
    caches = build_legal_anchor_osm_caches(sorted(node_ids), conn)
    _mark("osm_preload")

    exit_raw = build_exit_candidate_records(edges, shape_lonlat, caches=caches, role="exit")
    rj = build_rejoin_candidates_from_reverse_trace(edges_r, sl_r, total_m, caches=caches)
    _mark("candidate_build")

    ex_done, rj_done = merge_and_rank_records(exit_raw, rj)
    rows_db: List[Dict[str, Any]] = []
    for r in ex_done + rj_done:
        r2 = dict(r)
        r2["anchor_version"] = ANCHOR_VERSION
        rows_db.append(r2)
    _mark("merge_rank")

    if not rows_db:
        db.upsert_pattern_legal_anchor_pattern_status(
            feed_version=feed_version,
            pattern_id=pattern_id,
            anchor_version=ANCHOR_VERSION,
            outcome="no_candidates",
            row_count=0,
            conn=conn,
        )
        conn.commit()
        return "no_candidates"
    db.replace_pattern_legal_anchor_candidates(
        feed_version, pattern_id, rows_db, anchor_version=ANCHOR_VERSION, conn=conn
    )
    db.upsert_pattern_legal_anchor_pattern_status(
        feed_version=feed_version,
        pattern_id=pattern_id,
        anchor_version=ANCHOR_VERSION,
        outcome="ok",
        row_count=len(rows_db),
        conn=conn,
    )
    _mark("db_write")
    conn.commit()
    if timings_out is not None:
        timings_out.clear()
        timings_out.update(acc)
    if acc:
        log(
            "precompute-legal-anchors",
            f"pattern_id={pattern_id} phase_s={','.join(f'{k}={v:.3f}' for k, v in sorted(acc.items()))}",
        )
    return f"ok_rows_{len(rows_db)}"


def main() -> None:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Precompute legal anchor index for route patterns.")
    ap.add_argument("--limit", type=int, default=None, help="Max patterns (default: all).")
    ap.add_argument("--pattern-id", type=str, default=None, help="Single pattern id.")
    ap.add_argument("--force", action="store_true", help="Recompute even when status or rows exist.")
    ap.add_argument("--workers", type=int, default=1, help="Parallel worker threads (default 1).")
    ap.add_argument(
        "--valhalla-url",
        type=str,
        default=None,
        help="Override VALHALLA_URL for this run (e.g. http://127.0.0.1:8002).",
    )
    ap.add_argument(
        "--valhalla-max-concurrent",
        type=int,
        default=4,
        help="Max simultaneous Valhalla HTTP calls across workers (default 4).",
    )
    ap.add_argument(
        "--no-trace-db-cache",
        action="store_true",
        help="Disable read/write of pattern_trace_valhalla_cache (in-memory cache only).",
    )
    ap.add_argument(
        "--no-sql-prefilter",
        action="store_true",
        help="List all patterns including those without a resolvable shape in shapes_lines.",
    )
    args = ap.parse_args()

    global _valhalla_sem  # noqa: PLW0603
    _valhalla_sem = threading.BoundedSemaphore(max(1, int(args.valhalla_max_concurrent)))

    vh = valhalla_health(base_url=args.valhalla_url)
    if not vh.get("ok"):
        log("precompute-legal-anchors", f"valhalla_unhealthy {vh!r}")
        print(
            "Valhalla not reachable. Set VALHALLA_URL or pass --valhalla-url, e.g.\n"
            '  $env:VALHALLA_URL = "http://127.0.0.1:8002"',
            file=sys.stderr,
        )
        sys.exit(2)

    workers = max(1, int(args.workers))

    trace_mem: Dict[MemKey, Dict[str, Any]] = {}
    trace_mem_lock = threading.Lock()
    trace_flight_locks: Dict[MemKey, threading.Lock] = {}
    use_trace_db = not bool(args.no_trace_db_cache)
    sql_prefilter = not bool(args.no_sql_prefilter)

    if workers == 1:
        conn = db._get_conn()
        try:
            db.ensure_pattern_legal_anchor_schema(conn=conn)
            feed_id = db.get_active_feed_id(conn)
            feed_version = db.get_active_feed_version_key(conn)
            patterns = _list_patterns(conn, limit=args.limit, pattern_id=args.pattern_id, sql_prefilter_shapes=sql_prefilter)
            sids = sorted({str(p[2]) for p in patterns if p[2]})
            shape_map: Dict[str, LineString] = {}
            if sids:
                raw = db.get_shape_lines_bulk(feed_id, sids, conn=conn)
                for sid, geom in raw.items():
                    if isinstance(geom, LineString):
                        shape_map[str(sid)] = geom
            ok = skipped = 0
            for pattern_id, repr_trip_id, repr_shape_id in patterns:
                note = _process_one_pattern(
                    conn,
                    feed_id=feed_id,
                    feed_version=feed_version,
                    pattern_id=pattern_id,
                    repr_trip_id=repr_trip_id,
                    repr_shape_id=repr_shape_id,
                    shape_map=shape_map,
                    trace_mem=trace_mem,
                    trace_mem_lock=trace_mem_lock,
                    trace_flight_locks=trace_flight_locks,
                    force=bool(args.force),
                    valhalla_base_url=args.valhalla_url,
                    use_trace_db_cache=use_trace_db,
                )
                log("precompute-legal-anchors", f"pattern_id={pattern_id} result={note}")
                if note.startswith("ok"):
                    ok += 1
                elif "skipped" in note:
                    skipped += 1
            print(f"Processed {len(patterns)} patterns, ok={ok}, skipped={skipped}.")
        finally:
            conn.close()
        return

    # workers > 1
    main_conn = db._get_conn()
    try:
        db.ensure_pattern_legal_anchor_schema(conn=main_conn)
        feed_id = db.get_active_feed_id(main_conn)
        feed_version = db.get_active_feed_version_key(main_conn)
        patterns = _list_patterns(
            main_conn, limit=args.limit, pattern_id=args.pattern_id, sql_prefilter_shapes=sql_prefilter
        )
        sids = sorted({str(p[2]) for p in patterns if p[2]})
        shape_map = {}
        if sids:
            raw = db.get_shape_lines_bulk(feed_id, sids, conn=main_conn)
            for sid, geom in raw.items():
                if isinstance(geom, LineString):
                    shape_map[str(sid)] = geom
    finally:
        main_conn.close()

    ok = skipped = 0
    lock = threading.Lock()

    def _task(tup: Tuple[str, str, Optional[str]]) -> str:
        pattern_id, repr_trip_id, repr_shape_id = tup
        wc = _worker_conn()
        return _process_one_pattern(
            wc,
            feed_id=feed_id,
            feed_version=feed_version,
            pattern_id=pattern_id,
            repr_trip_id=repr_trip_id,
            repr_shape_id=repr_shape_id,
            shape_map=shape_map,
            trace_mem=trace_mem,
            trace_mem_lock=trace_mem_lock,
            trace_flight_locks=trace_flight_locks,
            force=bool(args.force),
            valhalla_base_url=args.valhalla_url,
            use_trace_db_cache=use_trace_db,
        )

    try:
        with ThreadPoolExecutor(max_workers=workers, initializer=_pool_init) as ex:
            future_to_pid = {ex.submit(_task, p): p[0] for p in patterns}
            for fut in as_completed(future_to_pid, timeout=86400.0):
                pid = future_to_pid.get(fut, "?")
                try:
                    note = fut.result()
                except Exception as e:
                    note = f"error:{e}"
                log("precompute-legal-anchors", f"pattern_id={pid} result={note}")
                with lock:
                    if str(note).startswith("ok"):
                        ok += 1
                    elif "skipped" in str(note):
                        skipped += 1
    finally:
        _pool_shutdown()

    print(f"Processed {len(patterns)} patterns, ok={ok}, skipped={skipped}.")


if __name__ == "__main__":
    main()
