"""
Populate ``pattern_osm_segments`` via Valhalla ``/trace_attributes`` and our ``osm_road_segments``.

Usage:
  python -m backend.scripts.match_patterns_to_osm --limit 5
  python -m backend.scripts.match_patterns_to_osm --pattern-id XYZ --force-refresh
  python -m backend.scripts.match_patterns_to_osm --all-patterns --limit 200 --workers 4
  python -m backend.scripts.match_patterns_to_osm --all-patterns --cursor-after ABC --limit 200

Requires ``DATABASE_URL``, ``VALHALLA_URL``, and ``VALHALLA_TRACE_ATTRIBUTES_ENABLED``.
"""

from __future__ import annotations

import argparse
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from backend.adapters.osm_detour import valhalla_health
from backend.bus_corridor.match_patterns_to_osm import match_single_pattern_to_osm
from backend.infra import db_access as db
from backend.infra.config import VALHALLA_TRACE_ATTRIBUTES_ENABLED, VALHALLA_URL
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import ensure_detour_v3_layer


def _connect(db_url: str):
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(db_url, cursor_factory=DictCursor)


def _check_valhalla_prerequisites(*, skip: bool) -> None:
    if skip:
        return
    if not VALHALLA_TRACE_ATTRIBUTES_ENABLED:
        print(
            "ERROR: VALHALLA_TRACE_ATTRIBUTES_ENABLED is false. Set it to 1/true.",
            file=sys.stderr,
        )
        sys.exit(1)
    if not (VALHALLA_URL or "").strip():
        print(
            "ERROR: VALHALLA_URL is not set. PowerShell example:\n"
            '  $env:VALHALLA_URL = "http://127.0.0.1:8002"\n'
            "Then re-run this script.",
            file=sys.stderr,
        )
        sys.exit(1)
    h = valhalla_health()
    if not h.get("ok"):
        print(
            f"ERROR: Valhalla is not reachable at {VALHALLA_URL!r}: {h.get('error', h)}",
            file=sys.stderr,
        )
        sys.exit(1)


def _fetch_pattern_page(
    cur,
    *,
    feed_id: int,
    route_id: Optional[str],
    pattern_id: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[str],
) -> List[Dict[str, Any]]:
    """Keyset page when cursor_after is set; else LIMIT/OFFSET on pattern_id order."""
    q = """
        SELECT pattern_id, route_id, direction_id, repr_trip_id, repr_shape_id
        FROM patterns
        WHERE feed_id = %s
    """
    params: List[Any] = [feed_id]
    if route_id:
        q += " AND route_id = %s"
        params.append(str(route_id))
    if pattern_id:
        q += " AND pattern_id = %s"
        params.append(str(pattern_id))
    if cursor_after is not None:
        q += " AND pattern_id > %s"
        params.append(str(cursor_after))
    q += " ORDER BY pattern_id LIMIT %s"
    params.append(int(limit))
    if offset > 0 and cursor_after is None:
        q += " OFFSET %s"
        params.append(int(offset))
    cur.execute(q, tuple(params))
    return [dict(r) for r in cur.fetchall()]


def _process_rows_sequential(
    rows: List[Dict[str, Any]],
    *,
    conn,
    feed_id: int,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    force_refresh: bool,
    verbose: bool,
) -> Tuple[int, int, int]:
    written = skipped = failed = 0
    for r in rows:
        pid = str(r["pattern_id"])
        if verbose:
            log("match_patterns_to_osm", f"processing pattern_id={pid}")
        try:
            res = match_single_pattern_to_osm(
                conn,
                feed_id=feed_id,
                pattern_id=pid,
                repr_trip_id=str(r["repr_trip_id"]) if r.get("repr_trip_id") else None,
                repr_shape_id=str(r["repr_shape_id"]) if r.get("repr_shape_id") else None,
                full_trace_max_km=float(full_trace_max_km),
                chunk_legs=int(chunk_legs),
                chunk_overlap=int(chunk_overlap),
                force_refresh=bool(force_refresh),
            )
            if res.status == "written":
                conn.commit()
                written += 1
            elif res.status.startswith("skipped") or res.status == "skipped_fingerprint":
                conn.rollback()
                skipped += 1
                if verbose:
                    log(
                        "match_patterns_to_osm",
                        f"pattern_id={pid} skipped={res.status} {res.message or ''}",
                    )
            else:
                conn.rollback()
                failed += 1
                if verbose:
                    log(
                        "match_patterns_to_osm",
                        f"pattern_id={pid} failed={res.status} {res.message or ''} notes={res.trace_notes}",
                    )
        except Exception:
            conn.rollback()
            failed += 1
            log("match_patterns_to_osm", f"pattern_id={pid} error", exc_info=True)
    return written, skipped, failed


def _process_rows_parallel_page(
    rows: List[Dict[str, Any]],
    ex: ThreadPoolExecutor,
    *,
    db_url: str,
    feed_id: int,
    max_in_flight: int,
    pool_conns: dict[int, Any],
    pool_lock: threading.Lock,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    force_refresh: bool,
    verbose: bool,
) -> Tuple[int, int, int]:
    """Submit one page to an existing executor; reuse per-thread connections in pool_conns."""
    written = skipped = failed = 0
    lock = threading.Lock()
    sem = threading.Semaphore(max(1, int(max_in_flight)))

    def _thread_conn() -> Any:
        tid = threading.get_ident()
        with pool_lock:
            c = pool_conns.get(tid)
            if c is None or c.closed:
                c = _connect(db_url)
                db.ensure_pattern_physical_layer_schema(conn=c)
                ensure_detour_v3_layer(c)
                pool_conns[tid] = c
            return c

    def one(row: Dict[str, Any]) -> Tuple[str, str]:
        pid = str(row["pattern_id"])
        sem.acquire()
        try:
            conn = _thread_conn()
            try:
                res = match_single_pattern_to_osm(
                    conn,
                    feed_id=feed_id,
                    pattern_id=pid,
                    repr_trip_id=str(row["repr_trip_id"]) if row.get("repr_trip_id") else None,
                    repr_shape_id=str(row["repr_shape_id"]) if row.get("repr_shape_id") else None,
                    full_trace_max_km=float(full_trace_max_km),
                    chunk_legs=int(chunk_legs),
                    chunk_overlap=int(chunk_overlap),
                    force_refresh=bool(force_refresh),
                )
                if res.status == "written":
                    conn.commit()
                    return "written", pid
                if res.status.startswith("skipped") or res.status == "skipped_fingerprint":
                    conn.rollback()
                    return "skipped", pid
                conn.rollback()
                return "failed", pid
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                log("match_patterns_to_osm", f"pattern_id={pid} error", exc_info=True)
                return "error", pid
        finally:
            sem.release()

    futures = {ex.submit(one, r): r for r in rows}
    for fut in as_completed(futures):
        r = futures[fut]
        pid = str(r["pattern_id"])
        if verbose:
            log("match_patterns_to_osm", f"completed pattern_id={pid}")
        try:
            outcome, _pid = fut.result()
        except Exception:
            with lock:
                failed += 1
            log("match_patterns_to_osm", f"pattern_id={pid} future_error", exc_info=True)
            continue
        with lock:
            if outcome == "written":
                written += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                failed += 1
    return written, skipped, failed


def main(argv: Optional[List[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(
        description="Map-match GTFS patterns to directed osm_road_segments.segment_id chain (pattern_osm_segments)."
    )
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument("--route-id", default=None)
    ap.add_argument("--pattern-id", default=None)
    ap.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max patterns per DB page (single batch), or batch size when using --all-patterns (default 50).",
    )
    ap.add_argument(
        "--offset",
        type=int,
        default=0,
        help="SQL OFFSET for the first page only when not using --all-patterns or keyset --cursor-after (default 0).",
    )
    ap.add_argument(
        "--cursor-after",
        default=None,
        metavar="PATTERN_ID",
        help="Keyset resume: only patterns with pattern_id > this value (string sort order).",
    )
    ap.add_argument(
        "--all-patterns",
        action="store_true",
        help="Page through all matching patterns with keyset pagination until none remain (uses --limit as page size).",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Thread pool size for processing a page (default 1 = sequential on one connection).",
    )
    ap.add_argument(
        "--max-in-flight",
        type=int,
        default=None,
        metavar="N",
        help="Cap concurrent pattern workers (Valhalla+DB); default equals --workers.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Select patterns and prerequisites only; exit before Valhalla/DB writes.",
    )
    ap.add_argument(
        "--skip-valhalla-check",
        action="store_true",
        help="Skip VALHALLA_URL / health check (offline tests only).",
    )
    ap.add_argument(
        "--force-refresh",
        action="store_true",
        help="Rebuild pattern_osm_segments even when rows already exist for the pattern.",
    )
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--full-trace-max-km", type=float, default=10.0)
    ap.add_argument("--chunk-legs", type=int, default=11)
    ap.add_argument("--chunk-overlap", type=int, default=1)
    args = ap.parse_args(argv)

    if args.limit < 1:
        ap.error("--limit must be >= 1")
    if args.offset < 0:
        ap.error("--offset must be >= 0")
    workers = max(1, int(args.workers))
    max_in_flight = args.max_in_flight if args.max_in_flight is not None else workers
    max_in_flight = max(1, int(max_in_flight))

    if not args.dry_run:
        _check_valhalla_prerequisites(skip=bool(args.skip_valhalla_check))

    db_url = args.database_url or db.DB_URL

    conn = _connect(db_url)
    try:
        db.ensure_pattern_physical_layer_schema(conn=conn)
        ensure_detour_v3_layer(conn)
        from backend.infra.pipeline_skip import ensure_pipeline_schema

        ensure_pipeline_schema(conn)
        feed_id = int(db.get_active_feed_id(conn))
    finally:
        conn.close()

    if args.all_patterns and args.offset > 0 and args.cursor_after is None:
        log(
            "match_patterns_to_osm",
            "warning=--all-patterns ignores --offset; use --cursor-after for keyset resume",
        )

    def dry_count_first_page() -> int:
        c = _connect(db_url)
        try:
            with c.cursor() as cur:
                rows = _fetch_pattern_page(
                    cur,
                    feed_id=feed_id,
                    route_id=args.route_id,
                    pattern_id=args.pattern_id,
                    limit=args.limit,
                    offset=args.offset if not args.all_patterns else 0,
                    cursor_after=args.cursor_after,
                )
                return len(rows)
        finally:
            c.close()

    if args.dry_run:
        n = dry_count_first_page()
        log(
            "match_patterns_to_osm",
            f"dry_run first_page_patterns={n} feed_id={feed_id} all_patterns={bool(args.all_patterns)}",
        )
        return 0

    total_written = total_skipped = total_failed = 0
    page_idx = 0
    cursor_after: Optional[str] = str(args.cursor_after) if args.cursor_after is not None else None
    pool_conns: dict[int, Any] = {}
    pool_lock = threading.Lock()

    try:
        if workers <= 1:
            while True:
                sel = _connect(db_url)
                try:
                    with sel.cursor() as cur:
                        off = int(args.offset) if page_idx == 0 and not args.all_patterns else 0
                        rows = _fetch_pattern_page(
                            cur,
                            feed_id=feed_id,
                            route_id=args.route_id,
                            pattern_id=args.pattern_id,
                            limit=args.limit,
                            offset=off,
                            cursor_after=cursor_after,
                        )
                finally:
                    sel.close()

                if not rows:
                    if page_idx == 0:
                        log("match_patterns_to_osm", "no patterns matched selection")
                    break

                page_idx += 1
                log(
                    "match_patterns_to_osm",
                    f"page={page_idx} patterns_in_page={len(rows)} workers=1",
                )

                run = _connect(db_url)
                try:
                    db.ensure_pattern_physical_layer_schema(conn=run)
                    ensure_detour_v3_layer(run)
                    w, s, f = _process_rows_sequential(
                        rows,
                        conn=run,
                        feed_id=feed_id,
                        full_trace_max_km=float(args.full_trace_max_km),
                        chunk_legs=int(args.chunk_legs),
                        chunk_overlap=int(args.chunk_overlap),
                        force_refresh=bool(args.force_refresh),
                        verbose=bool(args.verbose),
                    )
                finally:
                    run.close()

                total_written += w
                total_skipped += s
                total_failed += f
                log(
                    "match_patterns_to_osm",
                    f"page={page_idx} done written={w} skipped={s} failed={f} cumulative "
                    f"w={total_written} s={total_skipped} f={total_failed}",
                )

                if not args.all_patterns:
                    break
                cursor_after = str(rows[-1]["pattern_id"])
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                while True:
                    sel = _connect(db_url)
                    try:
                        with sel.cursor() as cur:
                            off = int(args.offset) if page_idx == 0 and not args.all_patterns else 0
                            rows = _fetch_pattern_page(
                                cur,
                                feed_id=feed_id,
                                route_id=args.route_id,
                                pattern_id=args.pattern_id,
                                limit=args.limit,
                                offset=off,
                                cursor_after=cursor_after,
                            )
                    finally:
                        sel.close()

                    if not rows:
                        if page_idx == 0:
                            log("match_patterns_to_osm", "no patterns matched selection")
                        break

                    page_idx += 1
                    log(
                        "match_patterns_to_osm",
                        f"page={page_idx} patterns_in_page={len(rows)} workers={workers} "
                        f"max_in_flight={max_in_flight}",
                    )

                    w, s, f = _process_rows_parallel_page(
                        rows,
                        ex,
                        db_url=db_url,
                        feed_id=feed_id,
                        max_in_flight=max_in_flight,
                        pool_conns=pool_conns,
                        pool_lock=pool_lock,
                        full_trace_max_km=float(args.full_trace_max_km),
                        chunk_legs=int(args.chunk_legs),
                        chunk_overlap=int(args.chunk_overlap),
                        force_refresh=bool(args.force_refresh),
                        verbose=bool(args.verbose),
                    )

                    total_written += w
                    total_skipped += s
                    total_failed += f
                    log(
                        "match_patterns_to_osm",
                        f"page={page_idx} done written={w} skipped={s} failed={f} cumulative "
                        f"w={total_written} s={total_skipped} f={total_failed}",
                    )

                    if not args.all_patterns:
                        break
                    cursor_after = str(rows[-1]["pattern_id"])
    finally:
        with pool_lock:
            for c in list(pool_conns.values()):
                try:
                    c.close()
                except Exception:
                    pass
            pool_conns.clear()

    log(
        "match_patterns_to_osm",
        f"done written={total_written} skipped={total_skipped} failed={total_failed} pages={page_idx}",
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
