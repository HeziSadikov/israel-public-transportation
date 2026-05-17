"""
Offline backfill: GTFS pattern → one Valhalla trace per pattern (or chunked) → pattern_edge / matches / summary.

Usage:
  python -m backend.scripts.backfill_pattern_edge_matches --limit 5
  python -m backend.scripts.backfill_pattern_edge_matches --route-id 12345 --pattern-id xyz
  python -m backend.scripts.backfill_pattern_edge_matches --workers 4 --match-version edge_matcher_v2_pattern_trace
  # By default ambiguous legs are retried; use --accept-ambiguous to skip them.
  # Use --verbose for per-pattern lines; default is quiet (summary only).
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from shapely.geometry import LineString
from shapely.ops import substring

from backend.adapters.osm_detour import valhalla_health
from backend.domain.detour_physical.edge_matcher import (
    score_gtfs_leg_against_edges,
    trace_pattern_split_to_legs,
)
from backend.domain.detour_physical.pattern_trace_split import (
    ensure_linestring,
    slice_shape_between_stop_indices,
)
from backend.infra import db_access as db
from backend.infra import pipeline_skip as ps
from backend.infra.config import VALHALLA_TRACE_ATTRIBUTES_ENABLED, VALHALLA_URL
from backend.infra.logging_utils import ensure_cli_action_logging, log

DEFAULT_MATCH_VERSION = "edge_matcher_v2_pattern_trace"


def _feed_version_key(conn) -> str:
    return db.get_active_feed_version_key(conn)


def _shape_line_for_pattern(conn, feed_id: int, repr_shape_id: Optional[str]) -> Optional[LineString]:
    if not repr_shape_id:
        return None
    rows = db.get_shape_lines_bulk(feed_id, [str(repr_shape_id)], conn=conn)
    g = rows.get(str(repr_shape_id))
    return g if isinstance(g, LineString) else None


def _stop_times_for_trip(conn, feed_id: int, trip_id: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT stop_id, stop_sequence, shape_dist_traveled
            FROM stop_times
            WHERE feed_id = %s AND trip_id = %s
            ORDER BY stop_sequence
            """,
            (feed_id, trip_id),
        )
        return [dict(r) for r in cur.fetchall()]


def _expected_leg_pairs(stop_rows: List[Dict[str, Any]]) -> Set[Tuple[int, int]]:
    out: Set[Tuple[int, int]] = set()
    for i in range(len(stop_rows) - 1):
        sa = stop_rows[i]
        sb = stop_rows[i + 1]
        out.add((int(sa["stop_sequence"]), int(sb["stop_sequence"])))
    return out


def _substrings_for_edges(gtfs_slice: LineString, edges: List[Dict[str, Any]]) -> List[LineString]:
    total_km = sum(float(e.get("length") or 0.0) for e in edges) or 1e-9
    out: List[LineString] = []
    acc = 0.0
    for e in edges:
        el = float(e.get("length") or 0.0)
        f0 = acc / total_km
        f1 = (acc + el) / total_km
        try:
            out.append(ensure_linestring(substring(gtfs_slice, f0, f1, normalized=True)))
        except Exception:
            out.append(gtfs_slice)
        acc += el
    return out


def _edge_node_ids(edge: Dict[str, Any], way_id: int, ordinal: int) -> Tuple[int, int]:
    a = edge.get("begin_node_id") or edge.get("begin_node")
    b = edge.get("end_node_id") or edge.get("end_node")
    try:
        fn = int(a) if a is not None else -(abs(way_id) * 1_000_000 + ordinal)
    except Exception:
        fn = -(abs(way_id) * 1_000_000 + ordinal)
    try:
        tn = int(b) if b is not None else -(abs(way_id) * 1_000_000 + ordinal + 1)
    except Exception:
        tn = -(abs(way_id) * 1_000_000 + ordinal + 1)
    return fn, tn


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


def backfill_pattern(
    conn,
    *,
    feed_id: int,
    feed_version: str,
    pattern_id: str,
    route_id: str,
    direction_id: Optional[int],
    repr_trip_id: Optional[str],
    repr_shape_id: Optional[str],
    match_version: str,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    force_refresh: bool,
    accept_ambiguous: bool = False,
    verbose: bool = False,
    log_first_pattern_diagnostic: bool = False,
) -> Tuple[int, int, int, str]:
    """
    Returns (edges_ok, ambiguous, failed, status) where status is done | skipped_already_complete | skipped_no_shape.
    """
    try:
        return _backfill_pattern_impl(
            conn,
            feed_id=feed_id,
            feed_version=feed_version,
            pattern_id=pattern_id,
            route_id=route_id,
            direction_id=direction_id,
            repr_trip_id=repr_trip_id,
            repr_shape_id=repr_shape_id,
            match_version=match_version,
            full_trace_max_km=full_trace_max_km,
            chunk_legs=chunk_legs,
            chunk_overlap=chunk_overlap,
            force_refresh=force_refresh,
            accept_ambiguous=accept_ambiguous,
            verbose=verbose,
            log_first_pattern_diagnostic=log_first_pattern_diagnostic,
        )
    except Exception:
        conn.rollback()
        raise


def _backfill_pattern_impl(
    conn,
    *,
    feed_id: int,
    feed_version: str,
    pattern_id: str,
    route_id: str,
    direction_id: Optional[int],
    repr_trip_id: Optional[str],
    repr_shape_id: Optional[str],
    match_version: str,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    force_refresh: bool,
    accept_ambiguous: bool = False,
    verbose: bool = False,
    log_first_pattern_diagnostic: bool = False,
) -> Tuple[int, int, int, str]:
    # Cheap path first: skip already-complete patterns without loading representative shape geometry.
    if repr_trip_id is None:
        return 0, 0, 1, "skipped_no_shape"
    stop_rows = _stop_times_for_trip(conn, feed_id, repr_trip_id)
    if len(stop_rows) < 2:
        return 0, 0, 1, "skipped_no_shape"

    expected = _expected_leg_pairs(stop_rows)
    n_legs = len(expected)

    pattern_sig = db.get_pattern_signature(feed_id, pattern_id, conn=conn)
    if not pattern_sig:
        pattern_sig = db.compute_pattern_signature(
            feed_id,
            pattern_id,
            repr_trip_id=repr_trip_id,
            repr_shape_id=repr_shape_id,
            conn=conn,
        )
    current_fp = ps.build_pattern_edge_match_fingerprint(
        pattern_signature=pattern_sig,
        match_version=match_version,
        full_trace_max_km=full_trace_max_km,
        chunk_legs=chunk_legs,
        chunk_overlap=chunk_overlap,
    )
    if not force_refresh:
        last_fp = ps.get_pattern_edge_match_success_fingerprint(
            conn, feed_id, pattern_id, match_version
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome FROM pattern_edge_match_status
                WHERE feed_id = %s AND pattern_id = %s AND match_version = %s
                """,
                (feed_id, pattern_id, match_version),
            )
            erow = cur.fetchone()
        last_outcome = erow.get("outcome") if erow else None
        if ps.may_skip(current_fp, last_fp, force=False, last_outcome=last_outcome):
            return 0, 0, 0, "skipped_fingerprint"

    if not force_refresh:
        have = db.list_pattern_edge_pairs_with_match_version(
            conn,
            feed_version,
            pattern_id,
            match_version,
            accept_ambiguous=accept_ambiguous,
        )
        legs_to_do = expected - have
    else:
        legs_to_do = set(expected)

    if not legs_to_do:
        ps.upsert_pattern_edge_match_status(
            conn,
            feed_id,
            pattern_id,
            match_version,
            input_fingerprint=current_fp,
            outcome=ps.OUTCOME_SUCCEEDED,
        )
        conn.commit()
        return 0, 0, 0, "skipped_fingerprint"

    shape_line = _shape_line_for_pattern(conn, feed_id, repr_shape_id)
    if shape_line is None:
        return 0, 0, 1, "skipped_no_shape"

    if verbose:
        log(
            "backfill_pattern_edge",
            f"processing pattern_id={pattern_id} route_id={route_id} "
            f"legs_remaining={len(legs_to_do)} total_legs={n_legs} match_version={match_version}",
        )

    leg_indices_needed: Set[int] = set()
    for i in range(len(stop_rows) - 1):
        sa, sb = stop_rows[i], stop_rows[i + 1]
        if (int(sa["stop_sequence"]), int(sb["stop_sequence"])) in legs_to_do:
            leg_indices_needed.add(i)

    per_leg_edges, trace_notes = trace_pattern_split_to_legs(
        shape_line,
        stop_rows,
        full_trace_max_km=full_trace_max_km,
        chunk_legs=chunk_legs,
        chunk_overlap=chunk_overlap,
        only_leg_indices=leg_indices_needed,
    )
    if verbose and log_first_pattern_diagnostic:
        log(
            "backfill_pattern_edge",
            f"pattern_id={pattern_id} route_id={route_id} trace_notes={trace_notes!r}",
        )

    seg_rows: List[Dict[str, Any]] = []
    pe_rows: List[Dict[str, Any]] = []
    leg_payload: List[Dict[str, Any]] = []

    ok = amb = fail = 0

    for i in range(len(stop_rows) - 1):
        sa = stop_rows[i]
        sb = stop_rows[i + 1]
        from_seq = int(sa["stop_sequence"])
        to_seq = int(sb["stop_sequence"])
        pair = (from_seq, to_seq)
        if pair not in legs_to_do:
            continue

        gtfs_slice = slice_shape_between_stop_indices(shape_line, stop_rows, i, i + 1)
        edges = per_leg_edges[i] if i < len(per_leg_edges) else None
        if not edges:
            fail += 1
            if verbose and log_first_pattern_diagnostic:
                log(
                    "backfill_pattern_edge",
                    f"first_leg_fail pattern_id={pattern_id} seq={from_seq}->{to_seq} no_edges",
                )
            continue

        filtered = [e for e in edges if int(e.get("way_id") or 0) > 0]
        if not filtered:
            fail += 1
            continue
        edges = filtered

        score = score_gtfs_leg_against_edges(gtfs_slice, edges)
        amb_flag = score.side_switch_count > 8
        sub_geoms = _substrings_for_edges(gtfs_slice, edges)

        for j, edge in enumerate(edges):
            way_id = int(edge.get("way_id") or 0)
            geom = sub_geoms[j] if j < len(sub_geoms) else gtfs_slice
            elen_m = float(edge.get("length") or 0.0) * 1000.0
            fn, tn = _edge_node_ids(edge, way_id, j)
            rc = edge.get("road_class")
            highway = str(rc) if rc else None
            seg_rows.append(
                {
                    "osm_way_id": way_id,
                    "from_node_id": fn,
                    "to_node_id": tn,
                    "geom_wkt": geom.wkt,
                    "length_m": elen_m,
                    "highway": highway,
                }
            )

        pe_rows.append(
            {
                "feed_version": feed_version,
                "pattern_id": pattern_id,
                "route_id": route_id,
                "direction_id": direction_id,
                "from_stop_id": str(sa["stop_id"]),
                "to_stop_id": str(sb["stop_id"]),
                "from_stop_sequence": from_seq,
                "to_stop_sequence": to_seq,
                "representative_trip_id": repr_trip_id,
                "representative_shape_id": str(repr_shape_id) if repr_shape_id else None,
                "gtfs_geom_wkt": gtfs_slice.wkt,
                "gtfs_length_m": float(gtfs_slice.length * 111_320.0),
            }
        )
        leg_payload.append(
            {
                "from_seq": from_seq,
                "to_seq": to_seq,
                "edges": edges,
                "sub_geoms": sub_geoms,
                "score": score,
                "amb_flag": amb_flag,
                "gtfs_wkt": gtfs_slice.wkt,
            }
        )

    if not pe_rows:
        return ok, amb, fail, "done"

    seg_map = db.bulk_upsert_osm_road_segments(seg_rows, conn=conn)

    pe_payload = [
        {
            "feed_version": r["feed_version"],
            "pattern_id": r["pattern_id"],
            "route_id": r["route_id"],
            "direction_id": r["direction_id"],
            "from_stop_id": r["from_stop_id"],
            "to_stop_id": r["to_stop_id"],
            "from_stop_sequence": r["from_stop_sequence"],
            "to_stop_sequence": r["to_stop_sequence"],
            "representative_trip_id": r["representative_trip_id"],
            "representative_shape_id": r["representative_shape_id"],
            "gtfs_geom_wkt": r["gtfs_geom_wkt"],
            "gtfs_length_m": r["gtfs_length_m"],
        }
        for r in pe_rows
    ]
    pe_map = db.bulk_upsert_pattern_edge_rows(pe_payload, conn=conn)

    match_flat: List[Tuple[int, Dict[str, Any]]] = []
    summary_rows: List[Dict[str, Any]] = []

    for leg in leg_payload:
        key = (int(leg["from_seq"]), int(leg["to_seq"]))
        peid = pe_map.get(key)
        if peid is None:
            fail += 1
            continue
        edges = leg["edges"]
        sub_geoms = leg["sub_geoms"]
        score = leg["score"]
        amb_flag = leg["amb_flag"]
        gtfs_wkt = leg["gtfs_wkt"]
        match_rows: List[Dict[str, Any]] = []
        seg_ids: List[int] = []
        for j, edge in enumerate(edges):
            way_id = int(edge.get("way_id") or 0)
            geom = sub_geoms[j] if j < len(sub_geoms) else None
            elen_m = float(edge.get("length") or 0.0) * 1000.0
            fn, tn = _edge_node_ids(edge, way_id, j)
            sk = (way_id, fn, tn)
            sid = seg_map.get(sk)
            if sid is None:
                sid = db.upsert_osm_road_segment(
                    osm_way_id=way_id,
                    from_node_id=fn,
                    to_node_id=tn,
                    geom_wkt=geom.wkt if geom is not None else "LINESTRING EMPTY",
                    length_m=elen_m,
                    highway=str(edge.get("road_class") or "") or None,
                    conn=conn,
                )
                seg_map[sk] = sid
            seg_ids.append(sid)
            match_rows.append(
                {
                    "ordinal": j,
                    "segment_id": sid,
                    "segment_forward": True,
                    "offset_mean_m": None,
                    "heading_error_deg": None,
                }
            )
        for mr in match_rows:
            match_flat.append((peid, mr))
        conf = min(1.0, max(0.0, (score.total + 5.0) / 15.0))
        cov = float(score.coverage_ratio)
        entry_seg = seg_ids[0] if seg_ids else None
        exit_seg = seg_ids[-1] if seg_ids else None
        summary_rows.append(
            {
                "pattern_edge_id": peid,
                "matched_geom_wkt": gtfs_wkt,
                "entry_segment_id": entry_seg,
                "exit_segment_id": exit_seg,
                "confidence": conf,
                "coverage_ratio": cov,
                "mean_offset_m": float(getattr(score, "mean_offset_m", 0.0) or 0.0),
                "mean_heading_error_deg": float(getattr(score, "mean_heading_error_deg", 0.0) or 0.0),
                "is_ambiguous": bool(amb_flag),
                "match_version": match_version,
            }
        )
        ok += 1
        if amb_flag:
            amb += 1

    db.bulk_delete_and_insert_pattern_edge_matches(match_flat, conn=conn)
    db.bulk_upsert_pattern_edge_match_summaries(summary_rows, conn=conn)
    ps.upsert_pattern_edge_match_status(
        conn,
        feed_id,
        pattern_id,
        match_version,
        input_fingerprint=current_fp,
        outcome=ps.OUTCOME_SUCCEEDED,
    )
    conn.commit()
    return ok, amb, fail, "done"


def _connect(db_url: str):
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(db_url, cursor_factory=DictCursor)


def _run_one_pattern(args_tuple: Tuple[Any, ...]) -> Tuple[str, int, int, int, str]:
    (
        row,
        database_url,
        match_version,
        full_trace_max_km,
        chunk_legs,
        chunk_overlap,
        force_refresh,
        accept_ambiguous,
        verbose,
        log_diag,
    ) = args_tuple
    conn = _connect(database_url)
    try:
        db.ensure_pattern_physical_layer_schema(conn=conn)
        ps.ensure_pipeline_schema(conn)
        feed_id = db.get_active_feed_id(conn)
        fv = _feed_version_key(conn)
        pid = str(row["pattern_id"])
        o, a, f, st = backfill_pattern(
            conn,
            feed_id=feed_id,
            feed_version=fv,
            pattern_id=pid,
            route_id=str(row.get("route_id") or ""),
            direction_id=row.get("direction_id"),
            repr_trip_id=str(row["repr_trip_id"]) if row.get("repr_trip_id") else None,
            repr_shape_id=str(row["repr_shape_id"]) if row.get("repr_shape_id") else None,
            match_version=match_version,
            full_trace_max_km=full_trace_max_km,
            chunk_legs=chunk_legs,
            chunk_overlap=chunk_overlap,
            force_refresh=force_refresh,
            accept_ambiguous=accept_ambiguous,
            verbose=verbose,
            log_first_pattern_diagnostic=log_diag,
        )
        return pid, o, a, f, st
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv: Optional[List[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Backfill pattern_edge_match* from Valhalla trace_attributes (pattern-level trace).")
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument("--route-id", default=None)
    ap.add_argument("--pattern-id", default=None)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument(
        "--skip-valhalla-check",
        action="store_true",
        help="Do not verify VALHALLA_URL / health (for tests or custom setups).",
    )
    ap.add_argument("--match-version", default=DEFAULT_MATCH_VERSION, help="Persisted match_version on summaries.")
    ap.add_argument(
        "--force-refresh",
        action="store_true",
        help="Recompute all legs even when summaries already match --match-version.",
    )
    ap.add_argument(
        "--accept-ambiguous",
        action="store_true",
        help="Treat ambiguous summaries as done and do not retry them (default: retry legs with is_ambiguous).",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Log each pattern as it is processed (default: quiet; only final summary).",
    )
    ap.add_argument("--full-trace-max-km", type=float, default=10.0, help="Trace full shape in one request when length is under this.")
    ap.add_argument("--chunk-legs", type=int, default=11, help="Legs per chunk when shape is long or full trace fails.")
    ap.add_argument("--chunk-overlap", type=int, default=1, help="Overlapping legs between consecutive chunks.")
    ap.add_argument("--workers", type=int, default=1, help="Parallel patterns (each worker uses its own DB connection).")
    args = ap.parse_args(argv)

    _check_valhalla_prerequisites(skip=bool(args.skip_valhalla_check))

    db_url = args.database_url or db.DB_URL

    conn = _connect(db_url)
    try:
        db.ensure_pattern_physical_layer_schema(conn=conn)
        ps.ensure_pipeline_schema(conn)
        feed_id = db.get_active_feed_id(conn)
        q = """
            SELECT pattern_id, route_id, direction_id, repr_trip_id, repr_shape_id
            FROM patterns
            WHERE feed_id = %s
        """
        params: List[Any] = [feed_id]
        if args.route_id:
            q += " AND route_id = %s"
            params.append(str(args.route_id))
        if args.pattern_id:
            q += " AND pattern_id = %s"
            params.append(str(args.pattern_id))
        q += " ORDER BY pattern_id LIMIT %s"
        params.append(int(args.limit))
        with conn.cursor() as cur:
            cur.execute(q, tuple(params))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    try:
        total_ok = total_amb = total_fail = 0
        skipped = 0

        worker_args = [
            (
                r,
                db_url,
                args.match_version,
                float(args.full_trace_max_km),
                int(args.chunk_legs),
                int(args.chunk_overlap),
                bool(args.force_refresh),
                bool(args.accept_ambiguous),
                bool(args.verbose),
                idx == 0,
            )
            for idx, r in enumerate(rows)
        ]

        if int(args.workers) <= 1:
            conn = _connect(db_url)
            try:
                db.ensure_pattern_physical_layer_schema(conn=conn)
                feed_id = db.get_active_feed_id(conn)
                fv = _feed_version_key(conn)
                for idx, r in enumerate(rows):
                    pid = str(r["pattern_id"])
                    o, a, f, st = backfill_pattern(
                        conn,
                        feed_id=feed_id,
                        feed_version=fv,
                        pattern_id=pid,
                        route_id=str(r.get("route_id") or ""),
                        direction_id=r.get("direction_id"),
                        repr_trip_id=str(r["repr_trip_id"]) if r.get("repr_trip_id") else None,
                        repr_shape_id=str(r["repr_shape_id"]) if r.get("repr_shape_id") else None,
                        match_version=args.match_version,
                        full_trace_max_km=float(args.full_trace_max_km),
                        chunk_legs=int(args.chunk_legs),
                        chunk_overlap=int(args.chunk_overlap),
                        force_refresh=bool(args.force_refresh),
                        accept_ambiguous=bool(args.accept_ambiguous),
                        verbose=bool(args.verbose),
                        log_first_pattern_diagnostic=(idx == 0),
                    )
                    total_ok += o
                    total_amb += a
                    total_fail += f
                    if st in ("skipped_already_complete", "skipped_fingerprint"):
                        skipped += 1
            finally:
                conn.close()
        else:
            with ThreadPoolExecutor(max_workers=int(args.workers)) as ex:
                futures = {ex.submit(_run_one_pattern, a): a[0] for a in worker_args}
                for fut in as_completed(futures):
                    _pid, o, a, f, st = fut.result()
                    total_ok += o
                    total_amb += a
                    total_fail += f
                    if st in ("skipped_already_complete", "skipped_fingerprint"):
                        skipped += 1

        log(
            "backfill_pattern_edge",
            f"done legs_written={total_ok} legs_flagged_ambiguous={total_amb} failed_legs={total_fail} "
            f"patterns_skipped_complete={skipped} batch_patterns={len(rows)}",
        )
        return 0
    except Exception as e:
        log("backfill_pattern_edge", f"error={e!s}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
