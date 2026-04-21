"""
Precompute pattern_legal_anchor_candidate rows via Valhalla trace_attributes + legal filter.

  python -m backend.scripts.precompute_legal_anchors --limit 20
  python -m backend.scripts.precompute_legal_anchors --pattern-id <hex>

Requires DATABASE_URL, VALHALLA_URL, and ensure_pattern_legal_anchor_schema applied.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString

from backend.adapters.osm_detour import match_route_attributes_detailed, valhalla_health
from backend.domain.detour_physical.edge_matcher import densify_linestring
from backend.domain.detour_physical.legal_anchor_index import (
    build_exit_candidate_records,
    build_rejoin_candidates_from_reverse_trace,
    merge_and_rank_records,
)
from backend.infra import db_access as db
from backend.infra.logging_utils import ensure_cli_action_logging, log

ANCHOR_VERSION = "legal_anchor_v1"


def _list_patterns(conn, *, limit: Optional[int], pattern_id: Optional[str]) -> List[Tuple[str, str, Optional[str]]]:
    """Return (pattern_id, repr_trip_id, repr_shape_id) for active feed."""
    feed_id = db.get_active_feed_id(conn)
    with conn.cursor() as cur:
        if pattern_id:
            cur.execute(
                """
                SELECT pattern_id, repr_trip_id, repr_shape_id
                FROM patterns
                WHERE feed_id = %s AND pattern_id = %s
                LIMIT 1
                """,
                (feed_id, pattern_id),
            )
        elif limit is not None:
            cur.execute(
                """
                SELECT pattern_id, repr_trip_id, repr_shape_id
                FROM patterns
                WHERE feed_id = %s
                ORDER BY pattern_id
                LIMIT %s
                """,
                (feed_id, int(limit)),
            )
        else:
            cur.execute(
                """
                SELECT pattern_id, repr_trip_id, repr_shape_id
                FROM patterns
                WHERE feed_id = %s
                ORDER BY pattern_id
                """,
                (feed_id,),
            )
        return [
            (str(r["pattern_id"]), str(r["repr_trip_id"]) if r["repr_trip_id"] else "", r.get("repr_shape_id"))
            for r in cur.fetchall()
        ]


def _shape_for_pattern(conn, feed_id: int, repr_shape_id: Optional[str]) -> Optional[LineString]:
    if not repr_shape_id:
        return None
    rows = db.get_shape_lines_bulk(feed_id, [str(repr_shape_id)], conn=conn)
    g = rows.get(str(repr_shape_id))
    return g if isinstance(g, LineString) else None


def _process_one_pattern(
    conn,
    feed_id: int,
    feed_version: str,
    pattern_id: str,
    repr_trip_id: str,
    repr_shape_id: Optional[str],
) -> str:
    line = _shape_for_pattern(conn, feed_id, repr_shape_id)
    if line is None or line.is_empty:
        return "no_shape"
    pts = densify_linestring(line, 15.0)
    if len(pts) < 2:
        return "too_few_points"
    detail = match_route_attributes_detailed(pts, costing="bus", timeout_s=45.0)
    if not detail:
        return "trace_failed"
    edges = detail.get("edges") or []
    shape_lonlat = detail.get("shape_lonlat")
    if not shape_lonlat:
        shape_lonlat = pts
    total_m = sum(float(e.get("length") or 0.0) for e in edges) * 1000.0
    exit_raw = build_exit_candidate_records(edges, shape_lonlat, conn=conn, role="exit")
    pts_rev = list(reversed(pts))
    detail_r = match_route_attributes_detailed(pts_rev, costing="bus", timeout_s=45.0)
    if detail_r and detail_r.get("edges"):
        edges_r = detail_r["edges"]
        sl_r = detail_r.get("shape_lonlat") or pts_rev
        rj = build_rejoin_candidates_from_reverse_trace(edges_r, sl_r, total_m, conn=conn)
    else:
        rj = []
    ex_done, rj_done = merge_and_rank_records(exit_raw, rj)
    rows_db: List[Dict[str, Any]] = []
    for r in ex_done + rj_done:
        r2 = dict(r)
        r2["anchor_version"] = ANCHOR_VERSION
        rows_db.append(r2)
    if not rows_db:
        return "no_candidates"
    db.replace_pattern_legal_anchor_candidates(feed_version, pattern_id, rows_db, anchor_version=ANCHOR_VERSION, conn=conn)
    return f"ok_rows_{len(rows_db)}"


def main() -> None:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Precompute legal anchor index for route patterns.")
    ap.add_argument("--limit", type=int, default=None, help="Max patterns (default: all).")
    ap.add_argument("--pattern-id", type=str, default=None, help="Single pattern id.")
    args = ap.parse_args()

    vh = valhalla_health()
    if not vh.get("ok"):
        log("precompute-legal-anchors", f"valhalla_unhealthy {vh!r}")
        print("Valhalla not reachable; set VALHALLA_URL and start the service.", file=sys.stderr)
        sys.exit(2)

    conn = db._get_conn()
    try:
        db.ensure_pattern_legal_anchor_schema(conn=conn)
        feed_id = db.get_active_feed_id(conn)
        feed_version = db.get_active_feed_version_key(conn)
        patterns = _list_patterns(conn, limit=args.limit, pattern_id=args.pattern_id)
        if not patterns:
            log("precompute-legal-anchors", "no_patterns")
            print("No patterns found.")
            return
        ok = 0
        for pattern_id, repr_trip_id, repr_shape_id in patterns:
            note = _process_one_pattern(conn, feed_id, feed_version, pattern_id, repr_trip_id, repr_shape_id)
            log("precompute-legal-anchors", f"pattern_id={pattern_id} result={note}")
            if note.startswith("ok"):
                ok += 1
        print(f"Processed {len(patterns)} patterns, successful inserts: {ok}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
