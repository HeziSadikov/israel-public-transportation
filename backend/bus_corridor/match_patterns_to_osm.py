"""Detour v3 M4: map-match GTFS patterns to ``pattern_osm_segments`` via Valhalla + our directed graph."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from shapely.geometry import LineString

from backend.domain.detour_physical.edge_matcher import (
    score_gtfs_leg_against_edges,
    trace_pattern_split_to_legs,
)
from backend.infra import db_access as db
from backend.infra import pipeline_skip as ps
from backend.infra.osm_import_db import IMPORT_SOURCE_V3

from .trace_segment_resolve import (
    dedupe_consecutive_trace_edges,
    flatten_per_leg_trace_edges,
    resolve_trace_edges_to_segment_ids,
    trace_edge_has_osm_endpoint_ids,
    trace_edge_resolution_triple,
)


SOURCE_VALHALLA_TRACE = "valhalla_trace"


@dataclass(frozen=True)
class PatternOsmMatchResult:
    pattern_id: str
    status: str
    segments_written: int
    trace_notes: Tuple[str, ...] = ()
    message: Optional[str] = None


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


def _confidence_from_flat_edges(shape_line: LineString, flat_edges: List[Dict[str, Any]]) -> float:
    score = score_gtfs_leg_against_edges(shape_line, flat_edges)
    return float(min(1.0, max(0.0, (score.total + 5.0) / 15.0)))


def _build_match_fingerprint(
    conn,
    feed_id: int,
    pattern_id: str,
    *,
    costing: str,
    densify_m: float,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    repr_trip_id: Optional[str],
    repr_shape_id: Optional[str],
) -> str:
    pattern_sig = db.get_pattern_signature(feed_id, pattern_id, conn=conn)
    if not pattern_sig:
        pattern_sig = db.compute_pattern_signature(
            feed_id,
            pattern_id,
            repr_trip_id=repr_trip_id,
            repr_shape_id=repr_shape_id,
            conn=conn,
        )
    osm_fp = ps.get_latest_osm_dataset_fingerprint(conn) or ""
    return ps.build_pattern_osm_match_fingerprint(
        pattern_signature=pattern_sig,
        osm_dataset_fingerprint=osm_fp,
        costing=costing,
        densify_m=densify_m,
        full_trace_max_km=full_trace_max_km,
        chunk_legs=chunk_legs,
        chunk_overlap=chunk_overlap,
    )


def _do_match_write(
    conn,
    *,
    feed_id: int,
    pid: str,
    repr_trip_id: Optional[str],
    repr_shape_id: Optional[str],
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
    costing: str,
    densify_m: float,
) -> PatternOsmMatchResult:
    shape_line = _shape_line_for_pattern(conn, feed_id, repr_shape_id)
    if shape_line is None or shape_line.is_empty:
        return PatternOsmMatchResult(pid, "skipped_no_shape", 0, message="no representative shape")

    if not repr_trip_id:
        return PatternOsmMatchResult(pid, "skipped_no_trip", 0, message="no repr_trip_id")
    stops = _stop_times_for_trip(conn, feed_id, str(repr_trip_id))
    if len(stops) < 2:
        return PatternOsmMatchResult(pid, "skipped_no_stops", 0, message="fewer than 2 stop_times rows")

    per_leg, notes = trace_pattern_split_to_legs(
        shape_line,
        stops,
        full_trace_max_km=full_trace_max_km,
        chunk_legs=chunk_legs,
        chunk_overlap=chunk_overlap,
        costing=costing,
        densify_m=densify_m,
        only_leg_indices=None,
    )
    nt = tuple(notes)

    flat = flatten_per_leg_trace_edges(per_leg)
    if not flat:
        return PatternOsmMatchResult(pid, "failed_trace", 0, trace_notes=nt, message="could not flatten leg edges")

    flat = dedupe_consecutive_trace_edges(flat)

    triples = [trace_edge_resolution_triple(e, i) for i, e in enumerate(flat)]
    triple_map = db.fetch_osm_road_segments_by_way_endpoint_triples(triples, conn=conn)
    way_ids = sorted(
        {
            int(e.get("way_id") or 0)
            for e in flat
            if not trace_edge_has_osm_endpoint_ids(e) and int(e.get("way_id") or 0) > 0
        }
    )
    way_map = db.fetch_osm_road_segments_by_way_ids(way_ids, conn=conn) if way_ids else {}

    ids, unresolved = resolve_trace_edges_to_segment_ids(
        flat,
        triple_map,
        v3_import_source=IMPORT_SOURCE_V3,
        way_to_rows=way_map,
    )
    if ids is None or not ids:
        return PatternOsmMatchResult(
            pid,
            "failed_resolve",
            0,
            trace_notes=nt,
            message=f"could not resolve osm segments (hint={unresolved})",
        )

    conf = _confidence_from_flat_edges(shape_line, flat)
    rows = [
        {
            "seq": i + 1,
            "segment_id": sid,
            "confidence": conf,
            "source": SOURCE_VALHALLA_TRACE,
        }
        for i, sid in enumerate(ids)
    ]
    db.replace_pattern_osm_segments(feed_id=feed_id, pattern_id=pid, rows=rows, conn=conn)

    return PatternOsmMatchResult(pid, "written", len(ids), trace_notes=nt)


def match_single_pattern_to_osm(
    conn,
    *,
    feed_id: int,
    pattern_id: str,
    repr_trip_id: Optional[str],
    repr_shape_id: Optional[str],
    full_trace_max_km: float = 10.0,
    chunk_legs: int = 11,
    chunk_overlap: int = 1,
    costing: str = "bus",
    densify_m: float = 15.0,
    force_refresh: bool = False,
) -> PatternOsmMatchResult:
    """
    Trace representative shape → Valhalla edges → ``osm_road_segments.segment_id`` chain;
    REPLACE rows in ``pattern_osm_segments`` for this pattern.

    Skip authorization uses content fingerprint + succeeded status only (never row counts).
    Caller should commit ``conn`` after a ``written`` result.
    """
    pid = str(pattern_id)
    current_fp = _build_match_fingerprint(
        conn,
        feed_id,
        pid,
        costing=costing,
        densify_m=densify_m,
        full_trace_max_km=full_trace_max_km,
        chunk_legs=chunk_legs,
        chunk_overlap=chunk_overlap,
        repr_trip_id=repr_trip_id,
        repr_shape_id=repr_shape_id,
    )

    def _last_fp() -> Optional[str]:
        fp, _ = ps.get_pattern_osm_match_success_fingerprint(conn, feed_id, pid)
        return fp

    def _last_outcome() -> Optional[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome FROM pattern_osm_match_status
                WHERE feed_id = %s AND pattern_id = %s
                """,
                (feed_id, pid),
            )
            row = cur.fetchone()
        return row.get("outcome") if row else None

    if ps.fast_path_may_skip(_last_fp, _last_outcome, current_fp, force=force_refresh):
        return PatternOsmMatchResult(pid, "skipped_fingerprint", 0, message="fingerprint unchanged")

    if not repr_trip_id:
        return PatternOsmMatchResult(pid, "skipped_no_trip", 0, message="no repr_trip_id")
    shape_line = _shape_line_for_pattern(conn, feed_id, repr_shape_id)
    if shape_line is None or shape_line.is_empty:
        return PatternOsmMatchResult(pid, "skipped_no_shape", 0, message="no representative shape")
    stops = _stop_times_for_trip(conn, feed_id, str(repr_trip_id))
    if len(stops) < 2:
        return PatternOsmMatchResult(pid, "skipped_no_stops", 0, message="fewer than 2 stop_times rows")

    unit = ps.UnitLockKey(
        stage_name=ps.StageName.PATTERN_OSM_MATCH.value,
        feed_id=feed_id,
        pattern_id=pid,
    )

    # Fingerprint / shape reads above may have opened an implicit transaction (autocommit off).
    ps.commit_if_in_transaction(conn)
    prev_autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with ps.rebuild_unit(
            conn,
            unit,
            current_fp,
            force=force_refresh,
            get_last_success_fp=_last_fp,
            get_last_outcome=_last_outcome,
            mark_running=lambda: ps.upsert_pattern_osm_match_status(
                conn, feed_id, pid, input_fingerprint=current_fp, outcome=ps.OUTCOME_RUNNING
            ),
            mark_succeeded=lambda: ps.upsert_pattern_osm_match_status(
                conn, feed_id, pid, input_fingerprint=current_fp, outcome=ps.OUTCOME_SUCCEEDED
            ),
            mark_failed=lambda: ps.upsert_pattern_osm_match_status(
                conn, feed_id, pid, input_fingerprint=current_fp, outcome=ps.OUTCOME_FAILED
            ),
            log_prefix="match_patterns_to_osm",
        ) as ry:
            if ry.skipped_after_lock:
                return PatternOsmMatchResult(
                    pid, "skipped_fingerprint", 0, message="skipped_after_lock"
                )
            res = _do_match_write(
                conn,
                feed_id=feed_id,
                pid=pid,
                repr_trip_id=repr_trip_id,
                repr_shape_id=repr_shape_id,
                full_trace_max_km=full_trace_max_km,
                chunk_legs=chunk_legs,
                chunk_overlap=chunk_overlap,
                costing=costing,
                densify_m=densify_m,
            )
            if res.status != "written":
                ry.failed_outcome = True
            return res
    finally:
        conn.autocommit = prev_autocommit


def match_patterns_to_osm(
    conn,
    *,
    feed_id: int,
    patterns: Sequence[Mapping[str, Any]],
    full_trace_max_km: float = 10.0,
    chunk_legs: int = 11,
    chunk_overlap: int = 1,
    costing: str = "bus",
    densify_m: float = 15.0,
    force_refresh: bool = False,
) -> List[PatternOsmMatchResult]:
    """Run ``match_single_pattern_to_osm`` over pre-selected ``patterns`` rows (``repr_*`` populated)."""
    out: List[PatternOsmMatchResult] = []
    for prow in patterns:
        out.append(
            match_single_pattern_to_osm(
                conn,
                feed_id=feed_id,
                pattern_id=str(prow["pattern_id"]),
                repr_trip_id=str(prow["repr_trip_id"]) if prow.get("repr_trip_id") else None,
                repr_shape_id=str(prow["repr_shape_id"]) if prow.get("repr_shape_id") else None,
                full_trace_max_km=full_trace_max_km,
                chunk_legs=chunk_legs,
                chunk_overlap=chunk_overlap,
                costing=costing,
                densify_m=densify_m,
                force_refresh=force_refresh,
            )
        )
    return out


__all__ = [
    "PatternOsmMatchResult",
    "SOURCE_VALHALLA_TRACE",
    "match_patterns_to_osm",
    "match_single_pattern_to_osm",
]
