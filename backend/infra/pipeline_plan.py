"""
Read-only pipeline planner for precompute_all_postgis.

Plans skip | run | run_partial per artifact using feed_pipeline_stages and caches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from psycopg2 import errors as pg_errors

from backend.infra import pipeline_skip as ps

# Stable artifact ordering for plan output and tests.
ARTIFACT_ORDER: tuple[str, ...] = (
    "ingest",
    "patterns",
    "gtfs_bus_way_evidence",
    "osm_import",
    "segment_turns",
    "pattern_osm_match",
    "bus_evidence",
    "graphs",
)

PLAN_COLUMNS = (
    "artifact",
    "action",
    "reason",
    "current_signature",
    "stored_signature",
    "outcome",
)


@dataclass
class PlanRequest:
    with_ingest: bool = False
    skip_patterns: bool = False
    skip_graphs: bool = False
    rebuild_gtfs_bus_way_evidence: bool = False
    with_osm_import: bool = False
    with_segment_turns: bool = False
    with_pattern_osm_match: bool = False
    with_bus_evidence: bool = False
    ingest_force: bool = False
    force_all: bool = False
    force_patterns: bool = False
    force_artifacts: Set[str] = field(default_factory=set)
    profiles: Sequence[str] = ("weekday", "friday", "saturday", "sunday")
    with_pretty_osm: bool = False
    fast_preview_geojson: bool = True
    gtfs_zip_path: Optional[Path] = None
    from_stage: Optional[str] = None
    to_stage: Optional[str] = None
    explain_skips: bool = False


@dataclass(frozen=True)
class PlanStep:
    artifact: str
    action: str  # skip | run | run_partial
    reason: str
    current_signature: str = ""
    stored_signature: str = ""
    outcome: str = ""


def _force(req: PlanRequest, artifact: str) -> bool:
    if req.force_all:
        return True
    if artifact in req.force_artifacts:
        return True
    if artifact == "ingest" and req.ingest_force:
        return True
    if artifact == "patterns" and req.force_patterns:
        return True
    return False


def _in_stage_range(req: PlanRequest, artifact: str) -> bool:
    if req.from_stage and ARTIFACT_ORDER.index(artifact) < ARTIFACT_ORDER.index(req.from_stage):
        return False
    if req.to_stage and ARTIFACT_ORDER.index(artifact) > ARTIFACT_ORDER.index(req.to_stage):
        return False
    return True


def _row_to_dict(row: Any, cur) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if hasattr(row, "keys"):
        return dict(row)
    if cur.description:
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    return None


def _safe_query(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _row_to_dict(cur.fetchone(), cur)
    except pg_errors.UndefinedTable:
        conn.rollback()
        return None


def _active_feed_checksum(conn) -> tuple[Optional[int], str]:
    row = _safe_query(
        conn,
        """
        SELECT id, checksum
        FROM feed_versions
        WHERE active = TRUE
        ORDER BY id DESC
        LIMIT 1
        """,
    )
    if not row:
        return None, ""
    return int(row["id"]), str(row.get("checksum") or "")


def _plan_ingest(conn, req: PlanRequest, zip_ck: str) -> PlanStep:
    artifact = "ingest"
    if not req.with_ingest:
        return PlanStep(artifact, "skip", "not requested")
    if _force(req, artifact):
        return PlanStep(
            artifact,
            "run",
            "ingest --force (new feed_versions row)",
            current_signature=zip_ck,
        )
    if not zip_ck:
        return PlanStep(artifact, "run", "no local zip checksum available")
    match = ps.find_feed_by_zip_checksum(conn, zip_ck)
    if match is None:
        return PlanStep(artifact, "run", "no feed_versions row for zip checksum", current_signature=zip_ck)
    st = ps.get_feed_stage(conn, match.feed_id, ps.StageName.INGEST.value)
    outcome = (st or {}).get("outcome") or ""
    stored = (st or {}).get("input_fingerprint") or ""
    registry_note = ""
    if st is None and not ps.feed_pipeline_stages_available(conn):
        registry_note = " (feed_pipeline_stages not migrated yet; using feed_versions checksum only)"
    if match.active and (
        (outcome == ps.OUTCOME_SUCCEEDED and stored == zip_ck)
        or (st is None and not _force(req, artifact))
    ):
        return PlanStep(
            artifact,
            "skip",
            f"reused existing feed_id={match.feed_id} by checksum{registry_note}",
            current_signature=zip_ck,
            stored_signature=stored,
            outcome=outcome or "n/a",
        )
    if match.active:
        return PlanStep(
            artifact,
            "skip",
            f"reused existing feed_id={match.feed_id} by checksum (stage refresh only){registry_note}",
            current_signature=zip_ck,
            stored_signature=stored,
            outcome=outcome,
        )
    return PlanStep(
        artifact,
        "skip",
        f"reactivate feed_id={match.feed_id} by checksum (no GTFS reload){registry_note}",
        current_signature=zip_ck,
        stored_signature=stored,
        outcome=outcome or "n/a",
    )


def _plan_feed_stage(
    conn,
    req: PlanRequest,
    *,
    artifact: str,
    stage_name: str,
    current_fp: str,
    feed_id: Optional[int],
    requested: bool,
    ingest_creates_new_feed: bool,
) -> PlanStep:
    if not requested:
        return PlanStep(artifact, "skip", "not requested")
    if feed_id is None:
        return PlanStep(artifact, "run", "no active feed", current_signature=current_fp[:16])
    if ingest_creates_new_feed:
        return PlanStep(
            artifact,
            "run",
            "ingest --force will create new feed_id",
            current_signature=current_fp[:16],
        )
    if _force(req, artifact):
        return PlanStep(artifact, "run", f"--force for {artifact}", current_signature=current_fp[:16])
    st = ps.get_feed_stage(conn, feed_id, stage_name)
    outcome = (st or {}).get("outcome") or ""
    stored = (st or {}).get("input_fingerprint") or ""
    if ps.may_skip(current_fp, stored, force=False, last_outcome=outcome):
        reason = "fingerprint match" if req.explain_skips else "unchanged"
        return PlanStep(
            artifact,
            "skip",
            reason,
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )
    if outcome == ps.OUTCOME_FAILED:
        return PlanStep(
            artifact,
            "run",
            "prior outcome failed",
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )
    if outcome == ps.OUTCOME_RUNNING:
        return PlanStep(
            artifact,
            "run",
            "prior outcome running/stale",
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )
    return PlanStep(
        artifact,
        "run",
        "fingerprint changed or no prior success",
        current_signature=current_fp[:16],
        stored_signature=(stored or "")[:16],
        outcome=outcome,
    )


def _scalar_count(conn, sql: str, params: tuple) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        if row is None:
            return 0
        return int(row[0])
    except pg_errors.UndefinedTable:
        conn.rollback()
        return None


def _count_route_pairs(conn, feed_id: int) -> int:
    n = _scalar_count(
        conn,
        """
        SELECT COUNT(*)::int FROM (
            SELECT DISTINCT route_id, direction_id
            FROM trips WHERE feed_id = %s
        ) s
        """,
        (feed_id,),
    )
    return n if n is not None else 0


def _count_graph_cache_routes(
    conn, feed_id: int, pretty_osm: bool
) -> Optional[int]:
    return _scalar_count(
        conn,
        """
        SELECT COUNT(*)::int FROM (
            SELECT DISTINCT route_id, direction_id
            FROM route_graph_cache
            WHERE feed_id = %s AND pretty_osm = %s
        ) s
        """,
        (feed_id, pretty_osm),
    )


def _count_preview_cache_routes(
    conn, feed_id: int, pretty_osm: bool, profile_key: str
) -> Optional[int]:
    return _scalar_count(
        conn,
        """
        SELECT COUNT(*)::int FROM (
            SELECT DISTINCT route_id, direction_id
            FROM route_preview_cache
            WHERE feed_id = %s AND pretty_osm = %s AND profile_key = %s
        ) s
        """,
        (feed_id, pretty_osm, profile_key),
    )


def _plan_graphs(
    conn,
    req: PlanRequest,
    feed_id: Optional[int],
    zip_ck: str,
    ingest_creates_new_feed: bool,
) -> PlanStep:
    artifact = "graphs"
    if req.skip_graphs:
        return PlanStep(artifact, "skip", "not requested (--skip-graphs)")
    profiles = list(req.profiles)
    current_fp = ps.build_graphs_stage_fingerprint(
        gtfs_zip_checksum=zip_ck,
        profiles=profiles,
        with_pretty_osm=req.with_pretty_osm,
        fast_preview_geojson=req.fast_preview_geojson,
    )
    if feed_id is None:
        return PlanStep(artifact, "run", "no active feed", current_signature=current_fp[:16])
    if ingest_creates_new_feed:
        return PlanStep(
            artifact,
            "run",
            "ingest --force will create new feed_id",
            current_signature=current_fp[:16],
        )
    if _force(req, artifact):
        return PlanStep(artifact, "run", "--force graphs", current_signature=current_fp[:16])

    st = ps.get_feed_stage(conn, feed_id, ps.StageName.GRAPHS.value)
    outcome = (st or {}).get("outcome") or ""
    stored = (st or {}).get("input_fingerprint") or ""
    fp_mismatch = not ps.may_skip(
        current_fp, stored, force=False, last_outcome=outcome
    )

    pretty_variants = [False]
    if req.with_pretty_osm:
        pretty_variants.append(True)

    total_routes = _count_route_pairs(conn, feed_id)
    if total_routes == 0:
        return PlanStep(artifact, "skip", "no trips for active feed", current_signature=current_fp[:16])

    missing_graph = 0
    missing_preview = 0
    cache_tables_missing = False
    for pretty in pretty_variants:
        cached = _count_graph_cache_routes(conn, feed_id, pretty)
        if cached is None:
            cache_tables_missing = True
            break
        missing_graph += max(0, total_routes - cached)
        for prof in profiles:
            prev = _count_preview_cache_routes(conn, feed_id, pretty, prof)
            if prev is None:
                cache_tables_missing = True
                break
            missing_preview += max(0, total_routes - prev)
        if cache_tables_missing:
            break

    if cache_tables_missing:
        return PlanStep(
            artifact,
            "run",
            "route_graph_cache/route_preview_cache unavailable or empty",
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )

    if fp_mismatch:
        return PlanStep(
            artifact,
            "run",
            "graphs fingerprint or outcome changed",
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )
    if missing_graph > 0 or missing_preview > 0:
        return PlanStep(
            artifact,
            "run_partial",
            f"missing graph_routes={missing_graph} preview_slots={missing_preview} of {total_routes}",
            current_signature=current_fp[:16],
            stored_signature=(stored or "")[:16],
            outcome=outcome,
        )
    reason = "cache complete" if req.explain_skips else "unchanged"
    return PlanStep(
        artifact,
        "skip",
        reason,
        current_signature=current_fp[:16],
        stored_signature=(stored or "")[:16],
        outcome=outcome,
    )


def _latest_osm_import_run_id(conn) -> Optional[int]:
    row = _safe_query(
        conn,
        """
        SELECT id FROM osm_import_runs
        ORDER BY id DESC
        LIMIT 1
        """,
    )
    return int(row["id"]) if row else None


def _plan_osm_stage(
    conn,
    req: PlanRequest,
    *,
    artifact: str,
    stage_name: str,
    current_fp: str,
    requested: bool,
    osm_registry_available: bool,
) -> PlanStep:
    if not requested:
        return PlanStep(artifact, "skip", "not requested")
    if not osm_registry_available:
        return PlanStep(
            artifact,
            "run",
            "OSM registry unavailable (osm_import_runs missing; import OSM first)",
            current_signature=current_fp[:16],
        )
    if _force(req, artifact):
        return PlanStep(artifact, "run", f"--force for {artifact}", current_signature=current_fp[:16])
    if not ps.osm_pipeline_stages_available(conn):
        return PlanStep(
            artifact,
            "run",
            "osm_pipeline_stages not migrated (re-run ensure_pipeline_schema after OSM import)",
            current_signature=current_fp[:16],
        )
    run_id = _latest_osm_import_run_id(conn)
    if run_id is None:
        return PlanStep(artifact, "run", "no osm_import_runs row", current_signature=current_fp[:16])
    st_row = _safe_query(
        conn,
        """
        SELECT input_fingerprint, outcome
        FROM osm_pipeline_stages
        WHERE osm_import_run_id = %s AND stage_name = %s
        """,
        (run_id, stage_name),
    )
    outcome = str((st_row or {}).get("outcome") or "")
    stored = str((st_row or {}).get("input_fingerprint") or "")

    if ps.may_skip(current_fp, stored, force=False, last_outcome=outcome):
        return PlanStep(
            artifact,
            "skip",
            "fingerprint match" if req.explain_skips else "unchanged",
            current_signature=current_fp[:16],
            stored_signature=stored[:16],
            outcome=outcome,
        )
    if outcome == ps.OUTCOME_FAILED:
        return PlanStep(
            artifact,
            "run",
            "prior outcome failed",
            current_signature=current_fp[:16],
            outcome=outcome,
        )
    return PlanStep(
        artifact,
        "run",
        "fingerprint changed or no prior success",
        current_signature=current_fp[:16],
        stored_signature=stored[:16],
        outcome=outcome,
    )


def compute_plan(conn, req: PlanRequest) -> List[PlanStep]:
    zip_ck = ""
    if req.gtfs_zip_path and req.gtfs_zip_path.is_file():
        zip_ck = ps.sha256_file(req.gtfs_zip_path)

    feed_id, active_ck = _active_feed_checksum(conn)
    if zip_ck and not active_ck:
        active_ck = zip_ck

    ingest_step = _plan_ingest(conn, req, zip_ck)
    ingest_creates_new_feed = bool(
        req.with_ingest
        and ingest_step.action == "run"
        and (
            _force(req, "ingest")
            or ingest_step.reason.startswith("no feed")
        )
    )

    patterns_fp = ps.build_patterns_stage_fingerprint(
        gtfs_zip_checksum=zip_ck or active_ck,
        cli={"legacy_checksum_only": True},
    )
    osm_registry_available = ps.osm_import_registry_available(conn)
    osm_fp = (
        ps.get_latest_osm_dataset_fingerprint(conn) or ""
        if osm_registry_available
        else ""
    )
    gbwe_fp = ps.build_gtfs_bus_way_evidence_fingerprint(
        gtfs_zip_checksum=zip_ck or active_ck,
        osm_dataset_fingerprint=osm_fp,
        skip_shapes_lines=False,
    )
    bus_fp = ps.build_bus_evidence_fingerprint(
        gtfs_zip_checksum=zip_ck or active_ck,
        osm_dataset_fingerprint=osm_fp,
    )
    seg_fp = ps.build_segment_turns_fingerprint(osm_dataset_fingerprint=osm_fp)
    osm_import_fp = osm_fp or "pending"

    steps_map: Dict[str, PlanStep] = {
        "ingest": ingest_step,
        "patterns": _plan_feed_stage(
            conn,
            req,
            artifact="patterns",
            stage_name=ps.StageName.PATTERNS.value,
            current_fp=patterns_fp,
            feed_id=feed_id,
            requested=not req.skip_patterns,
            ingest_creates_new_feed=ingest_creates_new_feed,
        ),
        "gtfs_bus_way_evidence": _plan_feed_stage(
            conn,
            req,
            artifact="gtfs_bus_way_evidence",
            stage_name=ps.StageName.GTFS_BUS_WAY_EVIDENCE.value,
            current_fp=gbwe_fp,
            feed_id=feed_id,
            requested=req.rebuild_gtfs_bus_way_evidence,
            ingest_creates_new_feed=ingest_creates_new_feed,
        ),
        "osm_import": _plan_osm_stage(
            conn,
            req,
            artifact="osm_import",
            stage_name=ps.StageName.OSM_IMPORT.value,
            current_fp=osm_import_fp,
            requested=req.with_osm_import,
            osm_registry_available=osm_registry_available,
        ),
        "segment_turns": _plan_osm_stage(
            conn,
            req,
            artifact="segment_turns",
            stage_name=ps.StageName.SEGMENT_TURNS.value,
            current_fp=seg_fp,
            requested=req.with_segment_turns,
            osm_registry_available=osm_registry_available,
        ),
        "pattern_osm_match": (
            PlanStep(
                "pattern_osm_match",
                "run",
                (
                    "OSM registry unavailable (osm_import_runs missing; import OSM first)"
                    if not osm_registry_available
                    else (
                        "--force pattern_osm_match"
                        if _force(req, "pattern_osm_match")
                        else "per-pattern skip in subprocess"
                    )
                ),
                current_signature=osm_fp[:16] if osm_fp else "",
            )
            if req.with_pattern_osm_match
            else PlanStep("pattern_osm_match", "skip", "not requested")
        ),
        "bus_evidence": _plan_feed_stage(
            conn,
            req,
            artifact="bus_evidence",
            stage_name=ps.StageName.BUS_EVIDENCE.value,
            current_fp=bus_fp,
            feed_id=feed_id,
            requested=req.with_bus_evidence,
            ingest_creates_new_feed=ingest_creates_new_feed,
        ),
        "graphs": _plan_graphs(
            conn,
            req,
            feed_id,
            zip_ck or active_ck,
            ingest_creates_new_feed,
        ),
    }

    if req.with_bus_evidence and req.rebuild_gtfs_bus_way_evidence:
        # Distinct artifacts; no double-run in orchestrator (planner lists both, executor runs each once).
        pass

    out: List[PlanStep] = []
    for name in ARTIFACT_ORDER:
        if name not in steps_map:
            continue
        step = steps_map[name]
        if _in_stage_range(req, name):
            out.append(step)
    return out


def format_plan_table(steps: Sequence[PlanStep]) -> str:
    """Stable column-aligned text for humans and tests."""
    rows: List[List[str]] = [list(PLAN_COLUMNS)]
    for s in steps:
        rows.append(
            [
                s.artifact,
                s.action,
                s.reason,
                s.current_signature,
                s.stored_signature,
                s.outcome,
            ]
        )
    widths = [max(len(r[i]) for r in rows) for i in range(len(PLAN_COLUMNS))]
    lines = []
    for i, row in enumerate(rows):
        line = "  ".join(cell.ljust(widths[j]) for j, cell in enumerate(row))
        lines.append(line.rstrip())
        if i == 0:
            lines.append("  ".join("-" * w for w in widths))
    return "\n".join(lines)
