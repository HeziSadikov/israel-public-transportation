"""
Content-addressed pipeline skip / rebuild helpers.

Skip only when: same input_fingerprint + previous outcome=succeeded + not force.
Existence/count checks must never authorize skip (sanity checks after fingerprint only).
"""

from __future__ import annotations

import hashlib
import json
import struct
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

from psycopg2.extras import Json

from backend.infra.logging_utils import log

# Bump when stage processing logic changes (even if inputs unchanged).
INGEST_ALGORITHM_VERSION = "ingest_v1"
PATTERNS_ALGORITHM_VERSION = "patterns_postgis_v1"
GRAPHS_ALGORITHM_VERSION = "graphs_precompute_v1"
OSM_IMPORT_ALGORITHM_VERSION = "osm_pbf_import_v1"
SEGMENT_TURNS_ALGORITHM_VERSION = "segment_turns_v1"
PATTERN_OSM_MATCH_ALGORITHM_VERSION = "pattern_osm_match_v1"
BUS_EVIDENCE_ALGORITHM_VERSION = "bus_evidence_v1"
GTFS_BUS_WAY_EVIDENCE_ALGORITHM_VERSION = "gtfs_bus_way_evidence_v1"
LEGAL_ANCHORS_ALGORITHM_VERSION = "legal_anchors_v1"
PATTERN_EDGE_MATCH_ALGORITHM_VERSION = "pattern_edge_match_v1"

OUTCOME_RUNNING = "running"
OUTCOME_SUCCEEDED = "succeeded"
OUTCOME_FAILED = "failed"

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "migrations"
    / "ensure_content_addressed_pipeline.sql"
)


class StageName(str, Enum):
    INGEST = "ingest"
    PATTERNS = "patterns"
    GRAPHS = "graphs"
    OSM_IMPORT = "osm_import"
    SEGMENT_TURNS = "segment_turns"
    PATTERN_OSM_MATCH = "pattern_osm_match"
    BUS_EVIDENCE = "bus_evidence"
    GTFS_BUS_WAY_EVIDENCE = "gtfs_bus_way_evidence"
    LEGAL_ANCHORS = "legal_anchors"
    PATTERN_EDGE_MATCH = "pattern_edge_match"


@dataclass(frozen=True)
class UnitLockKey:
    stage_name: str
    feed_id: Optional[int] = None
    osm_import_run_id: Optional[int] = None
    pattern_id: Optional[str] = None
    route_id: Optional[str] = None
    direction_id: Optional[str] = None
    match_version: Optional[str] = None

    def lock_raw(self) -> str:
        return ":".join(
            [
                self.stage_name,
                str(self.feed_id or ""),
                str(self.osm_import_run_id or ""),
                str(self.pattern_id or ""),
                str(self.route_id or ""),
                str(self.direction_id or ""),
                str(self.match_version or ""),
            ]
        )


@dataclass
class RebuildYield:
    skipped_after_lock: bool = False
    skipped_fast_path: bool = False


def fingerprint_sha256(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def may_skip(
    current_fp: str,
    last_fp: Optional[str],
    *,
    force: bool,
    last_outcome: Optional[str] = None,
) -> bool:
    if force:
        return False
    if not last_fp or last_outcome != OUTCOME_SUCCEEDED:
        return False
    return current_fp == last_fp


def advisory_lock_key(unit: UnitLockKey) -> Tuple[int, int]:
    digest = hashlib.sha256(unit.lock_raw().encode("utf-8")).digest()
    k1, k2 = struct.unpack(">ii", digest[:8])
    return int(k1), int(k2)


def acquire_unit_lock(conn, unit: UnitLockKey) -> None:
    k1, k2 = advisory_lock_key(unit)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (k1, k2))


def ensure_pipeline_schema(conn) -> None:
    if not _MIGRATION_PATH.exists():
        raise FileNotFoundError(f"Pipeline migration not found: {_MIGRATION_PATH}")
    sql = _MIGRATION_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log("pipeline-skip", f"applied migration {_MIGRATION_PATH.name}")


# ---------------------------------------------------------------------------
# Feed pipeline stages
# ---------------------------------------------------------------------------


def get_feed_stage(
    conn,
    feed_id: int,
    stage_name: str,
) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_fingerprint, outcome, completed_at, stats_json
            FROM feed_pipeline_stages
            WHERE feed_id = %s AND stage_name = %s
            """,
            (feed_id, stage_name),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_feed_stage_success_fingerprint(
    conn,
    feed_id: int,
    stage_name: str,
) -> Optional[str]:
    st = get_feed_stage(conn, feed_id, stage_name)
    if st and st.get("outcome") == OUTCOME_SUCCEEDED:
        return st.get("input_fingerprint")
    return None


def upsert_feed_stage(
    conn,
    feed_id: int,
    stage_name: str,
    *,
    input_fingerprint: str,
    outcome: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    completed = (
        datetime.now(timezone.utc)
        if outcome in (OUTCOME_SUCCEEDED, OUTCOME_FAILED)
        else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO feed_pipeline_stages (
                feed_id, stage_name, input_fingerprint, outcome, completed_at, stats_json
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (feed_id, stage_name) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                outcome = EXCLUDED.outcome,
                completed_at = EXCLUDED.completed_at,
                stats_json = EXCLUDED.stats_json
            """,
            (
                feed_id,
                stage_name,
                input_fingerprint,
                outcome,
                completed,
                Json(stats) if stats is not None else None,
            ),
        )


def mark_feed_running(
    conn, feed_id: int, stage_name: str, input_fingerprint: str
) -> None:
    upsert_feed_stage(
        conn,
        feed_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_RUNNING,
    )


def mark_feed_succeeded(
    conn,
    feed_id: int,
    stage_name: str,
    input_fingerprint: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    upsert_feed_stage(
        conn,
        feed_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_SUCCEEDED,
        stats=stats,
    )


def mark_feed_failed(
    conn,
    feed_id: int,
    stage_name: str,
    input_fingerprint: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    upsert_feed_stage(
        conn,
        feed_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_FAILED,
        stats=stats,
    )


# ---------------------------------------------------------------------------
# OSM pipeline stages
# ---------------------------------------------------------------------------


def get_osm_stage_success_fingerprint(
    conn,
    osm_import_run_id: int,
    stage_name: str,
) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_fingerprint, outcome
            FROM osm_pipeline_stages
            WHERE osm_import_run_id = %s AND stage_name = %s
            """,
            (osm_import_run_id, stage_name),
        )
        row = cur.fetchone()
    if row and row.get("outcome") == OUTCOME_SUCCEEDED:
        return row.get("input_fingerprint")
    return None


def upsert_osm_stage(
    conn,
    osm_import_run_id: int,
    stage_name: str,
    *,
    input_fingerprint: str,
    outcome: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    completed = (
        datetime.now(timezone.utc)
        if outcome in (OUTCOME_SUCCEEDED, OUTCOME_FAILED)
        else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO osm_pipeline_stages (
                osm_import_run_id, stage_name, input_fingerprint, outcome, completed_at, stats_json
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (osm_import_run_id, stage_name) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                outcome = EXCLUDED.outcome,
                completed_at = EXCLUDED.completed_at,
                stats_json = EXCLUDED.stats_json
            """,
            (
                osm_import_run_id,
                stage_name,
                input_fingerprint,
                outcome,
                completed,
                Json(stats) if stats is not None else None,
            ),
        )


def mark_osm_running(
    conn, osm_import_run_id: int, stage_name: str, input_fingerprint: str
) -> None:
    upsert_osm_stage(
        conn,
        osm_import_run_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_RUNNING,
    )


def mark_osm_succeeded(
    conn,
    osm_import_run_id: int,
    stage_name: str,
    input_fingerprint: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    upsert_osm_stage(
        conn,
        osm_import_run_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_SUCCEEDED,
        stats=stats,
    )


def mark_osm_failed(
    conn,
    osm_import_run_id: int,
    stage_name: str,
    input_fingerprint: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    upsert_osm_stage(
        conn,
        osm_import_run_id,
        stage_name,
        input_fingerprint=input_fingerprint,
        outcome=OUTCOME_FAILED,
        stats=stats,
    )


def find_successful_osm_import_by_dataset_fingerprint(
    conn,
    osm_dataset_fingerprint: str,
) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM osm_import_runs
            WHERE status IN ('success', 'verify_only')
              AND osm_dataset_fingerprint = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (osm_dataset_fingerprint,),
        )
        row = cur.fetchone()
    return int(row["id"]) if row else None


def set_osm_import_dataset_fingerprint(
    conn, run_id: int, osm_dataset_fingerprint: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE osm_import_runs
            SET osm_dataset_fingerprint = %s
            WHERE id = %s
            """,
            (osm_dataset_fingerprint, run_id),
        )


def get_latest_osm_dataset_fingerprint(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT osm_dataset_fingerprint
            FROM osm_import_runs
            WHERE status IN ('success', 'verify_only')
              AND osm_dataset_fingerprint IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    fp = row.get("osm_dataset_fingerprint") if row else None
    return str(fp) if fp else None


# ---------------------------------------------------------------------------
# Pattern OSM match status
# ---------------------------------------------------------------------------


def get_pattern_osm_match_success_fingerprint(
    conn, feed_id: int, pattern_id: str
) -> Tuple[Optional[str], Optional[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_fingerprint, outcome
            FROM pattern_osm_match_status
            WHERE feed_id = %s AND pattern_id = %s
            """,
            (feed_id, pattern_id),
        )
        row = cur.fetchone()
    if row and row.get("outcome") == OUTCOME_SUCCEEDED:
        return row.get("input_fingerprint"), row.get("outcome")
    return None, row.get("outcome") if row else None


def upsert_pattern_osm_match_status(
    conn,
    feed_id: int,
    pattern_id: str,
    *,
    input_fingerprint: str,
    outcome: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    completed = (
        datetime.now(timezone.utc)
        if outcome in (OUTCOME_SUCCEEDED, OUTCOME_FAILED)
        else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pattern_osm_match_status (
                feed_id, pattern_id, input_fingerprint, outcome, completed_at, stats_json
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (feed_id, pattern_id) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                outcome = EXCLUDED.outcome,
                completed_at = EXCLUDED.completed_at,
                stats_json = EXCLUDED.stats_json
            """,
            (
                feed_id,
                pattern_id,
                input_fingerprint,
                outcome,
                completed,
                Json(stats) if stats is not None else None,
            ),
        )


# ---------------------------------------------------------------------------
# Pattern edge match status
# ---------------------------------------------------------------------------


def get_pattern_edge_match_success_fingerprint(
    conn, feed_id: int, pattern_id: str, match_version: str
) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_fingerprint, outcome
            FROM pattern_edge_match_status
            WHERE feed_id = %s AND pattern_id = %s AND match_version = %s
            """,
            (feed_id, pattern_id, match_version),
        )
        row = cur.fetchone()
    if row and row.get("outcome") == OUTCOME_SUCCEEDED:
        return row.get("input_fingerprint")
    return None


def upsert_pattern_edge_match_status(
    conn,
    feed_id: int,
    pattern_id: str,
    match_version: str,
    *,
    input_fingerprint: str,
    outcome: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    completed = (
        datetime.now(timezone.utc)
        if outcome in (OUTCOME_SUCCEEDED, OUTCOME_FAILED)
        else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pattern_edge_match_status (
                feed_id, pattern_id, match_version, input_fingerprint, outcome, completed_at, stats_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (feed_id, pattern_id, match_version) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                outcome = EXCLUDED.outcome,
                completed_at = EXCLUDED.completed_at,
                stats_json = EXCLUDED.stats_json
            """,
            (
                feed_id,
                pattern_id,
                match_version,
                input_fingerprint,
                outcome,
                completed,
                Json(stats) if stats is not None else None,
            ),
        )


# ---------------------------------------------------------------------------
# Legal anchor pipeline columns
# ---------------------------------------------------------------------------


def get_legal_anchor_pipeline_success_fingerprint(
    conn,
    feed_version: str,
    pattern_id: str,
    anchor_version: str,
) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_fingerprint, pipeline_outcome
            FROM pattern_legal_anchor_pattern_status
            WHERE feed_version = %s AND pattern_id = %s AND anchor_version = %s
            """,
            (feed_version, pattern_id, anchor_version),
        )
        row = cur.fetchone()
    if row and row.get("pipeline_outcome") == OUTCOME_SUCCEEDED:
        return row.get("input_fingerprint")
    return None


def upsert_legal_anchor_pipeline_status(
    conn,
    feed_version: str,
    pattern_id: str,
    anchor_version: str,
    *,
    input_fingerprint: str,
    pipeline_outcome: str,
    business_outcome: Optional[str] = None,
    row_count: int = 0,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pattern_legal_anchor_pattern_status (
                feed_version, pattern_id, anchor_version,
                outcome, row_count, updated_at,
                input_fingerprint, pipeline_outcome
            )
            VALUES (%s, %s, %s, COALESCE(%s, 'pending'), %s, NOW(), %s, %s)
            ON CONFLICT (feed_version, pattern_id, anchor_version) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                pipeline_outcome = EXCLUDED.pipeline_outcome,
                outcome = COALESCE(EXCLUDED.outcome, pattern_legal_anchor_pattern_status.outcome),
                row_count = EXCLUDED.row_count,
                updated_at = NOW()
            """,
            (
                feed_version,
                pattern_id,
                anchor_version,
                business_outcome,
                row_count,
                input_fingerprint,
                pipeline_outcome,
            ),
        )


# ---------------------------------------------------------------------------
# Fingerprint builders
# ---------------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_osm_dataset_fingerprint(
    *,
    pbf_sha256: str,
    cli: Dict[str, Any],
    algorithm_version: str = OSM_IMPORT_ALGORITHM_VERSION,
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.OSM_IMPORT.value,
            "algorithm_version": algorithm_version,
            "cli": cli,
            "upstream": {"pbf_sha256": pbf_sha256},
        }
    )


def build_patterns_stage_fingerprint(
    *,
    gtfs_zip_checksum: str,
    cli: Dict[str, Any],
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.PATTERNS.value,
            "algorithm_version": PATTERNS_ALGORITHM_VERSION,
            "cli": cli,
            "upstream": {"gtfs_zip_checksum": gtfs_zip_checksum},
        }
    )


def build_segment_turns_fingerprint(
    *,
    osm_dataset_fingerprint: str,
    segment_import_run_id: Optional[int] = None,
) -> str:
    upstream: Dict[str, Any] = {"osm_dataset_fingerprint": osm_dataset_fingerprint}
    if segment_import_run_id is not None:
        upstream["segment_import_run_id"] = segment_import_run_id
    return fingerprint_sha256(
        {
            "stage": StageName.SEGMENT_TURNS.value,
            "algorithm_version": SEGMENT_TURNS_ALGORITHM_VERSION,
            "upstream": upstream,
        }
    )


def build_pattern_osm_match_fingerprint(
    *,
    pattern_signature: str,
    osm_dataset_fingerprint: str,
    costing: str,
    densify_m: float,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.PATTERN_OSM_MATCH.value,
            "algorithm_version": PATTERN_OSM_MATCH_ALGORITHM_VERSION,
            "upstream": {
                "pattern_signature": pattern_signature,
                "osm_dataset_fingerprint": osm_dataset_fingerprint,
            },
            "external_config": {
                "costing": costing,
                "densify_m": densify_m,
                "full_trace_max_km": full_trace_max_km,
                "chunk_legs": chunk_legs,
                "chunk_overlap": chunk_overlap,
            },
        }
    )


def build_bus_evidence_fingerprint(
    *,
    gtfs_zip_checksum: str,
    osm_dataset_fingerprint: Optional[str],
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.BUS_EVIDENCE.value,
            "algorithm_version": BUS_EVIDENCE_ALGORITHM_VERSION,
            "upstream": {
                "gtfs_zip_checksum": gtfs_zip_checksum,
                "osm_dataset_fingerprint": osm_dataset_fingerprint,
            },
        }
    )


def build_gtfs_bus_way_evidence_fingerprint(
    *,
    gtfs_zip_checksum: str,
    osm_dataset_fingerprint: Optional[str],
    skip_shapes_lines: bool,
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.GTFS_BUS_WAY_EVIDENCE.value,
            "algorithm_version": GTFS_BUS_WAY_EVIDENCE_ALGORITHM_VERSION,
            "cli": {"skip_shapes_lines": skip_shapes_lines},
            "upstream": {
                "gtfs_zip_checksum": gtfs_zip_checksum,
                "osm_dataset_fingerprint": osm_dataset_fingerprint,
            },
        }
    )


def build_legal_anchors_fingerprint(
    *,
    pattern_signature: str,
    anchor_version: str,
    trace_cache_version: str,
    valhalla_base_url: Optional[str],
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.LEGAL_ANCHORS.value,
            "algorithm_version": LEGAL_ANCHORS_ALGORITHM_VERSION,
            "upstream": {"pattern_signature": pattern_signature},
            "external_config": {
                "anchor_version": anchor_version,
                "trace_cache_version": trace_cache_version,
                "valhalla_base_url": valhalla_base_url or "",
            },
        }
    )


def build_pattern_edge_match_fingerprint(
    *,
    pattern_signature: str,
    match_version: str,
    full_trace_max_km: float,
    chunk_legs: int,
    chunk_overlap: int,
) -> str:
    return fingerprint_sha256(
        {
            "stage": StageName.PATTERN_EDGE_MATCH.value,
            "algorithm_version": PATTERN_EDGE_MATCH_ALGORITHM_VERSION,
            "upstream": {"pattern_signature": pattern_signature},
            "external_config": {
                "match_version": match_version,
                "full_trace_max_km": full_trace_max_km,
                "chunk_legs": chunk_legs,
                "chunk_overlap": chunk_overlap,
            },
        }
    )


def legacy_patterns_fingerprint_from_checksum(zip_checksum: str) -> str:
    """Back-compat: patterns_built_checksum == zip checksum."""
    return build_patterns_stage_fingerprint(
        gtfs_zip_checksum=zip_checksum,
        cli={"legacy_checksum_only": True},
    )


def feed_stage_may_skip(
    conn,
    feed_id: int,
    stage_name: str,
    current_fp: str,
    *,
    force: bool,
) -> bool:
    last = get_feed_stage_success_fingerprint(conn, feed_id, stage_name)
    st = get_feed_stage(conn, feed_id, stage_name)
    last_outcome = st.get("outcome") if st else None
    return may_skip(current_fp, last, force=force, last_outcome=last_outcome)


# ---------------------------------------------------------------------------
# Rebuild transaction helper
# ---------------------------------------------------------------------------


@contextmanager
def rebuild_unit(
    conn,
    unit: UnitLockKey,
    current_fp: str,
    *,
    force: bool,
    get_last_success_fp: Callable[[], Optional[str]],
    get_last_outcome: Callable[[], Optional[str]],
    mark_running: Callable[[], None],
    mark_succeeded: Callable[[], None],
    mark_failed: Callable[[], None],
    log_prefix: str = "pipeline-skip",
) -> Iterator[RebuildYield]:
    """
    Canonical rebuild flow inside one transaction (caller sets conn.autocommit=False).

    Yields RebuildYield; if skipped_after_lock, caller must not write output.
    On exception: rolls back rebuild txn, then mark_failed in a separate committed txn.
    """
    result = RebuildYield()
    prev_autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with conn:
            acquire_unit_lock(conn, unit)
            last_fp = get_last_success_fp()
            last_outcome = get_last_outcome()
            if may_skip(current_fp, last_fp, force=force, last_outcome=last_outcome):
                log(
                    log_prefix,
                    f"skipped_after_lock stage={unit.stage_name} fp={current_fp[:12]}...",
                )
                result.skipped_after_lock = True
                yield result
                return
            mark_running()
            yield result
            if not result.skipped_after_lock:
                mark_succeeded()
    except Exception as e:
        conn.rollback()
        conn.autocommit = True
        try:
            mark_failed()
        except Exception as mark_err:
            log(log_prefix, f"mark_failed error={mark_err!s}")
        log(log_prefix, f"rebuild failed stage={unit.stage_name} error={e!s}")
        raise
    finally:
        conn.autocommit = prev_autocommit


def fast_path_may_skip(
    get_last_success_fp: Callable[[], Optional[str]],
    get_last_outcome: Callable[[], Optional[str]],
    current_fp: str,
    *,
    force: bool,
) -> bool:
    return may_skip(
        current_fp,
        get_last_success_fp(),
        force=force,
        last_outcome=get_last_outcome(),
    )
