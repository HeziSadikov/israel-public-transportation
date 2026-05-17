"""
Rebuild ``osm_segment_turns`` (M2) from ``osm_road_segments`` + ``osm_turn_restrictions``.

Usage::

    python -m backend.scripts.build_segment_turns
    python -m backend.scripts.build_segment_turns --database-url postgresql://...
    python -m backend.scripts.build_segment_turns --import-run-id 42

Requires directed segments already populated (``import_osm_pbf --with-segments``).
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import psycopg2
from psycopg2.extras import DictCursor

from backend.infra import db_access as db
from backend.infra import pipeline_skip as ps
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import ensure_detour_v3_layer
from backend.osm_import.build_turn_table import build_segment_turns


def _connect(database_url: str):
    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def _latest_success_osm_run(conn) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, osm_dataset_fingerprint
            FROM osm_import_runs
            WHERE status IN ('success', 'verify_only')
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return dict(row) if row else None


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Rebuild osm_segment_turns (legal turn table, M2).")
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument(
        "--import-run-id",
        type=int,
        default=None,
        help="Only rebuild adjacency from segments with this import_run_id (both legs must match).",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even when segment_turns fingerprint matches last successful run.",
    )
    args = ap.parse_args(argv)

    db_url = args.database_url or db.DB_URL
    conn = _connect(db_url)
    run_id = 0
    current_fp = ""
    try:
        ensure_detour_v3_layer(conn)
        ps.ensure_pipeline_schema(conn)

        latest = _latest_success_osm_run(conn)
        osm_fp = (latest or {}).get("osm_dataset_fingerprint") or ps.get_latest_osm_dataset_fingerprint(conn) or ""
        run_id = int(args.import_run_id) if args.import_run_id is not None else int(latest["id"]) if latest else 0
        current_fp = ps.build_segment_turns_fingerprint(
            osm_dataset_fingerprint=osm_fp,
            segment_import_run_id=args.import_run_id,
        )

        if run_id and not args.force:
            last = ps.get_osm_stage_success_fingerprint(conn, run_id, ps.StageName.SEGMENT_TURNS.value)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT outcome FROM osm_pipeline_stages
                    WHERE osm_import_run_id = %s AND stage_name = %s
                    """,
                    (run_id, ps.StageName.SEGMENT_TURNS.value),
                )
                row = cur.fetchone()
            last_outcome = row.get("outcome") if row else None
            if ps.may_skip(current_fp, last, force=False, last_outcome=last_outcome):
                log(
                    "build_segment_turns",
                    f"Skip: fingerprint unchanged (osm_import_run_id={run_id})",
                )
                return 0

        if run_id:
            ps.mark_osm_running(conn, run_id, ps.StageName.SEGMENT_TURNS.value, current_fp)
            conn.commit()

        stats = build_segment_turns(conn, segment_import_run_id=args.import_run_id)
        if run_id:
            ps.mark_osm_succeeded(
                conn,
                run_id,
                ps.StageName.SEGMENT_TURNS.value,
                current_fp,
                stats=stats.to_dict() if hasattr(stats, "to_dict") else dict(stats),
            )
        conn.commit()
        log(
            "build_segment_turns",
            f"done turns_inserted={stats.turns_inserted} legal={stats.legal_turns} forbidden={stats.forbidden_turns}",
        )
        return 0
    except Exception as e:
        conn.rollback()
        if run_id:
            try:
                ps.mark_osm_failed(conn, run_id, ps.StageName.SEGMENT_TURNS.value, current_fp, stats={"error": str(e)})
                conn.commit()
            except Exception:
                pass
        log("build_segment_turns", f"error={e!s}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
