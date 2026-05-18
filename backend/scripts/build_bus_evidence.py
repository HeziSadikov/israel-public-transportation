"""Rebuild gtfs_bus_segment_evidence / gtfs_bus_turn_evidence from ``pattern_osm_segments``."""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from backend.bus_corridor.build_bus_evidence import build_bus_evidence
from backend.infra import db_access as db
from backend.infra import pipeline_skip as ps
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import ensure_detour_v3_layer


def _connect(database_url: str):
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(
        description="Aggregate pattern_osm_segments into gtfs_bus_segment_evidence and gtfs_bus_turn_evidence."
    )
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument(
        "--feed-id",
        type=int,
        default=None,
        help="Feed version id (default: active feed from feed_versions).",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even when bus_evidence fingerprint matches last successful run.",
    )
    ap.add_argument(
        "--exact-trip-counts",
        action="store_true",
        help=(
            "Use per-pattern trips join (slow). Default uses fast pre-aggregation "
            "by route/direction/shape."
        ),
    )
    args = ap.parse_args(argv)

    db_url = args.database_url or db.DB_URL
    conn = _connect(db_url)
    try:
        log("build_bus_evidence", "schema checks starting")
        db.ensure_pattern_physical_layer_schema(conn=conn)
        ensure_detour_v3_layer(conn)
        ps.ensure_pipeline_schema(conn)
        conn.commit()
        log("build_bus_evidence", "schema checks done")
        feed_id = int(args.feed_id) if args.feed_id is not None else int(db.get_active_feed_id(conn))
        with conn.cursor() as cur:
            cur.execute("SELECT checksum FROM feed_versions WHERE id = %s", (feed_id,))
            row = cur.fetchone()
        zip_ck = row["checksum"] if row else ""
        osm_fp = ps.get_latest_osm_dataset_fingerprint(conn)
        current_fp = ps.build_bus_evidence_fingerprint(
            gtfs_zip_checksum=zip_ck or "",
            osm_dataset_fingerprint=osm_fp,
        )
        if not args.force and ps.feed_stage_may_skip(
            conn, feed_id, ps.StageName.BUS_EVIDENCE.value, current_fp, force=False
        ):
            log("build_bus_evidence", f"Skip: fingerprint unchanged feed_id={feed_id}")
            return 0
        ps.mark_feed_running(conn, feed_id, ps.StageName.BUS_EVIDENCE.value, current_fp)
        conn.commit()
        trip_mode = "exact" if args.exact_trip_counts else "fast"
        log(
            "build_bus_evidence",
            f"feed_id={feed_id} starting aggregate trip_count_mode={trip_mode}",
        )
        out = build_bus_evidence(
            conn,
            feed_id=feed_id,
            commit=True,
            exact_trip_counts=bool(args.exact_trip_counts),
        )
        log("build_bus_evidence", f"feed_id={feed_id} committing")
        ps.mark_feed_succeeded(conn, feed_id, ps.StageName.BUS_EVIDENCE.value, current_fp, stats=out)
        conn.commit()
        log("build_bus_evidence", f"done {out!r}")
        return 0
    except Exception as e:
        conn.rollback()
        log("build_bus_evidence", f"error={e!s}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
