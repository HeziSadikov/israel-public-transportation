"""
Rebuild gtfs_bus_way_evidence for the active (or chosen) feed from GTFS shapes_lines
and osm_road_segments (same 30 m buffer logic as ingest_gtfs_postgis).

Use when the table is empty but trips/shapes data already exists — for example after
running precompute without --with-ingest.

  python -m backend.scripts.build_gtfs_bus_way_evidence
  python -m backend.scripts.build_gtfs_bus_way_evidence --database-url postgresql://...
  python -m backend.scripts.build_gtfs_bus_way_evidence --feed-id 42
  python -m backend.scripts.build_gtfs_bus_way_evidence --skip-shapes-lines
"""

from __future__ import annotations

import argparse
import sys
import time

import psycopg2
from psycopg2.extras import DictCursor

from backend.infra import db_access as db
from backend.infra import pipeline_skip as ps
from backend.infra.logging_utils import ensure_cli_action_logging, log


def _connect(database_url: str):
    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def main(argv: list[str] | None = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(
        description=(
            "Rebuild gtfs_bus_way_evidence from shapes_lines + trips + osm_road_segments "
            "(detour v2 GTFS table lookup)."
        )
    )
    ap.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL / db_access.DB_URL).",
    )
    ap.add_argument(
        "--feed-id",
        type=int,
        default=None,
        help="Feed id in feed_versions (default: active feed).",
    )
    ap.add_argument(
        "--skip-shapes-lines",
        action="store_true",
        help="Do not refresh shapes_lines from shapes (faster if shapes_lines is already current).",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even when fingerprint matches last successful run.",
    )
    args = ap.parse_args(argv)

    database_url = args.database_url or db.DB_URL
    log("gtfs-bus-way-evidence", "phase=main start")

    conn = _connect(database_url)
    try:
        if args.feed_id is not None:
            feed_id = int(args.feed_id)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM feed_versions WHERE id = %s LIMIT 1",
                    (feed_id,),
                )
                if cur.fetchone() is None:
                    print(
                        f"[build-gtfs-bus-way-evidence] No feed_versions row for feed_id={feed_id}.",
                        flush=True,
                    )
                    return 1
        else:
            feed_id = db.get_active_feed_id(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::bigint AS n FROM shapes WHERE feed_id = %s",
                (feed_id,),
            )
            n_shapes = int(cur.fetchone()["n"] or 0)
        if n_shapes == 0:
            print(
                "[build-gtfs-bus-way-evidence] shapes has 0 rows for this feed; "
                "run GTFS ingest first (shapes.txt required for this pipeline).",
                flush=True,
            )
            return 1

        ps.ensure_pipeline_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT checksum FROM feed_versions WHERE id = %s", (feed_id,))
            fv = cur.fetchone()
        zip_ck = fv["checksum"] if fv else ""
        osm_fp = ps.get_latest_osm_dataset_fingerprint(conn)
        current_fp = ps.build_gtfs_bus_way_evidence_fingerprint(
            gtfs_zip_checksum=zip_ck or "",
            osm_dataset_fingerprint=osm_fp,
            skip_shapes_lines=bool(args.skip_shapes_lines),
        )
        if not args.force and ps.feed_stage_may_skip(
            conn, feed_id, ps.StageName.GTFS_BUS_WAY_EVIDENCE.value, current_fp, force=False
        ):
            log("gtfs-bus-way-evidence", f"Skip: fingerprint unchanged feed_id={feed_id}")
            print(f"[build-gtfs-bus-way-evidence] Skip: fingerprint unchanged feed_id={feed_id}", flush=True)
            return 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::bigint AS n FROM gtfs_bus_way_evidence WHERE feed_id = %s",
                (feed_id,),
            )
            before = int(cur.fetchone()["n"] or 0)

        ps.mark_feed_running(conn, feed_id, ps.StageName.GTFS_BUS_WAY_EVIDENCE.value, current_fp)
        conn.commit()
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            if not args.skip_shapes_lines:
                log("gtfs-bus-way-evidence", f"phase=rebuild_shapes_lines start feed_id={feed_id}")
                t_sl = time.perf_counter()
                db.rebuild_shapes_lines_for_feed(cur, feed_id)
                log(
                    "gtfs-bus-way-evidence",
                    f"phase=rebuild_shapes_lines done feed_id={feed_id} elapsed_s={time.perf_counter() - t_sl:.2f}",
                )
            log("gtfs-bus-way-evidence", f"phase=rebuild_gtfs_bus_way_evidence start feed_id={feed_id}")
            t_ev = time.perf_counter()
            db.rebuild_gtfs_bus_way_evidence_for_feed(cur, feed_id)
            log(
                "gtfs-bus-way-evidence",
                f"phase=rebuild_gtfs_bus_way_evidence done feed_id={feed_id} elapsed_s={time.perf_counter() - t_ev:.2f}",
            )
        ps.mark_feed_succeeded(
            conn,
            feed_id,
            ps.StageName.GTFS_BUS_WAY_EVIDENCE.value,
            current_fp,
            stats={"rows_before": before},
        )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::bigint AS n FROM gtfs_bus_way_evidence WHERE feed_id = %s",
                (feed_id,),
            )
            after = int(cur.fetchone()["n"] or 0)

        elapsed = time.perf_counter() - t0
        print(
            f"[build-gtfs-bus-way-evidence] feed_id={feed_id} "
            f"gtfs_bus_way_evidence rows: {before} -> {after} (elapsed_s={elapsed:.2f})",
            flush=True,
        )
        log(
            "gtfs-bus-way-evidence",
            f"phase=main done feed_id={feed_id} rows_before={before} rows_after={after} elapsed_s={elapsed:.2f}",
        )
        if after == 0 and before == 0:
            print(
                "[build-gtfs-bus-way-evidence] Warning: table still empty — check trips.shape_id, "
                "shapes_lines geometry, and that osm_road_segments is populated.",
                flush=True,
            )
        return 0
    except Exception as e:
        conn.rollback()
        log("gtfs-bus-way-evidence", f"phase=main error err={e!s}")
        print(f"[build-gtfs-bus-way-evidence] Failed: {e}", flush=True)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
