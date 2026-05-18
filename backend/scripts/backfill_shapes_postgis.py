"""
Load shapes.txt + shapes_lines for an existing feed (e.g. after checksum reactivation skipped full ingest).

Usage::

    python -m backend.scripts.backfill_shapes_postgis
    python -m backend.scripts.backfill_shapes_postgis --feed-id 21
"""

from __future__ import annotations

import argparse
import time
import zipfile
from pathlib import Path
from typing import Dict, Tuple

import psycopg2

from backend.infra.config import LOCAL_GTFS_ZIP
from backend.infra.db_access import DB_URL, get_active_feed_id, rebuild_shapes_lines_for_feed
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.scripts.ingest_gtfs_postgis import _bulk_insert, _read_csv_from_zip


def backfill_shapes(
    gtfs_zip: Path,
    database_url: str,
    feed_id: int,
) -> None:
    conn = psycopg2.connect(database_url)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shapes_lines WHERE feed_id = %s", (feed_id,))
            cur.execute("DELETE FROM shapes WHERE feed_id = %s", (feed_id,))
            log("backfill-shapes", f"cleared shapes for feed_id={feed_id}")

        with zipfile.ZipFile(gtfs_zip, "r") as zf:
            t0 = time.perf_counter()
            shapes_points: Dict[Tuple[str, int], Tuple[float, float, float | None]] = {}
            for row in _read_csv_from_zip(zf, "shapes.txt"):
                shape_id = row.get("shape_id")
                if not shape_id:
                    continue
                try:
                    seq = int(row.get("shape_pt_sequence") or 0)
                except ValueError:
                    continue
                lat = float(row.get("shape_pt_lat") or 0.0)
                lon = float(row.get("shape_pt_lon") or 0.0)
                dist = row.get("shape_dist_traveled")
                try:
                    dist_f = float(dist) if dist is not None and dist != "" else None
                except ValueError:
                    dist_f = None
                shapes_points[(shape_id, seq)] = (lat, lon, dist_f)

            shapes_rows = [
                (feed_id, shape_id, seq, lat, lon, dist_f)
                for (shape_id, seq), (lat, lon, dist_f) in shapes_points.items()
            ]

        with conn.cursor() as cur:
            _bulk_insert(
                cur,
                "shapes",
                ("feed_id", "shape_id", "seq", "lat", "lon", "dist_traveled"),
                shapes_rows,
            )
            conn.commit()
            log(
                "backfill-shapes",
                f"loaded shapes rows={len(shapes_rows)} elapsed_s={time.perf_counter() - t0:.2f}",
            )

            t1 = time.perf_counter()
            rebuild_shapes_lines_for_feed(cur, feed_id)
            conn.commit()
            log(
                "backfill-shapes",
                f"shapes_lines rebuilt feed_id={feed_id} elapsed_s={time.perf_counter() - t1:.2f}",
            )
    finally:
        conn.close()


def main() -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Backfill shapes + shapes_lines for a feed from GTFS zip.")
    ap.add_argument("--gtfs-zip", type=str, default=str(LOCAL_GTFS_ZIP))
    ap.add_argument("--database-url", type=str, default=None)
    ap.add_argument("--feed-id", type=int, default=None, help="Default: active feed_versions.id")
    args = ap.parse_args()
    db = args.database_url or DB_URL
    conn = psycopg2.connect(db)
    try:
        feed_id = int(args.feed_id) if args.feed_id is not None else get_active_feed_id(conn)
    finally:
        conn.close()
    backfill_shapes(Path(args.gtfs_zip), db, feed_id)
    print(f"[backfill-shapes] done feed_id={feed_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
