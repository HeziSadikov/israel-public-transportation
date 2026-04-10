"""One-off: check DB + time get_routes_in_polygon_range (no server/UI)."""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs")

import psycopg2  # noqa: E402
from psycopg2.extras import DictCursor  # noqa: E402

from backend.logging_utils import ensure_cli_action_logging, log as action_log  # noqa: E402
from backend.area_routes_canonical_fixtures import (  # noqa: E402
    CANONICAL_AREA_DATE_YMD,
    CANONICAL_AREA_END_SEC,
    CANONICAL_AREA_POLYGON_WKT,
    CANONICAL_AREA_START_SEC,
)
from backend.db_access import get_active_feed_id, get_routes_in_polygon_range  # noqa: E402


def main() -> None:
    ensure_cli_action_logging()
    action_log("bench_area_pg", "phase=main start")
    def log(msg: str) -> None:
        print(msg, flush=True)

    url = os.environ["DATABASE_URL"]
    try:
        action_log("bench_area_pg", "phase=connect_db start")
        conn = psycopg2.connect(url, cursor_factory=DictCursor)
        action_log("bench_area_pg", "phase=connect_db done")
    except Exception as e:
        log(f"CONNECT_FAIL {e}")
        action_log("bench_area_pg", f"phase=connect_db error={e!s}")
        sys.exit(2)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'trips' AND indexname = 'idx_trips_feed_shape'
        """
    )
    log(f"idx_trips_feed_shape: {cur.fetchone()}")
    cur.execute("SELECT COUNT(*) FROM trips")
    log(f"trips_count: {cur.fetchone()[0]}")
    fid = get_active_feed_id(conn)
    log(f"active_feed_id: {fid}")

    wkt = CANONICAL_AREA_POLYGON_WKT
    t0 = time.perf_counter()
    cur.execute(
        """
        SELECT COUNT(*) FROM shapes_lines sl
        WHERE sl.feed_id = %s
          AND ST_Intersects(sl.geom, ST_GeomFromText(%s, 4326))
        """,
        (fid, wkt),
    )
    log(f"shapes_hits_count: {cur.fetchone()[0]} in {round(time.perf_counter() - t0, 3)}s")

    t0 = time.perf_counter()
    cur.execute(
        """
        SELECT COUNT(*) FROM trips t
        INNER JOIN (
            SELECT sl.feed_id, sl.shape_id FROM shapes_lines sl
            WHERE sl.feed_id = %s
              AND ST_Intersects(sl.geom, ST_GeomFromText(%s, 4326))
        ) sia ON sia.feed_id = t.feed_id AND t.shape_id = sia.shape_id
        WHERE t.feed_id = %s
        """,
        (fid, wkt, fid),
    )
    log(f"trips_matching_shapes: {cur.fetchone()[0]} in {round(time.perf_counter() - t0, 3)}s")

    cur.execute("SET statement_timeout = '180000'")  # 180s
    cur.close()

    # Small polygon over Tel Aviv-ish bbox (WKT 4326)
    t0 = time.perf_counter()
    action_log("bench_area_pg", "phase=area_query start")
    try:
        rows = get_routes_in_polygon_range(
            polygon_wkt=wkt,
            start_date_ymd=CANONICAL_AREA_DATE_YMD,
            start_sec=CANONICAL_AREA_START_SEC,
            end_date_ymd=CANONICAL_AREA_DATE_YMD,
            end_sec=CANONICAL_AREA_END_SEC,
            conn=conn,
        )
        dt = time.perf_counter() - t0
        log(f"AREA_QUERY_OK rows={len(rows)} seconds={round(dt, 3)}")
        action_log("bench_area_pg", f"phase=area_query done rows={len(rows)} elapsed_s={dt:.3f}")
    except Exception as e:
        dt = time.perf_counter() - t0
        log(f"AREA_QUERY_FAIL after_s={round(dt, 3)} err={e!r}")
        action_log("bench_area_pg", f"phase=area_query error elapsed_s={dt:.3f} err={e!r}")
        sys.exit(1)
    finally:
        conn.close()
        action_log("bench_area_pg", "phase=main done")


if __name__ == "__main__":
    main()
