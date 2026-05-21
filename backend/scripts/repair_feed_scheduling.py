"""
Backfill calendar, calendar_dates, and trip_time_bounds for an existing feed_id.

Use when trips/stop_times were loaded but scheduling tables are empty (area search returns 0).

    python -m backend.scripts.repair_feed_scheduling --feed-id 21
    python -m backend.scripts.repair_feed_scheduling --feed-id 21 --gtfs-zip ./israel-public-transportation.zip
"""

from __future__ import annotations

import argparse
import time
import zipfile
from pathlib import Path

from backend.infra.config import LOCAL_GTFS_ZIP
from backend.infra.db_access import DB_URL
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.scripts.ingest_gtfs_postgis import (
    _bulk_insert,
    _connect,
    _read_csv_from_zip,
)


def repair_feed_scheduling(
    *,
    feed_id: int,
    gtfs_zip: Path,
    database_url: str,
) -> None:
    if not gtfs_zip.is_file():
        raise FileNotFoundError(f"GTFS zip not found: {gtfs_zip}")

    conn = _connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM feed_versions WHERE id = %s", (feed_id,))
            if cur.fetchone() is None:
                raise ValueError(f"feed_versions.id={feed_id} does not exist")

            for table in ("trip_time_bounds", "calendar_dates", "calendar"):
                cur.execute(f"DELETE FROM {table} WHERE feed_id = %s", (feed_id,))
                log("repair/scheduling", f"cleared {table} feed_id={feed_id}")

        conn.commit()

        with zipfile.ZipFile(gtfs_zip, "r") as zf:
            with conn.cursor() as cur:
                t0 = time.perf_counter()
                cal_rows = []
                for row in _read_csv_from_zip(zf, "calendar.txt"):
                    cal_rows.append(
                        (
                            feed_id,
                            row.get("service_id"),
                            int(row.get("monday") or 0),
                            int(row.get("tuesday") or 0),
                            int(row.get("wednesday") or 0),
                            int(row.get("thursday") or 0),
                            int(row.get("friday") or 0),
                            int(row.get("saturday") or 0),
                            int(row.get("sunday") or 0),
                            int(row.get("start_date") or 0),
                            int(row.get("end_date") or 0),
                        )
                    )
                _bulk_insert(
                    cur,
                    "calendar",
                    (
                        "feed_id",
                        "service_id",
                        "monday",
                        "tuesday",
                        "wednesday",
                        "thursday",
                        "friday",
                        "saturday",
                        "sunday",
                        "start_date",
                        "end_date",
                    ),
                    cal_rows,
                )
                log(
                    "repair/scheduling",
                    f"calendar rows={len(cal_rows)} elapsed_s={time.perf_counter() - t0:.2f}",
                )

                t0 = time.perf_counter()
                cald_rows = []
                for row in _read_csv_from_zip(zf, "calendar_dates.txt"):
                    cald_rows.append(
                        (
                            feed_id,
                            row.get("service_id"),
                            int(row.get("date") or 0),
                            int(row.get("exception_type") or 0),
                        )
                    )
                _bulk_insert(
                    cur,
                    "calendar_dates",
                    ("feed_id", "service_id", "date", "exception_type"),
                    cald_rows,
                )
                log(
                    "repair/scheduling",
                    f"calendar_dates rows={len(cald_rows)} elapsed_s={time.perf_counter() - t0:.2f}",
                )

            conn.commit()

            print("[repair] Building trip_time_bounds from stop_times (may take several minutes)...", flush=True)
            t0 = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trip_time_bounds(feed_id, trip_id, first_sec, last_sec)
                    SELECT
                        st.feed_id,
                        st.trip_id,
                        MIN(EXTRACT(EPOCH FROM make_interval(
                            hours := split_part(st.arrival_time, ':', 1)::int,
                            mins   := split_part(st.arrival_time, ':', 2)::int,
                            secs   := COALESCE(NULLIF(split_part(st.arrival_time, ':', 3), '')::int, 0)
                        )))::int AS first_sec,
                        MAX(EXTRACT(EPOCH FROM make_interval(
                            hours := split_part(st.departure_time, ':', 1)::int,
                            mins   := split_part(st.departure_time, ':', 2)::int,
                            secs   := COALESCE(NULLIF(split_part(st.departure_time, ':', 3), '')::int, 0)
                        )))::int AS last_sec
                    FROM stop_times st
                    WHERE st.feed_id = %s
                    GROUP BY st.feed_id, st.trip_id
                    ON CONFLICT (feed_id, trip_id) DO NOTHING
                    """,
                    (feed_id,),
                )
            conn.commit()
            log(
                "repair/scheduling",
                f"trip_time_bounds done elapsed_s={time.perf_counter() - t0:.2f}",
            )

            with conn.cursor() as cur:
                cur.execute(
                    "ANALYZE calendar, calendar_dates, trip_time_bounds, trips, shapes_lines"
                )
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(start_date), MAX(end_date) FROM calendar WHERE feed_id = %s",
                (feed_id,),
            )
            span = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*)::bigint FROM trip_time_bounds WHERE feed_id = %s",
                (feed_id,),
            )
            bounds_n = cur.fetchone()[0]
        print(
            f"[repair] feed_id={feed_id} calendar_span={span} trip_time_bounds={bounds_n}",
            flush=True,
        )
    finally:
        conn.close()


def main() -> None:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Backfill scheduling tables for an existing feed.")
    ap.add_argument("--feed-id", type=int, required=True)
    ap.add_argument("--gtfs-zip", type=Path, default=LOCAL_GTFS_ZIP)
    ap.add_argument("--database-url", default=None)
    args = ap.parse_args()
    repair_feed_scheduling(
        feed_id=args.feed_id,
        gtfs_zip=args.gtfs_zip,
        database_url=args.database_url or DB_URL,
    )


if __name__ == "__main__":
    main()
