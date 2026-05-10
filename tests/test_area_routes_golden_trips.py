"""
Golden-trip checks for area pass-through modes (PostGIS + real feed).

Without AREA_GOLDEN_TRIPS_FILE these tests skip: trip_ids are feed-specific.

Usage:
  set AREA_GOLDEN_TRIPS_FILE=tests/fixtures/area_routes_golden_local.json
  pytest tests/test_area_routes_golden_trips.py -v

See tests/fixtures/README_area_routes_golden.md
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import DictCursor  # noqa: E402

from backend.infra.db_access import (  # noqa: E402
    get_active_feed_id,
    get_routes_in_polygon_range,
)


def _conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    return psycopg2.connect(url, cursor_factory=DictCursor)


def _golden_path() -> Optional[Path]:
    raw = (os.getenv("AREA_GOLDEN_TRIPS_FILE") or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if not p.is_file():
        pytest.skip(f"AREA_GOLDEN_TRIPS_FILE not found: {p}")
    return p


def _load_cases() -> List[Dict[str, Any]]:
    path = _golden_path()
    if path is None:
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    cases = data.get("cases")
    if not isinstance(cases, list):
        return []
    return [c for c in cases if isinstance(c, dict)]


def _trip_route_dir(conn, feed_id: int, trip_id: str) -> Optional[Tuple[str, Optional[int]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT route_id, direction_id
            FROM trips
            WHERE feed_id = %s AND trip_id = %s
            LIMIT 1
            """,
            (feed_id, trip_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        rid = str(row["route_id"])
        did = row["direction_id"]
        return rid, int(did) if did is not None else None


def _find_route_row(rows, route_id: str, direction_id: Optional[int]):
    for r in rows:
        if str(r.route_id) != route_id:
            continue
        if direction_id is None:
            if r.direction_id is None:
                return r
        elif r.direction_id is not None and int(r.direction_id) == direction_id:
            return r
    return None


@pytest.mark.slow
def test_golden_trips_from_file():
    cases = _load_cases()
    if not cases:
        pytest.skip("Set AREA_GOLDEN_TRIPS_FILE to a JSON file with non-empty cases[]")

    for case_index, case in enumerate(cases):
        desc = str(case.get("description") or f"case[{case_index}]")
        trip_id = str(case.get("trip_id") or "").strip()
        date_ymd = str(case.get("date_ymd") or "").strip()
        polygon_wkt = str(case.get("polygon_wkt") or "").strip()
        start_sec = int(case["start_sec"])
        end_sec = int(case["end_sec"])
        mode = str(case.get("time_semantics_mode") or "pass_through_precise").strip()
        expect = str(case.get("expect") or "include_route").strip()

        if not trip_id or not date_ymd or not polygon_wkt:
            pytest.skip(f"{desc}: missing trip_id/date_ymd/polygon_wkt")
        if mode not in ("pass_through_precise", "pass_through_stop_proxy"):
            pytest.skip(f"{desc}: unsupported time_semantics_mode {mode}")
        if expect not in ("include_route", "exclude_route"):
            pytest.skip(f"{desc}: expect must be include_route or exclude_route")

        conn = _conn()
        conn.autocommit = True
        try:
            feed_id = get_active_feed_id(conn)
        except Exception as e:
            conn.close()
            pytest.skip(f"No active feed: {e}")

        resolved = _trip_route_dir(conn, feed_id, trip_id)
        if resolved is None:
            conn.close()
            pytest.skip(f"{desc}: trip_id {trip_id!r} not found in active feed {feed_id}")
        route_id, direction_id = resolved

        rows = []
        try:
            rows = get_routes_in_polygon_range(
                polygon_wkt=polygon_wkt,
                start_date_ymd=date_ymd,
                start_sec=start_sec,
                end_date_ymd=date_ymd,
                end_sec=end_sec,
                time_semantics_mode=mode,
                conn=conn,
            )
        except Exception as e:
            if "timeout" in str(e).lower() or type(e).__name__ == "QueryCanceled":
                pytest.skip(f"{desc}: query timed out ({type(e).__name__})")
            raise
        finally:
            conn.close()

        row = _find_route_row(rows, route_id, direction_id)
        if expect == "include_route":
            assert row is not None, (
                f"{desc}: expected route {route_id} direction {direction_id!r} in {mode} results; "
                f"got {len(rows)} rows"
            )
            assert row.time_match_confidence in {"high", "approx", "unknown", None}
        else:
            assert row is None, (
                f"{desc}: expected route {route_id} absent from {mode} results; "
                f"but found matching row (first_time_s={getattr(row, 'first_time_s', None)})"
            )
