"""
PostGIS integration: area route search with fixed polygon (no frontend).

Skips when DATABASE_URL is unset or Postgres/active feed is unavailable.
Can take ~1–2 minutes on a full national feed.

  pytest tests/test_area_routes_postgis_integration.py -s

Override test day if your calendar does not include the default:

  set AREA_TEST_DATE_YMD=20250315
"""
from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import DictCursor  # noqa: E402

from backend.area_routes_canonical_fixtures import (  # noqa: E402
    CANONICAL_AREA_DATE_YMD,
    CANONICAL_AREA_END_SEC,
    CANONICAL_AREA_POLYGON_WKT,
    CANONICAL_AREA_START_SEC,
)
from backend.infra.db_access import (  # noqa: E402
    get_active_feed_id,
    get_active_feed_calendar_span,
    get_routes_in_polygon_range,
)


def _conn_or_skip():
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    try:
        return psycopg2.connect(url, cursor_factory=DictCursor)
    except Exception as e:
        pytest.skip(f"Postgres unavailable: {e}")


@pytest.mark.slow
def test_get_routes_in_polygon_range_canonical_polygon_returns_rows():
    conn = _conn_or_skip()
    conn.autocommit = True
    try:
        get_active_feed_id(conn)
    except Exception as e:
        conn.close()
        pytest.skip(f"No active feed: {e}")
    span = get_active_feed_calendar_span(conn=conn)
    if span and span[0] is not None and span[1] is not None:
        date_n = int(CANONICAL_AREA_DATE_YMD)
        if date_n < int(span[0]) or date_n > int(span[1]):
            conn.close()
            pytest.skip(
                f"Canonical date {CANONICAL_AREA_DATE_YMD} is outside loaded calendar span {span[0]}-{span[1]}"
            )
    try:
        rows = get_routes_in_polygon_range(
            polygon_wkt=CANONICAL_AREA_POLYGON_WKT,
            start_date_ymd=CANONICAL_AREA_DATE_YMD,
            start_sec=CANONICAL_AREA_START_SEC,
            end_date_ymd=CANONICAL_AREA_DATE_YMD,
            end_sec=CANONICAL_AREA_END_SEC,
            conn=conn,
        )
    finally:
        conn.close()
    assert len(rows) >= 1, (
        "Expected at least one route for canonical polygon; "
        "set AREA_TEST_DATE_YMD to a day inside your loaded GTFS calendar"
    )
