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


@pytest.mark.slow
@pytest.mark.parametrize("time_semantics_mode", ["pass_through_precise", "pass_through_stop_proxy"])
def test_get_routes_in_polygon_range_pass_through_modes(time_semantics_mode: str):
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
            time_semantics_mode=time_semantics_mode,
            conn=conn,
        )
    finally:
        conn.close()
    if not rows:
        pytest.skip(f"No rows returned for mode={time_semantics_mode} on canonical polygon/time")
    for row in rows:
        assert row.time_match_confidence in {"high", "approx", "unknown", None}


@pytest.mark.slow
def test_small_polygon_pass_through_not_stricter_than_legacy():
    """
    Regression guard: pass-through modes must not require a stop physically inside polygon.
    If legacy finds shape-intersection matches in this small AOI/time window, pass-through
    should also return at least one row.
    """
    conn = _conn_or_skip()
    conn.autocommit = True
    try:
        get_active_feed_id(conn)
    except Exception as e:
        conn.close()
        pytest.skip(f"No active feed: {e}")

    date_ymd = os.getenv("AREA_TEST_DATE_YMD", CANONICAL_AREA_DATE_YMD)
    start_sec = CANONICAL_AREA_START_SEC
    end_sec = CANONICAL_AREA_END_SEC
    small_polygon_wkt = (
        os.getenv("AREA_TEST_SMALL_POLYGON_WKT")
        or "POLYGON((34.74519 32.01620,34.74708 32.01620,34.74708 32.01713,34.74519 32.01713,34.74519 32.01620))"
    )

    try:
        legacy_rows = get_routes_in_polygon_range(
            polygon_wkt=small_polygon_wkt,
            start_date_ymd=date_ymd,
            start_sec=start_sec,
            end_date_ymd=date_ymd,
            end_sec=end_sec,
            time_semantics_mode="legacy_trip_overlap",
            conn=conn,
        )
        if not legacy_rows:
            pytest.skip("Legacy returned no rows for configured small polygon/date; cannot assert comparative behavior.")
        precise_rows = get_routes_in_polygon_range(
            polygon_wkt=small_polygon_wkt,
            start_date_ymd=date_ymd,
            start_sec=start_sec,
            end_date_ymd=date_ymd,
            end_sec=end_sec,
            time_semantics_mode="pass_through_precise",
            conn=conn,
        )
        proxy_rows = get_routes_in_polygon_range(
            polygon_wkt=small_polygon_wkt,
            start_date_ymd=date_ymd,
            start_sec=start_sec,
            end_date_ymd=date_ymd,
            end_sec=end_sec,
            time_semantics_mode="pass_through_stop_proxy",
            conn=conn,
        )
    finally:
        conn.close()

    assert len(precise_rows) >= 1
    assert len(proxy_rows) >= 1
