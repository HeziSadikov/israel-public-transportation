"""Unit tests for multi-day area search decomposition helpers."""
from __future__ import annotations

from backend.infra.db_access import (
    RouteInAreaRow,
    _GTFS_DAY_END_SEC,
    _iter_multi_day_windows,
    _merge_route_in_area_rows,
)


def test_iter_multi_day_windows_overnight_two_days():
    slices = _iter_multi_day_windows("20260520", 79200, "20260521", 18000)
    assert slices == [
        ("20260520", 79200, _GTFS_DAY_END_SEC),
        ("20260521", 0, 18000),
    ]


def test_iter_multi_day_windows_three_full_middle_days():
    slices = _iter_multi_day_windows("20260520", 36000, "20260522", 72000)
    assert slices == [
        ("20260520", 36000, _GTFS_DAY_END_SEC),
        ("20260521", 0, _GTFS_DAY_END_SEC),
        ("20260522", 0, 72000),
    ]


def test_merge_route_in_area_rows_combines_same_route():
    chunk_a = [
        RouteInAreaRow(
            route_id="R1",
            direction_id=0,
            route_short_name="1",
            route_long_name="Line",
            agency_id="A1",
            agency_name="Agency",
            first_time_s=79200,
            last_time_s=81000,
            trip_count=2,
            time_match_confidence="high",
        )
    ]
    chunk_b = [
        RouteInAreaRow(
            route_id="R1",
            direction_id=0,
            route_short_name="1",
            route_long_name="Line",
            agency_id="A1",
            agency_name="Agency",
            first_time_s=3600,
            last_time_s=7200,
            trip_count=1,
            time_match_confidence="unknown",
            time_match_note="estimated",
        )
    ]
    merged = _merge_route_in_area_rows([chunk_a, chunk_b])
    assert len(merged) == 1
    row = merged[0]
    assert row.first_time_s == 3600
    assert row.last_time_s == 81000
    assert row.trip_count == 3
    assert row.time_match_confidence == "unknown"
    assert row.time_match_note == "estimated"
