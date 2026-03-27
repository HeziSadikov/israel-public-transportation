from __future__ import annotations

import backend.db_access as db_access


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params):
        return None

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)

    def close(self):
        return None


def test_get_top_patterns_for_routes_strict_then_fallback(monkeypatch):
    rows = [
        {
            "pattern_id": "p_r1_a",
            "route_id": "R1",
            "direction_id": 0,
            "repr_trip_id": "t1",
            "repr_shape_id": "s1",
            "stop_ids": ["S1", "S2"],
            "frequency": 10,
            "used_shape": True,
            "active_trip_count": 3,
        },
        {
            "pattern_id": "p_r1_b",
            "route_id": "R1",
            "direction_id": 0,
            "repr_trip_id": "t2",
            "repr_shape_id": "s2",
            "stop_ids": ["S1", "S3"],
            "frequency": 9,
            "used_shape": True,
            "active_trip_count": 1,
        },
        {
            "pattern_id": "p_r1_c",
            "route_id": "R1",
            "direction_id": 0,
            "repr_trip_id": "t3",
            "repr_shape_id": "s3",
            "stop_ids": ["S1", "S4"],
            "frequency": 20,
            "used_shape": True,
            "active_trip_count": 0,
        },
        {
            "pattern_id": "p_r2_a",
            "route_id": "R2",
            "direction_id": 1,
            "repr_trip_id": "t4",
            "repr_shape_id": "s4",
            "stop_ids": ["X1", "X2"],
            "frequency": 7,
            "used_shape": True,
            "active_trip_count": 0,
        },
        {
            "pattern_id": "p_r2_b",
            "route_id": "R2",
            "direction_id": 1,
            "repr_trip_id": "t5",
            "repr_shape_id": "s5",
            "stop_ids": ["X1", "X3"],
            "frequency": 5,
            "used_shape": True,
            "active_trip_count": 0,
        },
    ]
    monkeypatch.setattr(db_access, "get_active_feed_id", lambda _conn=None: 1)

    selected = db_access.get_top_patterns_for_routes(
        route_ids=["R1", "R2"],
        date_ymd="20260101",
        start_sec=3600,
        end_sec=7200,
        k_per_route_dir=2,
        include_fallback=True,
        conn=_FakeConn(rows),
    )

    r1 = selected[("R1", "0")]
    assert [m.pattern_id for m in r1] == ["p_r1_a", "p_r1_b"]
    assert all(m.selection_source == "strict" for m in r1)

    r2 = selected[("R2", "1")]
    assert [m.pattern_id for m in r2] == ["p_r2_a", "p_r2_b"]
    assert all(m.selection_source == "fallback" for m in r2)


def test_get_top_patterns_for_routes_direction_filter(monkeypatch):
    rows = [
        {
            "pattern_id": "p_r1_dir0",
            "route_id": "R1",
            "direction_id": 0,
            "repr_trip_id": "t1",
            "repr_shape_id": "s1",
            "stop_ids": ["S1", "S2"],
            "frequency": 10,
            "used_shape": True,
            "active_trip_count": 1,
        },
        {
            "pattern_id": "p_r1_dir1",
            "route_id": "R1",
            "direction_id": 1,
            "repr_trip_id": "t2",
            "repr_shape_id": "s2",
            "stop_ids": ["S2", "S1"],
            "frequency": 10,
            "used_shape": True,
            "active_trip_count": 1,
        },
    ]
    monkeypatch.setattr(db_access, "get_active_feed_id", lambda _conn=None: 1)

    selected = db_access.get_top_patterns_for_routes(
        route_ids=["R1"],
        date_ymd="20260101",
        start_sec=3600,
        end_sec=7200,
        k_per_route_dir=2,
        direction_filter_by_route={"R1": "0"},
        include_fallback=True,
        conn=_FakeConn(rows),
    )

    assert ("R1", "0") in selected
    assert ("R1", "1") not in selected
