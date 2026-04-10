"""Unit tests for route-batched pattern build helpers (no Postgres)."""
from __future__ import annotations

import unittest

from backend.scripts.build_patterns_postgis import (
    _direction_ids_by_route,
    _trip_row_from_db,
)


class TestTripRowFromDb(unittest.TestCase):
    def test_direction_none_and_empty(self):
        base = {
            "trip_id": "t1",
            "route_id": "r1",
            "service_id": "svc",
            "shape_id": None,
            "headsign": None,
            "block_id": None,
        }
        self.assertIsNone(_trip_row_from_db({**base, "direction_id": None})["direction_id"])
        self.assertIsNone(_trip_row_from_db({**base, "direction_id": ""})["direction_id"])
        self.assertEqual(_trip_row_from_db({**base, "direction_id": 0})["direction_id"], "0")


class MockCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _sql, _params=None):
        pass

    def fetchall(self):
        return list(self._rows)


class TestDirectionIdsByRoute(unittest.TestCase):
    def test_merges_distinct_rows(self):
        cur = MockCursor(
            [
                {"route_id": "R1", "direction_id": None},
                {"route_id": "R1", "direction_id": 0},
                {"route_id": "R2", "direction_id": ""},
            ]
        )
        m = _direction_ids_by_route(cur, feed_id=99)
        self.assertEqual(m["R1"], {None, "0"})
        self.assertEqual(m["R2"], {None})


if __name__ == "__main__":
    unittest.main()
