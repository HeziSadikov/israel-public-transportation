"""
Tests for backend.area_search (find_routes_in_polygon and helpers).
Uses minimal in-memory GTFS data; no real GTFS file required.

Run from project root: python -m unittest tests.test_area_search
"""
from __future__ import annotations

import unittest
from pathlib import Path

from backend.gtfs_loader import GTFSFeed
from backend.area_search import find_routes_in_polygon, get_trip_time_bounds, _get_shapes_by_id
from backend.config import TRIP_TIME_BOUNDS_CACHE, SHAPES_BY_ID_CACHE


def _make_test_feed(source_path: Path) -> GTFSFeed:
    """Minimal in-memory feed: one route, one trip, one shape that crosses a small box."""
    routes = [
        {
            "route_id": "R1",
            "route_short_name": "1",
            "route_long_name": "Test Line",
            "agency_id": "A1",
            "route_type": "3",
        }
    ]
    trips = [
        {
            "trip_id": "T1",
            "route_id": "R1",
            "service_id": "S1",
            "shape_id": "SH1",
            "direction_id": "0",
        }
    ]
    # Trip T1: 08:00 -> 09:00 so time bounds (28800, 32400)
    stop_times = [
        {"trip_id": "T1", "stop_id": "s1", "stop_sequence": "1", "departure_time": "08:00:00", "arrival_time": "08:00:00"},
        {"trip_id": "T1", "stop_id": "s2", "stop_sequence": "2", "departure_time": "09:00:00", "arrival_time": "09:00:00"},
    ]
    # Shape SH1: line from (34.99, 32.005) to (35.02, 32.005) — crosses polygon at lon 35.0–35.01, lat 32.005
    shapes = [
        {"shape_id": "SH1", "shape_pt_sequence": "0", "shape_pt_lon": "34.99", "shape_pt_lat": "32.005"},
        {"shape_id": "SH1", "shape_pt_sequence": "1", "shape_pt_lon": "35.005", "shape_pt_lat": "32.005"},
        {"shape_id": "SH1", "shape_pt_sequence": "2", "shape_pt_lon": "35.02", "shape_pt_lat": "32.005"},
    ]
    # Service S1 active on 2025-03-03 only (calendar_dates exception)
    calendar = []
    calendar_dates = [
        {"service_id": "S1", "date": "20250303", "exception_type": "1"},
    ]
    stops = []  # not needed for area search

    return GTFSFeed(
        version_id="test",
        source_path=source_path,
        routes=routes,
        trips=trips,
        stop_times=stop_times,
        stops=stops,
        calendar_dates=calendar_dates,
        calendar=calendar,
        shapes=shapes,
    )


def _clear_caches():
    TRIP_TIME_BOUNDS_CACHE.clear()
    SHAPES_BY_ID_CACHE.clear()


class TestAreaSearch(unittest.TestCase):
    def setUp(self):
        _clear_caches()

    def tearDown(self):
        _clear_caches()

    def test_get_trip_time_bounds(self):
        """Trip time bounds are computed from stop_times."""
        path = Path("/fake/test/area_search/feed")
        feed = _make_test_feed(path)
        bounds = get_trip_time_bounds(feed)
        self.assertIn("T1", bounds)
        lo, hi = bounds["T1"]
        self.assertEqual(lo, 8 * 3600)  # 08:00:00
        self.assertEqual(hi, 9 * 3600)  # 09:00:00

    def test_get_shapes_by_id(self):
        """Shapes are grouped by shape_id and sorted by sequence."""
        path = Path("/fake/test/area_search/feed2")
        feed = _make_test_feed(path)
        shapes_by_id = _get_shapes_by_id(feed)
        self.assertIn("SH1", shapes_by_id)
        pts = shapes_by_id["SH1"]
        self.assertEqual(len(pts), 3)
        self.assertAlmostEqual(float(pts[0]["shape_pt_lon"]), 34.99)
        self.assertAlmostEqual(float(pts[2]["shape_pt_lon"]), 35.02)

    def test_find_routes_in_polygon_returns_route_when_shape_intersects(self):
        """When the shape line crosses the polygon, the route is returned."""
        path = Path("/fake/test/area_search/feed3")
        feed = _make_test_feed(path)
        # Polygon: small box that contains (35.0, 32.0)-(35.01, 32.01). Shape crosses at lat 32.005.
        polygon_geojson = {
            "type": "Polygon",
            "coordinates": [[[35.0, 32.0], [35.01, 32.0], [35.01, 32.01], [35.0, 32.01], [35.0, 32.0]]],
        }
        # Date 20250303 has S1 active; time window 07:00–10:00 includes trip T1 (08:00–09:00)
        results = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd="20250303",
            start_sec=7 * 3600,
            end_sec=10 * 3600,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["route_id"], "R1")
        self.assertEqual(results[0]["route_short_name"], "1")
        self.assertTrue(results[0]["direction_id"] is None or results[0]["direction_id"] == "0")
        self.assertEqual(results[0]["first_time_s"], 8 * 3600)
        self.assertEqual(results[0]["last_time_s"], 9 * 3600)

    def test_find_routes_in_polygon_empty_when_shape_outside(self):
        """When no shape intersects the polygon, result is empty."""
        path = Path("/fake/test/area_search/feed4")
        feed = _make_test_feed(path)
        # Polygon far away from our shape (which is around lon 35, lat 32)
        polygon_geojson = {
            "type": "Polygon",
            "coordinates": [[[30.0, 30.0], [30.01, 30.0], [30.01, 30.01], [30.0, 30.01], [30.0, 30.0]]],
        }
        results = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd="20250303",
            start_sec=7 * 3600,
            end_sec=10 * 3600,
        )
        self.assertEqual(len(results), 0)

    def test_find_routes_in_polygon_empty_when_time_window_misses_trip(self):
        """When the time window doesn't overlap the trip, no routes returned."""
        path = Path("/fake/test/area_search/feed5")
        feed = _make_test_feed(path)
        polygon_geojson = {
            "type": "Polygon",
            "coordinates": [[[35.0, 32.0], [35.01, 32.0], [35.01, 32.01], [35.0, 32.01], [35.0, 32.0]]],
        }
        # Trip is 08:00–09:00; window 12:00–13:00 does not overlap
        results = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd="20250303",
            start_sec=12 * 3600,
            end_sec=13 * 3600,
        )
        self.assertEqual(len(results), 0)

    def test_find_routes_in_polygon_empty_when_date_has_no_service(self):
        """When the date has no active service, result is empty."""
        path = Path("/fake/test/area_search/feed6")
        feed = _make_test_feed(path)
        polygon_geojson = {
            "type": "Polygon",
            "coordinates": [[[35.0, 32.0], [35.01, 32.0], [35.01, 32.01], [35.0, 32.01], [35.0, 32.0]]],
        }
        # S1 is only active on 20250303; 20250101 has no calendar_dates entry
        results = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd="20250101",
            start_sec=7 * 3600,
            end_sec=10 * 3600,
        )
        self.assertEqual(len(results), 0)

    def test_find_routes_in_polygon_empty_polygon(self):
        """Empty polygon returns empty list."""
        path = Path("/fake/test/area_search/feed7")
        feed = _make_test_feed(path)
        polygon_geojson = {"type": "Polygon", "coordinates": []}
        results = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd="20250303",
            start_sec=7 * 3600,
            end_sec=10 * 3600,
        )
        self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
