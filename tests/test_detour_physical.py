"""Unit tests for physical-layer helpers (no Valhalla)."""

from __future__ import annotations

from unittest.mock import patch

from shapely.geometry import LineString

from backend.domain.detour_physical.debug_geojson import (
    build_detour_debug_feature_collection,
    coords_from_geojson_linestring,
)
from backend.domain.detour_physical.edge_matcher import densify_linestring, match_gtfs_slice_to_osm_edges
from backend.domain.detour_v2.detour_validator import validate_detour_carriageway


def test_coords_from_geojson_linestring_feature():
    gj = {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": [[34.0, 31.5], [34.01, 31.51]]},
    }
    c = coords_from_geojson_linestring(gj)
    assert c is not None
    assert len(c) == 2
    assert c[0] == (34.0, 31.5)


def test_build_detour_debug_feature_collection_minimal():
    fc = build_detour_debug_feature_collection(
        gtfs_shape_coords_lonlat=[(34.0, 31.5), (34.02, 31.52)],
        exit_lon=34.0,
        exit_lat=31.5,
        rejoin_lon=34.02,
        rejoin_lat=31.52,
    )
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) >= 3


def test_densify_linestring():
    line = LineString([(34.0, 31.5), (34.1, 31.6)])
    pts = densify_linestring(line, step_m=500.0)
    assert len(pts) >= 2


def test_validate_detour_carriageway_rejects_when_opposite_to_expected():
    coords = [(34.0 + i * 0.001, 31.5) for i in range(30)]
    ok, reasons = validate_detour_carriageway(
        route_coords_lonlat=list(reversed(coords)),
        decoded=None,
        expected_exit_bearing_deg=90.0,
        expected_rejoin_bearing_deg=90.0,
        max_entry_delta_deg=95.0,
        max_rejoin_delta_deg=95.0,
    )
    assert not ok
    assert any("CARRIAGEWAY" in r for r in reasons)


@patch("backend.domain.detour_physical.edge_matcher.match_route_attributes", lambda *a, **k: None)
def test_match_gtfs_slice_fails_when_trace_empty():
    line = LineString([(34.0, 31.5), (34.02, 31.52)])
    r = match_gtfs_slice_to_osm_edges(line)
    assert r.success is False

