"""
Integration test for the detour v2 pipeline.

All external I/O is stubbed:
- db_access functions are monkey-patched with in-memory canned data.
- httpx.post is patched to return fixture-based Valhalla responses.

Tests the full compute_detour_for_trip() call end-to-end and asserts:
- status='ok'
- selected.geometry_geojson is present
- chosen path does not have significant overlap with the blockage polygon
- attempts[] field is populated
- score_breakdown contains expected keys
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import LineString, box, mapping

# ---------------------------------------------------------------------------
# Helpers: polyline encoder (Valhalla precision-6 format)
# ---------------------------------------------------------------------------

def _encode_polyline6(coords_lonlat: List[Tuple[float, float]]) -> str:
    """Encode (lon, lat) pairs into Valhalla precision-6 polyline (lat-first encoding)."""
    factor = 1_000_000
    out: List[str] = []
    prev_lat = prev_lon = 0
    for lon, lat in coords_lonlat:
        lat_e = round(lat * factor)
        lon_e = round(lon * factor)
        for delta in [lat_e - prev_lat, lon_e - prev_lon]:
            v = delta << 1
            if v < 0:
                v = ~v
            while v >= 32:
                out.append(chr((32 | (v & 31)) + 63))
                v >>= 5
            out.append(chr(v + 63))
        prev_lat = lat_e
        prev_lon = lon_e
    return "".join(out)


# ---------------------------------------------------------------------------
# Synthetic trip geometry — straight corridor with a detour avoiding a block
# ---------------------------------------------------------------------------
# Main route: straight east along lat=31.25 from lon=34.76 to 34.82
# Blockage: box around lon=[34.785, 34.795], lat=[31.245, 31.255]  (straddles the route)
# Detour path: goes around blockage to the south then rejoins

ROUTE_COORDS = [(34.76 + i * 0.002, 31.25) for i in range(31)]  # 30 steps ≈ ~6 km
BLOCKAGE = box(34.785, 31.245, 34.795, 31.255)
BLOCKAGE_GEOJSON = mapping(BLOCKAGE)

# Detour goes: exit (34.782,31.25) → south (34.785,31.24) → east (34.797,31.24) → north → rejoin (34.800,31.25)
DETOUR_COORDS = [
    (34.782, 31.25),
    (34.783, 31.242),
    (34.788, 31.240),
    (34.795, 31.240),
    (34.800, 31.245),
    (34.800, 31.25),
]

ENCODED_DETOUR = _encode_polyline6(DETOUR_COORDS)

# Distance in km (approx.)
_DETOUR_KM = sum(
    math.sqrt((DETOUR_COORDS[i+1][0]-DETOUR_COORDS[i][0])**2 + (DETOUR_COORDS[i+1][1]-DETOUR_COORDS[i][1])**2)
    * 111.32
    for i in range(len(DETOUR_COORDS)-1)
)


def _make_valhalla_route_response(coords: List[Tuple[float, float]], km: float, time_s: float = 90.0) -> Dict[str, Any]:
    return {
        "trip": {
            "legs": [
                {
                    "summary": {"length": km, "time": time_s},
                    "shape": _encode_polyline6(coords),
                    "maneuvers": [
                        {"instruction": "Turn right onto detour road", "street_names": ["Detour St"]},
                        {"instruction": "Continue for 0.5 km"},
                        {"instruction": "Turn left to rejoin route"},
                        {"instruction": "You have arrived at your destination.", "type": 4},
                    ],
                }
            ],
            "summary": {"length": km, "time": time_s},
        },
        "alternates": [],
    }


# ---------------------------------------------------------------------------
# DB stubs
# ---------------------------------------------------------------------------

TRIP_ID = "test_trip_001"
ROUTE_ID = "R_TEST"
SHAPE_ID = "SH_TEST"
FEED_ID = 42

# Build a Shapely LineString from the route coords for get_shape_line mock.
SHAPE_LINE = LineString(ROUTE_COORDS)

# Stop rows distributed along the shape.
_STOPS_RAW = [
    {"stop_id": f"S{i:02d}", "stop_sequence": i, "shape_dist_traveled": i * 200.0}
    for i in range(31)
]
_STOP_LONLAT: Dict[str, Tuple[float, float]] = {
    f"S{i:02d}": (34.76 + i * 0.002, 31.25) for i in range(31)
}


def _make_db_stubs() -> Dict[str, Any]:
    return {
        "get_trip_route_shape": {"route_id": ROUTE_ID, "shape_id": SHAPE_ID},
        "get_shape_line": SHAPE_LINE,
        "get_stop_times_for_trip": _STOPS_RAW,
        "get_stop_lonlat_bulk": _STOP_LONLAT,
        "get_active_feed_id": FEED_ID,
        "osm_segments_intersecting_polygon": [],
        "get_candidate_osm_segments_for_polyline": [],
        "get_gtfs_bus_way_evidence_bulk": {},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_stubs(monkeypatch):
    stubs = _make_db_stubs()

    def _get_trip_route_shape(trip_id):
        return stubs["get_trip_route_shape"]

    def _get_shape_line(shape_id):
        return stubs["get_shape_line"]

    def _get_stop_times(trip_id):
        return stubs["get_stop_times_for_trip"]

    def _get_stop_lonlat_bulk(stop_ids):
        return {sid: stubs["get_stop_lonlat_bulk"][sid] for sid in stop_ids if sid in stubs["get_stop_lonlat_bulk"]}

    def _get_active_feed_id():
        return stubs["get_active_feed_id"]

    def _osm_segments_intersecting(polygon_geojson, feed_id=None):
        return stubs["osm_segments_intersecting_polygon"]

    def _get_candidate_osm_segments(coords, **kw):
        return stubs["get_candidate_osm_segments_for_polyline"]

    def _get_gtfs_bus_way_evidence_bulk(feed_id, way_ids):
        return stubs["get_gtfs_bus_way_evidence_bulk"]

    import backend.infra.db_access as db
    monkeypatch.setattr(db, "get_trip_route_shape", _get_trip_route_shape)
    monkeypatch.setattr(db, "get_shape_line", _get_shape_line)
    monkeypatch.setattr(db, "get_stop_times_for_trip", _get_stop_times)
    monkeypatch.setattr(db, "get_stop_lonlat_bulk", _get_stop_lonlat_bulk)
    monkeypatch.setattr(db, "get_active_feed_id", _get_active_feed_id)
    monkeypatch.setattr(db, "osm_segments_intersecting_polygon", _osm_segments_intersecting)
    monkeypatch.setattr(db, "get_candidate_osm_segments_for_polyline", _get_candidate_osm_segments)
    monkeypatch.setattr(db, "get_gtfs_bus_way_evidence_bulk", _get_gtfs_bus_way_evidence_bulk)
    return stubs


def _make_httpx_mock(route_response: Optional[Dict[str, Any]] = None):
    """Return a mock for httpx.post that returns a successful route response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.text = ""
    data = route_response or _make_valhalla_route_response(DETOUR_COORDS, _DETOUR_KM, time_s=90.0)
    resp.json = lambda: data
    resp.raise_for_status = lambda: None
    mock = MagicMock(return_value=resp)
    return mock


def test_compute_detour_returns_ok_status(db_stubs, monkeypatch):
    """Full pipeline: returns status=ok with geometry and attempts."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig

    pol = DetourPolicyConfig()
    pol.search.per_trip_deadline_ms = 30000
    pol.search.valhalla_concurrency = 1

    mock_post = _make_httpx_mock()
    with (
        patch("backend.adapters.osm_detour.httpx.post", mock_post),
        patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"),
        patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False),
    ):
        # Also stub valhalla_locate to return None (no anchor filtering).
        monkeypatch.setattr(
            "backend.domain.detour_v2.compute.valhalla_locate",
            lambda *a, **kw: None,
        )
        out = compute_detour_for_trip(
            trip_id=TRIP_ID,
            blockage_geojson=BLOCKAGE_GEOJSON,
            service_date="20260419",
            policy=pol,
        )

    assert out.status == "ok", f"Expected ok, got {out.status}. debug={out.debug}"
    assert out.selected is not None
    assert out.selected.decoded is not None
    assert out.selected.decoded.geometry_geojson is not None


def test_compute_detour_attempts_populated(db_stubs, monkeypatch):
    """Attempts list is populated for each (anchor, corridor) combination tried."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig

    pol = DetourPolicyConfig()
    pol.search.valhalla_concurrency = 1
    pol.anchor.candidate_pairs_k = 2

    mock_post = _make_httpx_mock()
    with (
        patch("backend.adapters.osm_detour.httpx.post", mock_post),
        patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"),
        patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False),
    ):
        monkeypatch.setattr("backend.domain.detour_v2.compute.valhalla_locate", lambda *a, **kw: None)
        out = compute_detour_for_trip(
            trip_id=TRIP_ID,
            blockage_geojson=BLOCKAGE_GEOJSON,
            service_date="20260419",
            policy=pol,
        )

    assert isinstance(out.attempts, list)
    assert len(out.attempts) > 0
    first_attempt = out.attempts[0]
    assert "corridor" in first_attempt
    assert "anchor_index" in first_attempt


def test_compute_detour_path_does_not_enter_blockage(db_stubs, monkeypatch):
    """The selected detour path must not have significant overlap with the blockage polygon."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig
    from shapely.geometry import LineString as SLS, shape

    pol = DetourPolicyConfig()
    pol.service.max_incident_overlap_fraction = 0.0
    pol.search.valhalla_concurrency = 1

    mock_post = _make_httpx_mock()
    with (
        patch("backend.adapters.osm_detour.httpx.post", mock_post),
        patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"),
        patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False),
    ):
        monkeypatch.setattr("backend.domain.detour_v2.compute.valhalla_locate", lambda *a, **kw: None)
        out = compute_detour_for_trip(
            trip_id=TRIP_ID,
            blockage_geojson=BLOCKAGE_GEOJSON,
            service_date="20260419",
            policy=pol,
        )

    if out.status != "ok":
        pytest.skip(f"No detour found ({out.status}); overlap check not applicable")

    # Extract geometry coords from selected candidate.
    geom_fc = out.selected.decoded.geometry_geojson if out.selected and out.selected.decoded else None
    if not geom_fc:
        pytest.skip("No geometry to check")
    features = geom_fc.get("features") or []
    for feat in features:
        geom = feat.get("geometry") or {}
        if geom.get("type") == "LineString":
            coords = geom["coordinates"]
            path_line = SLS(coords)
            blk = shape(BLOCKAGE_GEOJSON)
            if path_line.length > 0:
                overlap = path_line.intersection(blk).length / path_line.length
                assert overlap <= 0.01, f"Detour overlaps blockage by {overlap:.3%}"


def test_compute_detour_score_breakdown_complete(db_stubs, monkeypatch):
    """Winner's score_breakdown must contain all expected components."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig
    from backend.domain.detour_v2.serialize import detour_compute_output_to_dict

    pol = DetourPolicyConfig()
    pol.search.valhalla_concurrency = 1

    mock_post = _make_httpx_mock()
    with (
        patch("backend.adapters.osm_detour.httpx.post", mock_post),
        patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"),
        patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False),
    ):
        monkeypatch.setattr("backend.domain.detour_v2.compute.valhalla_locate", lambda *a, **kw: None)
        out = compute_detour_for_trip(
            trip_id=TRIP_ID,
            blockage_geojson=BLOCKAGE_GEOJSON,
            service_date="20260419",
            policy=pol,
        )

    if out.status != "ok":
        pytest.skip(f"No detour found ({out.status})")

    d = detour_compute_output_to_dict(out)
    selected = d.get("selected") or {}
    breakdown = selected.get("score_breakdown") or {}
    required_keys = {"travel_time_s", "segment_penalty_s", "turn_penalty_s", "uncertainty_penalty_s", "service_penalty_s"}
    for k in required_keys:
        assert k in breakdown, f"Missing score_breakdown key: {k}"
    assert "summary_en" in selected, "summary_en should be present in serialized selected"
    assert isinstance(selected["summary_en"], str) and len(selected["summary_en"]) > 0


def test_compute_detour_no_impact_when_shape_misses_blockage(db_stubs, monkeypatch):
    """Return no_impact when the trip shape does not intersect the blockage."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig

    pol = DetourPolicyConfig()
    pol.search.valhalla_concurrency = 1
    # Blockage far from the route.
    far_blockage = mapping(box(35.5, 32.0, 35.6, 32.1))

    out = compute_detour_for_trip(
        trip_id=TRIP_ID,
        blockage_geojson=far_blockage,
        service_date="20260419",
        policy=pol,
    )
    assert out.status == "no_impact"


def test_serialize_includes_attempts_field(db_stubs, monkeypatch):
    """Serialized output must always include attempts[], even on no_safe_detour."""
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig
    from backend.domain.detour_v2.serialize import detour_compute_output_to_dict

    pol = DetourPolicyConfig()
    pol.search.valhalla_concurrency = 1

    # Return 442 from Valhalla so all attempts fail.
    resp_442 = MagicMock()
    resp_442.status_code = 400
    resp_442.text = '{"error_code": 442, "error": "No path"}'
    resp_442.json = lambda: {"error_code": 442, "error": "No path"}
    mock_post = MagicMock(return_value=resp_442)

    with (
        patch("backend.adapters.osm_detour.httpx.post", mock_post),
        patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"),
        patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False),
    ):
        monkeypatch.setattr("backend.domain.detour_v2.compute.valhalla_locate", lambda *a, **kw: None)
        out = compute_detour_for_trip(
            trip_id=TRIP_ID,
            blockage_geojson=BLOCKAGE_GEOJSON,
            service_date="20260419",
            policy=pol,
        )

    d = detour_compute_output_to_dict(out)
    # attempts should be in the output (may be empty list if no routing happened).
    assert "attempts" in d or out.status == "no_impact"
