"""
Contract tests driven by tests/fixtures/detour_v2_scenarios/*.json.

External I/O is stubbed (PostGIS + Valhalla). Run with: pytest tests/test_detour_v2_regression_scenarios.py -q
"""

from __future__ import annotations

import json
import math
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import LineString

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "detour_v2_scenarios"

TIER_ORDER = {
    "auto_ok": 4,
    "review_recommended": 3,
    "low_confidence": 2,
    "emergency_fallback": 1,
    "no_safe_detour": 1,
    "no_impact": 0,
    "error": -1,
}


def _encode_polyline6(coords_lonlat: List[Tuple[float, float]]) -> str:
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


def _load_scenarios() -> List[Dict[str, Any]]:
    paths = sorted(FIXTURE_DIR.glob("*.json"))
    assert paths, f"No JSON scenarios under {FIXTURE_DIR}"
    out = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            out.append(json.load(f))
    return out


def _build_stubs(scenario: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Any]:
    trip_id = str(scenario["trip_id"])
    coords = [(float(a[0]), float(a[1])) for a in scenario["route_coords_lonlat"]]
    line = LineString(coords)
    route_id = "R_TEST"
    shape_id = "SH_TEST"
    step = float(scenario.get("stop_shape_dist_step_m") or 200.0)
    stops_raw = [{"stop_id": f"S{i:02d}", "stop_sequence": i, "shape_dist_traveled": i * step} for i in range(len(coords))]
    stop_lonlat = {f"S{i:02d}": (coords[i][0], coords[i][1]) for i in range(len(coords))}
    stubs = {
        "get_trip_route_shape": {"route_id": route_id, "shape_id": shape_id},
        "get_shape_line": line,
        "get_stop_times_for_trip": stops_raw,
        "get_stop_lonlat_bulk": stop_lonlat,
        "get_active_feed_id": 42,
        "osm_segments_intersecting_polygon": [],
        "get_candidate_osm_segments_for_polyline": [],
        "get_gtfs_bus_way_evidence_bulk": {},
        "osm_segment_nodes_intersecting_polygon": [],
        "get_bus_edge_evidence_bulk": {},
        "get_bus_turn_evidence_bulk": {},
        "fetch_osm_turn_restrictions_by_via_nodes": [],
    }

    detour_coords = [(float(a[0]), float(a[1])) for a in scenario.get("detour_coords_lonlat") or []]
    km = 0.05
    if len(detour_coords) >= 2:
        km = sum(
            math.hypot(detour_coords[i + 1][0] - detour_coords[i][0], detour_coords[i + 1][1] - detour_coords[i][1])
            * 111.32
            for i in range(len(detour_coords) - 1)
        )
    mock_kind = str(scenario.get("mock_valhalla") or "none")

    def _make_uturn_route_response(coords: List[Tuple[float, float]], km: float) -> Dict[str, Any]:
        """Response with an explicit U-turn maneuver instruction."""
        return {
            "trip": {
                "legs": [
                    {
                        "summary": {"length": km, "time": 60.0},
                        "shape": _encode_polyline6(coords),
                        "maneuvers": [
                            {"instruction": "Head south", "type": 1},
                            {"instruction": "Make a U-turn", "type": 12, "maneuver_type": 12},
                            {"instruction": "You have arrived.", "type": 4},
                        ],
                    }
                ],
                "summary": {"length": km, "time": 60.0},
            },
            "alternates": [],
        }

    def _mock_post(*_a, **_kw):
        resp = MagicMock()
        resp.status_code = 200
        resp.text = ""
        if mock_kind == "http_442":
            resp.status_code = 400
            resp.text = '{"error_code": 442}'
            resp.json = lambda: {"error_code": 442, "error": "No path"}
        elif mock_kind == "detour_ok" and detour_coords:
            data = _make_valhalla_route_response(detour_coords, max(km, 0.05), time_s=90.0)
            resp.json = lambda: data
        elif mock_kind == "uturn_ok" and detour_coords:
            data = _make_uturn_route_response(detour_coords, max(km, 0.05))
            resp.json = lambda: data
        else:
            resp.json = lambda: {"trip": {"legs": [], "summary": {"length": 0, "time": 0}}, "alternates": []}
        resp.raise_for_status = lambda: None
        return resp

    return trip_id, stubs, _mock_post


def _apply_db_stubs(monkeypatch, stubs: Dict[str, Any]) -> None:
    import backend.infra.db_access as db

    monkeypatch.setattr(db, "get_trip_route_shape", lambda tid: stubs["get_trip_route_shape"])
    monkeypatch.setattr(db, "get_shape_line", lambda sid: stubs["get_shape_line"])
    monkeypatch.setattr(db, "get_stop_times_for_trip", lambda tid: stubs["get_stop_times_for_trip"])
    monkeypatch.setattr(db, "get_stop_lonlat_bulk", lambda sids: {s: stubs["get_stop_lonlat_bulk"][s] for s in sids if s in stubs["get_stop_lonlat_bulk"]})
    monkeypatch.setattr(db, "get_active_feed_id", lambda: stubs["get_active_feed_id"])
    monkeypatch.setattr(db, "osm_segments_intersecting_polygon", lambda *a, **kw: stubs["osm_segments_intersecting_polygon"])
    monkeypatch.setattr(db, "get_candidate_osm_segments_for_polyline", lambda *a, **kw: stubs["get_candidate_osm_segments_for_polyline"])
    monkeypatch.setattr(db, "get_gtfs_bus_way_evidence_bulk", lambda *a, **kw: stubs["get_gtfs_bus_way_evidence_bulk"])
    monkeypatch.setattr(db, "osm_segment_nodes_intersecting_polygon", lambda *a, **kw: stubs["osm_segment_nodes_intersecting_polygon"])
    monkeypatch.setattr(db, "fetch_osm_turn_restrictions_by_via_nodes", lambda *a, **kw: stubs["fetch_osm_turn_restrictions_by_via_nodes"])
    monkeypatch.setattr(db, "get_bus_edge_evidence_bulk", lambda *a, **kw: stubs.get("get_bus_edge_evidence_bulk") or {})
    monkeypatch.setattr(db, "get_bus_turn_evidence_bulk", lambda *a, **kw: stubs.get("get_bus_turn_evidence_bulk") or {})


@pytest.mark.parametrize("scenario", _load_scenarios(), ids=lambda s: s.get("id", "scenario"))
def test_detour_v2_scenario_contract(scenario: Dict[str, Any], monkeypatch):
    from backend.domain.detour_v2.compute import compute_detour_for_trip
    from backend.domain.detour_v2.policy import DetourPolicyConfig
    from backend.domain.detour_v2.trip_impact_analyzer import analyze_trip_impact

    trip_id, stubs, mock_post = _build_stubs(scenario)
    _apply_db_stubs(monkeypatch, stubs)
    exp = scenario.get("expect") or {}
    blockage = scenario["blockage_geojson"]
    pol = DetourPolicyConfig()
    pol.search.per_trip_deadline_ms = 30000
    pol.search.valhalla_concurrency = 1

    mock_kind = str(scenario.get("mock_valhalla") or "none")
    monkeypatch.setattr("backend.domain.detour_v2.compute.valhalla_locate", lambda *a, **kw: None)
    with ExitStack() as stack:
        stack.enter_context(patch("backend.adapters.osm_detour.httpx.post", MagicMock(side_effect=mock_post)))
        stack.enter_context(patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla-test:8002"))
        stack.enter_context(patch("backend.adapters.osm_detour.VALHALLA_TRACE_ATTRIBUTES_ENABLED", False))
        if mock_kind == "http_442":
            stack.enter_context(
                patch(
                    "backend.domain.detour_v2.compute.route_avoiding_polygon",
                    lambda *a, **kw: SimpleNamespace(
                        success=False, coordinates=[], time_s=0.0, distance_m=0.0, turn_by_turn=None
                    ),
                )
            )
        out = compute_detour_for_trip(
            trip_id=trip_id,
            blockage_geojson=blockage,
            service_date=str(scenario["service_date"]),
            policy=pol,
        )

    if "status" in exp:
        assert out.status == exp["status"], (
            f"{scenario['id']}: expected status={exp['status']} got {out.status} "
            f"(error={out.error}, attempts={[r.get('candidate_generation_reason') for r in (out.attempts or [])]})"
        )

    if exp.get("status") in ("no_impact", "no_safe_detour"):
        return

    assert out.status != "error", f"{scenario['id']}: {out.error}"

    min_tier = str(exp.get("min_tier_status") or "").lower()
    if min_tier:
        assert TIER_ORDER.get(out.status, -1) >= TIER_ORDER.get(min_tier, 0), (
            f"{scenario['id']}: status={out.status} wanted>={min_tier}"
        )

    mc = exp.get("min_confidence_score")
    if mc is not None and out.selected:
        assert float(out.selected.confidence_score or 0) >= float(mc) - 1e-6

    bspan = exp.get("blocked_span_m")
    tol = float(exp.get("blocked_span_tolerance_m") or 800.0)
    if bspan and stubs.get("get_shape_line"):
        impact = analyze_trip_impact(
            trip_id=trip_id,
            route_id="R_TEST",
            shape_id="SH_TEST",
            shape_line=stubs["get_shape_line"],
            blockage_geojson=blockage,
        )
        assert impact.blocked is not None
        lo = float(impact.blocked.blocked_start_m)
        hi = float(impact.blocked.blocked_end_m)
        assert lo <= float(bspan[1]) + tol and hi >= float(bspan[0]) - tol, (
            f"{scenario['id']}: blocked span [{lo},{hi}] vs expected {bspan} tol={tol}"
        )

    def _in_bbox(lon: float, lat: float, bbox: List[float]) -> bool:
        min_lon, min_lat, max_lon, max_lat = bbox
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    exb = exp.get("exit_bbox_lonlat")
    rjb = exp.get("rejoin_bbox_lonlat")
    if exb and out.anchors:
        assert _in_bbox(out.anchors.exit_lon, out.anchors.exit_lat, exb), scenario["id"]
    if rjb and out.anchors:
        assert _in_bbox(out.anchors.rejoin_lon, out.anchors.rejoin_lat, rjb), scenario["id"]

    mx_skip = exp.get("max_skipped_stops")
    if mx_skip is not None and out.stitching:
        assert len(out.stitching.skipped_stop_ids) <= int(mx_skip)

    forbidden = exp.get("forbidden_osm_way_ids") or []
    if forbidden and out.selected and out.selected.decoded:
        ways = {int(s.osm_way_id) for s in out.selected.decoded.road_segments if int(s.osm_way_id or 0) > 0}
        for wid in forbidden:
            assert int(wid) not in ways, f"{scenario['id']}: forbidden way {wid} appeared in detour"
