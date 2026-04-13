from __future__ import annotations

import pytest

from backend.detour_instructions_text import (
    merged_steps_to_geocode_queries,
    step_dict_to_geocode_query,
)
from backend.detour_narrative_geo import try_build_narrative_detour_linestring
from backend.osm_detour import OSMDetourResult, route_waypoints_avoiding_polygon


def test_step_dict_prefers_street() -> None:
    assert step_dict_to_geocode_query(
        {"street": "רחוב בזל", "instruction_he": "ימינה משה"}
    ) == "רחוב בזל"


def test_step_dict_strips_hebrew_turn_prefix() -> None:
    q = step_dict_to_geocode_query({"instruction_he": "שמאלה הרב יצחק רובינשטיין"})
    assert q is not None
    assert "רובינשטיין" in q
    assert not q.startswith("שמאלה")


def test_merged_steps_to_geocode_queries_order() -> None:
    steps = [
        {"instruction_he": "שמאלה כיכר וולפסון"},
        {"instruction_he": "ימינה הרב יצחק רובינשטיין"},
    ]
    qs = merged_steps_to_geocode_queries(steps)
    assert len(qs) == 2
    assert "וולפסון" in qs[0]


def test_try_build_none_without_valhalla(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.detour_narrative_geo.VALHALLA_URL", "")
    assert (
        try_build_narrative_detour_linestring(
            [{"instruction_he": "א"}, {"instruction_he": "ב"}], {}
        )
        is None
    )


def test_try_build_success_mocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.detour_narrative_geo.VALHALLA_URL", "http://localhost:8002")
    monkeypatch.setattr(
        "backend.detour_narrative_geo.geocode_ordered_waypoints",
        lambda queries, **kw: [(34.78, 32.08), (34.79, 32.09)],
    )

    def _fake_route(
        waypoints: list,
        blockage,
    ) -> OSMDetourResult:
        return OSMDetourResult(
            coordinates=[(34.78, 32.08), (34.785, 32.085), (34.79, 32.09)],
            distance_m=500.0,
            time_s=60.0,
            success=True,
            turn_by_turn=None,
        )

    monkeypatch.setattr("backend.detour_narrative_geo.route_waypoints_avoiding_polygon", _fake_route)
    monkeypatch.setattr("backend.detour_narrative_geo.map_match_coordinates", lambda c: None)

    blockage = {
        "type": "Polygon",
        "coordinates": [
            [[34.8, 32.1], [34.81, 32.1], [34.81, 32.11], [34.8, 32.11], [34.8, 32.1]]
        ],
    }
    road = try_build_narrative_detour_linestring(
        [
            {"instruction_he": "שמאלה רחוב א"},
            {"instruction_he": "ימינה רחוב ב"},
        ],
        blockage,
    )
    assert road is not None
    assert road["type"] == "LineString"
    assert len(road["coordinates"]) >= 2


def test_route_waypoints_requires_two_points(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.osm_detour.VALHALLA_URL", "http://x")
    r = route_waypoints_avoiding_polygon([(1.0, 1.0)], {})
    assert r.success is False
