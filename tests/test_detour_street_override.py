from __future__ import annotations

from backend import db_access
from backend.detour_geo_validation import (
    road_geojson_clear_of_blockage,
    road_linestring_from_geojson,
)
from backend.detour_instructions_text import instructions_text_he_to_steps
from backend.detour_street_override_response import build_detour_response_from_road_override


def test_instructions_text_he_to_steps_splits_commas_and_semicolons() -> None:
    steps = instructions_text_he_to_steps("ימינה בבלפור; שמאלה ברחביה, ישר")
    assert [s["instruction_he"] for s in steps] == ["ימינה בבלפור", "שמאלה ברחביה", "ישר"]


def test_instructions_text_he_to_steps_arabic_semicolon_and_newlines() -> None:
    text = "שורה א\u061b שורה ב\nשורה ג"
    steps = instructions_text_he_to_steps(text)
    assert [s["instruction_he"] for s in steps] == ["שורה א", "שורה ב", "שורה ג"]


def test_instructions_text_he_to_steps_empty() -> None:
    assert instructions_text_he_to_steps("") == []
    assert instructions_text_he_to_steps("   ,  ;  ") == []


def test_build_street_override_key_stable() -> None:
    k1 = db_access.build_street_override_key(
        "by_area", "R1", "0", "abc", "S1", "S2", "sig1"
    )
    k2 = db_access.build_street_override_key(
        "by_area", "R1", "0", "abc", "S1", "S2", "sig1"
    )
    assert k1 == k2
    k3 = db_access.build_street_override_key(
        "point", "R1", "0", "abc", "S1", "S2", "sig1"
    )
    assert k1 != k3


def test_road_clear_of_blockage_accepts_skirting_line() -> None:
    road = {
        "type": "LineString",
        "coordinates": [[34.78, 32.08], [34.79, 32.09]],
    }
    blockage = {
        "type": "Polygon",
        "coordinates": [[[34.8, 32.1], [34.81, 32.1], [34.81, 32.11], [34.8, 32.11], [34.8, 32.1]]],
    }
    assert road_geojson_clear_of_blockage(road, blockage) is True


def test_road_clear_of_blockage_rejects_through_polygon() -> None:
    road = {
        "type": "LineString",
        "coordinates": [[34.805, 32.105], [34.807, 32.106]],
    }
    blockage = {
        "type": "Polygon",
        "coordinates": [[[34.8, 32.1], [34.81, 32.1], [34.81, 32.11], [34.8, 32.11], [34.8, 32.1]]],
    }
    assert road_geojson_clear_of_blockage(road, blockage) is False


def test_road_linestring_empty_for_bad_geojson() -> None:
    assert road_linestring_from_geojson({}).is_empty


def test_build_detour_response_from_road_override() -> None:
    road = {"type": "LineString", "coordinates": [[34.8, 32.0], [34.81, 32.01]]}
    res = build_detour_response_from_road_override(
        road,
        [{"instruction_he": "ימינה", "instruction_en": "Right"}],
        blocked_edges_count=1,
        blocked_edges_geojson={"type": "FeatureCollection", "features": []},
        stop_path=["A", "B"],
        baseline_travel_time_s=100.0,
        baseline_distance_m=500.0,
        used_shape=False,
        used_osm_snapping=False,
        feed_version="v1",
        from_override=True,
    )
    assert res.from_override is True
    assert res.stop_path == ["A", "B"]
    assert res.turn_by_turn and res.turn_by_turn[0].instruction_he == "ימינה"
    assert res.path_geojson.get("type") == "FeatureCollection"
