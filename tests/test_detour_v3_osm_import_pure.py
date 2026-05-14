"""
Pure-Python unit tests for the detour v3 OSM importer helpers.

Covers ``backend.osm_import.access_rules`` and
``backend.osm_import.turn_restrictions``. Pyosmium / Postgres are not
required for these tests.
"""

from __future__ import annotations

import pytest

from backend.osm_import.access_rules import (
    HIGHWAY_ALLOWLIST,
    HIGHWAY_HARD_BLOCKLIST,
    is_bus_usable,
    is_highway_candidate,
    oneway_direction,
)
from backend.osm_import.turn_restrictions import (
    RestrictionRow,
    parse_restriction_relation,
)


# ---------------------------------------------------------------------------
# access_rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "highway,expected",
    [
        ("primary", True),
        ("residential", True),
        ("service", True),
        ("living_street", True),
        ("busway", True),
        ("motorway_link", True),
        ("primary_link", True),
        # blocklisted
        ("footway", False),
        ("path", False),
        ("cycleway", False),
        ("steps", False),
        ("pedestrian", False),
        ("construction", False),
        ("proposed", False),
        # unknown class without overrides
        ("track", False),
        (None, False),
        ("", False),
    ],
)
def test_is_highway_candidate_basic_classes(highway, expected):
    tags = {"highway": highway} if highway else {}
    assert is_highway_candidate(highway, tags) is expected


def test_is_highway_candidate_unknown_class_unlocked_by_bus_override():
    assert is_highway_candidate("track", {"highway": "track", "bus": "yes"}) is True
    assert is_highway_candidate("track", {"highway": "track", "psv": "designated"}) is True
    assert is_highway_candidate("track", {"highway": "track", "access": "yes"}) is True


def test_is_highway_candidate_blocklist_overrides_bus():
    # Even bus=yes does not unlock hard-blocklisted classes.
    assert (
        is_highway_candidate("footway", {"highway": "footway", "bus": "yes"}) is False
    )
    assert (
        is_highway_candidate("steps", {"highway": "steps", "psv": "designated"}) is False
    )


def test_is_bus_usable_overrides_and_denies():
    # Permissive: bus/psv override even on weird classes.
    assert is_bus_usable({"highway": "track", "bus": "designated"}) is True
    assert is_bus_usable({"highway": "service", "psv": "yes"}) is True
    # Restrictive access.
    assert is_bus_usable({"highway": "primary", "access": "private"}) is False
    assert is_bus_usable({"highway": "primary", "motor_vehicle": "no"}) is False
    assert is_bus_usable({"highway": "primary", "vehicle": "no"}) is False
    assert is_bus_usable({"highway": "primary", "bus": "no"}) is False
    assert is_bus_usable({"highway": "primary", "psv": "no"}) is False
    # Plain bus-usable.
    assert is_bus_usable({"highway": "primary"}) is True
    assert is_bus_usable({"highway": "residential"}) is True
    # Hard-blocklisted is still not bus-usable absent the override path.
    assert is_bus_usable({"highway": "footway"}) is False


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, "both"),
        ({"oneway": "yes"}, "forward"),
        ({"oneway": "true"}, "forward"),
        ({"oneway": "1"}, "forward"),
        ({"oneway": "-1"}, "backward"),
        ({"oneway": "reverse"}, "backward"),
        ({"oneway": "no"}, "both"),
        ({"oneway": "false"}, "both"),
        ({"junction": "roundabout"}, "forward"),
        ({"junction": "roundabout", "oneway": "no"}, "both"),
        ({"junction": "circular"}, "forward"),
    ],
)
def test_oneway_direction(tags, expected):
    assert oneway_direction(tags) == expected


def test_allowlist_blocklist_disjoint():
    # Sanity: a highway can't be in both sets.
    assert HIGHWAY_ALLOWLIST.isdisjoint(HIGHWAY_HARD_BLOCKLIST)


# ---------------------------------------------------------------------------
# turn_restrictions
# ---------------------------------------------------------------------------


def _members(*items):
    """Build a list of (type, ref, role) tuples."""
    return list(items)


def test_parse_no_left_turn_via_node_basic():
    row = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_left_turn"},
        _members(("way", 100, "from"), ("node", 200, "via"), ("way", 300, "to")),
    )
    assert isinstance(row, RestrictionRow)
    assert (
        row.from_way_id,
        row.via_node_id,
        row.via_way_id,
        row.to_way_id,
        row.restriction_type,
    ) == (100, 200, None, 300, "no_left_turn")
    assert row.applies_to_bus is False
    assert row.except_bus is False
    assert row.except_psv is False


def test_parse_restriction_bus_only_right_turn_via_way_with_except_psv():
    row = parse_restriction_relation(
        "restriction:bus",
        {
            "type": "restriction:bus",
            "restriction:bus": "only_right_turn",
            "except": "psv",
        },
        _members(("way", 1, "from"), ("way", 2, "via"), ("way", 3, "to")),
    )
    assert row is not None
    assert row.via_node_id is None
    assert row.via_way_id == 2
    assert row.applies_to_bus is True
    assert row.except_psv is True
    assert row.except_bus is False
    assert row.restriction_type == "only_right_turn"


def test_parse_restriction_falls_back_to_plain_tag_for_bus_type():
    # restriction:bus type but only a plain "restriction" tag.
    row = parse_restriction_relation(
        "restriction:bus",
        {"type": "restriction:bus", "restriction": "no_u_turn"},
        _members(("way", 1, "from"), ("node", 5, "via"), ("way", 2, "to")),
    )
    assert row is not None
    assert row.restriction_type == "no_u_turn"
    assert row.applies_to_bus is True


def test_parse_unsupported_restriction_value_returns_none():
    row = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_motor_vehicle"},
        _members(("way", 1, "from"), ("node", 2, "via"), ("way", 3, "to")),
    )
    assert row is None


def test_parse_missing_via_member_returns_none():
    row = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_left_turn"},
        _members(("way", 1, "from"), ("way", 3, "to")),
    )
    assert row is None


def test_parse_missing_from_or_to_returns_none():
    no_from = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_left_turn"},
        _members(("node", 1, "via"), ("way", 3, "to")),
    )
    assert no_from is None

    no_to = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_left_turn"},
        _members(("way", 1, "from"), ("node", 2, "via")),
    )
    assert no_to is None


def test_parse_non_restriction_type_returns_none():
    row = parse_restriction_relation(
        "route",
        {"type": "route"},
        _members(),
    )
    assert row is None


def test_parse_except_splits_on_comma_and_semicolon():
    row = parse_restriction_relation(
        "restriction",
        {
            "type": "restriction",
            "restriction": "no_right_turn",
            "except": "bus; psv,emergency",
        },
        _members(("way", 1, "from"), ("node", 2, "via"), ("way", 3, "to")),
    )
    assert row is not None
    assert row.except_bus is True
    assert row.except_psv is True


def test_parse_accepts_pyosmium_style_short_member_types():
    """pyosmium RelationMember.type is 'n'/'w'/'r'. Make sure we normalize."""

    class _M:
        def __init__(self, t, ref, role):
            self.type = t
            self.ref = ref
            self.role = role

    members = [_M("w", 7, "from"), _M("n", 8, "via"), _M("w", 9, "to")]
    row = parse_restriction_relation(
        "restriction",
        {"type": "restriction", "restriction": "no_straight_on"},
        members,
    )
    assert row is not None
    assert (row.from_way_id, row.via_node_id, row.to_way_id) == (7, 8, 9)
