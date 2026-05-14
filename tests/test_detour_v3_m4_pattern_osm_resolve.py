"""Pure tests for detour M4 pattern→segment resolution (no DB)."""

from __future__ import annotations

from backend.bus_corridor.trace_segment_resolve import (
    dedupe_consecutive_trace_edges,
    flatten_per_leg_trace_edges,
    pick_segment_for_trace_edge,
    resolve_trace_edges_to_segment_ids,
)


def test_flatten_per_leg_rejects_missing_leg():
    out = flatten_per_leg_trace_edges([[{"way_id": 1}], None, [{"way_id": 2}]])
    assert out is None


def test_flatten_and_dedupe_consecutive_duplicate_triples():
    e1 = {"way_id": 10, "begin_osm_node_id": 1, "end_osm_node_id": 2}
    e_dup = dict(e1)
    per = [[e1], [e_dup, {"way_id": 11, "begin_osm_node_id": 2, "end_osm_node_id": 3}]]
    flat = flatten_per_leg_trace_edges(per)
    assert flat is not None and len(flat) == 2


def test_dedupe_consecutive_keeps_ordered_distinct_edges():
    e1 = {"way_id": 1, "begin_osm_node_id": 100, "end_osm_node_id": 200}
    e2 = {"way_id": 2, "begin_osm_node_id": 200, "end_osm_node_id": 300}
    e_dup = dict(e1)
    got = dedupe_consecutive_trace_edges([e1, e_dup, e2])
    assert [x["way_id"] for x in got] == [1, 2]


def test_resolve_single_triple_via_db_stub_map():
    edge = {"way_id": 99, "begin_osm_node_id": 11, "end_osm_node_id": 22, "begin_heading": 88.0}
    triple_map = {
        (99, 11, 22): [
            {
                "segment_id": 1001,
                "heading_start_deg": 91.0,
                "heading_end_deg": None,
                "import_source": "detour_v3_pbf_import",
                "length_m": 120.0,
            },
            {
                "segment_id": 1002,
                "heading_start_deg": 5.0,
                "heading_end_deg": None,
                "import_source": None,
                "length_m": 80.0,
            },
        ],
    }
    ids, unres = resolve_trace_edges_to_segment_ids([edge], triple_map, v3_import_source="detour_v3_pbf_import")
    assert unres == 0 and ids == [1001]


def test_pick_fallback_prefers_deterministic_lowest_segment_when_headings_miss():
    edge = {"way_id": 1, "begin_osm_node_id": 1, "end_osm_node_id": 2}
    cand = [
        {"segment_id": 20, "heading_start_deg": None, "import_source": "detour_v3_pbf_import", "length_m": 10},
        {"segment_id": 10, "heading_start_deg": None, "import_source": None, "length_m": 10},
    ]
    sid = pick_segment_for_trace_edge(edge, 0, cand, v3_import_source="detour_v3_pbf_import")
    assert sid == 20


def test_pick_tie_prefers_lowest_sid_among_legacy_rows():
    edge = {"way_id": 1, "begin_osm_node_id": 1, "end_osm_node_id": 2}
    cand = [
        {"segment_id": 30, "heading_start_deg": None, "import_source": None, "length_m": 50},
        {"segment_id": 7, "heading_start_deg": None, "import_source": None, "length_m": 50},
    ]
    sid = pick_segment_for_trace_edge(edge, 0, cand, v3_import_source="detour_v3_pbf_import")
    assert sid == 7


def test_resolve_fails_when_no_rows_for_triple():
    edge = {"way_id": 1, "begin_osm_node_id": 1, "end_osm_node_id": 2}
    ids, hint = resolve_trace_edges_to_segment_ids([edge], {}, v3_import_source="detour_v3_pbf_import")
    assert ids is None and hint == 1
