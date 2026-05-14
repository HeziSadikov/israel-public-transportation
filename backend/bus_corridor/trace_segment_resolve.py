"""
Detour M4: turn Valhalla trace edge records into triples keyed like ``osm_road_segments``
and resolve them to ``segment_id`` (with duplicate-row disambiguation).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


def angular_distance_deg(a: float, b: float) -> float:
    d = abs((float(a) - float(b)) % 360.0)
    return min(d, 360.0 - d)


def edge_way_and_endpoint_nodes(edge: Dict[str, Any], ordinal: int, way_id: int) -> Tuple[int, int]:
    """Infer (from_osm_node, to_osm_node) matching backfill sentinel usage when IDs are missing."""
    a = edge.get("begin_osm_node_id") or edge.get("begin_node_id") or edge.get("begin_node")
    b = edge.get("end_osm_node_id") or edge.get("end_node_id") or edge.get("end_node")
    wa = abs(way_id)
    try:
        fn = int(a) if a is not None else -(wa * 1_000_000 + ordinal)
    except Exception:
        fn = -(wa * 1_000_000 + ordinal)
    try:
        tn = int(b) if b is not None else -(wa * 1_000_000 + ordinal + 1)
    except Exception:
        tn = -(wa * 1_000_000 + ordinal + 1)
    return fn, tn


def trace_edge_resolution_triple(edge: Dict[str, Any], ordinal: int) -> Tuple[int, int, int]:
    """(osm_way_id, from_node_id, to_node_id) — node ids agree with legacy pattern_edge tooling."""
    way_id = int(edge.get("way_id") or 0)
    fn, tn = edge_way_and_endpoint_nodes(edge, ordinal, way_id)
    return way_id, fn, tn


def flatten_per_leg_trace_edges(per_leg: Sequence[Optional[List[Dict[str, Any]]]]) -> Optional[List[Dict[str, Any]]]:
    """Concatenate per-leg edge lists from ``trace_pattern_split_to_legs``; fail if any leg is missing."""
    out: List[Dict[str, Any]] = []
    for chunk in per_leg:
        if not chunk:
            return None
        out.extend(chunk)
    return dedupe_consecutive_trace_edges(out)


def dedupe_consecutive_trace_edges(edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop adjacent duplicates that repeat the same OSM traversal triple."""
    if not edges:
        return edges
    flat: List[Dict[str, Any]] = []
    for idx, edge in enumerate(edges):
        w, fn, tn = trace_edge_resolution_triple(edge, idx)
        if flat:
            prev_i = len(flat) - 1
            pw, pfn, ptn = trace_edge_resolution_triple(flat[-1], prev_i)
            if (w, fn, tn) == (pw, pfn, ptn):
                continue
        flat.append(edge)
    return flat


def _candidate_heading_cost(edge: Dict[str, Any], cand: Dict[str, Any]) -> float:
    bh = edge.get("begin_heading")
    eh = edge.get("end_heading")
    hs = cand.get("heading_start_deg")
    he = cand.get("heading_end_deg")
    cost = 0.0
    parts = 0
    if bh is not None and hs is not None:
        cost += angular_distance_deg(float(bh), float(hs))
        parts += 1
    if eh is not None and he is not None:
        cost += angular_distance_deg(float(eh), float(he))
        parts += 1
    return cost / max(1, parts)


def pick_segment_for_trace_edge(
    edge: Dict[str, Any],
    ordinal: int,
    candidates: Sequence[Dict[str, Any]],
    *,
    import_source_bonus: float = -15.0,
    v3_import_source: str,
) -> Optional[int]:
    """
    Among DB rows matching the (way, from, to) triple, prefer v3-imported geometry and heading fit.

    ``import_source_bonus`` is subtracted from cost when row import_source equals v3_import_source.
    When heading-based costs tie, prefer v3 source then smallest ``segment_id`` (deterministic).
    """
    if not candidates:
        return None
    if len(candidates) == 1:
        return int(candidates[0]["segment_id"])
    scored: List[Tuple[float, int, Dict[str, Any]]] = []
    for cand in candidates:
        hcost = _candidate_heading_cost(edge, cand)
        isrc = cand.get("import_source")
        pref = import_source_bonus if isrc == v3_import_source else 0.0
        len_m = cand.get("length_m")
        len_penalty = float(len_m) / 5000.0 if len_m is not None else 0.0
        scored.append((hcost + len_penalty + pref, int(cand["segment_id"]), cand))
    scored.sort(key=lambda x: x[0])
    low = scored[0][0]
    tied_scored = [x for x in scored if abs(float(x[0]) - float(low)) < 1e-6]
    if len(tied_scored) == 1:
        return int(tied_scored[0][1])
    def _tie_break(x: Tuple[float, int, Dict[str, Any]]) -> Tuple[int, int]:
        cand = x[2]
        is_v3 = 1 if cand.get("import_source") == v3_import_source else 0
        sid = int(x[1])
        return (-is_v3, sid)

    tied_scored.sort(key=_tie_break)
    return int(tied_scored[0][1])


def resolve_trace_edges_to_segment_ids(
    edges: Sequence[Dict[str, Any]],
    triple_to_rows: Dict[Tuple[int, int, int], List[Dict[str, Any]]],
    *,
    v3_import_source: str,
) -> Tuple[Optional[List[int]], int]:
    """
    Map each trace edge to ``segment_id`` using pre-fetched candidates per triple.

    Returns (segment_id list or None on missing way_id / unresolved triple, unresolved_count hint).
    """
    ids: List[int] = []
    unresolved = 0
    for ordinal, edge in enumerate(edges):
        way_id = int(edge.get("way_id") or 0)
        if way_id <= 0:
            return None, len(edges)
        triple = trace_edge_resolution_triple(edge, ordinal)
        cand = triple_to_rows.get(triple) or []
        if not cand:
            unresolved += 1
            return None, unresolved
        sid = pick_segment_for_trace_edge(
            edge, ordinal, cand, v3_import_source=v3_import_source
        )
        ids.append(int(sid))
    deduped_ids: List[int] = []
    for sid in ids:
        if deduped_ids and deduped_ids[-1] == sid:
            continue
        deduped_ids.append(sid)
    return deduped_ids, 0


__all__ = [
    "angular_distance_deg",
    "flatten_per_leg_trace_edges",
    "dedupe_consecutive_trace_edges",
    "trace_edge_resolution_triple",
    "resolve_trace_edges_to_segment_ids",
]
