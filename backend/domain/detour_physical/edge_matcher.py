"""Map-match GTFS polylines to directed OSM edges via Valhalla trace_attributes."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Set, Tuple

from shapely.geometry import LineString
from shapely.ops import substring

from backend.adapters.osm_detour import match_route_attributes

from .edge_match_models import EdgeMatchResult, EdgeMatchScore
from .pattern_trace_split import (
    chunk_leg_ranges,
    chunk_leg_ranges_by_distance,
    choose_chunk_for_overlap_leg,
    cumulative_stop_meters_along_shape,
    ensure_linestring,
    extract_edges_for_shape_fractions,
    line_length_m,
    slice_shape_between_stop_indices,
)


def _line_len_m(line: LineString) -> float:
    try:
        return float(line.length * 111_320.0)
    except Exception:
        return 0.0


def densify_linestring(line: LineString, step_m: float = 15.0) -> List[Tuple[float, float]]:
    """Return (lon, lat) samples along the line."""
    if line.is_empty or line.geom_type != "LineString":
        return []
    total = _line_len_m(line)
    if total <= 0:
        c = list(line.coords)
        if not c:
            return []
        return [(float(c[0][0]), float(c[0][1]))]
    n = max(2, int(math.ceil(total / max(5.0, step_m))) + 1)
    out: List[Tuple[float, float]] = []
    for i in range(n):
        t = i / max(1, n - 1)
        p = line.interpolate(t, normalized=True)
        out.append((float(p.x), float(p.y)))
    return out


def _score_trace_edges(gtfs_line: LineString, edges: List[Dict[str, Any]]) -> EdgeMatchScore:
    """Heuristic score from Valhalla edge list (sequence continuity, coverage)."""
    if not edges:
        return EdgeMatchScore(total=-1e9)
    way_ids = [int(e.get("way_id") or 0) for e in edges]
    switches = sum(1 for i in range(1, len(way_ids)) if way_ids[i] != way_ids[i - 1] and way_ids[i] and way_ids[i - 1])
    continuity = 1.0 / (1.0 + switches * 0.15)
    lens = [float(e.get("length") or 0) for e in edges]
    traced = sum(lens)
    gtfs_len = max(1.0, _line_len_m(gtfs_line))
    coverage = min(1.5, traced / gtfs_len) if gtfs_len > 0 else 0.0
    total = continuity * 2.0 + coverage * 1.5 - switches * 0.05
    return EdgeMatchScore(
        continuity=continuity,
        coverage_ratio=min(1.0, coverage),
        mean_offset_m=0.0,
        mean_heading_error_deg=0.0,
        side_switch_count=switches,
        total=total,
    )


def trace_linestring_with_km_cap(
    line: LineString,
    *,
    max_km: float,
    costing: str = "bus",
    densify_m: float = 15.0,
) -> EdgeMatchResult:
    """Map-match ``line``, splitting into <= ``max_km`` slices when Valhalla rejects very long shapes."""
    if line.is_empty:
        return EdgeMatchResult(success=False, notes=["empty_line"])
    total_m = line_length_m(line)
    cap_m = max(1000.0, float(max_km) * 1000.0)
    if total_m <= cap_m:
        return match_gtfs_slice_to_osm_edges(line, costing=costing, densify_m=densify_m)
    edges_all: List[Dict[str, Any]] = []
    notes: List[str] = ["km_sliced_trace"]
    start_m = 0.0
    while start_m < total_m - 1.0:
        end_m = min(total_m, start_m + cap_m)
        t0, t1 = start_m / total_m, end_m / total_m
        sub = ensure_linestring(substring(line, t0, t1, normalized=True))
        res = match_gtfs_slice_to_osm_edges(sub, costing=costing, densify_m=densify_m)
        if not res.success or not res.edge_records:
            notes.append(f"subslice_fail_m={start_m:.0f}-{end_m:.0f}")
            return EdgeMatchResult(success=False, notes=notes)
        edges_all.extend(res.edge_records)
        start_m = end_m
    score = _score_trace_edges(line, edges_all) if edges_all else None
    return EdgeMatchResult(
        success=True,
        edge_records=edges_all,
        score=score,
        notes=notes + [f"edges={len(edges_all)}"],
    )


def match_full_shape_to_osm_edges(
    shape_line: LineString,
    *,
    costing: str = "bus",
    densify_m: float = 15.0,
) -> EdgeMatchResult:
    """
    Run Valhalla trace_attributes once on the entire representative shape polyline (Stage A).
    Same parameters as per-leg matching; use for pattern-level backfill.
    """
    return match_gtfs_slice_to_osm_edges(shape_line, costing=costing, densify_m=densify_m)


def score_gtfs_leg_against_edges(gtfs_leg_line: LineString, edges: List[Dict[str, Any]]) -> EdgeMatchScore:
    """Public wrapper for per-leg quality metrics (same heuristic as trace scoring)."""
    return _score_trace_edges(gtfs_leg_line, edges)


def trace_pattern_split_to_legs(
    shape_line: LineString,
    stop_rows: List[Dict[str, Any]],
    *,
    full_trace_max_km: float = 10.0,
    chunk_legs: int = 11,
    chunk_overlap: int = 1,
    costing: str = "bus",
    densify_m: float = 15.0,
    only_leg_indices: Optional[Set[int]] = None,
) -> Tuple[List[Optional[List[Dict[str, Any]]]], List[str]]:
    """
    Stage A+B: one trace (or overlapping chunk traces), then map each leg to an edge subchain.
    Returns (per_leg_edges, notes) — entries None where tracing/splitting failed for that leg.

    When only_leg_indices is set (resume / partial backfill), chunked mode only runs Valhalla for
    chunks that cover at least one of those leg indices, so large patterns do not re-trace the
    whole route when a few legs remain.
    """
    notes: List[str] = []
    n_stops = len(stop_rows)
    n_legs = max(0, n_stops - 1)
    if n_legs <= 0 or shape_line.is_empty:
        return [], ["no_legs_or_empty_shape"]
    if only_leg_indices is not None and not only_leg_indices:
        return [None] * n_legs, ["only_leg_indices_empty"]

    cum, S = cumulative_stop_meters_along_shape(shape_line, stop_rows)
    shape_km = line_length_m(shape_line) / 1000.0

    def leg_fractions(i: int) -> Tuple[float, float]:
        f0 = float(cum[i]) / S
        f1 = float(cum[i + 1]) / S
        return max(0.0, min(1.0, f0)), max(0.0, min(1.0, f1))

    def build_from_full(edges: List[Dict[str, Any]]) -> List[Optional[List[Dict[str, Any]]]]:
        out: List[Optional[List[Dict[str, Any]]]] = []
        for i in range(n_legs):
            f0, f1 = leg_fractions(i)
            leg = extract_edges_for_shape_fractions(edges, f0, f1)
            out.append(leg if leg else None)
        return out

    if shape_km <= full_trace_max_km:
        res = match_full_shape_to_osm_edges(shape_line, costing=costing, densify_m=densify_m)
        if res.success and res.edge_records:
            notes.append("full_shape_trace")
            full = build_from_full(res.edge_records)
            if only_leg_indices is not None:
                notes.append("masked_to_only_leg_indices")
                return [full[i] if i in only_leg_indices else None for i in range(n_legs)], notes
            return full, notes
        notes.append("full_trace_failed_fallback_chunk")

    max_span_m = float(full_trace_max_km) * 1000.0
    if shape_km > full_trace_max_km:
        ranges = chunk_leg_ranges_by_distance(
            n_legs,
            cum,
            max_span_m=max_span_m,
            max_legs_per_chunk=chunk_legs,
            overlap=chunk_overlap,
        )
        notes.append(f"distance_chunked chunks={len(ranges)}")
    else:
        ranges = chunk_leg_ranges(n_legs, chunk_legs=chunk_legs, overlap=chunk_overlap)
    if only_leg_indices is not None:
        need = only_leg_indices
        ranges = [(lo, hi) for (lo, hi) in ranges if any(lo <= k <= hi for k in need)]
        notes.append(f"partial_chunk_trace chunks={len(ranges)}")

    chunk_results: List[Tuple[Tuple[int, int], EdgeMatchResult]] = []
    for lo, hi in ranges:
        chunk_shape = slice_shape_between_stop_indices(shape_line, stop_rows, lo, hi + 1)
        res = trace_linestring_with_km_cap(
            chunk_shape,
            max_km=full_trace_max_km,
            costing=costing,
            densify_m=densify_m,
        )
        chunk_results.append(((lo, hi), res))
        if not res.success or not res.edge_records:
            notes.append(f"chunk_fail legs={lo}-{hi}")

    per_leg: List[Optional[List[Dict[str, Any]]]] = []
    for k in range(n_legs):
        if only_leg_indices is not None and k not in only_leg_indices:
            per_leg.append(None)
            continue
        candidates: List[Tuple[Tuple[int, int], EdgeMatchResult]] = []
        for (lo, hi), res in chunk_results:
            if lo <= k <= hi and res.success and res.edge_records:
                candidates.append(((lo, hi), res))
        if not candidates:
            per_leg.append(None)
            continue
        pick_i = choose_chunk_for_overlap_leg(k, candidates)
        (lo, hi), res = candidates[pick_i]
        d0 = float(cum[lo])
        d1 = float(cum[hi + 1])
        span = max(d1 - d0, 1e-6)
        ds = float(cum[k])
        de = float(cum[k + 1])
        f0 = (ds - d0) / span
        f1 = (de - d0) / span
        leg_edges = extract_edges_for_shape_fractions(res.edge_records, f0, f1)
        per_leg.append(leg_edges if leg_edges else None)

    notes.append("chunked_trace")
    return per_leg, notes


def match_gtfs_slice_to_osm_edges(
    gtfs_line: LineString,
    *,
    costing: str = "bus",
    densify_m: float = 15.0,
) -> EdgeMatchResult:
    """
    Densify the GTFS line and run Valhalla /trace_attributes (map_snap).
    Returns edge records for downstream persistence / validation.
    """
    if gtfs_line.is_empty:
        return EdgeMatchResult(success=False, notes=["empty_line"])
    pts = densify_linestring(gtfs_line, step_m=densify_m)
    if len(pts) < 2:
        return EdgeMatchResult(success=False, notes=["too_few_points"])
    edges = match_route_attributes(pts, costing=costing)
    if not edges:
        return EdgeMatchResult(success=False, notes=["trace_attributes_empty"])
    score = _score_trace_edges(gtfs_line, edges)
    amb = score.side_switch_count > 8
    return EdgeMatchResult(
        success=True,
        edge_records=edges,
        score=score,
        is_ambiguous=amb,
        notes=[f"edges={len(edges)}", f"score_total={score.total:.3f}"],
    )
