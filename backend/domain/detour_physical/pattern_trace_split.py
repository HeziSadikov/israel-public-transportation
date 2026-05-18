"""
Split a single Valhalla trace (ordered edge list) into per-leg subsequences using
cumulative distances along the GTFS shape vs cumulative matched-path length.

See backfill plan: Stage B — map stop intervals onto the matched edge chain.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from shapely.geometry import LineString
from shapely.ops import linemerge, substring


def ensure_linestring(geom: Any) -> LineString:
    """
    PostGIS pattern_edge / summary columns are LINESTRING. Shapely substring() can return a
    Point when the interval is degenerate; coerce to a minimal 2-point LineString.
    """
    if geom is None or geom.is_empty:
        return LineString()
    gt = geom.geom_type
    if gt == "LineString":
        return geom
    if gt == "Point":
        x, y = float(geom.x), float(geom.y)
        return LineString([(x, y), (x, y)])
    if gt == "MultiLineString":
        merged = linemerge(geom)
        if merged.geom_type == "LineString":
            return merged
        if merged.geom_type == "Point":
            x, y = float(merged.x), float(merged.y)
            return LineString([(x, y), (x, y)])
        try:
            return ensure_linestring(merged.geoms[0])
        except Exception:
            return LineString()
    try:
        coords = list(geom.coords)
        if len(coords) >= 2:
            return LineString(coords)
        if len(coords) == 1:
            c = coords[0]
            return LineString([c, c])
    except Exception:
        pass
    return LineString()


def slice_shape_between_stop_indices(
    shape_line: LineString,
    stop_rows: List[Dict[str, Any]],
    idx_a: int,
    idx_b: int,
) -> LineString:
    """
    Slice the shape between stop index idx_a and idx_b (both into stop_rows).
    Uses shape_dist_traveled when both present; else straight chord between endpoints.
    """
    total_m = line_length_m(shape_line) or 1.0
    sa = stop_rows[idx_a]
    sb = stop_rows[idx_b]
    da = sa.get("shape_dist_traveled")
    db_ = sb.get("shape_dist_traveled")
    if da is not None and db_ is not None:
        try:
            d0, d1 = float(da), float(db_)
            t0 = max(0.0, min(1.0, d0 / total_m))
            t1 = max(0.0, min(1.0, d1 / total_m))
            if t1 <= t0:
                t1 = min(1.0, t0 + 1e-5)
            return ensure_linestring(substring(shape_line, t0, t1, normalized=True))
        except Exception:
            pass
    lon0, lat0 = shape_line.coords[0]
    lon1, lat1 = shape_line.coords[-1]
    return ensure_linestring(LineString([(lon0, lat0), (lon1, lat1)]))


def line_length_m(line: LineString) -> float:
    try:
        return float(line.length * 111_320.0)
    except Exception:
        return 0.0


def cumulative_stop_meters_along_shape(shape_line: LineString, stop_rows: List[Dict[str, Any]]) -> Tuple[List[float], float]:
    """
    Cumulative distance (m) from trip start along the shape to each stop index.
    Uses shape_dist_traveled when present; otherwise proportional spacing along total length.
    Enforces non-decreasing distances. Returns (cumulative_per_index, S) where S normalizes leg fractions.
    """
    total_m = line_length_m(shape_line)
    n = len(stop_rows)
    if n == 0:
        return [], max(total_m, 1e-6)
    out: List[float] = []
    for i, r in enumerate(stop_rows):
        sdt = r.get("shape_dist_traveled")
        if sdt is not None:
            try:
                out.append(float(sdt))
            except Exception:
                out.append((i / max(1, n - 1)) * total_m)
        else:
            out.append((i / max(1, n - 1)) * total_m)
    for i in range(1, n):
        if out[i] < out[i - 1]:
            out[i] = out[i - 1]
    s_use = max(out[-1], total_m, 1e-6)
    return out, s_use


def edge_cumulative_meters(edges: List[Dict[str, Any]]) -> Tuple[List[float], float]:
    """cum[i] = distance to start of edge i along matched path; total M at end of last edge."""
    cum: List[float] = [0.0]
    for e in edges:
        lm = float(e.get("length") or 0.0) * 1000.0
        cum.append(cum[-1] + lm)
    m = cum[-1] if cum else 0.0
    return cum, m


def extract_edges_for_shape_fractions(
    full_edges: List[Dict[str, Any]],
    f0: float,
    f1: float,
) -> List[Dict[str, Any]]:
    """
    Map interval [f0, f1] (fractions 0..1 along matched path) to a contiguous sublist of edges.
    Linear alignment: position along matched path = fraction * M.
    """
    if not full_edges or f1 <= f0 + 1e-12:
        return []
    f0 = max(0.0, min(1.0, f0))
    f1 = max(0.0, min(1.0, f1))
    if f1 <= f0:
        return []
    cum, m = edge_cumulative_meters(full_edges)
    if m <= 0:
        return []
    ta = f0 * m
    tb = f1 * m
    n = len(full_edges)
    lo = 0
    for i in range(n):
        if cum[i + 1] > ta + 1e-9:
            lo = i
            break
    hi = lo
    for j in range(lo, n):
        hi = j
        if cum[j + 1] >= tb - 1e-6:
            break
    return full_edges[lo : hi + 1]


def chunk_leg_ranges_by_distance(
    n_legs: int,
    cum: List[float],
    *,
    max_span_m: float,
    max_legs_per_chunk: int = 11,
    overlap: int = 1,
) -> List[Tuple[int, int]]:
    """Inclusive leg ranges whose stop span along the shape is at most ``max_span_m``."""
    if n_legs <= 0:
        return []
    max_legs = max(1, int(max_legs_per_chunk))
    ov = max(0, int(overlap))
    out: List[Tuple[int, int]] = []
    lo = 0
    while lo < n_legs:
        hi = lo
        while hi < n_legs - 1:
            nxt = hi + 1
            span = float(cum[nxt + 1]) - float(cum[lo])
            legs = nxt - lo + 1
            if span > max_span_m or legs >= max_legs:
                break
            hi = nxt
        out.append((lo, hi))
        if hi >= n_legs - 1:
            break
        lo = max(lo + 1, hi - ov + 1)
    return out


def chunk_leg_ranges(
    n_legs: int,
    *,
    chunk_legs: int,
    overlap: int,
) -> List[Tuple[int, int]]:
    """
    Inclusive leg index ranges [first_leg, last_leg] for each chunk.
    overlap: e.g. 1 means consecutive chunks share one leg.
    """
    if n_legs <= 0:
        return []
    step = max(1, chunk_legs - overlap)
    out: List[Tuple[int, int]] = []
    start = 0
    while start < n_legs:
        end = min(n_legs - 1, start + chunk_legs - 1)
        out.append((start, end))
        if end >= n_legs - 1:
            break
        start += step
    return out


def choose_chunk_for_overlap_leg(
    leg_index: int,
    chunk_results: List[Tuple[Tuple[int, int], Any]],
) -> int:
    """
    When a leg is in multiple chunks, pick chunk index: prefer chunk whose [lo,hi] contains
    leg_index; tie-break by higher EdgeMatchScore.coverage_ratio on the chunk trace.
    """
    if not chunk_results:
        return 0
    best_i = 0
    best_key = (-1, -1.0)
    for i, ((lo, hi), res) in enumerate(chunk_results):
        contains = 1 if (lo <= leg_index <= hi) else 0
        cov = 0.0
        if res is not None and getattr(res, "score", None) is not None:
            cov = float(getattr(res.score, "coverage_ratio", 0.0) or 0.0)
        key = (contains, cov)
        if key > best_key:
            best_key = key
            best_i = i
    return best_i
