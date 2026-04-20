"""Intersect trip shape with blockage polygon; blocked interval along shape."""

from __future__ import annotations

from typing import Any, Dict, Optional

from shapely.geometry import LineString, shape as shp_shape
from shapely.ops import unary_union

from .models import BlockedShapeInterval, TripImpactResult


def _line_length_m(line: LineString) -> float:
    try:
        return float(line.length * 111_320.0)  # rough deg->m at Israel lat
    except Exception:
        return 0.0


def _cumulative_distances(line: LineString) -> list[float]:
    """Approximate cumulative distance (m) along line vertices."""
    coords = list(line.coords)
    if len(coords) < 2:
        return [0.0]
    d = [0.0]
    for i in range(1, len(coords)):
        p0 = coords[i - 1]
        p1 = coords[i]
        seg = LineString([p0, p1])
        d.append(d[-1] + _line_length_m(seg))
    return d


def blocked_interval_along_line(
    line: LineString,
    blockage_geojson: Dict[str, Any],
) -> Optional[BlockedShapeInterval]:
    """
    Find [blocked_start_m, blocked_end_m] along the shape where it meets the blockage polygon.
    Merges per-segment overlap using cumulative vertex distances (approximate meters).
    """
    try:
        poly = shp_shape(blockage_geojson)
        if poly.is_empty:
            return None
        if poly.geom_type not in ("Polygon", "MultiPolygon"):
            poly = poly.buffer(0)
    except Exception:
        return None

    total_m = _line_length_m(line)
    if total_m <= 0:
        return None

    cum = _cumulative_distances(line)
    coords = list(line.coords)
    ranges: list[tuple[float, float]] = []
    for i in range(len(coords) - 1):
        seg = LineString([coords[i], coords[i + 1]])
        try:
            if not seg.intersects(poly):
                continue
            inter = seg.intersection(poly)
            if inter.is_empty:
                continue
            lo_m = cum[i]
            hi_m = cum[i + 1]
            if inter.geom_type == "LineString" and len(inter.coords) >= 2:
                from shapely.geometry import Point

                d0 = seg.project(Point(inter.coords[0]))
                d1 = seg.project(Point(inter.coords[-1]))
                seg_len_m = hi_m - lo_m
                if seg.length > 0:
                    f0 = d0 / seg.length
                    f1 = d1 / seg.length
                    lo_m = cum[i] + min(f0, f1) * seg_len_m
                    hi_m = cum[i] + max(f0, f1) * seg_len_m
            ranges.append((lo_m, hi_m))
        except Exception:
            continue

    if not ranges:
        try:
            inter = line.intersection(poly)
            if inter.is_empty:
                return None
            if inter.geom_type == "LineString":
                from shapely.geometry import Point

                p0 = Point(inter.coords[0])
                p1 = Point(inter.coords[-1])
                d0 = float(line.project(p0)) / max(line.length, 1e-12) * total_m
                d1 = float(line.project(p1)) / max(line.length, 1e-12) * total_m
                lo, hi = min(d0, d1), max(d0, d1)
                return BlockedShapeInterval(lo, hi, total_m)
            if inter.geom_type == "MultiLineString":
                from shapely.geometry import Point

                g = max(inter.geoms, key=lambda x: x.length)
                p0 = Point(g.coords[0])
                p1 = Point(g.coords[-1])
                d0 = float(line.project(p0)) / max(line.length, 1e-12) * total_m
                d1 = float(line.project(p1)) / max(line.length, 1e-12) * total_m
                lo, hi = min(d0, d1), max(d0, d1)
                return BlockedShapeInterval(lo, hi, total_m)
        except Exception:
            pass
        return None

    lo = min(a for a, _ in ranges)
    hi = max(b for _, b in ranges)
    lo = max(0.0, lo - 10.0)
    hi = min(total_m, hi + 10.0)
    return BlockedShapeInterval(lo, hi, total_m)


def analyze_trip_impact(
    *,
    trip_id: str,
    route_id: str,
    shape_id: Optional[str],
    shape_line: Optional[Any],
    blockage_geojson: Dict[str, Any],
) -> TripImpactResult:
    if shape_line is None or getattr(shape_line, "is_empty", True):
        return TripImpactResult(
            trip_id=trip_id,
            route_id=route_id,
            shape_id=shape_id,
            blocked=None,
            intersects_blockage=False,
        )
    line = shape_line
    if line.geom_type != "LineString":
        return TripImpactResult(
            trip_id=trip_id,
            route_id=route_id,
            shape_id=shape_id,
            blocked=None,
            intersects_blockage=False,
        )

    try:
        poly = unary_union([shp_shape(blockage_geojson)])
        intersects = bool(line.intersects(poly))
    except Exception:
        intersects = False

    blocked = blocked_interval_along_line(line, blockage_geojson) if intersects else None
    return TripImpactResult(
        trip_id=trip_id,
        route_id=route_id,
        shape_id=shape_id,
        blocked=blocked,
        intersects_blockage=intersects and blocked is not None,
    )
