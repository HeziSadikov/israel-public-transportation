"""Locate OSM road intersections along a route shape for use as detour anchors.

A bus can only legally diverge from its route at a road intersection, not at a
mid-block stop. This module traces the route shape through Valhalla
/trace_attributes, inspects the ``end_node.intersecting_edges`` returned per
edge, and returns only nodes where a drivable cross-street meets the route.

The caller (compute.py) uses these IntersectionPoints instead of GTFS stop
positions when choosing exit / rejoin anchor candidates.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString, Point


# Road classes that are NOT drivable by a bus and must not be counted as
# valid cross-streets at a candidate junction.
_NON_DRIVABLE_CLASSES = frozenset({
    "service", "service_other", "alley", "driveway", "parking_aisle",
    "footway", "path", "cycleway", "pedestrian", "steps", "bridleway",
    "track", "living_street",
})

# Minimum accepted driveability values from Valhalla intersecting_edge dict.
_DRIVABLE = frozenset({"forward", "backward", "both"})


@dataclass(frozen=True)
class IntersectionPoint:
    """An OSM node along the route shape where at least one drivable cross-street meets the road."""

    shape_dist_m: float
    """Cumulative distance along the *original* route shape (meters)."""

    lon: float
    lat: float

    osm_node_id: int = 0
    """Real OSM node id when Valhalla returned end_osm_node_id; 0 otherwise."""

    road_class: str = ""
    """Road class of the route edge whose end-node this is (e.g. 'primary')."""

    cross_road_classes: Tuple[str, ...] = field(default_factory=tuple)
    """Road classes of the drivable cross-streets (excluding the through-road itself)."""

    cross_count: int = 0
    """Number of drivable cross-streets."""


def _line_length_m(coords: List[Tuple[float, float]]) -> float:
    if len(coords) < 2:
        return 0.0
    total = 0.0
    for i in range(len(coords) - 1):
        dx = (coords[i + 1][0] - coords[i][0]) * math.cos(math.radians(coords[i][1])) * 111_320.0
        dy = (coords[i + 1][1] - coords[i][1]) * 111_320.0
        total += math.hypot(dx, dy)
    return total


def _clip_coords_to_window(
    coords: List[Tuple[float, float]],
    total_m: float,
    start_m: float,
    end_m: float,
) -> List[Tuple[float, float]]:
    """Return the slice of *coords* that covers [start_m, end_m] along the route.

    Used only to limit the size of the /trace_attributes request.  The
    returned coordinates are still in the original geographic space so the
    subsequent Shapely projection gives correct original-route distances.
    """
    if len(coords) < 2 or total_m <= 0:
        return coords
    try:
        line = LineString(coords)
        start_frac = max(0.0, start_m / total_m)
        end_frac = min(1.0, end_m / total_m)
        from shapely.ops import substring as _substring
        sub = _substring(line, start_frac, end_frac, normalized=True)
        if sub is None or sub.is_empty:
            return coords
        clipped = list(sub.coords)
        if len(clipped) < 2:
            return coords
        return [(float(c[0]), float(c[1])) for c in clipped]
    except Exception:
        return coords


def _project_onto_route(
    lon: float,
    lat: float,
    route_line: LineString,
    route_total_deg: float,
    total_m: float,
) -> float:
    """Project a geographic point onto the route LineString and return meters along it.

    Uses Shapely's `project` (which works in coordinate degrees), then scales
    to meters using the known physical length.
    """
    if route_total_deg <= 0 or total_m <= 0:
        return 0.0
    proj_deg = float(route_line.project(Point(lon, lat)))
    return (proj_deg / route_total_deg) * total_m


def find_route_intersections(
    *,
    route_coords_lonlat: List[Tuple[float, float]],
    total_m: float,
    blocked_start_m: Optional[float] = None,
    blocked_end_m: Optional[float] = None,
    window_m: float = 5000.0,
) -> List[IntersectionPoint]:
    """Return OSM nodes along the route that are drivable junctions.

    Calls Valhalla ``/trace_attributes`` on a clipped slice of the route shape
    (±window_m around the blockage) and filters to nodes where at least one
    drivable, non-service cross-street is present.

    Returns an empty list when ``/trace_attributes`` is unavailable, the
    circuit breaker is open, or no qualifying junctions exist.
    """
    if len(route_coords_lonlat) < 2:
        return []

    # Build the full original-route LineString once.  All shape_dist_m values
    # are derived by projecting onto this line, so they are independent of how
    # Valhalla samples its matched shape (which can be 5–10x denser).
    total_m_resolved = total_m if total_m > 0 else _line_length_m(route_coords_lonlat)
    try:
        route_line = LineString(route_coords_lonlat)
        route_total_deg = float(route_line.length)
    except Exception:
        return []
    if route_total_deg <= 0 or total_m_resolved <= 0:
        return []

    # Clip coords sent to /trace_attributes to ±window_m around the blockage
    # for performance (long routes → large payloads).  The clipping is purely
    # an optimisation; distances are still computed from the original route_line.
    if blocked_start_m is not None and blocked_end_m is not None and total_m_resolved > 0:
        clip_start = max(0.0, blocked_start_m - window_m)
        clip_end = min(total_m_resolved, blocked_end_m + window_m)
        trace_coords = _clip_coords_to_window(
            route_coords_lonlat, total_m_resolved, clip_start, clip_end
        )
    else:
        trace_coords = list(route_coords_lonlat)

    if len(trace_coords) < 2:
        return []

    try:
        from backend.adapters.osm_detour import match_route_attributes_detailed
    except ImportError:
        return []

    detail = match_route_attributes_detailed(trace_coords, costing="bus", timeout_s=12.0)
    if not detail or not isinstance(detail.get("edges"), list):
        return []

    edges: List[Dict[str, Any]] = detail["edges"]
    shape_lonlat: Optional[List[Tuple[float, float]]] = detail.get("shape_lonlat")

    results: List[IntersectionPoint] = []

    for edge in edges:
        if not isinstance(edge, dict):
            continue

        # Determine the geographic position of this edge's end-node.
        # Prefer the matched shape point (more accurate snap) when available.
        end_shape_idx = edge.get("end_shape_index")
        if end_shape_idx is not None:
            end_shape_idx = int(end_shape_idx)
            if shape_lonlat and end_shape_idx < len(shape_lonlat):
                node_lon, node_lat = float(shape_lonlat[end_shape_idx][0]), float(shape_lonlat[end_shape_idx][1])
            elif end_shape_idx < len(trace_coords):
                node_lon, node_lat = trace_coords[end_shape_idx]
            else:
                continue
        else:
            continue

        # Compute shape_dist_m by projecting the node's (lon, lat) onto the
        # ORIGINAL full route line — not using Valhalla's shape index which
        # is relative to the matched (denser) shape, not the input coords.
        original_dist_m = _project_onto_route(
            node_lon, node_lat, route_line, route_total_deg, total_m_resolved
        )

        end_node = edge.get("end_node")
        if not isinstance(end_node, dict):
            # No intersection data available — skip.
            continue

        inter_edges = end_node.get("intersecting_edges") or []
        if not isinstance(inter_edges, list):
            continue

        # Check for at least one drivable non-service cross-street.
        drivable_cross: List[str] = []
        for ie in inter_edges:
            if not isinstance(ie, dict):
                continue
            driveability = str(ie.get("driveability") or "").lower()
            if driveability not in _DRIVABLE:
                continue
            rc = str(ie.get("road_class") or "").lower()
            if rc in _NON_DRIVABLE_CLASSES:
                continue
            # Filter out the through-road itself (same name consistency on both
            # sides means it's just a continuation, not a real turn).
            to_consistent = ie.get("to_edge_name_consistency", False)
            from_consistent = ie.get("from_edge_name_consistency", False)
            if to_consistent and from_consistent:
                continue
            drivable_cross.append(rc or "unknown")

        if not drivable_cross:
            continue

        route_road_class = str(edge.get("road_class") or "").lower()
        osm_node_id = int(edge.get("end_osm_node_id") or 0)

        results.append(
            IntersectionPoint(
                shape_dist_m=original_dist_m,
                lon=node_lon,
                lat=node_lat,
                osm_node_id=osm_node_id,
                road_class=route_road_class,
                cross_road_classes=tuple(drivable_cross),
                cross_count=len(drivable_cross),
            )
        )

    return results
