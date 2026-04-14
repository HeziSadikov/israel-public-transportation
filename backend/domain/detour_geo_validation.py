"""Validate detour road geometry against blockage polygons (street overrides)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString, shape


def geom_violates_blockage(route_geom, blocked_geom) -> bool:
    """True when route geometry meaningfully intersects blockage interior."""
    try:
        if route_geom.crosses(blocked_geom):
            return True
    except Exception:
        pass
    try:
        if route_geom.within(blocked_geom) or blocked_geom.contains(route_geom):
            return True
    except Exception:
        pass
    try:
        inter = route_geom.intersection(blocked_geom)
        if not inter.is_empty:
            if float(getattr(inter, "length", 0.0) or 0.0) > 0.0:
                return True
            if float(getattr(inter, "area", 0.0) or 0.0) > 0.0:
                return True
    except Exception:
        pass
    return False


def _as_shapely(geojson: Dict[str, Any]):
    g = shape(geojson)
    if g.is_empty:
        return None
    return g


def extract_line_coords_from_road_geojson(road: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    Normalize detour_road_geojson to a list of (lon, lat) vertices.
    Accepts Geometry, Feature, or FeatureCollection; LineString or MultiLineString.
    """
    if not isinstance(road, dict):
        return []
    t = road.get("type")
    coords: List[Tuple[float, float]] = []
    if t == "LineString":
        c = road.get("coordinates") or []
        for pt in c:
            if len(pt) >= 2:
                coords.append((float(pt[0]), float(pt[1])))
        return coords
    if t == "MultiLineString":
        for line in road.get("coordinates") or []:
            for pt in line:
                if len(pt) >= 2:
                    coords.append((float(pt[0]), float(pt[1])))
        return coords
    if t == "Feature":
        geom = road.get("geometry")
        if isinstance(geom, dict):
            return extract_line_coords_from_road_geojson(geom)
        return []
    if t == "FeatureCollection":
        for feat in road.get("features") or []:
            if isinstance(feat, dict):
                coords.extend(extract_line_coords_from_road_geojson(feat))
        return []
    return []


def road_linestring_from_geojson(road: Dict[str, Any]) -> LineString:
    """Single merged LineString for validation (concatenates MultiLineString parts)."""
    coords = extract_line_coords_from_road_geojson(road)
    if len(coords) < 2:
        return LineString()
    return LineString(coords)


def road_geojson_has_routable_geometry(road: Optional[Dict[str, Any]]) -> bool:
    """True when road GeoJSON has at least two distinct vertices ( drawable / valid path)."""
    if not road or not isinstance(road, dict):
        return False
    return len(extract_line_coords_from_road_geojson(road)) >= 2


def road_geojson_clear_of_blockage(
    road_geojson: Dict[str, Any],
    blockage_geojson: Dict[str, Any],
) -> bool:
    """False if road is empty, blockage empty/invalid, or road penetrates blockage."""
    line = road_linestring_from_geojson(road_geojson)
    if line.is_empty or len(line.coords) < 2:
        return False
    try:
        blocked = _as_shapely(blockage_geojson)
    except Exception:
        return False
    if blocked is None or blocked.is_empty:
        return False
    return not geom_violates_blockage(line, blocked)


def road_geojson_to_path_feature_collection(road_geojson: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap road geometry as path_geojson FeatureCollection for map clients."""
    line = road_linestring_from_geojson(road_geojson)
    if line.is_empty:
        return {"type": "FeatureCollection", "features": []}
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": list(line.coords)},
                "properties": {"kind": "street_override"},
            }
        ],
    }


def approximate_line_length_m(coords: List[Tuple[float, float]]) -> float:
    """Haversine length along polyline in meters."""
    if len(coords) < 2:
        return 0.0
    from math import radians, cos, sin, asin, sqrt

    def seg(a: Tuple[float, float], b: Tuple[float, float]) -> float:
        lon1, lat1 = radians(a[0]), radians(a[1])
        lon2, lat2 = radians(b[0]), radians(b[1])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        h = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(min(1.0, sqrt(h)))
        return 6371000.0 * c

    total = 0.0
    for i in range(len(coords) - 1):
        total += seg(coords[i], coords[i + 1])
    return total


def geometry_inflation_within_thresholds(
    *,
    baseline_distance_m: Optional[float],
    candidate_distance_m: Optional[float],
    max_ratio: float,
) -> bool:
    if candidate_distance_m is None or candidate_distance_m <= 0:
        return False
    if baseline_distance_m is None or baseline_distance_m <= 0:
        return True
    return (candidate_distance_m / baseline_distance_m) <= max_ratio
