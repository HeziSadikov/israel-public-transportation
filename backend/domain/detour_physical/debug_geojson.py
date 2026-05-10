"""Optional GeoJSON FeatureCollection for detour debugging (QGIS / MapLibre)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def coords_from_geojson_linestring(geometry_geojson: Optional[Dict[str, Any]]) -> Optional[List[Tuple[float, float]]]:
    """Extract (lon, lat) pairs from a GeoJSON Feature, LineString, or first LineString in a FeatureCollection."""
    if not geometry_geojson:
        return None
    if geometry_geojson.get("type") == "FeatureCollection":
        for feat in geometry_geojson.get("features") or []:
            if not isinstance(feat, dict):
                continue
            g = feat.get("geometry") if feat.get("type") == "Feature" else feat
            if isinstance(g, dict) and g.get("type") == "LineString":
                coords = g.get("coordinates") or []
                out: List[Tuple[float, float]] = []
                for c in coords:
                    if len(c) >= 2:
                        out.append((float(c[0]), float(c[1])))
                if len(out) >= 2:
                    return out
        return None
    g = geometry_geojson.get("geometry") if geometry_geojson.get("type") == "Feature" else geometry_geojson
    if not isinstance(g, dict) or g.get("type") != "LineString":
        return None
    coords = g.get("coordinates") or []
    out: List[Tuple[float, float]] = []
    for c in coords:
        if len(c) >= 2:
            out.append((float(c[0]), float(c[1])))
    return out if len(out) >= 2 else None


def _line_feature(
    coords_lonlat: List[tuple[float, float]],
    name: str,
    props: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if len(coords_lonlat) < 2:
        return None
    return {
        "type": "Feature",
        "properties": {"layer": name, **(props or {})},
        "geometry": {"type": "LineString", "coordinates": [[x, y] for x, y in coords_lonlat]},
    }


def _point_feature(lon: float, lat: float, name: str, props: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {"layer": name, **(props or {})},
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
    }


def build_detour_debug_feature_collection(
    *,
    gtfs_shape_coords_lonlat: Optional[List[tuple[float, float]]] = None,
    matched_physical_coords_lonlat: Optional[List[tuple[float, float]]] = None,
    blocked_span_on_matched_coords_lonlat: Optional[List[tuple[float, float]]] = None,
    raw_valhalla_coords_lonlat: Optional[List[tuple[float, float]]] = None,
    decoded_detour_coords_lonlat: Optional[List[tuple[float, float]]] = None,
    exit_lon: Optional[float] = None,
    exit_lat: Optional[float] = None,
    rejoin_lon: Optional[float] = None,
    rejoin_lat: Optional[float] = None,
    blockage_geojson: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a single GeoJSON FeatureCollection with labeled layers.
    Coordinates are (lon, lat) throughout.
    """
    features: List[Dict[str, Any]] = []
    if gtfs_shape_coords_lonlat and len(gtfs_shape_coords_lonlat) >= 2:
        f = _line_feature(gtfs_shape_coords_lonlat, "gtfs_shape", {"source": "gtfs"})
        if f:
            features.append(f)
    if matched_physical_coords_lonlat and len(matched_physical_coords_lonlat) >= 2:
        f = _line_feature(matched_physical_coords_lonlat, "matched_physical", {"source": "pattern_edge_match"})
        if f:
            features.append(f)
    if blocked_span_on_matched_coords_lonlat and len(blocked_span_on_matched_coords_lonlat) >= 2:
        f = _line_feature(
            blocked_span_on_matched_coords_lonlat,
            "blocked_span_matched",
            {"source": "pattern_edge_match", "role": "blocked_subline"},
        )
        if f:
            features.append(f)
    if raw_valhalla_coords_lonlat and len(raw_valhalla_coords_lonlat) >= 2:
        f = _line_feature(raw_valhalla_coords_lonlat, "raw_valhalla_detour", {"source": "valhalla_route"})
        if f:
            features.append(f)
    if decoded_detour_coords_lonlat and len(decoded_detour_coords_lonlat) >= 2:
        f = _line_feature(decoded_detour_coords_lonlat, "decoded_detour", {"source": "trace_attributes"})
        if f:
            features.append(f)
    if exit_lon is not None and exit_lat is not None:
        features.append(_point_feature(exit_lon, exit_lat, "anchor_exit", {}))
    if rejoin_lon is not None and rejoin_lat is not None:
        features.append(_point_feature(rejoin_lon, rejoin_lat, "anchor_rejoin", {}))
    if blockage_geojson and isinstance(blockage_geojson, dict) and blockage_geojson.get("type"):
        features.append({"type": "Feature", "properties": {"layer": "blockage"}, "geometry": blockage_geojson})
    out: Dict[str, Any] = {"type": "FeatureCollection", "features": features}
    if extra:
        out["metadata"] = extra
    return out
