"""Build API-shaped detour results from stored or manual street geometry."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .api_models import DetourResponse, DetourTurnStep
from .detour_geo_validation import (
    approximate_line_length_m,
    extract_line_coords_from_road_geojson,
    road_geojson_to_path_feature_collection,
)

EMPTY_FC: Dict[str, Any] = {"type": "FeatureCollection", "features": []}


def _steps_from_json(raw: Optional[List[Any]]) -> List[DetourTurnStep]:
    out: List[DetourTurnStep] = []
    for item in raw or []:
        if isinstance(item, DetourTurnStep):
            out.append(item)
            continue
        if isinstance(item, dict):
            try:
                out.append(DetourTurnStep.model_validate(item))
            except Exception:
                continue
    return out


def build_detour_response_from_road_override(
    road_geojson: Dict[str, Any],
    turn_by_turn: Optional[List[Any]],
    *,
    blocked_edges_count: int,
    blocked_edges_geojson: Dict[str, Any],
    stop_path: List[str],
    baseline_travel_time_s: Optional[float],
    baseline_distance_m: Optional[float],
    used_shape: bool,
    used_osm_snapping: bool,
    feed_version: str,
    from_override: bool,
) -> DetourResponse:
    path_geojson = road_geojson_to_path_feature_collection(road_geojson)
    coords = extract_line_coords_from_road_geojson(road_geojson)
    total_distance_m = approximate_line_length_m(coords)
    total_travel_time_s = total_distance_m / 5.0 if total_distance_m > 0 else None
    detour_delay_s = None
    detour_extra_distance_m = None
    if baseline_travel_time_s is not None and baseline_distance_m is not None and total_travel_time_s is not None:
        detour_delay_s = total_travel_time_s - baseline_travel_time_s
        detour_extra_distance_m = total_distance_m - baseline_distance_m
    steps = _steps_from_json(turn_by_turn)
    return DetourResponse(
        blocked_edges_count=blocked_edges_count,
        stop_path=stop_path,
        path_geojson=path_geojson,
        blocked_edges_geojson=blocked_edges_geojson,
        total_travel_time_s=total_travel_time_s,
        total_distance_m=total_distance_m,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=detour_delay_s,
        detour_extra_distance_m=detour_extra_distance_m,
        used_shape=used_shape,
        used_osm_snapping=used_osm_snapping,
        feed_version=feed_version,
        turn_by_turn=steps or None,
        from_override=from_override,
        instructions_only=False,
    )


def build_detour_response_instructions_only(
    turn_by_turn: Optional[List[Any]],
    *,
    blocked_edges_count: int,
    blocked_edges_geojson: Dict[str, Any],
    stop_path: List[str],
    baseline_travel_time_s: Optional[float],
    baseline_distance_m: Optional[float],
    used_shape: bool,
    used_osm_snapping: bool,
    feed_version: str,
    from_override: bool,
) -> DetourResponse:
    steps = _steps_from_json(turn_by_turn)
    return DetourResponse(
        blocked_edges_count=blocked_edges_count,
        stop_path=stop_path,
        path_geojson=EMPTY_FC,
        blocked_edges_geojson=blocked_edges_geojson,
        total_travel_time_s=None,
        total_distance_m=None,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=None,
        detour_extra_distance_m=None,
        used_shape=used_shape,
        used_osm_snapping=used_osm_snapping,
        feed_version=feed_version,
        turn_by_turn=steps or None,
        from_override=from_override,
        instructions_only=True,
    )


def turn_steps_to_jsonable(steps: Optional[List[Any]]) -> List[Dict[str, Any]]:
    """Serialize pydantic models or dicts for DB storage."""
    out: List[Dict[str, Any]] = []
    for s in steps or []:
        if isinstance(s, DetourTurnStep):
            out.append(s.model_dump(exclude_none=True))
        elif isinstance(s, dict):
            out.append(dict(s))
    return out
