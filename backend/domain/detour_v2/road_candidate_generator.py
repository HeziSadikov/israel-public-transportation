"""Generate multiple road-level detour candidates via Valhalla."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.adapters.osm_detour import route_avoiding_polygon_alternates_debug

from .models import RoadCandidate


def generate_candidates(
    exit_lon: float,
    exit_lat: float,
    rejoin_lon: float,
    rejoin_lat: float,
    blockage_geojson: Dict[str, Any],
    *,
    alternate_count: int = 2,
) -> List[RoadCandidate]:
    candidates, _debug = generate_candidates_with_debug(
        exit_lon=exit_lon,
        exit_lat=exit_lat,
        rejoin_lon=rejoin_lon,
        rejoin_lat=rejoin_lat,
        blockage_geojson=blockage_geojson,
        alternate_count=alternate_count,
    )
    return candidates


def generate_candidates_with_debug(
    exit_lon: float,
    exit_lat: float,
    rejoin_lon: float,
    rejoin_lat: float,
    blockage_geojson: Dict[str, Any],
    *,
    alternate_count: int = 2,
    exit_heading_deg: Optional[float] = None,
    rejoin_heading_deg: Optional[float] = None,
) -> Tuple[List[RoadCandidate], Dict[str, Any]]:
    """
    Produce named strategy candidates with compact no-candidate diagnostics.
    """
    raw, adapter_debug = route_avoiding_polygon_alternates_debug(
        exit_lon,
        exit_lat,
        rejoin_lon,
        rejoin_lat,
        blockage_geojson,
        costing="bus",
        alternate_count=alternate_count,
        exit_heading_deg=exit_heading_deg,
        rejoin_heading_deg=rejoin_heading_deg,
    )
    out: List[RoadCandidate] = []
    filtered_success_false = 0
    filtered_short_coords = 0
    for i, r in enumerate(raw):
        if not r.success:
            filtered_success_false += 1
            continue
        if len(r.coordinates) < 2:
            filtered_short_coords += 1
            continue
        name = "baseline_bus" if i == 0 else f"alternate_{i}"
        out.append(
            RoadCandidate(
                strategy=name,
                osm_result_coordinates=list(r.coordinates),
                distance_m=r.distance_m,
                time_s=r.time_s,
                success=True,
                turn_by_turn=r.turn_by_turn,
            )
        )
    reason = ""
    if not out:
        if adapter_debug.get("error_type") == "valhalla_url_missing":
            reason = "valhalla_url_missing"
        elif adapter_debug.get("error_type"):
            reason = "valhalla_http_error"
        elif (adapter_debug.get("primary_legs_count") or 0) == 0 and (adapter_debug.get("alternates_count") or 0) == 0:
            reason = "no_legs_in_response"
        elif filtered_short_coords > 0:
            reason = "all_candidates_filtered_short_geometry"
        elif filtered_success_false > 0:
            reason = "all_candidates_failed"
        else:
            reason = "unknown_no_candidates"
    else:
        reason = "valhalla_routes_returned"
    # Second pass: bias toward bus corridors is a no-op without map-matched penalties here;
    # duplicate as strategy variant for ranking diversity (feasibility differs only if we had OSM tags).
    if len(out) == 1:
        out.append(
            RoadCandidate(
                strategy="duplicate_for_scoring",
                osm_result_coordinates=list(out[0].osm_result_coordinates),
                distance_m=out[0].distance_m,
                time_s=out[0].time_s,
                success=True,
                turn_by_turn=out[0].turn_by_turn,
            )
        )
    debug: Dict[str, Any] = dict(adapter_debug)
    debug["filtered_success_false"] = filtered_success_false
    debug["filtered_short_coords"] = filtered_short_coords
    debug["raw_count"] = len(raw)
    debug["candidate_generation_reason"] = reason
    return out, debug
