from __future__ import annotations

from typing import Optional, List, Dict, Any, Literal
from enum import Enum

from pydantic import BaseModel, Field, model_validator


class DetourTurnStep(BaseModel):
    """Street-oriented turn; at least one text or structured field should be set."""

    instruction_he: Optional[str] = None
    instruction_en: Optional[str] = None
    street: Optional[str] = None
    toward_street: Optional[str] = None
    intersection_with: Optional[str] = None
    turn: Optional[str] = None  # e.g. left, right, straight, uturn
    distance_m: Optional[float] = None


class RouteSearchRequest(BaseModel):
    q: str
    limit: int = 20
    start_date: Optional[str] = None  # YYYYMMDD
    start_time: Optional[str] = None  # HH:MM or HH:MM:SS
    end_date: Optional[str] = None  # YYYYMMDD
    end_time: Optional[str] = None  # HH:MM or HH:MM:SS


class RouteInfo(BaseModel):
    route_id: str
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_name: Optional[str] = None
    route_type: Optional[int] = None
    trip_count: Optional[int] = None
    last_stop_name: Optional[str] = None


class FeedUpdateResponse(BaseModel):
    updated: bool
    online_ok: bool
    message: str
    active: Optional[Dict[str, Any]] = None


class FeedStatusResponse(BaseModel):
    active: Optional[Dict[str, Any]] = None
    history_len: int
    last_update_attempt: Optional[str] = None
    last_update_ok: Optional[bool] = None
    # MIN/MAX of calendar.start_date / end_date for the active PostGIS feed (None if no calendar rows).
    calendar_min_ymd: Optional[int] = None
    calendar_max_ymd: Optional[int] = None
    # When UTC “today” falls outside [calendar_min_ymd, calendar_max_ymd], hints to refresh GTFS or pick in-range dates.
    calendar_coverage_note: Optional[str] = None


class GraphBuildRequest(BaseModel):
    route_id: str
    direction_id: Optional[str] = None
    date: Optional[str] = None  # YYYYMMDD
    max_trips: Optional[int] = Field(default=50, ge=1)
    pretty_osm: bool = False


class GraphBuildResponse(BaseModel):
    pattern_id: str
    stop_count: int
    edge_count: int
    used_shape: bool
    used_osm_snapping: bool
    example_stop_ids: List[str]
    feed_version: str


class GraphStopsResponseStop(BaseModel):
    stop_id: str
    name: str
    lat: float
    lon: float
    sequence: int


class GraphStopsResponse(BaseModel):
    pattern_id: str
    stops: List[GraphStopsResponseStop]


class DetourRequest(BaseModel):
    route_id: str
    direction_id: Optional[str] = None
    pattern_id: Optional[str] = None
    date: Optional[str] = None
    start_stop_id: str
    end_stop_id: str
    blockage_geojson: Dict[str, Any]
    # Optional service window for candidate-route selection (HH:MM or HH:MM:SS).
    # Omit both for full extended service day (same as historical /detour behavior).
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    # Manual street detour (after auto failure or to replace); LineString/MultiLineString geometry.
    detour_road_geojson: Optional[Dict[str, Any]] = None
    turn_by_turn: Optional[List[DetourTurnStep]] = None
    # Single narrative; split server-side into steps if turn_by_turn omitted.
    instructions_text_he: Optional[str] = None
    remember_override: bool = False
    # GTFS graph routing: A* (default) or DFS with optional fallback to A*.
    routing_engine: Literal["astar", "dfs", "dijkstra"] = "astar"


class DetourResponse(BaseModel):
    blocked_edges_count: int
    stop_path: List[str]
    path_geojson: Dict[str, Any]
    blocked_edges_geojson: Dict[str, Any]
    total_travel_time_s: Optional[float] = None
    total_distance_m: Optional[float] = None
    baseline_travel_time_s: Optional[float] = None
    baseline_distance_m: Optional[float] = None
    detour_delay_s: Optional[float] = None
    detour_extra_distance_m: Optional[float] = None
    used_shape: bool
    used_osm_snapping: bool
    feed_version: str
    turn_by_turn: Optional[List[DetourTurnStep]] = None
    from_override: bool = False
    instructions_only: bool = False
    reason_code: Optional[str] = None
    strategy_used: Optional[str] = None
    confidence: Optional[float] = None
    diagnostics: Optional[Dict[str, Any]] = None


class AreaRoutesQuery(BaseModel):
    start_date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
    end_date: str  # YYYYMMDD
    end_time: str  # HH:MM or HH:MM:SS
    polygon_geojson: Dict[str, Any]
    max_results: int = Field(default=100, ge=1)


class AreaRouteResult(BaseModel):
    route_id: str
    direction_id: Optional[str] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_name: Optional[str] = None
    first_time: Optional[str] = None
    last_time: Optional[str] = None
    trip_count: Optional[int] = None
    last_stop_name: Optional[str] = None


class AreaRoutesResponse(BaseModel):
    routes: List[AreaRouteResult]
    calendar_hint: Optional[str] = None


class DetourByAreaMode(str, Enum):
    route = "route"
    all = "all"


class DetourByAreaRequest(BaseModel):
    mode: DetourByAreaMode
    route_id: Optional[str] = None  # required when mode=route
    direction_id: Optional[str] = None
    start_date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
    end_date: str  # YYYYMMDD
    end_time: str  # HH:MM or HH:MM:SS
    blockage_geojson: Dict[str, Any]
    max_routes: int = Field(default=20, ge=1)
    transfer_radius_m: float = Field(default=280.0, ge=10.0, le=1000.0)
    use_osm_detour: bool = False
    # When True, run Valhalla hybrid before GTFS multi-route A* (legacy order).
    prefer_osm_detour: bool = False
    detour_road_geojson: Optional[Dict[str, Any]] = None
    turn_by_turn: Optional[List[DetourTurnStep]] = None
    remember_override: bool = False
    # When applying a manual override for a specific route, pass entry/exit from last failed result.
    stop_before: Optional[str] = None
    stop_after: Optional[str] = None
    instructions_text_he: Optional[str] = None
    routing_engine: Literal["astar", "dfs", "dijkstra"] = "astar"


class DetourByAreaRouteResult(BaseModel):
    route_id: str
    direction_id: Optional[str] = None
    pattern_id: Optional[str] = None
    blocked_edges_count: int
    stop_before: Optional[str] = None
    stop_after: Optional[str] = None
    detour_stop_path: List[str] = []
    detour_geojson: Optional[Dict[str, Any]] = None
    replaced_segment_geojson: Optional[Dict[str, Any]] = None
    used_transfers: bool = False
    error: Optional[str] = None
    turn_by_turn: Optional[List[DetourTurnStep]] = None
    from_override: bool = False
    instructions_only: bool = False
    reason_code: Optional[str] = None
    strategy_used: Optional[str] = None
    confidence: Optional[float] = None
    diagnostics: Optional[Dict[str, Any]] = None


class DetourByAreaResponse(BaseModel):
    mode: DetourByAreaMode
    result: Optional[DetourByAreaRouteResult] = None
    results: Optional[List[DetourByAreaRouteResult]] = None
    feed_version: Optional[str] = None


class StopInBounds(BaseModel):
    stop_id: str
    stop_name: Optional[str] = None
    stop_code: Optional[str] = None
    stop_lat: float
    stop_lon: float


class StopSearchResult(BaseModel):
    stop_id: str
    stop_name: Optional[str] = None
    stop_code: Optional[str] = None
    stop_lat: float
    stop_lon: float


class StopRoutesRequest(BaseModel):
    stop_id: str
    start_date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
    end_date: str  # YYYYMMDD
    end_time: str  # HH:MM or HH:MM:SS
    max_results: int = Field(default=100, ge=1, le=500)


class StopRouteResult(BaseModel):
    route_id: str
    direction_id: Optional[str] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_name: Optional[str] = None
    first_time: Optional[str] = None
    last_time: Optional[str] = None


class StopRoutesResponse(BaseModel):
    stop_id: str
    routes: List[StopRouteResult]


class GeocodeResult(BaseModel):
    display_name: str
    lat: float
    lon: float
    place_id: Optional[int] = None


# --- Detour v2 -----------------------------------------------------------------


class IncidentCreateRequest(BaseModel):
    polygon_geojson: Dict[str, Any]
    incident_type: Optional[str] = None
    description: Optional[str] = None
    start_date: str
    start_time: str
    end_date: str
    end_time: str
    created_by: Optional[str] = None


class IncidentCreateResponse(BaseModel):
    incident_id: int
    affected_route_count: int
    derived_edge_ban_count: int
    policy_version: str


class DetourComputeV2Request(BaseModel):
    service_date: str
    trip_ids: List[str] = Field(default_factory=list)
    blockage_geojson: Dict[str, Any]
    incident_id: Optional[int] = None
    persist: bool = True
    route_id: Optional[str] = None
    direction_id: Optional[str] = None

    @model_validator(mode="after")
    def trip_ids_or_route_id(self) -> "DetourComputeV2Request":
        has_trips = any(t and str(t).strip() for t in self.trip_ids)
        has_route = bool(self.route_id and str(self.route_id).strip())
        if has_trips or has_route:
            return self
        raise ValueError("Either non-empty trip_ids or route_id is required")


class DetourComputeV2Response(BaseModel):
    results: List[Dict[str, Any]]
    detour_request_ids: List[int] = Field(default_factory=list)
    policy_version: str


class BusEdgeConstraintRequest(BaseModel):
    osm_way_id: int
    direction: Optional[str] = None
    constraint_type: str = "ban"
    severity: float = 1.0
    reason_code: Optional[str] = None
    notes: Optional[str] = None
    created_by: Optional[str] = None


class BusTurnConstraintRequest(BaseModel):
    from_way_id: int
    via_node_id: int
    to_way_id: int
    constraint_type: str = "ban"
    severity: float = 1.0
    reason_code: Optional[str] = None
    notes: Optional[str] = None
    created_by: Optional[str] = None


class ConstraintCreateResponse(BaseModel):
    id: int


class DetourApproveV2Request(BaseModel):
    approved_by: Optional[str] = None


class DetourApproveV2Response(BaseModel):
    approved_detour_id: int


class DetourV2DetailResponse(BaseModel):
    request: Dict[str, Any]
    candidates: List[Dict[str, Any]]

