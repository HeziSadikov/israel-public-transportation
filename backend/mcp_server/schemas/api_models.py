from __future__ import annotations

from typing import Optional, List, Dict, Any, Literal

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


class AreaRoutesQuery(BaseModel):
    start_date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
    end_date: str  # YYYYMMDD
    end_time: str  # HH:MM or HH:MM:SS
    polygon_geojson: Dict[str, Any]
    max_results: int = Field(default=100, ge=1)
    time_semantics_mode: Literal[
        "legacy_trip_overlap",
        "pass_through_precise",
        "pass_through_stop_proxy",
    ] = "legacy_trip_overlap"


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
    time_match_confidence: Optional[Literal["high", "approx", "unknown"]] = None
    time_match_note: Optional[str] = None


class AreaRoutesResponse(BaseModel):
    routes: List[AreaRouteResult]
    calendar_hint: Optional[str] = None


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
    detour_debug: bool = Field(
        default=False,
        description=(
            "When true: include debug GeoJSON in the response and emit one structured detours/v2/compute_ai log line per trip "
            "(same as debug_detour plus log_ai_summary). Prefer this over the legacy flags. "
            "For AI logs on every compute without GeoJSON, start the API with DETOUR_V2_DEBUG=1 "
            "(bash/zsh: DETOUR_V2_DEBUG=1 python -m run_uvicorn; PowerShell: $env:DETOUR_V2_DEBUG='1'; python -m run_uvicorn) "
            "or python -m run_uvicorn --detour-debug."
        ),
    )
    # Legacy: GeoJSON only — prefer detour_debug.
    debug_detour: bool = False
    # Legacy: compute_ai log only — prefer detour_debug (or DETOUR_V2_DEBUG / --detour-debug for global log-only).
    log_ai_summary: bool = False
    # Prefer PostGIS matched physical geometry when backfilled (USE_MATCHED_PHYSICAL_GEOMETRY also gates this).
    use_matched_physical: bool = False

    @model_validator(mode="after")
    def trip_ids_or_route_id(self) -> "DetourComputeV2Request":
        has_trips = any(t and str(t).strip() for t in self.trip_ids)
        has_route = bool(self.route_id and str(self.route_id).strip())
        if has_trips or has_route:
            return self
        raise ValueError("Either non-empty trip_ids or route_id is required")


class DetourComputeV2Response(BaseModel):
    """Each results[] row matches detour_compute_output_to_dict: status, trip_id, candidates[], selected{}, stitching{}, discarded[], attempts[]."""

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
    candidate_rank: Optional[int] = Field(
        default=None,
        description="1-based rank matching persisted detour_candidates.candidate_rank (top alternatives are 1..3).",
    )


class DetourApproveV2Response(BaseModel):
    approved_detour_id: int


class DetourV2DetailResponse(BaseModel):
    request: Dict[str, Any]
    candidates: List[Dict[str, Any]]

