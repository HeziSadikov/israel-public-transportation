from __future__ import annotations

from typing import Optional, List, Dict, Any
from enum import Enum

from pydantic import BaseModel, Field


class RouteSearchRequest(BaseModel):
    q: str
    limit: int = 20


class RouteInfo(BaseModel):
    route_id: str
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    agency_id: Optional[str] = None
    agency_name: Optional[str] = None
    route_type: Optional[int] = None


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


class AreaRoutesQuery(BaseModel):
    date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
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


class AreaRoutesResponse(BaseModel):
    routes: List[AreaRouteResult]


class DetourByAreaMode(str, Enum):
    route = "route"
    all = "all"


class DetourByAreaRequest(BaseModel):
    mode: DetourByAreaMode
    route_id: Optional[str] = None  # required when mode=route
    direction_id: Optional[str] = None
    date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
    end_time: str  # HH:MM or HH:MM:SS
    blockage_geojson: Dict[str, Any]
    max_routes: int = Field(default=20, ge=1)
    transfer_radius_m: float = Field(default=200.0, ge=10.0, le=1000.0)


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


class StopRoutesRequest(BaseModel):
    stop_id: str
    date: str  # YYYYMMDD
    start_time: str  # HH:MM or HH:MM:SS
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

