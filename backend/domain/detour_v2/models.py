"""Typed contracts for detour v2."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


@dataclass
class BlockedShapeInterval:
    """Blocked portion of trip shape along cumulative distance (meters)."""

    blocked_start_m: float
    blocked_end_m: float
    shape_length_m: float


@dataclass
class TripImpactResult:
    trip_id: str
    route_id: str
    shape_id: Optional[str]
    blocked: Optional[BlockedShapeInterval]
    intersects_blockage: bool


@dataclass
class AnchorPair:
    exit_lon: float
    exit_lat: float
    rejoin_lon: float
    rejoin_lat: float
    exit_stop_id: Optional[str] = None
    rejoin_stop_id: Optional[str] = None
    exit_shape_dist_m: float = 0.0
    rejoin_shape_dist_m: float = 0.0
    anchor_quality_note: Optional[str] = None
    # Forward bearings along GTFS shape (or matched physical path) at exit/rejoin — used for Valhalla heading snap.
    exit_forward_bearing_deg: Optional[float] = None
    rejoin_forward_bearing_deg: Optional[float] = None
    anchor_geometry_source: Optional[str] = None  # "gtfs_shape" | "matched_physical"
    anchor_source: Optional[str] = None  # "legal_index" | "stop_window" | None
    exit_osm_segment_id: Optional[int] = None
    rejoin_osm_segment_id: Optional[int] = None


@dataclass
class IncidentEdgeBan:
    osm_way_id: int
    direction: Optional[str] = None  # forward/backward or None if bidirectional ban


@dataclass
class IncidentTurnBan:
    from_way_id: int
    via_node_id: int
    to_way_id: int


@dataclass
class IncidentProjection:
    edge_bans: List[IncidentEdgeBan] = field(default_factory=list)
    turn_bans: List[IncidentTurnBan] = field(default_factory=list)
    segments_intersecting_polygon: int = 0


@dataclass
class RoadSegmentRef:
    segment_id: int
    osm_way_id: int
    from_node_id: int
    to_node_id: int
    sequence_index: int
    length_m: float
    travel_time_s: float
    highway: Optional[str] = None
    access: Optional[str] = None
    bus: Optional[str] = None
    psv: Optional[str] = None
    service: Optional[str] = None
    synthetic: bool = False


@dataclass
class TurnRef:
    from_segment_id: int
    to_segment_id: int
    via_node_id: int
    sequence_index: int
    turn_angle: Optional[float] = None
    turn_type: Optional[str] = None


@dataclass
class DecodedCandidate:
    road_segments: List[RoadSegmentRef]
    turns: List[TurnRef]
    geometry_geojson: Dict[str, Any]


@dataclass
class RoadCandidate:
    strategy: str
    osm_result_coordinates: List[tuple[float, float]]
    distance_m: float
    time_s: float
    success: bool
    turn_by_turn: Optional[List[Dict[str, Any]]] = None


@dataclass
class FeasibilityResult:
    accepted: bool
    hard_reject_reasons: List[str] = field(default_factory=list)
    segment_penalty_s: float = 0.0
    turn_penalty_s: float = 0.0
    uncertainty_penalty_s: float = 0.0
    service_penalty_s: float = 0.0
    evidence_bonus_s: float = 0.0
    notes: List[str] = field(default_factory=list)
    sharp_turn_count: int = 0


@dataclass
class RankedCandidate:
    strategy: str
    total_score: float
    travel_time_s: float
    distance_m: float
    decoded: Optional[DecodedCandidate] = None
    feasibility: Optional[FeasibilityResult] = None
    rejection_reasons: List[str] = field(default_factory=list)
    score_breakdown: Dict[str, float] = field(default_factory=dict)


@dataclass
class StitchingResult:
    exit_stop_index: int
    rejoin_stop_index: int
    served_stop_ids: List[str]
    skipped_stop_ids: List[str]
    stitch_ok: bool
    skipped_reasons: Dict[str, str] = field(default_factory=dict)
    stitch_notes: List[str] = field(default_factory=list)


@dataclass
class DetourComputeOutput:
    status: Literal["ok", "no_impact", "no_safe_detour", "error"]
    trip_id: str
    route_id: str
    anchors: Optional[AnchorPair] = None
    corridor_stage: Optional[str] = None
    candidates: List[RankedCandidate] = field(default_factory=list)
    selected: Optional[RankedCandidate] = None
    policy_version: str = ""
    error: Optional[str] = None
    stitching: Optional[StitchingResult] = None
    # Top-level attempt log (always present; contains per anchor/corridor routing outcome rows).
    attempts: List[Dict[str, Any]] = field(default_factory=list)
    debug: Optional[Dict[str, Any]] = None
