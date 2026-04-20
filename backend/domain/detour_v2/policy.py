"""Tunable detour policy for standard_plus_bus — all numeric thresholds are configurable."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from typing import Any, Optional


@dataclass
class AnchorPolicy:
    min_anchor_gap_m: float = 50.0
    search_before_window_m: float = 400.0
    search_after_window_m: float = 400.0
    anchor_shift_step_m: float = 200.0
    max_anchor_shift_m: float = 1500.0
    # Evaluate several exit/rejoin stop pairs and keep the best-scoring detour.
    candidate_pairs_k: int = 5
    # Per side cap before Cartesian pairing to avoid routing explosion.
    candidate_stops_per_side: int = 4
    # Anchor rescue: when first sweep finds nothing, expand per-side cap to this value.
    rescue_stops_per_side: int = 8
    # Validate anchor reachability via Valhalla /locate before routing (cuts 442 errors).
    min_locate_reachability_nodes: int = 50


@dataclass
class SnapPolicy:
    snap_radius_m: float = 50.0


@dataclass
class CorridorPolicy:
    narrow_buffer_m: float = 150.0
    medium_buffer_m: float = 300.0
    wide_buffer_m: float = 600.0
    major_road_extra_buffer_m: float = 300.0
    # Valhalla exclude_polygons: buffer (m) around the incident geometry only. Buffering the full
    # shape subsegment between anchors often severs the drivable graph (442); stage widens margin
    # around the closure without blocking the whole corridor.
    valhalla_exclude_buffer_narrow_m: float = 40.0
    valhalla_exclude_buffer_medium_m: float = 80.0
    valhalla_exclude_buffer_wide_m: float = 150.0
    # When clipping the incident to the exit–rejoin shape tube (meters half-width around subline).
    valhalla_exclude_clip_corridor_m: float = 420.0


@dataclass
class VehiclePolicy:
    width_threshold_m: float = 2.7
    # Thresholds for local-share hard rejection (only if reject_hard_local_share is True).
    local_fraction_hard_reject: float = 0.4
    unknown_width_local_fraction_hard_reject: float = 0.6
    # Off by default: incident detours often thread through service/residential; segment penalties
    # already apply. Set True to hard-reject when local share exceeds the thresholds above.
    reject_hard_local_share: bool = False
    # Off by default: detours intentionally use roads buses have not driven before.
    # GTFS evidence is used as a score bonus/penalty, not a hard gate.
    require_gtfs_way_evidence: bool = False
    min_gtfs_way_confidence: float = 0.15
    max_unknown_way_fraction: float = 0.15


@dataclass
class PenaltyPolicy:
    segment_local_per_km_s: float = 60.0
    segment_unclassified_per_km_s: float = 30.0
    segment_uncertainty_local_per_segment_s: float = 15.0
    segment_evidence_bonus_per_km_s: float = -30.0
    turn_sharp_s: float = 20.0
    turn_local_entry_s: float = 15.0
    turn_complex_s: float = 10.0
    anchor_quality_forced_local_s: float = 25.0
    # Penalty per sharp turn detected via /trace_attributes maneuver data.
    sharp_turn_per_occurrence_s: float = 25.0


@dataclass
class ServicePolicy:
    soft_extra_time_s: float = 300.0
    soft_extra_distance_m: float = 1500.0
    hard_extra_time_limit_s: float = 1200.0
    hard_extra_distance_limit_m: float = 8000.0
    stop_service_radius_m: float = 70.0
    u_turn_forbidden_radius_m: float = 100.0
    max_local_fraction: float = 0.4
    max_unknown_width_fraction: float = 0.6
    # Score impact for detours that skip service stops.
    skipped_stop_penalty_s: float = 90.0
    # Hard-reject when Valhalla reports kUturnLeft/Right (types 12/13) or explicit U-turn text.
    # Off by default: avoiding a blockage often forces a legal turnaround that Valhalla still
    # classifies as a U-turn maneuver; scoring still adds turn_pen penalties.
    reject_explicit_u_turn_maneuver: bool = False
    # Reverse-driving guardrails near anchors (bearing deltas in degrees).
    backtrack_penalty_bearing_deg: float = 125.0
    backtrack_hard_bearing_deg: float = 160.0
    # Penalty-only by default (see reject_explicit_u_turn_maneuver for the same rationale).
    reject_hard_backtrack: bool = False
    # Max share of detour polyline length allowed inside the incident geometry (0 = never accept
    # routing through the blockage; a tiny epsilon is applied in code for float noise).
    max_incident_overlap_fraction: float = 0.0
    # Sharp-turn guardrails from /trace_attributes maneuver.turn_degree.
    sharp_turn_threshold_deg: float = 120.0
    sharp_turn_hard_count: int = 3
    # Via-stop insertion: stops within this radius (m) of the detour path are served as through points.
    via_stop_corridor_m: float = 60.0
    # Accept via-stop route if time is within this factor of the non-via winner.
    via_extra_time_factor: float = 1.25


@dataclass
class SearchPolicy:
    # Parallel Valhalla calls: max concurrent requests per trip.
    valhalla_concurrency: int = 4
    # Early-accept: if best total_score <= this value, cancel remaining anchor/corridor combos.
    early_accept_score: float = 200.0
    # Per-trip soft deadline (ms). Returns best-so-far or no_safe_detour on expiry.
    per_trip_deadline_ms: int = 12000


@dataclass
class DetourPolicyConfig:
    """Versioned policy bundle; inject into all v2 modules."""

    version: str = "v2-default-4"
    anchor: AnchorPolicy = field(default_factory=AnchorPolicy)
    snap: SnapPolicy = field(default_factory=SnapPolicy)
    corridor: CorridorPolicy = field(default_factory=CorridorPolicy)
    vehicle: VehiclePolicy = field(default_factory=VehiclePolicy)
    penalty: PenaltyPolicy = field(default_factory=PenaltyPolicy)
    service: ServicePolicy = field(default_factory=ServicePolicy)
    search: SearchPolicy = field(default_factory=SearchPolicy)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "anchor": self.anchor.__dict__,
            "snap": self.snap.__dict__,
            "corridor": self.corridor.__dict__,
            "vehicle": self.vehicle.__dict__,
            "penalty": self.penalty.__dict__,
            "service": self.service.__dict__,
            "search": self.search.__dict__,
        }


def load_policy_from_json(path: Optional[str | Path] = None) -> DetourPolicyConfig:
    """Load policy from JSON file if DETOUR_POLICY_JSON is set or path given; else defaults."""
    env_path = os.getenv("DETOUR_POLICY_JSON", "").strip()
    p = Path(path) if path else (Path(env_path) if env_path else None)
    if not p or not p.is_file():
        return DetourPolicyConfig()
    raw = json.loads(p.read_text(encoding="utf-8"))
    return _policy_from_flat_dict(raw)


def _policy_from_flat_dict(raw: dict[str, Any]) -> DetourPolicyConfig:
    """Merge partial dict into DetourPolicyConfig."""
    cfg = DetourPolicyConfig()
    if "version" in raw:
        cfg.version = str(raw["version"])
    for sub, name in (
        (cfg.anchor, "anchor"),
        (cfg.snap, "snap"),
        (cfg.corridor, "corridor"),
        (cfg.vehicle, "vehicle"),
        (cfg.penalty, "penalty"),
        (cfg.service, "service"),
        (cfg.search, "search"),
    ):
        blob = raw.get(name)
        if isinstance(blob, dict):
            for k, v in blob.items():
                if hasattr(sub, k):
                    setattr(sub, k, type(getattr(sub, k))(v) if getattr(sub, k) is not None else v)
    return cfg


def get_default_policy() -> DetourPolicyConfig:
    return load_policy_from_json()
