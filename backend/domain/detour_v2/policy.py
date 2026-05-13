"""Tunable detour policy for standard_plus_bus — all numeric thresholds are configurable."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from typing import Any, List, Optional


@dataclass
class AnchorPolicy:
    min_anchor_gap_m: float = 50.0
    search_before_window_m: float = 400.0
    search_after_window_m: float = 400.0
    # Widening search: orchestrator uses each radius for before/after windows (meters).
    search_radii_m: List[float] = field(default_factory=lambda: [400.0, 800.0, 1500.0, 3000.0, 5000.0])
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
    # Thresholds for local-share hard rejection.
    local_fraction_hard_reject: float = 0.4
    unknown_width_local_fraction_hard_reject: float = 0.6
    # Hard-reject when local-road share exceeds threshold AND segments are not all synthetic.
    reject_hard_local_share: bool = True
    # Hard-reject any detour segment that lacks GTFS-bus or operator-approved evidence.
    # Pass 2 (relaxed fallback) disables this flag to always surface some candidate.
    require_gtfs_way_evidence: bool = True
    min_gtfs_way_confidence: float = 0.15
    max_unknown_way_fraction: float = 0.15
    # Hard-reject segments whose highway tag is non-drivable for buses.
    reject_segment_pedestrian_class: bool = True
    # Hard-reject segments with access=no/private that have no bus/psv exception.
    reject_segment_access_no_without_bus: bool = True
    # Hard-reject segments whose osm_way_id is in the incident edge bans.
    reject_segment_in_incident_ban: bool = True


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
    reject_explicit_u_turn_maneuver: bool = True
    # Reverse-driving guardrails near anchors (bearing deltas in degrees).
    backtrack_penalty_bearing_deg: float = 125.0
    backtrack_hard_bearing_deg: float = 160.0
    reject_hard_backtrack: bool = True
    # Max share of detour polyline length allowed inside the incident geometry (0 = never accept
    # routing through the blockage; a tiny epsilon is applied in code for float noise).
    max_incident_overlap_fraction: float = 0.0
    # Fraction of detour length that may coincide with the original route slice (exit→rejoin).
    # A value >= this threshold means "Valhalla just re-routed along the original road"
    # with no real bypass.  0.85 is safe: real detours still have coinciding ends.
    max_route_coincide_fraction: float = 0.50
    # Sharp-turn guardrails from /trace_attributes maneuver.turn_degree.
    sharp_turn_threshold_deg: float = 120.0
    sharp_turn_hard_count: int = 3
    # Via-stop insertion: stops within this radius (m) of the detour path are served as through points.
    via_stop_corridor_m: float = 60.0
    # Accept via-stop route if time is within this factor of the non-via winner.
    via_extra_time_factor: float = 1.25


@dataclass
class PhysicalPathPolicy:
    """Thresholds for persisted pattern-edge physical geometry (env: PHYSICAL_PATH_*)."""

    min_trip_coverage_ratio: float = 0.72
    max_ambiguous_stop_pairs: int = 2
    min_summary_confidence: float = 0.35
    max_mean_offset_m: float = 35.0
    anchor_min_coverage_ratio: float = 0.72
    max_weak_stop_pairs: int = 8
    hard_reject_wrong_entry_exit_segment: bool = True


@dataclass
class SearchPolicy:
    # Parallel Valhalla calls: max concurrent requests per trip.
    valhalla_concurrency: int = 4
    # Early-accept: if best total_score <= this value, cancel remaining anchor/corridor combos.
    early_accept_score: float = 200.0
    # Early-accept when tier is AUTO_OK (stops widening search).
    early_accept_tier_auto_ok: bool = True
    # Per-trip soft deadline (ms). Returns best-so-far tiered result on expiry.
    per_trip_deadline_ms: int = 12000


@dataclass
class DetourPolicyConfig:
    """Versioned policy bundle; inject into all v2 modules."""

    version: str = "v2-default-7"
    anchor: AnchorPolicy = field(default_factory=AnchorPolicy)
    snap: SnapPolicy = field(default_factory=SnapPolicy)
    corridor: CorridorPolicy = field(default_factory=CorridorPolicy)
    vehicle: VehiclePolicy = field(default_factory=VehiclePolicy)
    penalty: PenaltyPolicy = field(default_factory=PenaltyPolicy)
    service: ServicePolicy = field(default_factory=ServicePolicy)
    search: SearchPolicy = field(default_factory=SearchPolicy)
    physical_path: PhysicalPathPolicy = field(default_factory=PhysicalPathPolicy)

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
            "physical_path": self.physical_path.__dict__,
        }


def load_policy_from_json(path: Optional[str | Path] = None) -> DetourPolicyConfig:
    """Load policy from JSON file if DETOUR_POLICY_JSON is set or path given; else defaults."""
    env_path = os.getenv("DETOUR_POLICY_JSON", "").strip()
    p = Path(path) if path else (Path(env_path) if env_path else None)
    if not p or not p.is_file():
        cfg = DetourPolicyConfig()
        _apply_physical_path_env_overrides(cfg.physical_path)
        return cfg
    raw = json.loads(p.read_text(encoding="utf-8"))
    cfg = _policy_from_flat_dict(raw)
    _apply_physical_path_env_overrides(cfg.physical_path)
    return cfg


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
        (cfg.physical_path, "physical_path"),
    ):
        blob = raw.get(name)
        if isinstance(blob, dict):
            for k, v in blob.items():
                if not hasattr(sub, k):
                    continue
                cur = getattr(sub, k)
                if k == "search_radii_m" and isinstance(v, list):
                    setattr(sub, k, [float(x) for x in v])
                elif cur is not None and type(cur) is not type(v) and not isinstance(v, (list, dict)):
                    setattr(sub, k, type(cur)(v))
                else:
                    setattr(sub, k, v)
    return cfg


def _apply_physical_path_env_overrides(pp: PhysicalPathPolicy) -> None:
    """Apply PHYSICAL_PATH_* environment variables (see backend.infra.config)."""
    try:
        from backend.infra import config as _cfg
    except Exception:
        return
    pp.min_trip_coverage_ratio = float(_cfg.PHYSICAL_PATH_MIN_TRIP_COVERAGE_RATIO)
    pp.max_ambiguous_stop_pairs = int(_cfg.PHYSICAL_PATH_MAX_AMBIGUOUS_STOP_PAIRS)
    pp.min_summary_confidence = float(_cfg.PHYSICAL_PATH_MIN_SUMMARY_CONFIDENCE)
    pp.max_mean_offset_m = float(_cfg.PHYSICAL_PATH_MAX_MEAN_OFFSET_M)
    pp.anchor_min_coverage_ratio = float(_cfg.PHYSICAL_PATH_ANCHOR_MIN_COVERAGE_RATIO)
    pp.max_weak_stop_pairs = int(_cfg.PHYSICAL_PATH_MAX_WEAK_STOP_PAIRS)
    pp.hard_reject_wrong_entry_exit_segment = bool(_cfg.PHYSICAL_PATH_HARD_REJECT_WRONG_ENTRY_EXIT_SEGMENT)


def get_default_policy() -> DetourPolicyConfig:
    return load_policy_from_json()
