"""Bus feasibility for standard_plus_bus: bans, penalties, service checks."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from .models import (
    DecodedCandidate,
    FeasibilityResult,
    IncidentProjection,
    RoadSegmentRef,
)
from .policy import DetourPolicyConfig


MAJOR = frozenset(
    {
        "trunk",
        "trunk_link",
        "primary",
        "primary_link",
        "secondary",
        "secondary_link",
        "tertiary",
    }
)
LOCAL = frozenset({"residential", "service", "unclassified", "tertiary_link"})

# Valhalla DirectionsLeg.Maneuver.Type (directions.proto): kUturnRight=12, kUturnLeft=13
_VALHALLA_UTURN_MANEUVER_TYPES = frozenset({12, 13})
# Avoid substring false positives (e.g. "uturn" matching inside unrelated words).
_EXPLICIT_UTURN_TEXT = re.compile(
    r"\b(u[-\s]?turn|make\s+a\s+u[-\s]?turn|perform\s+a\s+u[-\s]?turn)\b",
    re.IGNORECASE,
)


def _text_indicates_explicit_u_turn(txt: str) -> bool:
    return bool(_EXPLICIT_UTURN_TEXT.search(txt))


def _maneuver_type_as_int(mt: Any) -> Optional[int]:
    """Normalize Valhalla maneuver `type` (JSON may use int, float, or numeric string)."""
    if mt is None or isinstance(mt, bool):
        return None
    if isinstance(mt, int):
        return mt
    if isinstance(mt, float):
        if mt != mt:  # NaN
            return None
        r = round(mt)
        if abs(mt - r) > 1e-6:
            return None
        return int(r)
    if isinstance(mt, str):
        s = mt.strip()
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            try:
                return int(s)
            except ValueError:
                return None
    return None


def _segment_class(highway: Optional[str]) -> str:
    h = (highway or "").lower()
    if h in MAJOR:
        return "major"
    if h in LOCAL:
        return "local"
    if h == "living_street":
        return "living"
    return "other"


def evaluate_candidate(
    *,
    decoded: DecodedCandidate,
    projection: IncidentProjection,
    policy: DetourPolicyConfig,
    baseline_blocked_distance_m: float,
    baseline_blocked_time_s: float,
    detour_distance_m: float,
    detour_time_s: float,
    banned_way_ids: Optional[Set[int]] = None,
    gtfs_way_evidence: Optional[Dict[int, Dict[str, Any]]] = None,
    turn_by_turn: Optional[List[Dict[str, Any]]] = None,
    bearing_delta_exit_deg: Optional[float] = None,
    bearing_delta_rejoin_deg: Optional[float] = None,
) -> FeasibilityResult:
    banned_way_ids = banned_way_ids or {b.osm_way_id for b in projection.edge_bans}
    hard: List[str] = []
    notes: List[str] = []
    seg_pen = 0.0
    turn_pen = 0.0
    unc_pen = 0.0
    svc_pen = 0.0
    ev_bonus = 0.0
    gtfs_way_evidence = gtfs_way_evidence or {}

    total_d = sum(s.length_m for s in decoded.road_segments) or detour_distance_m
    local_d = 0.0
    unk_local_d = 0.0
    unknown_way_d = 0.0

    for seg in decoded.road_segments:
        if seg.synthetic:
            unc_pen += policy.penalty.segment_uncertainty_local_per_segment_s * 0.25
            local_d += seg.length_m * 0.3
            unk_local_d += seg.length_m * 0.3
            unknown_way_d += seg.length_m
            continue
        cls = _segment_class(seg.highway)
        if seg.osm_way_id and seg.osm_way_id in banned_way_ids:
            hard.append(f"banned_way_{seg.osm_way_id}")
        access = (seg.access or "").strip().lower()
        bus_v = (seg.bus or "").strip().lower()
        psv_v = (seg.psv or "").strip().lower()
        if bus_v in {"no", "private"} or psv_v in {"no", "private"}:
            hard.append(f"non_bus_tag_way_{seg.osm_way_id}")
        if access in {"no", "private"} and bus_v not in {"yes", "designated"} and psv_v not in {"yes", "designated"}:
            hard.append(f"access_restricted_way_{seg.osm_way_id}")
        if policy.vehicle.require_gtfs_way_evidence:
            ev = gtfs_way_evidence.get(int(seg.osm_way_id or 0))
            conf = float((ev or {}).get("confidence_score") or 0.0)
            if conf < float(policy.vehicle.min_gtfs_way_confidence):
                hard.append(f"not_in_gtfs_bus_corridor_{seg.osm_way_id}")
                unknown_way_d += seg.length_m
        if cls == "living":
            hard.append("living_street")
        elif cls == "local":
            local_d += seg.length_m
            seg_pen += (seg.length_m / 1000.0) * policy.penalty.segment_local_per_km_s
            unk_local_d += seg.length_m
        elif cls == "other":
            seg_pen += (seg.length_m / 1000.0) * policy.penalty.segment_unclassified_per_km_s

    all_synthetic = all(s.synthetic for s in decoded.road_segments) if decoded.road_segments else True
    if total_d > 0 and not all_synthetic:
        lf = local_d / total_d
        uf = unk_local_d / total_d
        if bool(getattr(policy.vehicle, "reject_hard_local_share", False)):
            if lf > policy.vehicle.local_fraction_hard_reject:
                hard.append("local_fraction_exceeded")
            if uf > policy.vehicle.unknown_width_local_fraction_hard_reject:
                hard.append("unknown_width_local_fraction_exceeded")
        if policy.vehicle.require_gtfs_way_evidence:
            wf = unknown_way_d / total_d
            if wf > float(policy.vehicle.max_unknown_way_fraction):
                hard.append("gtfs_unknown_way_fraction_exceeded")

    extra_t = max(0.0, detour_time_s - baseline_blocked_time_s)
    extra_d = max(0.0, detour_distance_m - baseline_blocked_distance_m)
    if extra_t > policy.service.soft_extra_time_s:
        svc_pen += extra_t - policy.service.soft_extra_time_s
    if extra_d > policy.service.soft_extra_distance_m:
        svc_pen += (extra_d - policy.service.soft_extra_distance_m) / 10.0
    if detour_time_s > policy.service.hard_extra_time_limit_s:
        hard.append("service_time_excessive")
    if detour_distance_m > policy.service.hard_extra_distance_limit_m:
        hard.append("service_distance_excessive")

    # Maneuver-level quality checks (no-nonsense U-turns and reverse-looking exits/rejoins).
    if turn_by_turn:
        has_u_turn = False
        for row in turn_by_turn:
            mti = _maneuver_type_as_int(row.get("maneuver_type"))
            if mti is not None and mti in _VALHALLA_UTURN_MANEUVER_TYPES:
                has_u_turn = True
                break
            txt = str(row.get("instruction_en") or "")
            if _text_indicates_explicit_u_turn(txt):
                has_u_turn = True
                break
        if has_u_turn:
            turn_pen += max(float(policy.penalty.turn_sharp_s) * 2.0, 30.0)
            notes.append("explicit_u_turn_instruction")
            if bool(policy.service.reject_explicit_u_turn_maneuver):
                hard.append("u_turn_maneuver")

    pen_thr = float(policy.service.backtrack_penalty_bearing_deg)
    hard_thr = float(policy.service.backtrack_hard_bearing_deg)
    max_backtrack = max(float(bearing_delta_exit_deg or 0.0), float(bearing_delta_rejoin_deg or 0.0))
    if max_backtrack >= pen_thr:
        excess = max(0.0, max_backtrack - pen_thr)
        # Linear ramp from mild to strong penalty as heading reverses toward 180deg.
        turn_pen += float(policy.penalty.turn_sharp_s) * (1.0 + excess / max(1.0, 180.0 - pen_thr))
        notes.append(f"backtrack_heading_delta={round(max_backtrack, 1)}")
    if max_backtrack >= hard_thr and bool(policy.service.reject_hard_backtrack):
        hard.append("anchor_backtrack_heading")

    # Sharp-turn detection from /trace_attributes maneuver data (A4).
    # The decoded road segments carry turn_angle via TurnRef, but we can also count
    # Valhalla turn_by_turn steps that contain sharp-turn keywords.
    sharp_turn_thr = float(getattr(policy.service, "sharp_turn_threshold_deg", 120.0))
    sharp_turn_hard = int(getattr(policy.service, "sharp_turn_hard_count", 3))
    sharp_turn_pen_each = float(getattr(policy.penalty, "sharp_turn_per_occurrence_s", 25.0))
    sharp_count = 0
    if turn_by_turn:
        sharp_keywords = {"sharp left", "sharp right", "sharp turn", "hard left", "hard right"}
        for row in turn_by_turn:
            txt = str(row.get("instruction_en") or "").strip().lower()
            if any(kw in txt for kw in sharp_keywords):
                sharp_count += 1
    # Also count from segment turn_angle data if available.
    for turn in decoded.turns:
        if turn.turn_angle is not None and abs(float(turn.turn_angle)) >= sharp_turn_thr:
            sharp_count += 1
    if sharp_count > 0:
        turn_pen += sharp_count * sharp_turn_pen_each
        notes.append(f"sharp_turns={sharp_count}")
    if sharp_count >= sharp_turn_hard:
        hard.append(f"too_many_sharp_turns_{sharp_count}")

    accepted = len(hard) == 0
    return FeasibilityResult(
        accepted=accepted,
        hard_reject_reasons=hard,
        segment_penalty_s=seg_pen,
        turn_penalty_s=turn_pen,
        uncertainty_penalty_s=unc_pen,
        service_penalty_s=svc_pen,
        evidence_bonus_s=ev_bonus,
        notes=notes,
        sharp_turn_count=sharp_count,
    )
