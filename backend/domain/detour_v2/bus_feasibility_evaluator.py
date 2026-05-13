"""Bus feasibility: scorer + only four absolute hard rejects."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

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
# Highway classes that are never drivable by a bus.
NON_DRIVABLE = frozenset({"footway", "path", "cycleway", "pedestrian", "steps", "bridleway", "construction"})

# Valhalla DirectionsLeg.Maneuver.Type (directions.proto): kUturnRight=12, kUturnLeft=13
_VALHALLA_UTURN_MANEUVER_TYPES = frozenset({12, 13})
_EXPLICIT_UTURN_TEXT = re.compile(
    r"\b(u[-\s]?turn|make\s+a\s+u[-\s]?turn|perform\s+a\s+u[-\s]?turn)\b",
    re.IGNORECASE,
)


def _text_indicates_explicit_u_turn(txt: str) -> bool:
    return bool(_EXPLICIT_UTURN_TEXT.search(txt))


def _maneuver_type_as_int(mt: Any) -> Optional[int]:
    if mt is None or isinstance(mt, bool):
        return None
    if isinstance(mt, int):
        return mt
    if isinstance(mt, float):
        if mt != mt:
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


def _build_segment_index(segments: List[RoadSegmentRef]) -> Dict[int, RoadSegmentRef]:
    out: Dict[int, RoadSegmentRef] = {}
    for s in segments:
        out[int(s.segment_id)] = s
    return out


def _turn_ban_set(projection: IncidentProjection) -> Set[Tuple[int, int, int]]:
    return {(int(b.from_way_id), int(b.via_node_id), int(b.to_way_id)) for b in projection.turn_bans}


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
    bus_edge_evidence: Optional[Dict[int, Dict[str, Any]]] = None,
    turn_by_turn: Optional[List[Dict[str, Any]]] = None,
    bearing_delta_exit_deg: Optional[float] = None,
    bearing_delta_rejoin_deg: Optional[float] = None,
    path_intersects_blockage: bool = False,
    stitch_ok: bool = True,
    invalid_geometry: bool = False,
    carriageway_reasons: Optional[List[str]] = None,
    route_coincide_fraction: Optional[float] = None,
) -> FeasibilityResult:
    """
    Absolute hard rejects (only):
      - path_intersects_blockage
      - wrong_direction_rejoin (bearing delta >= backtrack_hard_bearing_deg)
      - invalid_geometry
      - cannot_stitch_service (rejoin not downstream of exit)
    Everything else is penalty + warnings + confidence reduction.
    """
    banned_way_ids = banned_way_ids or {b.osm_way_id for b in projection.edge_bans}
    gtfs_way_evidence = gtfs_way_evidence or {}
    bus_edge_evidence = bus_edge_evidence or {}
    hard: List[str] = []
    notes: List[str] = []
    warnings: List[str] = []
    seg_pen = 0.0
    turn_pen = 0.0
    unc_pen = 0.0
    svc_pen = 0.0
    ev_bonus = 0.0
    confidence = 1.0

    if invalid_geometry:
        hard.append("invalid_geometry")
        return FeasibilityResult(
            accepted=False,
            hard_reject_reasons=hard,
            confidence_score=0.0,
            warnings=["invalid_geometry"],
        )
    if path_intersects_blockage:
        hard.append("path_intersects_blockage")
        return FeasibilityResult(
            accepted=False,
            hard_reject_reasons=hard,
            confidence_score=0.0,
            warnings=["path_intersects_blockage"],
        )
    # Hard reject: detour coincides with the original route (no real bypass).
    if route_coincide_fraction is not None:
        cap_coincide = float(getattr(policy.service, "max_route_coincide_fraction", 0.85))
        if route_coincide_fraction >= cap_coincide:
            hard.append(f"no_real_bypass:coincide={round(route_coincide_fraction, 2)}")
            return FeasibilityResult(
                accepted=False,
                hard_reject_reasons=hard,
                confidence_score=0.0,
                warnings=[f"detour_coincides_with_route_{round(route_coincide_fraction * 100)}pct"],
            )
    if not stitch_ok:
        hard.append("cannot_stitch_service")
        return FeasibilityResult(
            accepted=False,
            hard_reject_reasons=hard,
            confidence_score=0.0,
            warnings=["rejoin_not_downstream"],
        )

    hard_thr = float(policy.service.backtrack_hard_bearing_deg)
    max_backtrack = max(float(bearing_delta_exit_deg or 0.0), float(bearing_delta_rejoin_deg or 0.0))
    if max_backtrack >= hard_thr and bool(getattr(policy.service, "reject_hard_backtrack", True)):
        hard.append("wrong_direction_rejoin")
        return FeasibilityResult(
            accepted=False,
            hard_reject_reasons=hard,
            confidence_score=0.0,
            warnings=[f"wrong_direction_rejoin_delta_deg={round(max_backtrack, 1)}"],
        )

    # Explicit U-turn maneuver check (kUturnRight=12, kUturnLeft=13).
    if turn_by_turn:
        _uturn_detected = False
        for row in turn_by_turn:
            mti = _maneuver_type_as_int(row.get("maneuver_type"))
            if mti is not None and mti in _VALHALLA_UTURN_MANEUVER_TYPES:
                _uturn_detected = True
                break
            txt = str(row.get("instruction_en") or "")
            if _text_indicates_explicit_u_turn(txt):
                _uturn_detected = True
                break
        if _uturn_detected:
            if bool(getattr(policy.service, "reject_explicit_u_turn_maneuver", True)):
                hard.append("explicit_u_turn_maneuver")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=["u_turn_maneuver_detected"],
                )
            else:
                # Penalty-only fallback when flag is disabled.
                turn_pen += max(float(policy.penalty.turn_sharp_s) * 2.0, 30.0)
                notes.append("explicit_u_turn_instruction")
                warnings.append("u_turn_maneuver_detected")
                confidence -= 0.05

    pen_thr = float(policy.service.backtrack_penalty_bearing_deg)
    if max_backtrack >= pen_thr:
        excess = max(0.0, max_backtrack - pen_thr)
        turn_pen += float(policy.penalty.turn_sharp_s) * (1.0 + excess / max(1.0, 180.0 - pen_thr))
        notes.append(f"backtrack_heading_delta={round(max_backtrack, 1)}")
        warnings.append("mild_anchor_heading_mismatch")
        confidence -= 0.05

    total_d = sum(s.length_m for s in decoded.road_segments) or detour_distance_m
    local_d = 0.0
    unk_local_d = 0.0
    unknown_way_d = 0.0

    all_synthetic = all(s.synthetic for s in decoded.road_segments) if decoded.road_segments else True

    for seg in decoded.road_segments:
        if seg.synthetic:
            unc_pen += policy.penalty.segment_uncertainty_local_per_segment_s * 0.25
            local_d += seg.length_m * 0.3
            unk_local_d += seg.length_m * 0.3
            unknown_way_d += seg.length_m
            warnings.append("synthetic_segment_uncertainty")
            confidence -= 0.02
            continue
        cls = _segment_class(seg.highway)
        hw = (seg.highway or "").strip().lower()
        access = (seg.access or "").strip().lower()
        bus_v = (seg.bus or "").strip().lower()
        psv_v = (seg.psv or "").strip().lower()
        wid = int(seg.osm_way_id or 0)

        # Hard reject: non-drivable road class (footway, path, cycleway, pedestrian, …).
        if not all_synthetic and bool(getattr(policy.vehicle, "reject_segment_pedestrian_class", True)):
            if hw in NON_DRIVABLE:
                hard.append(f"non_drivable_class:{hw}:{wid}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=[f"non_drivable_highway_{hw}_{wid}"],
                )

        # Hard reject: access=no/private without bus/psv exception.
        if not all_synthetic and bool(getattr(policy.vehicle, "reject_segment_access_no_without_bus", True)):
            if access in {"no", "private"} and bus_v not in {"yes", "designated"} and psv_v not in {"yes", "designated"}:
                hard.append(f"access_restricted_without_bus_exception:{wid}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=[f"access_restricted_way_{wid}"],
                )

        # Hard reject: bus explicitly forbidden.
        if not all_synthetic and (bus_v in {"no", "private"} or psv_v in {"no", "private"}):
            hard.append(f"bus_explicitly_forbidden:{wid}")
            return FeasibilityResult(
                accepted=False,
                hard_reject_reasons=hard,
                confidence_score=0.0,
                warnings=[f"non_bus_tag_way_{wid}"],
            )

        # Hard reject: segment is in the incident edge ban list.
        if not all_synthetic and bool(getattr(policy.vehicle, "reject_segment_in_incident_ban", True)):
            if wid and wid in banned_way_ids:
                hard.append(f"uses_banned_incident_way:{wid}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=[f"uses_banned_incident_way_{wid}"],
                )

        if wid and wid in banned_way_ids:
            seg_pen += 400.0
            warnings.append(f"uses_banned_incident_way_{wid}")
            confidence -= 0.15
        if bus_v in {"no", "private"} or psv_v in {"no", "private"}:
            seg_pen += 300.0
            warnings.append(f"non_bus_tag_way_{wid}")
            confidence -= 0.12
        if access in {"no", "private"} and bus_v not in {"yes", "designated"} and psv_v not in {"yes", "designated"}:
            seg_pen += 350.0
            warnings.append(f"access_restricted_way_{wid}")
            confidence -= 0.14
        if policy.vehicle.require_gtfs_way_evidence:
            wid_int = int(seg.osm_way_id or 0)
            ev = gtfs_way_evidence.get(wid_int)
            gtfs_conf = float((ev or {}).get("confidence_score") or 0.0)
            # Also credit operator-approved bus_edge_evidence as "known".
            bus_ev_entry = bus_edge_evidence.get(wid_int)
            bus_ev_conf = float((bus_ev_entry or {}).get("confidence_score") or 0.0)
            bus_ev_appr = int((bus_ev_entry or {}).get("approved_detour_count") or 0)
            has_known = (
                gtfs_conf >= float(policy.vehicle.min_gtfs_way_confidence)
                or bus_ev_conf >= 0.2
                or bus_ev_appr > 0
            )
            if not has_known:
                hard.append(f"not_in_gtfs_bus_corridor:{wid_int}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=[f"way_{wid_int}_no_gtfs_bus_evidence"],
                )
        if cls == "living":
            seg_pen += 500.0
            warnings.append("living_street")
            confidence -= 0.2
        elif cls == "local":
            local_d += seg.length_m
            seg_pen += (seg.length_m / 1000.0) * policy.penalty.segment_local_per_km_s
            unk_local_d += seg.length_m
        elif cls == "other":
            seg_pen += (seg.length_m / 1000.0) * policy.penalty.segment_unclassified_per_km_s

        if wid and wid in bus_edge_evidence:
            be = bus_edge_evidence[wid]
            bconf = float((be or {}).get("confidence_score") or 0.0)
            appr = int((be or {}).get("approved_detour_count") or 0)
            if bconf >= 0.2 or appr > 0:
                ev_bonus += (seg.length_m / 1000.0) * policy.penalty.segment_evidence_bonus_per_km_s
                confidence += 0.02

    if total_d > 0 and not all_synthetic:
        lf = local_d / total_d
        uf = unk_local_d / total_d
        if bool(getattr(policy.vehicle, "reject_hard_local_share", True)):
            if lf > policy.vehicle.local_fraction_hard_reject:
                hard.append(f"local_road_share_too_high:{round(lf, 2)}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=["local_fraction_exceeded"],
                )
            if uf > policy.vehicle.unknown_width_local_fraction_hard_reject:
                hard.append(f"unknown_local_share_too_high:{round(uf, 2)}")
                return FeasibilityResult(
                    accepted=False,
                    hard_reject_reasons=hard,
                    confidence_score=0.0,
                    warnings=["unknown_width_local_fraction_exceeded"],
                )
        else:
            if lf > policy.vehicle.local_fraction_hard_reject:
                seg_pen += 120.0
                warnings.append("high_local_road_share")
                confidence -= 0.08
            if uf > policy.vehicle.unknown_width_local_fraction_hard_reject:
                seg_pen += 120.0
                warnings.append("high_unknown_local_share")
                confidence -= 0.08
        if policy.vehicle.require_gtfs_way_evidence:
            wf = unknown_way_d / total_d
            if wf > float(policy.vehicle.max_unknown_way_fraction):
                seg_pen += 150.0
                warnings.append("gtfs_unknown_way_fraction_exceeded")
                confidence -= 0.1

    extra_t = max(0.0, detour_time_s - baseline_blocked_time_s)
    extra_d = max(0.0, detour_distance_m - baseline_blocked_distance_m)
    if extra_t > policy.service.soft_extra_time_s:
        svc_pen += extra_t - policy.service.soft_extra_time_s
    if extra_d > policy.service.soft_extra_distance_m:
        svc_pen += (extra_d - policy.service.soft_extra_distance_m) / 10.0
    if detour_time_s > policy.service.hard_extra_time_limit_s:
        svc_pen += (detour_time_s - policy.service.hard_extra_time_limit_s) * 0.5
        warnings.append("service_time_excessive")
        confidence -= 0.06
    if detour_distance_m > policy.service.hard_extra_distance_limit_m:
        svc_pen += (detour_distance_m - policy.service.hard_extra_distance_limit_m) / 20.0
        warnings.append("service_distance_excessive")
        confidence -= 0.06

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
    for turn in decoded.turns:
        if turn.turn_angle is not None and abs(float(turn.turn_angle)) >= sharp_turn_thr:
            sharp_count += 1
    if sharp_count > 0:
        turn_pen += sharp_count * sharp_turn_pen_each
        notes.append(f"sharp_turns={sharp_count}")
        warnings.append(f"sharp_turns_{sharp_count}")
        confidence -= 0.03 * sharp_count
    if sharp_count >= sharp_turn_hard:
        turn_pen += 200.0
        warnings.append(f"too_many_sharp_turns_{sharp_count}")
        confidence -= 0.1

    # OSM turn restrictions (incident projection): penalty only
    ban_set = _turn_ban_set(projection)
    if ban_set and decoded.turns:
        seg_by_id = _build_segment_index(decoded.road_segments)
        for turn in decoded.turns:
            fs = seg_by_id.get(int(turn.from_segment_id))
            ts = seg_by_id.get(int(turn.to_segment_id))
            if not fs or not ts:
                continue
            triplet = (int(fs.osm_way_id or 0), int(turn.via_node_id), int(ts.osm_way_id or 0))
            if triplet in ban_set:
                turn_pen += float(policy.penalty.turn_complex_s) * 4.0
                warnings.append(f"violates_osm_turn_restriction:{triplet}")

    if carriageway_reasons:
        for cr in carriageway_reasons:
            seg_pen += 180.0
            warnings.append(f"carriageway:{cr}")
            confidence -= 0.08

    confidence = max(0.0, min(1.0, confidence))
    return FeasibilityResult(
        accepted=True,
        hard_reject_reasons=[],
        segment_penalty_s=seg_pen,
        turn_penalty_s=turn_pen,
        uncertainty_penalty_s=unc_pen,
        service_penalty_s=svc_pen,
        evidence_bonus_s=ev_bonus,
        notes=notes,
        sharp_turn_count=sharp_count,
        confidence_score=confidence,
        warnings=warnings,
    )
