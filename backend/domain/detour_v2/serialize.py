"""JSON-serializable views of detour v2 dataclasses."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .models import (
    AnchorPair,
    DetourComputeOutput,
    FeasibilityResult,
    RankedCandidate,
    StitchingResult,
)


def _feas(f: FeasibilityResult) -> Dict[str, Any]:
    return {
        "accepted": f.accepted,
        "hard_reject_reasons": list(f.hard_reject_reasons),
        "segment_penalty_s": f.segment_penalty_s,
        "turn_penalty_s": f.turn_penalty_s,
        "uncertainty_penalty_s": f.uncertainty_penalty_s,
        "service_penalty_s": f.service_penalty_s,
        "evidence_bonus_s": f.evidence_bonus_s,
        "sharp_turn_count": f.sharp_turn_count,
        "notes": list(f.notes),
    }


def _ranked(c: RankedCandidate) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "strategy": c.strategy,
        "total_score": c.total_score,
        "travel_time_s": c.travel_time_s,
        "distance_m": c.distance_m,
        "rejection_reasons": list(c.rejection_reasons),
        "score_breakdown": dict(c.score_breakdown),
    }
    if c.feasibility:
        out["feasibility"] = _feas(c.feasibility)
    if c.decoded:
        out["geometry_geojson"] = c.decoded.geometry_geojson
    return out


def _anchors(a: AnchorPair) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "exit_lon": a.exit_lon,
        "exit_lat": a.exit_lat,
        "rejoin_lon": a.rejoin_lon,
        "rejoin_lat": a.rejoin_lat,
        "exit_stop_id": a.exit_stop_id,
        "rejoin_stop_id": a.rejoin_stop_id,
        "exit_shape_dist_m": a.exit_shape_dist_m,
        "rejoin_shape_dist_m": a.rejoin_shape_dist_m,
        "anchor_quality_note": a.anchor_quality_note,
    }
    if a.exit_forward_bearing_deg is not None:
        d["exit_forward_bearing_deg"] = a.exit_forward_bearing_deg
    if a.rejoin_forward_bearing_deg is not None:
        d["rejoin_forward_bearing_deg"] = a.rejoin_forward_bearing_deg
    if a.anchor_geometry_source:
        d["anchor_geometry_source"] = a.anchor_geometry_source
    if a.anchor_source:
        d["anchor_source"] = a.anchor_source
    if a.exit_osm_segment_id is not None:
        d["exit_osm_segment_id"] = a.exit_osm_segment_id
    if a.rejoin_osm_segment_id is not None:
        d["rejoin_osm_segment_id"] = a.rejoin_osm_segment_id
    return d


def _stitch(s: StitchingResult) -> Dict[str, Any]:
    return {
        "exit_stop_index": s.exit_stop_index,
        "rejoin_stop_index": s.rejoin_stop_index,
        "served_stop_ids": list(s.served_stop_ids),
        "skipped_stop_ids": list(s.skipped_stop_ids),
        "skipped_reasons": dict(s.skipped_reasons),
        "stitch_ok": s.stitch_ok,
        "stitch_notes": list(s.stitch_notes),
    }


def _summary_en(selected: RankedCandidate, out: DetourComputeOutput) -> str:
    """One-line human-readable explanation of why this candidate was selected."""
    parts: List[str] = []
    # Anchor info
    if out.anchors:
        exit_s = out.anchors.exit_stop_id or "?"
        rejoin_s = out.anchors.rejoin_stop_id or "?"
        parts.append(f"Exit stop {exit_s} → rejoin stop {rejoin_s}")
    # Corridor stage
    if out.corridor_stage:
        parts.append(f"corridor={out.corridor_stage}")
    # Distance / time extra vs baseline
    bd = selected.score_breakdown
    t_s = bd.get("travel_time_s", selected.travel_time_s)
    if isinstance(t_s, (int, float)):
        parts.append(f"{round(float(t_s))} s travel")
    # Distance
    d_m = selected.distance_m
    if d_m:
        parts.append(f"{round(d_m)} m")
    # Skipped stops
    skipped = int(bd.get("skipped_stops") or 0)
    if skipped > 0:
        parts.append(f"{skipped} stop(s) skipped")
    else:
        parts.append("all stops served")
    # GTFS evidence
    if out.selected and out.selected.decoded:
        segs = out.selected.decoded.road_segments
        non_syn = [s for s in segs if not s.synthetic and int(s.osm_way_id or 0) > 0]
        parts.append(f"OSM way IDs: {len(non_syn)}")
    # Sharp turns
    sharp = int(bd.get("sharp_turn_count") or 0)
    if sharp:
        parts.append(f"{sharp} sharp turn(s)")
    # Rejection reasons
    if selected.rejection_reasons:
        parts.append(f"REJECTED: {', '.join(selected.rejection_reasons[:3])}")
    return "; ".join(parts) if parts else "selected candidate"


def build_detour_ai_log_payload(out: DetourComputeOutput) -> Dict[str, Any]:
    """
    Compact, geometry-free summary for server logs / LLM debugging (paste one JSON line).
    Omits GeoJSON coordinates; keeps scores, feasibility rejects, anchors, stitching, attempts.
    """
    dbg = out.debug if isinstance(out.debug, dict) else {}
    candidates_brief: List[Dict[str, Any]] = []
    for c in out.candidates or []:
        row: Dict[str, Any] = {
            "strategy": c.strategy,
            "total_score": c.total_score,
            "travel_time_s": c.travel_time_s,
            "distance_m": c.distance_m,
            "rejection_reasons": list(c.rejection_reasons)[:20],
        }
        if c.feasibility:
            row["feasibility"] = _feas(c.feasibility)
        if c.score_breakdown:
            row["score_breakdown"] = dict(c.score_breakdown)
        candidates_brief.append(row)
    selected_brief: Optional[Dict[str, Any]] = None
    if out.selected:
        selected_brief = {
            "strategy": out.selected.strategy,
            "total_score": out.selected.total_score,
            "travel_time_s": out.selected.travel_time_s,
            "distance_m": out.selected.distance_m,
            "rejection_reasons": list(out.selected.rejection_reasons)[:20],
            "summary_en": _summary_en(out.selected, out),
        }
        if out.selected.feasibility:
            selected_brief["feasibility"] = _feas(out.selected.feasibility)
        if out.selected.score_breakdown:
            selected_brief["score_breakdown"] = dict(out.selected.score_breakdown)
    attempts_trim: List[Dict[str, Any]] = []
    for row in out.attempts or []:
        if not isinstance(row, dict):
            continue
        r2 = dict(row)
        ved = r2.get("valhalla_error_detail")
        if ved is not None and len(str(ved)) > 500:
            r2["valhalla_error_detail"] = str(ved)[:500] + "…"
        attempts_trim.append(r2)

    return {
        "trip_id": out.trip_id,
        "route_id": out.route_id,
        "status": out.status,
        "error": out.error,
        "policy_version": out.policy_version,
        "corridor_stage": out.corridor_stage,
        "physical_path_used": dbg.get("physical_path_used"),
        "physical_fallback_reason": dbg.get("physical_fallback_reason"),
        "matched_trip_metadata": dbg.get("matched_trip_metadata"),
        "anchors": _anchors(out.anchors) if out.anchors else None,
        "stitching": _stitch(out.stitching) if out.stitching else None,
        "attempts": attempts_trim,
        "candidates_ranked": candidates_brief,
        "selected": selected_brief,
    }


def format_detour_ai_log_line(out: DetourComputeOutput, *, max_chars: int = 48000) -> str:
    """Single-line JSON for logging (may truncate)."""
    payload = build_detour_ai_log_payload(out)
    s = json.dumps(payload, ensure_ascii=False, default=str)
    if len(s) > max_chars:
        s = s[: max_chars - 40] + '…","_truncated":true}'
    return s


def detour_compute_output_to_dict(out: DetourComputeOutput) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "status": out.status,
        "trip_id": out.trip_id,
        "route_id": out.route_id,
        "policy_version": out.policy_version,
        "error": out.error,
        "corridor_stage": out.corridor_stage,
    }
    if out.anchors:
        d["anchors"] = _anchors(out.anchors)
    if out.attempts:
        d["attempts"] = list(out.attempts)
    if out.candidates:
        d["candidates"] = [_ranked(c) for c in out.candidates]
    if out.selected:
        sel_dict = _ranked(out.selected)
        sel_dict["summary_en"] = _summary_en(out.selected, out)
        d["selected"] = sel_dict
    if out.stitching:
        d["stitching"] = _stitch(out.stitching)
    if out.debug is not None:
        d["debug"] = dict(out.debug)
    return d
