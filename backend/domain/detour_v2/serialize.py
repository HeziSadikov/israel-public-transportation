"""JSON-serializable views of detour v2 dataclasses."""

from __future__ import annotations

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
    return {
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
    if out.debug and out.status in ("no_safe_detour", "error"):
        d["debug"] = dict(out.debug)
    return d
