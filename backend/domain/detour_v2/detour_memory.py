"""Persist detour requests, candidates, approvals; update evidence."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .models import RankedCandidate


def save_detour_request(
    *,
    feed_id: int,
    trip_id: str,
    route_id: str,
    service_date: str,
    incident_id: Optional[int],
    status: str,
    payload_json: Dict[str, Any],
) -> int:
    from backend.infra import db_access as db

    return db.insert_detour_request(
        feed_id=feed_id,
        trip_id=trip_id,
        route_id=route_id,
        service_date=service_date,
        incident_id=incident_id,
        status=status,
        payload_json=payload_json,
    )


def save_candidates(
    detour_request_id: int,
    candidates: List[RankedCandidate],
    *,
    selected_strategy: Optional[str] = None,
    discarded: Optional[List[RankedCandidate]] = None,
) -> None:
    from backend.infra import db_access as db

    rank = 0
    for c in candidates:
        rank += 1
        accepted = c.total_score < float("inf")
        bd = dict(c.score_breakdown or {})
        bd["tier"] = c.tier
        bd["confidence_score"] = c.confidence_score
        bd["warnings"] = list(c.warnings or [])
        bd["hard_constraints_passed"] = list(c.hard_constraints_passed or [])
        db.insert_detour_candidate(
            detour_request_id=detour_request_id,
            candidate_rank=rank,
            strategy=c.strategy,
            geometry_json=c.decoded.geometry_geojson if c.decoded else None,
            road_sequence_json=[s.__dict__ for s in (c.decoded.road_segments if c.decoded else [])],
            turn_sequence_json=[t.__dict__ for t in (c.decoded.turns if c.decoded else [])],
            travel_time_s=c.travel_time_s,
            distance_m=c.distance_m,
            score=c.total_score,
            accepted=accepted,
            rejection_reasons_json=c.rejection_reasons,
            score_breakdown_json=bd,
        )
    base = 1000
    for i, c in enumerate(discarded or []):
        bd = dict(c.score_breakdown or {})
        bd["tier"] = c.tier
        bd["discarded"] = True
        bd["confidence_score"] = c.confidence_score
        bd["warnings"] = list(c.warnings or [])
        db.insert_detour_candidate(
            detour_request_id=detour_request_id,
            candidate_rank=base + i,
            strategy=c.strategy,
            geometry_json=c.decoded.geometry_geojson if c.decoded else None,
            road_sequence_json=[s.__dict__ for s in (c.decoded.road_segments if c.decoded else [])],
            turn_sequence_json=[t.__dict__ for t in (c.decoded.turns if c.decoded else [])],
            travel_time_s=c.travel_time_s,
            distance_m=c.distance_m,
            score=c.total_score,
            accepted=False,
            rejection_reasons_json=c.rejection_reasons,
            score_breakdown_json=bd,
        )


def approve_detour(
    *,
    feed_id: int,
    route_id: str,
    trip_pattern_key: str,
    incident_signature: str,
    geometry_json: Dict[str, Any],
    road_sequence: List[Dict[str, Any]],
    turn_sequence: List[Dict[str, Any]],
    approved_by: Optional[str],
) -> int:
    from backend.infra import db_access as db

    return db.insert_approved_detour(
        feed_id=feed_id,
        route_id=route_id,
        trip_pattern_key=trip_pattern_key,
        incident_signature=incident_signature,
        geometry_json=geometry_json,
        road_sequence_json=road_sequence,
        turn_sequence_json=turn_sequence,
        approved_by=approved_by,
    )


def bump_edge_evidence(osm_way_id: int, direction: Optional[str]) -> None:
    try:
        from backend.infra import db_access as db

        db.bump_bus_edge_evidence(osm_way_id, direction)
    except Exception:
        pass
