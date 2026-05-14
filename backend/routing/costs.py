"""Detour v3-facing cost primitives (edge length + gentle turn surcharge)."""

from __future__ import annotations

from dataclasses import dataclass
from math import atan2, cos, radians, sin, sqrt
from typing import AbstractSet, Literal, Optional

BusCorridorCostMode = Literal["strict_bus_corridor", "bus_corridor_plus_connectors"]


def normalize_heading_delta_deg(heading_end_in: float, heading_start_out: float) -> float:
    """Signed turn ``[-180, 180]`` from incoming tail bearing to outgoing head bearing."""
    return float((heading_start_out - heading_end_in + 540.0) % 360.0 - 180.0)


@dataclass(frozen=True, slots=True)
class RoutingCostProfile:
    """M2 baseline: shortest path mostly by geometric length."""

    sharp_turn_penalty_m: float = 8.0
    sharp_turn_threshold_deg: float = 95.0


DEFAULT_ROUTING_COST_PROFILE = RoutingCostProfile()


def edge_cost(segment_length_m: Optional[float]) -> float:
    if segment_length_m is None or segment_length_m <= 0:
        return 1.0
    return float(segment_length_m)


def move_cost_m(
    from_length_m: Optional[float],
    turn_angle_deg: Optional[float],
    *,
    profile: RoutingCostProfile = DEFAULT_ROUTING_COST_PROFILE,
) -> float:
    """Cost paid for traversing ``from_segment`` ending with turn ``angle`` onto successor."""
    c = edge_cost(from_length_m)
    if turn_angle_deg is None:
        return c
    if abs(float(turn_angle_deg)) >= profile.sharp_turn_threshold_deg:
        c += profile.sharp_turn_penalty_m
    return c


def haversine_m(lon_a: float, lat_a: float, lon_b: float, lat_b: float) -> float:
    """Approximate great-circle distance between WGS84 points (meters)."""
    r = 6371000.0
    phi1 = radians(lat_a)
    phi2 = radians(lat_b)
    dphi = radians(lat_b - lat_a)
    dl = radians(lon_b - lon_a)
    s = sin(dphi / 2.0) ** 2 + cos(phi1) * cos(phi2) * sin(dl / 2.0) ** 2
    return 2.0 * r * atan2(sqrt(s), sqrt(max(0.0, 1.0 - s)))


_SERVICE_LIKE = frozenset(
    {
        "service",
        "track",
        "pedestrian",
        "path",
        "footway",
        "steps",
        "cycleway",
    }
)


def bus_corridor_segment_enter_penalty_m(
    segment_id: int,
    highway: Optional[str],
    *,
    observed_segment_ids: AbstractSet[int],
    mode: BusCorridorCostMode = "bus_corridor_plus_connectors",
) -> float:
    """
    Extra meters added when A* *enters* a segment (M5 bus-corridor policy).

    * ``strict_bus_corridor`` — only GTFS-observed segments are cheap; others are effectively banned.
    * ``bus_corridor_plus_connectors`` — observed segments get no premium; legal connectors get a
      moderate surcharge; service/track-like classes get a larger surcharge.
    """
    hw = (highway or "").strip().lower() or "unknown"
    if mode == "strict_bus_corridor":
        if int(segment_id) in observed_segment_ids:
            return 0.0
        return 1e18

    if int(segment_id) in observed_segment_ids:
        return 0.0
    if hw in _SERVICE_LIKE:
        return 120.0
    if hw in {"residential", "living_street", "unclassified"}:
        return 55.0
    return 35.0


__all__ = [
    "normalize_heading_delta_deg",
    "RoutingCostProfile",
    "DEFAULT_ROUTING_COST_PROFILE",
    "edge_cost",
    "move_cost_m",
    "haversine_m",
    "BusCorridorCostMode",
    "bus_corridor_segment_enter_penalty_m",
]
