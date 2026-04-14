"""Tunable weights for A* routing on pattern-stop graphs (detours, /graph/build)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RoutingPolicy:
    """Edge weight shaping for `astar_route` (times in seconds)."""

    per_edge_penalty_s: float = 10.0
    transfer_penalty_s: float = 240.0
    # Additional transfer shaping for navigational realism.
    transfer_distance_penalty_per_m_s: float = 0.0
    pattern_switch_penalty_s: float = 0.0
    # frequency_discount: weight *= 1/(1 + frequency_discount_coef * min(freq, freq_cap))
    frequency_discount_coef: float = 0.01
    frequency_cap: float = 60.0
    # Heuristic: straight-line distance / max_speed_m_s (admissible upper bound on travel time)
    heuristic_max_speed_m_s: float = 22.22


def default_routing_policy() -> RoutingPolicy:
    """Load policy from environment with defaults matching historical hardcoded behavior."""
    return RoutingPolicy(
        per_edge_penalty_s=float(os.getenv("ROUTING_PER_EDGE_PENALTY_S", "10")),
        transfer_penalty_s=float(os.getenv("ROUTING_TRANSFER_PENALTY_S", "240")),
        transfer_distance_penalty_per_m_s=float(
            os.getenv("ROUTING_TRANSFER_DISTANCE_PENALTY_PER_M_S", "0")
        ),
        pattern_switch_penalty_s=float(os.getenv("ROUTING_PATTERN_SWITCH_PENALTY_S", "0")),
        frequency_discount_coef=float(os.getenv("ROUTING_FREQ_DISCOUNT_COEF", "0.01")),
        frequency_cap=float(os.getenv("ROUTING_FREQ_CAP", "60")),
        heuristic_max_speed_m_s=float(os.getenv("ROUTING_HEURISTIC_MAX_SPEED_M_S", "22.22")),
    )


def by_area_policy_profile() -> str:
    """Stable profile token used by `/detours/by-area` detour-answer cache keys."""
    return str(os.getenv("BY_AREA_ROUTING_POLICY_PROFILE", "weighted-v1")).strip() or "weighted-v1"


def default_by_area_routing_policy() -> RoutingPolicy:
    """
    By-area detour policy tuned for weighted goals:
    shorter feasible routes first, while discouraging transfer-heavy messy navigation.
    """
    return RoutingPolicy(
        per_edge_penalty_s=float(os.getenv("BY_AREA_ROUTING_PER_EDGE_PENALTY_S", "5")),
        transfer_penalty_s=float(os.getenv("BY_AREA_ROUTING_TRANSFER_PENALTY_S", "30")),
        transfer_distance_penalty_per_m_s=float(
            os.getenv("BY_AREA_ROUTING_TRANSFER_DISTANCE_PENALTY_PER_M_S", "0.05")
        ),
        pattern_switch_penalty_s=float(os.getenv("BY_AREA_ROUTING_PATTERN_SWITCH_PENALTY_S", "30")),
        frequency_discount_coef=float(os.getenv("BY_AREA_ROUTING_FREQ_DISCOUNT_COEF", "0.0")),
        frequency_cap=float(os.getenv("BY_AREA_ROUTING_FREQ_CAP", "60")),
        heuristic_max_speed_m_s=float(os.getenv("BY_AREA_ROUTING_HEURISTIC_MAX_SPEED_M_S", "22.22")),
    )
