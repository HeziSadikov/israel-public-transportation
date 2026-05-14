"""A* over directed road segments legalized by ``osm_segment_turns``."""

from __future__ import annotations

import heapq
from collections.abc import Set as AbstractSet
from typing import Callable, Dict, List, Optional, Tuple

from backend.routing.costs import (
    DEFAULT_ROUTING_COST_PROFILE,
    RoutingCostProfile,
    edge_cost,
    haversine_m,
    move_cost_m,
)
from backend.routing.road_graph_loader import RoadGraph, RoadSegment

HeuristicFn = Callable[[int], float]


def _default_heuristic(graph: RoadGraph, goal_node_id: int) -> HeuristicFn:
    g_lonlat = graph.node_geom.get(goal_node_id)
    if g_lonlat is None:
        return lambda _seg_id: 0.0
    glo, gla = g_lonlat

    def h(seg_id: int) -> float:
        cur = graph.segments.get(seg_id)
        if cur is None:
            return 0.0
        tn = graph.node_geom.get(cur.to_node_id)
        if tn is None:
            return 0.0
        clo, cla = tn
        return haversine_m(clo, cla, glo, gla)

    return h


def astar_shortest_path(
    graph: RoadGraph,
    start_node_id: int,
    goal_node_id: int,
    *,
    profile: RoutingCostProfile = DEFAULT_ROUTING_COST_PROFILE,
    heuristic: Optional[HeuristicFn] = None,
    banned_segment_ids: Optional[AbstractSet[int]] = None,
    banned_turn_pairs: Optional[AbstractSet[tuple[int, int]]] = None,
    segment_enter_extra_cost_m: Optional[Callable[[int], float]] = None,
    edge_cost_fn: Optional[Callable[[RoadSegment], float]] = None,
) -> Optional[List[int]]:
    """
    Return ordered segment ids for a path ``start_node`` → ``goal_node``.

    Follows successors from :class:`~RoadGraph`. ``None`` when unreachable.

    * ``banned_segment_ids`` — omit these directed segments everywhere
      (starting, traversing).
    * ``banned_turn_pairs`` — forbid move ``(from_segment_id,
      to_segment_id)``. Used with M3 polygon overlays (junction pivots).
    * ``segment_enter_extra_cost_m`` — optional additive meters when *entering*
      a successor segment (for bus-corridor priors); return ≥1e18 to skip.
    * ``edge_cost_fn`` — optional override for the cost of traversing the
      *current* segment edge (default: ``edge_cost(length_m)``).
    Tie-break deterministic using ``tie`` ordering in the priority queue heap.
    """
    if start_node_id == goal_node_id:
        return []

    ban_seg = banned_segment_ids or frozenset()
    ban_turn = banned_turn_pairs or frozenset()
    extra_fn = segment_enter_extra_cost_m
    use_edge_fn = edge_cost_fn

    starts = [
        sid
        for sid in graph.starts_from_node(start_node_id)
        if sid not in ban_seg
    ]
    if not starts:
        return None

    h_fun = heuristic or _default_heuristic(graph, goal_node_id)

    tie = 0
    pq: List[Tuple[float, int, float, int]] = []
    g_score: Dict[int, float] = {}
    came: Dict[int, int] = {}

    for sid in starts:
        seg = graph.segments[sid]
        gg = use_edge_fn(seg) if use_edge_fn is not None else edge_cost(seg.length_m)
        est_extra = 0.0
        if extra_fn is not None:
            try:
                est_extra = float(extra_fn(sid))
            except Exception:
                est_extra = 0.0
        if est_extra >= 1e17:
            continue
        gg += est_extra
        ff = gg + h_fun(sid)
        heapq.heappush(pq, (ff, tie, gg, sid))
        tie += 1
        g_score[sid] = gg

    while pq:
        _f, _bk, g_cur, sid = heapq.heappop(pq)
        if g_cur > g_score.get(sid, float("inf")) + 1e-9:
            continue

        if sid in ban_seg:
            continue

        seg = graph.segments[sid]
        if seg.to_node_id == goal_node_id:
            seq_rev: List[int] = []
            cur = sid
            while True:
                seq_rev.append(cur)
                p = came.get(cur)
                if p is None:
                    break
                cur = p
            return list(reversed(seq_rev))

        for mv in graph.successors_of(sid):
            cand = mv.to_segment_id
            if cand in ban_seg:
                continue
            if (sid, cand) in ban_turn:
                continue
            step_cost = move_cost_m(
                graph.segments[sid].length_m,
                mv.turn_angle_deg,
                profile=profile,
            )
            enter_extra = 0.0
            if extra_fn is not None:
                try:
                    enter_extra = float(extra_fn(cand))
                except Exception:
                    enter_extra = 0.0
            if enter_extra >= 1e17:
                continue
            tentative_g = g_cur + step_cost + enter_extra

            prev_cost = g_score.get(cand)
            if prev_cost is None or tentative_g < prev_cost:
                came[cand] = sid
                g_score[cand] = tentative_g
                f = tentative_g + h_fun(cand)
                heapq.heappush(pq, (f, tie, tentative_g, cand))
                tie += 1

    return None


__all__ = ["astar_shortest_path"]
