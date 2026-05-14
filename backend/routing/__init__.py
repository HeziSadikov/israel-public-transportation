"""
Detour v3 own-graph routing (M2+: segment graph; M3+: polygon overlays).

Exports the small public surface needed by CLIs / detour_v3 orchestration later.
"""

from backend.routing.astar import astar_shortest_path
from backend.routing.blockers import (
    BlockageBans,
    load_polygon_wkt_from_geojson_file,
    polygon_geojson_feature_to_wkt,
    project_polygon_to_bans,
    route_segments_avoid_polygon,
)
from backend.routing.costs import (
    DEFAULT_ROUTING_COST_PROFILE,
    RoutingCostProfile,
)
from backend.routing.road_graph_loader import (
    RoadGraph,
    RoadSegment,
    TurnMove,
    load_road_graph,
)

__all__ = [
    "astar_shortest_path",
    "BlockageBans",
    "project_polygon_to_bans",
    "polygon_geojson_feature_to_wkt",
    "load_polygon_wkt_from_geojson_file",
    "route_segments_avoid_polygon",
    "RoadGraph",
    "RoadSegment",
    "TurnMove",
    "load_road_graph",
    "RoutingCostProfile",
    "DEFAULT_ROUTING_COST_PROFILE",
]
