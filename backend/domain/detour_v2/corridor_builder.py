"""Buffered corridors around the affected shape segment."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from shapely.geometry import LineString, mapping, shape
from shapely.ops import substring, unary_union

from .models import AnchorPair
from .policy import DetourPolicyConfig

CorridorStage = Literal["narrow", "medium", "wide"]


def affected_shape_subline(
    line: LineString,
    exit_dist_m: float,
    rejoin_dist_m: float,
    total_m: float,
) -> LineString:
    """Clip line to [exit_dist_m, rejoin_dist_m] along approximate normalized distance."""
    if total_m <= 0:
        return line
    t0 = max(0.0, min(1.0, exit_dist_m / total_m))
    t1 = max(0.0, min(1.0, rejoin_dist_m / total_m))
    if t1 <= t0:
        t1 = min(1.0, t0 + 1e-6)
    try:
        return substring(line, t0, t1, normalized=True)
    except Exception:
        return line


def incident_exclusion_polygon_for_stage(
    blockage_geojson: Dict[str, Any],
    stage: CorridorStage,
    policy: DetourPolicyConfig,
    *,
    line: Optional[LineString] = None,
    anchors: Optional[AnchorPair] = None,
    shape_length_m: float = 0.0,
) -> Dict[str, Any]:
    """
    Polygon sent to Valhalla as exclude_polygons: incident geometry expanded by a modest buffer.
    Optionally intersect with a tube along the nominal route between anchors so huge user AOIs
    do not exclude entire parallel street grids (442).
    """
    buf_m = {
        "narrow": policy.corridor.valhalla_exclude_buffer_narrow_m,
        "medium": policy.corridor.valhalla_exclude_buffer_medium_m,
        "wide": policy.corridor.valhalla_exclude_buffer_wide_m,
    }[stage]
    try:
        g = shape(blockage_geojson)
    except Exception:
        return blockage_geojson
    if g.is_empty:
        return blockage_geojson
    deg = max(float(buf_m), 1.0) / 111_000.0
    try:
        buffered = g.buffer(deg)
    except Exception:
        return blockage_geojson

    if (
        line is not None
        and anchors is not None
        and float(shape_length_m) > 0.0
        and not line.is_empty
    ):
        try:
            sub = affected_shape_subline(
                line,
                anchors.exit_shape_dist_m,
                anchors.rejoin_shape_dist_m,
                shape_length_m,
            )
            clip_m = max(50.0, float(policy.corridor.valhalla_exclude_clip_corridor_m))
            clip_deg = clip_m / 111_000.0
            tube = sub.buffer(clip_deg)
            clipped = buffered.intersection(tube)
            if not clipped.is_empty:
                if clipped.geom_type in ("Polygon", "MultiPolygon"):
                    return mapping(clipped)
                if clipped.geom_type == "GeometryCollection":
                    polys = [
                        x
                        for x in clipped.geoms
                        if getattr(x, "geom_type", "") in ("Polygon", "MultiPolygon")
                    ]
                    if polys:
                        u = unary_union(polys)
                        if not u.is_empty and u.geom_type in ("Polygon", "MultiPolygon"):
                            return mapping(u)
        except Exception:
            pass

    try:
        return mapping(buffered)
    except Exception:
        return blockage_geojson


def corridor_polygon_for_stage(
    line: LineString,
    anchors: AnchorPair,
    total_m: float,
    stage: CorridorStage,
    policy: DetourPolicyConfig,
) -> Dict[str, Any]:
    sub = affected_shape_subline(line, anchors.exit_shape_dist_m, anchors.rejoin_shape_dist_m, total_m)
    buf_m = {
        "narrow": policy.corridor.narrow_buffer_m,
        "medium": policy.corridor.medium_buffer_m,
        "wide": policy.corridor.wide_buffer_m,
    }[stage]
    # degrees approx: 1 deg lat ~ 111km
    deg = buf_m / 111_000.0
    try:
        poly = sub.buffer(deg)
        return mapping(poly)
    except Exception:
        return mapping(sub.buffer(deg))


def corridor_stages_order() -> List[CorridorStage]:
    return ["narrow", "medium", "wide"]
