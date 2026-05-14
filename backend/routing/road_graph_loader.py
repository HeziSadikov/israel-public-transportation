"""Load directed segments + legal turns into an in-memory router graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence

from psycopg2.extensions import connection as PGConnection


@dataclass(frozen=True, slots=True)
class RoadSegment:
    segment_id: int
    from_node_id: int
    to_node_id: int
    length_m: float
    highway: Optional[str]


@dataclass(frozen=True, slots=True)
class TurnMove:
    to_segment_id: int
    turn_angle_deg: Optional[float]


@dataclass
class RoadGraph:
    segments: Dict[int, RoadSegment]
    node_geom: Dict[int, tuple[float, float]]
    successors: Mapping[int, Sequence[TurnMove]] = field(
        repr=False,
        default_factory=dict,
    )

    def successors_of(self, seg_id: int) -> Sequence[TurnMove]:
        return self.successors.get(seg_id, ())

    def starts_from_node(self, node_id: int) -> List[int]:
        """Directed segments whose traversal begins at ``node_id``."""
        out = [s.segment_id for s in self.segments.values() if s.from_node_id == node_id]
        out.sort()
        return out


def load_road_graph(
    conn: PGConnection,
    *,
    segment_import_run_id: Optional[int] = None,
) -> RoadGraph:
    """
    Populate a :class:`RoadGraph` from ``osm_road_segments`` and every
    non-forbidden edge in ``osm_segment_turns``.

    Node coordinates come from ``osm_nodes`` (used for heuristic only).

    * ``segment_import_run_id`` — when set, only segments belonging to this
      run are loaded AND only turns referencing those segment ids survive.
      ``None`` = full graph matching ``NOT is_forbidden`` turns globally.
    """
    segments: Dict[int, RoadSegment] = {}
    node_ids_need: MutableMapping[int, bool] = {}

    with conn.cursor() as cur:
        if segment_import_run_id is not None:
            cur.execute(
                """
                SELECT
                  s.segment_id,
                  s.from_node_id,
                  s.to_node_id,
                  COALESCE(s.length_m, 1)::double precision AS length_m,
                  s.highway
                FROM osm_road_segments s
                WHERE s.import_run_id = %s
                """,
                (segment_import_run_id,),
            )
        else:
            cur.execute(
                """
                SELECT
                  s.segment_id,
                  s.from_node_id,
                  s.to_node_id,
                  COALESCE(s.length_m, 1)::double precision AS length_m,
                  s.highway
                FROM osm_road_segments s
                """
            )

        for row in cur.fetchall() or []:
            try:
                sid = int(row[0])
                fn = int(row[1])
                tn = int(row[2])
                lm = float(row[3])
                hw = row[4]
            except Exception:
                sid = int(row["segment_id"])
                fn = int(row["from_node_id"])
                tn = int(row["to_node_id"])
                lm = float(row["length_m"])
                hw = row.get("highway")
            hw_s = None if hw is None else str(hw)
            segments[sid] = RoadSegment(sid, fn, tn, lm, hw_s)
            node_ids_need[fn] = True
            node_ids_need[tn] = True

        node_geom: Dict[int, tuple[float, float]] = {}
        if node_ids_need:
            cur.execute(
                """
                SELECT node_id,
                       ST_X(geom)::double precision AS lon,
                       ST_Y(geom)::double precision AS lat
                FROM osm_nodes
                WHERE node_id IN %s
                """,
                (tuple(sorted(node_ids_need.keys())),),
            )
            for row in cur.fetchall() or []:
                try:
                    nid = int(row[0])
                    lon = float(row[1])
                    lat = float(row[2])
                except Exception:
                    nid = int(row["node_id"])
                    lon = float(row["lon"])
                    lat = float(row["lat"])
                node_geom[nid] = (lon, lat)

        allowed_ids_tuple = tuple(segments.keys()) if segments else ()
        succ: Dict[int, List[TurnMove]] = {}

        def _consume_turn_rows(rows):
            for row in rows or []:
                try:
                    f = int(row[0])
                    t = int(row[1])
                    ang = row[2]
                except Exception:
                    f = int(row["from_segment_id"])
                    t = int(row["to_segment_id"])
                    ang = row.get("turn_angle_deg")
                if f not in segments or t not in segments:
                    continue
                ang_f = None if ang is None else float(ang)
                succ.setdefault(f, []).append(TurnMove(t, ang_f))

        if segment_import_run_id is not None:
            cur.execute(
                """
                SELECT t.from_segment_id, t.to_segment_id, t.turn_angle_deg
                FROM osm_segment_turns t
                INNER JOIN osm_road_segments s1
                  ON s1.segment_id = t.from_segment_id
                 AND s1.import_run_id = %s
                INNER JOIN osm_road_segments s2
                  ON s2.segment_id = t.to_segment_id
                 AND s2.import_run_id = %s
                WHERE NOT t.is_forbidden
                """,
                (segment_import_run_id, segment_import_run_id),
            )
            _consume_turn_rows(cur.fetchall())
        elif allowed_ids_tuple:
            tset = tuple(sorted(allowed_ids_tuple))
            cur.execute(
                """
                SELECT from_segment_id, to_segment_id, turn_angle_deg
                FROM osm_segment_turns
                WHERE NOT is_forbidden
                  AND from_segment_id IN %s
                  AND to_segment_id IN %s
                """,
                (tset, tset),
            )
            _consume_turn_rows(cur.fetchall())

        for k in list(succ.keys()):
            succ[k] = sorted(succ[k], key=lambda x: x.to_segment_id)

    successors_map = {k: tuple(v) for k, v in succ.items()}
    return RoadGraph(
        segments=segments,
        node_geom=node_geom,
        successors=successors_map,
    )


__all__ = ["RoadSegment", "TurnMove", "RoadGraph", "load_road_graph"]
