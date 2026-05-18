"""
Detour v3 OSM PBF importer.

Uses pyosmium to stream an OSM PBF extract (e.g. Geofabrik
Israel-and-Palestine) into Postgres/PostGIS:

  * ``osm_nodes``     — every referenced node (raw lon/lat).
  * ``osm_ways``      — ways whose ``highway=*`` is in the v1 allow-list (with
                        unknown highway classes admitted only when bus/psv/access
                        explicitly opens them).
  * ``osm_way_nodes`` — ordered ``(way_id, seq, node_id)`` for those ways.
  * ``osm_turn_restrictions`` — ``type=restriction`` (and ``restriction:bus``)
                        relations with v3 provenance
                        (``import_run_id`` + ``import_source``).

Way splitting + directed ``osm_road_segments`` is **M1C** and lives in
:mod:`backend.osm_import.way_splitter` / :mod:`build_directed_segments`.
M2 (legal turn table → ``osm_segment_turns``) reads what this importer wrote.

The importer never touches detour_v2 runtime behavior.

Re-runnability:
  * v3-only PK tables (``osm_nodes`` / ``osm_ways`` / ``osm_way_nodes``) use
    ``INSERT ... ON CONFLICT DO NOTHING``, so re-running without
    ``--reset-osm-import`` is safe but stale rows persist.
  * ``osm_turn_restrictions`` has no natural unique key; re-running without
    reset can produce duplicate v3-provenance rows. Use
    ``--reset-osm-import`` for a clean rebuild.

``--verify`` mode short-circuits all DB writes and only emits stats. Stats
shape (also stored in ``osm_import_runs.stats_json``):

    {
      "nodes_seen":         int,
      "nodes_written":      int,
      "ways_seen":          int,
      "ways_kept":          int,
      "ways_skipped":       int,
      "way_nodes_written":  int,
      "relations_seen":     int,
      "restrictions_kept":  int,
      "restrictions_skipped":int,
      "restriction_types":  {type_str: count},
      "phase_elapsed_s":    {phase: float},
      "verify_only":        bool,
    }
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json, execute_values

from backend.infra.logging_utils import log
from backend.infra.osm_import_db import IMPORT_SOURCE_V3
from backend.osm_import.access_rules import is_highway_candidate
from backend.osm_import.turn_restrictions import (
    RestrictionRow,
    parse_restriction_relation,
)


# Batch sizes for flushing to Postgres. PBF imports are I/O bound on the DB side;
# 50k rows per round-trip keeps memory bounded while letting psycopg2 batch well.
NODES_FLUSH_BATCH = 50_000
WAY_NODES_FLUSH_BATCH = 100_000
WAYS_FLUSH_BATCH = 10_000
RESTRICTIONS_FLUSH_BATCH = 2_000


# Tag columns we promote out of ``tags_json`` into typed columns on ``osm_ways``.
_PROMOTED_WAY_TAGS: Tuple[str, ...] = (
    "highway",
    "name",
    "oneway",
    "access",
    "bus",
    "psv",
    "service",
    "junction",
    "maxwidth",
    "maxheight",
)


@dataclass
class ImportStats:
    nodes_seen: int = 0
    nodes_written: int = 0
    ways_seen: int = 0
    ways_kept: int = 0
    ways_skipped: int = 0
    way_nodes_written: int = 0
    relations_seen: int = 0
    restrictions_kept: int = 0
    restrictions_skipped: int = 0
    restriction_types: Counter[str] = field(default_factory=Counter)
    phase_elapsed_s: Dict[str, float] = field(default_factory=dict)
    verify_only: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes_seen": self.nodes_seen,
            "nodes_written": self.nodes_written,
            "ways_seen": self.ways_seen,
            "ways_kept": self.ways_kept,
            "ways_skipped": self.ways_skipped,
            "way_nodes_written": self.way_nodes_written,
            "relations_seen": self.relations_seen,
            "restrictions_kept": self.restrictions_kept,
            "restrictions_skipped": self.restrictions_skipped,
            "restriction_types": dict(self.restriction_types),
            "phase_elapsed_s": dict(self.phase_elapsed_s),
            "verify_only": self.verify_only,
        }


def _safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ensure_osmium():
    try:
        import osmium  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "pyosmium is required for the detour v3 OSM PBF importer. "
            "Install with `pip install osmium` (or update requirements.txt and re-install)."
        ) from e
    return osmium


def resolve_pbf_path_for_pyosmium(pbf_path: Path) -> Path:
    """
    Return a path libosmium can open.

    On Windows, pyosmium fails to open PBF files when the path contains
    non-ASCII characters (e.g. a Hebrew user profile). Stage a copy under
    %TEMP% (ASCII short path) and reuse it while the source file is unchanged.
    """
    resolved = pbf_path.resolve()
    path_str = str(resolved)
    if sys.platform != "win32" or path_str.isascii():
        return resolved

    staging_dir = Path(tempfile.gettempdir()) / "israel_gtfs_osm"
    staging = staging_dir / resolved.name
    staging_dir.mkdir(parents=True, exist_ok=True)

    src_stat = resolved.stat()
    if staging.exists():
        st = staging.stat()
        if st.st_size == src_stat.st_size and st.st_mtime >= src_stat.st_mtime:
            log(
                "osm-pbf-import",
                f"using staged PBF {staging} (non-ASCII source path)",
            )
            return staging

    size_mb = src_stat.st_size / (1024 * 1024)
    log(
        "osm-pbf-import",
        (
            f"staging PBF to {staging} ({size_mb:.1f} MB); "
            "pyosmium on Windows cannot read non-ASCII paths"
        ),
    )
    shutil.copy2(resolved, staging)
    return staging


def run_pbf_import(
    conn,
    pbf_path: Path,
    *,
    import_run_id: int,
    verify: bool = False,
) -> ImportStats:
    """
    Stream ``pbf_path`` and write raw OSM tables + extended ``osm_turn_restrictions``.

    Parameters
    ----------
    conn : psycopg2 connection
        Caller owns the transaction. This function commits periodically (every
        flush) so a crash leaves a partial-but-consistent snapshot rather than
        rolling everything back.
    pbf_path : Path
        Path to the OSM PBF file.
    import_run_id : int
        ``osm_import_runs.id`` for this run; written to provenance columns on
        every ``osm_turn_restrictions`` row.
    verify : bool
        When True, no DB writes occur. Counts and per-phase elapsed are still
        produced.

    Returns
    -------
    ImportStats
        Counts + per-phase elapsed seconds.
    """
    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        raise FileNotFoundError(f"OSM PBF not found: {pbf_path}")
    pbf_osmium = resolve_pbf_path_for_pyosmium(pbf_path)

    osmium = _ensure_osmium()

    stats = ImportStats(verify_only=verify)

    # Buffers held in Python so we can flush in batches.
    nodes_buf: List[Tuple[int, float, float]] = []
    ways_buf: List[Tuple] = []
    way_nodes_buf: List[Tuple[int, int, int]] = []
    restrictions_buf: List[RestrictionRow] = []

    # Phase timings.
    t_phase_start = time.perf_counter()
    current_phase = {"name": "nodes"}

    def _bump_phase(name: str):
        nonlocal t_phase_start
        elapsed = time.perf_counter() - t_phase_start
        stats.phase_elapsed_s[current_phase["name"]] = (
            stats.phase_elapsed_s.get(current_phase["name"], 0.0) + elapsed
        )
        current_phase["name"] = name
        t_phase_start = time.perf_counter()

    # ---- DB flush helpers (no-ops in verify mode) -------------------------

    def _flush_nodes() -> None:
        if not nodes_buf:
            return
        n_before = len(nodes_buf)
        if not verify:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO osm_nodes (node_id, geom)
                    VALUES %s
                    ON CONFLICT (node_id) DO NOTHING
                    """,
                    nodes_buf,
                    template="(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
                    page_size=10_000,
                )
            conn.commit()
        stats.nodes_written += n_before
        nodes_buf.clear()

    def _flush_ways() -> None:
        if not ways_buf:
            return
        n_before = len(ways_buf)
        if not verify:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO osm_ways (
                        way_id, highway, name, oneway, access, bus, psv,
                        service, junction, maxwidth, maxheight, tags_json
                    )
                    VALUES %s
                    ON CONFLICT (way_id) DO NOTHING
                    """,
                    ways_buf,
                    page_size=2_000,
                )
            conn.commit()
        stats.ways_kept += n_before
        ways_buf.clear()

    def _flush_way_nodes() -> None:
        if not way_nodes_buf:
            return
        n_before = len(way_nodes_buf)
        if not verify:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO osm_way_nodes (way_id, seq, node_id)
                    VALUES %s
                    ON CONFLICT (way_id, seq) DO NOTHING
                    """,
                    way_nodes_buf,
                    page_size=20_000,
                )
            conn.commit()
        stats.way_nodes_written += n_before
        way_nodes_buf.clear()

    def _flush_restrictions() -> None:
        if not restrictions_buf:
            return
        n_before = len(restrictions_buf)
        if not verify:
            rows = [
                (
                    r.from_way_id,
                    # osm_turn_restrictions.via_node_id is NOT NULL in the legacy schema,
                    # so via-way-only restrictions store 0 there (with via_way_id set).
                    r.via_node_id if r.via_node_id is not None else 0,
                    r.to_way_id,
                    r.restriction_type,
                    Json(r.tags_json),
                    r.via_way_id,
                    r.applies_to_bus,
                    r.except_bus,
                    r.except_psv,
                    import_run_id,
                    IMPORT_SOURCE_V3,
                )
                for r in restrictions_buf
            ]
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO osm_turn_restrictions (
                        from_way_id, via_node_id, to_way_id, restriction_type,
                        tags_json, via_way_id, applies_to_bus,
                        except_bus, except_psv,
                        import_run_id, import_source
                    )
                    VALUES %s
                    """,
                    rows,
                    page_size=500,
                )
            conn.commit()
        stats.restrictions_kept += n_before
        restrictions_buf.clear()

    # ---- pyosmium handler -------------------------------------------------

    class _Handler(osmium.SimpleHandler):  # type: ignore[misc]
        def node(self, n: Any) -> None:
            stats.nodes_seen += 1
            try:
                loc = n.location
                if not loc.valid():
                    return
                nodes_buf.append((int(n.id), float(loc.lon), float(loc.lat)))
            except Exception:
                return
            if len(nodes_buf) >= NODES_FLUSH_BATCH:
                _flush_nodes()

        def way(self, w: Any) -> None:
            stats.ways_seen += 1
            if current_phase["name"] != "ways":
                _bump_phase("ways")
            try:
                tags = {str(t.k): str(t.v) for t in w.tags}
            except Exception:
                tags = {}
            highway = tags.get("highway")
            if not is_highway_candidate(highway, tags):
                stats.ways_skipped += 1
                return

            try:
                node_refs = [int(nr.ref) for nr in w.nodes]
            except Exception:
                node_refs = []
            if len(node_refs) < 2:
                stats.ways_skipped += 1
                return

            way_id = int(w.id)
            ways_buf.append(
                (
                    way_id,
                    tags.get("highway"),
                    tags.get("name"),
                    tags.get("oneway"),
                    tags.get("access"),
                    tags.get("bus"),
                    tags.get("psv"),
                    tags.get("service"),
                    tags.get("junction"),
                    _safe_float(tags.get("maxwidth")),
                    _safe_float(tags.get("maxheight")),
                    Json(tags),
                )
            )
            for seq, node_id in enumerate(node_refs):
                way_nodes_buf.append((way_id, seq, node_id))

            if len(ways_buf) >= WAYS_FLUSH_BATCH:
                _flush_ways()
            if len(way_nodes_buf) >= WAY_NODES_FLUSH_BATCH:
                _flush_way_nodes()

        def relation(self, r: Any) -> None:
            stats.relations_seen += 1
            if current_phase["name"] != "relations":
                # Flush remaining ways/way_nodes before switching phase so
                # phase_elapsed_s reflects real workload boundaries.
                _flush_ways()
                _flush_way_nodes()
                _bump_phase("relations")
            try:
                tags = {str(t.k): str(t.v) for t in r.tags}
            except Exception:
                tags = {}
            rel_type = tags.get("type")
            if rel_type not in ("restriction", "restriction:bus"):
                return

            members_view = [
                (
                    _mem_type(m),
                    int(m.ref),
                    str(getattr(m, "role", "") or ""),
                )
                for m in r.members
            ]
            row = parse_restriction_relation(rel_type, tags, members_view)
            if row is None:
                stats.restrictions_skipped += 1
                return
            stats.restriction_types[row.restriction_type] += 1
            restrictions_buf.append(row)
            if len(restrictions_buf) >= RESTRICTIONS_FLUSH_BATCH:
                _flush_restrictions()

    def _mem_type(m: Any) -> str:
        t = getattr(m, "type", "")
        s = str(t).strip().lower()
        if s == "n":
            return "node"
        if s == "w":
            return "way"
        if s == "r":
            return "relation"
        return s

    # ---- Drive the parse --------------------------------------------------

    log(
        "osm-pbf-import",
        f"start pbf={pbf_path} osmium_path={pbf_osmium} run_id={import_run_id} verify={verify}",
    )
    handler = _Handler()
    handler.apply_file(str(pbf_osmium))

    # Final flushes after the file is fully consumed.
    _flush_nodes()
    _flush_ways()
    _flush_way_nodes()
    _flush_restrictions()
    _bump_phase("done")

    log(
        "osm-pbf-import",
        (
            "done "
            f"nodes_seen={stats.nodes_seen} nodes_written={stats.nodes_written} "
            f"ways_seen={stats.ways_seen} ways_kept={stats.ways_kept} "
            f"ways_skipped={stats.ways_skipped} way_nodes={stats.way_nodes_written} "
            f"relations_seen={stats.relations_seen} restrictions_kept={stats.restrictions_kept} "
            f"restrictions_skipped={stats.restrictions_skipped} verify={verify}"
        ),
    )
    return stats


__all__ = ["run_pbf_import", "ImportStats"]
