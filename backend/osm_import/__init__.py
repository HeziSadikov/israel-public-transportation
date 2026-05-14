"""
Detour v3 OSM PBF import pipeline.

This package owns the full chain from an OSM PBF extract (e.g. Geofabrik
Israel-and-Palestine) to a directed, bus-aware routable graph in PostGIS:

* ``pbf_importer``         — pyosmium handler, writes raw osm_nodes / osm_ways
  / osm_way_nodes and parses turn-restriction relations into
  ``osm_turn_restrictions`` with v3 provenance.
* ``access_rules``         — pure tag → allow/deny helpers for highway and
  bus/psv/access tags.
* ``turn_restrictions``    — pure parsers for ``type=restriction`` relations.
* ``way_splitter``         — splits raw OSM ways at intersection / via nodes.
* ``build_directed_segments`` — writes directed rows to ``osm_road_segments``
  with bearings, length, full tags and v3 provenance.
* ``build_turn_table``     — M2 precomputed legal turn table (``osm_segment_turns``).

Detour v2 runtime behavior is not affected by this package.
"""

__all__: list[str] = []
