# Detour v2 engine

Road-graph-first detour computation for Israeli buses (`standard_plus_bus`).

## Relationship to legacy flow

- **Legacy** (`/api/v1/detours/by-area`, `detour_graph.py`): GTFS pattern graph + optional Valhalla hybrid; caches in `detour_by_area_cache`. Responses include `diagnostics.detour_engine` (`v1`|`v2`|`v3`, from `DETOUR_ENGINE` / request) for observability; per-trip bus-quality detours use `POST /api/v1/detours/compute`.
- **v2** (`/api/v1/detours/compute`, default when `DETOUR_ENGINE=v2` or `compute_engine=v2`): planned-service context from GTFS shapes/stops, Valhalla road candidates between anchors, feasibility on decoded segments/turns, explainable ranking; persistence in `detour_requests` / `detour_candidates` / `approved_detours`.
- **v3** (`backend/detour_v3`, selected when `DETOUR_ENGINE=v3` / `v3_default` or `compute_engine=v3`): PostGIS `pattern_osm_segments` + `osm_segment_turns` + A* avoidance routing; Valhalla is offline-only for map-matching in `match_patterns_to_osm`. **`pattern_osm_segments` covers the full representative GTFS shape** (`repr_shape_id`); stop_times do not bound corridor coverage. Trip impact and v3 anchors both use the same shape geometry.

## Module map

| Module | Role |
|--------|------|
| `policy.py` | `DetourPolicyConfig` — tunable thresholds (versioned). Includes `SearchPolicy`. |
| `models.py` | Request/response dataclasses (anchors, candidates, scores). |
| `trip_impact_analyzer.py` | Shape vs polygon → blocked interval along shape. |
| `anchor_selector.py` | Exit/rejoin anchors (stops + shape, policy-driven). Multiple candidate pairs. |
| `incident_projector.py` | Polygon → temporary edge/turn bans (PostGIS when OSM segments exist). |
| `corridor_builder.py` | Narrow/medium/wide buffers around affected shape segment. |
| `road_candidate_generator.py` | Multiple Valhalla strategies (`bus` costing). |
| `candidate_decoder.py` | Route geometry → `RoadSegmentRef` / `TurnRef` (trace_attributes first, DB proximity fallback, then synthetic). |
| `bus_feasibility_evaluator.py` | Scorer: only four absolute hard rejects (blockage overlap, wrong-direction rejoin, invalid geometry, stitch order); everything else is penalty + warnings + confidence. |
| `tier_classifier.py` | Deterministic `AUTO_OK` / `REVIEW_RECOMMENDED` / `LOW_CONFIDENCE` (+ orchestrator `EMERGENCY_FALLBACK`). |
| `detour_ranker.py` | Score + deterministic winner. Full `score_breakdown` always populated. |
| `detour_memory.py` | Persist requests/candidates; evidence updates on approve. |
| `compute.py` | Orchestrates the pipeline. Parallel Valhalla calls (ThreadPoolExecutor), anchor rescue, per-trip deadline, via-stop insertion. |
| `serialize.py` | Produces API dict including `attempts[]`, `score_breakdown`, `summary_en`. |

## Debug / policy endpoint

`GET /api/v1/detours/policy` — returns the live `DetourPolicyConfig` as JSON.

## Environment variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DETOUR_V2_ENABLED` | `true` | Enable v2 HTTP endpoints. |
| `DETOUR_ENGINE` | `v2` | `v1` \| `v2` \| `v3` \| `v3_default` — default per-trip engine when JSON `compute_engine` is omitted (`v3` / `v3_default` ⇒ detour_v3 graph). Legacy by-area tagging still distinguishes `v1` vs richer modes. |
| `DETOUR_V3_ENABLED` | `true` | When false, `/detours/compute` never runs v3 (falls back to v2). |
| `DETOUR_V3_IMPORT_RUN_ID` | — | Optional: restrict router load to segments from one provenance run id. |
| `DETOUR_V3_COST_MODE` | `bus_corridor_plus_connectors` | `strict_bus_corridor` or plus-connectors softer penalties. |
| `DETOUR_POLICY_JSON` | — | Path to JSON file overriding policy fields. |
| `VALHALLA_URL` | — | Valhalla base URL (e.g. `http://valhalla:8002`). |
| `VALHALLA_LOCATION_RADIUS_M` | `90` | Snap radius for route endpoints (reduces 442). |
| `VALHALLA_MANEUVER_PENALTY_S` | `45` | Penalty (s) discouraging awkward maneuvers. |
| `VALHALLA_SERVICE_PENALTY_S` | `30` | Penalty (s) for service road usage. |
| `VALHALLA_USE_LIVING_STREETS` | `0` | `use_living_streets` in bus costing (0=avoid). |
| `VALHALLA_USE_TRACKS` | `0` | `use_tracks` in bus costing (0=avoid). |
| `VALHALLA_SERVICE_FACTOR` | `1.5` | `service_factor` in bus costing. |
| `VALHALLA_PRIVATE_ACCESS_PENALTY` | `600` | Penalty (s) for private-access roads. |
| `VALHALLA_COUNTRY_CROSSING_PENALTY` | `600` | Penalty (s) for country crossings. |
| `VALHALLA_TRACE_ATTRIBUTES_ENABLED` | `true` | Use `/trace_attributes` for exact OSM way IDs. |
| `VALHALLA_CIRCUIT_FAIL_THRESHOLD` | `5` | Open circuit breaker after N consecutive failures. |
| `VALHALLA_CIRCUIT_COOLDOWN_S` | `30` | Seconds to keep circuit breaker open. |

**GTFS bus-way evidence:** `gtfs_bus_way_evidence` is populated during GTFS ingest (`ingest_gtfs_postgis.py`, shape-buffer proximity to `osm_road_segments`). Detour logs use `ways_with_gtfs_evidence_table_hit` = count of decoded OSM way IDs that return a row for the active `feed_id`. Zeros usually mean the table is empty for that feed, not that buses avoid those roads. Physical matching lives in `pattern_edge_match` (separate counts in `incident_projection` stage logs).

Policy sub-section overrides (via `DETOUR_POLICY_JSON`):
- `anchor.candidate_pairs_k` — how many exit/rejoin pairs to evaluate.
- `anchor.rescue_stops_per_side` — wider search when first sweep fails.
- `search.valhalla_concurrency` — parallel Valhalla threads per trip.
- `search.early_accept_score` — stop evaluating if score ≤ this (narrow corridor only).
- `search.per_trip_deadline_ms` — maximum ms before returning best-so-far.
- `service.via_stop_corridor_m` — radius for via-stop insertion.
- `service.sharp_turn_threshold_deg` — bearing delta for "sharp" turns.



## Data flow (high level)

1. Load trip shape + stop times → **TripImpactAnalyzer** → blocked interval.
2. **AnchorSelector** → exit/rejoin (lon/lat) snapped from shape/stops.
3. **IncidentProjector** → request-scoped banned segment ids / turn triplets.
4. **CorridorBuilder** → polygon corridors (narrow → wide).
5. **RoadCandidateGenerator** → N Valhalla routes avoiding blockage.
6. **CandidateDecoder** → segment/turn sequences for feasibility.
7. **BusFeasibilityEvaluator** + **DetourRanker** → union of candidates across anchor radii × corridors; top-3 tiered, or **emergency fallback** geometry if none rank.
8. **DetourMemory** → optional DB persistence.

OSM segment/turn tables (`osm_road_segments`, `osm_turn_restrictions`) are populated separately; when empty, decoding uses synthetic segment refs along the polyline so the pipeline still runs.
