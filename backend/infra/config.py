from pathlib import Path
import os
from typing import Optional


BASE_DIR = Path(__file__).resolve().parents[2]


def parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def parse_csv_env(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv)
    return [p.strip() for p in str(raw).split(",") if p.strip()]

# Local GTFS zip for offline/dev
LOCAL_GTFS_ZIP = BASE_DIR / "israel-public-transportation.zip"

# GTFS data directory for downloaded feeds
GTFS_DATA_DIR = BASE_DIR / "data" / "gtfs"
GTFS_DATA_DIR.mkdir(parents=True, exist_ok=True)

FEED_METADATA_PATH = GTFS_DATA_DIR / "feed_version.json"

# Remote GTFS base URL
GTFS_REMOTE_BASE = os.getenv(
    "GTFS_REMOTE_BASE",
    "https://gtfs.mot.gov.il/gtfsfiles",
)
GTFS_REMOTE_FILENAME = os.getenv(
    "GTFS_REMOTE_FILENAME",
    "israel-public-transportation.zip",
)

# OSM engine (OSRM / Valhalla) base URL — used for map-matching (OSRM)
OSM_ENGINE_URL = os.getenv("OSM_ENGINE_URL", "http://localhost:5000")

# Detour v3: OSM PBF importer.
# OSM_PBF_PATH points at the local extract that the pyosmium importer reads.
# OSM_PBF_URL is used by --fetch / --fetch-if-newer to refresh that file.
OSM_PBF_PATH = Path(os.getenv("OSM_PBF_PATH", str(BASE_DIR / "osm" / "israel.osm.pbf")))
OSM_PBF_URL = os.getenv(
    "OSM_PBF_URL",
    "https://download.geofabrik.de/asia/israel-and-palestine-latest.osm.pbf",
)

# Valhalla base URL — used for detour routing (route avoiding polygon).
# If set, /detour will use Valhalla to route around the blocked area on the road network.
# Example: http://localhost:8002 (Valhalla default port)
VALHALLA_URL = os.getenv("VALHALLA_URL", "")
# Meters: search radius to snap /route endpoints to the road graph (helps 442 near stops). 0 = omit (Valhalla default).
VALHALLA_LOCATION_RADIUS_M = max(0, parse_int_env("VALHALLA_LOCATION_RADIUS_M", 90))
# When set, prefer tighter snap on break locations if heading is provided (divided roads).
VALHALLA_HEADING_SNAP_RADIUS_M = max(0, parse_int_env("VALHALLA_HEADING_SNAP_RADIUS_M", 45))
# Degrees: Valhalla location heading tolerance (snaps to edges aligned with this direction).
VALHALLA_HEADING_TOLERANCE_DEG = max(1, parse_int_env("VALHALLA_HEADING_TOLERANCE_DEG", 60))
# Physical layer: GTFS pattern edges matched to OSM (PostGIS); when true, prefer matched geometry for impact/anchors if backfilled.
USE_MATCHED_PHYSICAL_GEOMETRY = parse_bool_env("USE_MATCHED_PHYSICAL_GEOMETRY", False)
# Prefer precomputed legal divergence/rejoin anchors from pattern_legal_anchor_candidate when rows exist.
LEGAL_ANCHOR_INDEX_ENABLED = parse_bool_env("LEGAL_ANCHOR_INDEX_ENABLED", True)
# Rows in pattern_legal_anchor_candidate / pattern_status are keyed by this version (must match precompute script).
LEGAL_ANCHOR_INDEX_ANCHOR_VERSION = os.getenv("LEGAL_ANCHOR_INDEX_ANCHOR_VERSION", "legal_anchor_v1").strip() or "legal_anchor_v1"
# Thresholds (mirrored in DetourPolicyConfig.physical_path; env wins at runtime via policy loader).
PHYSICAL_PATH_MIN_TRIP_COVERAGE_RATIO = parse_float_env("PHYSICAL_PATH_MIN_TRIP_COVERAGE_RATIO", 0.72)
PHYSICAL_PATH_MAX_AMBIGUOUS_STOP_PAIRS = max(0, parse_int_env("PHYSICAL_PATH_MAX_AMBIGUOUS_STOP_PAIRS", 2))
PHYSICAL_PATH_MIN_SUMMARY_CONFIDENCE = parse_float_env("PHYSICAL_PATH_MIN_SUMMARY_CONFIDENCE", 0.35)
PHYSICAL_PATH_MAX_MEAN_OFFSET_M = parse_float_env("PHYSICAL_PATH_MAX_MEAN_OFFSET_M", 35.0)
PHYSICAL_PATH_ANCHOR_MIN_COVERAGE_RATIO = parse_float_env("PHYSICAL_PATH_ANCHOR_MIN_COVERAGE_RATIO", 0.72)
PHYSICAL_PATH_MAX_WEAK_STOP_PAIRS = max(0, parse_int_env("PHYSICAL_PATH_MAX_WEAK_STOP_PAIRS", 8))
PHYSICAL_PATH_HARD_REJECT_WRONG_ENTRY_EXIT_SEGMENT = parse_bool_env(
    "PHYSICAL_PATH_HARD_REJECT_WRONG_ENTRY_EXIT_SEGMENT", True
)
DETOUR_V2_TIMING_LOG = parse_bool_env("DETOUR_V2_TIMING_LOG", False)
# Emit structured detours/v2/compute_ai log lines for every /detours/compute call (no automatic GeoJSON; use request detour_debug for that).
DETOUR_V2_DEBUG = parse_bool_env("DETOUR_V2_DEBUG", False)
# Deprecated alias for DETOUR_V2_DEBUG (AI log half only).
DETOUR_V2_AI_LOG = parse_bool_env("DETOUR_V2_AI_LOG", False)
VALIDATE_DETOUR_CARRIAGEWAY = parse_bool_env("VALIDATE_DETOUR_CARRIAGEWAY", True)
STRICT_ANCHOR_SNAPPING = parse_bool_env("STRICT_ANCHOR_SNAPPING", False)
PREFER_BUS_SERVED_OSM = parse_bool_env("PREFER_BUS_SERVED_OSM", False)
# Stop coordinate may replace shape-interpolated anchor if projected distance to shape is below this (meters).
ANCHOR_STOP_MAX_PROJECTION_M = max(5, parse_int_env("ANCHOR_STOP_MAX_PROJECTION_M", 85))
# Seconds-equivalent penalties used in Valhalla costing_options to reduce awkward maneuvers.
VALHALLA_MANEUVER_PENALTY_S = max(0, parse_int_env("VALHALLA_MANEUVER_PENALTY_S", 45))
VALHALLA_SERVICE_PENALTY_S = max(0, parse_int_env("VALHALLA_SERVICE_PENALTY_S", 30))
# Extended bus costing options (Phase A2).
VALHALLA_USE_LIVING_STREETS = max(0, parse_int_env("VALHALLA_USE_LIVING_STREETS", 0))
VALHALLA_USE_TRACKS = max(0, parse_int_env("VALHALLA_USE_TRACKS", 0))
VALHALLA_SERVICE_FACTOR = float(os.getenv("VALHALLA_SERVICE_FACTOR", "1.5"))
VALHALLA_PRIVATE_ACCESS_PENALTY = max(0, parse_int_env("VALHALLA_PRIVATE_ACCESS_PENALTY", 600))
VALHALLA_COUNTRY_CROSSING_PENALTY = max(0, parse_int_env("VALHALLA_COUNTRY_CROSSING_PENALTY", 600))
# Use /trace_attributes to get exact OSM way IDs from Valhalla route geometry (Phase A1).
VALHALLA_TRACE_ATTRIBUTES_ENABLED = parse_bool_env("VALHALLA_TRACE_ATTRIBUTES_ENABLED", True)
# Valhalla circuit breaker: open after N consecutive transport failures within WINDOW_S seconds.
VALHALLA_CIRCUIT_FAIL_THRESHOLD = max(1, parse_int_env("VALHALLA_CIRCUIT_FAIL_THRESHOLD", 5))
VALHALLA_CIRCUIT_COOLDOWN_S = max(1, parse_int_env("VALHALLA_CIRCUIT_COOLDOWN_S", 30))
HYBRID_DETOUR_ENABLED = parse_bool_env("HYBRID_DETOUR_ENABLED", True)
DETOUR_ALLOW_FEED_FALLBACK = parse_bool_env("DETOUR_ALLOW_FEED_FALLBACK", False)
# Higher k pulls more pattern variants per route into the detour graph (better urban coverage, more CPU).
DETOUR_TOP_K_PATTERNS = max(1, parse_int_env("DETOUR_TOP_K_PATTERNS", 3))
# Detour spatial selector uses a dedicated K so it can be wider than generic pattern selection.
DETOUR_TOP_K_PATTERNS_SPATIAL = max(1, parse_int_env("DETOUR_TOP_K_PATTERNS_SPATIAL", 8))
# Ignore tiny AOI touches; 0 keeps all intersections.
DETOUR_SPATIAL_MIN_OVERLAP_M = max(0, parse_int_env("DETOUR_SPATIAL_MIN_OVERLAP_M", 0))
# Cap on the number of candidate routes included in the detour graph. Routes are ranked by trip count
# and the busiest ones are kept. The primary route is always included regardless of rank.
DETOUR_MAX_CANDIDATE_ROUTES = max(1, parse_int_env("DETOUR_MAX_CANDIDATE_ROUTES", 80))
GRAPH_WARMUP_ENABLED = parse_bool_env("GRAPH_WARMUP_ENABLED", True)
GRAPH_WARMUP_TIMEOUT_S = max(10, parse_int_env("GRAPH_WARMUP_TIMEOUT_S", 300))
GRAPH_WARMUP_PROFILES = parse_csv_env(
    "GRAPH_WARMUP_PROFILES",
    "weekday,friday,saturday,sunday",
)
GRAPH_WARMUP_PREVIEWS_ENABLED = parse_bool_env("GRAPH_WARMUP_PREVIEWS_ENABLED", True)
GRAPH_WARMUP_PREVIEW_VERIFY_SIG = parse_bool_env("GRAPH_WARMUP_PREVIEW_VERIFY_SIG", True)
GRAPH_WARMUP_LOG_PROGRESS = parse_bool_env("GRAPH_WARMUP_LOG_PROGRESS", False)
# 0 = no cap (within GRAPH_WARMUP_TIMEOUT_S)
GRAPH_WARMUP_PREVIEW_MAX_ROUTES = max(0, parse_int_env("GRAPH_WARMUP_PREVIEW_MAX_ROUTES", 0))

# PostgreSQL/PostGIS: backend/db_access.py reads DATABASE_URL for area search, graph build, detours.
# Example: postgresql://user:pass@localhost:5432/israel_gtfs

# In-memory caches (simple process-level caching)
GRAPH_CACHE: dict = {}
GTFS_CACHE: dict = {}
# Route search: path -> list of (route_dict, normalized_search_str) for fast /routes/search
ROUTES_SEARCH_INDEX_CACHE: dict = {}

# Cached per-trip time bounds (seconds since service-day midnight),
# keyed by GTFS feed source path (string) -> {trip_id: (first_sec, last_sec)}
TRIP_TIME_BOUNDS_CACHE: dict = {}

# Cached shapes_by_id for area search: feed path -> {shape_id: [sorted shape_pt rows]}
SHAPES_BY_ID_CACHE: dict = {}

# On-disk cache for per-route graphs (persistent between restarts)
GRAPH_CACHE_DIR = BASE_DIR / "data" / "graph_cache"
GRAPH_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# GovMap tile proxy: upstream URL template with {z}, {x}, {y} (see /api/govmap-tiles in app.py).
GOVMAP_TILE_UPSTREAM_TEMPLATE = os.getenv("GOVMAP_TILE_UPSTREAM_TEMPLATE", "").strip()

# Detour v2 API and pipeline (road-graph-first).
DETOUR_V2_ENABLED = parse_bool_env("DETOUR_V2_ENABLED", True)
# Engine for legacy area-detour flows: "v2" tags diagnostics and prefers OSM/Valhalla hybrid when available;
# "v1" keeps previous hybrid gating (use_osm_detour flag only). Per-trip bus detours use POST /api/v1/detours/compute.
# "v3" / "v3_default": use detour v3 (PostGIS graph + pattern_osm_segments) for /detours/compute when not overridden per-request.
_DETOUR_ENGINE_RAW = (os.getenv("DETOUR_ENGINE", "v2") or "v2").strip().lower()
DETOUR_ENGINE: str = (
    _DETOUR_ENGINE_RAW if _DETOUR_ENGINE_RAW in ("v1", "v2", "v3", "v3_default") else "v2"
)
DETOUR_V3_ENABLED = parse_bool_env("DETOUR_V3_ENABLED", True)


def parse_optional_positive_int_env(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        v = int(str(raw).strip())
        return v if v > 0 else None
    except Exception:
        return None


# Restrict v3 road-graph load to one OSM import cohort (optional; unset = all segments in DB).
DETOUR_V3_IMPORT_RUN_ID = parse_optional_positive_int_env("DETOUR_V3_IMPORT_RUN_ID")
# Routing cost policy: "bus_corridor_plus_connectors" (default) or "strict_bus_corridor".
DETOUR_V3_COST_MODE = (
    (os.getenv("DETOUR_V3_COST_MODE", "bus_corridor_plus_connectors") or "bus_corridor_plus_connectors")
    .strip()
    .lower()
)


def detour_per_trip_engine_default() -> str:
    """Return \"v2\" or \"v3\" for POST /detours/compute when the request does not override."""
    if DETOUR_ENGINE in ("v3", "v3_default"):
        return "v3"
    return "v2"

