from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent

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

# Valhalla base URL — used for detour routing (route avoiding polygon).
# If set, /detour will use Valhalla to route around the blocked area on the road network.
# Example: http://localhost:8002 (Valhalla default port)
VALHALLA_URL = os.getenv("VALHALLA_URL", "")

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

