"""
End-to-end /area/routes check without browser or running uvicorn.

Uses FastAPI TestClient + canonical polygon from backend.area_routes_canonical_fixtures.
Skips graph cache warmup on import so startup stays fast.

  python scripts/verify_area_routes_http.py

Requires DATABASE_URL and a PostGIS DB with an active feed (same as the app).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from backend.infra.logging_utils import ensure_cli_action_logging, log

# Before importing app: config reads these at import time.
os.environ.setdefault("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs")
os.environ["GRAPH_WARMUP_ENABLED"] = "false"

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app import app  # noqa: E402
from backend.area_routes_canonical_fixtures import (  # noqa: E402
    CANONICAL_AREA_DATE_YMD,
    CANONICAL_AREA_END_TIME_HHMM,
    CANONICAL_AREA_POLYGON_GEOJSON,
    CANONICAL_AREA_START_TIME_HHMM,
)


def main() -> int:
    ensure_cli_action_logging()
    log("verify_area_http", "phase=main start")
    payload = {
        "polygon_geojson": CANONICAL_AREA_POLYGON_GEOJSON,
        "start_date": CANONICAL_AREA_DATE_YMD,
        "start_time": CANONICAL_AREA_START_TIME_HHMM,
        "end_date": CANONICAL_AREA_DATE_YMD,
        "end_time": CANONICAL_AREA_END_TIME_HHMM,
        "max_results": 200,
    }
    with TestClient(app) as client:
        log("verify_area_http", "phase=http_post start endpoint=/area/routes")
        r = client.post("/api/v1/area/routes", json=payload)
        log("verify_area_http", f"phase=http_post done status={r.status_code}")
    body = r.json() if r.content else {}
    routes = body.get("routes") if isinstance(body, dict) else None
    n = len(routes) if isinstance(routes, list) else -1
    line = f"HTTP {r.status_code} routes={n}"
    if r.status_code != 200:
        line += f" detail={body.get('detail', body)!r}"
    print(line, flush=True)
    if r.status_code != 200:
        print(json.dumps(body, indent=2, default=str)[:2000], flush=True)
        log("verify_area_http", "phase=main error")
        return 1
    if n < 1:
        print(
            "Expected at least one route; set AREA_TEST_DATE_YMD to a day in your GTFS calendar.",
            flush=True,
        )
        log("verify_area_http", "phase=main error empty_routes=true")
        return 1
    print("AREA_ROUTES_HTTP_OK", flush=True)
    log("verify_area_http", "phase=main done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
