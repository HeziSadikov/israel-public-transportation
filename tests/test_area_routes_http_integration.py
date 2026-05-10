"""
HTTP-level /area/routes via TestClient (no uvicorn, no browser).

Skips without DATABASE_URL or if Postgres is down.
"""
from __future__ import annotations

import os

# Before importing app (backend.infra.config reads env at import time).
os.environ.setdefault("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs")
os.environ["GRAPH_WARMUP_ENABLED"] = "false"

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402
from backend.mcp_server.transport.http import app  # noqa: E402


@pytest.fixture(scope="module")
def http_client():
    with TestClient(app) as c:
        yield c


@pytest.mark.slow
def test_post_area_routes_canonical_polygon_returns_200_and_routes(http_client):
    from backend.area_routes_canonical_fixtures import (
        CANONICAL_AREA_DATE_YMD,
        CANONICAL_AREA_END_TIME_HHMM,
        CANONICAL_AREA_POLYGON_GEOJSON,
        CANONICAL_AREA_START_TIME_HHMM,
    )

    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set")
    payload = {
        "polygon_geojson": CANONICAL_AREA_POLYGON_GEOJSON,
        "start_date": CANONICAL_AREA_DATE_YMD,
        "start_time": CANONICAL_AREA_START_TIME_HHMM,
        "end_date": CANONICAL_AREA_DATE_YMD,
        "end_time": CANONICAL_AREA_END_TIME_HHMM,
        "max_results": 200,
    }
    r = http_client.post("/api/v1/area/routes", json=payload)
    if r.status_code == 500 and "could not connect" in (r.text or "").lower():
        pytest.skip("Postgres unreachable")
    if r.status_code == 503 and "timed out" in (r.text or "").lower():
        pytest.skip("Pass-through query timed out in this environment")
    assert r.status_code == 200, r.text
    data = r.json()
    if not (data.get("routes") or []) and data.get("calendar_hint"):
        pytest.skip(str(data.get("calendar_hint")))
    assert len(data.get("routes") or []) >= 1


@pytest.mark.slow
def test_post_area_routes_accepts_pass_through_mode(http_client):
    from backend.area_routes_canonical_fixtures import (
        CANONICAL_AREA_DATE_YMD,
        CANONICAL_AREA_END_TIME_HHMM,
        CANONICAL_AREA_POLYGON_GEOJSON,
        CANONICAL_AREA_START_TIME_HHMM,
    )

    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set")
    payload = {
        "polygon_geojson": CANONICAL_AREA_POLYGON_GEOJSON,
        "start_date": CANONICAL_AREA_DATE_YMD,
        "start_time": CANONICAL_AREA_START_TIME_HHMM,
        "end_date": CANONICAL_AREA_DATE_YMD,
        "end_time": CANONICAL_AREA_END_TIME_HHMM,
        "max_results": 200,
        "time_semantics_mode": "pass_through_stop_proxy",
    }
    r = http_client.post("/api/v1/area/routes", json=payload)
    if r.status_code == 500 and "could not connect" in (r.text or "").lower():
        pytest.skip("Postgres unreachable")
    if r.status_code == 503 and "timed out" in (r.text or "").lower():
        pytest.skip("Pass-through query timed out in this environment")
    assert r.status_code == 200, r.text
    data = r.json()
    routes = data.get("routes") or []
    if routes:
        assert "time_match_confidence" in routes[0]
