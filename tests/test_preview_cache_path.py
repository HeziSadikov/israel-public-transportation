from __future__ import annotations

import pickle
import time

from fastapi import Response

from backend.mcp_server.transport import http as app_mod


def test_graph_preview_memory_hit(monkeypatch):
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig")
    monkeypatch.setattr(app_mod, "resolve_service_profile", lambda _d: "weekday")

    key = "postgis-1|R1|0|profile:weekday|gtfs|preview"
    payload = {
        "pattern_id": "p1",
        "stops": [],
        "route_geojson": {"type": "FeatureCollection", "features": []},
        "used_osm_snapping": False,
        "feed_version": "postgis-1",
    }
    app_mod.GRAPH_CACHE[key] = payload
    try:
        resp = Response()
        out = app_mod.graph_preview(
            response=resp,
            route_id="R1",
            direction_id="0",
            date="20260327",
            pretty_osm=False,
        )
        assert out["pattern_id"] == "p1"
        assert resp.headers.get("X-Cache-Hit") == "memory"
        assert resp.headers.get("X-Graph-Cache-Hit") == "none"
    finally:
        app_mod.GRAPH_CACHE.pop(key, None)


def test_graph_preview_postgres_preview_hit(monkeypatch):
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig")
    monkeypatch.setattr(app_mod, "resolve_service_profile", lambda _d: "weekday")

    payload = {
        "pattern_id": "p2",
        "stops": [{"stop_id": "S1", "name": "A", "lat": 1.0, "lon": 2.0, "sequence": 0}],
        "route_geojson": {"type": "FeatureCollection", "features": []},
        "used_osm_snapping": False,
        "feed_version": "postgis-1",
    }
    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_cached_route_preview_pg",
        lambda **_kwargs: {"pattern_id": "p2", "preview_blob": pickle.dumps(payload)},
    )

    resp = Response()
    out = app_mod.graph_preview(
        response=resp,
        route_id="R2",
        direction_id="1",
        date="20260327",
        pretty_osm=False,
    )
    assert out["pattern_id"] == "p2"
    assert resp.headers.get("X-Cache-Hit") == "postgres"
    assert resp.headers.get("X-Graph-Cache-Hit") == "none"


def test_preview_mem_key_matches_graph_preview():
    k = app_mod._preview_mem_key("postgis-1", "R1", "0", "weekday", False)
    assert k == "postgis-1|R1|0|profile:weekday|gtfs|preview"


def test_warmup_route_previews_hydrates_cache(monkeypatch):
    monkeypatch.setattr(app_mod, "GRAPH_WARMUP_PREVIEWS_ENABLED", True)
    monkeypatch.setattr(app_mod, "GRAPH_WARMUP_PREVIEW_VERIFY_SIG", False)
    monkeypatch.setattr(app_mod, "GRAPH_WARMUP_PREVIEW_MAX_ROUTES", 0)
    payload = {
        "pattern_id": "p9",
        "stops": [],
        "route_geojson": {"type": "FeatureCollection", "features": []},
        "used_osm_snapping": False,
        "feed_version": "postgis-1",
    }
    blob = pickle.dumps(payload)

    def _bulk(_feed_id, _profile, pretty_osm):
        if pretty_osm:
            return {}
        return {("RX", "1"): ("any_sig", "pat", blob)}

    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_cached_previews_bulk",
        _bulk,
    )
    app_mod.GRAPH_CACHE.clear()
    try:
        t0 = time.time()
        gtfs, osm, skip = app_mod._warmup_route_previews(
            1, "postgis-1", ["weekday"], t0, 60.0
        )
        assert gtfs == 1 and osm == 0 and skip == 0
        key = app_mod._preview_mem_key("postgis-1", "RX", "1", "weekday", False)
        assert app_mod.GRAPH_CACHE.get(key) == payload
    finally:
        app_mod.GRAPH_CACHE.clear()


def test_warmup_route_previews_skips_stale_sig(monkeypatch):
    monkeypatch.setattr(app_mod, "GRAPH_WARMUP_PREVIEWS_ENABLED", True)
    monkeypatch.setattr(app_mod, "GRAPH_WARMUP_PREVIEW_VERIFY_SIG", True)
    payload = {
        "pattern_id": "p9",
        "stops": [],
        "route_geojson": {"type": "FeatureCollection", "features": []},
        "used_osm_snapping": False,
        "feed_version": "postgis-1",
    }
    blob = pickle.dumps(payload)

    def _bulk(_feed_id, _profile, pretty_osm):
        if pretty_osm:
            return {}
        return {("RX", "1"): ("old_sig", "pat", blob)}

    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_cached_previews_bulk",
        _bulk,
    )
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_a, **_k: "new_sig")
    app_mod.GRAPH_CACHE.clear()
    try:
        t0 = time.time()
        gtfs, osm, skip = app_mod._warmup_route_previews(
            1, "postgis-1", ["weekday"], t0, 60.0
        )
        assert gtfs == 0 and osm == 0 and skip == 1
    finally:
        app_mod.GRAPH_CACHE.clear()
