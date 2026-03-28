from __future__ import annotations

import pickle

from fastapi import Response

import app as app_mod


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
