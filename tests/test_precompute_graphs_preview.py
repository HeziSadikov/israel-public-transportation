"""Unit tests for precompute_graphs_postgis preview helpers (no DB)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from shapely.geometry import LineString

from backend.infra.db_access import StopMeta

_ROOT = Path(__file__).resolve().parent.parent


def _load_precompute_mod():
    p = _ROOT / "scripts" / "precompute_graphs_postgis.py"
    spec = importlib.util.spec_from_file_location("precompute_graphs_postgis", p)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_build_preview_payload_from_stops_and_shape():
    mod = _load_precompute_mod()
    stops = [
        StopMeta(stop_id="1", name="A", lat=32.0, lon=34.8),
        StopMeta(stop_id="2", name="B", lat=32.1, lon=34.81),
    ]
    shape = LineString([(34.8, 32.0), (34.81, 32.1)])
    fc, sl = mod._build_preview_payload_from_stops_and_shape(stops, shape, None)
    assert fc["type"] == "FeatureCollection"
    assert len(sl) == 2
    assert sl[0]["stop_id"] == "1"
    assert sl[1]["sequence"] == 1
    geoms = [f["geometry"]["type"] for f in fc["features"]]
    assert geoms.count("Point") == 2
    assert geoms.count("LineString") == 1


def test_build_preview_snapped_prefers_snapped_over_shape():
    mod = _load_precompute_mod()
    stops = [StopMeta(stop_id="1", name="A", lat=32.0, lon=34.8)]
    shape = LineString([(34.8, 32.0), (34.81, 32.1)])
    snapped = LineString([(34.85, 32.05), (34.9, 32.15)])
    fc, _sl = mod._build_preview_payload_from_stops_and_shape(stops, shape, snapped)
    line_feats = [f for f in fc["features"] if f["geometry"]["type"] == "LineString"]
    assert len(line_feats) == 1
    assert line_feats[0]["properties"].get("kind") == "pattern_snapped"


def test_commit_route_bundle_commits_when_batch_size_one():
    mod = _load_precompute_mod()
    commits = []

    class _Conn:
        def commit(self):
            commits.append("commit")

    pending = [0]
    mod._commit_route_bundle(
        _Conn(),
        commit_bs=1,
        sp_name="sp_x",
        pending_since_commit=pending,
    )
    assert commits == ["commit"]
    assert pending[0] == 0
