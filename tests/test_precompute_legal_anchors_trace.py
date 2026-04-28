"""Trace single-flight and DB contract for legal-anchor precompute helpers."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

from backend.infra import db_access as db
from backend.infra.config import LEGAL_ANCHOR_INDEX_ANCHOR_VERSION


def test_resolve_trace_detail_singleflight_two_threads():
    import backend.scripts.precompute_legal_anchors as pla

    trace_mem: dict = {}
    trace_mem_lock = threading.Lock()
    trace_flight_locks: dict = {}
    calls = {"n": 0}

    def fake_run(*_a, valhalla_base_url=None, timeout_s=45.0, **_k):
        calls["n"] += 1
        time.sleep(0.2)
        return {"edges": [{"length": 1.0}], "shape_lonlat": [(34.0, 31.0), (34.1, 31.1)]}

    mem_key = ("fv1", "shapeA", "forward", pla.TRACE_CACHE_VERSION)
    results: list = []

    def worker():
        r = pla._resolve_trace_detail(
            mem_key=mem_key,
            conn=MagicMock(),
            feed_version="fv1",
            repr_shape_id="shapeA",
            direction="forward",
            pts=[(34.0, 31.0), (34.1, 31.1)],
            trace_mem=trace_mem,
            trace_mem_lock=trace_mem_lock,
            trace_flight_locks=trace_flight_locks,
            use_trace_db_cache=False,
            valhalla_base_url=None,
        )
        results.append(r)

    with patch.object(pla, "_run_trace_attributes", side_effect=fake_run):
        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

    assert calls["n"] == 1, "_run_trace_attributes should run once for concurrent same mem_key"
    assert len(results) == 2
    assert results[0] == results[1]


def test_replace_pattern_legal_anchor_delete_includes_version():
    mconn = MagicMock()
    mcur = MagicMock()
    mconn.cursor.return_value.__enter__.return_value = mcur
    db.replace_pattern_legal_anchor_candidates(
        "feed_v",
        "pat_1",
        [
            {
                "role": "exit",
                "rank_in_role": 1,
                "shape_dist_m": 1.0,
                "lon": 34.0,
                "lat": 31.0,
                "osm_node_id": None,
                "incoming_way_id": None,
                "score": 1.0,
                "trace_meta": {},
            }
        ],
        anchor_version="legal_anchor_v1",
        conn=mconn,
    )
    first = mcur.execute.call_args_list[0]
    sql = first[0][0]
    params = first[0][1]
    assert "DELETE FROM pattern_legal_anchor_candidate" in sql
    assert "anchor_version" in sql
    assert params == ("feed_v", "pat_1", "legal_anchor_v1")


def test_fetch_pattern_legal_anchor_candidates_filters_version():
    mconn = MagicMock()
    mcur = MagicMock()
    mcur.fetchall.return_value = []
    mconn.cursor.return_value.__enter__.return_value = mcur
    db.fetch_pattern_legal_anchor_candidates("fv", "pid", conn=mconn, anchor_version="custom_v")
    first = mcur.execute.call_args_list[0]
    sql = first[0][0]
    params = first[0][1]
    assert "anchor_version = %s" in sql
    assert params == ("fv", "pid", "custom_v")


def test_fetch_pattern_legal_anchor_defaults_to_config_version():
    mconn = MagicMock()
    mcur = MagicMock()
    mcur.fetchall.return_value = []
    mconn.cursor.return_value.__enter__.return_value = mcur
    db.fetch_pattern_legal_anchor_candidates("fv", "pid", conn=mconn)
    params = mcur.execute.call_args_list[0][0][1]
    assert params[2] == LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
