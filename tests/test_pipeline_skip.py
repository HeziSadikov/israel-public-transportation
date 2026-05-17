"""Unit tests for content-addressed pipeline skip helpers."""

from __future__ import annotations

from backend.infra import pipeline_skip as ps


def test_fingerprint_stable_and_sensitive():
    a = ps.fingerprint_sha256({"stage": "x", "v": 1})
    b = ps.fingerprint_sha256({"stage": "x", "v": 1})
    c = ps.fingerprint_sha256({"stage": "x", "v": 2})
    assert a == b
    assert a != c
    assert len(a) == 64


def test_may_skip_requires_succeeded_and_matching_fp():
    fp = "abc123"
    assert ps.may_skip(fp, fp, force=False, last_outcome=ps.OUTCOME_SUCCEEDED)
    assert not ps.may_skip(fp, fp, force=True, last_outcome=ps.OUTCOME_SUCCEEDED)
    assert not ps.may_skip(fp, fp, force=False, last_outcome=ps.OUTCOME_FAILED)
    assert not ps.may_skip(fp, fp, force=False, last_outcome=ps.OUTCOME_RUNNING)
    assert not ps.may_skip(fp, "other", force=False, last_outcome=ps.OUTCOME_SUCCEEDED)
    assert not ps.may_skip(fp, None, force=False, last_outcome=None)


def test_advisory_lock_key_deterministic():
    u1 = ps.UnitLockKey(stage_name="pattern_osm_match", feed_id=1, pattern_id="p1")
    u2 = ps.UnitLockKey(stage_name="pattern_osm_match", feed_id=1, pattern_id="p1")
    u3 = ps.UnitLockKey(stage_name="pattern_osm_match", feed_id=1, pattern_id="p2")
    assert ps.advisory_lock_key(u1) == ps.advisory_lock_key(u2)
    assert ps.advisory_lock_key(u1) != ps.advisory_lock_key(u3)


def test_build_pattern_osm_match_fingerprint_includes_config():
    fp1 = ps.build_pattern_osm_match_fingerprint(
        pattern_signature="psig",
        osm_dataset_fingerprint="osm",
        costing="bus",
        densify_m=15.0,
        full_trace_max_km=10.0,
        chunk_legs=11,
        chunk_overlap=1,
    )
    fp2 = ps.build_pattern_osm_match_fingerprint(
        pattern_signature="psig",
        osm_dataset_fingerprint="osm",
        costing="bus",
        densify_m=20.0,
        full_trace_max_km=10.0,
        chunk_legs=11,
        chunk_overlap=1,
    )
    assert fp1 != fp2


def test_algorithm_version_bump_invalidates_fingerprint():
    fp_v1 = ps.build_bus_evidence_fingerprint(
        gtfs_zip_checksum="z",
        osm_dataset_fingerprint="o",
    )
    old = ps.BUS_EVIDENCE_ALGORITHM_VERSION
    try:
        ps.BUS_EVIDENCE_ALGORITHM_VERSION = "bus_evidence_test_bump"
        fp_v2 = ps.build_bus_evidence_fingerprint(
            gtfs_zip_checksum="z",
            osm_dataset_fingerprint="o",
        )
        assert fp_v1 != fp_v2
    finally:
        ps.BUS_EVIDENCE_ALGORITHM_VERSION = old
