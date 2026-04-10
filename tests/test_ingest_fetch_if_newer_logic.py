from __future__ import annotations

from pathlib import Path

from backend.scripts.ingest_gtfs_postgis import _should_download_if_newer


def test_should_download_if_newer_when_zip_missing(tmp_path: Path):
    zip_path = tmp_path / "israel-public-transportation.zip"
    do_download, reason = _should_download_if_newer(
        gtfs_zip=zip_path,
        source_url="https://example.com/gtfs.zip",
        remote_meta={"etag": "abc", "last_modified": None, "content_length": None},
        local_meta={},
    )
    assert do_download is True
    assert reason == "zip_missing"


def test_should_skip_when_etag_unchanged(tmp_path: Path):
    zip_path = tmp_path / "israel-public-transportation.zip"
    zip_path.write_bytes(b"x")
    do_download, reason = _should_download_if_newer(
        gtfs_zip=zip_path,
        source_url="https://example.com/gtfs.zip",
        remote_meta={"etag": "same-etag", "last_modified": None, "content_length": None},
        local_meta={"source_url": "https://example.com/gtfs.zip", "etag": "same-etag"},
    )
    assert do_download is False
    assert reason == "etag_unchanged"


def test_should_download_when_etag_changed(tmp_path: Path):
    zip_path = tmp_path / "israel-public-transportation.zip"
    zip_path.write_bytes(b"x")
    do_download, reason = _should_download_if_newer(
        gtfs_zip=zip_path,
        source_url="https://example.com/gtfs.zip",
        remote_meta={"etag": "new-etag", "last_modified": None, "content_length": None},
        local_meta={"source_url": "https://example.com/gtfs.zip", "etag": "old-etag"},
    )
    assert do_download is True
    assert reason == "etag_changed"


def test_should_download_when_metadata_missing_even_with_existing_zip(tmp_path: Path):
    zip_path = tmp_path / "israel-public-transportation.zip"
    zip_path.write_bytes(b"x")
    do_download, reason = _should_download_if_newer(
        gtfs_zip=zip_path,
        source_url="https://example.com/gtfs.zip",
        remote_meta={},
        local_meta={"source_url": "https://example.com/gtfs.zip", "etag": "old-etag"},
    )
    assert do_download is True
    assert reason == "metadata_unavailable"
