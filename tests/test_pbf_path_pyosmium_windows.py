"""pyosmium path resolution on Windows (non-ASCII profile paths)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from backend.osm_import.pbf_importer import resolve_pbf_path_for_pyosmium


def test_ascii_path_unchanged(tmp_path: Path) -> None:
    if not str(tmp_path.resolve()).isascii():
        pytest.skip("pytest tmp path is non-ASCII on this host")
    pbf = tmp_path / "test.osm.pbf"
    pbf.write_bytes(b"\x00")
    assert resolve_pbf_path_for_pyosmium(pbf) == pbf.resolve()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only pyosmium path workaround")
def test_non_ascii_path_stages_on_windows(tmp_path: Path) -> None:
    non_ascii = tmp_path / "עברית" / "test.osm.pbf"
    non_ascii.parent.mkdir(parents=True)
    non_ascii.write_bytes(b"\x00\x00")
    staged = resolve_pbf_path_for_pyosmium(non_ascii)
    assert staged != non_ascii.resolve()
    assert str(staged).isascii()
    assert staged.exists()
