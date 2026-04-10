#!/usr/bin/env python3
"""
Automated area-search debug (no browser, no manual uvicorn).

Runs in order:
  1. scripts/_bench_area_pg.py          — direct PostGIS get_routes_in_polygon_range
  2. scripts/verify_area_routes_http.py — FastAPI POST /area/routes via TestClient
  3. pytest …postgis… + …http…          — same checks as tests

Usage (from repo root):

  python scripts/run_area_search_debug.py

Requires DATABASE_URL (defaults to postgresql://postgres@localhost:5432/israel_gtfs).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from backend.logging_utils import ensure_cli_action_logging, log

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable

DEFAULT_DATABASE_URL = "postgresql://postgres@localhost:5432/israel_gtfs"


def _env() -> dict[str, str]:
    e = os.environ.copy()
    e.setdefault("DATABASE_URL", DEFAULT_DATABASE_URL)
    e["GRAPH_WARMUP_ENABLED"] = "false"
    return e


def _run(label: str, cmd: list[str]) -> int:
    log("run_area_debug", f"phase=step start label={label!r}")
    p = subprocess.run(cmd, cwd=str(ROOT), env=_env())
    if p.returncode != 0:
        log("run_area_debug", f"phase=step error label={label!r} exit_code={p.returncode}")
    else:
        log("run_area_debug", f"phase=step done label={label!r}")
    return p.returncode


def main() -> int:
    ensure_cli_action_logging()
    log(
        "run_area_debug",
        f"phase=main start DATABASE_URL={_env().get('DATABASE_URL', '')!r} "
        f"GRAPH_WARMUP_ENABLED={_env().get('GRAPH_WARMUP_ENABLED')!r}",
    )
    steps: list[tuple[str, list[str]]] = [
        (
            "Bench: PostGIS get_routes_in_polygon_range",
            [PY, str(ROOT / "scripts" / "_bench_area_pg.py")],
        ),
        (
            "HTTP: TestClient POST /area/routes",
            [PY, str(ROOT / "scripts" / "verify_area_routes_http.py")],
        ),
        (
            "Pytest: integration tests",
            [
                PY,
                "-m",
                "pytest",
                str(ROOT / "tests" / "test_area_routes_postgis_integration.py"),
                str(ROOT / "tests" / "test_area_routes_http_integration.py"),
                "-q",
            ],
        ),
    ]
    for label, cmd in steps:
        rc = _run(label, cmd)
        if rc != 0:
            log("run_area_debug", "phase=main error")
            return rc
    log("run_area_debug", "phase=main done all_steps_passed=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
