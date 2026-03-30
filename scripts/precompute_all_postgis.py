"""
Run the full PostGIS data pipeline in one go:

  1. (Optional) GTFS ingest — same defaults as backend.scripts.ingest_gtfs_postgis
  2. Build route patterns + ride network — same defaults as build_patterns_postgis (auto date)
  3. Precompute route graphs + route previews — same as scripts.precompute_graphs_postgis

From the repository root:

    python -m scripts.precompute_all_postgis
    python -m scripts.precompute_all_postgis --with-ingest --workers 4
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _run_py_module(module: str, extra: list[str], database_url: str | None) -> None:
    cmd = [sys.executable, "-m", module]
    if database_url:
        cmd.extend(["--database-url", database_url])
    cmd.extend(extra)
    print(f"[precompute-all] {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if r.returncode != 0:
        sys.exit(r.returncode)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(
        description=(
            "One command: optional ingest, build_patterns_postgis, precompute_graphs_postgis."
        ),
    )
    ap.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="Postgres URL (default: DATABASE_URL env, same as other scripts).",
    )
    ap.add_argument(
        "--with-ingest",
        action="store_true",
        help="Run ingest_gtfs_postgis first (default zip + MOT source URL from ingest script).",
    )
    ap.add_argument(
        "--ingest-force",
        action="store_true",
        help="With --with-ingest: pass --force to re-ingest even if checksum matches.",
    )
    ap.add_argument(
        "--skip-patterns",
        action="store_true",
        help="Skip pattern build (requires patterns already in DB).",
    )
    ap.add_argument(
        "--skip-graphs",
        action="store_true",
        help="Stop after patterns; skip graph + preview precompute.",
    )
    ap.add_argument(
        "--pattern-date",
        type=str,
        default=None,
        help="Optional YYYYMMDD for pattern build (default: auto from feed calendar).",
    )
    ap.add_argument(
        "--force-patterns",
        action="store_true",
        help="Rebuild patterns even when they already match the active feed zip checksum.",
    )
    ap.add_argument(
        "--profiles",
        type=str,
        default="weekday,friday,saturday,sunday",
        help="Service profiles for graph/preview cache (match GRAPH_WARMUP_PROFILES).",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel workers for graph precompute.",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Progress log interval for graph precompute.",
    )
    ap.add_argument(
        "--with-pretty-osm",
        action="store_true",
        help="Forward to graph precompute: run OSRM map-match and pretty_osm cache rows (slow).",
    )
    args = ap.parse_args()
    db = args.database_url

    if args.with_ingest:
        ingest_extra: list[str] = []
        if args.ingest_force:
            ingest_extra.append("--force")
        _run_py_module("backend.scripts.ingest_gtfs_postgis", ingest_extra, db)

    if not args.skip_patterns:
        pat_extra: list[str] = []
        if args.pattern_date:
            pat_extra.extend(["--date", args.pattern_date])
        if args.force_patterns:
            pat_extra.append("--force")
        _run_py_module("backend.scripts.build_patterns_postgis", pat_extra, db)

    if not args.skip_graphs:
        graph_extra = [
            "--profiles",
            args.profiles,
            "--workers",
            str(args.workers),
            "--progress-every",
            str(args.progress_every),
        ]
        if args.with_pretty_osm:
            graph_extra.append("--with-pretty-osm")
        _run_py_module("scripts.precompute_graphs_postgis", graph_extra, db)

    print("[precompute-all] Done.", flush=True)


if __name__ == "__main__":
    main()
