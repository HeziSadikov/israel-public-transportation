"""
Run the full PostGIS data pipeline in one go:

  1. (Optional) GTFS ingest — same defaults as backend.scripts.ingest_gtfs_postgis.
     Ingest builds shapes_lines and populates gtfs_bus_way_evidence (GTFS shapes → OSM ways,
     used by detour v2); skipping ingest leaves that table unchanged unless you add step 2b.
  2. Build route patterns + ride network — same defaults as build_patterns_postgis (auto date)
  2b. (Optional) Rebuild gtfs_bus_way_evidence — see --rebuild-gtfs-bus-way-evidence
  3. Precompute route graphs + route previews — same as scripts.precompute_graphs_postgis

From the repository root:

    python -m scripts.precompute_all_postgis
    python -m scripts.precompute_all_postgis --with-ingest --workers 4
    python -m scripts.precompute_all_postgis --with-ingest --ingest-fetch-always --workers 4
    python -m scripts.precompute_all_postgis --rebuild-gtfs-bus-way-evidence --workers 4
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from backend.infra.logging_utils import ensure_cli_action_logging, log

REPO_ROOT = Path(__file__).resolve().parent.parent


def _run_py_module(module: str, extra: list[str], database_url: str | None) -> None:
    cmd = [sys.executable, "-u", "-m", module]
    if database_url:
        cmd.extend(["--database-url", database_url])
    cmd.extend(extra)
    print(f"[precompute-all] {' '.join(cmd)}", flush=True)
    t0 = time.perf_counter()
    log("precompute-all", f"phase=subprocess module={module} start args={extra!r}")
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    r = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env)
    elapsed = time.perf_counter() - t0
    if r.returncode != 0:
        log(
            "precompute-all",
            f"phase=subprocess module={module} error exit_code={r.returncode} elapsed_s={elapsed:.2f}",
        )
        sys.exit(r.returncode)
    log(
        "precompute-all",
        f"phase=subprocess module={module} done exit_code=0 elapsed_s={elapsed:.2f}",
    )


def main() -> None:
    ensure_cli_action_logging()
    log("precompute-all", "phase=main start")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(
        description=(
            "One command: optional ingest, build_patterns_postgis, optional gtfs_bus_way_evidence "
            "rebuild, precompute_graphs_postgis."
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
        help=(
            "Run ingest_gtfs_postgis first (default zip + MOT source URL from ingest script). "
            "Also builds gtfs_bus_way_evidence for detour v2."
        ),
    )
    ap.add_argument(
        "--ingest-force",
        action="store_true",
        help="With --with-ingest: pass --force to re-ingest even if checksum matches.",
    )
    ap.add_argument(
        "--ingest-fetch",
        action="store_true",
        help="With --with-ingest: pass --fetch (download GTFS to default zip path if missing).",
    )
    ap.add_argument(
        "--ingest-fetch-always",
        action="store_true",
        help="With --with-ingest: pass --fetch-always (always download GTFS before ingest).",
    )
    ap.add_argument(
        "--ingest-fetch-if-newer",
        action="store_true",
        help=(
            "With --with-ingest: pass --fetch-if-newer "
            "(download only when remote metadata indicates a newer GTFS zip)."
        ),
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
        "--ignore-calendar",
        action="store_true",
        help="Forward to build_patterns_postgis: use all trips (topology tooling, not operational).",
    )
    ap.add_argument(
        "--route-batch-size",
        type=int,
        default=200,
        metavar="N",
        help="Forward to build_patterns_postgis: routes per trips/stop_times batch (default 200).",
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
    ap.add_argument(
        "--rebuild-gtfs-bus-way-evidence",
        action="store_true",
        help=(
            "After patterns: run backend.scripts.build_gtfs_bus_way_evidence (refreshes shapes_lines "
            "then rebuilds gtfs_bus_way_evidence). Use when you did not pass --with-ingest but need "
            "detour v2 GTFS way evidence. Idempotent if ingest already ran in the same invocation."
        ),
    )
    args = ap.parse_args()
    db = args.database_url

    if args.route_batch_size < 1:
        ap.error("--route-batch-size must be >= 1")

    ingest_fetch_modes = (
        int(bool(args.ingest_fetch))
        + int(bool(args.ingest_fetch_always))
        + int(bool(args.ingest_fetch_if_newer))
    )
    if ingest_fetch_modes > 1:
        ap.error(
            "Use only one of --ingest-fetch, --ingest-fetch-always, and --ingest-fetch-if-newer"
        )

    if args.with_ingest:
        log("precompute-all", "phase=ingest_pipeline start")
        ingest_extra: list[str] = []
        if args.ingest_force:
            ingest_extra.append("--force")
        if args.ingest_fetch_always:
            ingest_extra.append("--fetch-always")
        elif args.ingest_fetch_if_newer:
            ingest_extra.append("--fetch-if-newer")
        elif args.ingest_fetch:
            ingest_extra.append("--fetch")
        _run_py_module("backend.scripts.ingest_gtfs_postgis", ingest_extra, db)
        log("precompute-all", "phase=ingest_pipeline done")

    if not args.skip_patterns:
        log("precompute-all", "phase=patterns_pipeline start")
        pat_extra: list[str] = []
        if args.pattern_date:
            pat_extra.extend(["--date", args.pattern_date])
        if args.force_patterns:
            pat_extra.append("--force")
        if args.ignore_calendar:
            pat_extra.append("--ignore-calendar")
        if args.route_batch_size != 200:
            pat_extra.extend(["--route-batch-size", str(args.route_batch_size)])
        _run_py_module("backend.scripts.build_patterns_postgis", pat_extra, db)
        log("precompute-all", "phase=patterns_pipeline done")

    if args.rebuild_gtfs_bus_way_evidence:
        log("precompute-all", "phase=gtfs_bus_way_evidence start")
        _run_py_module("backend.scripts.build_gtfs_bus_way_evidence", [], db)
        log("precompute-all", "phase=gtfs_bus_way_evidence done")

    if not args.skip_graphs:
        log("precompute-all", "phase=graphs_pipeline start")
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
        log("precompute-all", "phase=graphs_pipeline done")

    print("[precompute-all] Done.", flush=True)
    log("precompute-all", "phase=main done")


if __name__ == "__main__":
    main()
