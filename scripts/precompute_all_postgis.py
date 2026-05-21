"""
Run the full PostGIS data pipeline in one go:

  1. (Optional) GTFS ingest - ``backend.scripts.ingest_gtfs_postgis`` (checksum skip when zip unchanged).
  2. Build route patterns - ``build_patterns_postgis`` (``patterns_built_checksum`` skip when zip unchanged).
  2b. (Optional) ``--rebuild-gtfs-bus-way-evidence``
  3. Graph precompute - ``scripts.precompute_graphs_postgis`` (cache + route signatures skip redundant routes).
  4. (Optional) Detour v3: OSM import, segment turns, pattern-to-OSM match, bus evidence.

Recommended full Detour v3 command (copy-paste from repo root; in PowerShell use line continuation ``^`` or a single line; use ``=`` for args whose values start with ``-``)::

    python -m scripts.precompute_all_postgis \\
      --with-ingest --ingest-fetch-if-newer --workers 4 \\
      --with-osm-import --osm-import-extra=--with-segments,--with-turns \\
      --with-segment-turns --with-pattern-osm-match --with-bus-evidence

Optional conveniences on this orchestrator (same script)::

    --osm-pbf-fetch-if-newer   prepend ``--fetch-if-newer`` to ``import_osm_pbf`` argv (PBF only when remote newer)
    --pattern-osm-all          append ``--all-patterns,--limit,200`` to ``match_patterns_to_osm`` (full-feed match; heavy)

Content-addressed skip: each subprocess skips only when ``input_fingerprint`` matches a prior
``outcome=succeeded`` run (see ``backend/infra/pipeline_skip.py``). Use ``--force-all`` to rebuild anyway.

Incremental examples (omit ``--with-ingest`` when GTFS is already loaded; fingerprint skips apply inside each subprocess)::

    python -m scripts.precompute_all_postgis --skip-patterns --skip-graphs \\
      --with-osm-import --osm-import-extra=--with-segments,--with-turns --with-segment-turns \\
      --with-pattern-osm-match --with-bus-evidence

    python -m scripts.precompute_all_postgis --with-ingest --ingest-fetch-if-newer --workers 4 \\
      --pattern-osm-all --pattern-osm-workers 4
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from backend.infra.config import LOCAL_GTFS_ZIP
from backend.infra.db_access import DB_URL
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.pipeline_plan import (
    ARTIFACT_ORDER,
    PlanRequest,
    PlanStep,
    compute_plan,
    format_plan_table,
)
from backend.infra.pipeline_skip import (
    find_feed_by_zip_checksum,
    reactivate_feed,
    sha256_file,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _plan_action(plan, artifact: str) -> str:
    for step in plan:
        if step.artifact == artifact:
            return step.action
    return "skip"


def _should_execute(plan, artifact: str) -> bool:
    return _plan_action(plan, artifact) in ("run", "run_partial")


def _plan_step(plan: list[PlanStep], artifact: str) -> PlanStep | None:
    for step in plan:
        if step.artifact == artifact:
            return step
    return None


def _apply_planned_ingest_reactivation(db: str, ingest_step: PlanStep | None) -> None:
    """When plan skips ingest via checksum reuse/reactivate, still switch active feed."""
    if ingest_step is None or ingest_step.action != "skip":
        return
    reason = ingest_step.reason or ""
    if "reactivate feed_id=" not in reason:
        return
    if not LOCAL_GTFS_ZIP.is_file():
        log("precompute-all", "phase=ingest_reactivate skipped (no local GTFS zip)")
        return
    import psycopg2

    zip_ck = sha256_file(LOCAL_GTFS_ZIP)
    conn = psycopg2.connect(db)
    try:
        conn.autocommit = False
        match = find_feed_by_zip_checksum(conn, zip_ck)
        if match is None or match.active:
            return
        reactivate_feed(conn, match.feed_id, zip_ck)
        log(
            "precompute-all",
            f"phase=ingest_reactivate done reused existing feed_id={match.feed_id} by checksum",
        )
    finally:
        conn.close()


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
            "rebuild, optional detour v3 OSM/pattern/evidence layers, precompute_graphs_postgis. "
            "See module docstring for the recommended full Detour v3 argv; shorthands: "
            "--osm-pbf-fetch-if-newer, --pattern-osm-all, --pattern-osm-workers."
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
            "Does not build gtfs_bus_way_evidence (use --rebuild-gtfs-bus-way-evidence)."
        ),
    )
    ap.add_argument(
        "--plan-only",
        action="store_true",
        help="Print pipeline plan and exit (read-only: no downloads, ingest, or stage writes).",
    )
    ap.add_argument(
        "--explain-skips",
        action="store_true",
        help="Verbose skip reasons in plan output.",
    )
    ap.add_argument(
        "--from-stage",
        type=str,
        default=None,
        choices=ARTIFACT_ORDER,
        help="Plan/execute from this artifact onward.",
    )
    ap.add_argument(
        "--to-stage",
        type=str,
        default=None,
        choices=ARTIFACT_ORDER,
        help="Plan/execute through this artifact.",
    )
    ap.add_argument(
        "--force-artifact",
        action="append",
        default=[],
        metavar="NAME",
        help=f"Force rebuild for artifact ({', '.join(ARTIFACT_ORDER)}). Repeatable.",
    )
    ap.add_argument(
        "--force-all",
        action="store_true",
        help=(
            "Pass --force / --force-refresh / --force-signatures to subprocesses that support "
            "content-addressed skip overrides."
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
        "--rebuild-gtfs-bus-way-evidence",
        action="store_true",
        help=(
            "After pattern build: run backend.scripts.build_gtfs_bus_way_evidence "
            "(GTFS shapes to OSM ways for detour v2; ingest already does this when --with-ingest is used)."
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
        help=(
            "Parallel workers for scripts.precompute_graphs_postgis (ProcessPoolExecutor). "
            "Also the default for match_patterns_to_osm when --with-pattern-osm-match is set "
            "unless overridden by --pattern-osm-workers. Does not parallelize ingest, pattern build, "
            "OSM import, or bus evidence."
        ),
    )
    ap.add_argument(
        "--pattern-osm-workers",
        type=int,
        default=None,
        metavar="N",
        help=(
            "With --with-pattern-osm-match: pass --workers to match_patterns_to_osm (Valhalla+DB threads). "
            "Default: same as --workers."
        ),
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
        "--full-edge-preview-geojson",
        action="store_true",
        help="Forward to graph precompute: slow full-edge preview GeoJSON (default is fast preview).",
    )
    ap.add_argument(
        "--with-osm-import",
        action="store_true",
        help="After patterns: run backend.scripts.import_osm_pbf (pass extra args via --osm-import-extra).",
    )
    ap.add_argument(
        "--osm-import-extra",
        type=str,
        default="",
        help=(
            "Comma-separated argv tokens forwarded to import_osm_pbf "
            "(e.g. --with-segments,--with-turns). Split on commas only. "
            "Values starting with - must use equals form: "
            "--osm-import-extra=--with-segments,--with-turns "
            "(otherwise argparse treats the next --flag as a new option)."
        ),
    )
    ap.add_argument(
        "--osm-pbf-fetch-if-newer",
        action="store_true",
        help=(
            "With --with-osm-import: prepend --fetch-if-newer to import_osm_pbf "
            "(download PBF only when remote metadata says newer)."
        ),
    )
    ap.add_argument(
        "--with-pattern-osm-match",
        action="store_true",
        help="After optional OSM import: run backend.scripts.match_patterns_to_osm (Valhalla + pattern_osm_segments).",
    )
    ap.add_argument(
        "--pattern-osm-extra",
        type=str,
        default="",
        help=(
            "Comma-separated tokens for match_patterns_to_osm. "
            "If any token starts with -, use --pattern-osm-extra=--limit,10 style."
        ),
    )
    ap.add_argument(
        "--pattern-osm-all",
        action="store_true",
        help=(
            "With --with-pattern-osm-match: append --all-patterns,--limit,200 to match argv "
            "(full-feed Valhalla map-match; combine with --pattern-osm-workers)."
        ),
    )
    ap.add_argument(
        "--with-bus-evidence",
        action="store_true",
        help="Rebuild gtfs_bus_segment_evidence / gtfs_bus_turn_evidence via backend.scripts.build_bus_evidence.",
    )
    ap.add_argument(
        "--with-segment-turns",
        action="store_true",
        help=(
            "Run backend.scripts.build_segment_turns (rebuild osm_segment_turns). "
            "Use after segments exist; redundant when import_osm_pbf was run with --with-turns in --osm-import-extra."
        ),
    )
    ap.add_argument(
        "--segment-turns-import-run-id",
        type=int,
        default=None,
        metavar="ID",
        help="Forward to build_segment_turns: only adjacency from segments with this import_run_id.",
    )
    args = ap.parse_args()
    db = args.database_url or DB_URL
    profiles = [p.strip() for p in str(args.profiles).split(",") if p.strip()]
    force_artifacts = {str(a).strip() for a in (args.force_artifact or []) if str(a).strip()}

    plan_req = PlanRequest(
        with_ingest=bool(args.with_ingest),
        skip_patterns=bool(args.skip_patterns),
        skip_graphs=bool(args.skip_graphs),
        rebuild_gtfs_bus_way_evidence=bool(args.rebuild_gtfs_bus_way_evidence),
        with_osm_import=bool(args.with_osm_import),
        with_segment_turns=bool(args.with_segment_turns),
        with_pattern_osm_match=bool(args.with_pattern_osm_match),
        with_bus_evidence=bool(args.with_bus_evidence),
        ingest_force=bool(args.ingest_force),
        force_all=bool(args.force_all),
        force_patterns=bool(args.force_patterns),
        force_artifacts=force_artifacts,
        profiles=profiles or ["weekday", "friday", "saturday", "sunday"],
        with_pretty_osm=bool(args.with_pretty_osm),
        fast_preview_geojson=not bool(args.full_edge_preview_geojson),
        gtfs_zip_path=LOCAL_GTFS_ZIP,
        from_stage=args.from_stage,
        to_stage=args.to_stage,
        explain_skips=bool(args.explain_skips),
    )

    import psycopg2

    plan_conn = psycopg2.connect(db)
    try:
        plan = compute_plan(plan_conn, plan_req)
    finally:
        plan_conn.close()

    print("[precompute-all] Pipeline plan:", flush=True)
    print(format_plan_table(plan), flush=True)
    if args.plan_only:
        log("precompute-all", "phase=plan_only done")
        return

    if args.pattern_osm_all and not args.with_pattern_osm_match:
        ap.error("--pattern-osm-all requires --with-pattern-osm-match")
    if args.osm_pbf_fetch_if_newer and not args.with_osm_import:
        ap.error("--osm-pbf-fetch-if-newer requires --with-osm-import")

    if int(args.workers) > 1 or (
        args.pattern_osm_workers is not None and int(args.pattern_osm_workers) > 1
    ):
        log(
            "precompute-all",
            "note=parallel workers: graphs use --workers; pattern OSM match uses --pattern-osm-workers or --workers",
        )

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

    force_all = bool(args.force_all)

    if args.with_ingest and _should_execute(plan, "ingest"):
        log("precompute-all", "phase=ingest_pipeline start")
        ingest_extra: list[str] = []
        if args.ingest_force or force_all:
            ingest_extra.append("--force")
        if args.ingest_fetch_always:
            ingest_extra.append("--fetch-always")
        elif args.ingest_fetch_if_newer:
            ingest_extra.append("--fetch-if-newer")
        elif args.ingest_fetch:
            ingest_extra.append("--fetch")
        _run_py_module("backend.scripts.ingest_gtfs_postgis", ingest_extra, db)
        log("precompute-all", "phase=ingest_pipeline done")
    elif args.with_ingest:
        _apply_planned_ingest_reactivation(db, _plan_step(plan, "ingest"))
        log("precompute-all", "phase=ingest_pipeline skipped per plan")

    if not args.skip_patterns and _should_execute(plan, "patterns"):
        log("precompute-all", "phase=patterns_pipeline start")
        pat_extra: list[str] = []
        if args.pattern_date:
            pat_extra.extend(["--date", args.pattern_date])
        if args.force_patterns or force_all:
            pat_extra.append("--force")
        if args.ignore_calendar:
            pat_extra.append("--ignore-calendar")
        if args.route_batch_size != 200:
            pat_extra.extend(["--route-batch-size", str(args.route_batch_size)])
        _run_py_module("backend.scripts.build_patterns_postgis", pat_extra, db)
        log("precompute-all", "phase=patterns_pipeline done")
    elif not args.skip_patterns:
        log("precompute-all", "phase=patterns_pipeline skipped per plan")

    if args.rebuild_gtfs_bus_way_evidence and _should_execute(plan, "gtfs_bus_way_evidence"):
        log("precompute-all", "phase=gtfs_bus_way_evidence start")
        gbwe_extra = ["--force"] if force_all else []
        _run_py_module("backend.scripts.build_gtfs_bus_way_evidence", gbwe_extra, db)
        log("precompute-all", "phase=gtfs_bus_way_evidence done")
    elif args.rebuild_gtfs_bus_way_evidence:
        log("precompute-all", "phase=gtfs_bus_way_evidence skipped per plan")

    if args.with_osm_import and _should_execute(plan, "osm_import"):
        log("precompute-all", "phase=osm_import start")
        extra_toks = [t.strip() for t in str(args.osm_import_extra).split(",") if t.strip()]
        if args.osm_pbf_fetch_if_newer and not any(
            t == "--fetch-if-newer" or t.startswith("--fetch-if-newer=") for t in extra_toks
        ):
            extra_toks = ["--fetch-if-newer", *extra_toks]
        if force_all and not any(t == "--force" or t.startswith("--force=") for t in extra_toks):
            extra_toks = ["--force", *extra_toks]
        _run_py_module("backend.scripts.import_osm_pbf", extra_toks, db)
        log("precompute-all", "phase=osm_import done")
    elif args.with_osm_import:
        log("precompute-all", "phase=osm_import skipped per plan")

    if args.with_segment_turns and _should_execute(plan, "segment_turns"):
        log("precompute-all", "phase=segment_turns start")
        st_extra: list[str] = []
        if args.segment_turns_import_run_id is not None:
            st_extra.extend(["--import-run-id", str(int(args.segment_turns_import_run_id))])
        if force_all:
            st_extra.append("--force")
        _run_py_module("backend.scripts.build_segment_turns", st_extra, db)
        log("precompute-all", "phase=segment_turns done")
    elif args.with_segment_turns:
        log("precompute-all", "phase=segment_turns skipped per plan")

    if args.with_pattern_osm_match and _should_execute(plan, "pattern_osm_match"):
        log("precompute-all", "phase=pattern_osm_match start")
        pom_extra = [t.strip() for t in str(args.pattern_osm_extra).split(",") if t.strip()]
        if force_all and not any(t == "--force-refresh" or t.startswith("--force-refresh=") for t in pom_extra):
            pom_extra.append("--force-refresh")
        if args.pattern_osm_all and "--all-patterns" not in pom_extra:
            pom_extra.extend(["--all-patterns", "--limit", "200"])
        pom_workers = (
            int(args.pattern_osm_workers)
            if args.pattern_osm_workers is not None
            else int(args.workers)
        )
        pom_workers = max(1, pom_workers)
        if not any(t == "--workers" or t.startswith("--workers=") for t in pom_extra):
            pom_extra.extend(["--workers", str(pom_workers)])
        if not any(t == "--slice-overlap-m" or t.startswith("--slice-overlap-m=") for t in pom_extra):
            pom_extra.extend(["--slice-overlap-m", "200"])
        _run_py_module("backend.scripts.match_patterns_to_osm", pom_extra, db)
        log("precompute-all", "phase=pattern_osm_match done")
    elif args.with_pattern_osm_match:
        log("precompute-all", "phase=pattern_osm_match skipped per plan")

    if args.with_bus_evidence and _should_execute(plan, "bus_evidence"):
        log("precompute-all", "phase=bus_evidence start")
        be_extra = ["--force"] if force_all else []
        _run_py_module("backend.scripts.build_bus_evidence", be_extra, db)
        log("precompute-all", "phase=bus_evidence done")
    elif args.with_bus_evidence:
        log("precompute-all", "phase=bus_evidence skipped per plan")

    if not args.skip_graphs and _should_execute(plan, "graphs"):
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
        if args.full_edge_preview_geojson:
            graph_extra.append("--full-edge-preview-geojson")
        if force_all:
            graph_extra.append("--force-signatures")
        _run_py_module("scripts.precompute_graphs_postgis", graph_extra, db)
        log("precompute-all", "phase=graphs_pipeline done")
    elif not args.skip_graphs:
        log("precompute-all", "phase=graphs_pipeline skipped per plan")

    print("[precompute-all] Done.", flush=True)
    log("precompute-all", "phase=main done")


if __name__ == "__main__":
    main()
