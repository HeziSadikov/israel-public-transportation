"""
Detour v3 OSM PBF import CLI.

Usage::

    # Default: read OSM_PBF_PATH (osm/israel.osm.pbf) and import into DATABASE_URL.
    python -m backend.scripts.import_osm_pbf

    # Override the local PBF path.
    python -m backend.scripts.import_osm_pbf --pbf osm/israel.osm.pbf

    # Refresh the local PBF from OSM_PBF_URL only when the remote is newer.
    python -m backend.scripts.import_osm_pbf --fetch-if-newer

    # Always re-download then import.
    python -m backend.scripts.import_osm_pbf --fetch

    # Count what would be imported but do not write to Postgres.
    python -m backend.scripts.import_osm_pbf --verify

    # Reset only v3-imported rows (legacy osm_road_segments / osm_turn_restrictions
    # rows are preserved).
    python -m backend.scripts.import_osm_pbf --reset-osm-import

    # M1C + M2: segments + ``osm_segment_turns``.
    python -m backend.scripts.import_osm_pbf --with-segments --with-turns

    # DESTRUCTIVE: wipe ALL rows from shared OSM tables, including legacy ones.
    python -m backend.scripts.import_osm_pbf --reset-osm-import \\
        --dangerously-truncate-shared-osm-tables --yes

Behavior:

* Applies the idempotent ``ensure_detour_v3_layer.sql`` migration first.
* Inserts a row into ``osm_import_runs`` and records its id as provenance
  on every shared-table row written by this run
  (``import_source = 'detour_v3_pbf_import'``).
* On success / failure, updates ``osm_import_runs`` with ``finished_at``,
  ``status``, and ``stats_json`` for an audit trail.
* Never touches ``detour_v2`` runtime behavior, GTFS tables, incidents,
  ``pattern_edge_match``, or ``approved_detours``.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2

from backend.infra import db_access as db
from backend.infra.config import OSM_PBF_PATH, OSM_PBF_URL
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import (
    ensure_detour_v3_layer,
    finish_osm_import_run,
    reset_v3_osm_import,
    start_osm_import_run,
)
from backend.infra.osm_pbf_download import (
    download_osm_pbf,
    get_remote_osm_pbf_metadata,
    parse_last_modified,
)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="import_osm_pbf",
        description=(
            "Detour v3 OSM PBF importer. Streams an OSM PBF into raw "
            "osm_nodes / osm_ways / osm_way_nodes + extended "
            "osm_turn_restrictions (with v3 provenance). Optionally builds "
            "directed osm_road_segments (``--with-segments``, M1C) and "
            "precomputed legal turns (``--with-turns``, M2)."
        ),
    )
    parser.add_argument(
        "--pbf",
        default=None,
        help=(
            "Path to the OSM PBF file (default: OSM_PBF_PATH env / "
            "osm/israel.osm.pbf)."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL / db_access.DB_URL).",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Always download from OSM_PBF_URL before importing.",
    )
    parser.add_argument(
        "--fetch-if-newer",
        action="store_true",
        help=(
            "Download from OSM_PBF_URL only when the remote's "
            "ETag/Last-Modified/Content-Length indicates a different file."
        ),
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help=(
            "Count what would be imported but do not write to Postgres. "
            "An osm_import_runs row is still created (status=verify_only)."
        ),
    )
    parser.add_argument(
        "--reset-osm-import",
        action="store_true",
        help=(
            "Provenance-scoped reset: truncates v3-only tables and deletes "
            "v3-provenance rows from osm_road_segments / osm_turn_restrictions. "
            "Legacy rows (NULL provenance) are preserved unless "
            "--dangerously-truncate-shared-osm-tables is also passed."
        ),
    )
    parser.add_argument(
        "--dangerously-truncate-shared-osm-tables",
        action="store_true",
        help=(
            "Combined with --reset-osm-import, fully TRUNCATE shared tables "
            "(osm_road_segments, osm_turn_restrictions) including legacy "
            "rows. Requires --yes for unattended use."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Skip interactive confirmation for "
            "--dangerously-truncate-shared-osm-tables."
        ),
    )
    parser.add_argument(
        "--with-segments",
        action="store_true",
        help=(
            "After the raw PBF import, run the way splitter and build "
            "directed osm_road_segments rows (M1C)."
        ),
    )
    parser.add_argument(
        "--with-turns",
        action="store_true",
        help=(
            "After directed segments exist, rebuild ``osm_segment_turns`` "
            "(M2). Scoped to the same import run as ``--with-segments`` when "
            "both are set; otherwise use ``--segment-import-run-id`` or "
            "(with ``--skip-import``) rebuild from all segments in the table."
        ),
    )
    parser.add_argument(
        "--segment-import-run-id",
        type=int,
        default=None,
        help=(
            "When building turns (``--with-turns``), only use "
            "``osm_road_segments.import_run_id`` matching this value on both "
            "endpoints. Overrides the default scoping from ``--with-segments``."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-import even when osm_dataset_fingerprint matches the last successful run.",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help=(
            "Skip the actual PBF parsing. Useful when combined with "
            "--reset-osm-import (e.g. for dev clean-slate) or --with-segments "
            "to re-build segments from data already in osm_ways."
        ),
    )
    return parser.parse_args(argv)


def _resolve_pbf_path(args: argparse.Namespace) -> Path:
    if args.pbf:
        return Path(args.pbf).expanduser().resolve()
    return Path(OSM_PBF_PATH).expanduser().resolve()


def _confirm_dangerous_truncate(args: argparse.Namespace) -> bool:
    if not args.dangerously_truncate_shared_osm_tables:
        return True
    if args.yes:
        return True
    sys.stderr.write(
        "[import_osm_pbf] WARNING: --dangerously-truncate-shared-osm-tables\n"
        "will DELETE every row in osm_road_segments and osm_turn_restrictions,\n"
        "including legacy non-v3 rows. Type 'TRUNCATE' to proceed: "
    )
    sys.stderr.flush()
    try:
        line = sys.stdin.readline().strip()
    except Exception:
        line = ""
    return line == "TRUNCATE"


def _maybe_fetch_pbf(
    pbf_path: Path,
    *,
    fetch_always: bool,
    fetch_if_newer: bool,
) -> Dict[str, Any]:
    """Download the PBF when requested. Returns metadata used for the import run row."""
    fetch_meta: Dict[str, Any] = {
        "fetched": False,
        "fetch_reason": None,
        "pbf_url": OSM_PBF_URL,
        "pbf_remote_last_modified": None,
        "pbf_remote_etag": None,
        "pbf_remote_content_length": None,
    }
    if not (fetch_always or fetch_if_newer):
        return fetch_meta
    if not OSM_PBF_URL:
        log("import-osm-pbf", "fetch requested but OSM_PBF_URL is empty; skipping")
        return fetch_meta

    if fetch_always:
        log("import-osm-pbf", f"--fetch: downloading {OSM_PBF_URL} -> {pbf_path}")
        download_osm_pbf(pbf_path, OSM_PBF_URL)
        fetch_meta["fetched"] = True
        fetch_meta["fetch_reason"] = "fetch_always"
        return fetch_meta

    # --fetch-if-newer
    if not pbf_path.exists():
        log(
            "import-osm-pbf",
            f"--fetch-if-newer: local PBF missing at {pbf_path}; downloading",
        )
        download_osm_pbf(pbf_path, OSM_PBF_URL)
        fetch_meta["fetched"] = True
        fetch_meta["fetch_reason"] = "missing_local"
        return fetch_meta

    try:
        remote = get_remote_osm_pbf_metadata(OSM_PBF_URL)
    except Exception as e:
        log(
            "import-osm-pbf",
            f"--fetch-if-newer: metadata probe failed ({e!s}); downloading anyway",
        )
        download_osm_pbf(pbf_path, OSM_PBF_URL)
        fetch_meta["fetched"] = True
        fetch_meta["fetch_reason"] = "metadata_probe_failed"
        return fetch_meta

    fetch_meta["pbf_remote_etag"] = remote.get("etag")
    fetch_meta["pbf_remote_last_modified"] = remote.get("last_modified")
    fetch_meta["pbf_remote_content_length"] = remote.get("content_length")

    local_size = pbf_path.stat().st_size if pbf_path.exists() else None
    local_mtime = (
        _dt.datetime.fromtimestamp(pbf_path.stat().st_mtime, tz=_dt.timezone.utc)
        if pbf_path.exists()
        else None
    )
    remote_size_str = remote.get("content_length") or ""
    try:
        remote_size = int(remote_size_str) if remote_size_str else None
    except ValueError:
        remote_size = None
    remote_modified = parse_last_modified(remote.get("last_modified"))

    needs = False
    reason = "up_to_date"
    if remote_size is not None and local_size is not None and remote_size != local_size:
        needs = True
        reason = "size_changed"
    elif (
        remote_modified is not None
        and local_mtime is not None
        and remote_modified > local_mtime
    ):
        needs = True
        reason = "remote_newer"

    if needs:
        log(
            "import-osm-pbf",
            f"--fetch-if-newer: refresh reason={reason} downloading ...",
        )
        download_osm_pbf(pbf_path, OSM_PBF_URL)
        fetch_meta["fetched"] = True
        fetch_meta["fetch_reason"] = reason
    else:
        log("import-osm-pbf", f"--fetch-if-newer: local PBF is up to date ({reason})")
        fetch_meta["fetch_reason"] = reason
    return fetch_meta


def _file_metadata(pbf_path: Path) -> Dict[str, Any]:
    if not pbf_path.exists():
        return {"pbf_size_bytes": None, "pbf_modified_at": None}
    st = pbf_path.stat()
    return {
        "pbf_size_bytes": int(st.st_size),
        "pbf_modified_at": _dt.datetime.fromtimestamp(st.st_mtime, tz=_dt.timezone.utc),
    }


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    args = _parse_args(argv)

    if (
        args.dangerously_truncate_shared_osm_tables
        and not args.reset_osm_import
    ):
        sys.stderr.write(
            "[import_osm_pbf] --dangerously-truncate-shared-osm-tables requires "
            "--reset-osm-import.\n"
        )
        return 2

    if not _confirm_dangerous_truncate(args):
        sys.stderr.write("[import_osm_pbf] confirmation rejected; aborting.\n")
        return 3

    database_url = args.database_url or db.DB_URL
    pbf_path = _resolve_pbf_path(args)

    log(
        "import-osm-pbf",
        (
            "config "
            f"pbf={pbf_path} "
            f"pbf_url={OSM_PBF_URL} "
            f"fetch={args.fetch} fetch_if_newer={args.fetch_if_newer} "
            f"verify={args.verify} reset={args.reset_osm_import} "
            f"dangerous={args.dangerously_truncate_shared_osm_tables} "
            f"with_segments={args.with_segments} with_turns={args.with_turns} "
            f"segment_import_run_id={args.segment_import_run_id} "
            f"skip_import={args.skip_import}"
        ),
    )

    conn = psycopg2.connect(database_url)
    try:
        # ---- Phase 1: migration ------------------------------------------
        t_mig = time.perf_counter()
        ensure_detour_v3_layer(conn)
        from backend.infra.pipeline_skip import (
            build_osm_dataset_fingerprint,
            ensure_pipeline_schema,
            find_successful_osm_import_by_dataset_fingerprint,
            sha256_file,
            OSM_IMPORT_ALGORITHM_VERSION,
        )

        ensure_pipeline_schema(conn)
        log(
            "import-osm-pbf",
            f"migration applied elapsed_s={time.perf_counter() - t_mig:.2f}",
        )

        # ---- Phase 2: optional reset -------------------------------------
        reset_stats: Optional[Dict[str, Any]] = None
        if args.reset_osm_import:
            t_reset = time.perf_counter()
            reset_stats = reset_v3_osm_import(
                conn,
                dangerously_truncate_shared=args.dangerously_truncate_shared_osm_tables,
            )
            log(
                "import-osm-pbf",
                f"reset done elapsed_s={time.perf_counter() - t_reset:.2f} stats={reset_stats}",
            )

        # ---- Phase 3: optional fetch -------------------------------------
        fetch_meta = _maybe_fetch_pbf(
            pbf_path,
            fetch_always=args.fetch,
            fetch_if_newer=args.fetch_if_newer,
        )

        do_import = not args.skip_import
        if do_import and not pbf_path.exists():
            sys.stderr.write(
                f"[import_osm_pbf] OSM PBF not found at {pbf_path}. "
                "Pass --fetch / --fetch-if-newer or place a file at that path.\n"
            )
            return 4

        file_meta = _file_metadata(pbf_path) if do_import else {
            "pbf_size_bytes": None,
            "pbf_modified_at": None,
        }

        osm_cli = {
            "verify": bool(args.verify),
            "with_segments": bool(args.with_segments),
            "with_turns": bool(args.with_turns),
            "segment_import_run_id": args.segment_import_run_id,
            "skip_import": bool(args.skip_import),
            "reset_osm_import": bool(args.reset_osm_import),
        }
        pbf_sha = sha256_file(pbf_path) if pbf_path.exists() else ""
        osm_dataset_fp = build_osm_dataset_fingerprint(pbf_sha256=pbf_sha, cli=osm_cli)
        existing_run = (
            find_successful_osm_import_by_dataset_fingerprint(conn, osm_dataset_fp)
            if do_import and not args.verify and not args.force and not args.reset_osm_import
            else None
        )
        if existing_run:
            log(
                "import-osm-pbf",
                f"Skip: osm_dataset_fingerprint unchanged (run_id={existing_run})",
            )
            print(
                json.dumps(
                    {
                        "skipped": True,
                        "reason": "osm_dataset_fingerprint_unchanged",
                        "osm_dataset_fingerprint": osm_dataset_fp,
                        "existing_run_id": existing_run,
                    },
                    indent=2,
                ),
                flush=True,
            )
            return 0

        # ---- Phase 4: create osm_import_runs row -------------------------
        run_id = start_osm_import_run(
            conn,
            pbf_path=str(pbf_path),
            pbf_url=OSM_PBF_URL,
            pbf_size_bytes=file_meta["pbf_size_bytes"],
            pbf_modified_at=file_meta["pbf_modified_at"],
            status="verify_only" if args.verify else "running",
        )

        combined_stats: Dict[str, Any] = {
            "fetch": fetch_meta,
            "reset": reset_stats,
            "config": {
                "verify": bool(args.verify),
                "with_segments": bool(args.with_segments),
                "with_turns": bool(args.with_turns),
                "segment_import_run_id": args.segment_import_run_id,
                "skip_import": bool(args.skip_import),
                "reset_osm_import": bool(args.reset_osm_import),
                "dangerously_truncate_shared_osm_tables": bool(
                    args.dangerously_truncate_shared_osm_tables
                ),
            },
        }

        # ---- Phase 5: PBF parse ------------------------------------------
        if do_import:
            from backend.osm_import.pbf_importer import run_pbf_import

            t_import = time.perf_counter()
            try:
                import_stats = run_pbf_import(
                    conn,
                    pbf_path,
                    import_run_id=run_id,
                    verify=args.verify,
                )
            except Exception as e:
                log("import-osm-pbf", f"import failed err={e!s}")
                finish_osm_import_run(
                    conn,
                    run_id,
                    status="failed",
                    stats={**combined_stats, "error": str(e)},
                )
                raise

            combined_stats["import"] = import_stats.to_dict()
            log(
                "import-osm-pbf",
                f"import phase done elapsed_s={time.perf_counter() - t_import:.2f}",
            )
        else:
            combined_stats["import"] = None
            log("import-osm-pbf", "--skip-import: skipping PBF parse")

        # ---- Phase 6: optional directed segments (M1C) -------------------
        if args.with_segments and not args.verify:
            from backend.osm_import.build_directed_segments import (
                build_directed_segments,
            )

            t_seg = time.perf_counter()
            try:
                seg_stats = build_directed_segments(conn, import_run_id=run_id)
            except Exception as e:
                log("import-osm-pbf", f"build_directed_segments failed err={e!s}")
                finish_osm_import_run(
                    conn,
                    run_id,
                    status="failed",
                    stats={**combined_stats, "directed_segments_error": str(e)},
                )
                raise
            combined_stats["directed_segments"] = seg_stats.to_dict()
            log(
                "import-osm-pbf",
                (
                    "directed segments built "
                    f"elapsed_s={time.perf_counter() - t_seg:.2f} "
                    f"stats={seg_stats.to_dict()}"
                ),
            )

        # ---- Phase 6b: turn table (M2) -------------------------------------
        if args.with_turns and not args.verify:
            from backend.osm_import.build_turn_table import build_segment_turns

            turn_filter: Optional[int]
            if args.segment_import_run_id is not None:
                turn_filter = args.segment_import_run_id
            elif args.with_segments:
                turn_filter = run_id
            else:
                turn_filter = None

            t_turn = time.perf_counter()
            try:
                turn_stats = build_segment_turns(
                    conn,
                    segment_import_run_id=turn_filter,
                )
            except Exception as e:
                log("import-osm-pbf", f"build_segment_turns failed err={e!s}")
                finish_osm_import_run(
                    conn,
                    run_id,
                    status="failed",
                    stats={
                        **combined_stats,
                        "segment_turns_error": str(e),
                    },
                )
                raise
            combined_stats["segment_turns"] = {
                **turn_stats.to_dict(),
                "segment_import_run_id_used": turn_filter,
            }
            log(
                "import-osm-pbf",
                (
                    "segment turns built "
                    f"elapsed_s={time.perf_counter() - t_turn:.2f} "
                    f"filter_run_id={turn_filter} "
                    f"stats={turn_stats.to_dict()}"
                ),
            )

        # ---- Phase 7: finalize osm_import_runs ---------------------------
        final_status = "verify_only" if args.verify else "success"
        from backend.infra.pipeline_skip import (
            mark_osm_succeeded,
            set_osm_import_dataset_fingerprint,
            StageName,
        )

        set_osm_import_dataset_fingerprint(conn, run_id, osm_dataset_fp)
        conn.commit()
        finish_osm_import_run(
            conn,
            run_id,
            status=final_status,
            stats=combined_stats,
        )
        if final_status == "success":
            mark_osm_succeeded(
                conn,
                run_id,
                StageName.OSM_IMPORT.value,
                osm_dataset_fp,
                stats={"algorithm_version": OSM_IMPORT_ALGORITHM_VERSION},
            )
            conn.commit()
        log(
            "import-osm-pbf",
            f"run id={run_id} status={final_status} ok",
        )
        # Compact summary for shell consumers.
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "status": final_status,
                    "pbf_path": str(pbf_path),
                    "stats": combined_stats,
                },
                default=str,
                indent=2,
            ),
            flush=True,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
