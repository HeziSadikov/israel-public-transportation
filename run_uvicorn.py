from __future__ import annotations

import argparse
import os

import uvicorn

from backend.infra.uvicorn_logging import LOGGING_CONFIG


def _want_reload() -> bool:
    """
    File watching the whole bind-mounted repo in Docker often hits ENOMEM after startup
    (WatchfilesRustInternalError: Cannot allocate memory). Default reload off in Docker;
    set UVICORN_RELOAD=1 to force it on.
    """
    v = (os.getenv("UVICORN_RELOAD") or "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "yes", "on"):
        return True
    return not os.path.exists("/.dockerenv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MCP HTTP API (Uvicorn).")
    parser.add_argument(
        "--detour-debug",
        action="store_true",
        help="Set DETOUR_V2_DEBUG=1 so every /api/v1/detours/compute emits detours/v2/compute_ai logs (no automatic GeoJSON).",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable Uvicorn autoreload (same as UVICORN_RELOAD=0).",
    )
    args = parser.parse_args()
    if args.detour_debug:
        os.environ["DETOUR_V2_DEBUG"] = "1"
    if args.no_reload:
        os.environ["UVICORN_RELOAD"] = "0"

    uvicorn.run(
        "backend.mcp_server.transport.http:app",
        host="0.0.0.0",
        port=8000,
        reload=_want_reload(),
        log_config=LOGGING_CONFIG,
    )


if __name__ == "__main__":
    main()
