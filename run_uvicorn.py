from __future__ import annotations

import argparse
import os

import uvicorn

from backend.infra.uvicorn_logging import LOGGING_CONFIG


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MCP HTTP API (Uvicorn).")
    parser.add_argument(
        "--detour-debug",
        action="store_true",
        help="Set DETOUR_V2_DEBUG=1 so every /api/v1/detours/compute emits detours/v2/compute_ai logs (no automatic GeoJSON).",
    )
    args = parser.parse_args()
    if args.detour_debug:
        os.environ["DETOUR_V2_DEBUG"] = "1"

    uvicorn.run(
        "backend.mcp_server.transport.http:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_config=LOGGING_CONFIG,
    )


if __name__ == "__main__":
    main()
