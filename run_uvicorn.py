from __future__ import annotations

import uvicorn

from backend.infra.uvicorn_logging import LOGGING_CONFIG


def main() -> None:
    uvicorn.run(
        "backend.mcp_server.transport.http:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_config=LOGGING_CONFIG,
    )


if __name__ == "__main__":
    main()
