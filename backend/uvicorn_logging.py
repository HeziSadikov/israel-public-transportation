"""Load shared Uvicorn dictConfig from repo-root ``uvicorn_logging.json``.

The JSON file can be passed to the CLI as ``--log-config`` so the reload
**parent** process uses the same timestamped, colorized format as workers
(``app`` import also applies this config for ``python -m uvicorn`` without the flag).
"""

from __future__ import annotations

import json
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_LOG_CONFIG_PATH = _PROJECT_ROOT / "uvicorn_logging.json"

LOGGING_CONFIG = json.loads(_LOG_CONFIG_PATH.read_text(encoding="utf-8"))
