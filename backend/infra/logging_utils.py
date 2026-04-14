from __future__ import annotations

import logging
import sys
from datetime import datetime

_ACTION_LOGGER = logging.getLogger("app.action")

def now_ts() -> str:
    """Return local time as HH:MM:SS for backward compatibility."""
    return datetime.now().strftime("%H:%M:%S")


def ensure_cli_action_logging() -> None:
    """
    Attach a stderr handler with timestamps when ``app.action`` has no handlers.

    Uvicorn loads ``uvicorn_logging.json`` (configures ``app.action``). Standalone
    CLIs do not, so ``log()`` INFO records would be dropped by the root logger.
    """
    if _ACTION_LOGGER.handlers:
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _ACTION_LOGGER.addHandler(h)
    _ACTION_LOGGER.setLevel(logging.INFO)
    _ACTION_LOGGER.propagate = False


def log(tag: str, msg: str, *, flush: bool = True) -> None:
    """
    Emit structured app action logs through stdlib logging.
    """
    _ACTION_LOGGER.info("[%s] %s", tag, msg)
    if flush:
        for h in _ACTION_LOGGER.handlers:
            try:
                h.flush()
            except Exception:
                pass
        try:
            sys.stderr.flush()
        except Exception:
            pass

