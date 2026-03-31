from __future__ import annotations

import logging
from datetime import datetime

_ACTION_LOGGER = logging.getLogger("app.action")

def now_ts() -> str:
    """Return local time as HH:MM:SS for backward compatibility."""
    return datetime.now().strftime("%H:%M:%S")

def log(tag: str, msg: str, *, flush: bool = True) -> None:
    """
    Emit structured app action logs through stdlib logging.
    """
    # Keep ``flush`` for call-site compatibility with the previous print-based helper.
    _ = flush
    _ACTION_LOGGER.info("[%s] %s", tag, msg)

