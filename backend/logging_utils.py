from __future__ import annotations

from datetime import datetime
from typing import Any


def now_ts() -> str:
    """
    Return local time as HH:MM:SS.

    On your workstation this is effectively Israel time, which is what you expect
    to see in logs.
    """
    return datetime.now().strftime("%H:%M:%S")


def log(tag: str, msg: str, *, flush: bool = True) -> None:
    """
    Print a log line with a consistent timestamp and tag.

    Example:
        [19:52:36] [feed/update] Downloading GTFS...
    """
    ts = now_ts()
    text = f"[{ts}] [{tag}] {msg}"
    # Use plain print so this stays simple and works in scripts.
    if flush:
        print(text, flush=True)
    else:
        print(text)

