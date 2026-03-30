from __future__ import annotations

import sys
from datetime import datetime


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

    Matches Uvicorn's leading ``HH:MM:SS`` style (no brackets around the time).

    Example:
        19:52:36 [feed/update] Downloading GTFS ...
    """
    ts = now_ts()
    text = f"{ts} [{tag}] {msg}"
    # Write to stderr so lines show next to Uvicorn's default/error logs
    # (uvicorn_logging.json routes those to sys.stderr, not stdout).
    print(text, file=sys.stderr, flush=flush)

