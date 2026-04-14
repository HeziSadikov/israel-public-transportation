import json
import sqlite3
from pathlib import Path

from backend.infra.config import BASE_DIR
from backend.infra.logging_utils import ensure_cli_action_logging, log


def main() -> None:
    ensure_cli_action_logging()
    log("check_db_schema", "phase=main start")
    db = BASE_DIR / "data" / "gtfs.db"
    print("DB exists:", db.exists())
    if not db.exists():
        log("check_db_schema", "phase=main done db_missing=true")
        return

    log("check_db_schema", "phase=sqlite_connect start")
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    log("check_db_schema", "phase=sqlite_connect done")

    # stop_times columns
    cur.execute("PRAGMA table_info(stop_times)")
    cols = {row["name"]: row["type"] for row in cur.fetchall()}
    print("STOP_TIMES_COLUMNS:", json.dumps(cols, ensure_ascii=False))

    # helper tables
    cur.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name IN ('trip_time_bounds', 'shape_bbox')"
    )
    tables = [row["name"] for row in cur.fetchall()]
    print("EXTRA_TABLES:", json.dumps(tables, ensure_ascii=False))

    conn.close()
    log("check_db_schema", "phase=main done")


if __name__ == "__main__":
    main()

