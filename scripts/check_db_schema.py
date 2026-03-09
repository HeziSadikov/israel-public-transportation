import json
import sqlite3
from pathlib import Path


def main() -> None:
    db = Path(r"c:\Users\חל\Desktop\israel-public-transportation\data\gtfs.db")
    print("DB exists:", db.exists())
    if not db.exists():
        return

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

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


if __name__ == "__main__":
    main()

