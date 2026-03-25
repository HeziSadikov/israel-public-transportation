### Logging and timestamps

- All custom backend logs (scripts and selected endpoints) use **local time** in `HH:MM:SS` format via `backend/logging_utils.py`.
- Use `now_ts()` or `log(tag, msg)` when adding new logs:

```python
from backend.logging_utils import log

log("feed/update", "Downloading GTFS ...")
log("patterns", "Building patterns ...")
```

- The `log` helper prints lines like:

```text
[19:52:36] [feed/update] Downloading GTFS ...
```

#### Uvicorn / FastAPI logs

- To run the app with Uvicorn logs that also use `HH:MM:SS` timestamps, you can use the optional wrapper:

```bash
python -m run_uvicorn
```

- This uses `run_uvicorn.py`, which configures Uvicorn’s loggers with a formatter like:

```text
19:52:36 [INFO] uvicorn.error: Started server process [pid] ...
```

