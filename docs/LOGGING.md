### Logging and timestamps

- All custom backend logs (scripts and selected endpoints) use **local time** in `HH:MM:SS` format via `backend/logging_utils.py`.
- Use `now_ts()` or `log(tag, msg)` when adding new logs:

```python
from backend.logging_utils import log

log("feed/update", "Downloading GTFS ...")
log("patterns", "Building patterns ...")
```

- The `log` helper prints lines like (same leading time style as Uvicorn, then a single bracketed tag):

```text
19:52:36 [feed/update] Downloading GTFS ...
```

#### Uvicorn / FastAPI logs

- Config lives in repo-root [`uvicorn_logging.json`](../uvicorn_logging.json) (loaded by [`backend/uvicorn_logging.py`](../backend/uvicorn_logging.py)). It uses Uvicorn’s `DefaultFormatter` / `AccessFormatter` so levels and access lines are colorized when stderr/stdout are TTYs, with a leading `HH:MM:SS` time.
- **`python -m run_uvicorn`** passes that config as `log_config`, so the **reload parent** and workers share the same format (recommended; also what `start-backend.ps1` / `start-backend.bat` run).
- If you start Uvicorn yourself, the **reload parent** must see that config too, or the first few lines (“Will watch…”, “Uvicorn running…”, “Started reloader process…”) stay on Uvicorn’s default formatter (no leading time). Two ways:
  - **Flag:**  
    `py -m uvicorn app:app --reload --port 8000 --log-config uvicorn_logging.json`
  - **Env (no extra flags):** from the project root, set `UVICORN_LOG_CONFIG` before the command (Uvicorn’s CLI uses Click’s `UVICORN_` prefix). PowerShell:  
    `$env:UVICORN_LOG_CONFIG = "$PWD\uvicorn_logging.json"`  
    cmd.exe:  
    `set UVICORN_LOG_CONFIG=%CD%\uvicorn_logging.json`  
    Or run **`.\dev.ps1 app:app --reload --port 8000`** / **`dev.bat app:app --reload --port 8000`**, which set that variable for you.

- **`--env-file .env` is not enough** for `UVICORN_LOG_CONFIG`: Uvicorn loads `.env` only after the CLI has already read the environment, so put the variable in your shell profile, use `dev.ps1` / `dev.bat`, or use `--log-config`.

- Importing `app` still runs `dictConfig(LOGGING_CONFIG)` so worker processes match even when the parent did not use the config above.

Example Uvicorn line shape:

```text
19:52:36 INFO:     Started server process [pid] ...
```
