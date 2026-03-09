# How to run the backend so /health works

The backend **must** be started from the **project folder** (where `app.py` is). If you run it from your user folder or anywhere else, you get:

`Error loading ASGI app. Could not import module "app".`

and nothing listens on port 8000, so http://localhost:8000/health never works.

## Option 1: Double‑click the script (easiest)

1. In File Explorer, go to the project folder:  
   `C:\Users\חל\Desktop\israel-public-transportation`
2. Double‑click **`start-backend.bat`**
3. A terminal opens. Wait until you see:  
   `Uvicorn running on http://127.0.0.1:8000`
4. In your browser open: **http://127.0.0.1:8000/health**  
   You should see: `{"status":"ok","time":"..."}`

Leave that terminal window open while you use the app.

## Option 2: Run from terminal

1. Open PowerShell or Command Prompt.
2. Go to the project folder:
   ```powershell
   cd "C:\Users\חל\Desktop\israel-public-transportation"
   ```
3. Start the backend (use `0.0.0.0` so the server accepts connections properly on Windows):
   ```powershell
   python -m uvicorn app:app --reload --port 8000 --host 0.0.0.0
   ```
4. When you see `Uvicorn running on http://0.0.0.0:8000`, try in the browser: **http://127.0.0.1:8000/health** (no space at the end).
5. To test from the terminal (same machine): run `.\check-health.ps1`. If that works but the browser doesn’t, the server is fine and the issue is the browser or firewall.

## If it still doesn’t work

- **“python is not recognized”**  
  Install Python and make sure “Add Python to PATH” was checked. Or try `py -m uvicorn app:app --reload --port 8000 --host 127.0.0.1` instead of `python -m ...`.

- **“Could not import module app”**  
  You’re not in the project folder. Run `cd` to the path above and try again.

- **Port 8000 in use**  
  Use another port, e.g. `--port 8001`, then open http://127.0.0.1:8001/health and set the frontend `API_BASE` to that URL.

- **Browser says “can’t connect” but the server is running**  
  - Use **http** (not https).  
  - No space after `/health`: `http://127.0.0.1:8000/health`  
  - Run `.\check-health.ps1` in PowerShell from the project folder. If that succeeds, the server is OK; try another browser or allow Windows Firewall for Python.
