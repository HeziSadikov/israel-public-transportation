@echo off
cd /d "%~dp0"
echo Starting backend from: %CD%
echo.
echo When you see "Uvicorn running on http://127.0.0.1:8000", open in browser:
echo   http://127.0.0.1:8000/health
echo.
python -m uvicorn app:app --reload --port 8000 --host 0.0.0.0
if errorlevel 1 (
  echo.
  echo Backend failed. Make sure you run this from the project folder:
  echo   %CD%
  echo and that app.py exists here.
)
pause
