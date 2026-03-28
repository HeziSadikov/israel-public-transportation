@echo off
REM Same as dev.ps1: timestamps on reload parent. Usage: dev.bat app:app --reload --port 8000
set "UVICORN_LOG_CONFIG=%~dp0uvicorn_logging.json"
py -m uvicorn %*
