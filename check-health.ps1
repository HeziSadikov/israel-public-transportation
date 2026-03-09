# Test if the backend is responding. Run while the backend is running.
# Usage: .\check-health.ps1

try {
  $r = Invoke-WebRequest -Uri "http://127.0.0.1:8000/health" -UseBasicParsing -TimeoutSec 5
  Write-Host "OK - Backend responded:" -ForegroundColor Green
  Write-Host $r.Content
} catch {
  Write-Host "Failed to reach http://127.0.0.1:8000/health" -ForegroundColor Red
  Write-Host $_.Exception.Message
  Write-Host ""
  Write-Host "Make sure the backend is running (start-backend.bat or: python -m uvicorn app:app --port 8000 --host 0.0.0.0)"
}
