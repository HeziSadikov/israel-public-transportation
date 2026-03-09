# Start the backend from the project folder so Python finds the app.
# Run from PowerShell: .\start-backend.ps1
# Or right-click -> Run with PowerShell

Set-Location $PSScriptRoot
Write-Host "Starting backend from: $(Get-Location)" -ForegroundColor Cyan
Write-Host "API will be at: http://127.0.0.1:8000" -ForegroundColor Green
Write-Host "Health check: http://127.0.0.1:8000/health" -ForegroundColor Green
Write-Host ""
python -m uvicorn app:app --reload --port 8000 --host 0.0.0.0
