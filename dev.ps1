# Run Uvicorn with repo log config so the reload parent gets timestamps too.
# Usage (from project root): .\dev.ps1 app:app --reload --port 8000
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $UvicornArgs
)
$env:UVICORN_LOG_CONFIG = Join-Path $PSScriptRoot "uvicorn_logging.json"
if (-not (Test-Path $env:UVICORN_LOG_CONFIG)) {
    Write-Error "Missing $env:UVICORN_LOG_CONFIG"
    exit 1
}
& py -m uvicorn @UvicornArgs
