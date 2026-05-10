param(
    [switch]$WithRouting,
    [int]$Workers = 4,
    [switch]$SkipSchema,
    [switch]$SkipPrecompute
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Title,
        [scriptblock]$Action
    )
    Write-Host ""
    Write-Host "==> $Title" -ForegroundColor Cyan
    & $Action
}

function Invoke-Docker {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )
    & docker @Args
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed: docker $($Args -join ' ')"
    }
}

function Wait-PostgisHealthy {
    param(
        [int]$TimeoutSeconds = 120
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect --format "{{.State.Health.Status}}" israel-gtfs-postgis 2>$null
        if ($status -eq "healthy") {
            Write-Host "PostGIS is healthy." -ForegroundColor Green
            return
        }
        Write-Host "Waiting for PostGIS health... (current: $status)"
        Start-Sleep -Seconds 3
    }
    throw "Timed out waiting for postgis health after $TimeoutSeconds seconds."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found in PATH."
}

if (-not (Test-Path (Join-Path $PSScriptRoot ".env")) -and (Test-Path (Join-Path $PSScriptRoot ".env.example"))) {
    Invoke-Step "Creating .env from .env.example" {
        Copy-Item (Join-Path $PSScriptRoot ".env.example") (Join-Path $PSScriptRoot ".env")
    }
}

$composeArgs = @("compose")
if ($WithRouting) {
    $composeArgs += @("--profile", "routing")
}
$composeArgs += @("up", "--build", "-d")

Invoke-Step "Starting Docker dev stack" {
    Invoke-Docker @composeArgs
}

Invoke-Step "Waiting for PostGIS to become healthy" {
    Wait-PostgisHealthy
}

if (-not $SkipSchema) {
    Invoke-Step "Applying PostGIS schema" {
        Invoke-Docker compose exec postgis psql -U user -d israel_gtfs -f /backend/sql/schema/db_postgis_schema.sql
    }
} else {
    Write-Host "Skipping schema step (-SkipSchema)." -ForegroundColor Yellow
}

if (-not $SkipPrecompute) {
    Invoke-Step "Running ingest + precompute pipeline" {
        Invoke-Docker compose run --rm backend python -m scripts.precompute_all_postgis --with-ingest --workers $Workers
    }
} else {
    Write-Host "Skipping precompute step (-SkipPrecompute)." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "Frontend: http://localhost:5173"
Write-Host "Backend health: http://localhost:8000/health"
if ($WithRouting) {
    Write-Host "Routing profile is enabled (Valhalla + OSRM)." -ForegroundColor Green
}
