# One-shot runner after Windows re-login: starts Docker Desktop, waits for the
# engine, builds/starts the compose stack (data on drive D via Docker Desktop
# config), waits for OpenMetadata to become healthy, then runs the SCALE=small
# experiment inside the spark container. Logs land in results\run_small_log.txt.
#
#   powershell -ExecutionPolicy Bypass -File scripts\windows_run_small.ps1
$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:PATH += ";D:\Docker\DockerDesktop\resources\bin"

function Wait-For($label, $script, $timeoutMin) {
    $deadline = (Get-Date).AddMinutes($timeoutMin)
    while ((Get-Date) -lt $deadline) {
        if (& $script) { Write-Host "$label: OK"; return $true }
        Start-Sleep -Seconds 10
    }
    Write-Host "$label: TIMEOUT after $timeoutMin min"
    return $false
}

# 1. Docker engine
if (-not (docker info 2>$null)) {
    Start-Process "D:\Docker\DockerDesktop\Docker Desktop.exe"
}
if (-not (Wait-For "docker engine" { docker info 2>$null } 8)) { exit 1 }

# 2. Build + start the stack (first build downloads images and Spark jars: 10-20 min)
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Write-Host "compose up failed"; exit 1 }

# 3. Wait until OpenMetadata is healthy
if (-not (Wait-For "openmetadata healthy" {
    (docker inspect --format '{{.State.Health.Status}}' openmetadata 2>$null) -eq 'healthy'
} 15)) {
    docker compose ps
    docker logs openmetadata-migrate 2>&1 | Select-Object -Last 40
    exit 1
}

# 4. Run the small-scale experiment inside the spark container
New-Item -ItemType Directory -Force results | Out-Null
docker compose exec -T spark bash -lc "cd /opt/lakehouse && SCALE=small bash scripts/run_all.sh" 2>&1 |
    Tee-Object -FilePath results\run_small_log.txt
Write-Host "=== done, exit=$LASTEXITCODE. Log: results\run_small_log.txt ==="
