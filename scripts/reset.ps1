# ---------------------------------------------------------------------------
# Reset the environment to a clean slate (Windows / PowerShell).
#
# Tears the stack down (including data volumes), starts it again, waits for the
# broker + Schema Registry to be healthy, and (re)creates the topics. Run this
# off-camera before recording the demo.
#
#   ./scripts/reset.ps1
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = if (Test-Path ".venv/Scripts/python.exe") { ".venv/Scripts/python.exe" } else { "python" }

Write-Host "==> Tearing down existing stack (with volumes)" -ForegroundColor Cyan
docker compose down -v

Write-Host "==> Starting stack" -ForegroundColor Cyan
docker compose up -d

Write-Host "==> Waiting for Schema Registry to be ready" -ForegroundColor Cyan
$ready = $false
foreach ($i in 1..30) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8081/subjects" -UseBasicParsing -TimeoutSec 3
        if ($resp.StatusCode -eq 200) { $ready = $true; break }
    } catch { Start-Sleep -Seconds 3 }
}
if (-not $ready) { throw "Schema Registry did not become ready in time." }
Write-Host "    Schema Registry is up." -ForegroundColor Green

Write-Host "==> Creating topics" -ForegroundColor Cyan
& $python -m order_pipeline.admin

Write-Host "==> Ready. Stack is clean; topics created." -ForegroundColor Green
