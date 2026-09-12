# ---------------------------------------------------------------------------
# End-to-end demo (Windows / PowerShell).
#
# Prerequisites: the stack is already running (docker compose up -d) and the
# Python dependencies are installed in the active environment.
#
#   ./scripts/demo.ps1
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Stop"

# Resolve the project root (parent of this script's folder) and run from there.
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Prefer the venv Python if present.
$python = if (Test-Path ".venv/Scripts/python.exe") { ".venv/Scripts/python.exe" } else { "python" }

Write-Host "==> Creating topics" -ForegroundColor Cyan
& $python -m order_pipeline.admin

Write-Host "==> Producing 200 orders (5% invalid)" -ForegroundColor Cyan
& $python -m order_pipeline.producer --count 200 --invalid-rate 0.05 --rate 50 --seed 42

Write-Host "==> Consuming (stops after 200 messages)" -ForegroundColor Cyan
& $python -m order_pipeline.consumer --max-messages 200

Write-Host "==> Inspecting the Dead Letter Queue" -ForegroundColor Cyan
& $python -m order_pipeline.dlq_inspector --timeout 5
