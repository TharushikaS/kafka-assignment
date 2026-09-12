#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# End-to-end demo (Linux / macOS / Git Bash).
#
# Prerequisites: the stack is already running (docker compose up -d) and the
# Python dependencies are installed in the active environment.
#
#   ./scripts/demo.sh
# ---------------------------------------------------------------------------
set -euo pipefail

# Run from the project root (parent of this script's folder).
cd "$(dirname "$0")/.."

# Prefer the venv Python if present.
if [ -x ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then
  PYTHON=".venv/Scripts/python.exe"
else
  PYTHON="python"
fi

echo "==> Creating topics"
"$PYTHON" -m order_pipeline.admin

echo "==> Producing 200 orders (5% invalid)"
"$PYTHON" -m order_pipeline.producer --count 200 --invalid-rate 0.05 --rate 50 --seed 42

echo "==> Consuming (stops after 200 messages)"
"$PYTHON" -m order_pipeline.consumer --max-messages 200

echo "==> Inspecting the Dead Letter Queue"
"$PYTHON" -m order_pipeline.dlq_inspector --timeout 5
