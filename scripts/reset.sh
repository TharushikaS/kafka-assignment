#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Reset the environment to a clean slate (Linux / macOS / Git Bash).
#
# Tears the stack down (including data volumes), starts it again, waits for the
# broker + Schema Registry to be healthy, and (re)creates the topics. Run this
# off-camera before recording the demo.
#
#   ./scripts/reset.sh
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -x ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then
  PYTHON=".venv/Scripts/python.exe"
else
  PYTHON="python"
fi

echo "==> Tearing down existing stack (with volumes)"
docker compose down -v

echo "==> Starting stack"
docker compose up -d

echo "==> Waiting for Schema Registry to be ready"
ready=false
for _ in $(seq 1 30); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/subjects || true)" = "200" ]; then
    ready=true
    break
  fi
  sleep 3
done
if [ "$ready" != "true" ]; then
  echo "Schema Registry did not become ready in time." >&2
  exit 1
fi
echo "    Schema Registry is up."

echo "==> Creating topics"
"$PYTHON" -m order_pipeline.admin

echo "==> Ready. Stack is clean; topics created."
