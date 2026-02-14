#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if docker compose version >/dev/null 2>&1; then
  compose_cmd=(docker compose)
else
  compose_cmd=(docker-compose)
fi

"${compose_cmd[@]}" down --remove-orphans
set +e
"${compose_cmd[@]}" up --build --abort-on-container-exit --exit-code-from job-tracker
status=$?
set -e
"${compose_cmd[@]}" down --remove-orphans
exit "$status"
