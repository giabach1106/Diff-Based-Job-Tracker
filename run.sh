#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if docker compose version >/dev/null 2>&1; then
  compose_cmd=(docker compose)
else
  compose_cmd=(docker-compose)
fi

set +e
"${compose_cmd[@]}" up --build --abort-on-container-exit --exit-code-from job-tracker job-tracker
status=$?
set -e
"${compose_cmd[@]}" rm -f -s job-tracker >/dev/null 2>&1 || true
exit "$status"
