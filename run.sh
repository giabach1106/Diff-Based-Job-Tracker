#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

docker-compose down --remove-orphans
set +e
docker-compose up --build --abort-on-container-exit
status=$?
set -e
docker-compose down --remove-orphans
exit "$status"
