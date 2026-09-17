#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_DIR}"
mkdir -p logs

# Keep Instagram discovery and bulk media traffic in separate phases.
docker compose stop media-worker >/dev/null 2>&1 || true

collection_exit=0
docker compose run --rm app python -m pipeline run-scheduled --skip-jobs --export "$@" || collection_exit=$?

worker_exit=0
docker compose up -d media-worker || worker_exit=$?
if (( worker_exit != 0 )); then
  echo "Failed to start media-worker after collection." >&2
  exit "${worker_exit}"
fi

exit "${collection_exit}"
