#!/usr/bin/env bash
set -euo pipefail

require_docker_access() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi

  echo "[FAIL] Docker API is not reachable from this shell." >&2
  echo "[FAIL] Ensure Docker is running and rerun the checks with permission to access /var/run/docker.sock." >&2
  exit 2
}
