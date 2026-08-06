#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

is_placeholder() {
  local value="$1"
  [[ "$value" == *"<"* ]] && return 0
  [[ "$value" == *">"* ]] && return 0
  [[ "$value" == *"REAL_PI_IP"* ]] && return 0
  [[ "$value" == *"OCTOPRINT_PI_IP"* ]] && return 0
  [[ "$value" == *"YOUR_NEW_OCTOPRINT_API_KEY"* ]] && return 0
  [[ "$value" == *"your-octoprint-api-key"* ]] && return 0
  return 1
}

ARGS=("$@")

if [[ -n "${MOCK_MACHINE_TWIN_FILE:-}" ]]; then
  if [[ ! -f "${MOCK_MACHINE_TWIN_FILE}" ]]; then
    echo "MOCK_MACHINE_TWIN_FILE does not exist" >&2
    exit 1
  fi
  ARGS=(--mock-file "${MOCK_MACHINE_TWIN_FILE}" "${ARGS[@]}")
else
  if [[ -z "${OCTOPRINT_URL:-}" ]]; then
    echo "OCTOPRINT_URL is not set" >&2
    exit 1
  fi

  if [[ -z "${OCTOPRINT_API_KEY:-}" ]]; then
    echo "OCTOPRINT_API_KEY is not set" >&2
    exit 1
  fi

  if is_placeholder "${OCTOPRINT_URL}"; then
    echo "OCTOPRINT_URL appears to contain a placeholder" >&2
    exit 1
  fi

  if is_placeholder "${OCTOPRINT_API_KEY}"; then
    echo "OCTOPRINT_API_KEY appears to contain a placeholder" >&2
    exit 1
  fi
fi

mkdir -p "${REPO_ROOT}/logs/manufacturing_twin"
cd "${REPO_ROOT}"
exec python3 manufacturing_twin/manufacturing_twin_sync.py "${ARGS[@]}"
