#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${TESTBED_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${TESTBED_DIR}/.venv/bin/activate"
elif [[ -f "${TESTBED_DIR}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${TESTBED_DIR}/venv/bin/activate"
fi

mkdir -p "${TESTBED_DIR}/logs/digital_twin"
cd "${SCRIPT_DIR}"

echo "[digital-twin] testbed directory: ${TESTBED_DIR}"
exec python3 ../digital_twin/twin_sync.py --interval 2
