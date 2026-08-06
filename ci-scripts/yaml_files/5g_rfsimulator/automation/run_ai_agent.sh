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

mkdir -p "${TESTBED_DIR}/logs/ai_agent"
cd "${TESTBED_DIR}"

echo "[ai-agent] testbed directory: ${TESTBED_DIR}"
exec python3 ai_agent/decision_agent.py --interval 2
