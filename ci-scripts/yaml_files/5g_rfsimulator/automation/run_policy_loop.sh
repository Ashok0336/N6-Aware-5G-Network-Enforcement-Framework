#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${TESTBED_DIR}/policy_manager/config.yaml}"

# shellcheck disable=SC1091
source "${TESTBED_DIR}/testbed-env.sh"
mkdir -p "${TESTBED_DIR}/logs/policy"

echo "[policy-loop] authoritative config: ${CONFIG_PATH}"
echo "[policy-loop] default mode: dry-run"

cd "${TESTBED_DIR}"
exec env PYTHONPATH="${TESTBED_DIR}:${PYTHONPATH:-}" python3 -m policy_manager.app --config "${CONFIG_PATH}" "$@"
