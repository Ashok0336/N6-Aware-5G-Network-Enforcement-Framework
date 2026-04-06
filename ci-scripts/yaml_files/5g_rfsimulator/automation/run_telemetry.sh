#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_CONFIG_PATH="${TESTBED_DIR}/telemetry/config.yaml"
CONFIG_PATH="${TELEMETRY_CONFIG_PATH:-${CONFIG_PATH:-${DEFAULT_CONFIG_PATH}}}"
export TELEMETRY_CONFIG_PATH="${CONFIG_PATH}"

# shellcheck disable=SC1091
source "${TESTBED_DIR}/testbed-env.sh"
mkdir -p "${TESTBED_DIR}/logs/telemetry"

echo "[telemetry] authoritative config: ${CONFIG_PATH}"
echo "[telemetry] output directory: ${TESTBED_DIR}/logs/telemetry"

cd "${TESTBED_DIR}"
exec env PYTHONPATH="${TESTBED_DIR}:${PYTHONPATH:-}" python3 -m telemetry.telemetry_main --config "${CONFIG_PATH}" "$@"
