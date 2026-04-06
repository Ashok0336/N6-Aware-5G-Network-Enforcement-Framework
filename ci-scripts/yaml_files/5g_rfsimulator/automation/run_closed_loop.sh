#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_TELEMETRY_CONFIG_PATH="${TESTBED_DIR}/telemetry/config.yaml"
TELEMETRY_CONFIG_PATH="${TELEMETRY_CONFIG_PATH:-${CONFIG_PATH:-${DEFAULT_TELEMETRY_CONFIG_PATH}}}"
export TELEMETRY_CONFIG_PATH

POLICY_ARGS=()
if [[ "${1:-}" == "--active" ]]; then
  POLICY_ARGS+=("--active")
  shift
elif [[ "${1:-}" == "--dry-run" ]]; then
  POLICY_ARGS+=("--dry-run")
  shift
fi

TELEMETRY_ARGS=("$@")

telemetry_pid=""
policy_pid=""

cleanup() {
  if [[ -n "${policy_pid}" ]] && kill -0 "${policy_pid}" >/dev/null 2>&1; then
    kill "${policy_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${telemetry_pid}" ]] && kill -0 "${telemetry_pid}" >/dev/null 2>&1; then
    kill "${telemetry_pid}" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "[closed-loop] starting telemetry service..."
echo "[closed-loop] telemetry config: ${TELEMETRY_CONFIG_PATH}"
"${SCRIPT_DIR}/run_telemetry.sh" "${TELEMETRY_ARGS[@]}" &
telemetry_pid="$!"

sleep 2

echo "[closed-loop] starting policy loop..."
"${SCRIPT_DIR}/run_policy_loop.sh" "${POLICY_ARGS[@]}" &
policy_pid="$!"

echo "[closed-loop] telemetry pid=${telemetry_pid}"
echo "[closed-loop] policy pid=${policy_pid}"
echo "[closed-loop] press Ctrl+C to stop both services."

wait "${telemetry_pid}" "${policy_pid}"
