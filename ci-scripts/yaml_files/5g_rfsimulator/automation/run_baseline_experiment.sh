#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./experiment_common.sh
source "${SCRIPT_DIR}/experiment_common.sh"

SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/../logs/experiments}"
RESULTS_DIR="${RESULTS_DIR:-}"

usage() {
  cat <<'EOF'
Usage: ./run_baseline_experiment.sh [--mapping PATH] [--duration SECONDS] [--results-dir DIR]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mapping)
      SERVICE_MAPPING_PATH="$2"
      shift 2
      ;;
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --results-dir)
      RESULTS_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[baseline-experiment] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$RESULTS_DIR" ]]; then
  RESULTS_DIR="$(prepare_results_dir "baseline" "$RESULTS_ROOT")"
fi
mkdir -p "$RESULTS_DIR"

declare -A PATHS=()
while IFS='=' read -r key value; do
  PATHS["$key"]="$value"
done < <(generate_runtime_configs "$RESULTS_DIR" "baseline" "$SERVICE_MAPPING_PATH")

PIDS=()
cleanup() {
  local code=$?
  stop_background_processes "${PIDS[@]}"
  exit "$code"
}
trap cleanup INT TERM EXIT

echo "[baseline-experiment] Results directory: ${PATHS[RESULTS_DIR]}"
echo "[baseline-experiment] Telemetry logs: ${PATHS[TELEMETRY_DIR]}"
echo "[baseline-experiment] Traffic logs: ${PATHS[TRAFFIC_DIR]}"

python3 "${SCRIPT_DIR}/telemetry_collector.py" \
  --config "${PATHS[TELEMETRY_CONFIG]}" \
  >"${RESULTS_DIR}/telemetry_stdout.log" 2>&1 &
PIDS+=("$!")
sleep 2

"${SCRIPT_DIR}/run_all_traffic.sh" \
  --mapping "$SERVICE_MAPPING_PATH" \
  --output-root "${PATHS[TRAFFIC_DIR]}" \
  --duration "$DURATION_SECONDS"

sleep 4
stop_background_processes "${PIDS[@]}"
trap - INT TERM EXIT

echo "[baseline-experiment] Completed."
echo "[baseline-experiment] Manifest: ${PATHS[MANIFEST_PATH]}"
echo "[baseline-experiment] Results stored under: ${PATHS[RESULTS_DIR]}"
