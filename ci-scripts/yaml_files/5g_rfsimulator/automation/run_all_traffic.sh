#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./traffic_common.sh
source "${SCRIPT_DIR}/traffic_common.sh"

SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/../logs/traffic}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"

usage() {
  cat <<'EOF'
Usage: ./run_all_traffic.sh [--mapping PATH] [--output-root DIR] [--duration SECONDS]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mapping)
      SERVICE_MAPPING_PATH="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[all-traffic] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_ROOT"
ensure_ext_dn_udp_sinks "$SERVICE_MAPPING_PATH"

PIDS=()
"${SCRIPT_DIR}/run_control_traffic.sh" \
  --mapping "$SERVICE_MAPPING_PATH" \
  --output-dir "${OUTPUT_ROOT}/real_time_control" \
  --duration "$DURATION_SECONDS" \
  --skip-server-setup &
PIDS+=("$!")

"${SCRIPT_DIR}/run_data_traffic.sh" \
  --mapping "$SERVICE_MAPPING_PATH" \
  --output-dir "${OUTPUT_ROOT}/high_throughput_data" \
  --duration "$DURATION_SECONDS" \
  --skip-server-setup &
PIDS+=("$!")

"${SCRIPT_DIR}/run_sensor_traffic.sh" \
  --mapping "$SERVICE_MAPPING_PATH" \
  --output-dir "${OUTPUT_ROOT}/sensor_telemetry" \
  --duration "$DURATION_SECONDS" \
  --skip-server-setup &
PIDS+=("$!")

wait "${PIDS[@]}"

echo "[all-traffic] Multi-service traffic logs stored under: $OUTPUT_ROOT"
