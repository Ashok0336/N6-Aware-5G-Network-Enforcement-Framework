#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./traffic_common.sh
source "${SCRIPT_DIR}/traffic_common.sh"

SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/../logs/traffic/high_throughput_data}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
SKIP_SERVER_SETUP=false

usage() {
  cat <<'EOF'
Usage: ./run_data_traffic.sh [--mapping PATH] [--output-dir DIR] [--duration SECONDS] [--skip-server-setup]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mapping)
      SERVICE_MAPPING_PATH="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --skip-server-setup)
      SKIP_SERVER_SETUP=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[data-traffic] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

TARGET_IP=""
TARGET_PORT=""
UDP_PAYLOAD_BYTES="1200"
UDP_PACKETS_PER_BURST="21"
UDP_BURST_INTERVAL_SECONDS="0.01"
UE_BINDINGS=()

while IFS=$'\t' read -r key v1 v2 v3 v4; do
  case "$key" in
    TARGET_IP)
      TARGET_IP="$v1"
      ;;
    TARGET_PORT)
      TARGET_PORT="$v1"
      ;;
    UDP_PAYLOAD_BYTES)
      if [[ -n "$v1" ]]; then
        UDP_PAYLOAD_BYTES="$v1"
      fi
      ;;
    UDP_PACKETS_PER_BURST)
      if [[ -n "$v1" ]]; then
        UDP_PACKETS_PER_BURST="$v1"
      fi
      ;;
    UDP_BURST_INTERVAL_SECONDS)
      if [[ -n "$v1" ]]; then
        UDP_BURST_INTERVAL_SECONDS="$v1"
      fi
      ;;
    UE_BINDING)
      UE_BINDINGS+=("$(format_ue_binding "$v1" "$v2" "$v3" "$v4")")
      ;;
  esac
done < <(get_service_plan_lines "high_throughput_data" "$SERVICE_MAPPING_PATH")

if [[ -z "$TARGET_IP" || -z "$TARGET_PORT" ]]; then
  echo "[data-traffic] ERROR: Missing TARGET_IP or TARGET_PORT in service plan." >&2
  exit 1
fi

if [[ "${#UE_BINDINGS[@]}" -eq 0 ]]; then
  echo "[data-traffic] ERROR: No UE bindings were resolved for high_throughput_data." >&2
  exit 1
fi

if [[ "$SKIP_SERVER_SETUP" != "true" ]]; then
  ensure_ext_dn_udp_sinks "$SERVICE_MAPPING_PATH"
fi

mkdir -p "$OUTPUT_DIR"
echo "[data-traffic] Resolved ${#UE_BINDINGS[@]} UE binding(s) for high_throughput_data."

PIDS=()
for binding in "${UE_BINDINGS[@]}"; do
  container_name=""
  ue_label=""
  log_file_name=""
  auxiliary_logs=""
  parse_ue_binding "$binding" container_name ue_label log_file_name auxiliary_logs
  if [[ -z "$container_name" || -z "$ue_label" || -z "$log_file_name" ]]; then
    echo "[data-traffic] ERROR: Malformed UE binding: ${binding@Q}" >&2
    exit 1
  fi
  log_path="${OUTPUT_DIR}/${log_file_name}"
  echo "[data-traffic] START ue=${ue_label} container=${container_name} bulk UDP -> ${TARGET_IP}:${TARGET_PORT} (${log_path})"
  run_udp_sender \
    "$container_name" \
    "high_throughput_data" \
    "bulk_udp" \
    "$TARGET_IP" \
    "$TARGET_PORT" \
    "$DURATION_SECONDS" \
    "$UDP_PAYLOAD_BYTES" \
    "$UDP_PACKETS_PER_BURST" \
    "$UDP_BURST_INTERVAL_SECONDS" \
    >"$log_path" 2>&1 &
  PIDS+=("$!")
done

wait "${PIDS[@]}"

echo "[data-traffic] Logs stored under: $OUTPUT_DIR"
