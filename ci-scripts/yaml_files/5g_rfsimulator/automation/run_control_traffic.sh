#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./traffic_common.sh
source "${SCRIPT_DIR}/traffic_common.sh"

SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/../logs/traffic/real_time_control}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
SKIP_SERVER_SETUP=false

usage() {
  cat <<'EOF'
Usage: ./run_control_traffic.sh [--mapping PATH] [--output-dir DIR] [--duration SECONDS] [--skip-server-setup]
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
      echo "[control-traffic] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

EXT_DN_CONTAINER=""
TARGET_IP=""
TARGET_PORT=""
PING_INTERVAL_SECONDS="1.0"
UDP_PAYLOAD_BYTES="128"
UDP_PACKETS_PER_BURST="1"
UDP_BURST_INTERVAL_SECONDS="0.1"
UE_BINDINGS=()

while IFS=$'\t' read -r key v1 v2 v3 v4; do
  case "$key" in
    EXT_DN_CONTAINER)
      EXT_DN_CONTAINER="$v1"
      ;;
    TARGET_IP)
      TARGET_IP="$v1"
      ;;
    TARGET_PORT)
      TARGET_PORT="$v1"
      ;;
    PING_INTERVAL_SECONDS)
      if [[ -n "$v1" ]]; then
        PING_INTERVAL_SECONDS="$v1"
      fi
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
done < <(get_service_plan_lines "real_time_control" "$SERVICE_MAPPING_PATH")

if [[ -z "$TARGET_IP" || -z "$TARGET_PORT" ]]; then
  echo "[control-traffic] ERROR: Missing TARGET_IP or TARGET_PORT in service plan." >&2
  exit 1
fi

if [[ "${#UE_BINDINGS[@]}" -eq 0 ]]; then
  echo "[control-traffic] ERROR: No UE bindings were resolved for real_time_control." >&2
  exit 1
fi

if [[ "$SKIP_SERVER_SETUP" != "true" ]]; then
  ensure_ext_dn_udp_sinks "$SERVICE_MAPPING_PATH"
fi

mkdir -p "$OUTPUT_DIR"
echo "[control-traffic] Resolved ${#UE_BINDINGS[@]} UE binding(s) for real_time_control."

PING_COUNT="$(python3 - "$DURATION_SECONDS" "$PING_INTERVAL_SECONDS" <<'PY'
import math
import sys
duration = float(sys.argv[1])
interval = float(sys.argv[2]) if float(sys.argv[2]) > 0 else 1.0
print(max(1, int(math.ceil(duration / interval))))
PY
)"

PIDS=()
for binding in "${UE_BINDINGS[@]}"; do
  container_name=""
  ue_label=""
  log_file_name=""
  auxiliary_logs=""
  parse_ue_binding "$binding" container_name ue_label log_file_name auxiliary_logs
  if [[ -z "$container_name" || -z "$ue_label" || -z "$log_file_name" ]]; then
    echo "[control-traffic] ERROR: Malformed UE binding: ${binding@Q}" >&2
    exit 1
  fi
  control_log_path="${OUTPUT_DIR}/${log_file_name}"
  echo "[control-traffic] START ue=${ue_label} container=${container_name} ping -> ${TARGET_IP} (${control_log_path})"
  docker exec "$container_name" bash -lc \
    "ping -i ${PING_INTERVAL_SECONDS} -c ${PING_COUNT} ${TARGET_IP}" \
    >"$control_log_path" 2>&1 &
  PIDS+=("$!")

  IFS=',' read -r udp_log_name _ <<<"$auxiliary_logs"
  if [[ -n "${udp_log_name}" ]]; then
    udp_log_path="${OUTPUT_DIR}/${udp_log_name}"
    echo "[control-traffic] START ue=${ue_label} container=${container_name} low-rate UDP -> ${TARGET_IP}:${TARGET_PORT} (${udp_log_path})"
    run_udp_sender \
      "$container_name" \
      "real_time_control" \
      "control_probe" \
      "$TARGET_IP" \
      "$TARGET_PORT" \
      "$DURATION_SECONDS" \
      "$UDP_PAYLOAD_BYTES" \
      "$UDP_PACKETS_PER_BURST" \
      "$UDP_BURST_INTERVAL_SECONDS" \
      >"$udp_log_path" 2>&1 &
    PIDS+=("$!")
  fi
done

wait "${PIDS[@]}"

echo "[control-traffic] Logs stored under: $OUTPUT_DIR"
