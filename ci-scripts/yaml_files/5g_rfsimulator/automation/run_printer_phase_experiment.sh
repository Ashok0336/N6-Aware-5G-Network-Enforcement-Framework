#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck disable=SC1091
source "${TESTBED_DIR}/testbed-env.sh"

DURATION_IDLE="${DURATION_IDLE:-60}"
DURATION_ACTIVE="${DURATION_ACTIVE:-120}"
LIVE_MODE="${LIVE_MODE:-live}"
MANUFACTURING_TWIN_ENABLED="${MANUFACTURING_TWIN_ENABLED:-true}"
MANUFACTURING_TWIN_LATEST_PATH="${MANUFACTURING_TWIN_LATEST_PATH:-logs/manufacturing_twin/latest_machine_twin_state.json}"
RESULTS_ROOT="${RESULTS_ROOT:-logs/experiments/ccnc}"
SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"

if [[ -z "${OCTOPRINT_URL:-}" ]]; then
  echo "[printer-phase] ERROR: OCTOPRINT_URL is required." >&2
  exit 1
fi

if [[ -z "${OCTOPRINT_API_KEY:-}" ]]; then
  echo "[printer-phase] ERROR: OCTOPRINT_API_KEY is required." >&2
  exit 1
fi

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${TESTBED_DIR}/${RESULTS_ROOT}/printer_phase_${TIMESTAMP}"
mkdir -p \
  "${RESULTS_DIR}/traffic/idle" \
  "${RESULTS_DIR}/traffic/active" \
  "${RESULTS_DIR}/telemetry" \
  "${RESULTS_DIR}/digital_twin" \
  "${RESULTS_DIR}/manufacturing_twin" \
  "${RESULTS_DIR}/evidence"

PIDS=()
LABELS=()
ERRORS=()

log() {
  echo "[printer-phase] $*"
}

warn() {
  echo "[printer-phase][WARN] $*" >&2
  ERRORS+=("$*")
}

start_background() {
  local label="$1"
  shift
  log "starting ${label}"
  "$@" >"${RESULTS_DIR}/${label}.log" 2>&1 &
  PIDS+=("$!")
  LABELS+=("$label")
}

cleanup() {
  local count="${#PIDS[@]}"
  for ((idx = count - 1; idx >= 0; idx--)); do
    local pid="${PIDS[$idx]}"
    local label="${LABELS[$idx]}"
    if kill -0 "$pid" >/dev/null 2>&1; then
      log "stopping ${label} pid=${pid}"
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  sleep 1
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup INT TERM EXIT

dump_ovs_flows() {
  local output_path="$1"
  {
    echo "# $(date -Is)"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>&1 || true
  } >"$output_path"
}

dump_onos_queue_counter() {
  local output_path="$1"
  {
    echo "# $(date -Is)"
    echo "## OVS set_queue flows installed through ONOS/queue-app path"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep "set_queue" || true
    echo
    echo "## ONOS devices"
    curl -fsS -u "${ONOS_AUTH}" "${ONOS_DEVICES_URL}" 2>&1 || true
  } >"$output_path"
}

queue_rule_presence() {
  local flow_file="$1"
  if grep -q "set_queue:1" "$flow_file" \
    && grep -q "set_queue:2" "$flow_file" \
    && grep -q "set_queue:3" "$flow_file"; then
    echo "all_present"
  elif grep -q "set_queue" "$flow_file"; then
    echo "partial"
  else
    echo "none"
  fi
}

copy_if_exists() {
  local source_path="$1"
  local destination_path="$2"
  if [[ -e "$source_path" ]]; then
    cp -a "$source_path" "$destination_path" 2>/dev/null || true
  fi
}

copy_artifacts() {
  copy_if_exists "${TESTBED_DIR}/logs/manufacturing_twin/machine_twin_state.jsonl" "${RESULTS_DIR}/manufacturing_twin/machine_twin_state.jsonl"
  copy_if_exists "${TESTBED_DIR}/logs/manufacturing_twin/machine_twin_metrics.csv" "${RESULTS_DIR}/manufacturing_twin/machine_twin_metrics.csv"
  copy_if_exists "${TESTBED_DIR}/logs/manufacturing_twin/latest_machine_twin_state.json" "${RESULTS_DIR}/manufacturing_twin/latest_machine_twin_state.json"
  if [[ -d "${TESTBED_DIR}/logs/telemetry" ]]; then
    cp -a "${TESTBED_DIR}/logs/telemetry/." "${RESULTS_DIR}/telemetry/" 2>/dev/null || true
  fi
  if [[ -d "${TESTBED_DIR}/logs/digital_twin" ]]; then
    cp -a "${TESTBED_DIR}/logs/digital_twin/." "${RESULTS_DIR}/digital_twin/" 2>/dev/null || true
  fi
  if [[ -d "${TESTBED_DIR}/logs/policy" ]]; then
    mkdir -p "${RESULTS_DIR}/policy"
    cp -a "${TESTBED_DIR}/logs/policy/." "${RESULTS_DIR}/policy/" 2>/dev/null || true
  fi
}

latest_phase_summary() {
  python3 - "${TESTBED_DIR}/logs/manufacturing_twin/latest_machine_twin_state.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print("phase=unknown machine_state=missing_latest_twin")
    raise SystemExit(0)
try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    print(f"phase=unknown machine_state=unreadable_latest_twin error={exc}")
    raise SystemExit(0)
phase = data.get("manufacturing_phase") or "unknown"
state = data.get("printer_state_text") or data.get("availability") or "unknown"
progress = data.get("job_progress_percent")
print(f"phase={phase} machine_state={state} job_progress_percent={progress}")
PY
}

policy_applied_count() {
  if [[ ! -f "${RESULTS_DIR}/policy_manager.log" ]]; then
    echo 0
    return 0
  fi
  grep -E "applied=(True|true)" "${RESULTS_DIR}/policy_manager.log" 2>/dev/null | wc -l
}

run_active_service_traffic() {
  log "running active manufacturing service traffic for ${DURATION_ACTIVE}s"
  "${SCRIPT_DIR}/run_control_traffic.sh" \
    --mapping "${SERVICE_MAPPING_PATH}" \
    --output-dir "${RESULTS_DIR}/traffic/active/real_time_control" \
    --duration "${DURATION_ACTIVE}" \
    >"${RESULTS_DIR}/control_traffic.log" 2>&1 &
  local control_pid="$!"

  "${SCRIPT_DIR}/run_sensor_traffic.sh" \
    --mapping "${SERVICE_MAPPING_PATH}" \
    --output-dir "${RESULTS_DIR}/traffic/active/sensor_telemetry" \
    --duration "${DURATION_ACTIVE}" \
    >"${RESULTS_DIR}/sensor_traffic.log" 2>&1 &
  local sensor_pid="$!"

  "${SCRIPT_DIR}/run_data_traffic.sh" \
    --mapping "${SERVICE_MAPPING_PATH}" \
    --output-dir "${RESULTS_DIR}/traffic/active/high_throughput_data" \
    --duration "${DURATION_ACTIVE}" \
    >"${RESULTS_DIR}/data_traffic.log" 2>&1 &
  local data_pid="$!"

  wait "$control_pid" "$sensor_pid" "$data_pid" || warn "one or more service traffic scripts exited non-zero"
}

log "result directory: ${RESULTS_DIR}"
log "safe mode: OctoPrint telemetry read-only; printer control is never invoked"
log "assuming OAI/ONOS/OVS containers are already running"

dump_ovs_flows "${RESULTS_DIR}/evidence/ovs_flows_before.txt"

start_background manufacturing_twin "${TESTBED_DIR}/manufacturing_twin/run_manufacturing_twin.sh"
sleep 2
start_background telemetry "${SCRIPT_DIR}/run_telemetry.sh"
sleep 2
start_background digital_twin "${SCRIPT_DIR}/run_digital_twin.sh"
sleep 2

if [[ -f "${TESTBED_DIR}/digital_twin/twin_snapshot_logger.py" ]]; then
  SNAPSHOT_DURATION="$(python3 - "${DURATION_IDLE}" "${DURATION_ACTIVE}" <<'PY'
import sys
print(int(float(sys.argv[1]) + float(sys.argv[2]) + 30))
PY
)"
  start_background twin_snapshot_logger \
    python3 "${TESTBED_DIR}/digital_twin/twin_snapshot_logger.py" \
      --output-dir "${RESULTS_DIR}/digital_twin" \
      --duration "${SNAPSHOT_DURATION}" \
      --interval 2
else
  warn "digital_twin/twin_snapshot_logger.py not found; skipping snapshot logger"
fi

POLICY_MODE_ARGS=()
case "${LIVE_MODE}" in
  live|active)
    POLICY_MODE_ARGS=(--live)
    ;;
  dry-run|dry_run|dryrun)
    POLICY_MODE_ARGS=(--dry-run)
    ;;
  *)
    warn "unknown LIVE_MODE=${LIVE_MODE}; defaulting policy manager to --live"
    POLICY_MODE_ARGS=(--live)
    ;;
esac

start_background policy_manager \
  env \
    LIVE_MODE="${LIVE_MODE}" \
    MANUFACTURING_TWIN_ENABLED="${MANUFACTURING_TWIN_ENABLED}" \
    MANUFACTURING_TWIN_LATEST_PATH="${MANUFACTURING_TWIN_LATEST_PATH}" \
    "${SCRIPT_DIR}/run_policy_manager.sh" "${POLICY_MODE_ARGS[@]}"
sleep 5

log "idle observation window: ${DURATION_IDLE}s"
sleep "${DURATION_IDLE}"

run_active_service_traffic

dump_ovs_flows "${RESULTS_DIR}/evidence/ovs_flows_after.txt"
dump_onos_queue_counter "${RESULTS_DIR}/evidence/onos_queue_counter_after.txt"

cleanup
trap - INT TERM EXIT
copy_artifacts

QUEUE_RULE_PRESENCE="$(queue_rule_presence "${RESULTS_DIR}/evidence/ovs_flows_after.txt")"
POLICY_APPLIED_COUNT="$(policy_applied_count)"
PHASE_SUMMARY="$(latest_phase_summary)"

if grep -RIE "Traceback|Exception|ERROR|enforcement_error" \
  "${RESULTS_DIR}/manufacturing_twin.log" \
  "${RESULTS_DIR}/telemetry.log" \
  "${RESULTS_DIR}/digital_twin.log" \
  "${RESULTS_DIR}/policy_manager.log" \
  >"${RESULTS_DIR}/errors.txt" 2>/dev/null; then
  ERRORS+=("see ${RESULTS_DIR}/errors.txt")
else
  rm -f "${RESULTS_DIR}/errors.txt"
fi

{
  echo "result_dir=${RESULTS_DIR}"
  echo "${PHASE_SUMMARY}"
  echo "queue_rule_presence=${QUEUE_RULE_PRESENCE}"
  echo "policy_applied_count=${POLICY_APPLIED_COUNT}"
  if [[ "${#ERRORS[@]}" -eq 0 ]]; then
    echo "errors=none"
  else
    printf 'errors=%s\n' "${ERRORS[*]}"
  fi
} >"${RESULTS_DIR}/summary.txt"

log "summary"
cat "${RESULTS_DIR}/summary.txt"
