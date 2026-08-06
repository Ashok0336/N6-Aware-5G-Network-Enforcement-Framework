#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/manufacturing_workflow_${TIMESTAMP}}"
SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
PHASE_SECONDS="${PHASE_SECONDS:-30}"

mkdir -p "$RESULTS_DIR"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup INT TERM EXIT

start_background() {
  local label="$1"
  shift
  "$@" >"${RESULTS_DIR}/${label}.log" 2>&1 &
  PIDS+=("$!")
}

analyze_phase() {
  local phase_dir="$1"
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "$phase_dir" >"${phase_dir}/analyze_sla_violations.log" 2>&1 || true
  python3 "${SCRIPT_DIR}/policy_decision_logger.py" "$phase_dir" >"${phase_dir}/policy_decision_logger.log" 2>&1 || true
  python3 "${SCRIPT_DIR}/compute_manufacturing_utility.py" "$phase_dir" >"${phase_dir}/compute_manufacturing_utility.log" 2>&1 || true
}

run_job_setup() {
  local phase_dir="${RESULTS_DIR}/job_setup"
  mkdir -p "$phase_dir"
  echo "[manufacturing-workflow] phase=job_setup"
  "${SCRIPT_DIR}/run_data_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/high_throughput_data" --duration "$PHASE_SECONDS" >"${phase_dir}/data_traffic.log" 2>&1 &
  local p1="$!"
  "${SCRIPT_DIR}/run_sensor_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/sensor_telemetry" --duration "$PHASE_SECONDS" >"${phase_dir}/sensor_traffic.log" 2>&1 &
  local p2="$!"
  wait "$p1" "$p2" || true
  analyze_phase "$phase_dir"
}

run_active_printing() {
  local phase_dir="${RESULTS_DIR}/active_printing"
  mkdir -p "$phase_dir"
  echo "[manufacturing-workflow] phase=active_printing"
  "${SCRIPT_DIR}/run_control_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/real_time_control" --duration "$PHASE_SECONDS" >"${phase_dir}/control_traffic.log" 2>&1 &
  local p1="$!"
  "${SCRIPT_DIR}/run_sensor_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/sensor_telemetry" --duration "$PHASE_SECONDS" >"${phase_dir}/sensor_traffic.log" 2>&1 &
  local p2="$!"
  "${SCRIPT_DIR}/run_data_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/high_throughput_data" --duration "$PHASE_SECONDS" >"${phase_dir}/data_traffic.log" 2>&1 &
  local p3="$!"
  wait "$p1" "$p2" "$p3" || true
  analyze_phase "$phase_dir"
}

run_quality_inspection() {
  local phase_dir="${RESULTS_DIR}/quality_inspection"
  mkdir -p "$phase_dir"
  echo "[manufacturing-workflow] phase=quality_inspection"
  "${SCRIPT_DIR}/run_sensor_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/sensor_telemetry" --duration "$PHASE_SECONDS" >"${phase_dir}/sensor_traffic.log" 2>&1 &
  local p1="$!"
  "${SCRIPT_DIR}/run_data_traffic.sh" --mapping "$SERVICE_MAPPING_PATH" --output-dir "${phase_dir}/traffic/high_throughput_data" --duration "$PHASE_SECONDS" >"${phase_dir}/data_traffic.log" 2>&1 &
  local p2="$!"
  wait "$p1" "$p2" || true
  analyze_phase "$phase_dir"
}

echo "[manufacturing-workflow] results directory: ${RESULTS_DIR}"
start_background telemetry "${SCRIPT_DIR}/run_telemetry.sh"
sleep 2
start_background digital_twin "${SCRIPT_DIR}/run_digital_twin.sh"
sleep 2
start_background policy_manager "${SCRIPT_DIR}/run_policy_manager.sh"
sleep 2
start_background twin_snapshot_logger python3 "${TESTBED_DIR}/digital_twin/twin_snapshot_logger.py" --output-dir "$RESULTS_DIR" --duration "$((PHASE_SECONDS * 3 + 10))" --interval 2

run_job_setup
run_active_printing
run_quality_inspection

cleanup
trap - INT TERM EXIT
python3 "${TESTBED_DIR}/digital_twin/twin_accuracy_evaluator.py" "$RESULTS_DIR" --telemetry-dir "${TESTBED_DIR}/logs/telemetry" --snapshot-dir "$RESULTS_DIR" >"${RESULTS_DIR}/twin_accuracy_evaluator.log" 2>&1 || true
python3 "${SCRIPT_DIR}/compute_manufacturing_utility.py" "$RESULTS_DIR" >"${RESULTS_DIR}/compute_manufacturing_utility.log" 2>&1 || true
echo "[manufacturing-workflow] completed. Results stored under: ${RESULTS_DIR}"
