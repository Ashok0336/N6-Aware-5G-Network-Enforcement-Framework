#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/ccnc_main_${TIMESTAMP}}"
TRAFFIC_DIR="${TESTBED_DIR}/logs/traffic"
DURATION="${DURATION:-${DURATION_SECONDS:-60}}"
LIVE_MODE="${LIVE_MODE:-dry-run}"
ONOS_CONT="${ONOS_CONT:-onos}"
KARAF_CLIENT="${KARAF_CLIENT:-/root/onos/apache-karaf-4.2.14/bin/client}"
KARAF_USER="${KARAF_USER:-karaf}"
KARAF_PASSWORD="${KARAF_PASSWORD:-karaf}"
ONOS_IP="${ONOS_IP:-192.168.71.160}"
ONOS_PORT="${ONOS_PORT:-8181}"
ONOS_AUTH="${ONOS_AUTH:-onos:rocks}"
UPF_PORT="${UPF_PORT:-1}"
EDN_PORT="${EDN_PORT:-2}"

mkdir -p "$BASE_DIR"

MODE_DIR=""
MODE_PIDS=()
POLICY_MANAGER_PID=""
POLICY_MANAGER_STATUS=""
ENFORCEMENT_RESET_STATUS=""
STATIC_QOS_INSTALL_STATUS=""
TRAFFIC_EXIT_CODE=0
TRAFFIC_LOG_COUNT=0
POLICY_MODE_ARGS=()

LIVE_MODE="${LIVE_MODE,,}"
case "$LIVE_MODE" in
  dry-run)
    POLICY_MODE_ARGS=(--dry-run)
    ;;
  live)
    POLICY_MODE_ARGS=(--live)
    ;;
  *)
    echo "[ccnc-main] ERROR: LIVE_MODE must be dry-run or live; got '${LIVE_MODE}'" >&2
    exit 2
    ;;
esac

warn() {
  echo "[ccnc-main] WARNING: $*" >&2
}

start_background() {
  local label="$1"
  shift
  echo "[ccnc-main] starting ${label}"
  "$@" >"${MODE_DIR}/${label}.log" 2>&1 &
  local pid="$!"
  MODE_PIDS+=("$pid")
  if [[ "$label" == "policy_manager" ]]; then
    POLICY_MANAGER_PID="$pid"
    POLICY_MANAGER_STATUS="running"
  fi
}

start_policy_manager() {
  start_background policy_manager bash "${SCRIPT_DIR}/run_policy_manager.sh" "${POLICY_MODE_ARGS[@]}"
}

stop_background() {
  local pid=""
  for pid in "${MODE_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  sleep 1
  for pid in "${MODE_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done
  MODE_PIDS=()
}

karaf_command() {
  local command="$1"
  docker exec "$ONOS_CONT" bash -lc "printf '%s\n' '${command}' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b" >/dev/null 2>&1
}

remove_onos_queue_app() {
  local bundle_ids
  bundle_ids="$(
    docker exec "$ONOS_CONT" bash -lc "printf 'bundle:list\n' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b 2>/dev/null | grep -Ei 'org\\.oai\\.slicequeue|ONOS Slice Queue App' | sed -n 's/^[^0-9]*\([0-9][0-9]*\).*/\1/p'" 2>/dev/null || true
  )"
  if [[ -z "$bundle_ids" ]]; then
    return 0
  fi
  local bundle_id=""
  for bundle_id in $bundle_ids; do
    karaf_command "bundle:stop ${bundle_id}" || true
    karaf_command "bundle:uninstall ${bundle_id}" || true
  done
}

wait_for_no_set_queue_flows() {
  for _ in {1..30}; do
    if ! docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null | grep -q "set_queue"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

get_onos_device_id() {
  curl -sS -u "$ONOS_AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/devices" 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); dev=[x.get("id") for x in d.get("devices",[]) if x.get("available") is True]; print(dev[0] if dev else "")'
}

post_basic_flow() {
  local device_id="$1"
  local eth_type="$2"
  local in_port="$3"
  local out_port="$4"
  local priority="$5"
  curl -sS -u "$ONOS_AUTH" -H "Content-Type: application/json" \
    -X POST "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/flows/${device_id}" \
    -d "{
      \"priority\": ${priority},
      \"timeout\": 0,
      \"isPermanent\": true,
      \"deviceId\": \"${device_id}\",
      \"treatment\": {\"instructions\": [{\"type\": \"OUTPUT\", \"port\": \"${out_port}\"}]},
      \"selector\": {\"criteria\": [
        {\"type\": \"IN_PORT\", \"port\": \"${in_port}\"},
        {\"type\": \"ETH_TYPE\", \"ethType\": \"${eth_type}\"}
      ]}
    }" >/dev/null
}

install_basic_n6_forwarding() {
  local device_id
  device_id="$(get_onos_device_id || true)"
  if [[ -z "$device_id" ]]; then
    warn "could not resolve ONOS device for basic N6 forwarding"
    return 1
  fi
  post_basic_flow "$device_id" "0x0800" "$UPF_PORT" "$EDN_PORT" 5000 || return 1
  post_basic_flow "$device_id" "0x0800" "$EDN_PORT" "$UPF_PORT" 5000 || return 1
  post_basic_flow "$device_id" "0x0806" "$UPF_PORT" "$EDN_PORT" 45000 || return 1
  post_basic_flow "$device_id" "0x0806" "$EDN_PORT" "$UPF_PORT" 45000 || return 1
}

save_queue_evidence() {
  local file_name="$1"
  {
    echo "# $(date -Is)"
    docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null || true
  } >"${MODE_DIR}/${file_name}"
}

reset_enforcement_for_mode() {
  local mode="$1"
  echo "[ccnc-main] resetting ONOS enforcement for ${mode}"
  ENFORCEMENT_RESET_STATUS="failed"
  if [[ ! -x "${TESTBED_DIR}/clear-slice-flows.sh" ]]; then
    warn "${mode}: clear-slice-flows.sh is missing or not executable"
    return 1
  fi
  if ! bash "${TESTBED_DIR}/clear-slice-flows.sh" >"${MODE_DIR}/clear_slice_flows.log" 2>&1; then
    warn "${mode}: ONOS queue cleanup did not verify"
    return 1
  fi
  if ! wait_for_no_set_queue_flows; then
    warn "${mode}: set_queue flows are still visible after clear-slice-flows.sh"
    return 1
  fi
  ENFORCEMENT_RESET_STATUS="ok"
  return 0
}

clear_traffic_logs() {
  rm -rf "$TRAFFIC_DIR"
  mkdir -p "$TRAFFIC_DIR"
}

copy_traffic_logs() {
  local destination="${MODE_DIR}/traffic"
  rm -rf "$destination"
  mkdir -p "$destination"
  if [[ -d "$TRAFFIC_DIR" ]]; then
    cp -a "${TRAFFIC_DIR}/." "$destination/" 2>/dev/null || true
  fi
}

traffic_log_count() {
  if [[ ! -d "${MODE_DIR}/traffic" ]]; then
    echo 0
    return 0
  fi
  find "${MODE_DIR}/traffic" -type f | wc -l
}

write_mode_status() {
  local mode="$1"
  local traffic_exit_code="$2"
  local log_count="$3"
  local static_qos_install_status="${4:-}"
  local policy_manager_status="${5:-}"
  local enforcement_reset_status="${6:-}"
  python3 - "$MODE_DIR" "$mode" "$traffic_exit_code" "$log_count" "$static_qos_install_status" "$policy_manager_status" "$enforcement_reset_status" <<'PY'
import json
import sys
from pathlib import Path

mode_dir = Path(sys.argv[1])
mode = sys.argv[2]
traffic_exit_code = int(sys.argv[3])
log_count = int(sys.argv[4])
static_qos_install_status = sys.argv[5]
policy_manager_status = sys.argv[6]
enforcement_reset_status = sys.argv[7]
status = (
    "ok"
    if traffic_exit_code == 0
    and log_count > 0
    and enforcement_reset_status != "failed"
    and policy_manager_status != "failed"
    and static_qos_install_status != "failed"
    else "failed"
)
payload = {
    "mode": mode,
    "status": status,
    "traffic_exit_code": traffic_exit_code,
    "traffic_log_count": log_count,
}
if static_qos_install_status:
    payload["static_qos_install_status"] = static_qos_install_status
if policy_manager_status:
    payload["policy_manager_status"] = policy_manager_status
if enforcement_reset_status:
    payload["enforcement_reset_status"] = enforcement_reset_status
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

write_current_mode_status() {
  local mode="$1"
  write_mode_status "$mode" "$TRAFFIC_EXIT_CODE" "$TRAFFIC_LOG_COUNT" "$STATIC_QOS_INSTALL_STATUS" "$POLICY_MANAGER_STATUS" "$ENFORCEMENT_RESET_STATUS"
}

run_analysis() {
  local mode="$1"
  echo "[ccnc-main] analyzing ${mode}"
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "$MODE_DIR" >"${MODE_DIR}/analyze_sla_violations.log" 2>&1 || warn "${mode}: SLA analysis failed"
  python3 "${SCRIPT_DIR}/policy_decision_logger.py" "$MODE_DIR" >"${MODE_DIR}/policy_decision_logger.log" 2>&1 || warn "${mode}: policy decision logging failed"
  python3 "${SCRIPT_DIR}/compute_manufacturing_utility.py" "$MODE_DIR" >"${MODE_DIR}/compute_manufacturing_utility.log" 2>&1 || warn "${mode}: manufacturing utility computation failed"
}

run_traffic_for_mode() {
  local mode="$1"
  local log_name="$2"
  local static_qos_install_status="${3:-}"
  echo "[ccnc-main] running ${mode} traffic for ${DURATION}s"
  bash "${SCRIPT_DIR}/run_all_traffic.sh" --duration "$DURATION" >"${MODE_DIR}/${log_name}" 2>&1
  local traffic_code=$?
  if [[ "$traffic_code" -ne 0 ]]; then
    warn "${mode}: run_all_traffic.sh exited with code ${traffic_code}; copying any logs that were produced"
  fi
  copy_traffic_logs
  local logs_found
  logs_found="$(traffic_log_count)"
  if [[ "$logs_found" -eq 0 ]]; then
    warn "${mode}: no traffic logs found after run"
  fi
  TRAFFIC_EXIT_CODE="$traffic_code"
  TRAFFIC_LOG_COUNT="$logs_found"
  STATIC_QOS_INSTALL_STATUS="$static_qos_install_status"
}

set_queue_flows_visible() {
  local flows
  flows="$(docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null || true)"
  echo "$flows" | grep -q "set_queue:1" \
    && echo "$flows" | grep -q "set_queue:2" \
    && echo "$flows" | grep -q "set_queue:3"
}

any_set_queue_flow_visible() {
  docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null | grep -q "set_queue"
}

run_fifo() {
  local mode="fifo"
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_PID=""
  POLICY_MANAGER_STATUS=""
  STATIC_QOS_INSTALL_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_traffic_logs
  reset_enforcement_for_mode "$mode" || true
  save_queue_evidence "onos_queue_counter_before.txt"
  if any_set_queue_flow_visible; then
    warn "${mode}: FIFO is contaminated by set_queue flows"
    ENFORCEMENT_RESET_STATUS="failed"
  fi
  run_traffic_for_mode "$mode" "fifo_experiment.log"
  save_queue_evidence "onos_queue_counter_after.txt"
  write_current_mode_status "$mode"
  run_analysis "$mode"
}

run_static_qos() {
  local mode="static_qos"
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_PID=""
  POLICY_MANAGER_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_traffic_logs
  reset_enforcement_for_mode "$mode" || true
  local static_qos_install_status="not_run"
  if [[ -x "${TESTBED_DIR}/install-slice-flows.sh" ]]; then
    echo "[ccnc-main] installing static N6 slice flows"
    if bash "${TESTBED_DIR}/install-slice-flows.sh" >"${MODE_DIR}/static_qos_install_flows.log" 2>&1 && set_queue_flows_visible; then
      static_qos_install_status="ok"
    else
      static_qos_install_status="failed"
      warn "${mode}: ONOS queue rule installation did not verify"
    fi
  else
    static_qos_install_status="missing_installer"
    warn "${mode}: install-slice-flows.sh is not executable"
  fi
  save_queue_evidence "onos_queue_counter_before.txt"
  run_traffic_for_mode "$mode" "static_qos_experiment.log" "$static_qos_install_status"
  save_queue_evidence "onos_queue_counter_after.txt"
  write_current_mode_status "$mode"
  run_analysis "$mode"
}

check_policy_manager_status() {
  if [[ -z "$POLICY_MANAGER_PID" ]]; then
    return 0
  fi
  if ! kill -0 "$POLICY_MANAGER_PID" >/dev/null 2>&1; then
    POLICY_MANAGER_STATUS="failed"
    warn "policy_manager exited before mode completed"
  elif grep -Eq "Traceback|JSONDecodeError|Exception|enforcement_error=" "${MODE_DIR}/policy_manager.log" 2>/dev/null; then
    POLICY_MANAGER_STATUS="failed"
    warn "policy_manager error detected"
  elif [[ "$LIVE_MODE" == "live" ]] && grep -q "mode: dry-run" "${MODE_DIR}/policy_manager.log" 2>/dev/null; then
    POLICY_MANAGER_STATUS="failed"
    warn "LIVE_MODE=live but policy_manager.log shows dry-run mode"
  else
    POLICY_MANAGER_STATUS="ok"
  fi
}

run_n6_only() {
  local mode="n6_only"
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_PID=""
  POLICY_MANAGER_STATUS="not_started"
  STATIC_QOS_INSTALL_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_traffic_logs
  reset_enforcement_for_mode "$mode" || true
  save_queue_evidence "onos_queue_counter_before.txt"
  trap stop_background INT TERM EXIT
  start_background telemetry bash "${SCRIPT_DIR}/run_telemetry.sh"
  sleep 2
  start_policy_manager
  sleep 2
  check_policy_manager_status
  run_traffic_for_mode "$mode" "n6_only_experiment.log"
  check_policy_manager_status
  stop_background
  trap - INT TERM EXIT
  save_queue_evidence "onos_queue_counter_after.txt"
  write_current_mode_status "$mode"
  run_analysis "$mode"
}

run_dt_assisted() {
  local mode="dt_assisted"
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_PID=""
  POLICY_MANAGER_STATUS="not_started"
  STATIC_QOS_INSTALL_STATUS=""
  mkdir -p "${MODE_DIR}/digital_twin"
  clear_traffic_logs
  reset_enforcement_for_mode "$mode" || true
  save_queue_evidence "onos_queue_counter_before.txt"
  trap stop_background INT TERM EXIT
  start_background telemetry bash "${SCRIPT_DIR}/run_telemetry.sh"
  sleep 2
  start_background digital_twin bash "${SCRIPT_DIR}/run_digital_twin.sh"
  sleep 2
  start_background twin_snapshot_logger python3 "${TESTBED_DIR}/digital_twin/twin_snapshot_logger.py" --output-dir "${MODE_DIR}/digital_twin" --duration "$DURATION" --interval 1
  sleep 1
  start_policy_manager
  sleep 2
  check_policy_manager_status
  run_traffic_for_mode "$mode" "dt_assisted_traffic.log"
  check_policy_manager_status
  stop_background
  trap - INT TERM EXIT
  save_queue_evidence "onos_queue_counter_after.txt"
  write_current_mode_status "$mode"
  run_analysis "$mode"
  python3 "${TESTBED_DIR}/digital_twin/twin_accuracy_evaluator.py" "$MODE_DIR" --telemetry-dir "${TESTBED_DIR}/logs/telemetry" --snapshot-dir "${MODE_DIR}/digital_twin" >"${MODE_DIR}/twin_accuracy_evaluator.log" 2>&1 || warn "${mode}: twin accuracy evaluation failed"
}

write_cross_mode_summary() {
  python3 - "$BASE_DIR" <<'PY'
import csv
import json
from pathlib import Path
import sys

base_dir = Path(sys.argv[1])
modes = ["fifo", "static_qos", "n6_only", "dt_assisted"]
rows = []
for mode in modes:
    mode_dir = base_dir / mode
    status_path = mode_dir / "mode_status.json"
    summary_path = mode_dir / "summary_metrics.json"
    utility_path = mode_dir / "manufacturing_utility_summary.json"
    policy_path = mode_dir / "policy_decision_summary.json"
    twin_path = mode_dir / "twin_accuracy_summary.json"
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    utility = json.loads(utility_path.read_text()) if utility_path.exists() else {}
    policy = json.loads(policy_path.read_text()) if policy_path.exists() else {}
    twin = json.loads(twin_path.read_text()) if twin_path.exists() else {}
    rows.append({
        "mode": mode,
        "status": status.get("status", "failed"),
        "traffic_exit_code": status.get("traffic_exit_code", ""),
        "traffic_log_count": status.get("traffic_log_count", 0),
        "sla_checks": summary.get("sla_checks", 0),
        "sla_violations": summary.get("sla_violations", 0),
        "sla_violation_rate": summary.get("sla_violation_rate", 0.0),
        "manufacturing_utility_all": utility.get("phase_utility", {}).get("all", 0.0),
        "policy_decisions": policy.get("decision_count", 0),
        "policy_changes": policy.get("policy_change_count", 0),
        "twin_matched_samples": twin.get("matched_samples", ""),
        "twin_mean_relative_error": twin.get("mean_relative_error", ""),
        "twin_mean_staleness_seconds": twin.get("mean_staleness_seconds", ""),
    })

fifo = next((row for row in rows if row["mode"] == "fifo"), None)
fifo_rate = float(fifo["sla_violation_rate"]) if fifo else 0.0
for row in rows:
    rate = float(row["sla_violation_rate"])
    row["sla_violation_reduction_vs_fifo"] = ((fifo_rate - rate) / fifo_rate) if fifo_rate else 0.0

fields = [
    "mode",
    "status",
    "traffic_exit_code",
    "traffic_log_count",
    "sla_checks",
    "sla_violations",
    "sla_violation_rate",
    "sla_violation_reduction_vs_fifo",
    "manufacturing_utility_all",
    "policy_decisions",
    "policy_changes",
    "twin_matched_samples",
    "twin_mean_relative_error",
    "twin_mean_staleness_seconds",
]
out = base_dir / "ccnc_cross_mode_summary.csv"
with out.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
print(f"[ccnc-main] cross-mode summary: {out}")
PY
}

echo "[ccnc-main] results directory: ${BASE_DIR}"
echo "[ccnc-main] traffic duration: ${DURATION}s"
echo "[ccnc-main] live mode: ${LIVE_MODE}"
run_fifo
run_static_qos
run_n6_only
run_dt_assisted
write_cross_mode_summary
echo "[ccnc-main] completed digital twin-assisted N6 edge enforcement result generation."
