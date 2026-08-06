#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=./traffic_common.sh
source "${SCRIPT_DIR}/traffic_common.sh"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/burst_cross_mode_${TIMESTAMP}}"
SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
WARMUP="${WARMUP:-${WARMUP_SECONDS:-30}}"
BURST="${BURST:-${BURST_SECONDS:-60}}"
RECOVERY="${RECOVERY:-${RECOVERY_SECONDS:-60}}"
LIVE_MODE="${LIVE_MODE:-live}"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"

mkdir -p "$RESULTS_DIR"

MODE_DIR=""
MODE_PIDS=()
POLICY_MANAGER_PID=""
POLICY_MANAGER_STATUS=""
ENFORCEMENT_STATUS=""
STATIC_QOS_INSTALL_STATUS=""

warn() {
  echo "[burst-cross-mode] WARNING: $*" >&2
}

start_background() {
  local label="$1"
  shift
  echo "[burst-cross-mode] starting ${label}"
  "$@" >"${MODE_DIR}/${label}.log" 2>&1 &
  local pid="$!"
  MODE_PIDS+=("$pid")
  if [[ "$label" == "policy_manager" ]]; then
    POLICY_MANAGER_PID="$pid"
    POLICY_MANAGER_STATUS="running"
  fi
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
  POLICY_MANAGER_PID=""
}

save_queue_evidence() {
  local file_name="$1"
  {
    echo "# $(date -Is)"
    docker exec "$OVS_CONTAINER_NAME" ovs-ofctl -O OpenFlow13 dump-flows "$OVS_BRIDGE_NAME" 2>/dev/null || true
  } >"${MODE_DIR}/${file_name}"
}

set_queue_flows_visible() {
  local flows
  flows="$(docker exec "$OVS_CONTAINER_NAME" ovs-ofctl -O OpenFlow13 dump-flows "$OVS_BRIDGE_NAME" 2>/dev/null || true)"
  echo "$flows" | grep -q "set_queue:1" \
    && echo "$flows" | grep -q "set_queue:2" \
    && echo "$flows" | grep -q "set_queue:3"
}

any_set_queue_flow_visible() {
  docker exec "$OVS_CONTAINER_NAME" ovs-ofctl -O OpenFlow13 dump-flows "$OVS_BRIDGE_NAME" 2>/dev/null | grep -q "set_queue"
}

clear_enforcement() {
  local mode="$1"
  ENFORCEMENT_STATUS="failed"
  echo "[burst-cross-mode] clearing ONOS queue rules for ${mode}"
  if bash "${TESTBED_DIR}/clear-slice-flows.sh" >"${MODE_DIR}/clear_slice_flows.log" 2>&1; then
    ENFORCEMENT_STATUS="ok"
  else
    warn "${mode}: clear-slice-flows.sh failed"
  fi
}

install_static_qos() {
  STATIC_QOS_INSTALL_STATUS="failed"
  echo "[burst-cross-mode] installing ONOS queue app rules"
  if bash "${TESTBED_DIR}/install-slice-flows.sh" >"${MODE_DIR}/install_slice_flows.log" 2>&1 && set_queue_flows_visible; then
    STATIC_QOS_INSTALL_STATUS="ok"
  else
    warn "ONOS queue app rules did not verify"
  fi
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
  elif grep -q "mode: dry-run" "${MODE_DIR}/policy_manager.log" 2>/dev/null; then
    POLICY_MANAGER_STATUS="failed"
    warn "policy_manager unexpectedly ran in dry-run mode"
  else
    POLICY_MANAGER_STATUS="ok"
  fi
}

run_normal_phase_traffic() {
  local phase_dir="$1"
  local duration="$2"
  bash "${SCRIPT_DIR}/run_all_traffic.sh" \
    --mapping "$SERVICE_MAPPING_PATH" \
    --output-root "${phase_dir}/traffic" \
    --duration "$duration" \
    >"${phase_dir}/traffic.log" 2>&1
}

run_burst_phase_traffic() {
  local phase_dir="$1"
  local duration="$2"
  mkdir -p "${phase_dir}/traffic"
  ensure_ext_dn_udp_sinks "$SERVICE_MAPPING_PATH"
  bash "${SCRIPT_DIR}/run_control_traffic.sh" \
    --mapping "$SERVICE_MAPPING_PATH" \
    --output-dir "${phase_dir}/traffic/real_time_control" \
    --duration "$duration" \
    --skip-server-setup \
    >"${phase_dir}/control_traffic.log" 2>&1 &
  local p1="$!"
  bash "${SCRIPT_DIR}/run_sensor_traffic.sh" \
    --mapping "$SERVICE_MAPPING_PATH" \
    --output-dir "${phase_dir}/traffic/sensor_telemetry" \
    --duration "$duration" \
    --skip-server-setup \
    >"${phase_dir}/sensor_traffic.log" 2>&1 &
  local p2="$!"
  bash "${SCRIPT_DIR}/run_data_traffic.sh" \
    --mapping "$SERVICE_MAPPING_PATH" \
    --output-dir "${phase_dir}/traffic/high_throughput_data" \
    --duration "$duration" \
    --skip-server-setup \
    >"${phase_dir}/data_traffic_primary.log" 2>&1 &
  local p3="$!"
  bash "${SCRIPT_DIR}/run_data_traffic.sh" \
    --mapping "$SERVICE_MAPPING_PATH" \
    --output-dir "${phase_dir}/traffic/high_throughput_data_burst_extra" \
    --duration "$duration" \
    --skip-server-setup \
    >"${phase_dir}/data_traffic_burst_extra.log" 2>&1 &
  local p4="$!"
  wait "$p1" "$p2" "$p3" "$p4"
}

analyze_phase() {
  local phase_dir="$1"
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "$phase_dir" >"${phase_dir}/analyze_sla_violations.log" 2>&1 || warn "${phase_dir}: SLA analysis failed"
  python3 "${SCRIPT_DIR}/policy_decision_logger.py" "$phase_dir" >"${phase_dir}/policy_decision_logger.log" 2>&1 || warn "${phase_dir}: policy decision logging failed"
  python3 "${SCRIPT_DIR}/compute_manufacturing_utility.py" "$phase_dir" >"${phase_dir}/compute_manufacturing_utility.log" 2>&1 || warn "${phase_dir}: utility computation failed"
}

run_phase() {
  local mode="$1"
  local phase="$2"
  local duration="$3"
  local phase_dir="${MODE_DIR}/${phase}"
  mkdir -p "$phase_dir"
  echo "[burst-cross-mode] mode=${mode} phase=${phase} duration=${duration}s"
  if [[ "$phase" == "burst" ]]; then
    run_burst_phase_traffic "$phase_dir" "$duration" || warn "${mode}/${phase}: burst traffic exited nonzero"
  else
    run_normal_phase_traffic "$phase_dir" "$duration" || warn "${mode}/${phase}: mixed traffic exited nonzero"
  fi
  analyze_phase "$phase_dir"
}

write_mode_status() {
  local mode="$1"
  python3 - "$MODE_DIR" "$mode" "$ENFORCEMENT_STATUS" "$STATIC_QOS_INSTALL_STATUS" "$POLICY_MANAGER_STATUS" <<'PY'
import json
import sys
from pathlib import Path

mode_dir = Path(sys.argv[1])
mode = sys.argv[2]
enforcement = sys.argv[3]
static_install = sys.argv[4]
policy = sys.argv[5]
status = "ok"
if enforcement == "failed" or static_install == "failed" or policy == "failed":
    status = "failed"
payload = {
    "mode": mode,
    "status": status,
    "enforcement_reset_status": enforcement,
}
if static_install:
    payload["static_qos_install_status"] = static_install
if policy:
    payload["policy_manager_status"] = policy
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_fifo() {
  local mode="fifo"
  MODE_DIR="${RESULTS_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_STATUS=""
  STATIC_QOS_INSTALL_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_enforcement "$mode"
  save_queue_evidence "onos_queue_counter_before.txt"
  if any_set_queue_flow_visible; then
    warn "fifo: set_queue flows present before warmup"
    ENFORCEMENT_STATUS="failed"
  fi
  run_phase "$mode" warmup "$WARMUP"
  save_queue_evidence "onos_queue_counter_after_warmup.txt"
  run_phase "$mode" burst "$BURST"
  save_queue_evidence "onos_queue_counter_after_burst.txt"
  run_phase "$mode" recovery "$RECOVERY"
  save_queue_evidence "onos_queue_counter_after_recovery.txt"
  if any_set_queue_flow_visible; then
    warn "fifo: set_queue flows present after run"
    ENFORCEMENT_STATUS="failed"
  fi
  write_mode_status "$mode"
}

run_static_qos() {
  local mode="static_qos"
  MODE_DIR="${RESULTS_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_enforcement "$mode"
  install_static_qos
  save_queue_evidence "onos_queue_counter_before.txt"
  if ! set_queue_flows_visible; then
    STATIC_QOS_INSTALL_STATUS="failed"
  fi
  run_phase "$mode" warmup "$WARMUP"
  save_queue_evidence "onos_queue_counter_after_warmup.txt"
  run_phase "$mode" burst "$BURST"
  save_queue_evidence "onos_queue_counter_after_burst.txt"
  run_phase "$mode" recovery "$RECOVERY"
  save_queue_evidence "onos_queue_counter_after_recovery.txt"
  write_mode_status "$mode"
}

run_n6_only() {
  local mode="n6_only"
  MODE_DIR="${RESULTS_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_STATUS="not_started"
  STATIC_QOS_INSTALL_STATUS=""
  mkdir -p "$MODE_DIR"
  clear_enforcement "$mode"
  save_queue_evidence "onos_queue_counter_before.txt"
  trap stop_background INT TERM EXIT
  start_background telemetry bash "${SCRIPT_DIR}/run_telemetry.sh"
  sleep 2
  start_background policy_manager bash "${SCRIPT_DIR}/run_policy_manager.sh" --live
  sleep 2
  run_phase "$mode" warmup "$WARMUP"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_warmup.txt"
  run_phase "$mode" burst "$BURST"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_burst.txt"
  run_phase "$mode" recovery "$RECOVERY"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_recovery.txt"
  stop_background
  trap - INT TERM EXIT
  write_mode_status "$mode"
}

run_dt_assisted() {
  local mode="dt_assisted"
  MODE_DIR="${RESULTS_DIR}/${mode}"
  MODE_PIDS=()
  POLICY_MANAGER_STATUS="not_started"
  STATIC_QOS_INSTALL_STATUS=""
  local total_duration=$((WARMUP + BURST + RECOVERY + 20))
  mkdir -p "${MODE_DIR}/digital_twin"
  clear_enforcement "$mode"
  save_queue_evidence "onos_queue_counter_before.txt"
  trap stop_background INT TERM EXIT
  start_background telemetry bash "${SCRIPT_DIR}/run_telemetry.sh"
  sleep 2
  start_background digital_twin bash "${SCRIPT_DIR}/run_digital_twin.sh"
  sleep 2
  start_background twin_snapshot_logger python3 "${TESTBED_DIR}/digital_twin/twin_snapshot_logger.py" --output-dir "${MODE_DIR}/digital_twin" --duration "$total_duration" --interval 1
  sleep 1
  start_background policy_manager bash "${SCRIPT_DIR}/run_policy_manager.sh" --live
  sleep 2
  run_phase "$mode" warmup "$WARMUP"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_warmup.txt"
  run_phase "$mode" burst "$BURST"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_burst.txt"
  run_phase "$mode" recovery "$RECOVERY"
  check_policy_manager_status
  save_queue_evidence "onos_queue_counter_after_recovery.txt"
  stop_background
  trap - INT TERM EXIT
  python3 "${TESTBED_DIR}/digital_twin/twin_accuracy_evaluator.py" "$MODE_DIR" --telemetry-dir "${TESTBED_DIR}/logs/telemetry" --snapshot-dir "${MODE_DIR}/digital_twin" >"${MODE_DIR}/twin_accuracy_evaluator.log" 2>&1 || warn "dt_assisted: twin accuracy evaluation failed"
  write_mode_status "$mode"
}

write_cross_mode_summaries() {
  python3 - "$RESULTS_DIR" <<'PY'
import csv
import json
import sys
from pathlib import Path

base = Path(sys.argv[1])
modes = ["fifo", "static_qos", "n6_only", "dt_assisted"]
phases = ["warmup", "burst", "recovery"]

def load_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}

def metric_value(phase_dir, service, metric, column="mean"):
    path = phase_dir / "service_metrics.csv"
    if not path.exists():
        return ""
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("service_class") == service and row.get("metric") == metric:
                return row.get(column) or row.get("latest") or ""
    return ""

rows = []
for mode in modes:
    mode_dir = base / mode
    mode_status = load_json(mode_dir / "mode_status.json")
    twin = load_json(mode_dir / "twin_accuracy_summary.json")
    for phase in phases:
        phase_dir = mode_dir / phase
        summary = load_json(phase_dir / "summary_metrics.json")
        utility = load_json(phase_dir / "manufacturing_utility_summary.json")
        policy = load_json(phase_dir / "policy_decision_summary.json")
        rows.append({
            "mode": mode,
            "phase": phase,
            "status": mode_status.get("status", "failed"),
            "control_latency_avg_ms": metric_value(phase_dir, "real_time_control", "latency_avg_ms"),
            "control_latency_max_ms": metric_value(phase_dir, "real_time_control", "latency_max_ms", "max"),
            "control_loss_percent": metric_value(phase_dir, "real_time_control", "loss_percent"),
            "data_throughput_bps": metric_value(phase_dir, "high_throughput_data", "throughput_bps"),
            "data_loss_percent": metric_value(phase_dir, "high_throughput_data", "loss_percent"),
            "sensor_delivery_ratio_percent": metric_value(phase_dir, "sensor_telemetry", "delivery_ratio_percent"),
            "sensor_loss_percent": metric_value(phase_dir, "sensor_telemetry", "loss_percent"),
            "sla_checks": summary.get("sla_checks", 0),
            "sla_violations": summary.get("sla_violations", 0),
            "sla_violation_rate": summary.get("sla_violation_rate", 0.0),
            "manufacturing_utility": utility.get("phase_utility", {}).get("all", 0.0),
            "policy_decisions": policy.get("decision_count", 0),
            "policy_changes": policy.get("policy_change_count", 0),
            "twin_matched_samples": twin.get("matched_samples", "") if mode == "dt_assisted" else "",
            "twin_mean_relative_error": twin.get("mean_relative_error", "") if mode == "dt_assisted" else "",
            "twin_mean_staleness_seconds": twin.get("mean_staleness_seconds", "") if mode == "dt_assisted" else "",
        })

summary_fields = [
    "mode", "phase", "status", "control_latency_avg_ms", "control_latency_max_ms",
    "control_loss_percent", "data_throughput_bps", "data_loss_percent",
    "sensor_delivery_ratio_percent", "sensor_loss_percent", "sla_checks",
    "sla_violations", "sla_violation_rate", "manufacturing_utility",
    "policy_decisions", "policy_changes", "twin_matched_samples",
    "twin_mean_relative_error", "twin_mean_staleness_seconds",
]
with (base / "burst_cross_mode_summary.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=summary_fields)
    writer.writeheader()
    writer.writerows(rows)

by_mode_phase = {(row["mode"], row["phase"]): row for row in rows}
fifo_burst = by_mode_phase.get(("fifo", "burst"), {})
fifo_recovery = by_mode_phase.get(("fifo", "recovery"), {})
fifo_burst_latency = float(fifo_burst.get("control_latency_avg_ms") or 0.0)
fifo_recovery_latency = float(fifo_recovery.get("control_latency_avg_ms") or 0.0)
fifo_sla_rate = float(fifo_burst.get("sla_violation_rate") or 0.0)
fifo_utility = float(fifo_recovery.get("manufacturing_utility") or 0.0)

recovery_rows = []
for mode in modes:
    burst = by_mode_phase.get((mode, "burst"), {})
    recovery = by_mode_phase.get((mode, "recovery"), {})
    burst_latency = float(burst.get("control_latency_avg_ms") or 0.0)
    recovery_latency = float(recovery.get("control_latency_avg_ms") or 0.0)
    sla_rate = float(burst.get("sla_violation_rate") or 0.0)
    utility = float(recovery.get("manufacturing_utility") or 0.0)
    recovery_rows.append({
        "mode": mode,
        "burst_control_latency_avg_ms": burst_latency,
        "recovery_control_latency_avg_ms": recovery_latency,
        "latency_recovery_improvement_vs_fifo": (fifo_recovery_latency - recovery_latency) / fifo_recovery_latency if fifo_recovery_latency else 0.0,
        "sla_violation_reduction_vs_fifo": (fifo_sla_rate - sla_rate) / fifo_sla_rate if fifo_sla_rate else 0.0,
        "utility_improvement_vs_fifo": (utility - fifo_utility) / fifo_utility if fifo_utility else 0.0,
    })

recovery_fields = [
    "mode", "burst_control_latency_avg_ms", "recovery_control_latency_avg_ms",
    "latency_recovery_improvement_vs_fifo", "sla_violation_reduction_vs_fifo",
    "utility_improvement_vs_fifo",
]
with (base / "burst_recovery_summary.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=recovery_fields)
    writer.writeheader()
    writer.writerows(recovery_rows)
print(f"[burst-cross-mode] wrote summaries under {base}")
PY
}

echo "[burst-cross-mode] results directory: ${RESULTS_DIR}"
echo "[burst-cross-mode] durations warmup=${WARMUP}s burst=${BURST}s recovery=${RECOVERY}s"
echo "[burst-cross-mode] policy mode for adaptive modes: live"

run_fifo
run_static_qos
run_n6_only
run_dt_assisted
write_cross_mode_summaries

echo "[burst-cross-mode] completed. Results stored under: ${RESULTS_DIR}"
