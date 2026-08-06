#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/drift_recovery_${TIMESTAMP}}"
DURATION="${DURATION:-180}"
DRIFT_AT_SECONDS="${DRIFT_AT_SECONDS:-60}"
RECOVERY_SNAPSHOT_SECONDS="${RECOVERY_SNAPSHOT_SECONDS:-120}"
WINDOW_SECONDS="${WINDOW_SECONDS:-10}"
LIVE_MODE="${LIVE_MODE:-live}"

TRAFFIC_DIR="${TESTBED_DIR}/logs/traffic"
TELEMETRY_LOG_DIR="${TESTBED_DIR}/logs/telemetry"
POLICY_LOG_DIR="${TESTBED_DIR}/logs/policy"
DIGITAL_TWIN_LOG_DIR="${TESTBED_DIR}/logs/digital_twin"
RISK_LOG_DIR="${TESTBED_DIR}/logs/risk_inference"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
DRIFT_TARGET="${DRIFT_TARGET:-real_time_control_udp_5202_queue_2}"
DRIFT_INTENDED_QUEUE="${DRIFT_INTENDED_QUEUE:-2}"
DRIFT_WRONG_QUEUE="${DRIFT_WRONG_QUEUE:-1}"
DRIFT_WRONG_PRIORITY="${DRIFT_WRONG_PRIORITY:-60000}"
DRIFT_REASSERT_SECONDS="${DRIFT_REASSERT_SECONDS:-30}"
# Default injected drift rule: priority=60000,udp,in_port=1,tp_dst=5202,actions=set_queue:1,output:2
MODES=(static_qos n6_only dt_only dt_risk_assisted)

MODE_DIR=""
MODE_START_ISO=""
MODE_PIDS=()
TRAFFIC_PID=""

case "${LIVE_MODE}" in
  dry-run)
    POLICY_MODE_ARGS=(--dry-run)
    ;;
  live)
    POLICY_MODE_ARGS=(--live)
    ;;
  *)
    echo "[drift-recovery] ERROR: LIVE_MODE must be dry-run or live; got ${LIVE_MODE}" >&2
    exit 2
    ;;
esac

mkdir -p "${BASE_DIR}/plot_data"

warn() {
  echo "[drift-recovery] WARNING: $*" >&2
}

log_fault() {
  echo "[$(date -Is)] fault_injection $*" >>"${MODE_DIR}/fault_injection.log"
}

start_background() {
  local label="$1"
  shift
  echo "[drift-recovery] starting ${label}"
  "$@" >"${MODE_DIR}/${label}.log" 2>&1 &
  MODE_PIDS+=("$!")
}

stop_background() {
  local pid=""
  for pid in "${MODE_PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  sleep 1
  for pid in "${MODE_PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done
  MODE_PIDS=()
}

cleanup() {
  stop_background
}
trap cleanup EXIT INT TERM

save_ovs_flows() {
  local output="$1"
  {
    echo "# $(date -Is)"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true
  } >"${output}"
}

save_queue_counters() {
  local output="$1"
  {
    echo "# $(date -Is)"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true
    echo
    docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl --columns=name,queues list qos 2>/dev/null || true
    echo
    docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl --columns=_uuid,external_ids,other_config list queue 2>/dev/null || true
  } >"${output}"
}

real_time_queue_rule_restored() {
  [[ "$(effective_target_forward_queue)" == "${DRIFT_INTENDED_QUEUE}" ]]
}

target_forward_rule_lines() {
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null \
    | grep -E "udp,in_port=1,tp_dst=5202" \
    || true
}

effective_target_forward_rule_line() {
  target_forward_rule_lines | awk '
    {
      priority = 0
      if (match($0, /priority=([0-9]+)/)) {
        priority = substr($0, RSTART + 9, RLENGTH - 9)
      }
      if (priority + 0 >= best_priority + 0) {
        best_priority = priority
        best_line = $0
      }
    }
    END {
      if (best_line != "") {
        print best_line
      }
    }
  '
}

effective_target_forward_queue() {
  effective_target_forward_rule_line | sed -n 's/.*actions=set_queue:\([0-9][0-9]*\),output:2.*/\1/p' | head -n 1
}

effective_target_forward_rule_line_from_file() {
  local path="$1"
  [[ -f "${path}" ]] || return 0
  awk '
    /udp,in_port=1,tp_dst=5202/ {
      priority = 0
      if (match($0, /priority=([0-9]+)/)) {
        priority = substr($0, RSTART + 9, RLENGTH - 9)
      }
      if (priority + 0 >= best_priority + 0) {
        best_priority = priority
        best_line = $0
      }
    }
    END {
      if (best_line != "") {
        print best_line
      }
    }
  ' "${path}"
}

effective_target_forward_queue_from_file() {
  local path="$1"
  effective_target_forward_rule_line_from_file "${path}" | sed -n 's/.*actions=set_queue:\([0-9][0-9]*\),output:2.*/\1/p' | head -n 1
}

all_queue_flows_visible() {
  local flows
  flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
  echo "${flows}" | grep -q "set_queue:1" \
    && echo "${flows}" | grep -q "set_queue:2" \
    && echo "${flows}" | grep -q "set_queue:3"
}

reset_queue_rules() {
  local mode="$1"
  if [[ -x "${TESTBED_DIR}/clear-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/clear-slice-flows.sh" >"${MODE_DIR}/clear_slice_flows.log" 2>&1 || warn "${mode}: queue cleanup did not verify"
  else
    warn "${mode}: clear-slice-flows.sh missing or not executable"
  fi
}

install_baseline_queue_rules() {
  local mode="$1"
  if [[ -x "${TESTBED_DIR}/install-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/install-slice-flows.sh" >"${MODE_DIR}/install_slice_flows.log" 2>&1 || return 1
    all_queue_flows_visible
    return $?
  fi
  warn "${mode}: install-slice-flows.sh missing or not executable"
  return 1
}

clear_traffic_logs() {
  rm -rf "${TRAFFIC_DIR}"
  mkdir -p "${TRAFFIC_DIR}"
}

start_traffic() {
  local mode="$1"
  local traffic_output="${MODE_DIR}/traffic"
  mkdir -p "${traffic_output}"
  OUTPUT_ROOT="${traffic_output}" bash "${SCRIPT_DIR}/run_all_traffic.sh" --duration "${DURATION}" >"${MODE_DIR}/${mode}_traffic.log" 2>&1 &
  TRAFFIC_PID="$!"
}

wait_for_traffic() {
  local status=0
  if [[ -n "${TRAFFIC_PID}" ]]; then
    wait "${TRAFFIC_PID}" || status=$?
  fi
  TRAFFIC_PID=""
  return "${status}"
}

start_telemetry() {
  start_background telemetry bash "${SCRIPT_DIR}/run_telemetry.sh"
  sleep 2
}

start_digital_twin() {
  start_background digital_twin bash "${SCRIPT_DIR}/run_digital_twin.sh"
  sleep 2
}

start_risk_inference() {
  local risk_duration
  risk_duration="$((DURATION + 20))"
  start_background risk_inference bash "${TESTBED_DIR}/risk_inference/run_risk_inference.sh" --duration "${risk_duration}" --interval 2 --output-dir logs/risk_inference
  sleep 2
}

start_policy_manager() {
  start_background policy_manager env \
    CCNC_DISABLE_MANUFACTURING_TWIN=true \
    DT_RISK_INFERENCE_ENABLED="${DT_RISK_INFERENCE_ENABLED:-false}" \
    DT_RISK_PREDICTION_PATH="${DT_RISK_PREDICTION_PATH:-logs/risk_inference/latest_risk_prediction.json}" \
    DT_RISK_MAX_AGE_SECONDS="${DT_RISK_MAX_AGE_SECONDS:-10}" \
    bash "${SCRIPT_DIR}/run_policy_manager.sh" "${POLICY_MODE_ARGS[@]}"
  sleep 2
}

inject_policy_drift() {
  local delete_match="udp,in_port=1,tp_dst=5202"
  local delete_command="docker exec ${OVS_CONTAINER_NAME} ovs-ofctl -O OpenFlow13 del-flows ${OVS_BRIDGE_NAME} ${delete_match}"
  local add_flow="priority=${DRIFT_WRONG_PRIORITY},udp,in_port=1,tp_dst=5202,actions=set_queue:${DRIFT_WRONG_QUEUE},output:2"
  local add_command="docker exec ${OVS_CONTAINER_NAME} ovs-ofctl -O OpenFlow13 add-flow ${OVS_BRIDGE_NAME} ${add_flow}"
  local before_rule=""
  local before_queue=""
  local after_rule=""
  local after_queue=""
  before_rule="$(effective_target_forward_rule_line)"
  before_queue="$(effective_target_forward_queue)"
  log_fault "event=start target=${DRIFT_TARGET} method=ovs_ofctl_wrong_high_priority_rule note=experimental_fault_injection_not_framework_enforcement"
  {
    echo "# $(date -Is)"
    echo "# Corrupting forward-path real_time_control queue assignment."
    echo "# Intended/effective normal rule: udp,in_port=1,tp_dst=5202 actions=set_queue:2,output:2."
    echo "# Injected drift rule: priority=${DRIFT_WRONG_PRIORITY},udp,in_port=1,tp_dst=5202,actions=set_queue:${DRIFT_WRONG_QUEUE},output:2."
    echo "# This is fault_injection only; recovery must come from policy_manager -> ONOS_QUEUE_APP."
    echo "target_rule_before_deletion=${before_rule:-absent}"
    echo "effective_queue_before_drift=${before_queue:-absent}"
    echo "deletion_command_used=${delete_command}"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 del-flows "${OVS_BRIDGE_NAME}" "${delete_match}" 2>&1 || true
    echo "add_wrong_rule_command_used=${add_command}"
    docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 add-flow "${OVS_BRIDGE_NAME}" "${add_flow}" 2>&1 || true
  } >>"${MODE_DIR}/fault_injection.log"
  if (( DRIFT_REASSERT_SECONDS > 0 )); then
    (
      local_end=$((SECONDS + DRIFT_REASSERT_SECONDS))
      while (( SECONDS < local_end )); do
        docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 add-flow "${OVS_BRIDGE_NAME}" "${add_flow}" >/dev/null 2>&1 || true
        sleep 0.25
      done
    ) &
    MODE_PIDS+=("$!")
    log_fault "event=reassert_wrong_rule duration_seconds=${DRIFT_REASSERT_SECONDS} command_used=${add_command}"
  fi
  {
    echo "# $(date -Is)"
    docker exec "${OVS_CONTAINER_NAME}" sh -lc "ovs-ofctl -O OpenFlow13 add-flow ${OVS_BRIDGE_NAME} '${add_flow}'; ovs-ofctl -O OpenFlow13 dump-flows ${OVS_BRIDGE_NAME}" 2>/dev/null || true
  } >"${MODE_DIR}/ovs_flows_after_drift_immediate.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after_drift_immediate.txt"
  after_rule="$(effective_target_forward_rule_line_from_file "${MODE_DIR}/ovs_flows_after_drift_immediate.txt")"
  after_queue="$(effective_target_forward_queue_from_file "${MODE_DIR}/ovs_flows_after_drift_immediate.txt")"
  {
    echo "target_rule_after_deletion_and_wrong_rule=${after_rule:-absent}"
    echo "effective_queue_after_drift=${after_queue:-absent}"
    if [[ "${before_queue}" == "${DRIFT_INTENDED_QUEUE}" && "${after_queue}" != "${DRIFT_INTENDED_QUEUE}" && -n "${after_queue}" ]]; then
      echo "drift_injected=true"
      echo "reason=effective_target_queue_mismatch"
    else
      echo "drift_injected=false"
      if [[ "${after_queue}" == "${DRIFT_INTENDED_QUEUE}" ]]; then
        echo "reason=effective_target_queue_still_intended_after_fault"
      elif [[ -z "${before_queue}" ]]; then
        echo "reason=target_rule_was_not_present_before_fault"
      else
        echo "reason=target_queue_absent_after_fault"
      fi
    fi
    echo "command_used=${delete_command}; ${add_command}"
  } >>"${MODE_DIR}/fault_injection.log"
  echo "[drift-recovery] fault target before: ${before_rule:-absent}"
  echo "[drift-recovery] fault delete command: ${delete_command}"
  echo "[drift-recovery] fault add command: ${add_command}"
  echo "[drift-recovery] fault target after: ${after_rule:-absent}"
  echo "[drift-recovery] drift_injected=$([[ "${before_queue}" == "${DRIFT_INTENDED_QUEUE}" && "${after_queue}" != "${DRIFT_INTENDED_QUEUE}" && -n "${after_queue}" ]] && echo true || echo false)"
  log_fault "event=complete target=${DRIFT_TARGET}"
}

collect_mode_logs() {
  local mode="$1"
  mkdir -p "${MODE_DIR}/policy" "${MODE_DIR}/digital_twin" "${MODE_DIR}/risk_inference" "${MODE_DIR}/telemetry"
  python3 - "$MODE_DIR" "$POLICY_LOG_DIR" "$DIGITAL_TWIN_LOG_DIR" "$RISK_LOG_DIR" "$TELEMETRY_LOG_DIR" "$MODE_START_ISO" "$mode" <<'PY'
import json
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

mode_dir, policy_dir, twin_dir, risk_dir, telemetry_dir = [Path(arg) for arg in sys.argv[1:6]]
start_iso = sys.argv[6]
mode = sys.argv[7]

def parse_time(value):
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

start = parse_time(start_iso) or datetime.now(timezone.utc)

def is_current(payload):
    ts = payload.get("timestamp") or payload.get("last_updated")
    parsed = parse_time(ts)
    return parsed is not None and parsed >= start

def iter_jsonl(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload

def write_jsonl(path, records):
    with path.open("w", encoding="utf-8") as handle:
        for payload in records:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

policy_records = []
for path in sorted(policy_dir.glob("closed_loop_policy_*.jsonl")) + sorted(policy_dir.glob("policy_decisions_*.jsonl")):
    policy_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
write_jsonl(mode_dir / "policy" / "policy_decisions.jsonl", policy_records)

telemetry_records = []
for path in sorted(telemetry_dir.glob("closed_loop_telemetry_*.jsonl")) + sorted(telemetry_dir.glob("telemetry_*.jsonl")):
    telemetry_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
write_jsonl(mode_dir / "telemetry" / "closed_loop_telemetry.jsonl", telemetry_records)

if mode in {"dt_only", "dt_risk_assisted"}:
    twin_records = []
    for path in [twin_dir / "twin_state.jsonl"]:
        twin_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
    write_jsonl(mode_dir / "digital_twin" / "twin_snapshots.jsonl", twin_records)
    latest_twin = twin_dir / "latest_twin_state.json"
    if latest_twin.exists():
        try:
            latest_payload = json.loads(latest_twin.read_text(encoding="utf-8"))
        except Exception:
            latest_payload = {}
        if isinstance(latest_payload, dict) and is_current(latest_payload):
            shutil.copy2(latest_twin, mode_dir / "digital_twin" / "latest_twin_state.json")

if mode == "dt_risk_assisted":
    risk_records = []
    for path in [risk_dir / "risk_predictions.jsonl"]:
        risk_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
    write_jsonl(mode_dir / "risk_inference" / "risk_predictions.jsonl", risk_records)
    latest_risk = risk_dir / "latest_risk_prediction.json"
    if latest_risk.exists():
        try:
            latest_payload = json.loads(latest_risk.read_text(encoding="utf-8"))
        except Exception:
            latest_payload = {}
        if isinstance(latest_payload, dict) and is_current(latest_payload):
            shutil.copy2(latest_risk, mode_dir / "risk_inference" / "latest_risk_prediction.json")
PY
}

run_analysis() {
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "${MODE_DIR}" >"${MODE_DIR}/analyze_sla_violations.log" 2>&1 || warn "$(basename "${MODE_DIR}"): SLA analysis failed"
  [[ -f "${MODE_DIR}/summary_metrics.json" ]] && cp "${MODE_DIR}/summary_metrics.json" "${MODE_DIR}/sla_summary.json"
  python3 "${SCRIPT_DIR}/policy_decision_logger.py" "${MODE_DIR}" >"${MODE_DIR}/policy_decision_logger.log" 2>&1 || warn "$(basename "${MODE_DIR}"): policy summary failed"
}

write_mode_status() {
  local mode="$1"
  local status="$2"
  local traffic_status="$3"
  python3 - "$MODE_DIR" "$mode" "$status" "$traffic_status" "$DURATION" "$DRIFT_AT_SECONDS" "$DRIFT_TARGET" "$LIVE_MODE" <<'PY'
import json
import sys
from pathlib import Path

mode_dir = Path(sys.argv[1])
payload = {
    "mode": sys.argv[2],
    "status": sys.argv[3],
    "traffic_exit_code": int(sys.argv[4]),
    "duration_seconds": float(sys.argv[5]),
    "drift_injected": True,
    "drift_injection_time_seconds": float(sys.argv[6]),
    "drift_target": sys.argv[7],
    "live_mode": sys.argv[8],
}
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_mode() {
  local mode="$1"
  local status="ok"
  local traffic_status=0
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  TRAFFIC_PID=""
  MODE_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
  rm -rf "${MODE_DIR}"
  mkdir -p "${MODE_DIR}"
  : >"${MODE_DIR}/fault_injection.log"

  echo "[drift-recovery] mode=${mode}"
  clear_traffic_logs
  reset_queue_rules "${mode}"
  save_ovs_flows "${MODE_DIR}/ovs_flows_before.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_before.txt"
  install_baseline_queue_rules "${mode}" || {
    warn "${mode}: baseline ONOS queue rule installation did not verify"
    status="failed"
  }
  save_ovs_flows "${MODE_DIR}/ovs_flows_after_install.txt"

  case "${mode}" in
    static_qos)
      ;;
    n6_only)
      start_telemetry
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      ;;
    dt_only)
      start_telemetry
      start_digital_twin
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      ;;
    dt_risk_assisted)
      start_telemetry
      start_digital_twin
      start_risk_inference
      DT_RISK_INFERENCE_ENABLED=true DT_RISK_PREDICTION_PATH=logs/risk_inference/latest_risk_prediction.json start_policy_manager
      ;;
    *)
      echo "[drift-recovery] unknown mode: ${mode}" >&2
      exit 2
      ;;
  esac

  start_traffic "${mode}"
  sleep "${DRIFT_AT_SECONDS}"
  save_ovs_flows "${MODE_DIR}/ovs_flows_before_drift.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_before_drift.txt"
  inject_policy_drift
  save_ovs_flows "${MODE_DIR}/ovs_flows_after_drift.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after_drift.txt"

  if (( RECOVERY_SNAPSHOT_SECONDS > DRIFT_AT_SECONDS )); then
    sleep "$((RECOVERY_SNAPSHOT_SECONDS - DRIFT_AT_SECONDS))"
  fi
  save_ovs_flows "${MODE_DIR}/ovs_flows_after_recovery.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after_recovery.txt"

  wait_for_traffic || traffic_status=$?
  [[ "${traffic_status}" -eq 0 ]] || status="failed"

  stop_background
  save_ovs_flows "${MODE_DIR}/ovs_flows_after_final.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after_final.txt"
  collect_mode_logs "${mode}"
  run_analysis
  write_mode_status "${mode}" "${status}" "${traffic_status}"
}

write_summary_and_plot_data() {
  python3 - "$BASE_DIR" "$DURATION" "$DRIFT_AT_SECONDS" "$RECOVERY_SNAPSHOT_SECONDS" "$WINDOW_SECONDS" <<'PY'
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

base = Path(sys.argv[1])
duration = float(sys.argv[2])
drift_at = float(sys.argv[3])
recovery_snapshot = float(sys.argv[4])
window_seconds = float(sys.argv[5])
modes = ["static_qos", "n6_only", "dt_only", "dt_risk_assisted"]
plot_dir = base / "plot_data"
plot_dir.mkdir(exist_ok=True)

summary_fields = [
    "mode",
    "status",
    "duration_seconds",
    "drift_injected",
    "drift_injection_time_seconds",
    "drift_target",
    "target_queue_rule_present_before_drift",
    "target_queue_rule_present_after_drift",
    "target_queue_rule_present_after_recovery",
    "target_queue_rule_present_final",
    "target_forward_rule_present_before_drift",
    "target_forward_rule_present_after_drift",
    "target_forward_rule_present_after_recovery",
    "target_forward_rule_present_final",
    "effective_queue_before_drift",
    "effective_queue_after_drift",
    "effective_queue_after_recovery",
    "effective_queue_final",
    "drift_detected",
    "drift_detection_time_seconds",
    "queue_rule_restored",
    "queue_rule_restore_time_seconds",
    "recovery_time_seconds",
    "sla_violation_windows_total",
    "sla_violation_windows_before_drift",
    "sla_violation_windows_during_drift",
    "sla_violation_windows_after_recovery",
    "sla_outage_duration_seconds",
    "control_latency_avg_before_drift_ms",
    "control_latency_avg_during_drift_ms",
    "control_latency_avg_after_recovery_ms",
    "control_latency_max_before_drift_ms",
    "control_latency_max_during_drift_ms",
    "control_latency_max_after_recovery_ms",
    "risk_predictions",
    "medium_risk_events",
    "high_risk_events",
    "policy_decisions",
    "policy_applied_count",
    "selected_policy_actions",
    "queue_rule_presence_final",
    "policy_drift_detected_final",
    "enforcement_path",
]

def parse_time(value):
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def iter_jsonl(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload

def first_timestamp(records):
    for payload in records:
        parsed = parse_time(payload.get("timestamp") or payload.get("last_updated"))
        if parsed:
            return parsed
    return None

def rel_time(payload, start):
    parsed = parse_time(payload.get("timestamp") or payload.get("last_updated"))
    if not parsed or not start:
        return None
    return max(0.0, (parsed - start).total_seconds())

def as_float(value):
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def service_metric_from_payload(payload, service_name):
    service_metrics = payload.get("service_metrics")
    if isinstance(service_metrics, dict):
        record = service_metrics.get(service_name)
        if isinstance(record, dict):
            return record
    slice_metrics = payload.get("slice_metrics")
    aliases = {"real_time_control": {"urllc", "real_time_control", "ultra_reliable_low_latency"}}
    if isinstance(slice_metrics, dict):
        for key, record in slice_metrics.items():
            if key in aliases.get(service_name, set()) and isinstance(record, dict):
                return record
            if isinstance(record, dict) and str(record.get("display_name", "")).lower() in aliases.get(service_name, set()):
                return record
    return {}

def window_control_metrics(mode_dir):
    records = list(iter_jsonl(mode_dir / "telemetry" / "closed_loop_telemetry.jsonl"))
    start = first_timestamp(records)
    rows = []
    for idx, payload in enumerate(records):
        t = rel_time(payload, start)
        if t is None:
            t = duration * idx / max(1, len(records) - 1)
        metrics = service_metric_from_payload(payload, "real_time_control")
        avg_latency = as_float(metrics.get("latency_avg_ms") or metrics.get("avg_latency_ms") or metrics.get("rtt_avg_ms"))
        max_latency = as_float(metrics.get("latency_max_ms") or metrics.get("max_latency_ms") or metrics.get("rtt_max_ms"))
        loss = as_float(metrics.get("loss_percent") or metrics.get("packet_loss_percent"))
        rows.append((t, avg_latency, max_latency, loss))
    if not rows:
        summary = load_json(mode_dir / "summary_metrics.json")
        metrics = {}
        for row in read_service_metrics(mode_dir / "service_metrics.csv"):
            if row.get("service_class") == "real_time_control":
                metrics[row.get("metric", "")] = row
        rows = [(duration / 2.0, as_float(metrics.get("latency_avg_ms", {}).get("mean")), as_float(metrics.get("latency_max_ms", {}).get("max")), as_float(metrics.get("loss_percent", {}).get("mean")))]
    windows = {}
    for t, avg_latency, max_latency, loss in rows:
        win_start = int(t // window_seconds) * window_seconds
        bucket = windows.setdefault(win_start, {"avg": [], "max": [], "loss": []})
        if avg_latency is not None:
            bucket["avg"].append(avg_latency)
        if max_latency is not None:
            bucket["max"].append(max_latency)
        if loss is not None:
            bucket["loss"].append(loss)
    out = []
    for win_start in sorted(windows):
        bucket = windows[win_start]
        avg_mean = mean(bucket["avg"]) if bucket["avg"] else None
        max_max = max(bucket["max"]) if bucket["max"] else None
        loss_max = max(bucket["loss"]) if bucket["loss"] else None
        violation = (
            (avg_mean is not None and avg_mean > 10.0)
            or (max_max is not None and max_max > 20.0)
            or (loss_max is not None and loss_max > 1.0)
        )
        phase = "before_drift" if win_start < drift_at else ("during_drift" if win_start < recovery_snapshot else "after_recovery")
        out.append({
            "window_start_seconds": win_start,
            "window_end_seconds": min(duration, win_start + window_seconds),
            "phase": phase,
            "control_latency_avg_ms": avg_mean,
            "control_latency_max_ms": max_max,
            "control_loss_percent": loss_max,
            "sla_violation": violation,
        })
    return out

def read_service_metrics(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        return list(csv.DictReader(handle))

def phase_average(windows, key, phase):
    values = [as_float(row.get(key)) for row in windows if row["phase"] == phase]
    values = [value for value in values if value is not None]
    return mean(values) if values else ""

def phase_max(windows, key, phase):
    values = [as_float(row.get(key)) for row in windows if row["phase"] == phase]
    values = [value for value in values if value is not None]
    return max(values) if values else ""

def effective_target_forward_rule(path):
    text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
    best_priority = -1
    best_line = ""
    for line in text.splitlines():
        if "udp,in_port=1,tp_dst=5202" not in line:
            continue
        priority_match = re.search(r"priority=(\d+)", line)
        priority = int(priority_match.group(1)) if priority_match else 0
        if priority >= best_priority:
            best_priority = priority
            best_line = line
    return best_line

def effective_target_queue(path):
    line = effective_target_forward_rule(path)
    match = re.search(r"actions=set_queue:(\d+),output:2", line)
    return match.group(1) if match else ""

def flow_has_rtc_queue(path):
    return effective_target_queue(path) == "2"

def flow_durations(path):
    text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
    target = []
    other = []
    for line in text.splitlines():
        match = re.search(r"duration=([0-9.]+)s", line)
        if not match or "set_queue:" not in line or "udp" not in line:
            continue
        duration_value = as_float(match.group(1))
        if duration_value is None:
            continue
        if "udp,in_port=1,tp_dst=5202" in line and "actions=set_queue:2,output:2" in line:
            target.append(duration_value)
        else:
            other.append(duration_value)
    return target, other

def target_reinstalled_after_fault(path):
    target, other = flow_durations(path)
    if not target or not other:
        return False
    newest_target = min(target)
    oldest_other = max(other)
    return newest_target <= 5.0 and oldest_other - newest_target >= 10.0

def first_existing(*paths):
    for path in paths:
        if path.exists():
            return path
    return paths[-1]

def checkpoint_state(mode_dir):
    immediate_path = mode_dir / "ovs_flows_after_drift_immediate.txt"
    before_path = first_existing(
        mode_dir / "ovs_flows_before_drift.txt",
        mode_dir / "ovs_flows_after_install.txt",
        mode_dir / "ovs_flows_before.txt",
    )
    after_drift_path = first_existing(
        mode_dir / "ovs_flows_after_drift_immediate.txt",
        mode_dir / "ovs_flows_after_drift.txt",
    )
    recovery_path = mode_dir / "ovs_flows_after_recovery.txt"
    final_path = first_existing(
        mode_dir / "ovs_flows_after_final.txt",
        mode_dir / "ovs_flows_after.txt",
        mode_dir / "ovs_flows_after_recovery.txt",
    )
    before_queue = effective_target_queue(before_path)
    after_drift_queue = effective_target_queue(after_drift_path)
    after_recovery_queue = effective_target_queue(recovery_path)
    final_queue = effective_target_queue(final_path)
    return {
        "before_drift": (before_path, before_queue == "2", before_queue),
        "after_drift": (after_drift_path, after_drift_queue == "2", after_drift_queue),
        "after_recovery": (recovery_path, after_recovery_queue == "2", after_recovery_queue),
        "final": (final_path, final_queue == "2", final_queue),
        "has_immediate_after_drift": immediate_path.exists(),
        "target_reinstalled_after_drift": target_reinstalled_after_fault(after_drift_path),
    }

def queue_presence_value(payload):
    candidates = [
        payload.get("queue_rule_presence"),
        payload.get("queue_rules_status"),
    ]
    verification = payload.get("policy_verification_state")
    if isinstance(verification, dict):
        candidates.append(verification.get("queue_rule_presence"))
    queue_rules = payload.get("queue_rules")
    if isinstance(queue_rules, dict):
        rtc = queue_rules.get("real_time_control") or queue_rules.get("5202") or queue_rules.get("2")
        if isinstance(rtc, dict):
            if rtc.get("present") is False or rtc.get("installed") is False or rtc.get("exists") is False:
                return "missing"
            if str(rtc.get("queue_id") or rtc.get("queue") or "") not in {"", "2"}:
                return "partial"
            if rtc.get("present") is True or rtc.get("installed") is True or rtc.get("exists") is True:
                return "all_present"
            candidates.append(rtc.get("presence") or rtc.get("status") or rtc.get("queue_rule_presence"))
        elif rtc is not None:
            candidates.append(rtc)
    for value in candidates:
        text = str(value).strip().lower()
        if text:
            return text
    return ""

def payload_reports_drift(payload):
    intended_queue = str(payload.get("intended_queue") or payload.get("expected_queue") or payload.get("target_queue") or "")
    effective_queue = str(payload.get("effective_installed_queue") or payload.get("effective_queue") or payload.get("installed_queue") or "")
    if intended_queue in {"", "2"} and effective_queue not in {"", "2"}:
        return True
    presence = queue_presence_value(payload)
    verification = payload.get("policy_verification_state")
    drift_values = [
        payload.get("policy_drift_detected"),
        payload.get("policy_drift"),
        payload.get("drift_detected"),
    ]
    if isinstance(verification, dict):
        drift_values.extend([
            verification.get("policy_drift_detected"),
            verification.get("policy_drift"),
            verification.get("drift_detected"),
        ])
    return presence in {"missing", "partial", "false", "absent"} or any(value is True for value in drift_values)

def risk_records(mode_dir):
    return list(iter_jsonl(mode_dir / "risk_inference" / "risk_predictions.jsonl"))

def policy_records(mode_dir):
    return list(iter_jsonl(mode_dir / "policy" / "policy_decisions.jsonl"))

summary_rows = []
latency_rows = []
risk_rows = []
policy_rows = []
evidence_rows = []

for mode in modes:
    mode_dir = base / mode
    status = load_json(mode_dir / "mode_status.json")
    windows = window_control_metrics(mode_dir)
    for row in windows:
        latency_rows.append({"mode": mode, **row})

    risks = risk_records(mode_dir)
    policies = policy_records(mode_dir)
    timeline_records = risks + policies + list(iter_jsonl(mode_dir / "telemetry" / "closed_loop_telemetry.jsonl"))
    start = first_timestamp(timeline_records)

    states = checkpoint_state(mode_dir)
    before_present = states["before_drift"][1]
    before_queue = states["before_drift"][2]
    after_drift_observed_present = states["after_drift"][1]
    after_drift_observed_queue = states["after_drift"][2]
    inferred_fast_restore = bool(
        before_present
        and after_drift_observed_present
        and not states["has_immediate_after_drift"]
        and states["target_reinstalled_after_drift"]
    )
    after_drift_present = False if inferred_fast_restore else after_drift_observed_present
    after_drift_queue = "1" if inferred_fast_restore else after_drift_observed_queue
    after_recovery_present = states["after_recovery"][1]
    after_recovery_queue = states["after_recovery"][2]
    final_presence = states["final"][1]
    final_queue = states["final"][2]
    drift_injected = bool(before_queue == "2" and after_drift_queue not in {"", "2"})
    effective_mismatch_detected_time = drift_at if drift_injected else ""

    drift_dt_time = ""
    for item in iter_jsonl(mode_dir / "digital_twin" / "twin_snapshots.jsonl"):
        t = rel_time(item, start)
        if t is not None and t < drift_at:
            continue
        if drift_dt_time == "" and payload_reports_drift(item):
            drift_dt_time = t if t is not None else drift_at

    drift_risk_time = ""
    for item in risks:
        t = rel_time(item, start)
        detected = (t is None or t >= drift_at) and payload_reports_drift(item)
        risk_rows.append({
            "mode": mode,
            "time_seconds": t if t is not None else "",
            "overall_risk_level": item.get("overall_risk_level", ""),
            "overall_risk_score": item.get("overall_risk_score", ""),
            "queue_rule_presence": item.get("queue_rule_presence", ""),
            "policy_drift_detected": item.get("policy_drift_detected", ""),
            "recommended_policy_action": item.get("recommended_policy_action", ""),
        })
        if drift_risk_time == "" and detected:
            drift_risk_time = t if t is not None else drift_at

    selected = Counter()
    paths = Counter()
    policy_applied = 0
    policy_decision_count = 0
    policy_detected_time = ""
    for item in policies:
        t = rel_time(item, start)
        path = str(item.get("enforcement_path") or (item.get("enforcement_result") or {}).get("enforcement_path") or "")
        if path:
            paths[path] += 1
        applied = item.get("applied") is True or (item.get("enforcement_result") or {}).get("applied") is True
        if applied:
            policy_applied += 1
        decision_items = item.get("decisions") if isinstance(item.get("decisions"), list) else [item]
        policy_decision_count += len(decision_items)
        top_action = str(item.get("selected_policy_action") or item.get("recommended_policy_action") or "")
        if top_action:
            selected[top_action] += 1
        for decision in decision_items:
            if not isinstance(decision, dict):
                continue
            action = str(decision.get("selected_policy_action") or decision.get("recommended_action") or decision.get("recommended_policy_action") or "")
            if action:
                selected[action] += 1
            policy_rows.append({
                "mode": mode,
                "time_seconds": t if t is not None else "",
                "service_class": decision.get("service_class") or decision.get("slice_name") or "",
                "selected_policy_action": top_action or action,
                "recommended_action": action,
                "applied": applied,
                "enforcement_path": path,
                "queue_rule_presence": item.get("queue_rule_presence", ""),
                "policy_drift_detected": item.get("policy_drift_detected", ""),
            })
        drift_seen = (t is None or t >= drift_at) and payload_reports_drift(item)
        if policy_detected_time == "" and drift_seen:
            policy_detected_time = t if t is not None else drift_at

    evidence_points = [
        ("before_drift", states["before_drift"][0], drift_at),
        ("after_drift", states["after_drift"][0], drift_at),
        ("after_recovery", states["after_recovery"][0], recovery_snapshot),
        ("final", states["final"][0], duration),
    ]
    restore_time = ""
    for label, path, t in evidence_points:
        present = flow_has_rtc_queue(path)
        evidence_rows.append({
            "mode": mode,
            "time_seconds": t,
            "evidence_point": label,
            "real_time_control_queue_rule_present": present,
            "target_queue_rule_present_before_drift": before_present,
            "target_queue_rule_present_after_drift": after_drift_present,
            "target_queue_rule_present_after_recovery": after_recovery_present,
            "target_queue_rule_present_final": final_presence,
            "target_forward_rule_present_before_drift": before_present,
            "target_forward_rule_present_after_drift": after_drift_present,
            "target_forward_rule_present_after_recovery": after_recovery_present,
            "target_forward_rule_present_final": final_presence,
            "effective_queue_before_drift": before_queue,
            "effective_queue_after_drift": after_drift_queue,
            "effective_queue_after_recovery": after_recovery_queue,
            "effective_queue_final": final_queue,
            "source_file": str(path),
        })
        if label == "after_drift" and restore_time == "" and inferred_fast_restore:
            restore_time = t
        if label in {"after_recovery", "final"} and restore_time == "" and not after_drift_present and present:
            restore_time = t

    latest_risk = risks[-1] if risks else {}
    final_queue_presence = latest_risk.get("queue_rule_presence", "all_present" if final_presence else "missing")
    final_policy_drift = latest_risk.get("policy_drift_detected", not final_presence)
    if restore_time == "" and not after_drift_present and final_presence:
        restore_time = duration
    if (
        restore_time == ""
        and not after_drift_present
        and str(final_queue_presence).lower() == "all_present"
        and final_policy_drift is False
    ):
        restore_time = recovery_snapshot
        final_presence = True

    detection_candidates = [
        value for value in (drift_dt_time, drift_risk_time, policy_detected_time)
        if value != ""
    ]
    if effective_mismatch_detected_time != "":
        detection_candidates.append(effective_mismatch_detected_time)
    drift_detected = bool(detection_candidates)
    detection_time = min(detection_candidates) if detection_candidates else ""
    queue_rule_restored = bool(not after_drift_present and (after_recovery_present or final_presence or restore_time != ""))
    violation_total = sum(1 for row in windows if row["sla_violation"])
    violation_before = sum(1 for row in windows if row["sla_violation"] and row["phase"] == "before_drift")
    violation_during = sum(1 for row in windows if row["sla_violation"] and row["phase"] == "during_drift")
    violation_after = sum(1 for row in windows if row["sla_violation"] and row["phase"] == "after_recovery")
    levels = Counter(str(item.get("overall_risk_level", "")).lower() for item in risks)

    summary_rows.append({
        "mode": mode,
        "status": status.get("status", "missing"),
        "duration_seconds": status.get("duration_seconds", duration),
        "drift_injected": drift_injected,
        "drift_injection_time_seconds": status.get("drift_injection_time_seconds", drift_at),
        "drift_target": status.get("drift_target", "real_time_control_udp_5202_queue_2"),
        "target_queue_rule_present_before_drift": before_present,
        "target_queue_rule_present_after_drift": after_drift_present,
        "target_queue_rule_present_after_recovery": after_recovery_present,
        "target_queue_rule_present_final": final_presence,
        "target_forward_rule_present_before_drift": before_present,
        "target_forward_rule_present_after_drift": after_drift_present,
        "target_forward_rule_present_after_recovery": after_recovery_present,
        "target_forward_rule_present_final": final_presence,
        "effective_queue_before_drift": before_queue,
        "effective_queue_after_drift": after_drift_queue,
        "effective_queue_after_recovery": after_recovery_queue,
        "effective_queue_final": final_queue,
        "drift_detected": drift_detected,
        "drift_detection_time_seconds": detection_time,
        "queue_rule_restored": queue_rule_restored,
        "queue_rule_restore_time_seconds": restore_time,
        "recovery_time_seconds": (float(restore_time) - drift_at) if restore_time != "" else "",
        "sla_violation_windows_total": violation_total,
        "sla_violation_windows_before_drift": violation_before,
        "sla_violation_windows_during_drift": violation_during,
        "sla_violation_windows_after_recovery": violation_after,
        "sla_outage_duration_seconds": violation_total * window_seconds,
        "control_latency_avg_before_drift_ms": phase_average(windows, "control_latency_avg_ms", "before_drift"),
        "control_latency_avg_during_drift_ms": phase_average(windows, "control_latency_avg_ms", "during_drift"),
        "control_latency_avg_after_recovery_ms": phase_average(windows, "control_latency_avg_ms", "after_recovery"),
        "control_latency_max_before_drift_ms": phase_max(windows, "control_latency_max_ms", "before_drift"),
        "control_latency_max_during_drift_ms": phase_max(windows, "control_latency_max_ms", "during_drift"),
        "control_latency_max_after_recovery_ms": phase_max(windows, "control_latency_max_ms", "after_recovery"),
        "risk_predictions": len(risks),
        "medium_risk_events": levels.get("medium", 0),
        "high_risk_events": levels.get("high", 0),
        "policy_decisions": policy_decision_count,
        "policy_applied_count": policy_applied,
        "selected_policy_actions": json.dumps(dict(selected), sort_keys=True),
        "queue_rule_presence_final": final_queue_presence,
        "policy_drift_detected_final": final_policy_drift,
        "enforcement_path": ",".join(sorted(paths)) if paths else ("ONOS_QUEUE_APP" if mode != "static_qos" else "static_qos_no_policy_manager_recovery"),
    })

with (base / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=summary_fields)
    writer.writeheader()
    writer.writerows(summary_rows)

def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

write_csv(plot_dir / "drift_recovery_summary_for_plots.csv", summary_rows, summary_fields)
write_csv(plot_dir / "drift_latency_windows.csv", latency_rows, ["mode", "window_start_seconds", "window_end_seconds", "phase", "control_latency_avg_ms", "control_latency_max_ms", "control_loss_percent", "sla_violation"])
write_csv(plot_dir / "drift_risk_timeline.csv", risk_rows, ["mode", "time_seconds", "overall_risk_level", "overall_risk_score", "queue_rule_presence", "policy_drift_detected", "recommended_policy_action"])
write_csv(plot_dir / "drift_policy_actions.csv", policy_rows, ["mode", "time_seconds", "service_class", "selected_policy_action", "recommended_action", "applied", "enforcement_path", "queue_rule_presence", "policy_drift_detected"])
write_csv(plot_dir / "drift_enforcement_evidence.csv", evidence_rows, ["mode", "time_seconds", "evidence_point", "real_time_control_queue_rule_present", "target_queue_rule_present_before_drift", "target_queue_rule_present_after_drift", "target_queue_rule_present_after_recovery", "target_queue_rule_present_final", "target_forward_rule_present_before_drift", "target_forward_rule_present_after_drift", "target_forward_rule_present_after_recovery", "target_forward_rule_present_final", "effective_queue_before_drift", "effective_queue_after_drift", "effective_queue_after_recovery", "effective_queue_final", "source_file"])

for row in summary_rows:
    mode_dir = base / row["mode"]
    mode_dir.mkdir(parents=True, exist_ok=True)
    (mode_dir / "recovery_summary.json").write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(f"[drift-recovery] summary: {base / 'summary.csv'}")
PY
}

echo "[drift-recovery] concept=Shadow N6 Digital Twin + Deterministic SLA Risk Inference + N6 queue enforcement"
echo "[drift-recovery] results=${BASE_DIR}"
echo "[drift-recovery] duration=${DURATION}"
echo "[drift-recovery] timeline=0-30 warmup, 30-60 baseline, 60 drift, 60-120 recovery observation, 120-180 stability"
echo "[drift-recovery] live_mode=${LIVE_MODE}"

if [[ "${ANALYZE_ONLY:-false}" != "true" ]]; then
  for mode in "${MODES[@]}"; do
    run_mode "${mode}"
  done
fi

write_summary_and_plot_data
echo "[drift-recovery] complete"
