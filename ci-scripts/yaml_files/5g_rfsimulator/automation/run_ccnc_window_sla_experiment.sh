#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_ID="${RUN_ID:-run_1}"
BASE_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/window_sla_${TIMESTAMP}}"
DURATION="${DURATION:-180}"
WINDOW_SECONDS="${WINDOW_SECONDS:-10}"
LIVE_MODE="${LIVE_MODE:-live}"
CONGESTION_PACKETS_PER_BURST="${CONGESTION_PACKETS_PER_BURST:-${DATA_CONGESTION_PACKETS_PER_BURST:-6}}"
CONGESTION_BURST_INTERVAL_SECONDS="${CONGESTION_BURST_INTERVAL_SECONDS:-${DATA_CONGESTION_BURST_INTERVAL_SECONDS:-0.005}}"
DATA_CONGESTION_PACKETS_PER_BURST="${CONGESTION_PACKETS_PER_BURST}"
DATA_CONGESTION_BURST_INTERVAL_SECONDS="${CONGESTION_BURST_INTERVAL_SECONDS}"
DATA_CONGESTION_PAYLOAD_BYTES="${DATA_CONGESTION_PAYLOAD_BYTES:-1200}"
POLICY_LOOP_INTERVAL_SECONDS="${POLICY_LOOP_INTERVAL_SECONDS:-5}"

TRAFFIC_DIR="${TESTBED_DIR}/logs/traffic"
TELEMETRY_LOG_DIR="${TESTBED_DIR}/logs/telemetry"
POLICY_LOG_DIR="${TESTBED_DIR}/logs/policy"
DIGITAL_TWIN_LOG_DIR="${TESTBED_DIR}/logs/digital_twin"
RISK_LOG_DIR="${TESTBED_DIR}/logs/risk_inference"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
MODES=(fifo static_qos n6_only dt_only dt_risk_assisted)

MODE_DIR=""
MODE_START_ISO=""
MODE_PIDS=()
CONGESTION_MAPPING_PATH=""
MODE_TELEMETRY_CONFIG_PATH=""
MODE_POLICY_CONFIG_PATH=""
MODE_TELEMETRY_METRICS_PORT=""
MODE_POLICY_METRICS_PORT=""

case "${LIVE_MODE}" in
  dry-run)
    POLICY_MODE_ARGS=(--dry-run)
    ;;
  live)
    POLICY_MODE_ARGS=(--live)
    ;;
  *)
    echo "[window-sla-experiment] ERROR: LIVE_MODE must be dry-run or live; got ${LIVE_MODE}" >&2
    exit 2
    ;;
esac

mkdir -p "${BASE_DIR}"

warn() {
  echo "[window-sla-experiment] WARNING: $*" >&2
}

start_background() {
  local label="$1"
  shift
  echo "[window-sla-experiment] starting ${label}"
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

all_queue_flows_visible() {
  local flows
  flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
  echo "${flows}" | grep -q "set_queue:1" \
    && echo "${flows}" | grep -q "set_queue:2" \
    && echo "${flows}" | grep -q "set_queue:3"
}

any_queue_flow_visible() {
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep -q "set_queue"
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

create_congestion_mapping() {
  CONGESTION_MAPPING_PATH="${MODE_DIR}/runtime_service_mapping_congested.json"
  python3 - "${SCRIPT_DIR}/service_mapping.yaml" "${CONGESTION_MAPPING_PATH}" \
    "${DATA_CONGESTION_PACKETS_PER_BURST}" \
    "${DATA_CONGESTION_BURST_INTERVAL_SECONDS}" \
    "${DATA_CONGESTION_PAYLOAD_BYTES}" <<'PY'
import json
import sys
from pathlib import Path

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
packets = int(float(sys.argv[3]))
interval = float(sys.argv[4])
payload = int(float(sys.argv[5]))
mapping = json.loads(source.read_text(encoding="utf-8"))
profile = mapping["service_classes"]["high_throughput_data"].setdefault("traffic_profile", {})
profile["udp_packets_per_burst"] = packets
profile["udp_burst_interval_seconds"] = interval
profile["udp_payload_bytes"] = payload
profile["traffic_pattern"] = "continuous_udp"
profile["window_sla_note"] = "continuous offered load for per-window throughput measurement"
dest.write_text(json.dumps(mapping, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

create_runtime_telemetry_config() {
  MODE_TELEMETRY_CONFIG_PATH="${MODE_DIR}/runtime_telemetry_config.yaml"
  python3 - "${TESTBED_DIR}/telemetry/config.yaml" "${MODE_TELEMETRY_CONFIG_PATH}" "${MODE_DIR}/traffic" "${MODE_TELEMETRY_METRICS_PORT}" <<'PY'
import sys
from pathlib import Path

import yaml

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
traffic_dir = Path(sys.argv[3]).resolve()
metrics_port = int(sys.argv[4])
payload = yaml.safe_load(source.read_text(encoding="utf-8"))
telemetry = payload.setdefault("telemetry", {})
telemetry.setdefault("iperf", {})["log_search_dirs"] = [str(traffic_dir)]
telemetry["output_dir"] = str(dest.parent / "telemetry")
telemetry["latest_snapshot_path"] = str(dest.parent / "telemetry" / "closed_loop_latest.json")
telemetry["metrics_http_port"] = metrics_port
dest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
PY
}

create_runtime_policy_config() {
  MODE_POLICY_CONFIG_PATH="${MODE_DIR}/runtime_policy_config.yaml"
  python3 - "${TESTBED_DIR}/policy_manager/config.yaml" "${MODE_POLICY_CONFIG_PATH}" "${MODE_DIR}/telemetry" "${MODE_DIR}/policy" "${MODE_POLICY_METRICS_PORT}" <<'PY'
import sys
from pathlib import Path

import yaml

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
telemetry_dir = Path(sys.argv[3]).resolve()
policy_dir = Path(sys.argv[4]).resolve()
metrics_port = int(sys.argv[5])
payload = yaml.safe_load(source.read_text(encoding="utf-8"))
policy = payload.setdefault("policy_manager", {})
policy["telemetry_dir"] = str(telemetry_dir)
policy["log_dir"] = str(policy_dir)
policy["metrics_http_port"] = metrics_port
policy["manufacturing_twin_enabled"] = False
dest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
PY
}

run_traffic() {
  local mode="$1"
  local traffic_output="${MODE_DIR}/traffic"
  mkdir -p "${traffic_output}"
  OUTPUT_ROOT="${traffic_output}" bash "${SCRIPT_DIR}/run_all_traffic.sh" \
    --mapping "${CONGESTION_MAPPING_PATH}" \
    --duration "${DURATION}" >"${MODE_DIR}/${mode}_traffic.log" 2>&1
}

start_telemetry() {
  start_background telemetry env TELEMETRY_CONFIG_PATH="${MODE_TELEMETRY_CONFIG_PATH}" bash "${SCRIPT_DIR}/run_telemetry.sh"
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
    CONFIG_PATH="${MODE_POLICY_CONFIG_PATH}" \
    DT_RISK_INFERENCE_ENABLED="${DT_RISK_INFERENCE_ENABLED:-false}" \
    DT_RISK_PREDICTION_PATH="${DT_RISK_PREDICTION_PATH:-logs/risk_inference/latest_risk_prediction.json}" \
    DT_RISK_MAX_AGE_SECONDS="${DT_RISK_MAX_AGE_SECONDS:-10}" \
    bash "${SCRIPT_DIR}/run_policy_manager.sh" "${POLICY_MODE_ARGS[@]}"
  sleep 2
}

run_policy_once() {
  local label="$1"
  env \
    PYTHONUNBUFFERED=1 \
    CCNC_DISABLE_MANUFACTURING_TWIN=true \
    DT_RISK_INFERENCE_ENABLED="${DT_RISK_INFERENCE_ENABLED:-false}" \
    DT_RISK_PREDICTION_PATH="${DT_RISK_PREDICTION_PATH:-logs/risk_inference/latest_risk_prediction.json}" \
    DT_RISK_MAX_AGE_SECONDS="${DT_RISK_MAX_AGE_SECONDS:-10}" \
    PYTHONPATH="${TESTBED_DIR}:${PYTHONPATH:-}" \
    python3 -u -m policy_manager.app --config "${MODE_POLICY_CONFIG_PATH}" --once "${POLICY_MODE_ARGS[@]}" \
      >>"${MODE_DIR}/policy_manager.log" 2>&1 || warn "${label}: one policy loop iteration failed"
}

start_policy_loop_driver() {
  local label="$1"
  echo "[window-sla-experiment] starting policy_loop_driver"
  (
    set +e
    local start_epoch end_epoch now decision_count applied_count actions
    start_epoch="$(date +%s)"
    end_epoch=$((start_epoch + DURATION))
    decision_count=0
    applied_count=0
    echo "policy_loop_start_time=$(date -Is)"
    while true; do
      now="$(date +%s)"
      [[ "${now}" -ge "${end_epoch}" ]] && break
      run_policy_once "${label}"
      applied_count="$(python3 - "${MODE_DIR}/policy" <<'PY'
import json
import sys
from pathlib import Path
count = 0
for path in sorted(Path(sys.argv[1]).glob("closed_loop_policy_*.jsonl")):
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("applied") is True or (row.get("enforcement_result") or {}).get("applied") is True:
            count += 1
print(count)
PY
)"
      sleep "${POLICY_LOOP_INTERVAL_SECONDS}"
    done
    actions="$(python3 - "${MODE_DIR}/policy" <<'PY'
import json
import sys
from collections import Counter
from pathlib import Path
actions = Counter()
paths = Counter()
decisions = 0
for path in sorted(Path(sys.argv[1]).glob("closed_loop_policy_*.jsonl")):
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        decisions += 1
        action = row.get("selected_policy_action") or row.get("recommended_policy_action")
        if action:
            actions[str(action)] += 1
        ep = row.get("enforcement_path") or (row.get("enforcement_result") or {}).get("enforcement_path")
        if ep:
            paths[str(ep)] += 1
print(f"policy_loop_decisions={decisions}")
print(f"selected_actions={dict(actions)}")
print(f"enforcement_paths={dict(paths)}")
PY
)"
    echo "policy_loop_stop_time=$(date -Is)"
    echo "policy_loop_applied_actions=${applied_count}"
    echo "${actions}"
  ) >"${MODE_DIR}/policy_loop_driver.log" 2>&1 &
  MODE_PIDS+=("$!")
  sleep 1
}

collect_mode_logs() {
  local mode="$1"
  mkdir -p "${MODE_DIR}/policy" "${MODE_DIR}/digital_twin" "${MODE_DIR}/risk_inference" "${MODE_DIR}/telemetry"
  python3 - "$MODE_DIR" "$POLICY_LOG_DIR" "$DIGITAL_TWIN_LOG_DIR" "$RISK_LOG_DIR" "$TELEMETRY_LOG_DIR" "$MODE_START_ISO" "$mode" <<'PY'
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

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
    parsed = parse_time(payload.get("timestamp") or payload.get("last_updated"))
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
policy_sources = (
    sorted((mode_dir / "policy").glob("closed_loop_policy_*.jsonl"))
    + sorted(policy_dir.glob("closed_loop_policy_*.jsonl"))
    + sorted(policy_dir.glob("policy_decisions_*.jsonl"))
)
for path in policy_sources:
    policy_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
write_jsonl(mode_dir / "policy" / "policy_decisions.jsonl", policy_records)
shutil.copy2(mode_dir / "policy" / "policy_decisions.jsonl", mode_dir / "policy_decisions.jsonl")

telemetry_records = []
telemetry_sources = (
    sorted((mode_dir / "telemetry").glob("closed_loop_telemetry_*.jsonl"))
    + sorted(telemetry_dir.glob("closed_loop_telemetry_*.jsonl"))
    + sorted(telemetry_dir.glob("telemetry_*.jsonl"))
)
for path in telemetry_sources:
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
    shutil.copy2(mode_dir / "risk_inference" / "risk_predictions.jsonl", mode_dir / "risk_predictions.jsonl")
PY
}

write_mode_status() {
  local mode="$1"
  local status="$2"
  local traffic_status="$3"
  python3 - "$MODE_DIR" "$mode" "$status" "$traffic_status" "$DURATION" "$RUN_ID" "$LIVE_MODE" <<'PY'
import json
import sys
from pathlib import Path

mode_dir = Path(sys.argv[1])
payload = {
    "mode": sys.argv[2],
    "status": sys.argv[3],
    "traffic_exit_code": int(sys.argv[4]),
    "duration_seconds": float(sys.argv[5]),
    "run_id": sys.argv[6],
    "live_mode": sys.argv[7],
}
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_analysis() {
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "${MODE_DIR}" >"${MODE_DIR}/analyze_sla_violations.log" 2>&1 || warn "$(basename "${MODE_DIR}"): aggregate SLA analysis failed"
  [[ -f "${MODE_DIR}/summary_metrics.json" ]] && cp "${MODE_DIR}/summary_metrics.json" "${MODE_DIR}/sla_summary.json"
  python3 "${SCRIPT_DIR}/analyze_window_sla_results.py" "${MODE_DIR}" --window-seconds "${WINDOW_SECONDS}" --duration-seconds "${DURATION}" >"${MODE_DIR}/analyze_window_sla_results.log" 2>&1 || warn "$(basename "${MODE_DIR}"): window SLA analysis failed"
}

run_mode() {
  local mode="$1"
  local status="ok"
  local traffic_status=0
  local mode_index=0
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  MODE_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
  case "${mode}" in
    fifo) mode_index=0 ;;
    static_qos) mode_index=1 ;;
    n6_only) mode_index=2 ;;
    dt_only) mode_index=3 ;;
    dt_risk_assisted) mode_index=4 ;;
  esac
  MODE_TELEMETRY_METRICS_PORT="$((8100 + mode_index))"
  MODE_POLICY_METRICS_PORT="$((8200 + mode_index))"
  mkdir -p "${MODE_DIR}" "${MODE_DIR}/policy" "${MODE_DIR}/telemetry"
  echo "[window-sla-experiment] mode=${mode}"
  clear_traffic_logs
  create_congestion_mapping
  mkdir -p "${MODE_DIR}/traffic"
  create_runtime_telemetry_config
  create_runtime_policy_config
  reset_queue_rules "${mode}"

  if [[ "${mode}" == "fifo" ]]; then
    if any_queue_flow_visible; then
      warn "fifo: set_queue flows remain after cleanup"
      status="failed"
    fi
  else
    install_baseline_queue_rules "${mode}" || {
      warn "${mode}: baseline queue rules did not verify"
      status="failed"
    }
  fi

  case "${mode}" in
    fifo|static_qos)
      ;;
    n6_only)
      start_telemetry
      DT_RISK_INFERENCE_ENABLED=false start_policy_loop_driver "${mode}"
      ;;
    dt_only)
      start_telemetry
      start_digital_twin
      DT_RISK_INFERENCE_ENABLED=false start_policy_loop_driver "${mode}"
      ;;
    dt_risk_assisted)
      start_telemetry
      start_digital_twin
      start_risk_inference
      DT_RISK_INFERENCE_ENABLED=true DT_RISK_PREDICTION_PATH=logs/risk_inference/latest_risk_prediction.json start_policy_loop_driver "${mode}"
      ;;
    *)
      echo "[window-sla-experiment] unknown mode: ${mode}" >&2
      exit 2
      ;;
  esac

  run_traffic "${mode}" || traffic_status=$?
  [[ "${traffic_status}" -eq 0 ]] || status="failed"
  stop_background
  save_ovs_flows "${MODE_DIR}/ovs_flows_after.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after.txt"
  collect_mode_logs "${mode}"
  write_mode_status "${mode}" "${status}" "${traffic_status}"
  run_analysis
}

write_top_level_summaries() {
  python3 "${SCRIPT_DIR}/analyze_window_sla_results.py" "${BASE_DIR}" --window-seconds "${WINDOW_SECONDS}" --duration-seconds "${DURATION}" >"${BASE_DIR}/analyze_window_sla_results.log" 2>&1
  cp "${BASE_DIR}/window_sla_by_mode.csv" "${BASE_DIR}/window_sla_all_modes.csv"
  python3 - "$BASE_DIR" <<'PY'
import csv
import json
from collections import Counter
from pathlib import Path
from statistics import mean, pstdev
import sys

base = Path(sys.argv[1])
modes = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]

def read_csv(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        return list(csv.DictReader(handle))

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

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in fields} for row in rows])

window_summary = read_csv(base / "window_sla_summary.csv")
by_mode = {row.get("mode"): row for row in window_summary}
summary_rows = []
evidence_rows = []
risk_rows = []
policy_rows = []
mean_rows = []

for mode in modes:
    mode_dir = base / mode
    status = {}
    try:
        status = json.loads((mode_dir / "mode_status.json").read_text(encoding="utf-8"))
    except Exception:
        pass
    risks = list(iter_jsonl(mode_dir / "risk_inference" / "risk_predictions.jsonl"))
    policies = list(iter_jsonl(mode_dir / "policy" / "policy_decisions.jsonl"))
    policy_actions = Counter()
    policy_applied = 0
    paths = Counter()
    for payload in policies:
        action = str(payload.get("selected_policy_action") or payload.get("recommended_policy_action") or "")
        if action:
            policy_actions[action] += 1
        for decision in payload.get("decisions", []) if isinstance(payload.get("decisions"), list) else []:
            decision_action = str(decision.get("selected_policy_action") or decision.get("recommended_action") or "")
            if decision_action:
                policy_actions[decision_action] += 1
        if payload.get("applied") is True or (payload.get("enforcement_result") or {}).get("applied") is True:
            policy_applied += 1
        path = str(payload.get("enforcement_path") or (payload.get("enforcement_result") or {}).get("enforcement_path") or "")
        if path:
            paths[path] += 1
    risk_levels = Counter(str(payload.get("overall_risk_level", "")).lower() for payload in risks)
    row = by_mode.get(mode, {})
    merged = {
        "mode": mode,
        "status": status.get("status", "missing"),
        "run_id": status.get("run_id", ""),
        "window_sla_violation_rate": row.get("window_sla_violation_rate", ""),
        "total_violation_duration_seconds": row.get("total_violation_duration_seconds", ""),
        "avg_violation_severity": row.get("avg_violation_severity", ""),
        "rtc_avg_latency_ms_mean": row.get("rtc_avg_latency_ms_mean", ""),
        "rtc_p95_latency_ms_mean": row.get("rtc_p95_latency_ms_mean", ""),
        "rtc_loss_percent_mean": row.get("rtc_loss_percent_mean", ""),
        "data_throughput_mbps_mean": row.get("data_throughput_mbps_mean", ""),
        "sensor_delivery_ratio_percent_mean": row.get("sensor_delivery_ratio_percent_mean", ""),
        "risk_predictions": len(risks),
        "medium_risk_events": risk_levels.get("medium", 0),
        "high_risk_events": risk_levels.get("high", 0),
        "policy_decisions": len(policies),
        "policy_applied_count": policy_applied,
        "selected_policy_actions": json.dumps(dict(policy_actions), sort_keys=True),
        "enforcement_path": ",".join(sorted(paths)) if paths else ("none" if mode in {"fifo", "static_qos"} else "ONOS_QUEUE_APP"),
    }
    summary_rows.append(merged)
    evidence_rows.append({
        "mode": mode,
        "queue_rule_presence_final": "present" if "set_queue:2" in (mode_dir / "ovs_flows_after.txt").read_text(encoding="utf-8", errors="ignore") else "missing",
        "enforcement_path": merged["enforcement_path"],
        "ovs_flows_after": str(mode_dir / "ovs_flows_after.txt"),
        "queue_counters_after": str(mode_dir / "queue_counters_after.txt"),
    })
    risk_rows.append({
        "mode": mode,
        "risk_predictions": len(risks),
        "medium_risk_events": risk_levels.get("medium", 0),
        "high_risk_events": risk_levels.get("high", 0),
    })
    policy_rows.append({
        "mode": mode,
        "policy_decisions": len(policies),
        "policy_applied_count": policy_applied,
        "selected_policy_actions": json.dumps(dict(policy_actions), sort_keys=True),
    })

for mode in modes:
    rows = [row for row in summary_rows if row["mode"] == mode]
    def vals(key):
        out = [to_float(row.get(key)) for row in rows]
        return [value for value in out if value is not None]
    def avg(key):
        data = vals(key)
        return mean(data) if data else ""
    def std(key):
        data = vals(key)
        return pstdev(data) if len(data) > 1 else 0.0 if data else ""
    mean_rows.append({
        "mode": mode,
        "window_sla_violation_rate_mean": avg("window_sla_violation_rate"),
        "window_sla_violation_rate_std": std("window_sla_violation_rate"),
        "total_violation_duration_seconds_mean": avg("total_violation_duration_seconds"),
        "avg_violation_severity_mean": avg("avg_violation_severity"),
        "rtc_avg_latency_ms_mean": avg("rtc_avg_latency_ms_mean"),
        "rtc_p95_latency_ms_mean": avg("rtc_p95_latency_ms_mean"),
        "rtc_loss_percent_mean": avg("rtc_loss_percent_mean"),
        "data_throughput_mbps_mean": avg("data_throughput_mbps_mean"),
        "sensor_delivery_ratio_percent_mean": avg("sensor_delivery_ratio_percent_mean"),
        "risk_predictions_mean": avg("risk_predictions"),
        "policy_decisions_mean": avg("policy_decisions"),
        "policy_applied_count_mean": avg("policy_applied_count"),
        "enforcement_path": rows[0]["enforcement_path"] if rows else "",
    })

summary_fields = [
    "mode", "status", "run_id", "window_sla_violation_rate", "total_violation_duration_seconds",
    "avg_violation_severity", "rtc_avg_latency_ms_mean", "rtc_p95_latency_ms_mean",
    "rtc_loss_percent_mean", "data_throughput_mbps_mean", "sensor_delivery_ratio_percent_mean",
    "risk_predictions", "medium_risk_events", "high_risk_events", "policy_decisions",
    "policy_applied_count", "selected_policy_actions", "enforcement_path",
]
mean_fields = [
    "mode", "window_sla_violation_rate_mean", "window_sla_violation_rate_std",
    "total_violation_duration_seconds_mean", "avg_violation_severity_mean",
    "rtc_avg_latency_ms_mean", "rtc_p95_latency_ms_mean", "rtc_loss_percent_mean",
    "data_throughput_mbps_mean", "sensor_delivery_ratio_percent_mean",
    "risk_predictions_mean", "policy_decisions_mean", "policy_applied_count_mean", "enforcement_path",
]
write_csv(base / "summary.csv", summary_rows, summary_fields)
  # analyze_window_sla_results.py already writes the validity-aware
  # window_sla_mean_std_summary.csv. Keep that corrected file intact.
write_csv(base / "enforcement_evidence.csv", evidence_rows, ["mode", "queue_rule_presence_final", "enforcement_path", "ovs_flows_after", "queue_counters_after"])
write_csv(base / "risk_prediction_summary.csv", risk_rows, ["mode", "risk_predictions", "medium_risk_events", "high_risk_events"])
write_csv(base / "policy_action_counts.csv", policy_rows, ["mode", "policy_decisions", "policy_applied_count", "selected_policy_actions"])
PY
}

echo "[window-sla-experiment] concept=Shadow N6 Digital Twin + Deterministic SLA Risk Inference + ONOS-controlled N6 edge enforcement"
echo "[window-sla-experiment] results=${BASE_DIR}"
echo "[window-sla-experiment] duration=${DURATION}"
echo "[window-sla-experiment] run_id=${RUN_ID}"
echo "[window-sla-experiment] congestion_packets_per_burst=${DATA_CONGESTION_PACKETS_PER_BURST}"
echo "[window-sla-experiment] congestion_burst_interval_seconds=${DATA_CONGESTION_BURST_INTERVAL_SECONDS}"
echo "[window-sla-experiment] env_CONGESTION_PACKETS_PER_BURST=${CONGESTION_PACKETS_PER_BURST}"
echo "[window-sla-experiment] env_CONGESTION_BURST_INTERVAL_SECONDS=${CONGESTION_BURST_INTERVAL_SECONDS}"
echo "[window-sla-experiment] policy_loop_interval_seconds=${POLICY_LOOP_INTERVAL_SECONDS}"
echo "[window-sla-experiment] live_mode=${LIVE_MODE}"

for mode in "${MODES[@]}"; do
  run_mode "${mode}"
done

write_top_level_summaries
echo "[window-sla-experiment] complete"
