#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BASE_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/dt_risk_${TIMESTAMP}}"
DURATION="${DURATION:-60}"
LIVE_MODE="${LIVE_MODE:-dry-run}"

TRAFFIC_DIR="${TESTBED_DIR}/logs/traffic"
POLICY_LOG_DIR="${TESTBED_DIR}/logs/policy"
DIGITAL_TWIN_LOG_DIR="${TESTBED_DIR}/logs/digital_twin"
RISK_LOG_DIR="${TESTBED_DIR}/logs/risk_inference"

MODES=(fifo static_qos n6_only dt_only dt_risk_assisted)
MODE_DIR=""
MODE_START_ISO=""
MODE_PIDS=()

case "${LIVE_MODE}" in
  dry-run)
    POLICY_MODE_ARGS=(--dry-run)
    ;;
  live)
    POLICY_MODE_ARGS=(--live)
    ;;
  *)
    echo "[dt-risk-experiment] ERROR: LIVE_MODE must be dry-run or live; got ${LIVE_MODE}" >&2
    exit 2
    ;;
esac

mkdir -p "${BASE_DIR}"

warn() {
  echo "[dt-risk-experiment] WARNING: $*" >&2
}

start_background() {
  local label="$1"
  shift
  echo "[dt-risk-experiment] starting ${label}"
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
    docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null || true
  } >"${output}"
}

save_queue_counters() {
  local output="$1"
  {
    echo "# $(date -Is)"
    docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 2>/dev/null || true
    echo
    docker exec ovs ovs-vsctl --columns=name,queues list qos 2>/dev/null || true
    echo
    docker exec ovs ovs-vsctl --columns=_uuid,external_ids,other_config list queue 2>/dev/null || true
  } >"${output}"
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

reset_queue_rules() {
  local mode="$1"
  if [[ -x "${TESTBED_DIR}/clear-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/clear-slice-flows.sh" >"${MODE_DIR}/clear_slice_flows.log" 2>&1 || warn "${mode}: queue cleanup did not verify"
  else
    warn "${mode}: clear-slice-flows.sh missing or not executable"
  fi
}

install_static_queue_rules() {
  if [[ -x "${TESTBED_DIR}/install-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/install-slice-flows.sh" >"${MODE_DIR}/static_qos_install_flows.log" 2>&1 || return 1
    set_queue_flows_visible
    return $?
  fi
  return 1
}

run_traffic() {
  local mode="$1"
  local traffic_output="${MODE_DIR}/traffic"
  mkdir -p "${traffic_output}"
  OUTPUT_ROOT="${traffic_output}" bash "${SCRIPT_DIR}/run_all_traffic.sh" --duration "${DURATION}" >"${MODE_DIR}/${mode}_traffic.log" 2>&1
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

mode_status() {
  local mode="$1"
  local status="$2"
  python3 - "$MODE_DIR" "$mode" "$status" "$DURATION" <<'PY'
import json
import sys
from pathlib import Path

mode_dir = Path(sys.argv[1])
payload = {
    "mode": sys.argv[2],
    "status": sys.argv[3],
    "duration_seconds": float(sys.argv[4]),
}
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

collect_mode_logs() {
  local mode="$1"
  mkdir -p "${MODE_DIR}/policy" "${MODE_DIR}/digital_twin" "${MODE_DIR}/risk_inference"
  python3 - "$MODE_DIR" "$POLICY_LOG_DIR" "$DIGITAL_TWIN_LOG_DIR" "$RISK_LOG_DIR" "$MODE_START_ISO" "$mode" <<'PY'
import json
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

mode_dir, policy_dir, twin_dir, risk_dir = [Path(arg) for arg in sys.argv[1:5]]
start_iso = sys.argv[5]
mode = sys.argv[6]

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

policy_records = []
for path in sorted(policy_dir.glob("closed_loop_policy_*.jsonl")) + sorted(policy_dir.glob("policy_decisions_*.jsonl")):
    records = [payload for payload in iter_jsonl(path) if is_current(payload)]
    if records:
        policy_records.extend(records)

with (mode_dir / "policy" / "policy_decisions.jsonl").open("w", encoding="utf-8") as handle:
    for payload in policy_records:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")

if mode in {"dt_only", "dt_risk_assisted"}:
    twin_records = []
    for path in [twin_dir / "twin_state.jsonl"]:
        twin_records.extend(payload for payload in iter_jsonl(path) if is_current(payload))
    with (mode_dir / "digital_twin" / "twin_snapshots.jsonl").open("w", encoding="utf-8") as handle:
        for payload in twin_records:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
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
    with (mode_dir / "risk_inference" / "risk_predictions.jsonl").open("w", encoding="utf-8") as handle:
        for payload in risk_records:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
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
  python3 "${SCRIPT_DIR}/analyze_sla_violations.py" "$MODE_DIR" >"${MODE_DIR}/analyze_sla_violations.log" 2>&1 || warn "$(basename "$MODE_DIR"): SLA analysis failed"
  [[ -f "${MODE_DIR}/summary_metrics.json" ]] && cp "${MODE_DIR}/summary_metrics.json" "${MODE_DIR}/sla_summary.json"
  python3 "${SCRIPT_DIR}/policy_decision_logger.py" "$MODE_DIR" >"${MODE_DIR}/policy_decision_logger.log" 2>&1 || warn "$(basename "$MODE_DIR"): policy summary failed"
}

run_mode() {
  local mode="$1"
  MODE_DIR="${BASE_DIR}/${mode}"
  MODE_PIDS=()
  MODE_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
  rm -rf "${MODE_DIR}"
  mkdir -p "${MODE_DIR}"
  echo "[dt-risk-experiment] mode=${mode}"
  local status="ok"

  reset_queue_rules "$mode"
  if [[ "$mode" == "static_qos" ]]; then
    install_static_queue_rules || {
      warn "${mode}: static queue rule installation did not verify"
      status="failed"
    }
  fi
  if [[ "$mode" == "fifo" ]] && any_set_queue_flow_visible; then
    warn "${mode}: set_queue flows are visible before FIFO traffic"
    status="failed"
  fi

  save_ovs_flows "${MODE_DIR}/ovs_flows_before.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_before.txt"

  case "$mode" in
    fifo)
      run_traffic "$mode" || status="failed"
      ;;
    static_qos)
      run_traffic "$mode" || status="failed"
      ;;
    n6_only)
      start_telemetry
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      run_traffic "$mode" || status="failed"
      ;;
    dt_only)
      start_telemetry
      start_digital_twin
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      run_traffic "$mode" || status="failed"
      ;;
    dt_risk_assisted)
      start_telemetry
      start_digital_twin
      start_risk_inference
      DT_RISK_INFERENCE_ENABLED=true DT_RISK_PREDICTION_PATH=logs/risk_inference/latest_risk_prediction.json start_policy_manager
      run_traffic "$mode" || status="failed"
      ;;
    *)
      echo "[dt-risk-experiment] unknown mode: ${mode}" >&2
      exit 2
      ;;
  esac

  stop_background
  save_ovs_flows "${MODE_DIR}/ovs_flows_after.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after.txt"
  collect_mode_logs "$mode"
  run_analysis
  mode_status "$mode" "$status"
}

write_summary() {
  python3 - "$BASE_DIR" "$DURATION" <<'PY'
import csv
import json
from collections import Counter
from pathlib import Path
from statistics import mean
import sys

base = Path(sys.argv[1])
duration = float(sys.argv[2])
modes = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]
fields = [
    "mode", "status", "duration_seconds", "sla_checks", "sla_violations", "sla_violation_rate",
    "control_latency_avg_ms", "control_latency_max_ms", "control_loss_percent",
    "data_throughput_bps", "sensor_delivery_ratio_percent", "queue_rule_presence",
    "policy_drift_detected", "risk_predictions", "low_risk_events", "medium_risk_events",
    "high_risk_events", "overall_risk_score_avg", "overall_risk_score_max",
    "policy_decisions", "policy_applied_count", "selected_policy_actions", "enforcement_path",
]

def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def iter_jsonl(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            yield payload

def metric(mode_dir, service, metric_name, column="mean"):
    path = mode_dir / "service_metrics.csv"
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("service_class") == service and row.get("metric") == metric_name:
                return row.get(column) or row.get("latest") or ""
    return ""

def policy_decision_count(records):
    count = 0
    for item in records:
        decisions = item.get("decisions")
        if isinstance(decisions, list):
            count += len(decisions)
        else:
            count += 1
    return count

rows = []
for mode in modes:
    mode_dir = base / mode
    status = load_json(mode_dir / "mode_status.json")
    sla = load_json(mode_dir / "sla_summary.json") or load_json(mode_dir / "summary_metrics.json")
    risks = list(iter_jsonl(mode_dir / "risk_inference" / "risk_predictions.jsonl"))
    policies = list(iter_jsonl(mode_dir / "policy" / "policy_decisions.jsonl"))
    levels = Counter(str(item.get("overall_risk_level", "unknown")).lower() for item in risks)
    scores = [float(item["overall_risk_score"]) for item in risks if item.get("overall_risk_score") is not None]
    selected = Counter(str(item.get("selected_policy_action", "")) for item in policies if item.get("selected_policy_action"))
    paths = Counter(str(item.get("enforcement_path", "")) for item in policies if item.get("enforcement_path"))
    latest_risk = risks[-1] if risks else {}
    rows.append({
        "mode": mode,
        "status": status.get("status", "missing"),
        "duration_seconds": status.get("duration_seconds", duration),
        "sla_checks": sla.get("sla_checks", 0),
        "sla_violations": sla.get("sla_violations", 0),
        "sla_violation_rate": sla.get("sla_violation_rate", 0.0),
        "control_latency_avg_ms": metric(mode_dir, "real_time_control", "latency_avg_ms"),
        "control_latency_max_ms": metric(mode_dir, "real_time_control", "latency_max_ms", "max"),
        "control_loss_percent": metric(mode_dir, "real_time_control", "loss_percent"),
        "data_throughput_bps": metric(mode_dir, "high_throughput_data", "throughput_bps"),
        "sensor_delivery_ratio_percent": metric(mode_dir, "sensor_telemetry", "delivery_ratio_percent"),
        "queue_rule_presence": latest_risk.get("queue_rule_presence", ""),
        "policy_drift_detected": latest_risk.get("policy_drift_detected", ""),
        "risk_predictions": len(risks),
        "low_risk_events": levels.get("low", 0),
        "medium_risk_events": levels.get("medium", 0),
        "high_risk_events": levels.get("high", 0),
        "overall_risk_score_avg": mean(scores) if scores else "",
        "overall_risk_score_max": max(scores) if scores else "",
        "policy_decisions": policy_decision_count(policies),
        "policy_applied_count": sum(1 for item in policies if item.get("applied") is True or (item.get("enforcement_result") or {}).get("applied") is True),
        "selected_policy_actions": json.dumps(dict(selected), sort_keys=True),
        "enforcement_path": ",".join(sorted(paths)) if paths else "",
    })

with (base / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
print(f"[dt-risk-experiment] summary: {base / 'summary.csv'}")
PY
}

echo "[dt-risk-experiment] concept=Shadow N6 Digital Twin + Deterministic Predictive SLA Risk Inference + Deterministic N6 Edge Enforcement"
echo "[dt-risk-experiment] results=${BASE_DIR}"
echo "[dt-risk-experiment] duration=${DURATION}"
echo "[dt-risk-experiment] live_mode=${LIVE_MODE}"

for mode in "${MODES[@]}"; do
  run_mode "$mode"
done

write_summary
echo "[dt-risk-experiment] complete"
