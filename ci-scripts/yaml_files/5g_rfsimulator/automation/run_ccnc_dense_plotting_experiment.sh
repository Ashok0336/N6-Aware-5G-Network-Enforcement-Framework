#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_ID="${RUN_ID:-run_1}"
DURATION="${DURATION:-300}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-1}"
CONGESTION_PACKETS_PER_BURST="${CONGESTION_PACKETS_PER_BURST:-6}"
CONGESTION_BURST_INTERVAL_SECONDS="${CONGESTION_BURST_INTERVAL_SECONDS:-0.005}"
DATA_CONGESTION_PAYLOAD_BYTES="${DATA_CONGESTION_PAYLOAD_BYTES:-1200}"
LIVE_MODE="${LIVE_MODE:-live}"
BASE_DIR="${RESULTS_DIR:-${TESTBED_DIR}/logs/experiments/ccnc/dense_plotting_${TIMESTAMP}}"

OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
RISK_LOG_DIR="${TESTBED_DIR}/logs/risk_inference"
MODES=(fifo static_qos n6_only dt_only dt_risk_assisted)

MODE_DIR=""
MODE_START_ISO=""
MODE_PIDS=()
MODE_TELEMETRY_CONFIG_PATH=""
MODE_POLICY_CONFIG_PATH=""
MODE_SERVICE_MAPPING_PATH=""
MODE_TELEMETRY_METRICS_PORT=""
MODE_POLICY_METRICS_PORT=""

case "${LIVE_MODE}" in
  dry-run) POLICY_MODE_ARGS=(--dry-run) ;;
  live) POLICY_MODE_ARGS=(--live) ;;
  *)
    echo "[dense-plotting] ERROR: LIVE_MODE must be dry-run or live; got ${LIVE_MODE}" >&2
    exit 2
    ;;
esac

mkdir -p "${BASE_DIR}"

warn() {
  echo "[dense-plotting] WARNING: $*" >&2
}

start_background() {
  local label="$1"
  shift
  echo "[dense-plotting] starting ${label}"
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

any_queue_flow_visible() {
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep -q "set_queue"
}

all_queue_flows_visible() {
  local flows
  flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
  echo "${flows}" | grep -q "set_queue:1" \
    && echo "${flows}" | grep -q "set_queue:2" \
    && echo "${flows}" | grep -q "set_queue:3"
}

reset_queue_rules() {
  if [[ -x "${TESTBED_DIR}/clear-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/clear-slice-flows.sh" >"${MODE_DIR}/clear_slice_flows.log" 2>&1 || warn "$(basename "${MODE_DIR}"): queue cleanup did not verify"
  fi
}

install_baseline_queue_rules() {
  if [[ -x "${TESTBED_DIR}/install-slice-flows.sh" ]]; then
    bash "${TESTBED_DIR}/install-slice-flows.sh" >"${MODE_DIR}/install_slice_flows.log" 2>&1 || return 1
    all_queue_flows_visible
    return $?
  fi
  return 1
}

create_runtime_service_mapping() {
  MODE_SERVICE_MAPPING_PATH="${MODE_DIR}/runtime_service_mapping_dense.json"
  python3 - "${SCRIPT_DIR}/service_mapping.yaml" "${MODE_SERVICE_MAPPING_PATH}" \
    "${SAMPLE_INTERVAL_SECONDS}" "${CONGESTION_PACKETS_PER_BURST}" \
    "${CONGESTION_BURST_INTERVAL_SECONDS}" "${DATA_CONGESTION_PAYLOAD_BYTES}" <<'PY'
import json
import sys
from pathlib import Path

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
sample_interval = float(sys.argv[3])
data_packets = int(float(sys.argv[4]))
data_interval = float(sys.argv[5])
data_payload = int(float(sys.argv[6]))
mapping = json.loads(source.read_text(encoding="utf-8"))
services = mapping["service_classes"]
services["real_time_control"].setdefault("traffic_profile", {})["ping_interval_seconds"] = sample_interval
data_profile = services["high_throughput_data"].setdefault("traffic_profile", {})
data_profile["udp_packets_per_burst"] = data_packets
data_profile["udp_burst_interval_seconds"] = data_interval
data_profile["udp_payload_bytes"] = data_payload
data_profile["traffic_pattern"] = "continuous_udp_dense_plotting"
sensor_profile = services["sensor_telemetry"].setdefault("traffic_profile", {})
sensor_profile["udp_burst_interval_seconds"] = sample_interval
sensor_profile["traffic_pattern"] = "continuous_timestamped_udp_dense_plotting"
dest.write_text(json.dumps(mapping, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

create_runtime_telemetry_config() {
  MODE_TELEMETRY_CONFIG_PATH="${MODE_DIR}/runtime_telemetry_config.yaml"
  python3 - "${TESTBED_DIR}/telemetry/config.yaml" "${MODE_TELEMETRY_CONFIG_PATH}" \
    "${MODE_DIR}/traffic" "${MODE_TELEMETRY_METRICS_PORT}" "${SAMPLE_INTERVAL_SECONDS}" <<'PY'
import sys
from pathlib import Path

import yaml

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
traffic_dir = Path(sys.argv[3]).resolve()
metrics_port = int(sys.argv[4])
sample_interval = float(sys.argv[5])
payload = yaml.safe_load(source.read_text(encoding="utf-8"))
telemetry = payload.setdefault("telemetry", {})
telemetry["poll_interval_seconds"] = sample_interval
telemetry["output_dir"] = str(dest.parent / "telemetry")
telemetry["latest_snapshot_path"] = str(dest.parent / "telemetry" / "closed_loop_latest.json")
telemetry["metrics_http_port"] = metrics_port
telemetry.setdefault("iperf", {})["log_search_dirs"] = [str(traffic_dir)]
for probe in telemetry.setdefault("ping", {}).setdefault("probes", []):
    probe["count"] = 1
    probe["timeout_seconds"] = 1
dest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
PY
}

create_runtime_policy_config() {
  MODE_POLICY_CONFIG_PATH="${MODE_DIR}/runtime_policy_config.yaml"
  python3 - "${TESTBED_DIR}/policy_manager/config.yaml" "${MODE_POLICY_CONFIG_PATH}" \
    "${MODE_DIR}/telemetry" "${MODE_DIR}/policy" "${MODE_POLICY_METRICS_PORT}" "${SAMPLE_INTERVAL_SECONDS}" <<'PY'
import sys
from pathlib import Path

import yaml

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
telemetry_dir = Path(sys.argv[3]).resolve()
policy_dir = Path(sys.argv[4]).resolve()
metrics_port = int(sys.argv[5])
sample_interval = float(sys.argv[6])
payload = yaml.safe_load(source.read_text(encoding="utf-8"))
policy = payload.setdefault("policy_manager", {})
policy["telemetry_dir"] = str(telemetry_dir)
policy["log_dir"] = str(policy_dir)
policy["metrics_http_port"] = metrics_port
policy["poll_interval_seconds"] = sample_interval
policy["decision_cooldown_seconds"] = max(2.0, sample_interval * 2.0)
policy["manufacturing_twin_enabled"] = False
dest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
PY
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
  risk_duration="$(python3 - "${DURATION}" <<'PY'
import sys
print(int(float(sys.argv[1]) + 20))
PY
)"
  start_background risk_inference bash "${TESTBED_DIR}/risk_inference/run_risk_inference.sh" --duration "${risk_duration}" --interval "${SAMPLE_INTERVAL_SECONDS}" --output-dir logs/risk_inference
  sleep 2
}

start_policy_manager() {
  echo "policy_loop_start_time=$(date -Is)" >"${MODE_DIR}/policy_loop_driver.log"
  start_background policy_manager env \
    CONFIG_PATH="${MODE_POLICY_CONFIG_PATH}" \
    CCNC_DISABLE_MANUFACTURING_TWIN=true \
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
      >>"${MODE_DIR}/policy_manager.log" 2>&1 || warn "${label}: one policy iteration failed"
}

summarize_policy_loop() {
  python3 - "${MODE_DIR}/policy" >>"${MODE_DIR}/policy_loop_driver.log" <<'PY'
import json
import sys
from collections import Counter
from pathlib import Path

actions = Counter()
paths = Counter()
applied = 0
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
        if row.get("applied") is True or (row.get("enforcement_result") or {}).get("applied") is True:
            applied += 1
        path_name = row.get("enforcement_path") or (row.get("enforcement_result") or {}).get("enforcement_path")
        if path_name:
            paths[str(path_name)] += 1
print(f"policy_loop_decisions={decisions}")
print(f"policy_loop_applied_actions={applied}")
print(f"selected_actions={dict(actions)}")
print(f"enforcement_paths={dict(paths)}")
PY
  echo "policy_loop_stop_time=$(date -Is)" >>"${MODE_DIR}/policy_loop_driver.log"
}

start_policy_loop_driver() {
  local label="$1"
  (
    set +e
    local start_epoch end_epoch now actions
    start_epoch="$(date +%s)"
    end_epoch=$((start_epoch + ${DURATION%.*}))
    echo "policy_loop_start_time=$(date -Is)"
    while true; do
      now="$(date +%s)"
      [[ "${now}" -ge "${end_epoch}" ]] && break
      run_policy_once "${label}"
      sleep "${SAMPLE_INTERVAL_SECONDS}"
    done
    actions="$(python3 - "${MODE_DIR}/policy" <<'PY'
import json
import sys
from collections import Counter
from pathlib import Path
actions = Counter()
paths = Counter()
applied = 0
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
        if row.get("applied") is True or (row.get("enforcement_result") or {}).get("applied") is True:
            applied += 1
        path_name = row.get("enforcement_path") or (row.get("enforcement_result") or {}).get("enforcement_path")
        if path_name:
            paths[str(path_name)] += 1
print(f"policy_loop_decisions={decisions}")
print(f"policy_loop_applied_actions={applied}")
print(f"selected_actions={dict(actions)}")
print(f"enforcement_paths={dict(paths)}")
PY
)"
    echo "policy_loop_stop_time=$(date -Is)"
    echo "${actions}"
  ) >"${MODE_DIR}/policy_loop_driver.log" 2>&1 &
  MODE_PIDS+=("$!")
  sleep 1
}

run_traffic() {
  OUTPUT_ROOT="${MODE_DIR}/traffic" bash "${SCRIPT_DIR}/run_all_traffic.sh" \
    --mapping "${MODE_SERVICE_MAPPING_PATH}" \
    --duration "${DURATION}" >"${MODE_DIR}/traffic.log" 2>&1
}

copy_mode_risk_predictions() {
  mkdir -p "${MODE_DIR}/risk_inference"
  python3 - "${MODE_DIR}" "${RISK_LOG_DIR}" "${MODE_START_ISO}" <<'PY'
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

mode_dir = Path(sys.argv[1])
risk_dir = Path(sys.argv[2])
start_iso = sys.argv[3]

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
records = []
path = risk_dir / "risk_predictions.jsonl"
if path.exists():
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = parse_time(row.get("timestamp"))
        if ts is not None and ts >= start:
            records.append(row)
out = mode_dir / "risk_inference" / "risk_predictions.jsonl"
with out.open("w", encoding="utf-8") as handle:
    for row in records:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
shutil.copy2(out, mode_dir / "risk_predictions.jsonl")
PY
}

write_mode_status() {
  local mode="$1"
  local status="$2"
  local traffic_status="$3"
  python3 - "$MODE_DIR" "$mode" "$status" "$traffic_status" "$DURATION" "$RUN_ID" "$SAMPLE_INTERVAL_SECONDS" "$LIVE_MODE" <<'PY'
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
    "sample_interval_seconds": float(sys.argv[7]),
    "live_mode": sys.argv[8],
}
(mode_dir / "mode_status.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

extract_dense_csvs() {
  python3 - "${MODE_DIR}" "${RUN_ID}" "${SAMPLE_INTERVAL_SECONDS}" <<'PY'
import csv
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

mode_dir = Path(sys.argv[1])
run_id = sys.argv[2]
sample_interval = float(sys.argv[3])
mode = mode_dir.name

def parse_time(value):
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def iso(ts):
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z") if ts else ""

def iter_jsonl(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            yield row

def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})

def to_float(value):
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None

def first_ts_from_sources():
    candidates = []
    for path in sorted((mode_dir / "telemetry").glob("closed_loop_telemetry_*.jsonl")):
        for row in iter_jsonl(path):
            ts = parse_time(row.get("timestamp"))
            if ts:
                candidates.append(ts)
                break
    for path in sorted((mode_dir / "traffic").rglob("*.log")):
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = re.search(r"timestamp=([^\s]+)", line)
            if match:
                ts = parse_time(match.group(1))
                if ts:
                    candidates.append(ts)
                    break
            match = re.match(r"\[([0-9.]+)\].*time=([0-9.]+)\s*ms", line)
            if match:
                candidates.append(datetime.fromtimestamp(float(match.group(1)), tz=timezone.utc))
                break
    return min(candidates) if candidates else datetime.now(timezone.utc)

start_ts = first_ts_from_sources()

def rel_seconds(ts):
    return max(0.0, (ts - start_ts).total_seconds()) if ts else ""

def bucket(t):
    value = to_float(t)
    return int(value // sample_interval) if value is not None else None

telemetry_rows = []
for path in sorted((mode_dir / "telemetry").glob("closed_loop_telemetry_*.jsonl")):
    telemetry_rows.extend(iter_jsonl(path))
telemetry_rows = [row for row in telemetry_rows if row.get("event_type") != "collector_stop"]

control_samples = []
for path in sorted((mode_dir / "traffic" / "real_time_control").glob("control_*.log")):
    if "_udp" in path.name:
        continue
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = re.match(r"\[([0-9.]+)\].*time=([0-9.]+)\s*ms", line)
        if not match:
            continue
        ts = datetime.fromtimestamp(float(match.group(1)), tz=timezone.utc)
        latency = float(match.group(2))
        control_samples.append({"ts": ts, "latency": latency})

control_by_bucket = defaultdict(list)
for sample in control_samples:
    control_by_bucket[bucket(rel_seconds(sample["ts"]))].append(sample)
control_rows = []
for idx in sorted(key for key in control_by_bucket if key is not None):
    values = [item["latency"] for item in control_by_bucket[idx]]
    ts = min(item["ts"] for item in control_by_bucket[idx])
    control_rows.append({
        "run_id": run_id,
        "mode": mode,
        "time_seconds": idx * sample_interval,
        "timestamp": iso(ts),
        "latency_avg_ms": mean(values),
        "latency_max_ms": max(values),
        "loss_percent": "",
        "service_class": "real_time_control",
    })

data_points = []
sensor_sent_points = []
for service, subdir in (("high_throughput_data", "high_throughput_data"), ("sensor_telemetry", "sensor_telemetry")):
    for path in sorted((mode_dir / "traffic" / subdir).glob("*.log")):
        previous = None
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = re.search(r"timestamp=([^\s]+).*elapsed_seconds=([0-9.]+).*sent_packets=([0-9]+).*bytes_sent=([0-9]+)", line)
            if not match:
                continue
            ts = parse_time(match.group(1))
            packets = int(match.group(3))
            bytes_sent = int(match.group(4))
            if previous is not None and ts is not None:
                prev_ts, prev_packets, prev_bytes = previous
                dt = max(1e-9, (ts - prev_ts).total_seconds())
                delta_bytes = max(0, bytes_sent - prev_bytes)
                delta_packets = max(0, packets - prev_packets)
                record = {
                    "source": str(path),
                    "ts": ts,
                    "throughput_bps": delta_bytes * 8.0 / dt,
                    "packets": delta_packets,
                    "total_packets": packets,
                }
                if service == "high_throughput_data":
                    data_points.append(record)
                else:
                    sensor_sent_points.append(record)
            if ts is not None:
                previous = (ts, packets, bytes_sent)

data_by_bucket = defaultdict(lambda: defaultdict(list))
for point in data_points:
    idx = bucket(rel_seconds(point["ts"]))
    if idx is not None:
        data_by_bucket[idx][point["source"]].append(point)
data_rows = []
for idx in sorted(data_by_bucket):
    source_means = [mean(item["throughput_bps"] for item in points) for points in data_by_bucket[idx].values() if points]
    ts = min(point["ts"] for points in data_by_bucket[idx].values() for point in points)
    throughput = sum(source_means)
    data_rows.append({
        "run_id": run_id,
        "mode": mode,
        "time_seconds": idx * sample_interval,
        "timestamp": iso(ts),
        "throughput_bps": throughput,
        "throughput_mbps": throughput / 1_000_000.0,
        "service_class": "high_throughput_data",
    })

sensor_rows = []
for row in telemetry_rows:
    ts = parse_time(row.get("timestamp"))
    metrics = (row.get("slice_metrics") or {}).get("mmtc") or {}
    if not ts or not isinstance(metrics, dict):
        continue
    delivery = to_float(metrics.get("delivery_ratio_percent") or metrics.get("reliability_proxy_percent"))
    received = to_float(metrics.get("flow_packets_total"))
    expected = to_float(metrics.get("sender_packets_total"))
    if delivery is None and received is None and expected is None:
        continue
    sensor_rows.append({
        "run_id": run_id,
        "mode": mode,
        "time_seconds": rel_seconds(ts),
        "timestamp": iso(ts),
        "delivery_ratio_percent": delivery if delivery is not None else "",
        "received_packets": int(received) if received is not None else "",
        "expected_packets": int(expected) if expected is not None else "",
        "service_class": "sensor_telemetry",
    })

service_rows = []
for row in telemetry_rows:
    ts = parse_time(row.get("timestamp"))
    if not ts:
        continue
    for slice_name, service in (("urllc", "real_time_control"), ("embb", "high_throughput_data"), ("mmtc", "sensor_telemetry")):
        metrics = (row.get("slice_metrics") or {}).get(slice_name) or {}
        if not isinstance(metrics, dict):
            continue
        service_rows.append({
            "run_id": run_id,
            "mode": mode,
            "time_seconds": rel_seconds(ts),
            "timestamp": iso(ts),
            "service_class": service,
            "latency_avg_ms": metrics.get("latency_avg_ms", ""),
            "latency_max_ms": metrics.get("latency_max_ms", ""),
            "loss_percent": metrics.get("loss_percent", ""),
            "throughput_bps": metrics.get("throughput_bps", ""),
            "delivery_ratio_percent": metrics.get("delivery_ratio_percent", metrics.get("reliability_proxy_percent", "")),
        })

rtc_by_bucket = {bucket(row["time_seconds"]): row for row in control_rows}
data_by_bucket_row = {bucket(row["time_seconds"]): row for row in data_rows}
sensor_by_bucket = {bucket(row["time_seconds"]): row for row in sensor_rows}
window_rows = []
all_buckets = sorted(set(rtc_by_bucket) | set(data_by_bucket_row) | set(sensor_by_bucket))
for idx in all_buckets:
    if idx is None:
        continue
    rtc = rtc_by_bucket.get(idx, {})
    data = data_by_bucket_row.get(idx, {})
    sensor = sensor_by_bucket.get(idx, {})
    violations = {}
    severities = {}
    if rtc:
        avg_latency = to_float(rtc.get("latency_avg_ms"))
        max_latency = to_float(rtc.get("latency_max_ms"))
        violations["real_time_control"] = (avg_latency is not None and avg_latency > 10.0) or (max_latency is not None and max_latency > 20.0)
        severities["rtc_avg_latency"] = max(0.0, ((avg_latency or 0.0) - 10.0) / 10.0) if avg_latency is not None else 0.0
        severities["rtc_max_latency"] = max(0.0, ((max_latency or 0.0) - 20.0) / 20.0) if max_latency is not None else 0.0
    if data:
        throughput = to_float(data.get("throughput_bps"))
        violations["high_throughput_data"] = throughput is not None and throughput < 50_000_000.0
        severities["data_throughput"] = max(0.0, (50_000_000.0 - (throughput or 0.0)) / 50_000_000.0) if throughput is not None else 0.0
    if sensor:
        delivery = to_float(sensor.get("delivery_ratio_percent"))
        violations["sensor_telemetry"] = delivery is not None and delivery < 98.0
        severities["sensor_delivery"] = max(0.0, (98.0 - (delivery or 0.0)) / 98.0) if delivery is not None else 0.0
    valid = len(violations)
    weighted = sum(1 for value in violations.values() if value) / valid if valid else ""
    dominant = max(severities, key=severities.get) if severities and max(severities.values()) > 0 else ""
    window_rows.append({
        "run_id": run_id,
        "mode": mode,
        "time_seconds": idx * sample_interval,
        "window_index": idx,
        "real_time_control_sla_violation": violations.get("real_time_control", ""),
        "high_throughput_data_sla_violation": violations.get("high_throughput_data", ""),
        "sensor_telemetry_sla_violation": violations.get("sensor_telemetry", ""),
        "weighted_sla_violation_score": weighted,
        "dominant_violation_metric": dominant,
    })

risk_rows = []
risk_path = mode_dir / "risk_predictions.jsonl"
for row in iter_jsonl(risk_path):
    ts = parse_time(row.get("timestamp"))
    service_risks = row.get("service_risks") if isinstance(row.get("service_risks"), dict) else {}
    def service_score(name):
        record = service_risks.get(name) if isinstance(service_risks.get(name), dict) else {}
        return record.get("risk_score", "")
    risk_rows.append({
        "run_id": run_id,
        "mode": mode,
        "time_seconds": rel_seconds(ts) if ts else "",
        "timestamp": iso(ts),
        "overall_risk_score": row.get("overall_risk_score", ""),
        "overall_risk_level": row.get("overall_risk_level", ""),
        "real_time_control_risk": service_score("real_time_control"),
        "high_throughput_data_risk": service_score("high_throughput_data"),
        "sensor_telemetry_risk": service_score("sensor_telemetry"),
        "recommended_policy_action": row.get("recommended_policy_action", ""),
    })

policy_rows = []
for path in sorted((mode_dir / "policy").glob("closed_loop_policy_*.jsonl")) + [mode_dir / "policy_decisions.jsonl"]:
    for row in iter_jsonl(path):
        ts = parse_time(row.get("timestamp"))
        if not ts:
            continue
        enforcement = row.get("enforcement_result") if isinstance(row.get("enforcement_result"), dict) else {}
        policy_rows.append({
            "run_id": run_id,
            "mode": mode,
            "time_seconds": rel_seconds(ts),
            "timestamp": iso(ts),
            "selected_policy_action": row.get("selected_policy_action") or row.get("recommended_policy_action") or "",
            "applied": row.get("applied", enforcement.get("applied", "")),
            "enforcement_path": row.get("enforcement_path") or enforcement.get("enforcement_path", ""),
            "action_reason": row.get("action_reason", ""),
        })
dedup = {(row["timestamp"], row["selected_policy_action"], row["applied"]): row for row in policy_rows}
policy_rows = sorted(dedup.values(), key=lambda item: item["time_seconds"])

write_csv(mode_dir / "dense_control_latency_timeseries.csv", control_rows, ["run_id", "mode", "time_seconds", "timestamp", "latency_avg_ms", "latency_max_ms", "loss_percent", "service_class"])
write_csv(mode_dir / "dense_data_throughput_timeseries.csv", data_rows, ["run_id", "mode", "time_seconds", "timestamp", "throughput_bps", "throughput_mbps", "service_class"])
write_csv(mode_dir / "dense_sensor_telemetry_timeseries.csv", sensor_rows, ["run_id", "mode", "time_seconds", "timestamp", "delivery_ratio_percent", "received_packets", "expected_packets", "service_class"])
write_csv(mode_dir / "dense_service_metrics_timeseries.csv", service_rows, ["run_id", "mode", "time_seconds", "timestamp", "service_class", "latency_avg_ms", "latency_max_ms", "loss_percent", "throughput_bps", "delivery_ratio_percent"])
write_csv(mode_dir / "dense_window_sla_timeseries.csv", window_rows, ["run_id", "mode", "time_seconds", "window_index", "real_time_control_sla_violation", "high_throughput_data_sla_violation", "sensor_telemetry_sla_violation", "weighted_sla_violation_score", "dominant_violation_metric"])
write_csv(mode_dir / "dense_risk_predictions_timeseries.csv", risk_rows, ["run_id", "mode", "time_seconds", "timestamp", "overall_risk_score", "overall_risk_level", "real_time_control_risk", "high_throughput_data_risk", "sensor_telemetry_risk", "recommended_policy_action"])
write_csv(mode_dir / "dense_policy_actions_timeseries.csv", policy_rows, ["run_id", "mode", "time_seconds", "timestamp", "selected_policy_action", "applied", "enforcement_path", "action_reason"])
PY
}

combine_dense_csvs() {
  python3 - "${BASE_DIR}" <<'PY'
import csv
import json
import sys
from pathlib import Path

base = Path(sys.argv[1])
modes = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]
files = {
    "dense_control_latency_timeseries.csv": "dense_all_modes_control_latency.csv",
    "dense_data_throughput_timeseries.csv": "dense_all_modes_data_throughput.csv",
    "dense_sensor_telemetry_timeseries.csv": "dense_all_modes_sensor_telemetry.csv",
    "dense_service_metrics_timeseries.csv": "dense_all_modes_service_metrics.csv",
    "dense_window_sla_timeseries.csv": "dense_all_modes_window_sla.csv",
    "dense_policy_actions_timeseries.csv": "dense_all_modes_policy_actions.csv",
    "dense_risk_predictions_timeseries.csv": "dense_all_modes_risk_predictions.csv",
}

def read_csv(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))

def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in fields} for row in rows])

summary = []
for src_name, dest_name in files.items():
    rows = []
    fields = []
    for mode in modes:
        path = base / mode / src_name
        mode_rows = read_csv(path)
        if mode_rows and not fields:
            fields = list(mode_rows[0].keys())
        rows.extend(mode_rows)
    if fields:
        write_csv(base / dest_name, rows, fields)

for mode in modes:
    mode_dir = base / mode
    status = {}
    try:
        status = json.loads((mode_dir / "mode_status.json").read_text(encoding="utf-8"))
    except Exception:
        pass
    summary.append({
        "run_id": status.get("run_id", ""),
        "mode": mode,
        "status": status.get("status", "missing"),
        "duration_seconds": status.get("duration_seconds", ""),
        "sample_interval_seconds": status.get("sample_interval_seconds", ""),
        "control_latency_samples": len(read_csv(mode_dir / "dense_control_latency_timeseries.csv")),
        "data_throughput_samples": len(read_csv(mode_dir / "dense_data_throughput_timeseries.csv")),
        "sensor_telemetry_samples": len(read_csv(mode_dir / "dense_sensor_telemetry_timeseries.csv")),
        "service_metric_samples": len(read_csv(mode_dir / "dense_service_metrics_timeseries.csv")),
        "window_sla_samples": len(read_csv(mode_dir / "dense_window_sla_timeseries.csv")),
        "policy_action_samples": len(read_csv(mode_dir / "dense_policy_actions_timeseries.csv")),
        "risk_prediction_samples": len(read_csv(mode_dir / "dense_risk_predictions_timeseries.csv")),
        "enforcement_path": "ONOS_QUEUE_APP" if mode not in {"fifo", "static_qos"} else ("FIFO_NO_QUEUE_RULES" if mode == "fifo" else "STATIC_OVS_QUEUE_RULES"),
    })
write_csv(base / "dense_plotting_summary.csv", summary, [
    "run_id", "mode", "status", "duration_seconds", "sample_interval_seconds",
    "control_latency_samples", "data_throughput_samples", "sensor_telemetry_samples",
    "service_metric_samples", "window_sla_samples", "policy_action_samples",
    "risk_prediction_samples", "enforcement_path",
])
PY
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
  MODE_TELEMETRY_METRICS_PORT="$((8300 + mode_index))"
  MODE_POLICY_METRICS_PORT="$((8400 + mode_index))"
  mkdir -p "${MODE_DIR}" "${MODE_DIR}/traffic" "${MODE_DIR}/telemetry" "${MODE_DIR}/policy" "${MODE_DIR}/risk_inference"
  echo "[dense-plotting] mode=${mode}"
  create_runtime_service_mapping
  create_runtime_telemetry_config
  create_runtime_policy_config
  reset_queue_rules
  if [[ "${mode}" == "fifo" ]]; then
    if any_queue_flow_visible; then
      warn "fifo: set_queue flows remain after cleanup"
      status="failed"
    fi
  else
    install_baseline_queue_rules || {
      warn "${mode}: baseline queue rules did not verify"
      status="failed"
    }
  fi

  start_telemetry
  case "${mode}" in
    fifo|static_qos)
      ;;
    n6_only)
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      ;;
    dt_only)
      start_digital_twin
      DT_RISK_INFERENCE_ENABLED=false start_policy_manager
      ;;
    dt_risk_assisted)
      start_digital_twin
      start_risk_inference
      DT_RISK_INFERENCE_ENABLED=true DT_RISK_PREDICTION_PATH=logs/risk_inference/latest_risk_prediction.json start_policy_manager
      ;;
  esac

  run_traffic || traffic_status=$?
  [[ "${traffic_status}" -eq 0 ]] || status="failed"
  stop_background
  if [[ "${mode}" != "fifo" && "${mode}" != "static_qos" ]]; then
    summarize_policy_loop
  fi
  save_ovs_flows "${MODE_DIR}/ovs_flows_after.txt"
  save_queue_counters "${MODE_DIR}/queue_counters_after.txt"
  copy_mode_risk_predictions
  write_mode_status "${mode}" "${status}" "${traffic_status}"
  extract_dense_csvs
}

echo "[dense-plotting] concept=Shadow N6 Digital Twin + Deterministic SLA Risk Inference + ONOS-controlled N6 edge enforcement"
echo "[dense-plotting] results=${BASE_DIR}"
echo "[dense-plotting] duration=${DURATION}"
echo "[dense-plotting] sample_interval_seconds=${SAMPLE_INTERVAL_SECONDS}"
echo "[dense-plotting] congestion_packets_per_burst=${CONGESTION_PACKETS_PER_BURST}"
echo "[dense-plotting] congestion_burst_interval_seconds=${CONGESTION_BURST_INTERVAL_SECONDS}"
echo "[dense-plotting] run_id=${RUN_ID}"
echo "[dense-plotting] live_mode=${LIVE_MODE}"

for mode in "${MODES[@]}"; do
  run_mode "${mode}"
done

combine_dense_csvs
echo "[dense-plotting] complete"
