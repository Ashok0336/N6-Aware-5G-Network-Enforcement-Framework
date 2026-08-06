#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs/6g_prototype"
STATUS_PATH="${LOG_DIR}/system_status.json"
INTERVAL_SECONDS=2
MODE_ARG=""
MODE_LABEL="config-default"

usage() {
  cat <<'EOF'
Usage: ./run_6g_prototype.sh [--interval SECONDS] [--dry-run | --live]

Starts the 6G prototype control stack:
  telemetry -> digital twin -> ML predictor -> AI agent -> policy manager -> ONOS -> OVS

The script writes process logs and health state under:
  logs/6g_prototype/

Options:
  --interval SECONDS  Health/policy bridge interval. Default: 2.
  --dry-run           Plan policy enforcement without changing ONOS/OVS.
  --live              Apply policy enforcement through the existing manager.
  -h, --help          Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)
      INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --dry-run)
      MODE_ARG="--dry-run"
      MODE_LABEL="dry-run"
      shift
      ;;
    --live)
      MODE_ARG="--live"
      MODE_LABEL="live"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[6g-prototype] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -f "${SCRIPT_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.venv/bin/activate"
elif [[ -f "${SCRIPT_DIR}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/venv/bin/activate"
fi

mkdir -p \
  "${LOG_DIR}" \
  "${SCRIPT_DIR}/logs/telemetry" \
  "${SCRIPT_DIR}/logs/digital_twin" \
  "${SCRIPT_DIR}/logs/ml_predictor" \
  "${SCRIPT_DIR}/logs/ai_agent" \
  "${SCRIPT_DIR}/logs/policy" \
  "${SCRIPT_DIR}/logs/enforcement"

telemetry_pid=""
twin_pid=""
ml_pid=""
agent_pid=""
policy_pid=""
health_pid=""

write_pid_file() {
  local name="$1"
  local pid="$2"
  printf '%s\n' "$pid" >"${LOG_DIR}/${name}.pid"
}

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

latest_agent_action() {
  python3 - "${SCRIPT_DIR}/logs/ai_agent/agent_decisions.jsonl" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
latest = {}
if path.exists():
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            latest = payload
print(latest.get("action", ""))
PY
}

policy_manager_loop() {
  echo "[6g-prototype][policy-manager] starting bridge/enforcement loop mode=${MODE_LABEL}"
  while true; do
    if python3 "${SCRIPT_DIR}/ai_agent/policy_bridge.py"; then
      action="$(latest_agent_action)"
      case "$action" in
        ""|"MAINTAIN_POLICY"|"MAINTAIN_CURRENT_POLICY"|"HOLD_ACTION")
          echo "[6g-prototype][policy-manager] ${action:-no-agent-action} -> no enforcement action"
          ;;
        "RESTORE_DEFAULT_POLICY")
          echo "[6g-prototype][policy-manager] RESTORE_DEFAULT_POLICY -> rollback_default_policy.sh"
          "${SCRIPT_DIR}/automation/rollback_default_policy.sh" ${MODE_ARG:+"$MODE_ARG"} || true
          ;;
        *)
          echo "[6g-prototype][policy-manager] ${action} -> enforcement_manager.py"
          python3 "${SCRIPT_DIR}/automation/enforcement_manager.py" \
            --config "${SCRIPT_DIR}/automation/enforcement_config.yaml" \
            ${MODE_ARG:+"$MODE_ARG"} \
            --once || true
          ;;
      esac
    else
      echo "[6g-prototype][policy-manager] bridge failed; retrying"
    fi
    sleep "$INTERVAL_SECONDS"
  done
}

health_monitor_loop() {
  while true; do
    python3 - \
      "$STATUS_PATH" \
      "$telemetry_pid" \
      "$twin_pid" \
      "$ml_pid" \
      "$agent_pid" \
      "$policy_pid" \
      "$MODE_LABEL" \
      "$SCRIPT_DIR" <<'PY'
import json
import os
import pathlib
import subprocess
import sys
from datetime import datetime, timezone

status_path = pathlib.Path(sys.argv[1])
telemetry_pid, twin_pid, ml_pid, agent_pid, policy_pid = sys.argv[2:7]
mode_label = sys.argv[7]
root = pathlib.Path(sys.argv[8])

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def pid_running(pid_text):
    try:
        pid = int(pid_text)
        os.kill(pid, 0)
        return True
    except Exception:
        return False

def latest_jsonl(path):
    path = pathlib.Path(path)
    latest = None
    count = 0
    if not path.exists():
        return None, 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                latest = payload
                count += 1
    return latest, count

def run_cmd(args, timeout=4):
    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        return {
            "ok": completed.returncode == 0,
            "return_code": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "stdout": "", "stderr": ""}

twin, twin_count = latest_jsonl(root / "logs/digital_twin/twin_state.jsonl")
prediction, prediction_count = latest_jsonl(root / "logs/ml_predictor/predictions.jsonl")
agent, agent_count = latest_jsonl(root / "logs/ai_agent/agent_decisions.jsonl")

onos_status = {"ok": False}
try:
    import urllib.request
    import base64
    request = urllib.request.Request("http://192.168.71.160:8181/onos/v1/devices")
    token = base64.b64encode(b"onos:rocks").decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=4) as response:
        payload = json.loads(response.read().decode("utf-8"))
    devices = payload.get("devices", []) if isinstance(payload, dict) else []
    available = [item for item in devices if isinstance(item, dict) and item.get("available") is True]
    onos_status = {
        "ok": True,
        "device_count": len(devices),
        "available_device_count": len(available),
    }
except Exception as exc:
    onos_status = {"ok": False, "error": str(exc)}

ovs_show = run_cmd(["docker", "exec", "ovs", "ovs-vsctl", "show"])
ovs_controller = run_cmd(["docker", "exec", "ovs", "ovs-vsctl", "list", "controller"])
ovs_status = {
    "ok": bool(ovs_show.get("ok")),
    "bridge_present": "Bridge br-n6" in ovs_show.get("stdout", ""),
    "controller_connected": "is_connected        : true" in ovs_controller.get("stdout", ""),
    "show_error": ovs_show.get("stderr") or ovs_show.get("error"),
}

status = {
    "timestamp": utc_now(),
    "mode": mode_label,
    "processes": {
        "telemetry": {"pid": telemetry_pid, "running": pid_running(telemetry_pid)},
        "digital_twin": {"pid": twin_pid, "running": pid_running(twin_pid)},
        "ml_predictor": {"pid": ml_pid, "running": pid_running(ml_pid)},
        "ai_agent": {"pid": agent_pid, "running": pid_running(agent_pid)},
        "policy_manager": {"pid": policy_pid, "running": pid_running(policy_pid)},
    },
    "twin_status": {
        "ok": twin is not None,
        "records": twin_count,
        "last_updated": (twin or {}).get("last_updated"),
        "ovs_controller_connected_in_twin": ((twin or {}).get("ovs_status") or {}).get("controller_connected"),
        "onos_ok_in_twin": ((twin or {}).get("onos_status") or {}).get("ok"),
    },
    "prediction_status": {
        "ok": prediction is not None,
        "records": prediction_count,
        "timestamp": (prediction or {}).get("timestamp"),
        "urllc_sla_violation_risk": (prediction or {}).get("urllc_sla_violation_risk"),
        "embb_congestion_risk": (prediction or {}).get("embb_congestion_risk"),
    },
    "ai_agent_status": {
        "ok": agent is not None,
        "records": agent_count,
        "timestamp": (agent or {}).get("timestamp"),
        "action": (agent or {}).get("action"),
        "enforcement_performed": (agent or {}).get("enforcement_performed"),
    },
    "onos_status": onos_status,
    "ovs_status": ovs_status,
}

status_path.parent.mkdir(parents=True, exist_ok=True)
tmp = status_path.with_suffix(".json.tmp")
tmp.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(status_path)
PY
    sleep "$INTERVAL_SECONDS"
  done
}

cleanup() {
  echo "[6g-prototype] stopping services..."
  for pid in "$health_pid" "$policy_pid" "$agent_pid" "$ml_pid" "$twin_pid" "$telemetry_pid"; do
    if is_running "$pid"; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  wait >/dev/null 2>&1 || true
  python3 - "$STATUS_PATH" <<'PY' || true
import json
import pathlib
import sys
from datetime import datetime, timezone

path = pathlib.Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
try:
    status = json.loads(path.read_text(encoding="utf-8"))
except json.JSONDecodeError:
    raise SystemExit(0)
for process in status.get("processes", {}).values():
    if isinstance(process, dict):
        process["running"] = False
status["stopped_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}
trap cleanup EXIT INT TERM

cd "${SCRIPT_DIR}"

echo "[6g-prototype] starting telemetry..."
"${SCRIPT_DIR}/automation/run_telemetry.sh" >"${LOG_DIR}/telemetry.log" 2>&1 &
telemetry_pid="$!"
write_pid_file telemetry "$telemetry_pid"

sleep 2

echo "[6g-prototype] starting digital twin..."
"${SCRIPT_DIR}/automation/run_digital_twin.sh" >"${LOG_DIR}/digital_twin.log" 2>&1 &
twin_pid="$!"
write_pid_file digital_twin "$twin_pid"

sleep 2

echo "[6g-prototype] starting ML predictor..."
"${SCRIPT_DIR}/automation/run_ml_predictor.sh" >"${LOG_DIR}/ml_predictor.log" 2>&1 &
ml_pid="$!"
write_pid_file ml_predictor "$ml_pid"

sleep 2

echo "[6g-prototype] starting AI agent..."
"${SCRIPT_DIR}/automation/run_ai_agent.sh" >"${LOG_DIR}/ai_agent.log" 2>&1 &
agent_pid="$!"
write_pid_file ai_agent "$agent_pid"

sleep 2

echo "[6g-prototype] starting policy manager bridge..."
policy_manager_loop >"${LOG_DIR}/policy_manager.log" 2>&1 &
policy_pid="$!"
write_pid_file policy_manager "$policy_pid"

echo "[6g-prototype] starting health monitor..."
health_monitor_loop >"${LOG_DIR}/health_monitor.log" 2>&1 &
health_pid="$!"
write_pid_file health_monitor "$health_pid"

echo "[6g-prototype] running."
echo "[6g-prototype] status: ${STATUS_PATH}"
echo "[6g-prototype] logs: ${LOG_DIR}"
echo "[6g-prototype] press Ctrl+C to stop."

wait "$telemetry_pid" "$twin_pid" "$ml_pid" "$agent_pid" "$policy_pid" "$health_pid"
