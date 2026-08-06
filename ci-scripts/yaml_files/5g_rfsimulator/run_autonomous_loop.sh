#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTERVAL_SECONDS=2
RUN_ONCE=false
MODE_ARG=""
MODE_LABEL="config-default"

usage() {
  cat <<'EOF'
Usage: ./run_autonomous_loop.sh [--once] [--interval SECONDS] [--dry-run | --live]

Runs one or more autonomous control-loop cycles:
  Digital Twin -> ML Predictor -> AI Agent -> Policy Bridge -> Policy Manager -> ONOS -> OVS

The AI Agent writes decisions first. Enforcement is delegated to the existing
enforcement_manager.py, ONOS client, OVS logic, and rollback_default_policy.sh.

Options:
  --once              Run one cycle and exit.
  --interval SECONDS  Sleep between cycles. Default: 2.
  --dry-run           Plan enforcement without changing ONOS/OVS.
  --live              Apply enforcement to ONOS/OVS.
  -h, --help          Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once)
      RUN_ONCE=true
      shift
      ;;
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
      echo "[autonomous-loop] ERROR: Unknown argument: $1" >&2
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
  "${SCRIPT_DIR}/logs/digital_twin" \
  "${SCRIPT_DIR}/logs/ml_predictor" \
  "${SCRIPT_DIR}/logs/ai_agent" \
  "${SCRIPT_DIR}/logs/policy" \
  "${SCRIPT_DIR}/logs/enforcement"

latest_agent_action() {
  python3 - "${SCRIPT_DIR}/logs/ai_agent/agent_decisions.jsonl" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
latest = None
if path.exists():
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            latest = payload
print((latest or {}).get("action", ""))
PY
}

run_cycle() {
  echo "[autonomous-loop] cycle started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

  python3 "${SCRIPT_DIR}/digital_twin/twin_sync.py" --once
  python3 "${SCRIPT_DIR}/ml_predictor/dataset_builder.py"
  python3 "${SCRIPT_DIR}/ml_predictor/train_model.py"
  python3 "${SCRIPT_DIR}/ml_predictor/predict_state.py" --once
  python3 "${SCRIPT_DIR}/ai_agent/decision_agent.py" --once

  local action
  action="$(latest_agent_action)"
  echo "[autonomous-loop] latest agent action: ${action:-unknown}"

  if [[ "$action" == "MAINTAIN_POLICY" || "$action" == "MAINTAIN_CURRENT_POLICY" || "$action" == "HOLD_ACTION" || -z "$action" ]]; then
    echo "[autonomous-loop] ${action:-no-agent-action} -> no policy enforcement action."
    return 0
  fi

  if [[ "$action" == "RESTORE_DEFAULT_POLICY" ]]; then
    echo "[autonomous-loop] RESTORE_DEFAULT_POLICY -> rollback_default_policy.sh"
    "${SCRIPT_DIR}/automation/rollback_default_policy.sh" ${MODE_ARG:+"$MODE_ARG"}
    return 0
  fi

  python3 "${SCRIPT_DIR}/ai_agent/policy_bridge.py"
  echo "[autonomous-loop] agent decision bridged into logs/policy/policy_decisions_agent.jsonl"
  echo "[autonomous-loop] invoking existing enforcement_manager.py (${MODE_LABEL})"
  python3 "${SCRIPT_DIR}/automation/enforcement_manager.py" \
    --config "${SCRIPT_DIR}/automation/enforcement_config.yaml" \
    ${MODE_ARG:+"$MODE_ARG"} \
    --once
}

cd "${SCRIPT_DIR}"

if [[ "$RUN_ONCE" == "true" ]]; then
  run_cycle
  exit 0
fi

echo "[autonomous-loop] starting loop interval_seconds=${INTERVAL_SECONDS} mode=${MODE_LABEL}"
while true; do
  run_cycle
  sleep "$INTERVAL_SECONDS"
done
