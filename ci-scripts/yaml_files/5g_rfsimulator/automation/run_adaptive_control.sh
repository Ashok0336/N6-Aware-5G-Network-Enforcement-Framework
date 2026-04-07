#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY_CONFIG_PATH="${POLICY_CONFIG_PATH:-${SCRIPT_DIR}/policy_config.yaml}"
ENFORCEMENT_CONFIG_PATH="${ENFORCEMENT_CONFIG_PATH:-${SCRIPT_DIR}/enforcement_config.yaml}"
POLICY_MANAGER_PATH="${SCRIPT_DIR}/policy_manager.py"
ENFORCEMENT_MANAGER_PATH="${SCRIPT_DIR}/enforcement_manager.py"
START_POLICY_MANAGER=true
START_ENFORCEMENT_MANAGER=true
MODE_ARG=""
MODE_LABEL=""
POLICY_MODE_ARG=""

usage() {
  cat <<'EOF'
Usage: ./run_adaptive_control.sh [--dry-run | --live] [--skip-policy-manager] [--skip-enforcement-manager]

Options:
  --dry-run              Force dry-run mode for enforcement_manager.py
  --live                 Force live mode for enforcement_manager.py
  --policy-config PATH   Override policy manager config path
  --enforcement-config PATH
                         Override enforcement manager config path
  --skip-policy-manager  Do not launch policy_manager.py
  --skip-enforcement-manager
                         Do not launch enforcement_manager.py
  -h, --help             Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
    --policy-config)
      POLICY_CONFIG_PATH="$2"
      shift 2
      ;;
    --enforcement-config)
      ENFORCEMENT_CONFIG_PATH="$2"
      shift 2
      ;;
    --skip-policy-manager)
      START_POLICY_MANAGER=false
      shift
      ;;
    --skip-enforcement-manager)
      START_ENFORCEMENT_MANAGER=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[adaptive-control] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

readarray -t CONFIG_VALUES < <(python3 - "$ENFORCEMENT_CONFIG_PATH" <<'PY'
import json
import pathlib
import sys

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

config_path = pathlib.Path(sys.argv[1]).resolve()
raw_text = config_path.read_text(encoding="utf-8")
if yaml is not None:
    config = yaml.safe_load(raw_text)
else:
    config = json.loads(raw_text)

def parse_bool(value, field_name):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1, 0.0, 1.0):
        return bool(int(value))
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be boolean, got {value!r}")

enforcement = config.get("enforcement", {})
policy_log_dir = (config_path.parent / enforcement.get("policy_log_dir", "../logs/policy")).resolve()
enforcement_log_dir = (config_path.parent / enforcement.get("log_dir", "../logs/enforcement")).resolve()
default_dry_run = parse_bool(enforcement.get("dry_run", True), "enforcement.dry_run")
print(policy_log_dir)
print(enforcement_log_dir)
print("true" if default_dry_run else "false")
PY
)

POLICY_LOG_DIR="${CONFIG_VALUES[0]}"
ENFORCEMENT_LOG_DIR="${CONFIG_VALUES[1]}"
CONFIG_DRY_RUN="${CONFIG_VALUES[2]}"

mkdir -p "$POLICY_LOG_DIR" "$ENFORCEMENT_LOG_DIR"

if [[ -z "$MODE_ARG" ]]; then
  if [[ "$CONFIG_DRY_RUN" == "true" ]]; then
    MODE_ARG="--dry-run"
    MODE_LABEL="dry-run"
  else
    MODE_ARG="--live"
    MODE_LABEL="live"
  fi
fi

if [[ "$MODE_ARG" == "--dry-run" ]]; then
  POLICY_MODE_ARG="--dry-run"
else
  POLICY_MODE_ARG="--active"
fi

echo "[adaptive-control] Policy config: $POLICY_CONFIG_PATH"
echo "[adaptive-control] Enforcement config: $ENFORCEMENT_CONFIG_PATH"
echo "[adaptive-control] Policy logs: $POLICY_LOG_DIR"
echo "[adaptive-control] Enforcement logs: $ENFORCEMENT_LOG_DIR"
echo "[adaptive-control] Enforcement mode: $MODE_LABEL"
echo "[adaptive-control] Telemetry collector should already be running or telemetry logs should already exist."

PIDS=()

cleanup() {
  local code=$?
  if [[ ${#PIDS[@]} -gt 0 ]]; then
    echo "[adaptive-control] Stopping child processes..."
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  exit "$code"
}

trap cleanup INT TERM

if [[ "$START_POLICY_MANAGER" == "true" ]]; then
  echo "[adaptive-control] Starting policy manager..."
  python3 "$POLICY_MANAGER_PATH" --config "$POLICY_CONFIG_PATH" "$POLICY_MODE_ARG" &
  POLICY_PID=$!
  PIDS+=("$POLICY_PID")
  echo "[adaptive-control] policy_manager.py pid=$POLICY_PID"
  sleep 2
else
  echo "[adaptive-control] Skipping policy manager launch."
fi

if [[ "$START_ENFORCEMENT_MANAGER" == "true" ]]; then
  echo "[adaptive-control] Starting enforcement manager..."
  python3 "$ENFORCEMENT_MANAGER_PATH" --config "$ENFORCEMENT_CONFIG_PATH" "$MODE_ARG" &
  ENFORCEMENT_PID=$!
  PIDS+=("$ENFORCEMENT_PID")
  echo "[adaptive-control] enforcement_manager.py pid=$ENFORCEMENT_PID"
else
  echo "[adaptive-control] Skipping enforcement manager launch."
fi

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "[adaptive-control] Nothing was launched."
  exit 0
fi

wait -n "${PIDS[@]}"
cleanup
