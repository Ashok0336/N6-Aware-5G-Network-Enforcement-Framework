#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./experiment_common.sh
source "${SCRIPT_DIR}/experiment_common.sh"

SERVICE_MAPPING_PATH="${SERVICE_MAPPING_PATH:-${SCRIPT_DIR}/service_mapping.yaml}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/../logs/experiments}"
RESULTS_DIR="${RESULTS_DIR:-}"
MODE_ARG=""
MODE_LABEL=""

usage() {
  cat <<'EOF'
Usage: ./run_static_experiment.sh [--mapping PATH] [--duration SECONDS] [--results-dir DIR] [--dry-run | --live]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mapping)
      SERVICE_MAPPING_PATH="$2"
      shift 2
      ;;
    --duration)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --results-dir)
      RESULTS_DIR="$2"
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
      echo "[static-experiment] ERROR: Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$RESULTS_DIR" ]]; then
  RESULTS_DIR="$(prepare_results_dir "static" "$RESULTS_ROOT")"
fi
mkdir -p "$RESULTS_DIR"

declare -A PATHS=()
while IFS='=' read -r key value; do
  PATHS["$key"]="$value"
done < <(generate_runtime_configs "$RESULTS_DIR" "static" "$SERVICE_MAPPING_PATH")

if [[ -z "$MODE_ARG" ]]; then
  CONFIG_DRY_RUN="$(
    python3 - "${PATHS[ENFORCEMENT_CONFIG]}" <<'PY'
import json
import pathlib
import sys

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

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

config_path = pathlib.Path(sys.argv[1]).resolve()
raw_text = config_path.read_text(encoding="utf-8")
if yaml is not None:
    config = yaml.safe_load(raw_text)
else:
    config = json.loads(raw_text)

dry_run = parse_bool(config.get("enforcement", {}).get("dry_run", True), "enforcement.dry_run")
print("true" if dry_run else "false")
PY
  )"
  if [[ "$CONFIG_DRY_RUN" == "true" ]]; then
    MODE_ARG="--dry-run"
    MODE_LABEL="dry-run"
  else
    MODE_ARG="--live"
    MODE_LABEL="live"
  fi
fi

PIDS=()
cleanup() {
  local code=$?
  stop_background_processes "${PIDS[@]}"
  exit "$code"
}
trap cleanup INT TERM EXIT

echo "[static-experiment] Results directory: ${PATHS[RESULTS_DIR]}"
echo "[static-experiment] Applying the baseline queue profile in ${MODE_LABEL} mode."

python3 "${SCRIPT_DIR}/enforcement_manager.py" \
  --config "${PATHS[ENFORCEMENT_CONFIG]}" \
  "$MODE_ARG" \
  --restore-default \
  --once \
  >"${RESULTS_DIR}/static_enforcement_stdout.log" 2>&1

python3 "${SCRIPT_DIR}/telemetry_collector.py" \
  --config "${PATHS[TELEMETRY_CONFIG]}" \
  >"${RESULTS_DIR}/telemetry_stdout.log" 2>&1 &
PIDS+=("$!")
sleep 2

"${SCRIPT_DIR}/run_all_traffic.sh" \
  --mapping "$SERVICE_MAPPING_PATH" \
  --output-root "${PATHS[TRAFFIC_DIR]}" \
  --duration "$DURATION_SECONDS"

sleep 4
stop_background_processes "${PIDS[@]}"
trap - INT TERM EXIT

echo "[static-experiment] Completed."
echo "[static-experiment] Manifest: ${PATHS[MANIFEST_PATH]}"
echo "[static-experiment] Results stored under: ${PATHS[RESULTS_DIR]}"
