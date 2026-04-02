#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENFORCEMENT_CONFIG_PATH="${ENFORCEMENT_CONFIG_PATH:-${SCRIPT_DIR}/enforcement_config.yaml}"
ENFORCEMENT_MANAGER_PATH="${SCRIPT_DIR}/enforcement_manager.py"
MODE_ARG="--live"
MODE_LABEL="live"

usage() {
  cat <<'EOF'
Usage: ./rollback_default_policy.sh [--dry-run | --live] [--config PATH]

Options:
  --dry-run      Show the rollback operations without changing OVS or ONOS
  --live         Restore the baseline queue profile immediately
  --config PATH  Override enforcement config path
  -h, --help     Show this help message
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
    --config)
      ENFORCEMENT_CONFIG_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[rollback] ERROR: Unknown argument: $1" >&2
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

enforcement = config.get("enforcement", {})
log_dir = (config_path.parent / enforcement.get("log_dir", "../logs/enforcement")).resolve()
baseline = enforcement.get("queue_profiles", {}).get("baseline", {})
print(log_dir)
print(json.dumps(baseline, sort_keys=True))
PY
)

ENFORCEMENT_LOG_DIR="${CONFIG_VALUES[0]}"
BASELINE_PROFILE="${CONFIG_VALUES[1]}"

mkdir -p "$ENFORCEMENT_LOG_DIR"

echo "[rollback] Config: $ENFORCEMENT_CONFIG_PATH"
echo "[rollback] Mode: $MODE_LABEL"
echo "[rollback] Enforcement logs: $ENFORCEMENT_LOG_DIR"
echo "[rollback] Baseline queue profile: $BASELINE_PROFILE"
echo "[rollback] Restoring the default/baseline policy..."

python3 "$ENFORCEMENT_MANAGER_PATH" \
  --config "$ENFORCEMENT_CONFIG_PATH" \
  "$MODE_ARG" \
  --restore-default \
  --once

echo "[rollback] Restored the baseline queue profile in $MODE_LABEL mode."
