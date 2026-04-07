#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-${TESTBED_DIR}/policy_manager/config.yaml}"

# shellcheck disable=SC1091
source "${TESTBED_DIR}/testbed-env.sh"
mkdir -p "${TESTBED_DIR}/logs/policy"

MODE_LABEL=""
MODE_SOURCE="config"
for arg in "$@"; do
  case "$arg" in
    --dry-run)
      MODE_LABEL="dry-run"
      MODE_SOURCE="cli"
      ;;
    --active)
      MODE_LABEL="active"
      MODE_SOURCE="cli"
      ;;
  esac
done

if [[ -z "$MODE_LABEL" ]]; then
  MODE_LABEL="$(
    env PYTHONPATH="${TESTBED_DIR}:${PYTHONPATH:-}" python3 - "${CONFIG_PATH}" <<'PY'
import sys
from pathlib import Path

from policy_manager.config import load_policy_config

config = load_policy_config(Path(sys.argv[1]).resolve())
print("dry-run" if config["dry_run_only"] else "active")
PY
  )"
fi

echo "[policy-loop] authoritative config: ${CONFIG_PATH}"
echo "[policy-loop] mode: ${MODE_LABEL} (${MODE_SOURCE})"

cd "${TESTBED_DIR}"
exec env PYTHONPATH="${TESTBED_DIR}:${PYTHONPATH:-}" python3 -m policy_manager.app --config "${CONFIG_PATH}" "$@"
