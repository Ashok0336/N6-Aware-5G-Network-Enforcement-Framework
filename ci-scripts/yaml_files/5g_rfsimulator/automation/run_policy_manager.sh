#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[run_policy_manager] Delegating to ./run_policy_loop.sh so the closed-loop layer has one authoritative launcher."
exec "${SCRIPT_DIR}/run_policy_loop.sh" "$@"
