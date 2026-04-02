#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

echo "[install_onos_flows] Delegating to ./install-slice-flows.sh so the repo uses one authoritative ONOS flow installer."
exec "${HERE}/install-slice-flows.sh" "$@"
