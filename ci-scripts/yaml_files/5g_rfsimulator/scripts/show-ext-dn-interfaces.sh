#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc '
set -euo pipefail
echo "== ip -br link =="
ip -br link
echo
echo "== ip -br addr =="
ip -br addr
echo
echo "== ip route =="
ip route
'
