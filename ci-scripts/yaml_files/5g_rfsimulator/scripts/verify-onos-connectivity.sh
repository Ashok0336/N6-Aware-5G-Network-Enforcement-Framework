#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

echo "== ONOS REST devices =="
curl -fsS -u "${ONOS_AUTH}" "${ONOS_DEVICES_URL}" | python3 -m json.tool
echo
echo "== Expected OpenFlow controller =="
echo "tcp:${ONOS_CTRL}"
echo
echo "== OVS controller target =="
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get-controller "${OVS_BRIDGE_NAME}"
echo
echo "== OVS controller connectivity =="
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list controller
