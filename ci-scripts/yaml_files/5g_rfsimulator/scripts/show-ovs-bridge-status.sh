#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

docker exec "${OVS_CONTAINER_NAME}" bash -lc "
set -euo pipefail
echo '== ovs-vsctl show =='
ovs-vsctl show
echo
echo '== controller =='
ovs-vsctl get-controller '${OVS_BRIDGE_NAME}'
echo 'expected=tcp:${ONOS_CTRL}'
echo
echo '== controller details =='
ovs-vsctl list controller
echo
echo '== bridge ports =='
ovs-vsctl list-ports '${OVS_BRIDGE_NAME}'
echo
echo '== OpenFlow port map =='
ovs-ofctl -O OpenFlow13 show '${OVS_BRIDGE_NAME}'
"
