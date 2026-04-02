#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

ONOS_IP=${ONOS_IP:-${ONOS_REST_HOST}}
ONOS_PORT=${ONOS_PORT:-${ONOS_REST_PORT}}
AUTH=${AUTH:-${ONOS_AUTH}}
BR=${BR:-${OVS_BRIDGE_NAME}}

echo "=== [1] Container status ==="
docker ps --format "table {{.Names}}\t{{.Status}}" | egrep "rfsim5g-mysql|rfsim5g-oai-amf|rfsim5g-oai-smf|rfsim5g-oai-upf|rfsim5g-oai-ext-dn|onos|ovs" || true
echo

echo "=== [2] OVS controller connectivity ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl show
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list controller
if docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list controller | grep -q "is_connected[[:space:]]*:[[:space:]]*true"; then
  echo "PASS: OVS controller connected to ONOS."
else
  echo "FAIL: OVS controller not connected to ONOS."
  echo "Configured controller endpoint(s):"
  docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get-controller "$BR" || true
  echo "Hint: keep ovs ONOS_CTRL aligned with the ONOS container OpenFlow listener, currently ${ONOS_CTRL}."
fi
echo

echo "=== [3] ONOS device availability ==="
DEVICE_JSON="$(curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/devices")"
echo "$DEVICE_JSON" | python3 -c '
import sys,json
d=json.load(sys.stdin)
dev=[x for x in d.get("devices",[]) if x.get("available") is True]
print(f"available_devices={len(dev)}")
print(dev[0]["id"] if dev else "")
'
echo

echo "=== [4] OVS QoS / Queue ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list qos
echo "---"
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list queue
echo

echo "=== [5] Required set_queue slice rules ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" | egrep "udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203|set_queue"
BASELINE=$(
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" \
    | awk -F'[=, ]+' '/udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203/ {for(i=1;i<=NF;i++) if($i=="n_packets"){s+=$(i+1)}} END{print s+0}'
)
echo "baseline_packets=${BASELINE}"
echo

echo "=== [6] Run paper traffic generator (iperf3 + ping) ==="
"${HERE}/start-paper-traffic.sh"
echo

echo "=== [7] Post-traffic counters ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" | egrep "udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203|set_queue"
AFTER=$(
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" \
    | awk -F'[=, ]+' '/udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203/ {for(i=1;i<=NF;i++) if($i=="n_packets"){s+=$(i+1)}} END{print s+0}'
)
echo "after_packets=${AFTER}"
echo

echo "=== [8] PASS/FAIL ==="
if docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" | egrep -q "udp,tp_dst=5201.*set_queue:1" \
  && docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" | egrep -q "udp,tp_dst=5202.*set_queue:2" \
  && docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "$BR" | egrep -q "udp,tp_dst=5203.*set_queue:3"; then
  if [ "$AFTER" -gt "$BASELINE" ]; then
    echo "PASS: set_queue slice rules exist and counters increased (${BASELINE} -> ${AFTER})."
  else
    echo "FAIL: set_queue rules exist but counters did not increase (${BASELINE} -> ${AFTER})."
    exit 1
  fi
else
  echo "FAIL: required set_queue rules are missing."
  echo "Run ./deploy-onos-slice-app.sh"
  exit 1
fi
