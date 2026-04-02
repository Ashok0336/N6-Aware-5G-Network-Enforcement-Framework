#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

ONOS_IP=${ONOS_IP:-${ONOS_REST_HOST}}
ONOS_PORT=${ONOS_PORT:-${ONOS_REST_PORT}}
AUTH=${AUTH:-${ONOS_AUTH}}

echo "=== [A] Container status ==="
docker ps --format "table {{.Names}}\t{{.Status}}" | egrep "rfsim5g-mysql|rfsim5g-oai-upf|rfsim5g-oai-ext-dn|onos|ovs" || true
echo

echo "=== [B] OVS QoS / queue settings ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list qos || true
echo "---"
docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list queue || true
echo

echo "=== [C] ONOS flow verification (UDP dst 5201/5202/5203) ==="
DEVICE_ID="$(
  curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/devices" \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); dev=[x.get("id") for x in d.get("devices",[]) if x.get("available") is True]; print(dev[0] if dev else "")'
)"
if [[ -z "$DEVICE_ID" ]]; then
  echo "FAIL: no ONOS device available"
  exit 1
fi
echo "device_id=$DEVICE_ID"

FLOW_JSON="$(curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/flows/${DEVICE_ID}")"
echo "$FLOW_JSON" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for f in d.get("flows",[]):
  crit={c.get("type"):c for c in f.get("selector",{}).get("criteria",[])}
  if "UDP_DST" in crit:
    print(f"priority={f.get(\"priority\")} udp_dst={crit['UDP_DST'].get('udpPort')} packets={f.get('packets')} bytes={f.get('bytes')} state={f.get('state')}")
'
echo

echo "=== [D] Baseline OVS flow counters (UDP dst 5201/5202/5203) ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" | egrep "udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203" || true
BASELINE=$(
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" \
    | awk -F'[=, ]+' '/udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203/ {for(i=1;i<=NF;i++) if($i=="n_packets"){s+=$(i+1)}} END{print s+0}'
)
echo "baseline_udp_rule_packets=${BASELINE}"
echo

echo "=== [E] Generate UDP traffic from UE containers ==="
docker exec rfsim5g-oai-nr-ue bash -lc "for i in \$(seq 1 200); do echo embb-\$i > /dev/udp/${EXT_DN_TARGET_IP}/5201; done; echo ue1-done" || true
docker exec rfsim5g-oai-nr-ue2 bash -lc "for i in \$(seq 1 200); do echo urllc-\$i > /dev/udp/${EXT_DN_TARGET_IP}/5202; done; echo ue2-done" || true
docker exec rfsim5g-oai-nr-ue3 bash -lc "for i in \$(seq 1 200); do echo mmtc-\$i > /dev/udp/${EXT_DN_TARGET_IP}/5203; done; echo ue3-done" || true
echo

echo "=== [F] Post-traffic OVS counters ==="
docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" | egrep "udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203" || true
AFTER=$(
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" \
    | awk -F'[=, ]+' '/udp,tp_dst=5201|udp,tp_dst=5202|udp,tp_dst=5203/ {for(i=1;i<=NF;i++) if($i=="n_packets"){s+=$(i+1)}} END{print s+0}'
)
echo "after_udp_rule_packets=${AFTER}"
echo

echo "=== [G] PASS/FAIL ==="
if echo "$FLOW_JSON" | egrep -q '"udpPort"[[:space:]]*:[[:space:]]*5201' \
 && echo "$FLOW_JSON" | egrep -q '"udpPort"[[:space:]]*:[[:space:]]*5202' \
 && echo "$FLOW_JSON" | egrep -q '"udpPort"[[:space:]]*:[[:space:]]*5203'; then
  if [ "$AFTER" -gt "$BASELINE" ]; then
    echo "PASS: ONOS UDP rules for 5201/5202/5203 exist and counters increased (${BASELINE} -> ${AFTER})."
  else
    echo "FAIL: ONOS UDP rules exist but OVS packet counters did not increase (${BASELINE} -> ${AFTER})."
  fi
else
  echo "FAIL: ONOS does not show all required UDP rules (5201/5202/5203)."
fi
