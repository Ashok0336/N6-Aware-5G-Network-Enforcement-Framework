#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

ONOS_IP=${ONOS_IP:-${ONOS_REST_HOST}}
ONOS_PORT=${ONOS_PORT:-${ONOS_REST_PORT}}
AUTH=${AUTH:-${ONOS_AUTH}}
OVS_CONTAINER_NAME=${OVS_CONTAINER_NAME:-ovs}
OVS_BRIDGE_NAME=${OVS_BRIDGE_NAME:-br-n6}
OVS_UPF_PORT_NAME=${OVS_UPF_PORT_NAME:-v-upf-host}
OVS_EDN_PORT_NAME=${OVS_EDN_PORT_NAME:-v-edn-host}
N6_ARP_FORWARD_PRIORITY=${N6_ARP_FORWARD_PRIORITY:-45000}
N6_BASE_FORWARD_PRIORITY=${N6_BASE_FORWARD_PRIORITY:-5000}

echo "[slice-flows] Waiting for ONOS REST at http://${ONOS_IP}:${ONOS_PORT} ..."
DEVICE_JSON=""
for i in {1..90}; do
  if DEVICE_JSON="$(curl -sS -u "$AUTH" -w '\n%{http_code}' "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/devices")"; then
    HTTP_CODE="$(echo "$DEVICE_JSON" | tail -n1)"
    BODY="$(echo "$DEVICE_JSON" | sed '$d')"
    if [[ "$HTTP_CODE" == "200" ]]; then
      AVAILABLE_COUNT="$(
        echo "$BODY" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(sum(1 for x in d.get("devices",[]) if x.get("available") is True))'
      )"
      if [[ "${AVAILABLE_COUNT}" -ge 1 ]]; then
        DEVICE_JSON="$BODY"
        break
      fi
      echo "[slice-flows] waiting for device discovery (available_devices=0)"
    fi
  fi
  sleep 2
done
if [[ -z "${DEVICE_JSON}" || "${AVAILABLE_COUNT:-0}" -lt 1 ]]; then
  echo "ERROR: ONOS did not report available devices within 180s."
  exit 1
fi

DEVICE_ID="$(
  echo "$DEVICE_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); dev=[x.get("id") for x in d.get("devices",[]) if x.get("available") is True]; print(dev[0] if dev else "")'
)"
if [[ -z "$DEVICE_ID" ]]; then
  echo "ERROR: No available ONOS device."
  exit 1
fi
echo "[slice-flows] DEVICE_ID=${DEVICE_ID}"

PORTS_JSON="$(curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/devices/${DEVICE_ID}/ports")"
read -r UPF_PORT EDN_PORT < <(
  OVS_UPF_PORT_NAME="${OVS_UPF_PORT_NAME}" OVS_EDN_PORT_NAME="${OVS_EDN_PORT_NAME}" \
    python3 -c '
import json, os, sys
d=json.load(sys.stdin)
ports=d.get("ports",[])
def find(name):
  for p in ports:
    ann=(p.get("annotations") or {})
    if (ann.get("portName") or ann.get("name") or ann.get("ifName") or "") == name:
      return str(p.get("port"))
  return ""
print(find(os.environ["OVS_UPF_PORT_NAME"]), find(os.environ["OVS_EDN_PORT_NAME"]))
' <<< "$PORTS_JSON"
)
if [[ -z "$UPF_PORT" || -z "$EDN_PORT" ]]; then
  echo "ERROR: Could not resolve OVS ports from ONOS annotations."
  exit 1
fi
echo "[slice-flows] UPF_PORT=${UPF_PORT} EDN_PORT=${EDN_PORT}"

wait_for_set_queue_flows() {
  for _ in {1..30}; do
    local flows
    flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
    if echo "$flows" | grep -q "udp,tp_dst=5201.*set_queue:1" \
      && echo "$flows" | grep -q "udp,tp_dst=5202.*set_queue:2" \
      && echo "$flows" | grep -q "udp,tp_dst=5203.*set_queue:3"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

post_flow() {
  local payload="$1"
  local resp
  local code
  resp="$(mktemp)"
  code="$(curl -sS -u "$AUTH" -H "Content-Type: application/json" -o "$resp" -w "%{http_code}" \
    -X POST "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/flows/${DEVICE_ID}" -d "$payload")"
  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    echo "[slice-flows] ERROR posting flow (HTTP $code):"
    local msg
    msg="$(cat "$resp")"
    echo "$msg"
    rm -f "$resp"
    echo
    if [[ "$code" == "400" ]] && echo "$msg" | grep -qi "SET_QUEUE is not supported"; then
      echo "[slice-flows] REST SET_QUEUE unsupported. Triggering ONOS app fallback..."
      "${HERE}/deploy-onos-slice-app.sh"
      echo "[slice-flows] Waiting up to 60s for ONOS app rules to appear in OVS..."
      if wait_for_set_queue_flows; then
        echo "[slice-flows] ONOS app fallback deployed and slice queue flows are present."
        exit 0
      fi
      echo "[slice-flows] ERROR: ONOS app deployed but required set_queue flows did not appear within 60s."
      exit 1
    fi
    exit 1
  fi
  rm -f "$resp"
}

mk_udp_queue_flow() {
  local prio="$1"
  local dport="$2"
  local queue_id="$3"
  cat <<JSON
{
  "priority": ${prio},
  "timeout": 0,
  "isPermanent": true,
  "deviceId": "${DEVICE_ID}",
  "treatment": {
    "instructions": [
      { "type": "SET_QUEUE", "queueId": ${queue_id} },
      { "type": "OUTPUT", "port": "${EDN_PORT}" }
    ]
  },
  "selector": {
    "criteria": [
      { "type": "IN_PORT", "port": "${UPF_PORT}" },
      { "type": "ETH_TYPE", "ethType": "0x0800" },
      { "type": "IP_PROTO", "protocol": 17 },
      { "type": "UDP_DST", "udpPort": ${dport} }
    ]
  }
}
JSON
}

mk_base_forward_flow() {
  cat <<JSON
{
  "priority": ${N6_BASE_FORWARD_PRIORITY},
  "timeout": 0,
  "isPermanent": true,
  "deviceId": "${DEVICE_ID}",
  "treatment": {
    "instructions": [
      { "type": "OUTPUT", "port": "${EDN_PORT}" }
    ]
  },
  "selector": {
    "criteria": [
      { "type": "IN_PORT", "port": "${UPF_PORT}" },
      { "type": "ETH_TYPE", "ethType": "0x0800" }
    ]
  }
}
JSON
}

mk_reverse_flow() {
  cat <<JSON
{
  "priority": 20000,
  "timeout": 0,
  "isPermanent": true,
  "deviceId": "${DEVICE_ID}",
  "treatment": {
    "instructions": [
      { "type": "OUTPUT", "port": "${UPF_PORT}" }
    ]
  },
  "selector": {
    "criteria": [
      { "type": "IN_PORT", "port": "${EDN_PORT}" },
      { "type": "ETH_TYPE", "ethType": "0x0800" }
    ]
  }
}
JSON
}

mk_arp_flow() {
  local in="$1"
  local out="$2"
  cat <<JSON
{
  "priority": ${N6_ARP_FORWARD_PRIORITY},
  "timeout": 0,
  "isPermanent": true,
  "deviceId": "${DEVICE_ID}",
  "treatment": {
    "instructions": [
      { "type": "OUTPUT", "port": "${out}" }
    ]
  },
  "selector": {
    "criteria": [
      { "type": "IN_PORT", "port": "${in}" },
      { "type": "ETH_TYPE", "ethType": "0x0806" }
    ]
  }
}
JSON
}

echo "[slice-flows] Installing ONOS slice queue rules..."
post_flow "$(mk_base_forward_flow)"
post_flow "$(mk_udp_queue_flow 40000 5201 1)"
post_flow "$(mk_udp_queue_flow 50000 5202 2)"
post_flow "$(mk_udp_queue_flow 30000 5203 3)"
post_flow "$(mk_reverse_flow)"
post_flow "$(mk_arp_flow "$UPF_PORT" "$EDN_PORT")"
post_flow "$(mk_arp_flow "$EDN_PORT" "$UPF_PORT")"

echo "[slice-flows] Installed. Quick rule check:"
curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}/onos/v1/flows/${DEVICE_ID}" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); flows=d.get("flows",[]); \
for f in flows:\n  crit={c.get("type"):c for c in f.get("selector",{}).get("criteria",[])}\n  if "UDP_DST" in crit:\n    print(f"priority={f.get(\"priority\")} udp_dst={crit[\"UDP_DST\"].get(\"udpPort\")} state={f.get(\"state\")}")'
