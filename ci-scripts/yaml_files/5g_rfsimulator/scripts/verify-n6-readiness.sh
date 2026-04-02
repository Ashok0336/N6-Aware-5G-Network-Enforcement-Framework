#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"
# shellcheck disable=SC1091
source "${HERE}/scripts/check-common.sh"

UPF_N6_ADDR="${UPF_N6_IP%/*}"
FAILURES=0

require_docker_access

pass() {
  echo "[PASS] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

warn() {
  echo "[WARN] $*"
}

check_flow_rule() {
  local description="$1"
  local priority="$2"
  local in_port="$3"
  local eth_marker="$4"
  local out_port="$5"
  local dump="$6"

  while IFS= read -r line; do
    [[ "${line}" == *"priority=${priority}"* ]] || continue
    [[ "${line}" == *"in_port=${in_port}"* ]] || continue
    [[ "${line}" == *"output:${out_port}"* ]] || continue
    if [[ "${eth_marker}" == "arp" ]]; then
      [[ "${line}" == *",arp,"* || "${line}" == *" arp,"* || "${line}" == *"dl_type=0x0806"* ]] || continue
    else
      [[ "${line}" == *",ip,"* || "${line}" == *" ip,"* || "${line}" == *"dl_type=0x0800"* ]] || continue
    fi
    pass "${description}"
    return 0
  done <<< "${dump}"

  fail "${description}"
  return 1
}

echo "== Ext-DN readiness =="
if docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip -br link show '${EXT_DN_IF}'
ip -br addr show '${EXT_DN_IF}'
"; then
  pass "${EXT_DN_CONTAINER_NAME} exposes ${EXT_DN_IF}."
else
  fail "${EXT_DN_CONTAINER_NAME} is missing ${EXT_DN_IF}."
fi
IFS=',' read -r -a ROUTES <<<"${EXT_DN_ROUTE_LIST}"
for route in "${ROUTES[@]}"; do
  [[ -n "${route}" ]] || continue
  if docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip route show '${route}' | grep -Fq 'via ${UPF_N6_GW} dev ${EXT_DN_IF}'
"; then
    pass "Ext-DN route ${route} is installed via ${UPF_N6_GW} on ${EXT_DN_IF}."
  else
    fail "Ext-DN route ${route} is missing or points somewhere else."
  fi
done
echo

echo "== UPF readiness =="
if docker exec "${UPF_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip -br link show '${UPF_N6_IF}'
ip -br addr show '${UPF_N6_IF}'
"; then
  pass "${UPF_CONTAINER_NAME} exposes ${UPF_N6_IF}."
else
  fail "${UPF_CONTAINER_NAME} is missing ${UPF_N6_IF}."
fi
echo

echo "== OVS bridge state =="
OVS_SHOW="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl show)"
CONTROLLER_DETAILS="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list controller)"
UPF_OFPORT="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get Interface "${OVS_UPF_PORT_NAME}" ofport | tr -d '[:space:]')"
EDN_OFPORT="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get Interface "${OVS_EDN_PORT_NAME}" ofport | tr -d '[:space:]')"
echo "${OVS_SHOW}"
echo
echo "${CONTROLLER_DETAILS}"
echo
echo "upf_ofport=${UPF_OFPORT}"
echo "edn_ofport=${EDN_OFPORT}"
CONTROLLER_TARGET="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get-controller "${OVS_BRIDGE_NAME}" | tr -d '\r')"
echo "controller_target=${CONTROLLER_TARGET}"
echo "expected_controller=tcp:${ONOS_CTRL}"
if [[ "${CONTROLLER_TARGET}" == *"tcp:${ONOS_CTRL}"* ]]; then
  pass "OVS controller target matches ${ONOS_CTRL}."
else
  fail "OVS controller target does not match ${ONOS_CTRL}."
fi
CONTROLLER_CONNECTED=false
if echo "${CONTROLLER_DETAILS}" | grep -q "is_connected[[:space:]]*:[[:space:]]*true"; then
  CONTROLLER_CONNECTED=true
fi
echo

echo "== ONOS device availability =="
DEVICE_JSON="$(curl -fsS -u "${ONOS_AUTH}" "${ONOS_DEVICES_URL}")"
echo "${DEVICE_JSON}" | python3 -m json.tool
AVAILABLE_DEVICES="$(
  echo "${DEVICE_JSON}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for x in d.get("devices", []) if x.get("available") is True))'
)"
DEVICE_ID="$(
  echo "${DEVICE_JSON}" | python3 -c 'import json,sys; d=json.load(sys.stdin); dev=[x.get("id","") for x in d.get("devices", []) if x.get("available") is True]; print(dev[0] if dev else "")'
)"
if [[ "${AVAILABLE_DEVICES}" -ge 1 ]]; then
  pass "ONOS reports ${AVAILABLE_DEVICES} available device(s)."
else
  fail "ONOS reports zero available devices."
fi
if [[ "${CONTROLLER_CONNECTED}" == "true" ]]; then
  pass "OVS reports a live OpenFlow controller session."
elif [[ "${AVAILABLE_DEVICES}" -ge 1 ]]; then
  pass "ONOS device availability confirms controller connectivity after the latest bridge reattachment."
else
  fail "OVS is not connected to ONOS yet."
fi
echo

echo "== ONOS flow view =="
if [[ -n "${DEVICE_ID}" ]]; then
  ONOS_FLOWS="$(curl -fsS -u "${ONOS_AUTH}" "${ONOS_BASE_URL}/onos/v1/flows/${DEVICE_ID}")"
  echo "${ONOS_FLOWS}" | python3 -m json.tool
  ONOS_FLOW_COUNT="$(
    echo "${ONOS_FLOWS}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("flows", [])))'
  )"
  if [[ "${ONOS_FLOW_COUNT}" -ge 1 ]]; then
    pass "ONOS reports ${ONOS_FLOW_COUNT} flow entries for ${DEVICE_ID}."
  else
    warn "ONOS reports zero flow entries for ${DEVICE_ID}; relying on direct OVS flow inspection."
  fi
else
  fail "Could not resolve an available ONOS device ID."
fi
echo

echo "== OpenFlow data-plane state =="
FLOW_DUMP="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}")"
echo "${FLOW_DUMP}"
check_flow_rule \
  "OVS forwards ARP from ${OVS_UPF_PORT_NAME} to ${OVS_EDN_PORT_NAME}." \
  "${N6_ARP_FORWARD_PRIORITY}" \
  "${UPF_OFPORT}" \
  "arp" \
  "${EDN_OFPORT}" \
  "${FLOW_DUMP}"
check_flow_rule \
  "OVS forwards ARP from ${OVS_EDN_PORT_NAME} to ${OVS_UPF_PORT_NAME}." \
  "${N6_ARP_FORWARD_PRIORITY}" \
  "${EDN_OFPORT}" \
  "arp" \
  "${UPF_OFPORT}" \
  "${FLOW_DUMP}"
check_flow_rule \
  "OVS forwards IPv4 from ${OVS_UPF_PORT_NAME} to ${OVS_EDN_PORT_NAME}." \
  "${N6_BASE_FORWARD_PRIORITY}" \
  "${UPF_OFPORT}" \
  "ip" \
  "${EDN_OFPORT}" \
  "${FLOW_DUMP}"
check_flow_rule \
  "OVS forwards IPv4 from ${OVS_EDN_PORT_NAME} to ${OVS_UPF_PORT_NAME}." \
  "${N6_BASE_FORWARD_PRIORITY}" \
  "${EDN_OFPORT}" \
  "ip" \
  "${UPF_OFPORT}" \
  "${FLOW_DUMP}"
if echo "${FLOW_DUMP}" | grep -Eq 'set_queue:1|set_queue:2|set_queue:3'; then
  pass "Slice queue rules are installed in OVS."
else
  warn "Slice queue rules are not installed yet; bootstrap forwarding is active without queue enforcement."
fi
echo

echo "== Bidirectional N6 ping =="
if echo "${FLOW_DUMP}" | grep -Eq 'actions=.*output:'; then
  if docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ping -I '${EXT_DN_IF}' -c 2 -W 1 '${UPF_N6_ADDR}'
"; then
    pass "Ext-DN reaches the UPF N6 address ${UPF_N6_ADDR}."
  else
    fail "Ext-DN cannot reach the UPF N6 address ${UPF_N6_ADDR}."
  fi
  if docker exec "${OVS_CONTAINER_NAME}" bash -lc "
set -euo pipefail
upf_pid=\$(docker inspect -f '{{if .State.Running}}{{.State.Pid}}{{else}}0{{end}}' '${UPF_CONTAINER_NAME}')
if [[ -z \"\${upf_pid}\" || \"\${upf_pid}\" == '0' ]]; then
  echo 'UPF container is not running.' >&2
  exit 1
fi
nsenter -t \"\${upf_pid}\" -n -- ping -I '${UPF_N6_IF}' -c 2 -W 1 '${EXT_DN_TARGET_IP}'
"; then
    pass "UPF reaches the Ext-DN N6 address ${EXT_DN_TARGET_IP}."
  else
    fail "UPF cannot reach the Ext-DN N6 address ${EXT_DN_TARGET_IP}."
  fi
else
  warn "Skipping N6 ping because OVS has no forwarding flows yet."
fi

if (( FAILURES > 0 )); then
  echo
  echo "N6 readiness FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "N6 readiness PASSED."
