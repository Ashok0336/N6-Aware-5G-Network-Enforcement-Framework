#!/usr/bin/env bash
set -u

OVS_CONTAINER="${OVS_CONTAINER:-ovs}"
ONOS_CONTAINER="${ONOS_CONTAINER:-onos}"
BRIDGE="${OVS_BRIDGE_NAME:-br-n6}"
UPF_PORT="${OVS_UPF_PORT_NAME:-v-upf-host}"
EDN_PORT="${OVS_EDN_PORT_NAME:-v-edn-host}"
EXT_DN_IP="${EXT_DN_IP:-192.168.72.135}"
UE_CONTAINER="${UE_CONTAINER:-rfsim5g-oai-nr-ue}"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf '[ready] PASS: %s\n' "$*"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf '[ready] FAIL: %s\n' "$*" >&2
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  printf '[ready] WARN: %s\n' "$*" >&2
}

container_running() {
  docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true
}

if container_running "$OVS_CONTAINER"; then
  pass "OVS container ${OVS_CONTAINER} is running"
else
  fail "OVS container ${OVS_CONTAINER} is not running"
fi

if container_running "$ONOS_CONTAINER"; then
  pass "ONOS container ${ONOS_CONTAINER} is running"
else
  fail "ONOS container ${ONOS_CONTAINER} is not running"
fi

if docker exec "$OVS_CONTAINER" ovs-vsctl br-exists "$BRIDGE" >/dev/null 2>&1; then
  pass "OVS bridge ${BRIDGE} exists"
else
  fail "OVS bridge ${BRIDGE} does not exist"
fi

for port in "$UPF_PORT" "$EDN_PORT"; do
  if docker exec "$OVS_CONTAINER" ovs-vsctl list-ports "$BRIDGE" 2>/dev/null | grep -qx "$port"; then
    pass "OVS bridge ${BRIDGE} has port ${port}"
  else
    fail "OVS bridge ${BRIDGE} is missing port ${port}"
  fi
done

if docker exec "$OVS_CONTAINER" ovs-vsctl get-controller "$BRIDGE" >/dev/null 2>&1; then
  controller="$(docker exec "$OVS_CONTAINER" ovs-vsctl get-controller "$BRIDGE" 2>/dev/null | tr -d '\r')"
  if [[ -n "$controller" ]]; then
    pass "OVS controller configured: ${controller}"
  else
    warn "OVS controller is not configured on ${BRIDGE}"
  fi
else
  warn "Could not read OVS controller for ${BRIDGE}"
fi

if docker exec "$ONOS_CONTAINER" curl -sf http://127.0.0.1:8181/onos/v1/devices -u onos:rocks >/tmp/onos-devices.json 2>/dev/null; then
  if grep -q '"available"[[:space:]]*:[[:space:]]*true' /tmp/onos-devices.json; then
    pass "ONOS reports at least one available device"
  else
    warn "ONOS REST is reachable but no available device was reported"
  fi
  rm -f /tmp/onos-devices.json
else
  warn "Could not query ONOS devices through container REST endpoint"
fi

if container_running "$UE_CONTAINER"; then
  if docker exec "$UE_CONTAINER" bash -lc "ping -I oaitun_ue1 -c 2 -W 2 ${EXT_DN_IP}" >/dev/null 2>&1; then
    pass "${UE_CONTAINER} can ping Ext-DN ${EXT_DN_IP}"
  else
    fail "${UE_CONTAINER} cannot ping Ext-DN ${EXT_DN_IP} through oaitun_ue1"
  fi
else
  fail "UE container ${UE_CONTAINER} is not running"
fi

if curl -sf http://localhost:8000/metrics >/dev/null 2>&1; then
  pass "Telemetry endpoint http://localhost:8000/metrics is reachable"
else
  fail "Telemetry endpoint http://localhost:8000/metrics is not reachable"
fi

if pgrep -f 'run_closed_loop.sh|run_policy_loop.sh|policy_manager' >/dev/null 2>&1; then
  if curl -sf http://localhost:8001/metrics >/dev/null 2>&1; then
    pass "Policy endpoint http://localhost:8001/metrics is reachable"
  else
    fail "Closed loop appears to be running, but policy endpoint http://localhost:8001/metrics is not reachable"
  fi
else
  if curl -sf http://localhost:8001/metrics >/dev/null 2>&1; then
    pass "Policy endpoint http://localhost:8001/metrics is reachable"
  else
    warn "Policy endpoint http://localhost:8001/metrics is not reachable; this is expected when closed loop is stopped"
  fi
fi

printf '\n[ready] Summary: pass=%s warn=%s fail=%s\n' "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT"
if [[ "$FAIL_COUNT" -gt 0 ]]; then
  exit 1
fi
