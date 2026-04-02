#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"
# shellcheck disable=SC1091
source "${HERE}/scripts/check-common.sh"

FAILURES=0

require_docker_access

pass() {
  echo "[PASS] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

container_state="$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${OVS_CONTAINER_NAME}" 2>/dev/null || true)"
container_available=true

echo "== OVS container state =="
if [[ -n "${container_state}" ]]; then
  echo "${OVS_CONTAINER_NAME}: ${container_state}"
else
  fail "container ${OVS_CONTAINER_NAME} was not found."
  container_available=false
fi
echo

if [[ "${container_available}" == "true" ]] && docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl br-exists "${OVS_BRIDGE_NAME}"; then
  pass "bridge ${OVS_BRIDGE_NAME} exists."
else
  fail "bridge ${OVS_BRIDGE_NAME} does not exist."
fi

for port_name in "${OVS_UPF_PORT_NAME}" "${OVS_EDN_PORT_NAME}"; do
  if [[ "${container_available}" == "true" ]] && docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list-ports "${OVS_BRIDGE_NAME}" | grep -qx "${port_name}"; then
    pass "bridge ${OVS_BRIDGE_NAME} includes port ${port_name}."
  else
    fail "bridge ${OVS_BRIDGE_NAME} is missing port ${port_name}."
  fi
done

controller_target=""
if [[ "${container_available}" == "true" ]]; then
  controller_target="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl get-controller "${OVS_BRIDGE_NAME}" | tr -d '\r' || true)"
fi
echo "controller_target=${controller_target}"
echo "expected_controller=tcp:${ONOS_CTRL}"
if [[ "${controller_target}" == *"tcp:${ONOS_CTRL}"* ]]; then
  pass "controller target matches ${ONOS_CTRL}."
else
  fail "controller target does not match ${ONOS_CTRL}."
fi

controller_details=""
if [[ "${container_available}" == "true" ]]; then
  controller_details="$(docker exec "${OVS_CONTAINER_NAME}" ovs-vsctl list controller || true)"
fi
if echo "${controller_details}" | grep -q "is_connected[[:space:]]*:[[:space:]]*true"; then
  pass "OVS reports an active OpenFlow controller session."
else
  fail "OVS is not connected to ONOS."
fi

echo
echo "== OVS bridge snapshot =="
if [[ "${container_available}" == "true" ]]; then
  docker exec "${OVS_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ovs-vsctl show
echo
ovs-vsctl list controller
echo
ovs-vsctl list-ports '${OVS_BRIDGE_NAME}'
echo
ovs-ofctl -O OpenFlow13 show '${OVS_BRIDGE_NAME}'
"
fi

if (( FAILURES > 0 )); then
  echo
  echo "== Recent ovs-init log =="
  docker exec "${OVS_CONTAINER_NAME}" tail -n 120 /var/log/openvswitch/ovs-init.log 2>/dev/null || true
  echo
  echo "OVS check FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "OVS check PASSED."
