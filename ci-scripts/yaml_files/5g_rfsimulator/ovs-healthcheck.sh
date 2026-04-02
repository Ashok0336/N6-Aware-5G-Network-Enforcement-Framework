#!/usr/bin/env bash
set -euo pipefail

# Docker health for OVS checks local bridge readiness only.
# Controller-driven forwarding validation belongs in scripts/check_ovs.sh and check_n6_path.sh.
BR="${BR:-${OVS_BRIDGE_NAME:-br-n6}}"
V_UPF_HOST="${V_UPF_HOST:-${OVS_UPF_PORT_NAME:-v-upf-host}}"
V_EDN_HOST="${V_EDN_HOST:-${OVS_EDN_PORT_NAME:-v-edn-host}}"

fail() {
  echo "[ovs-healthcheck] FAIL: $*" >&2
  exit 1
}

ovs-vsctl br-exists "${BR}" || fail "bridge ${BR} does not exist."
ovs-vsctl list-ports "${BR}" | grep -qx "${V_UPF_HOST}" || fail "bridge ${BR} is missing port ${V_UPF_HOST}."
ovs-vsctl list-ports "${BR}" | grep -qx "${V_EDN_HOST}" || fail "bridge ${BR} is missing port ${V_EDN_HOST}."
ip link show "${BR}" >/dev/null 2>&1 || fail "bridge link ${BR} is missing from the namespace."

exit 0
