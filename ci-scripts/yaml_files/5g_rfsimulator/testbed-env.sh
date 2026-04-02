#!/usr/bin/env bash
set -euo pipefail

# Shared environment loader for the authoritative docker-compose.yaml deployment.
# Scripts should source this file instead of hardcoding ONOS, OVS, or N6 details.
TESTBED_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${TESTBED_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${TESTBED_DIR}/.env"
  set +a
fi

export TESTBED_DIR
export ONOS_HOST="${ONOS_HOST:-${ONOS_CONTAINER_IP:-192.168.71.160}}"
export OVS_HOST="${OVS_HOST:-${OVS_CONTAINER_IP:-192.168.71.161}}"
export ONOS_CONTAINER_IP="${ONOS_CONTAINER_IP:-${ONOS_HOST}}"
export OVS_CONTAINER_IP="${OVS_CONTAINER_IP:-${OVS_HOST}}"
export ONOS_REST_HOST="${ONOS_REST_HOST:-${ONOS_HOST}}"
export ONOS_REST_PORT="${ONOS_REST_PORT:-8181}"
export ONOS_OF_HOST="${ONOS_OF_HOST:-${ONOS_HOST}}"
export ONOS_OF_PORT="${ONOS_OF_PORT:-6653}"
export ONOS_AUTH="${ONOS_AUTH:-onos:rocks}"
export ONOS_USERNAME="${ONOS_USERNAME:-${ONOS_AUTH%%:*}}"
export ONOS_PASSWORD="${ONOS_PASSWORD:-${ONOS_AUTH#*:}}"
export ONOS_CTRL="${ONOS_CTRL:-${ONOS_HOST}:${ONOS_OF_PORT}}"

export ONOS_CONTAINER_NAME="${ONOS_CONTAINER_NAME:-onos}"
export OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
export OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
export OVS_UPF_PORT_NAME="${OVS_UPF_PORT_NAME:-v-upf-host}"
export OVS_EDN_PORT_NAME="${OVS_EDN_PORT_NAME:-v-edn-host}"
export N6_ARP_FORWARD_PRIORITY="${N6_ARP_FORWARD_PRIORITY:-45000}"
export N6_BASE_FORWARD_PRIORITY="${N6_BASE_FORWARD_PRIORITY:-5000}"

export MYSQL_CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-rfsim5g-mysql}"
export AMF_CONTAINER_NAME="${AMF_CONTAINER_NAME:-rfsim5g-oai-amf}"
export SMF_CONTAINER_NAME="${SMF_CONTAINER_NAME:-rfsim5g-oai-smf}"
export GNB_CONTAINER_NAME="${GNB_CONTAINER_NAME:-rfsim5g-oai-gnb}"
export UPF_CONTAINER_NAME="${UPF_CONTAINER_NAME:-rfsim5g-oai-upf}"
export UPF_N6_IF="${UPF_N6_IF:-n6ovs0}"
export UPF_N6_IP="${UPF_N6_IP:-192.168.72.134/26}"
export UPF_N6_GW="${UPF_N6_GW:-${UPF_N6_IP%/*}}"

export EXT_DN_CONTAINER_NAME="${EXT_DN_CONTAINER_NAME:-rfsim5g-oai-ext-dn}"
export EXT_DN_IF="${EXT_DN_IF:-dn0}"
export EXT_DN_N6_IP="${EXT_DN_N6_IP:-192.168.72.135/26}"
export EXT_DN_ROUTE_LIST="${EXT_DN_ROUTE_LIST:-12.1.1.0/24,12.1.2.0/24,12.1.3.0/24}"
export EXT_DN_TARGET_IP="${EXT_DN_TARGET_IP:-${EXT_DN_N6_IP%/*}}"

export ONOS_BASE_URL="${ONOS_BASE_URL:-http://${ONOS_HOST}:${ONOS_REST_PORT}}"
export ONOS_DEVICES_URL="${ONOS_DEVICES_URL:-${ONOS_BASE_URL}/onos/v1/devices}"
