#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

ONOS_IP="${ONOS_IP:-${ONOS_REST_HOST}}"
ONOS_PORT="${ONOS_PORT:-${ONOS_REST_PORT}}"
AUTH="${AUTH:-${ONOS_AUTH}}"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
UPF_PORT="${UPF_PORT:-1}"
EDN_PORT="${EDN_PORT:-2}"

rest_get() {
  local path="$1"
  curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}${path}" 2>/dev/null \
    || docker exec "${ONOS_CONTAINER_NAME:-onos}" curl -fsS -u "$AUTH" "http://localhost:${ONOS_PORT}${path}" 2>/dev/null
}

wait_for_onos_device() {
  echo "[slice-flows] Waiting for ONOS REST at http://${ONOS_IP}:${ONOS_PORT} ..."
  local body=""
  local available_count=0
  for _ in {1..90}; do
    if body="$(rest_get "/onos/v1/devices")"; then
      available_count="$(
        echo "$body" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(sum(1 for x in d.get("devices",[]) if x.get("available") is True))' 2>/dev/null || echo 0
      )"
      if [[ "$available_count" -ge 1 ]]; then
        DEVICE_ID="$(
          echo "$body" | python3 -c 'import sys,json; d=json.load(sys.stdin); dev=[x.get("id") for x in d.get("devices",[]) if x.get("available") is True]; print(dev[0] if dev else "")' 2>/dev/null || true
        )"
        echo "[slice-flows] DEVICE_ID=${DEVICE_ID}"
        return 0
      fi
    fi
    echo "[slice-flows] waiting for ONOS device discovery"
    sleep 2
  done
  echo "[slice-flows] ERROR: ONOS did not report an available OVS device within 180s." >&2
  return 1
}

verify_set_queue_flows() {
  local flows
  flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
  echo "$flows" | grep -q "set_queue:1" \
    && echo "$flows" | grep -q "set_queue:2" \
    && echo "$flows" | grep -q "set_queue:3"
}

print_set_queue_flows() {
  echo "[slice-flows] Current set_queue flows:"
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep "set_queue" || true
}

wait_for_set_queue_flows() {
  for _ in {1..30}; do
    if verify_set_queue_flows; then
      return 0
    fi
    sleep 2
  done
  return 1
}

DEVICE_ID=""
wait_for_onos_device

if verify_set_queue_flows; then
  echo "[slice-flows] Existing ONOS queue-selection rules are present."
  print_set_queue_flows
  exit 0
fi

echo "[slice-flows] set_queue rules are missing; deploying ONOS slice queue app..."
"${HERE}/deploy-onos-slice-app.sh"

echo "[slice-flows] Verifying ONOS app queue rules in OVS..."
if wait_for_set_queue_flows; then
  echo "[slice-flows] SUCCESS: set_queue:1, set_queue:2, and set_queue:3 are present."
  print_set_queue_flows
  exit 0
fi

echo "[slice-flows] ERROR: ONOS app did not install verified queue-selection rules." >&2
exit 1
