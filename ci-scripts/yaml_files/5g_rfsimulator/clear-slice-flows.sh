#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

ONOS_IP="${ONOS_IP:-${ONOS_REST_HOST}}"
ONOS_PORT="${ONOS_PORT:-${ONOS_REST_PORT}}"
AUTH="${AUTH:-${ONOS_AUTH}}"
ONOS_CONT="${ONOS_CONT:-${ONOS_CONTAINER_NAME:-onos}}"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
KARAF_CLIENT="${KARAF_CLIENT:-/root/onos/apache-karaf-4.2.14/bin/client}"
KARAF_USER="${KARAF_USER:-karaf}"
KARAF_PASSWORD="${KARAF_PASSWORD:-karaf}"
UPF_PORT="${UPF_PORT:-1}"
EDN_PORT="${EDN_PORT:-2}"
APP_ID="${APP_ID:-org.oai.slicequeue}"
APP_MATCH_REGEX="${APP_MATCH_REGEX:-org\\.oai\\.slicequeue|onos-slice-queue|ONOS Slice Queue App}"

rest_get() {
  local path="$1"
  curl -fsS -u "$AUTH" "http://${ONOS_IP}:${ONOS_PORT}${path}" 2>/dev/null \
    || docker exec "$ONOS_CONT" curl -fsS -u "$AUTH" "http://localhost:${ONOS_PORT}${path}" 2>/dev/null
}

rest_post_json() {
  local path="$1"
  local payload="$2"
  curl -fsS -u "$AUTH" -H "Content-Type: application/json" \
    -X POST "http://${ONOS_IP}:${ONOS_PORT}${path}" -d "$payload" >/dev/null 2>&1 \
    || printf '%s' "$payload" | docker exec -i "$ONOS_CONT" curl -fsS -u "$AUTH" -H "Content-Type: application/json" \
      -X POST "http://localhost:${ONOS_PORT}${path}" -d @- >/dev/null 2>&1
}

karaf() {
  local command="$1"
  docker exec "$ONOS_CONT" bash -lc "printf '%s\n' '${command}' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b" >/dev/null 2>&1
}

find_queue_bundle_ids() {
  docker exec "$ONOS_CONT" bash -lc "printf 'bundle:list\n' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b 2>/dev/null | grep -Ei '${APP_MATCH_REGEX}' | sed -n 's/^[^0-9]*\([0-9][0-9]*\).*/\1/p'" 2>/dev/null || true
}

remove_queue_app() {
  local bundle_ids
  bundle_ids="$(find_queue_bundle_ids)"
  if [[ -z "$bundle_ids" ]]; then
    echo "[clear-slice-flows] ${APP_ID} bundle is not active."
    return 0
  fi

  local bundle_id=""
  for bundle_id in $bundle_ids; do
    echo "[clear-slice-flows] Deactivating ${APP_ID} bundle_id=${bundle_id}"
    karaf "bundle:stop ${bundle_id}" || true
    karaf "bundle:uninstall ${bundle_id}" || true
  done
}

set_queue_flows_present() {
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep -q "set_queue"
}

wait_for_no_set_queue_flows() {
  for _ in {1..30}; do
    if ! set_queue_flows_present; then
      return 0
    fi
    sleep 1
  done
  return 1
}

get_onos_device_id() {
  rest_get "/onos/v1/devices" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); dev=[x.get("id") for x in d.get("devices",[]) if x.get("available") is True]; print(dev[0] if dev else "")' 2>/dev/null
}

post_basic_flow() {
  local device_id="$1"
  local eth_type="$2"
  local in_port="$3"
  local out_port="$4"
  local priority="$5"
  local payload
  payload="{
      \"priority\": ${priority},
      \"timeout\": 0,
      \"isPermanent\": true,
      \"deviceId\": \"${device_id}\",
      \"treatment\": {\"instructions\": [{\"type\": \"OUTPUT\", \"port\": \"${out_port}\"}]},
      \"selector\": {\"criteria\": [
        {\"type\": \"IN_PORT\", \"port\": \"${in_port}\"},
        {\"type\": \"ETH_TYPE\", \"ethType\": \"${eth_type}\"}
      ]}
    }"
  rest_post_json "/onos/v1/flows/${device_id}" "$payload"
}

install_basic_forwarding() {
  local device_id
  device_id="$(get_onos_device_id || true)"
  if [[ -z "$device_id" ]]; then
    echo "[clear-slice-flows] ERROR: no available ONOS device." >&2
    return 1
  fi

  post_basic_flow "$device_id" "0x0806" "$UPF_PORT" "$EDN_PORT" 45000
  post_basic_flow "$device_id" "0x0806" "$EDN_PORT" "$UPF_PORT" 45000
  post_basic_flow "$device_id" "0x0800" "$UPF_PORT" "$EDN_PORT" 5000
  post_basic_flow "$device_id" "0x0800" "$EDN_PORT" "$UPF_PORT" 5000
}

remove_queue_app

if ! wait_for_no_set_queue_flows; then
  echo "[clear-slice-flows] ERROR: set_queue flows remain after ONOS app cleanup." >&2
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep "set_queue" || true
  exit 1
fi

install_basic_forwarding

if set_queue_flows_present; then
  echo "[clear-slice-flows] ERROR: set_queue flows remain after basic forwarding install." >&2
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null | grep "set_queue" || true
  exit 1
fi

echo "[clear-slice-flows] SUCCESS: no set_queue flows remain"
