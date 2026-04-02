#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

FAILURES=0

pass() {
  echo "[PASS] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

echo "== ONOS REST endpoint =="
echo "${ONOS_DEVICES_URL}"
echo

if ! device_json="$(curl -fsS -u "${ONOS_AUTH}" "${ONOS_DEVICES_URL}")"; then
  fail "could not reach ONOS REST at ${ONOS_DEVICES_URL}."
else
  pass "ONOS REST endpoint is reachable."
fi

if [[ -n "${device_json:-}" ]]; then
  echo "== ONOS devices =="
  echo "${device_json}" | python3 -m json.tool
  echo

  available_devices="$(
    echo "${device_json}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for x in d.get("devices", []) if x.get("available") is True))'
  )"
  if [[ "${available_devices}" -ge 1 ]]; then
    pass "ONOS reports ${available_devices} available device(s)."
  else
    fail "ONOS reports zero available devices."
  fi

  ovs_device_count="$(
    OVS_CONTAINER_IP="${OVS_CONTAINER_IP}" \
      python3 -c 'import json,os,sys; d=json.load(sys.stdin); ip=os.environ.get("OVS_CONTAINER_IP",""); print(sum(1 for x in d.get("devices", []) if (x.get("annotations") or {}).get("managementAddress") == ip))' <<< "${device_json}"
  )"
  if [[ "${ovs_device_count}" -ge 1 ]]; then
    pass "ONOS sees the expected OVS device at ${OVS_CONTAINER_IP}."
  else
    fail "ONOS does not report an OVS device with management address ${OVS_CONTAINER_IP}."
  fi
fi

if (( FAILURES > 0 )); then
  echo
  echo "== Recent ONOS logs =="
  docker logs --tail 120 "${ONOS_CONTAINER_NAME}" 2>&1 || true
  echo
  echo "ONOS check FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "ONOS check PASSED."
