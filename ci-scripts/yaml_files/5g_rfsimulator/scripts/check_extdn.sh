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

container_state="$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${EXT_DN_CONTAINER_NAME}" 2>/dev/null || true)"
container_available=true

echo "== Ext-DN container state =="
if [[ -n "${container_state}" ]]; then
  echo "${EXT_DN_CONTAINER_NAME}: ${container_state}"
else
  fail "container ${EXT_DN_CONTAINER_NAME} was not found."
  container_available=false
fi
echo

if [[ "${container_available}" == "true" ]] && docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip link show '${EXT_DN_IF}' >/dev/null 2>&1
"; then
  pass "interface ${EXT_DN_IF} exists in ${EXT_DN_CONTAINER_NAME}."
else
  fail "interface ${EXT_DN_IF} is missing in ${EXT_DN_CONTAINER_NAME}."
fi

IFS=',' read -r -a routes <<<"${EXT_DN_ROUTE_LIST}"
for route in "${routes[@]}"; do
  [[ -n "${route}" ]] || continue
  if [[ "${container_available}" == "true" ]] && docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip route show '${route}' | grep -Fq 'via ${UPF_N6_GW} dev ${EXT_DN_IF}'
"; then
    pass "route ${route} is installed via ${UPF_N6_GW} on ${EXT_DN_IF}."
  else
    fail "route ${route} is missing or misconfigured."
  fi
done

echo
echo "== Ext-DN interface snapshot =="
if [[ "${container_available}" == "true" ]]; then
  docker exec "${EXT_DN_CONTAINER_NAME}" bash -lc '
set -euo pipefail
ip -br link
echo
ip -br addr
echo
ip route
'
fi

if (( FAILURES > 0 )); then
  echo
  echo "== Recent ext-DN logs =="
  docker logs --tail 80 "${EXT_DN_CONTAINER_NAME}" 2>&1 || true
  echo
  echo "Ext-DN check FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "Ext-DN check PASSED."
