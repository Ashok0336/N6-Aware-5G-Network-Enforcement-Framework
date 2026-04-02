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

container_state="$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${UPF_CONTAINER_NAME}" 2>/dev/null || true)"
container_available=true
expected_upf_n6_addr="${UPF_N6_IP%/*}"

echo "== UPF container state =="
if [[ -n "${container_state}" ]]; then
  echo "${UPF_CONTAINER_NAME}: ${container_state}"
else
  fail "container ${UPF_CONTAINER_NAME} was not found."
  container_available=false
fi
echo

if [[ "${container_available}" == "true" ]] && docker exec "${UPF_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip link show '${UPF_N6_IF}' >/dev/null 2>&1
"; then
  pass "interface ${UPF_N6_IF} exists in ${UPF_CONTAINER_NAME}."
else
  fail "interface ${UPF_N6_IF} is missing in ${UPF_CONTAINER_NAME}."
fi

if [[ "${container_available}" == "true" ]] && docker exec "${UPF_CONTAINER_NAME}" bash -lc "
set -euo pipefail
ip -4 addr show dev '${UPF_N6_IF}' | grep -Fq '${expected_upf_n6_addr}/'
"; then
  pass "interface ${UPF_N6_IF} has the expected address ${UPF_N6_IP}."
else
  fail "interface ${UPF_N6_IF} is missing the expected address ${UPF_N6_IP}."
fi

echo
echo "== UPF interface snapshot =="
if [[ "${container_available}" == "true" ]]; then
  docker exec "${UPF_CONTAINER_NAME}" bash -lc '
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
  echo "== Recent UPF logs =="
  docker logs --tail 80 "${UPF_CONTAINER_NAME}" 2>&1 || true
  echo
  echo "UPF check FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "UPF check PASSED."
