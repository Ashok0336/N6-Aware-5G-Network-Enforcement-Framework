#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"
# shellcheck disable=SC1091
source "${HERE}/scripts/check-common.sh"

FAILURES=0
FAILED_CONTAINERS=()

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

check_container() {
  local name="$1"
  local required="${2:-true}"
  local state=""
  local health=""

  if ! state="$(docker inspect -f '{{.State.Status}}' "${name}" 2>/dev/null)"; then
    if [[ "${required}" == "true" ]]; then
      fail "required container ${name} was not found."
      FAILED_CONTAINERS+=("${name}")
    else
      warn "optional container ${name} was not found."
    fi
    return 0
  fi

  health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${name}" 2>/dev/null || echo "unknown")"
  echo "${name}: state=${state} health=${health}"

  if [[ "${state}" != "running" ]]; then
    fail "${name} is not running."
    FAILED_CONTAINERS+=("${name}")
    return 0
  fi

  if [[ "${health}" != "no-healthcheck" && "${health}" != "healthy" ]]; then
    fail "${name} health is ${health}, expected healthy."
    FAILED_CONTAINERS+=("${name}")
    return 0
  fi

  pass "${name} is running and healthy enough for validation."
}

echo "== Required containers =="
required_containers=(
  "${MYSQL_CONTAINER_NAME}"
  "${AMF_CONTAINER_NAME}"
  "${SMF_CONTAINER_NAME}"
  "${UPF_CONTAINER_NAME}"
  "${EXT_DN_CONTAINER_NAME}"
  "${ONOS_CONTAINER_NAME}"
  "${OVS_CONTAINER_NAME}"
  "${GNB_CONTAINER_NAME}"
)

for name in "${required_containers[@]}"; do
  check_container "${name}" "true"
done

echo
echo "== Optional UE containers =="
optional_found=0
for ue_name in \
  rfsim5g-oai-nr-ue \
  rfsim5g-oai-nr-ue2 \
  rfsim5g-oai-nr-ue3 \
  rfsim5g-oai-nr-ue4 \
  rfsim5g-oai-nr-ue5 \
  rfsim5g-oai-nr-ue6 \
  rfsim5g-oai-nr-ue7 \
  rfsim5g-oai-nr-ue8 \
  rfsim5g-oai-nr-ue9 \
  rfsim5g-oai-nr-ue10; do
  if docker inspect -f '{{.State.Status}}' "${ue_name}" >/dev/null 2>&1; then
    optional_found=$((optional_found + 1))
    check_container "${ue_name}" "false"
  fi
done

if (( optional_found == 0 )); then
  warn "no optional UE containers were found."
fi

if (( FAILURES > 0 )); then
  echo
  echo "== Container summary =="
  docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E 'onos|ovs|rfsim5g' || true
  echo
  for name in "${FAILED_CONTAINERS[@]}"; do
    echo "== Recent logs: ${name} =="
    docker logs --tail 60 "${name}" 2>&1 || true
    echo
  done
  echo "Container check FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "Container check PASSED."
