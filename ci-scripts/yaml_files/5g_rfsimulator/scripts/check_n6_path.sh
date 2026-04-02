#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
FAILURES=0

run_check() {
  local label="$1"
  local script_path="$2"

  echo "== ${label} =="
  if "${script_path}"; then
    :
  else
    FAILURES=$((FAILURES + 1))
  fi
  echo
}

echo "== Composite N6 path check =="
echo
run_check "Ext-DN validation" "${HERE}/check_extdn.sh"
run_check "UPF validation" "${HERE}/check_upf.sh"
run_check "OVS validation" "${HERE}/check_ovs.sh"
run_check "ONOS validation" "${HERE}/check_onos.sh"
run_check "N6 forwarding validation" "${HERE}/check_n6_forwarding.sh"

if (( FAILURES > 0 )); then
  echo "N6 path check FAILED with ${FAILURES} failing stage(s)." >&2
  exit 1
fi

echo "N6 path check PASSED."
