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

echo "== End-to-end testbed check =="
echo
run_check "Container validation" "${HERE}/check_containers.sh"
run_check "N6 path validation" "${HERE}/check_n6_path.sh"

if (( FAILURES > 0 )); then
  echo "End-to-end check FAILED with ${FAILURES} failing stage(s)." >&2
  exit 1
fi

echo "End-to-end check PASSED."
