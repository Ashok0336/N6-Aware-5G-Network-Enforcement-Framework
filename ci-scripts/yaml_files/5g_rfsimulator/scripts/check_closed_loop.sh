#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"
# shellcheck disable=SC1091
source "${HERE}/scripts/check-common.sh"

FAILURES=0

pass() {
  echo "[PASS] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

run_python_check() {
  local description="$1"
  shift
  if python3 - "$@"; then
    pass "${description}"
  else
    fail "${description}"
  fi
}

echo "== Closed-loop telemetry =="
LATEST_TELEMETRY="$(
  HERE="${HERE}" python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ["HERE"]) / "logs" / "telemetry"
files = sorted(root.glob("closed_loop_telemetry_*.jsonl"), key=lambda p: p.stat().st_mtime)
print(files[-1] if files else "")
PY
)"
if [[ -z "${LATEST_TELEMETRY}" || ! -f "${LATEST_TELEMETRY}" ]]; then
  fail "No closed-loop telemetry log found under logs/telemetry."
else
  pass "Found telemetry log ${LATEST_TELEMETRY}."
  run_python_check \
    "Telemetry snapshot exposes OVS, ONOS, ping, iperf, Docker, and slice metrics." \
    "${LATEST_TELEMETRY}" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if not lines:
    raise SystemExit(1)
payload = json.loads(lines[-1])
telemetry = payload["telemetry"]
required = ["ovs", "onos", "ping", "iperf", "docker"]
if not all(key in telemetry for key in required):
    raise SystemExit(1)
if not all(name in payload.get("slice_metrics", {}) for name in ("embb", "urllc", "mmtc")):
    raise SystemExit(1)
PY
fi
echo

echo "== Closed-loop policy =="
LATEST_POLICY="$(
  HERE="${HERE}" python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ["HERE"]) / "logs" / "policy"
files = sorted(root.glob("closed_loop_policy_*.jsonl"), key=lambda p: p.stat().st_mtime)
print(files[-1] if files else "")
PY
)"
if [[ -z "${LATEST_POLICY}" || ! -f "${LATEST_POLICY}" ]]; then
  fail "No closed-loop policy JSONL log found under logs/policy."
else
  pass "Found policy log ${LATEST_POLICY}."
  run_python_check \
    "Policy log includes decisions for eMBB, URLLC, and mMTC." \
    "${LATEST_POLICY}" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if not lines:
    raise SystemExit(1)
payload = json.loads(lines[-1])
decisions = {item["slice_name"] for item in payload.get("decisions", [])}
if decisions != {"embb", "urllc", "mmtc"}:
    raise SystemExit(1)
PY
fi

LATEST_POLICY_CSV="$(
  HERE="${HERE}" python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ["HERE"]) / "logs" / "policy"
files = sorted(root.glob("closed_loop_policy_*.csv"), key=lambda p: p.stat().st_mtime)
print(files[-1] if files else "")
PY
)"
if [[ -z "${LATEST_POLICY_CSV}" || ! -f "${LATEST_POLICY_CSV}" ]]; then
  fail "No closed-loop policy CSV log found under logs/policy."
else
  pass "Found policy CSV log ${LATEST_POLICY_CSV}."
fi

if (( FAILURES > 0 )); then
  echo
  echo "Closed-loop validation FAILED with ${FAILURES} issue(s)." >&2
  exit 1
fi

echo
echo "Closed-loop validation PASSED."
