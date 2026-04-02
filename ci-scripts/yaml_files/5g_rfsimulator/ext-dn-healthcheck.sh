#!/usr/bin/env bash
set -euo pipefail

# Docker health for ext-DN checks only local container readiness.
# Strict N6 reachability belongs in scripts/check_n6_path.sh and verify-n6-readiness.sh.
EXT_DN_IF="${EXT_DN_IF:-dn0}"
UPF_N6_GW="${UPF_N6_GW:-192.168.72.134}"
UPF_N6_GW="${UPF_N6_GW%%/*}"
EXT_DN_ROUTE_LIST="${EXT_DN_ROUTE_LIST:-12.1.1.0/24,12.1.2.0/24,12.1.3.0/24}"

fail() {
  echo "[ext-dn-healthcheck] FAIL: $*" >&2
  exit 1
}

test -r /proc/1/status || fail "PID 1 is not readable."
ip link show "${EXT_DN_IF}" >/dev/null 2>&1 || fail "interface ${EXT_DN_IF} is missing."

IFS=',' read -r -a routes <<<"${EXT_DN_ROUTE_LIST}"
for route in "${routes[@]}"; do
  [[ -n "${route}" ]] || continue
  ip route show "${route}" | grep -Fq "via ${UPF_N6_GW} dev ${EXT_DN_IF}" \
    || fail "route ${route} is missing or not installed via ${UPF_N6_GW} on ${EXT_DN_IF}."
done

exit 0
