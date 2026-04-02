#!/usr/bin/env bash
set -euo pipefail

PUBLIC_IF="${PUBLIC_IF:-eth0}"
EXT_DN_IF="${EXT_DN_IF:-dn0}"
UPF_N6_GW="${UPF_N6_GW:-192.168.72.134}"
UPF_N6_GW="${UPF_N6_GW%%/*}"
EXT_DN_WAIT_TIMEOUT_SECONDS="${EXT_DN_WAIT_TIMEOUT_SECONDS:-120}"
EXT_DN_WAIT_LOG_INTERVAL_SECONDS="${EXT_DN_WAIT_LOG_INTERVAL_SECONDS:-5}"
EXT_DN_ROUTE_LIST="${EXT_DN_ROUTE_LIST:-12.1.1.0/24,12.1.2.0/24,12.1.3.0/24}"
EXT_DN_STARTUP_PROBE_ENABLED="${EXT_DN_STARTUP_PROBE_ENABLED:-true}"
EXT_DN_STARTUP_PROBE_TARGET="${EXT_DN_STARTUP_PROBE_TARGET:-${UPF_N6_GW}}"
EXT_DN_STARTUP_PROBE_COUNT="${EXT_DN_STARTUP_PROBE_COUNT:-1}"
EXT_DN_STARTUP_PROBE_TIMEOUT_SECONDS="${EXT_DN_STARTUP_PROBE_TIMEOUT_SECONDS:-1}"
EXT_DN_RECONCILE_INTERVAL_SECONDS="${EXT_DN_RECONCILE_INTERVAL_SECONDS:-5}"

log() {
  echo "[ext-dn-entrypoint] $*"
}

dump_debug() {
  log "Interface snapshot:"
  ip -br link || true
  log "Address snapshot:"
  ip -br addr || true
  log "Route snapshot:"
  ip route || true
  log "NAT table snapshot:"
  iptables -t nat -S || true
}

ensure_nat_rule() {
  if iptables -t nat -C POSTROUTING -o "${PUBLIC_IF}" -j MASQUERADE >/dev/null 2>&1; then
    return 0
  fi
  iptables -t nat -A POSTROUTING -o "${PUBLIC_IF}" -j MASQUERADE
}

ensure_sysctls() {
  sysctl -qw net.ipv4.conf.all.rp_filter=0 || true
  sysctl -qw net.ipv4.conf.default.rp_filter=0 || true
  sysctl -qw "net.ipv4.conf.${EXT_DN_IF}.rp_filter=0" || true
}

wait_for_interface() {
  local deadline=$((SECONDS + EXT_DN_WAIT_TIMEOUT_SECONDS))
  local next_log_at=$((SECONDS + EXT_DN_WAIT_LOG_INTERVAL_SECONDS))
  local interface_list=""
  while (( SECONDS < deadline )); do
    if ip link show "${EXT_DN_IF}" >/dev/null 2>&1; then
      return 0
    fi
    if (( SECONDS >= next_log_at )); then
      interface_list="$(ip -o link show | awk -F': ' '{print $2}' | paste -sd ',' -)"
      log "${EXT_DN_IF} not present yet after $((EXT_DN_WAIT_TIMEOUT_SECONDS - (deadline - SECONDS)))s. Current interfaces: ${interface_list}"
      next_log_at=$((SECONDS + EXT_DN_WAIT_LOG_INTERVAL_SECONDS))
    fi
    sleep 1
  done
  return 1
}

configure_routes() {
  local route=""
  IFS=',' read -r -a routes <<<"${EXT_DN_ROUTE_LIST}"
  for route in "${routes[@]}"; do
    [[ -n "${route}" ]] || continue
    ip route replace "${route}" via "${UPF_N6_GW}" dev "${EXT_DN_IF}"
  done
}

routes_need_repair() {
  local route=""
  IFS=',' read -r -a routes <<<"${EXT_DN_ROUTE_LIST}"
  for route in "${routes[@]}"; do
    [[ -n "${route}" ]] || continue
    if ! ip route show "${route}" | grep -Fq "via ${UPF_N6_GW} dev ${EXT_DN_IF}"; then
      return 0
    fi
  done
  return 1
}

nonfatal_startup_probe() {
  if [[ "${EXT_DN_STARTUP_PROBE_ENABLED}" != "true" ]]; then
    log "Skipping non-fatal N6 startup probe because EXT_DN_STARTUP_PROBE_ENABLED=${EXT_DN_STARTUP_PROBE_ENABLED}."
    return 0
  fi

  log "Running non-fatal N6 startup probe to ${EXT_DN_STARTUP_PROBE_TARGET} via ${EXT_DN_IF} ..."
  if ping -I "${EXT_DN_IF}" -c "${EXT_DN_STARTUP_PROBE_COUNT}" -W "${EXT_DN_STARTUP_PROBE_TIMEOUT_SECONDS}" "${EXT_DN_STARTUP_PROBE_TARGET}"; then
    log "N6 startup probe succeeded."
    return 0
  fi

  log "WARN: N6 startup probe to ${EXT_DN_STARTUP_PROBE_TARGET} failed. Keeping the container alive for debugging."
  dump_debug
}

ensure_nat_rule
log "Waiting up to ${EXT_DN_WAIT_TIMEOUT_SECONDS}s for ${EXT_DN_IF} from OVS..."
if ! wait_for_interface; then
  log "ERROR: ${EXT_DN_IF} did not appear within ${EXT_DN_WAIT_TIMEOUT_SECONDS}s."
  dump_debug
  exit 1
fi

log "Detected ${EXT_DN_IF}; applying N6-side routes now."
ip -br link show "${EXT_DN_IF}" || true
ip -br addr show "${EXT_DN_IF}" || true

ensure_sysctls
configure_routes
log "Installed N6 routes via ${UPF_N6_GW} on ${EXT_DN_IF}."
ip route | grep -E '12\.1\.1\.0/24|12\.1\.2\.0/24|12\.1\.3\.0/24' || true
nonfatal_startup_probe

while true; do
  ensure_nat_rule
  if ip link show "${EXT_DN_IF}" >/dev/null 2>&1; then
    ensure_sysctls
    if routes_need_repair; then
      log "Detected missing N6 route state on ${EXT_DN_IF}; reinstalling routes."
      configure_routes
      ip route | grep -E '12\.1\.1\.0/24|12\.1\.2\.0/24|12\.1\.3\.0/24' || true
    fi
  else
    log "WARN: ${EXT_DN_IF} disappeared after startup; waiting for OVS to reconcile it again."
  fi
  sleep "${EXT_DN_RECONCILE_INTERVAL_SECONDS}"
done
