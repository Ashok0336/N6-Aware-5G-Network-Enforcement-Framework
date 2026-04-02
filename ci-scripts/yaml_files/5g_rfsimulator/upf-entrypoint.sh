#!/usr/bin/env bash
set -euo pipefail

UPF_CONFIG_PATH="${UPF_CONFIG_PATH:-/openair-upf/etc/config.yaml}"
UPF_N6_IF="${UPF_N6_IF:-n6ovs0}"
UPF_WAIT_TIMEOUT_SECONDS="${UPF_WAIT_TIMEOUT_SECONDS:-120}"

log() {
  echo "[upf-entrypoint] $*"
}

dump_debug() {
  log "Interface snapshot:"
  ip -br link || true
  log "Address snapshot:"
  ip -br addr || true
  log "Route snapshot:"
  ip route || true
}

wait_for_interface() {
  local deadline=$((SECONDS + UPF_WAIT_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if ip link show "${UPF_N6_IF}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

ensure_sysctls() {
  sysctl -qw net.ipv4.conf.all.rp_filter=0 || true
  sysctl -qw net.ipv4.conf.default.rp_filter=0 || true
  sysctl -qw "net.ipv4.conf.${UPF_N6_IF}.rp_filter=0" || true
}

log "Waiting up to ${UPF_WAIT_TIMEOUT_SECONDS}s for ${UPF_N6_IF} from OVS..."
if ! wait_for_interface; then
  log "ERROR: ${UPF_N6_IF} did not appear within ${UPF_WAIT_TIMEOUT_SECONDS}s."
  dump_debug
  exit 1
fi

log "Detected ${UPF_N6_IF}:"
ip -br link show "${UPF_N6_IF}" || true
ip -br addr show "${UPF_N6_IF}" || true
ensure_sysctls

exec /openair-upf/bin/oai_upf -c "${UPF_CONFIG_PATH}"
