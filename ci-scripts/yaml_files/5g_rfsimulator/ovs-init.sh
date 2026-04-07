#!/usr/bin/env bash
set -euo pipefail

UPF_CONT="${UPF_CONT:-${UPF_CONTAINER_NAME:-rfsim5g-oai-upf}}"
EDN_CONT="${EDN_CONT:-${EXT_DN_CONTAINER_NAME:-rfsim5g-oai-ext-dn}}"
UPF_N6_IF="${UPF_N6_IF:-n6ovs0}"
EDN_IF="${EDN_IF:-dn0}"
UPF_N6_IP="${UPF_N6_IP:-192.168.72.134/26}"
EDN_IP="${EDN_IP:-192.168.72.135/26}"
ONOS_CTRL="${ONOS_CTRL:-${ONOS_HOST:-192.168.71.160}:6653}"
ONOS_CTRL_TIMEOUT_SECONDS="${ONOS_CTRL_TIMEOUT_SECONDS:-30}"
N6_ARP_FORWARD_PRIORITY="${N6_ARP_FORWARD_PRIORITY:-45000}"
N6_BASE_FORWARD_PRIORITY="${N6_BASE_FORWARD_PRIORITY:-5000}"
BOOTSTRAP_FLOW_COOKIE="${BOOTSTRAP_FLOW_COOKIE:-0x6e360001}"

BR="${BR:-${OVS_BRIDGE_NAME:-br-n6}}"
V_UPF_HOST="${V_UPF_HOST:-${OVS_UPF_PORT_NAME:-v-upf-host}}"
V_UPF_CONT="${V_UPF_CONT:-v-upf}"
V_EDN_HOST="${V_EDN_HOST:-${OVS_EDN_PORT_NAME:-v-edn-host}}"
V_EDN_CONT="${V_EDN_CONT:-v-edn}"

log() {
  echo "[ovs-init] $*"
}

warn() {
  echo "[ovs-init] WARN: $*" >&2
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[ovs-init] ERROR: required command '$1' not found" >&2
    exit 1
  }
}

in_ns() {
  nsenter -t "$1" -n -- bash -lc "$2"
}

container_pid() {
  docker inspect -f '{{if .State.Running}}{{.State.Pid}}{{else}}0{{end}}' "$1" 2>/dev/null || echo 0
}

wait_pid() {
  local container_name="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  local pid=""

  while (( SECONDS < deadline )); do
    pid="$(container_pid "$container_name")"
    if [[ -n "$pid" && "$pid" != "0" ]]; then
      echo "$pid"
      return 0
    fi
    log "waiting for running container ${container_name} ..."
    sleep 1
  done

  echo "[ovs-init] ERROR: container ${container_name} is not running after ${timeout_seconds}s" >&2
  return 1
}

host_if_exists() {
  ip link show "$1" >/dev/null 2>&1
}

ns_if_exists() {
  nsenter -t "$1" -n -- ip link show "$2" >/dev/null 2>&1
}

port_on_bridge() {
  ovs-vsctl list-ports "$BR" 2>/dev/null | grep -qx "$1"
}

show_bridge_snapshot() {
  log "OVS bridge snapshot:"
  ovs-vsctl show || true
  ovs-vsctl get-controller "$BR" 2>/dev/null || true
  ovs-ofctl -O OpenFlow13 show "$BR" 2>/dev/null || true
  ovs-ofctl -O OpenFlow13 dump-flows "$BR" 2>/dev/null || true
  ip -br link 2>/dev/null | grep -E "^(${BR}|${V_UPF_HOST}|${V_EDN_HOST})" || true
}

prepare_bridge() {
  ovs-vsctl --may-exist add-br "$BR"
  ovs-vsctl set bridge "$BR" datapath_type=netdev
  ovs-vsctl set bridge "$BR" protocols=OpenFlow13
  ovs-vsctl set-fail-mode "$BR" secure
  ovs-vsctl set-controller "$BR" "tcp:${ONOS_CTRL}"
  ip link set "$BR" up || true
}

recreate_attachment() {
  local pid="$1"
  local desired_if="$2"
  local address_cidr="$3"
  local host_if="$4"
  local peer_if="$5"
  local role="$6"

  log "Re-attaching ${role}: ${host_if} <-> ${desired_if}"
  ip link del "$host_if" 2>/dev/null || true
  ip link del "$peer_if" 2>/dev/null || true
  in_ns "$pid" "ip link del ${desired_if} 2>/dev/null || true"

  ip link add "$host_if" type veth peer name "$peer_if"
  ip link set "$peer_if" netns "$pid"

  in_ns "$pid" "
    set -euo pipefail
    ip link set ${peer_if} name ${desired_if}
    ip addr flush dev ${desired_if} || true
    ip addr add ${address_cidr} dev ${desired_if}
    ip link set ${desired_if} up
    sysctl -qw net.ipv4.conf.all.rp_filter=0 || true
    sysctl -qw net.ipv4.conf.default.rp_filter=0 || true
    sysctl -qw net.ipv4.conf.${desired_if}.rp_filter=0 || true
  "

  ip link set "$host_if" up
  ovs-vsctl --if-exists del-port "$BR" "$host_if"
  ovs-vsctl --may-exist add-port "$BR" "$host_if"
}

ensure_attachment() {
  local pid="$1"
  local desired_if="$2"
  local address_cidr="$3"
  local host_if="$4"
  local peer_if="$5"
  local role="$6"

  if ns_if_exists "$pid" "$desired_if" && host_if_exists "$host_if" && port_on_bridge "$host_if"; then
    in_ns "$pid" "
      set -euo pipefail
      sysctl -qw net.ipv4.conf.all.rp_filter=0 || true
      sysctl -qw net.ipv4.conf.default.rp_filter=0 || true
      sysctl -qw net.ipv4.conf.${desired_if}.rp_filter=0 || true
    "
    ip link set "$host_if" up || true
    return 0
  fi

  recreate_attachment "$pid" "$desired_if" "$address_cidr" "$host_if" "$peer_if" "$role"
}

wait_for_ofport() {
  local port_name="$1"
  local deadline=$((SECONDS + 15))
  local ofport=""

  while (( SECONDS < deadline )); do
    ofport="$(ovs-vsctl get Interface "$port_name" ofport 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ "$ofport" =~ ^[0-9]+$ ]] && (( ofport > 0 )); then
      echo "$ofport"
      return 0
    fi
    sleep 1
  done

  echo "[ovs-init] ERROR: could not resolve OpenFlow port for ${port_name}" >&2
  return 1
}

flow_has_match_tokens() {
  local line="$1"
  shift
  local token=""

  for token in "$@"; do
    [[ "${line}" == *"${token}"* ]] || return 1
  done

  return 0
}

flow_exists() {
  local priority="$1"
  local match="$2"
  local actions="$3"
  local flow_dump=""
  local line=""
  local match_tokens=()

  flow_dump="$(ovs-ofctl -O OpenFlow13 dump-flows "$BR" 2>/dev/null || true)"
  IFS=',' read -r -a match_tokens <<<"${match}"

  while IFS= read -r line; do
    [[ "${line}" == *"priority=${priority}"* ]] || continue
    [[ "${line}" == *"actions=${actions}"* ]] || continue
    flow_has_match_tokens "${line}" "${match_tokens[@]}" || continue
    return 0
  done <<<"${flow_dump}"

  return 1
}

ensure_flow() {
  local priority="$1"
  local match="$2"
  local actions="$3"

  if flow_exists "${priority}" "${match}" "${actions}"; then
    return 0
  fi

  ovs-ofctl -O OpenFlow13 add-flow "$BR" \
    "cookie=${BOOTSTRAP_FLOW_COOKIE},priority=${priority},${match},actions=${actions}"
}

ensure_bootstrap_forwarding() {
  local upf_ofport
  local edn_ofport

  upf_ofport="$(wait_for_ofport "$V_UPF_HOST")"
  edn_ofport="$(wait_for_ofport "$V_EDN_HOST")"

  # Keep LLDP/BDDP controller discovery intact, but override the default ARP punt
  # with direct two-port ARP forwarding so N6 neighbors can resolve after a clean boot.
  ensure_flow "${N6_ARP_FORWARD_PRIORITY}" "in_port=${upf_ofport},arp" "output:${edn_ofport}"
  ensure_flow "${N6_ARP_FORWARD_PRIORITY}" "in_port=${edn_ofport},arp" "output:${upf_ofport}"

  # Provide deterministic bootstrap IPv4 forwarding in both directions.
  # ONOS-installed slice queue flows use higher priorities and override these
  # generic rules when policy is present.
  ensure_flow "${N6_BASE_FORWARD_PRIORITY}" "in_port=${upf_ofport},ip" "output:${edn_ofport}"
  ensure_flow "${N6_BASE_FORWARD_PRIORITY}" "in_port=${edn_ofport},ip" "output:${upf_ofport}"

  log "Ensured bootstrap N6 forwarding flows without resetting matching counters."
}

ensure_qos() {
  local existing_qos=""
  existing_qos="$(ovs-vsctl --if-exists get port "$V_EDN_HOST" qos 2>/dev/null | tr -d '[:space:]' || true)"
  if [[ -n "$existing_qos" && "$existing_qos" != "[]" ]]; then
    return 0
  fi

  ovs-vsctl --if-exists clear port "$V_EDN_HOST" qos
  ovs-vsctl -- set port "$V_EDN_HOST" qos=@newqos \
    -- --id=@newqos create qos type=linux-htb other-config:max-rate=120000000 \
       queues:1=@q1 queues:2=@q2 queues:3=@q3 \
    -- --id=@q1 create queue other-config:min-rate=50000000 other-config:max-rate=100000000 \
    -- --id=@q2 create queue other-config:min-rate=10000000 other-config:max-rate=20000000 \
    -- --id=@q3 create queue other-config:min-rate=1000000 other-config:max-rate=5000000
}

controller_connected() {
  ovs-vsctl list controller 2>/dev/null | grep -q "is_connected[[:space:]]*:[[:space:]]*true"
}

wait_for_controller() {
  local deadline=$((SECONDS + ONOS_CTRL_TIMEOUT_SECONDS))
  local stable=0

  while (( SECONDS < deadline )); do
    if controller_connected; then
      stable=$((stable + 1))
      if (( stable >= 2 )); then
        log "Controller connected to ${ONOS_CTRL}."
        return 0
      fi
    else
      stable=0
    fi
    sleep 1
  done

  warn "controller ${ONOS_CTRL} is not connected yet."
  show_bridge_snapshot
  return 1
}

main() {
  need_cmd docker
  need_cmd nsenter
  need_cmd ovs-vsctl
  need_cmd ovs-ofctl
  need_cmd ip

  log "Reconciling N6 bridge ${BR} with controller ${ONOS_CTRL}"
  prepare_bridge

  local upf_pid
  local edn_pid
  upf_pid="$(wait_pid "$UPF_CONT")"
  edn_pid="$(wait_pid "$EDN_CONT")"

  ensure_attachment "$upf_pid" "$UPF_N6_IF" "$UPF_N6_IP" "$V_UPF_HOST" "$V_UPF_CONT" "UPF"
  ensure_attachment "$edn_pid" "$EDN_IF" "$EDN_IP" "$V_EDN_HOST" "$V_EDN_CONT" "Ext-DN"
  ensure_qos
  ensure_bootstrap_forwarding
  show_bridge_snapshot
  wait_for_controller
}

main "$@"
