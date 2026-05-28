#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RESULTS_ROOT="${RESULTS_ROOT:-${TESTBED_DIR}/results_real}"
RUNS="${RUNS:-1}"
DURATION_SECONDS="${DURATION_SECONDS:-60}"
EXT_DN_IP="${EXT_DN_IP:-192.168.72.135}"
UE_ROUTE_CIDR="${UE_ROUTE_CIDR:-192.168.72.128/26}"
TRAFFIC_LOG_ROOT="${TRAFFIC_LOG_ROOT:-${TESTBED_DIR}/logs/traffic}"

OVS_CONTAINER="${OVS_CONTAINER:-ovs}"
ONOS_CONTAINER="${ONOS_CONTAINER:-onos}"
EXT_DN_CONTAINER="${EXT_DN_CONTAINER:-rfsim5g-oai-ext-dn}"
BRIDGE="${OVS_BRIDGE_NAME:-br-n6}"
UPF_PORT="${OVS_UPF_PORT_NAME:-v-upf-host}"
EDN_PORT="${OVS_EDN_PORT_NAME:-v-edn-host}"

METHODS=(fifo static_ovs static_slicing proposed_closed_loop)
LOADS=(load_30 load_60 load_80 load_95)
REQUIRED_CONTAINERS=(
  ovs
  onos
  rfsim5g-oai-upf
  rfsim5g-oai-ext-dn
  rfsim5g-oai-nr-ue
  rfsim5g-oai-nr-ue2
  rfsim5g-oai-nr-ue3
  rfsim5g-oai-nr-ue4
  rfsim5g-oai-nr-ue5
  rfsim5g-oai-nr-ue6
  rfsim5g-oai-nr-ue7
  rfsim5g-oai-nr-ue8
  rfsim5g-oai-nr-ue9
  rfsim5g-oai-nr-ue10
)

log() {
  printf '[results] %s\n' "$*"
}

warn() {
  printf '[results] WARN: %s\n' "$*" >&2
}

run_ok() {
  "$@" || warn "command failed: $*"
}

container_running() {
  docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true
}

ue_container_name() {
  local index="$1"
  if [[ "$index" == "1" ]]; then
    echo "rfsim5g-oai-nr-ue"
  else
    echo "rfsim5g-oai-nr-ue${index}"
  fi
}

check_required_containers() {
  local missing=0
  log "Checking required containers..."
  for name in "${REQUIRED_CONTAINERS[@]}"; do
    if container_running "$name"; then
      log "  OK ${name}"
    else
      warn "  missing or stopped: ${name}"
      missing=1
    fi
  done
  return "$missing"
}

ensure_ue_routes() {
  log "Ensuring UE routes to ${UE_ROUTE_CIDR} via oaitun_ue1 when available..."
  local ue
  for idx in $(seq 1 10); do
    ue="$(ue_container_name "$idx")"
    if ! container_running "$ue"; then
      warn "Skipping route setup for stopped container ${ue}"
      continue
    fi
    run_ok docker exec "$ue" bash -lc "ip link show oaitun_ue1 >/dev/null 2>&1 && ip route replace ${UE_ROUTE_CIDR} dev oaitun_ue1 || true"
  done
}

stop_closed_loop_processes() {
  log "Stopping any existing closed-loop helper processes..."
  pkill -f "${SCRIPT_DIR}/run_closed_loop.sh" >/dev/null 2>&1 || true
  pkill -f "${SCRIPT_DIR}/run_telemetry.sh" >/dev/null 2>&1 || true
  pkill -f "${SCRIPT_DIR}/run_policy_loop.sh" >/dev/null 2>&1 || true
  pkill -f "telemetry_main.py" >/dev/null 2>&1 || true
  pkill -f "policy_manager" >/dev/null 2>&1 || true
}

ovs_ofport() {
  docker exec "$OVS_CONTAINER" ovs-vsctl --if-exists get Interface "$1" ofport 2>/dev/null | tr -d '\r'
}

install_basic_forwarding() {
  local upf_ofport edn_ofport
  upf_ofport="$(ovs_ofport "$UPF_PORT")"
  edn_ofport="$(ovs_ofport "$EDN_PORT")"
  if [[ -z "$upf_ofport" || -z "$edn_ofport" || "$upf_ofport" == "-1" || "$edn_ofport" == "-1" ]]; then
    warn "Cannot install basic forwarding; missing ofports for ${UPF_PORT}/${EDN_PORT}"
    return 0
  fi
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=5000,ip,in_port=${upf_ofport},actions=output:${edn_ofport}"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=20000,ip,in_port=${edn_ofport},actions=output:${upf_ofport}"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=45000,arp,in_port=${upf_ofport},actions=output:${edn_ofport}"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=45000,arp,in_port=${edn_ofport},actions=output:${upf_ofport}"
}

remove_slice_flows() {
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 del-flows "$BRIDGE" "udp,tp_dst=5201"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 del-flows "$BRIDGE" "udp,tp_dst=5202"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 del-flows "$BRIDGE" "udp,tp_dst=5203"
}

clear_ovs_qos() {
  log "Removing OVS QoS and Queue objects without deleting ${BRIDGE}..."
  run_ok docker exec "$OVS_CONTAINER" ovs-vsctl --if-exists clear Port "$EDN_PORT" qos
  run_ok docker exec "$OVS_CONTAINER" bash -lc "for q in \$(ovs-vsctl --format=csv --data=bare --no-heading list Queue _uuid 2>/dev/null); do ovs-vsctl --if-exists destroy Queue \"\$q\"; done"
  run_ok docker exec "$OVS_CONTAINER" bash -lc "for q in \$(ovs-vsctl --format=csv --data=bare --no-heading list QoS _uuid 2>/dev/null); do ovs-vsctl --if-exists destroy QoS \"\$q\"; done"
}

configure_qos() {
  local q1_min="$1" q1_max="$2" q2_min="$3" q2_max="$4" q3_min="$5" q3_max="$6"
  clear_ovs_qos
  log "Configuring OVS queues on ${EDN_PORT}: q1=${q1_min}/${q1_max}, q2=${q2_min}/${q2_max}, q3=${q3_min}/${q3_max} bps"
  run_ok docker exec "$OVS_CONTAINER" ovs-vsctl \
    -- set Port "$EDN_PORT" qos=@qos \
    -- --id=@qos create QoS type=linux-htb other-config:max-rate=100000000 queues:1=@q1 queues:2=@q2 queues:3=@q3 \
    -- --id=@q1 create Queue other-config:min-rate="$q1_min" other-config:max-rate="$q1_max" \
    -- --id=@q2 create Queue other-config:min-rate="$q2_min" other-config:max-rate="$q2_max" \
    -- --id=@q3 create Queue other-config:min-rate="$q3_min" other-config:max-rate="$q3_max"
}

install_queue_flows() {
  local upf_ofport edn_ofport
  upf_ofport="$(ovs_ofport "$UPF_PORT")"
  edn_ofport="$(ovs_ofport "$EDN_PORT")"
  if [[ -z "$upf_ofport" || -z "$edn_ofport" || "$upf_ofport" == "-1" || "$edn_ofport" == "-1" ]]; then
    warn "Cannot install queue flows; missing ofports for ${UPF_PORT}/${EDN_PORT}"
    return 0
  fi
  remove_slice_flows
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=40000,udp,in_port=${upf_ofport},tp_dst=5201,actions=set_queue:1,output:${edn_ofport}"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=50000,udp,in_port=${upf_ofport},tp_dst=5202,actions=set_queue:2,output:${edn_ofport}"
  run_ok docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 add-flow "$BRIDGE" "priority=30000,udp,in_port=${upf_ofport},tp_dst=5203,actions=set_queue:3,output:${edn_ofport}"
  install_basic_forwarding
}

configure_method() {
  local method="$1"
  case "$method" in
    fifo)
      stop_closed_loop_processes
      remove_slice_flows
      clear_ovs_qos
      install_basic_forwarding
      ;;
    static_ovs)
      stop_closed_loop_processes
      configure_qos 50000000 100000000 10000000 20000000 1000000 5000000
      install_queue_flows
      ;;
    static_slicing)
      stop_closed_loop_processes
      configure_qos 70000000 70000000 20000000 20000000 10000000 10000000
      install_queue_flows
      ;;
    proposed_closed_loop)
      stop_closed_loop_processes
      remove_slice_flows
      install_basic_forwarding
      ;;
    *)
      warn "Unknown method ${method}"
      ;;
  esac
}

start_closed_loop_for_run() {
  local run_dir="$1"
  printf '[results] %s\n' "Starting proposed closed loop in active mode for this run..." >&2
  TELEMETRY_CONFIG_PATH="${TESTBED_DIR}/telemetry/config.yaml" "${SCRIPT_DIR}/run_closed_loop.sh" --active >"${run_dir}/closed_loop.log" 2>&1 &
  echo "$!"
}

stop_pid() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

run_ping_probes() {
  local run_dir="$1"
  local ok=0

  if docker exec "$(ue_container_name 1)" bash -lc "ping -I oaitun_ue1 -c 3 -W 2 ${EXT_DN_IP}" >"${run_dir}/urllc_ue1_ping.log" 2>&1; then
    ok=1
  else
    warn "UE1 ping probe failed for ${run_dir}"
  fi

  if docker exec "$(ue_container_name 2)" bash -lc "ping -I oaitun_ue1 -c 3 -W 2 ${EXT_DN_IP}" >"${run_dir}/urllc_ue2_ping.log" 2>&1; then
    ok=1
  else
    warn "UE2 ping probe failed for ${run_dir}"
  fi

  return "$((1 - ok))"
}

run_framework_traffic() {
  local run_dir="$1" load_name="$2"
  mkdir -p "$TRAFFIC_LOG_ROOT"
  log "Running framework traffic generator for ${load_name}; primary traffic source is automation/run_all_traffic.sh"
  (
    cd "$TESTBED_DIR" || exit 1
    DURATION_SECONDS="$DURATION_SECONDS" OUTPUT_ROOT="$TRAFFIC_LOG_ROOT" bash "${SCRIPT_DIR}/run_all_traffic.sh" --duration "$DURATION_SECONDS"
  ) >"${run_dir}/run_all_traffic.log" 2>&1 || warn "automation/run_all_traffic.sh reported a failure; continuing with available telemetry and OVS data"
}

collect_snapshots() {
  local run_dir="$1"
  log "Collecting metrics and OVS snapshots into ${run_dir}"
  curl -s http://localhost:8000/metrics >"${run_dir}/prometheus_metrics_snapshot.txt" 2>"${run_dir}/prometheus_metrics_snapshot.err" || true
  if ! curl -s http://localhost:8001/metrics >"${run_dir}/policy_metrics_snapshot.txt" 2>"${run_dir}/policy_metrics_snapshot.err"; then
    echo "policy metrics endpoint unavailable for this run" >"${run_dir}/policy_metrics_snapshot.txt"
  fi
  docker exec "$OVS_CONTAINER" ovs-ofctl -O OpenFlow13 dump-flows "$BRIDGE" >"${run_dir}/ovs_flows.log" 2>&1 || true
  docker exec "$OVS_CONTAINER" ovs-vsctl list queue >"${run_dir}/ovs_queues.log" 2>&1 || true
  docker exec "$OVS_CONTAINER" ovs-vsctl show >"${run_dir}/ovs_bridge_show.log" 2>&1 || true
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' >"${run_dir}/docker_status.log" 2>&1 || true

  mkdir -p "${run_dir}/traffic_logs" "${run_dir}/telemetry_jsonl" "${run_dir}/policy_jsonl"
  if [[ -d "$TRAFFIC_LOG_ROOT" ]]; then
    cp -a "${TRAFFIC_LOG_ROOT}/." "${run_dir}/traffic_logs/" 2>/dev/null || true
  fi
  find "${TESTBED_DIR}/logs/telemetry" -maxdepth 1 -type f -name '*.jsonl' -exec cp -f {} "${run_dir}/telemetry_jsonl/" \; 2>/dev/null || true
  find "${TESTBED_DIR}/logs/policy" -maxdepth 1 -type f -name '*.jsonl' -exec cp -f {} "${run_dir}/policy_jsonl/" \; 2>/dev/null || true
}

write_filtered_metrics() {
  local run_dir="$1"
  {
    echo "# Filtered paper metrics"
    echo "# Source: telemetry and policy Prometheus snapshots plus OVS queue stats."
    grep -E 'urllc_latency_avg_ms|urllc_jitter_ms|embb_throughput_bps|mmtc_delivery_ratio_percent|ovs_queue_packets_total|ovs_queue_bytes_total' "${run_dir}/prometheus_metrics_snapshot.txt" 2>/dev/null || true
    grep -Ei 'policy|decision|slice|queue|latency|jitter|throughput|delivery' "${run_dir}/policy_metrics_snapshot.txt" 2>/dev/null || true
  } >"${run_dir}/filtered_metrics.txt"
}

warn_on_suspicious_metrics() {
  local run_dir="$1"
  if awk 'BEGIN {found=0; bad=0} /^#/ {next} index($1, "embb_throughput_bps") == 1 {found=1; if ($2 == 0 || $2 == "0.0") bad=1} END {exit !(found && bad)}' "${run_dir}/prometheus_metrics_snapshot.txt" 2>/dev/null; then
    warn "embb_throughput_bps is 0 in ${run_dir}; keeping run but check traffic/telemetry"
    echo "WARNING: embb_throughput_bps is 0" >>"${run_dir}/filtered_metrics.txt"
  fi
  if awk 'BEGIN {found=0; bad=0} /^#/ {next} index($1, "mmtc_delivery_ratio_percent") == 1 {found=1; if ($2 == "NaN" || $2 == "nan") bad=1} END {exit !(found && bad)}' "${run_dir}/prometheus_metrics_snapshot.txt" 2>/dev/null; then
    warn "mmtc_delivery_ratio_percent is NaN in ${run_dir}; keeping run but check sensor traffic/telemetry"
    echo "WARNING: mmtc_delivery_ratio_percent is NaN" >>"${run_dir}/filtered_metrics.txt"
  fi
}

file_has_content() {
  [[ -s "$1" ]]
}

determine_run_status() {
  local run_dir="$1" ping_ok="$2"
  local ovs_ok=0 telemetry_ok=0
  if file_has_content "${run_dir}/ovs_flows.log" && grep -q "OFPST_FLOW" "${run_dir}/ovs_flows.log"; then
    ovs_ok=1
  fi
  if file_has_content "${run_dir}/prometheus_metrics_snapshot.txt"; then
    telemetry_ok=1
  fi

  if [[ "$ping_ok" == "1" && "$ovs_ok" == "1" && "$telemetry_ok" == "1" ]]; then
    echo "completed"
  else
    echo "partial"
  fi
}

write_summary() {
  local run_dir="$1" method="$2" load_name="$3" run="$4" status="$5" ping_ok="${6:-unknown}"
  {
    echo "method=${method}"
    echo "load=${load_name}"
    echo "run=${run}"
    echo "duration_seconds=${DURATION_SECONDS}"
    echo "status=${status}"
    echo "completion_rule=completed requires UE ping, OVS flow capture, and telemetry metrics capture"
    echo "ue_ping_ok=${ping_ok}"
    echo "ext_dn_ip=${EXT_DN_IP}"
    echo "traffic_log_root=${TRAFFIC_LOG_ROOT}"
    echo "timestamp=$(date -Is)"
    echo
    echo "Industrial UE mapping:"
    echo "UE1 robotic arm control; UE2 AGV/mobile robot control; UE3 camera 1; UE4 camera 2; UE5 AR/VR support; UE6 3D printer monitoring; UE7 temperature sensor; UE8 vibration sensor; UE9 energy/machine-health telemetry; UE10 inventory/environmental sensor."
    echo
    echo "Key files:"
    echo "urllc_ue1_ping.log"
    echo "urllc_ue2_ping.log"
    echo "run_all_traffic.log"
    echo "traffic_logs/"
    echo "prometheus_metrics_snapshot.txt"
    echo "policy_metrics_snapshot.txt"
    echo "filtered_metrics.txt"
    echo "ovs_flows.log"
    echo "ovs_queues.log"
    echo "ovs_bridge_show.log"
    echo "docker_status.log"
    echo "telemetry_jsonl/"
    echo "policy_jsonl/"
  } >"${run_dir}/run_summary.txt"
}

run_one_experiment() {
  local method="$1" load_name="$2" run="$3"
  local run_dir="${RESULTS_ROOT}/${method}/${load_name}/run_${run}"
  local closed_loop_pid=""
  local ping_ok=0 status="partial"
  mkdir -p "$run_dir"
  write_summary "$run_dir" "$method" "$load_name" "$run" "started" "unknown"

  log "=== ${method} ${load_name} run_${run} ==="
  configure_method "$method"
  if [[ "$method" == "proposed_closed_loop" ]]; then
    closed_loop_pid="$(start_closed_loop_for_run "$run_dir")"
    sleep 5
  fi

  if run_ping_probes "$run_dir"; then
    ping_ok=1
  fi
  run_framework_traffic "$run_dir" "$load_name"
  collect_snapshots "$run_dir"
  write_filtered_metrics "$run_dir"
  warn_on_suspicious_metrics "$run_dir"
  status="$(determine_run_status "$run_dir" "$ping_ok")"
  stop_pid "$closed_loop_pid"
  write_summary "$run_dir" "$method" "$load_name" "$run" "$status" "$ping_ok"
}

main() {
  cd "$TESTBED_DIR" || exit 1
  mkdir -p "$RESULTS_ROOT"
  log "Results root: ${RESULTS_ROOT}"
  log "RUNS=${RUNS} DURATION_SECONDS=${DURATION_SECONDS}"

  if ! check_required_containers; then
    warn "Some required containers are not running. Continuing so partial readiness/results can still be collected."
  fi
  ensure_ue_routes

  local method load run
  for method in "${METHODS[@]}"; do
    for load in "${LOADS[@]}"; do
      for run in $(seq 1 "$RUNS"); do
        run_one_experiment "$method" "$load" "$run" || warn "Experiment failed: ${method} ${load} run_${run}"
      done
    done
  done

  stop_closed_loop_processes
  log "All experiment attempts finished. Results are under ${RESULTS_ROOT}"
}

main "$@"
