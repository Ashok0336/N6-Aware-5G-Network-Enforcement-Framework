#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SERVICE_MAPPING_PATH="${SCRIPT_DIR}/service_mapping.yaml"

ensure_tool_in_container() {
  local container_name="$1"
  local binary_name="$2"
  local package_name="$3"
  if docker exec "$container_name" bash -lc "command -v ${binary_name} >/dev/null 2>&1"; then
    return 0
  fi
  docker exec "$container_name" bash -lc \
    "apt-get update >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get install -y ${package_name} >/dev/null"
}

get_service_plan_lines() {
  local service_class="$1"
  local service_mapping_path="${2:-$DEFAULT_SERVICE_MAPPING_PATH}"
  python3 - "$SCRIPT_DIR" "$service_mapping_path" "$service_class" <<'PY'
import pathlib
import sys

script_dir = pathlib.Path(sys.argv[1]).resolve()
mapping_path = pathlib.Path(sys.argv[2]).resolve()
service_class = sys.argv[3]
sys.path.insert(0, str(script_dir))

from service_mapping_utils import normalize_service_mapping

mapping = normalize_service_mapping(mapping_path)
service = mapping["service_classes"][service_class]
defaults = mapping.get("defaults", {})
profile = service.get("traffic_profile", {})

print("EXT_DN_CONTAINER\t" + str(defaults.get("ext_dn_container", "rfsim5g-oai-ext-dn")))
print("TARGET_IP\t" + str(service.get("target_ip") or defaults.get("target_ip") or "192.168.72.135"))
ports = service.get("target_ports") or []
print("TARGET_PORT\t" + (str(ports[0]) if ports else ""))
for key in (
    "ping_interval_seconds",
    "udp_payload_bytes",
    "udp_packets_per_burst",
    "udp_burst_interval_seconds",
):
    print(f"{key.upper()}\t{profile.get(key, '')}")
for binding in service.get("ue_bindings", []):
    auxiliary = ",".join(binding.get("auxiliary_log_file_names", []))
    print(
        "\t".join(
            [
                "UE_BINDING",
                str(binding.get("container_name", "")),
                str(binding.get("ue_label", "")),
                str(binding.get("log_file_name", "")),
                auxiliary,
            ]
        )
    )
PY
}

format_ue_binding() {
  local container_name="$1"
  local ue_label="$2"
  local log_file_name="$3"
  local auxiliary="${4-}"
  printf '%s\t%s\t%s\t%s' "$container_name" "$ue_label" "$log_file_name" "$auxiliary"
}

parse_ue_binding() {
  local binding="$1"
  local -n container_name_ref="$2"
  local -n ue_label_ref="$3"
  local -n log_file_name_ref="$4"
  local -n auxiliary_ref="$5"
  IFS=$'\t' read -r container_name_ref ue_label_ref log_file_name_ref auxiliary_ref <<<"$binding"
}

get_ue_tunnel_ip() {
  local container_name="$1"
  docker exec "$container_name" bash -lc \
    "ip -4 addr show oaitun_ue1 2>/dev/null | awk '/inet / {print \$2}' | cut -d/ -f1 | head -n1"
}

get_udp_sink_plan_lines() {
  local service_mapping_path="${1:-$DEFAULT_SERVICE_MAPPING_PATH}"
  python3 - "$SCRIPT_DIR" "$service_mapping_path" <<'PY'
import pathlib
import sys

script_dir = pathlib.Path(sys.argv[1]).resolve()
mapping_path = pathlib.Path(sys.argv[2]).resolve()
sys.path.insert(0, str(script_dir))

from service_mapping_utils import normalize_service_mapping

mapping = normalize_service_mapping(mapping_path)
defaults = mapping.get("defaults", {})
print("EXT_DN_CONTAINER\t" + str(defaults.get("ext_dn_container", "rfsim5g-oai-ext-dn")))
ports = []
for service in mapping.get("service_classes", {}).values():
    for port in service.get("target_ports", []):
        ports.append(int(port))
for port in sorted(set(ports)):
    print("TARGET_PORT\t" + str(port))
PY
}

ensure_ext_dn_udp_sinks() {
  local service_mapping_path="${1:-$DEFAULT_SERVICE_MAPPING_PATH}"
  local ext_dn_container=""
  local ports=()
  while IFS=$'\t' read -r key value; do
    case "$key" in
      EXT_DN_CONTAINER)
        ext_dn_container="$value"
        ;;
      TARGET_PORT)
        ports+=("$value")
        ;;
    esac
  done < <(get_udp_sink_plan_lines "$service_mapping_path")

  if [[ -z "$ext_dn_container" ]]; then
    echo "[traffic] ERROR: Could not resolve Ext-DN container from service mapping." >&2
    return 1
  fi

  ensure_tool_in_container "$ext_dn_container" nc netcat-openbsd
  for port in "${ports[@]}"; do
    docker exec "$ext_dn_container" bash -lc \
      "current_shell_pid=\$\$; current_parent_pid=\${PPID:-0}; for pid in \$(pgrep -f 'nc -u -l -k ${port}' || true); do if [[ \"\$pid\" == \"\$current_shell_pid\" || \"\$pid\" == \"\$current_parent_pid\" ]]; then continue; fi; kill \"\$pid\" 2>/dev/null || true; done; nohup sh -lc 'while true; do nc -u -l -k ${port} >/dev/null 2>&1; done' >/tmp/udp-sink-${port}.log 2>&1 &"
  done
  echo "[traffic] Ext-DN UDP sinks are ready in ${ext_dn_container} on ports: ${ports[*]}"
}

run_udp_sender() {
  local container_name="$1"
  local service_class="$2"
  local traffic_mode="$3"
  local target_ip="$4"
  local target_port="$5"
  local duration_seconds="$6"
  local payload_bytes="$7"
  local packets_per_burst="$8"
  local burst_interval_seconds="$9"
  local source_ip="${10}"

  docker exec "$container_name" bash -lc "
set -euo pipefail
target_ip='${target_ip}'
target_port='${target_port}'
duration_seconds='${duration_seconds}'
payload_bytes='${payload_bytes}'
packets_per_burst='${packets_per_burst}'
burst_interval_seconds='${burst_interval_seconds}'
service_class='${service_class}'
traffic_mode='${traffic_mode}'
source_ip='${source_ip}'
if [[ -z \"\${source_ip}\" ]]; then
  echo \"ERROR: missing oaitun_ue1\" >&2
  exit 1
fi
echo \"START type=udp_sender service_class=\${service_class} mode=\${traffic_mode} tunnel_interface=oaitun_ue1 source_ip=\${source_ip} payload_bytes=\${payload_bytes} target_ip=\${target_ip} target_port=\${target_port} burst_packets=\${packets_per_burst} burst_interval_seconds=\${burst_interval_seconds} duration_seconds=\${duration_seconds}\"
python3 - \"\${source_ip}\" \"\${target_ip}\" \"\${target_port}\" \"\${duration_seconds}\" \"\${payload_bytes}\" \"\${packets_per_burst}\" \"\${burst_interval_seconds}\" \"\${service_class}\" \"\${traffic_mode}\" <<'PY'
import socket
import sys
import time
from datetime import datetime, timezone

source_ip = sys.argv[1]
target_ip = sys.argv[2]
target_port = int(sys.argv[3])
duration_seconds = float(sys.argv[4])
payload_bytes = int(sys.argv[5])
packets_per_burst = int(sys.argv[6])
burst_interval_seconds = float(sys.argv[7])
service_class = sys.argv[8]
traffic_mode = sys.argv[9]

payload = b'x' * payload_bytes
packets_sent = 0
bytes_sent = 0
start_time = time.monotonic()
end_time = time.monotonic() + max(0.0, duration_seconds)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((source_ip, 0))
try:
    while time.monotonic() < end_time:
        for _ in range(max(1, packets_per_burst)):
            sock.sendto(payload, (target_ip, target_port))
            packets_sent += 1
            bytes_sent += payload_bytes
        elapsed = time.monotonic() - start_time
        timestamp = datetime.now(timezone.utc).isoformat(timespec=\"milliseconds\").replace(\"+00:00\", \"Z\")
        print(
            f\"timestamp={timestamp}\"
            f\" elapsed_seconds={elapsed:.6f}\"
            f\" sent_packets={packets_sent}\"
            f\" bytes_sent={bytes_sent}\",
            flush=True,
        )
        time.sleep(max(0.0, burst_interval_seconds))
finally:
    sock.close()

duration_for_rate = duration_seconds if duration_seconds > 0 else 1.0
average_bitrate_bps = int(bytes_sent * 8 / duration_for_rate)
packet_rate_per_second = int(packets_sent / duration_for_rate)
print(
    \"SUMMARY type=udp_sender\"
    f\" service_class={service_class}\"
    f\" mode={traffic_mode}\"
    f\" packets_sent={packets_sent}\"
    f\" sent_packets={packets_sent}\"
    f\" bytes_sent={bytes_sent}\"
    f\" average_bitrate_bps={average_bitrate_bps}\"
    f\" packet_rate_per_second={packet_rate_per_second}\"
    f\" payload_bytes={payload_bytes}\"
    f\" source_ip={source_ip}\"
    f\" target_ip={target_ip}\"
    f\" target_port={target_port}\"
    f\" burst_packets={packets_per_burst}\"
    f\" burst_interval_seconds={burst_interval_seconds}\"
    f\" duration_seconds={duration_seconds}\",
    flush=True,
)
PY
"
}
