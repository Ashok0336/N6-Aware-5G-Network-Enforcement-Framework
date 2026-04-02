#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SERVICE_MAPPING_PATH="${SCRIPT_DIR}/service_mapping.yaml"
DEFAULT_TELEMETRY_CONFIG_PATH="${SCRIPT_DIR}/config.yaml"
DEFAULT_POLICY_CONFIG_PATH="${SCRIPT_DIR}/policy_config.yaml"
DEFAULT_ENFORCEMENT_CONFIG_PATH="${SCRIPT_DIR}/enforcement_config.yaml"

prepare_results_dir() {
  local mode_name="$1"
  local root_dir="${2:-${SCRIPT_DIR}/../logs/experiments}"
  local stamp
  stamp="$(date +%Y%m%d_%H%M%S)"
  echo "${root_dir}/${mode_name}_${stamp}"
}

generate_runtime_configs() {
  local results_dir="$1"
  local mode_name="$2"
  local service_mapping_path="${3:-$DEFAULT_SERVICE_MAPPING_PATH}"
  local telemetry_template_path="${4:-$DEFAULT_TELEMETRY_CONFIG_PATH}"
  local policy_template_path="${5:-$DEFAULT_POLICY_CONFIG_PATH}"
  local enforcement_template_path="${6:-$DEFAULT_ENFORCEMENT_CONFIG_PATH}"

  python3 - "$SCRIPT_DIR" "$results_dir" "$mode_name" "$service_mapping_path" "$telemetry_template_path" "$policy_template_path" "$enforcement_template_path" <<'PY'
import json
import pathlib
import sys

script_dir = pathlib.Path(sys.argv[1]).resolve()
results_dir = pathlib.Path(sys.argv[2]).resolve()
mode_name = sys.argv[3]
service_mapping_path = pathlib.Path(sys.argv[4]).resolve()
telemetry_template_path = pathlib.Path(sys.argv[5]).resolve()
policy_template_path = pathlib.Path(sys.argv[6]).resolve()
enforcement_template_path = pathlib.Path(sys.argv[7]).resolve()
sys.path.insert(0, str(script_dir))

from service_mapping_utils import load_yaml_or_json, normalize_service_mapping

mapping = normalize_service_mapping(service_mapping_path)
runtime_dir = results_dir / "runtime_configs"
telemetry_dir = results_dir / "telemetry"
policy_dir = results_dir / "policy"
enforcement_dir = results_dir / "enforcement"
traffic_dir = results_dir / "traffic"
metadata_dir = results_dir / "metadata"
for directory in (runtime_dir, telemetry_dir, policy_dir, enforcement_dir, traffic_dir, metadata_dir):
    directory.mkdir(parents=True, exist_ok=True)

service_dirs = {
    service_name: str((traffic_dir / service_name).resolve())
    for service_name in mapping.get("service_classes", {})
}
for service_dir in service_dirs.values():
    pathlib.Path(service_dir).mkdir(parents=True, exist_ok=True)

telemetry_cfg = load_yaml_or_json(telemetry_template_path)
telemetry_cfg["service_mapping_path"] = str(service_mapping_path)
telemetry_cfg["log_dir"] = str(telemetry_dir)
telemetry_cfg["traffic_log_search_dirs"] = [str(traffic_dir)]
for service_name, service_cfg in telemetry_cfg.get("service_classes", {}).items():
    if isinstance(service_cfg, dict):
        service_cfg["log_search_dirs"] = [service_dirs.get(service_name, str(traffic_dir))]

policy_cfg = load_yaml_or_json(policy_template_path)
policy_cfg["service_mapping_path"] = str(service_mapping_path)
policy_cfg["telemetry_dir"] = str(telemetry_dir)
policy_cfg["log_dir"] = str(policy_dir)

enforcement_cfg = load_yaml_or_json(enforcement_template_path)
enforcement_cfg["service_mapping_path"] = str(service_mapping_path)
enforcement_cfg.setdefault("enforcement", {})
enforcement_cfg["enforcement"]["policy_log_dir"] = str(policy_dir)
enforcement_cfg["enforcement"]["log_dir"] = str(enforcement_dir)

telemetry_config_path = runtime_dir / "telemetry_config.json"
policy_config_path = runtime_dir / "policy_config.json"
enforcement_config_path = runtime_dir / "enforcement_config.json"
manifest_path = metadata_dir / "experiment_manifest.json"

telemetry_config_path.write_text(json.dumps(telemetry_cfg, indent=2, sort_keys=True) + "\n", encoding="utf-8")
policy_config_path.write_text(json.dumps(policy_cfg, indent=2, sort_keys=True) + "\n", encoding="utf-8")
enforcement_config_path.write_text(json.dumps(enforcement_cfg, indent=2, sort_keys=True) + "\n", encoding="utf-8")

manifest = {
    "mode": mode_name,
    "results_dir": str(results_dir),
    "traffic_dir": str(traffic_dir),
    "telemetry_dir": str(telemetry_dir),
    "policy_dir": str(policy_dir),
    "enforcement_dir": str(enforcement_dir),
    "runtime_config_dir": str(runtime_dir),
    "service_mapping_path": str(service_mapping_path),
    "service_directories": service_dirs,
    "service_classes": mapping.get("service_classes", {}),
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(f"RESULTS_DIR={results_dir}")
print(f"TRAFFIC_DIR={traffic_dir}")
print(f"TELEMETRY_DIR={telemetry_dir}")
print(f"POLICY_DIR={policy_dir}")
print(f"ENFORCEMENT_DIR={enforcement_dir}")
print(f"TELEMETRY_CONFIG={telemetry_config_path}")
print(f"POLICY_CONFIG={policy_config_path}")
print(f"ENFORCEMENT_CONFIG={enforcement_config_path}")
print(f"MANIFEST_PATH={manifest_path}")
for service_name, service_dir in sorted(service_dirs.items()):
    print(f"SERVICE_DIR__{service_name.upper()}={service_dir}")
PY
}

stop_background_processes() {
  local pids=()
  local pid=""
  for pid in "$@"; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done
  if [[ ${#pids[@]} -eq 0 ]]; then
    return 0
  fi
  kill "${pids[@]}" 2>/dev/null || true
  wait "${pids[@]}" 2>/dev/null || true
}
