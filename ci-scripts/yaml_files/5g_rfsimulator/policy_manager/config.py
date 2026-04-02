from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from policy_manager.utils import load_structured_file, normalize_path


def load_policy_config(config_path: Path) -> Dict[str, Any]:
    raw_config = load_structured_file(config_path)
    cfg = dict(raw_config.get("policy_manager", {}))
    base_dir = config_path.parent

    onos_host = os.getenv("ONOS_HOST", "192.168.71.160")
    onos_rest_port = os.getenv("ONOS_REST_PORT", "8181")
    onos_base_url = os.getenv("ONOS_BASE_URL", f"http://{onos_host}:{onos_rest_port}")
    onos_auth_parts = (os.getenv("ONOS_AUTH", "onos:rocks").split(":", 1) + ["rocks"])[:2]
    onos_username = os.getenv("ONOS_USERNAME", onos_auth_parts[0])
    onos_password = os.getenv("ONOS_PASSWORD", onos_auth_parts[1])

    cfg.setdefault("poll_interval_seconds", 5)
    cfg.setdefault("decision_cooldown_seconds", 10)
    cfg.setdefault("dry_run_only", True)
    cfg.setdefault("telemetry_glob", "closed_loop_telemetry_*.jsonl")
    cfg.setdefault("telemetry_dir", "../logs/telemetry")
    cfg.setdefault("log_dir", "../logs/policy")
    cfg.setdefault("log_prefix", "closed_loop_policy")
    cfg.setdefault("ensure_onos_slice_flows", True)
    cfg.setdefault("force_onos_flow_refresh", False)

    onos_cfg = dict(cfg.get("onos", {}))
    onos_cfg.setdefault("base_url", onos_base_url)
    onos_cfg.setdefault("devices_path", "/onos/v1/devices")
    onos_cfg.setdefault("username", onos_username)
    onos_cfg.setdefault("password", onos_password)
    onos_cfg.setdefault("timeout_seconds", 5)
    onos_cfg.setdefault("upf_port_name", os.getenv("OVS_UPF_PORT_NAME", "v-upf-host"))
    onos_cfg.setdefault("edn_port_name", os.getenv("OVS_EDN_PORT_NAME", "v-edn-host"))
    cfg["onos"] = onos_cfg

    ovs_cfg = dict(cfg.get("ovs", {}))
    ovs_cfg.setdefault("container_name", os.getenv("OVS_CONTAINER_NAME", "ovs"))
    ovs_cfg.setdefault("bridge_name", os.getenv("OVS_BRIDGE_NAME", "br-n6"))
    ovs_cfg.setdefault("egress_port_name", os.getenv("OVS_EDN_PORT_NAME", "v-edn-host"))
    cfg["ovs"] = ovs_cfg

    cfg["telemetry_dir"] = str(normalize_path(base_dir, cfg["telemetry_dir"]))
    cfg["log_dir"] = str(normalize_path(base_dir, cfg["log_dir"]))
    cfg["poll_interval_seconds"] = float(cfg["poll_interval_seconds"])
    cfg["decision_cooldown_seconds"] = float(cfg["decision_cooldown_seconds"])
    cfg["dry_run_only"] = bool(cfg["dry_run_only"])
    cfg["ensure_onos_slice_flows"] = bool(cfg["ensure_onos_slice_flows"])
    cfg["force_onos_flow_refresh"] = bool(cfg["force_onos_flow_refresh"])

    queue_profiles = dict(cfg.get("queue_profiles", {}))
    cfg["queue_profiles"] = queue_profiles
    slices = dict(cfg.get("slices", {}))
    normalized_slices: Dict[str, Any] = {}
    for slice_name, slice_cfg in slices.items():
        entry = dict(slice_cfg)
        entry.setdefault("display_name", slice_name.upper())
        entry.setdefault("action_name", f"BOOST_{slice_name.upper()}")
        entry.setdefault("sla", {})
        if "udp_port" in entry:
            entry["udp_port"] = int(entry["udp_port"])
        if "queue_id" in entry:
            entry["queue_id"] = int(entry["queue_id"])
        entry["sla"] = {
            key: float(value) if isinstance(value, (int, float)) or str(value).replace(".", "", 1).isdigit() else value
            for key, value in dict(entry["sla"]).items()
        }
        normalized_slices[str(slice_name)] = entry
    cfg["slices"] = normalized_slices
    return cfg
