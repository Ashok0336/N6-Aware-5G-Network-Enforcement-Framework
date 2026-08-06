from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from policy_manager.utils import coerce_bool, load_structured_file, normalize_path


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
    metrics_http_host = os.getenv("POLICY_MANAGER_METRICS_HOST", "").strip()
    metrics_http_port = os.getenv("POLICY_MANAGER_METRICS_PORT", "").strip()
    manufacturing_twin_enabled = os.getenv("MANUFACTURING_TWIN_ENABLED", "").strip()
    manufacturing_twin_latest_path = os.getenv("MANUFACTURING_TWIN_LATEST_PATH", "").strip()
    ccnc_disable_manufacturing_twin = os.getenv("CCNC_DISABLE_MANUFACTURING_TWIN", "").strip()
    risk_inference_enabled = os.getenv("DT_RISK_INFERENCE_ENABLED", "").strip()
    risk_prediction_path = os.getenv("DT_RISK_PREDICTION_PATH", "").strip()
    risk_max_age_seconds = os.getenv("DT_RISK_MAX_AGE_SECONDS", "").strip()
    risk_action_cooldown_seconds = os.getenv("DT_RISK_ACTION_COOLDOWN_SECONDS", "").strip()

    cfg.setdefault("poll_interval_seconds", 5)
    cfg.setdefault("decision_cooldown_seconds", 10)
    cfg.setdefault("dry_run_only", True)
    cfg.setdefault("metrics_http_host", "0.0.0.0")
    cfg.setdefault("metrics_http_port", 8001)
    cfg.setdefault("telemetry_glob", "closed_loop_telemetry_*.jsonl")
    cfg.setdefault("telemetry_dir", "../logs/telemetry")
    cfg.setdefault("log_dir", "../logs/policy")
    cfg.setdefault("log_prefix", "closed_loop_policy")
    cfg.setdefault("ensure_onos_slice_flows", True)
    cfg.setdefault("force_onos_flow_refresh", False)
    cfg.setdefault("manufacturing_twin_enabled", False)
    cfg.setdefault("manufacturing_twin_latest_path", "../logs/manufacturing_twin/latest_machine_twin_state.json")
    cfg.setdefault("manufacturing_twin_max_age_seconds", 10)
    cfg.setdefault("risk_inference_enabled", False)
    cfg.setdefault("risk_prediction_path", "../logs/risk_inference/latest_risk_prediction.json")
    cfg.setdefault("risk_max_age_seconds", 10)
    cfg.setdefault("risk_low_action", "MAINTAIN_CURRENT_POLICY")
    cfg.setdefault("risk_medium_action", "VERIFY_QUEUE_RULES")
    cfg.setdefault("risk_high_action", "VERIFY_OR_REINSTALL_QUEUE_RULES")
    cfg.setdefault("risk_action_cooldown_seconds", 10)
    if metrics_http_host:
        cfg["metrics_http_host"] = metrics_http_host
    if metrics_http_port:
        cfg["metrics_http_port"] = metrics_http_port
    if manufacturing_twin_enabled:
        cfg["manufacturing_twin_enabled"] = manufacturing_twin_enabled
    if manufacturing_twin_latest_path:
        cfg["manufacturing_twin_latest_path"] = manufacturing_twin_latest_path
    if ccnc_disable_manufacturing_twin and coerce_bool(
        ccnc_disable_manufacturing_twin,
        field_name="CCNC_DISABLE_MANUFACTURING_TWIN",
    ):
        cfg["manufacturing_twin_enabled"] = False
    if risk_inference_enabled:
        cfg["risk_inference_enabled"] = risk_inference_enabled
    if risk_prediction_path:
        cfg["risk_prediction_path"] = risk_prediction_path
    if risk_max_age_seconds:
        cfg["risk_max_age_seconds"] = risk_max_age_seconds
    if risk_action_cooldown_seconds:
        cfg["risk_action_cooldown_seconds"] = risk_action_cooldown_seconds

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
    cfg["manufacturing_twin_latest_path"] = str(
        _normalize_manufacturing_twin_path(base_dir, cfg["manufacturing_twin_latest_path"])
    )
    cfg["risk_prediction_path"] = str(_normalize_repo_artifact_path(base_dir, cfg["risk_prediction_path"]))
    cfg["poll_interval_seconds"] = float(cfg["poll_interval_seconds"])
    cfg["decision_cooldown_seconds"] = float(cfg["decision_cooldown_seconds"])
    cfg["dry_run_only"] = coerce_bool(
        cfg["dry_run_only"],
        field_name="policy_manager.dry_run_only",
    )
    cfg["metrics_http_host"] = str(cfg["metrics_http_host"])
    cfg["metrics_http_port"] = int(cfg["metrics_http_port"])
    cfg["ensure_onos_slice_flows"] = coerce_bool(
        cfg["ensure_onos_slice_flows"],
        field_name="policy_manager.ensure_onos_slice_flows",
    )
    cfg["force_onos_flow_refresh"] = coerce_bool(
        cfg["force_onos_flow_refresh"],
        field_name="policy_manager.force_onos_flow_refresh",
    )
    cfg["manufacturing_twin_enabled"] = coerce_bool(
        cfg["manufacturing_twin_enabled"],
        field_name="policy_manager.manufacturing_twin_enabled",
    )
    cfg["manufacturing_twin_max_age_seconds"] = float(cfg["manufacturing_twin_max_age_seconds"])
    cfg["risk_inference_enabled"] = coerce_bool(
        cfg["risk_inference_enabled"],
        field_name="policy_manager.risk_inference_enabled",
    )
    cfg["risk_max_age_seconds"] = float(cfg["risk_max_age_seconds"])
    cfg["risk_low_action"] = str(cfg["risk_low_action"])
    cfg["risk_medium_action"] = str(cfg["risk_medium_action"])
    cfg["risk_high_action"] = str(cfg["risk_high_action"])
    cfg["risk_action_cooldown_seconds"] = float(cfg["risk_action_cooldown_seconds"])

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


def _normalize_manufacturing_twin_path(base_dir: Path, value: Any) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] == "logs":
        return (base_dir.parent / candidate).resolve()
    return normalize_path(base_dir, candidate)


def _normalize_repo_artifact_path(base_dir: Path, value: Any) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] in {"logs", "risk_inference", "digital_twin"}:
        return (base_dir.parent / candidate).resolve()
    return normalize_path(base_dir, candidate)
