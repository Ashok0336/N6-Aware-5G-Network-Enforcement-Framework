#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
SUPPORTED_ACTIONS = {
    "HOLD_ACTION",
    "MAINTAIN_CURRENT_POLICY",
    "INCREASE_CONTROL_PRIORITY",
    "TIGHTEN_DATA_SHAPING",
    "PROTECT_SENSOR_MIN_BW",
}


def log(message: str) -> None:
    print(f"[ai-agent] {message}", flush=True)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_config(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    if not path.exists():
        return config
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            config[key.strip()] = _coerce_scalar(value.strip())
    return config


def _coerce_scalar(value: str) -> Any:
    if value in {"", "null", "None"}:
        return None
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value.strip("'\"")


def resolve_config_path(value: Optional[str]) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def resolve_data_path(value: Any, config_path: Path) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


def latest_jsonl_record(path: Path) -> Optional[Dict[str, Any]]:
    records = recent_jsonl_records(path, limit=1)
    return records[-1] if records else None


def recent_jsonl_records(path: Path, limit: int = 3) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                log(f"skipping malformed JSONL line in {path}:{line_number}: {exc}")
                continue
            if isinstance(payload, dict):
                records.append(payload)
                if len(records) > limit:
                    records.pop(0)
    return records


def build_decision(
    twin_state: Dict[str, Any],
    prediction: Dict[str, Any],
    recent_twin_states: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    services = _services_by_slice(twin_state)
    urllc = services.get("urllc", {})
    embb = services.get("embb", {})
    mmtc = services.get("mmtc", {})

    urllc_risk = int(prediction.get("urllc_sla_violation_risk") or 0)
    embb_risk = int(prediction.get("embb_congestion_risk") or 0)
    sensor_risk = _sensor_risk(mmtc)
    stability = _stability_summary(recent_twin_states or [twin_state])
    reasons: List[str] = []

    if stability["ovs_unstable"] or stability["onos_unstable"]:
        action = "HOLD_ACTION"
        if stability["ovs_unstable"]:
            reasons.append("OVS controller connectivity is unstable across the latest twin samples")
        if stability["onos_unstable"]:
            reasons.append("ONOS health is unstable across the latest twin samples")
    elif urllc_risk:
        action = "INCREASE_CONTROL_PRIORITY"
        reasons.append("prediction reports high URLLC SLA violation risk")
    elif embb_risk:
        action = "TIGHTEN_DATA_SHAPING"
        reasons.append("prediction reports high eMBB congestion risk")
    elif sensor_risk:
        action = "PROTECT_SENSOR_MIN_BW"
        reasons.append("Digital Twin reports high sensor telemetry risk")
    else:
        action = "MAINTAIN_CURRENT_POLICY"
        reasons.append("no high-risk condition detected")

    decision = {
        "timestamp": utc_timestamp(),
        "action": action,
        "supported_actions": sorted(SUPPORTED_ACTIONS),
        "dry_run": True,
        "decision_only": True,
        "enforcement_performed": False,
        "reasons": reasons,
        "inputs": {
            "twin_timestamp": twin_state.get("last_updated"),
            "prediction_timestamp": prediction.get("timestamp"),
        },
        "evidence": {
            "urllc_latency_avg_ms": _to_float(urllc.get("latency_avg_ms")),
            "urllc_latency_max_ms": _to_float(urllc.get("latency_max_ms")),
            "embb_throughput_bps": _to_float(embb.get("throughput_bps")),
            "mmtc_throughput_bps": _to_float(mmtc.get("throughput_bps")),
            "mmtc_packet_loss_percent": _to_float(mmtc.get("packet_loss_percent")),
            "mmtc_sla_violation_risk": mmtc.get("sla_violation_risk"),
            "urllc_sla_violation_risk": urllc_risk,
            "embb_congestion_risk": embb_risk,
            "sensor_telemetry_risk": int(sensor_risk),
            "prediction_recommended_attention": prediction.get("recommended_attention", []),
            "ovs_controller_connected_last_3": stability["ovs_controller_connected_last_3"],
            "onos_ok_last_3": stability["onos_ok_last_3"],
            "ovs_unstable_count": stability["ovs_unstable_count"],
            "onos_unstable_count": stability["onos_unstable_count"],
            "stability_sample_count": stability["sample_count"],
            "stability_majority_threshold": stability["majority_threshold"],
        },
    }
    if decision["action"] not in SUPPORTED_ACTIONS:
        raise ValueError(f"unsupported action generated: {decision['action']}")
    return decision


def append_decision(decision: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(decision, sort_keys=True) + "\n")


def run_once(config: Dict[str, Any], config_path: Path) -> Optional[Dict[str, Any]]:
    twin_state_path = resolve_data_path(
        config.get("twin_state_path", "../logs/digital_twin/twin_state.jsonl"),
        config_path,
    )
    predictions_path = resolve_data_path(
        config.get("predictions_path", "../logs/ml_predictor/predictions.jsonl"),
        config_path,
    )
    decisions_output_path = resolve_data_path(
        config.get("decisions_output_path", "../logs/ai_agent/agent_decisions.jsonl"),
        config_path,
    )

    recent_twin_states = recent_jsonl_records(twin_state_path, limit=3)
    twin_state = recent_twin_states[-1] if recent_twin_states else None
    prediction = latest_jsonl_record(predictions_path)
    if twin_state is None:
        log(f"no twin state records found at {twin_state_path}")
        return None
    if prediction is None:
        log(f"no prediction records found at {predictions_path}")
        return None

    decision = build_decision(
        twin_state=twin_state,
        prediction=prediction,
        recent_twin_states=recent_twin_states,
    )
    append_decision(decision, decisions_output_path)
    log(f"loaded {len(recent_twin_states)} twin state sample(s) from {twin_state_path}")
    log(f"loaded prediction from {predictions_path}")
    log(f"wrote dry-run decision {decision['action']} to {decisions_output_path}")
    return decision


def _services_by_slice(twin_state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    services: Dict[str, Dict[str, Any]] = {}
    raw_services = twin_state.get("services", [])
    if not isinstance(raw_services, list):
        return services
    for item in raw_services:
        if not isinstance(item, dict):
            continue
        slice_name = item.get("slice_name")
        if slice_name:
            services[str(slice_name)] = item
    return services


def _sensor_risk(mmtc: Dict[str, Any]) -> bool:
    sla_risk = str(mmtc.get("sla_violation_risk") or "").lower()
    loss = _to_float(mmtc.get("packet_loss_percent"))
    if sla_risk == "high":
        return True
    if loss is not None and loss > 1.0:
        return True
    return False


def _stability_summary(recent_twin_states: List[Dict[str, Any]]) -> Dict[str, Any]:
    ovs_values: List[bool] = []
    onos_values: List[bool] = []
    for state in recent_twin_states[-3:]:
        ovs_status = state.get("ovs_status", {}) if isinstance(state.get("ovs_status"), dict) else {}
        onos_status = state.get("onos_status", {}) if isinstance(state.get("onos_status"), dict) else {}
        ovs_values.append(ovs_status.get("controller_connected") is True)
        onos_values.append(onos_status.get("ok") is True)

    sample_count = len(ovs_values)
    majority_threshold = (sample_count // 2) + 1 if sample_count else 1
    ovs_unstable_count = sum(1 for value in ovs_values if not value)
    onos_unstable_count = sum(1 for value in onos_values if not value)
    return {
        "ovs_controller_connected_last_3": ovs_values,
        "onos_ok_last_3": onos_values,
        "ovs_unstable_count": ovs_unstable_count,
        "onos_unstable_count": onos_unstable_count,
        "ovs_unstable": ovs_unstable_count >= majority_threshold,
        "onos_unstable": onos_unstable_count >= majority_threshold,
        "sample_count": sample_count,
        "majority_threshold": majority_threshold,
    }


def _to_float(value: Any) -> Optional[float]:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate dry-run AI agent decisions from twin and ML prediction logs.")
    parser.add_argument("--config", help="Path to ai_agent/config.yaml.")
    parser.add_argument("--once", action="store_true", help="Write one agent decision and exit.")
    parser.add_argument("--interval", type=float, help="Continuously generate decisions every N seconds.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    interval = args.interval
    if interval is None:
        interval = float(config.get("agent_interval_seconds", 2))

    if args.once:
        run_once(config, config_path)
        return 0

    log(f"starting dry-run decision loop interval_seconds={interval}")
    try:
        while True:
            run_once(config, config_path)
            time.sleep(interval)
    except KeyboardInterrupt:
        log("stopped")
        return 0


if __name__ == "__main__":
    sys.exit(main())
