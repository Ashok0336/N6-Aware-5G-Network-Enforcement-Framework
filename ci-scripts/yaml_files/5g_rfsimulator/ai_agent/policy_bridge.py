#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
AGENT_TO_POLICY = {
    "HOLD_ACTION": ("all", "MAINTAIN_CURRENT_POLICY"),
    "MAINTAIN_CURRENT_POLICY": ("all", "MAINTAIN_CURRENT_POLICY"),
    "TIGHTEN_DATA_SHAPING": ("high_throughput_data", "TIGHTEN_DATA_SHAPING"),
    "PROTECT_SENSOR_MIN_BW": ("sensor_telemetry", "PROTECT_SENSOR_MIN_BW"),
    "MAINTAIN_POLICY": ("all", "MAINTAIN_CURRENT_POLICY"),
    "INCREASE_CONTROL_PRIORITY": ("real_time_control", "INCREASE_CONTROL_PRIORITY"),
    "REDUCE_EMBB_RATE": ("high_throughput_data", "TIGHTEN_DATA_SHAPING"),
    "PROTECT_SENSOR_BANDWIDTH": ("sensor_telemetry", "PROTECT_SENSOR_MIN_BW"),
    "RESTORE_DEFAULT_POLICY": ("all", "RESTORE_DEFAULT_POLICY"),
}
ALL_SERVICE_CLASSES = [
    "real_time_control",
    "high_throughput_data",
    "sensor_telemetry",
]


def log(message: str) -> None:
    print(f"[agent-policy-bridge] {message}", flush=True)


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
    latest: Optional[Dict[str, Any]] = None
    if not path.exists():
        return None
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
                payload["_source_line_number"] = line_number
                latest = payload
    return latest


def action_targets(action: str) -> List[Tuple[str, str]]:
    if action not in AGENT_TO_POLICY:
        raise ValueError(f"unsupported agent action: {action}")
    service_class, policy_action = AGENT_TO_POLICY[action]
    if service_class == "all":
        return [(item, policy_action) for item in ALL_SERVICE_CLASSES]
    return [(service_class, policy_action)]


def build_policy_decisions(agent_decision: Dict[str, Any]) -> List[Dict[str, Any]]:
    action = str(agent_decision.get("action") or "MAINTAIN_CURRENT_POLICY")
    generated_at = utc_timestamp()
    records: List[Dict[str, Any]] = []
    for service_class, policy_action in action_targets(action):
        records.append(
            {
                "event_type": "policy_decision",
                "timestamp": generated_at,
                "run_id": f"agent-policy-bridge-{generated_at}",
                "mode": "autonomous_agent",
                "policy_name": f"agent_{service_class}",
                "service_class": service_class,
                "recommended_action": policy_action,
                "state_after_decision": (
                    "MAINTAIN_CURRENT_POLICY"
                    if policy_action == "RESTORE_DEFAULT_POLICY"
                    else policy_action
                ),
                "candidate_action": policy_action,
                "decision_state": "agent_generated",
                "detected_condition": action.lower(),
                "explanation": _explanation(action, service_class, policy_action),
                "agent_decision_reference": {
                    "timestamp": agent_decision.get("timestamp"),
                    "action": action,
                    "source_line_number": agent_decision.get("_source_line_number"),
                    "inputs": agent_decision.get("inputs"),
                    "evidence": agent_decision.get("evidence"),
                    "reasons": agent_decision.get("reasons", []),
                },
                "external_enforcement_only": True,
                "no_oai_core_changes": True,
            }
        )
    return records


def _explanation(action: str, service_class: str, policy_action: str) -> str:
    if action == "HOLD_ACTION":
        return "AI agent held policy action because OVS/ONOS stability was not confirmed."
    if action in {"MAINTAIN_POLICY", "MAINTAIN_CURRENT_POLICY"}:
        return "AI agent requested no policy change; holding the current/default policy."
    if action == "INCREASE_CONTROL_PRIORITY":
        return "AI agent requested queue 2 boost for real-time control traffic."
    if action in {"REDUCE_EMBB_RATE", "TIGHTEN_DATA_SHAPING"}:
        return "AI agent requested queue 1 shaping for high-throughput eMBB traffic."
    if action in {"PROTECT_SENSOR_BANDWIDTH", "PROTECT_SENSOR_MIN_BW"}:
        return "AI agent requested queue 3 minimum bandwidth protection for sensor telemetry."
    if action == "RESTORE_DEFAULT_POLICY":
        return f"AI agent requested baseline restore for {service_class}; policy action={policy_action}."
    return f"AI agent action {action} mapped to policy action {policy_action}."


def write_policy_decisions(records: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def run_once(config: Dict[str, Any], config_path: Path) -> Optional[Path]:
    decisions_path = resolve_data_path(
        config.get("decisions_output_path", "../logs/ai_agent/agent_decisions.jsonl"),
        config_path,
    )
    policy_output_path = resolve_data_path(
        config.get("agent_policy_output_path", "../logs/policy/agent_policy_decisions.jsonl"),
        config_path,
    )
    agent_decision = latest_jsonl_record(decisions_path)
    if agent_decision is None:
        log(f"no agent decisions found at {decisions_path}")
        return None
    records = build_policy_decisions(agent_decision)
    write_policy_decisions(records, policy_output_path)
    log(f"loaded agent decision {agent_decision.get('action')} from {decisions_path}")
    log(f"wrote {len(records)} policy decision record(s) to {policy_output_path}")
    return policy_output_path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bridge AI agent decisions into policy-manager decision JSONL.")
    parser.add_argument("--config", help="Path to ai_agent/config.yaml.")
    parser.add_argument(
        "--output",
        help="Override output policy decision JSONL path.",
    )
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    if args.output:
        config["agent_policy_output_path"] = args.output
    run_once(config, config_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
