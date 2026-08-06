#!/usr/bin/env python3
"""CLI for deterministic SLA risk inference from the Shadow N6 Digital Twin."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    from .risk_features import DEFAULT_EXPECTED_QUEUE_MAPPING, load_twin_features, twin_state_age_seconds
    from .risk_rules import component_scores
    from .risk_score import score_prediction
except ImportError:
    from risk_features import DEFAULT_EXPECTED_QUEUE_MAPPING, load_twin_features, twin_state_age_seconds
    from risk_rules import component_scores
    from risk_score import score_prediction


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic SLA risk scoring for the Shadow N6 Digital Twin.")
    parser.add_argument("--twin-state-path", type=Path, default=None, help="Optional twin state JSON or JSONL path.")
    parser.add_argument("--config", type=Path, default=Path("risk_inference/config.yaml"), help="Risk scoring config path.")
    parser.add_argument("--output-dir", type=Path, default=Path("logs/risk_inference"), help="Output directory for risk predictions.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(_resolve_path(args.config))
    output_dir = _resolve_path(args.output_dir)
    mapping = config.get("expected_queue_mapping", DEFAULT_EXPECTED_QUEUE_MAPPING)
    features = load_twin_features(args.twin_state_path, mapping)
    age_seconds = twin_state_age_seconds(features.timestamp)
    data_quality_status = _data_quality_status(features.missing_fields, age_seconds, config)
    inference_status = _inference_status(data_quality_status)
    valid_for_policy = inference_status == "ok"
    controller_available = _controller_available(features)
    prediction = score_prediction(
        component_scores(features, config),
        config,
        valid_for_policy=valid_for_policy,
        queue_rule_presence=features.queue_rule_presence_overall,
        policy_drift_detected=features.policy_drift,
        controller_available=controller_available,
    )

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "inference_method": "deterministic_sla_risk_scoring",
        "inference_status": inference_status,
        "valid_for_policy": valid_for_policy,
        "data_quality_status": data_quality_status,
        "queue_rule_presence": features.queue_rule_presence_overall,
        "policy_drift_detected": features.policy_drift,
        "controller_available": controller_available,
        "overall_risk_score": prediction["overall_risk_score"],
        "overall_risk_level": prediction["overall_risk_level"],
        "service_risks": prediction["service_risks"],
        "recommended_policy_action": prediction["recommended_policy_action"],
        "action_reason": prediction["action_reason"],
        "enforcement_churn_guard_applied": False,
        "explanation": prediction["explanation"],
        "missing_fields": features.missing_fields,
        "twin_state_age_seconds": age_seconds,
        "twin_state_source": features.source_path,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    latest_path = output_dir / str(config.get("output_paths", {}).get("latest_prediction", "latest_risk_prediction.json"))
    jsonl_path = output_dir / str(config.get("output_paths", {}).get("prediction_log", "risk_predictions.jsonl"))
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    print(f"[risk-inference] wrote {latest_path}")
    return 0


def load_config(path: Path) -> Dict[str, Any]:
    default = {
        "expected_queue_mapping": DEFAULT_EXPECTED_QUEUE_MAPPING,
        "risk_weights": {
            "sla_margin_weight": 0.40,
            "trend_weight": 0.25,
            "queue_pressure_weight": 0.20,
            "enforcement_mismatch_weight": 0.15,
        },
        "risk_boundaries": {"low_max": 0.33, "medium_max": 0.66},
        "max_twin_age_seconds": 30,
    }
    if not path.exists():
        return default
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return _deep_merge(default, payload if isinstance(payload, dict) else {})
    except ImportError:
        return _deep_merge(default, _parse_simple_yaml(path.read_text(encoding="utf-8")))


def _parse_simple_yaml(text: str) -> Dict[str, Any]:
    root: Dict[str, Any] = {}
    stack: List[tuple[int, Dict[str, Any]]] = [(-1, root)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip() or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, value = line.strip().split(":", 1)
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        value = value.strip()
        if not value:
            child: Dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _coerce_scalar(value)
    return root


def _coerce_scalar(value: str) -> Any:
    value = value.strip().strip('"').strip("'")
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_path(path: Path) -> Path:
    expanded = path.expanduser()
    return expanded if expanded.is_absolute() else (ROOT / expanded)


def _data_quality_status(missing_fields: List[str], age_seconds: Any, config: Dict[str, Any]) -> List[str]:
    statuses: List[str] = []
    max_age = float(config.get("max_twin_age_seconds", 30))
    if age_seconds is None or age_seconds > max_age:
        statuses.append("stale_twin_state")
    if any(field.startswith("service_metrics.") for field in missing_fields):
        statuses.append("partial_observation")
    if "queue_rules" in missing_fields:
        statuses.append("queue_rule_presence_unknown")
    return statuses or ["ok"]


def _inference_status(data_quality_status: List[str]) -> str:
    if "stale_twin_state" in data_quality_status:
        return "stale_twin_state"
    if "partial_observation" in data_quality_status:
        return "partial_observation"
    return "ok"


def _controller_available(features: Any) -> bool:
    return features.onos.get("ok") is not False and features.ovs.get("controller_connected") is not False


if __name__ == "__main__":
    raise SystemExit(main())
