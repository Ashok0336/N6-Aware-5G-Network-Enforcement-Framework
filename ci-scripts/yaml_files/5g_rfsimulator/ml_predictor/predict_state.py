#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from .dataset_builder import (
        extract_features,
        latest_twin_state,
        load_config,
        resolve_config_path,
        resolve_data_path,
    )
except ImportError:
    from dataset_builder import (
        extract_features,
        latest_twin_state,
        load_config,
        resolve_config_path,
        resolve_data_path,
    )


def log(message: str) -> None:
    print(f"[ml-predictor] {message}", flush=True)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_model(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"model file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"model file is not a JSON object: {path}")
    return payload


def predict_from_state(state: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    features = extract_features(state)
    thresholds = model.get("thresholds", {}) if isinstance(model.get("thresholds"), dict) else {}
    urllc_threshold = _to_float(thresholds.get("urllc_latency_avg_ms"))
    embb_threshold = _to_float(thresholds.get("embb_throughput_bps"))
    if urllc_threshold is None:
        urllc_threshold = 20.0
    if embb_threshold is None:
        embb_threshold = 85000000.0

    urllc_latency = _to_float(features.get("urllc_latency_avg_ms"))
    embb_throughput = _to_float(features.get("embb_throughput_bps"))
    urllc_risk = int(urllc_latency is not None and urllc_latency > urllc_threshold)
    embb_risk = int(embb_throughput is not None and embb_throughput > embb_threshold)

    attention: List[str] = []
    if urllc_risk:
        attention.append("real_time_control")
    if embb_risk:
        attention.append("high_throughput_data")

    return {
        "timestamp": features.get("timestamp") or utc_timestamp(),
        "urllc_sla_violation_risk": urllc_risk,
        "embb_congestion_risk": embb_risk,
        "recommended_attention": attention,
    }


def append_prediction(prediction: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(prediction, sort_keys=True) + "\n")


def predict_once(config: Dict[str, Any], config_path: Path) -> Optional[Dict[str, Any]]:
    twin_state_path = resolve_data_path(
        config.get("twin_state_path", "../logs/digital_twin/twin_state.jsonl"),
        config_path,
    )
    model_path = resolve_data_path(
        config.get("model_output_path", "models/baseline_model.json"),
        config_path,
    )
    prediction_output_path = resolve_data_path(
        config.get("prediction_output_path", "../logs/ml_predictor/predictions.jsonl"),
        config_path,
    )

    state = latest_twin_state(twin_state_path)
    if state is None:
        log(f"no twin state records found at {twin_state_path}")
        return None
    model = load_model(model_path)
    prediction = predict_from_state(state, model)
    append_prediction(prediction, prediction_output_path)
    log(f"loaded twin state from {twin_state_path}")
    log(f"loaded model from {model_path}")
    log(f"wrote prediction to {prediction_output_path}")
    return prediction


def _to_float(value: Any) -> Optional[float]:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Predict risks from the latest Digital Twin state.")
    parser.add_argument("--config", help="Path to ml_predictor/config.yaml.")
    parser.add_argument("--once", action="store_true", help="Write one prediction and exit.")
    parser.add_argument("--interval", type=float, help="Continuously predict every N seconds.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    interval = args.interval
    if interval is None:
        interval = float(config.get("prediction_interval_seconds", 2))

    if args.once:
        predict_once(config, config_path)
        return 0

    log(f"starting prediction loop interval_seconds={interval}")
    try:
        while True:
            predict_once(config, config_path)
            time.sleep(interval)
    except KeyboardInterrupt:
        log("stopped")
        return 0


if __name__ == "__main__":
    sys.exit(main())

