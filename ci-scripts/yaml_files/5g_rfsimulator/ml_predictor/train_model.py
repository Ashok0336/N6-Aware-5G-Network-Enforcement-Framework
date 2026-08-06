#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from .dataset_builder import load_config, resolve_config_path, resolve_data_path
except ImportError:
    from dataset_builder import load_config, resolve_config_path, resolve_data_path


def log(message: str) -> None:
    print(f"[ml-predictor] {message}", flush=True)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def train_baseline_model(
    dataset_path: Path,
    model_output_path: Path,
    urllc_latency_threshold_ms: float,
    embb_throughput_threshold_bps: float,
) -> Dict[str, Any]:
    row_count = 0
    positive_urllc = 0
    positive_embb = 0
    if dataset_path.exists():
        with dataset_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_count += 1
                positive_urllc += int(row.get("urllc_sla_violation") == "1")
                positive_embb += int(row.get("embb_congestion_risk") == "1")

    model = {
        "model_type": "threshold_baseline",
        "created_at": utc_timestamp(),
        "feature_defaults": {
            "missing_numeric": None,
            "missing_boolean": 0,
        },
        "thresholds": {
            "urllc_latency_avg_ms": urllc_latency_threshold_ms,
            "embb_throughput_bps": embb_throughput_threshold_bps,
        },
        "labels": {
            "urllc_sla_violation": "1 if urllc_latency_avg_ms exceeds threshold else 0",
            "embb_congestion_risk": "1 if embb_throughput_bps exceeds threshold else 0",
        },
        "training_summary": {
            "dataset_path": str(dataset_path),
            "row_count": row_count,
            "urllc_positive_rows": positive_urllc,
            "embb_positive_rows": positive_embb,
        },
    }
    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    model_output_path.write_text(json.dumps(model, indent=2, sort_keys=True), encoding="utf-8")
    return model


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the baseline ML predictor from dataset.csv.")
    parser.add_argument("--config", help="Path to ml_predictor/config.yaml.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    dataset_path = resolve_data_path(
        config.get("dataset_output_path", "../logs/ml_predictor/dataset.csv"),
        config_path,
    )
    model_output_path = resolve_data_path(
        config.get("model_output_path", "models/baseline_model.json"),
        config_path,
    )
    model = train_baseline_model(
        dataset_path=dataset_path,
        model_output_path=model_output_path,
        urllc_latency_threshold_ms=float(config.get("urllc_latency_threshold_ms", 20)),
        embb_throughput_threshold_bps=float(config.get("embb_throughput_threshold_bps", 85000000)),
    )
    log(f"read dataset from {dataset_path}")
    log(f"wrote baseline model to {model_output_path}")
    log(f"training rows={model['training_summary']['row_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

