#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List


PHASE_WEIGHTS = {
    "job_setup": {"high_throughput_data": 0.50, "sensor_telemetry": 0.25, "real_time_control": 0.25},
    "active_printing": {"real_time_control": 0.50, "sensor_telemetry": 0.30, "high_throughput_data": 0.20},
    "quality_inspection": {"sensor_telemetry": 0.50, "real_time_control": 0.30, "high_throughput_data": 0.20},
    "all": {"real_time_control": 0.45, "sensor_telemetry": 0.35, "high_throughput_data": 0.20},
}
SERVICES = ["real_time_control", "sensor_telemetry", "high_throughput_data"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute phase-aware additive manufacturing utility.")
    parser.add_argument("experiment_dir", nargs="?", default=".", help="Experiment directory.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()


def read_violations(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def service_scores(rows: Iterable[Dict[str, str]]) -> Dict[str, float]:
    checks = {service: 0 for service in SERVICES}
    violations = {service: 0 for service in SERVICES}
    for row in rows:
        service = row.get("service_class", "")
        if service not in checks:
            continue
        checks[service] += 1
        if row.get("violation", "").lower() in {"true", "1", "yes"}:
            violations[service] += 1
    scores = {}
    for service in SERVICES:
        if checks[service] == 0:
            scores[service] = 1.0
        else:
            scores[service] = max(0.0, 1.0 - violations[service] / checks[service])
    return scores


def main() -> int:
    args = parse_args()
    exp_dir = resolve_path(args.experiment_dir)
    exp_dir.mkdir(parents=True, exist_ok=True)
    rows = read_violations(exp_dir / "sla_violations.csv")
    scores = service_scores(rows)

    utility_rows = []
    for phase, weights in PHASE_WEIGHTS.items():
        utility = sum(scores[service] * weight for service, weight in weights.items())
        row = {
            "phase": phase,
            "utility_score": utility,
            "control_score": scores["real_time_control"],
            "sensor_score": scores["sensor_telemetry"],
            "data_score": scores["high_throughput_data"],
            "control_weight": weights.get("real_time_control", 0.0),
            "sensor_weight": weights.get("sensor_telemetry", 0.0),
            "data_weight": weights.get("high_throughput_data", 0.0),
        }
        utility_rows.append(row)

    fields = ["phase", "utility_score", "control_score", "sensor_score", "data_score", "control_weight", "sensor_weight", "data_weight"]
    with (exp_dir / "manufacturing_utility.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(utility_rows)

    summary = {
        "experiment_dir": str(exp_dir),
        "service_scores": scores,
        "phase_utility": {row["phase"]: row["utility_score"] for row in utility_rows},
        "sla_rows": len(rows),
    }
    (exp_dir / "manufacturing_utility_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[manufacturing-utility] wrote phase utility scores to {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
