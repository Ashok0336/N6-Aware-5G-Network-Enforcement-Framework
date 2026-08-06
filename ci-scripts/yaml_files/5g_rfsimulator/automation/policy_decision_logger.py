#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


THRESHOLDS = {
    ("real_time_control", "latency_avg_ms"): ("<=", 10.0, "protect_control_latency"),
    ("real_time_control", "latency_max_ms"): ("<=", 20.0, "cap_control_tail_latency"),
    ("real_time_control", "loss_percent"): ("<=", 1.0, "protect_control_reliability"),
    ("high_throughput_data", "throughput_bps"): (">=", 50000000.0, "restore_data_throughput"),
    ("sensor_telemetry", "loss_percent"): ("<=", 2.0, "protect_sensor_delivery"),
    ("sensor_telemetry", "delivery_ratio_percent"): (">=", 98.0, "protect_sensor_delivery_ratio"),
}
DEFAULT_HYSTERESIS = 0.10
DEFAULT_MIN_HOLD_SECONDS = 15.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate explainable deterministic CCNC policy decisions.")
    parser.add_argument("experiment_dir", nargs="?", default=".", help="Experiment directory containing service_metrics.csv.")
    parser.add_argument("--hysteresis", type=float, default=DEFAULT_HYSTERESIS)
    parser.add_argument("--min-hold-seconds", type=float, default=DEFAULT_MIN_HOLD_SECONDS)
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()


def read_metrics(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def in_violation(value: float, operator: str, threshold: float) -> bool:
    if operator == "<=":
        return value > threshold
    if operator == ">=":
        return value < threshold
    return False


def clear_with_hysteresis(value: float, operator: str, threshold: float, hysteresis: float) -> bool:
    if operator == "<=":
        return value <= threshold * (1.0 - hysteresis)
    if operator == ">=":
        return value >= threshold * (1.0 + hysteresis)
    return True


def recommend(service: str, metric: str, violation: bool) -> str:
    if not violation:
        return "maintain_current_n6_policy"
    if service == "real_time_control":
        return "raise_control_priority_queue"
    if service == "sensor_telemetry":
        return "reserve_sensor_queue_capacity"
    if service == "high_throughput_data":
        return "shape_bulk_data_queue"
    return "maintain_current_n6_policy"


def main() -> int:
    args = parse_args()
    exp_dir = resolve_path(args.experiment_dir)
    exp_dir.mkdir(parents=True, exist_ok=True)
    metrics = read_metrics(exp_dir / "service_metrics.csv")

    decisions: List[Dict[str, Any]] = []
    last_action: Dict[str, str] = defaultdict(lambda: "maintain_current_n6_policy")
    last_change_time: Dict[str, float] = defaultdict(lambda: -1e9)
    policy_changes: Dict[str, int] = defaultdict(int)

    for index, row in enumerate(metrics):
        service = row.get("service_class", "")
        metric = row.get("metric", "")
        key = (service, metric)
        if key not in THRESHOLDS:
            continue
        operator, threshold, reason = THRESHOLDS[key]
        observed = as_float(row.get("latest", row.get("mean")))
        violation = in_violation(observed, operator, threshold)
        candidate = recommend(service, metric, violation)
        previous = last_action[service]
        elapsed = float(index) * 5.0 - last_change_time[service]
        held = elapsed >= args.min_hold_seconds
        if previous != "maintain_current_n6_policy" and not violation and not clear_with_hysteresis(observed, operator, threshold, args.hysteresis):
            candidate = previous
            state = "hold_hysteresis_band"
        elif previous != candidate and not held:
            candidate = previous
            state = "hold_minimum_policy_time"
        else:
            state = "violation_detected" if violation else "within_sla"

        changed = candidate != previous
        if changed:
            last_action[service] = candidate
            last_change_time[service] = float(index) * 5.0
            policy_changes[service] += 1

        decisions.append(
            {
                "event_type": "explainable_dt_policy_decision",
                "decision_source": "digital_twin_sla_threshold_policy",
                "decision_type": "deterministic_threshold_with_hysteresis",
                "service_class": service,
                "metric": metric,
                "observed_value": observed,
                "threshold": threshold,
                "operator": operator,
                "reason": reason,
                "previous_action": previous,
                "recommended_action": candidate,
                "decision_state": state,
                "policy_changed": changed,
                "hysteresis": args.hysteresis,
                "min_hold_seconds": args.min_hold_seconds,
                "enforcement_status": "pending_or_verified_by_n6_enforcement_logs",
            }
        )

    with (exp_dir / "policy_decisions_explainable.jsonl").open("w", encoding="utf-8") as handle:
        for decision in decisions:
            handle.write(json.dumps(decision, sort_keys=True) + "\n")

    stability_rows = []
    services = sorted({decision["service_class"] for decision in decisions})
    for service in services:
        count = policy_changes.get(service, 0)
        total = sum(1 for decision in decisions if decision["service_class"] == service)
        stability_rows.append(
            {
                "service_class": service,
                "decision_count": total,
                "policy_change_count": count,
                "oscillation_index": (count / total) if total else 0.0,
                "hysteresis": args.hysteresis,
                "min_hold_seconds": args.min_hold_seconds,
            }
        )
    with (exp_dir / "policy_stability.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = ["service_class", "decision_count", "policy_change_count", "oscillation_index", "hysteresis", "min_hold_seconds"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(stability_rows)

    summary = {
        "decision_count": len(decisions),
        "policy_change_count": sum(policy_changes.values()),
        "policy_changes_by_service": dict(policy_changes),
        "hysteresis": args.hysteresis,
        "min_hold_seconds": args.min_hold_seconds,
    }
    (exp_dir / "policy_decision_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[policy-decision-logger] wrote {len(decisions)} deterministic decisions to {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
