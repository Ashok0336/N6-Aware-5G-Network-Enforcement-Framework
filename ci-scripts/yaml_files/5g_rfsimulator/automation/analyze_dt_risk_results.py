#!/usr/bin/env python3
"""Analyze DT-risk CCNC experiment outputs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_GLOB = ROOT / "logs/experiments/ccnc"
MODES = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]
SUMMARY_FIELDS = [
    "mode",
    "status",
    "duration_seconds",
    "sla_checks",
    "sla_violations",
    "sla_violation_rate",
    "control_latency_avg_ms",
    "control_latency_max_ms",
    "control_loss_percent",
    "data_throughput_bps",
    "sensor_delivery_ratio_percent",
    "queue_rule_presence",
    "policy_drift_detected",
    "risk_predictions",
    "low_risk_events",
    "medium_risk_events",
    "high_risk_events",
    "overall_risk_score_avg",
    "overall_risk_score_max",
    "policy_decisions",
    "policy_applied_count",
    "selected_policy_actions",
    "enforcement_path",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze DT-risk CCNC result directories.")
    parser.add_argument("--input", type=Path, default=None, help="Result directory. Default: latest logs/experiments/ccnc/dt_risk_*")
    return parser.parse_args()


def resolve_input(path: Optional[Path]) -> Path:
    if path is not None:
        candidate = path.expanduser()
        return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    candidates = sorted(DEFAULT_RESULTS_GLOB.glob("dt_risk_*"), key=lambda item: item.stat().st_mtime if item.exists() else 0)
    if not candidates:
        raise FileNotFoundError(f"No dt_risk_* result directories found under {DEFAULT_RESULTS_GLOB}")
    return candidates[-1].resolve()


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).strip().replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group(0))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    field_list = list(fields)
    for row in rows:
        for key in row:
            if key not in field_list:
                field_list.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_list, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def mode_dirs(result_dir: Path) -> List[Tuple[str, Path]]:
    return [(mode, result_dir / mode) for mode in MODES if (result_dir / mode).exists()]


def metric_value(mode_dir: Path, service: str, metric: str, column: str = "mean") -> Any:
    candidates = [mode_dir / "service_metrics.csv", *sorted(mode_dir.glob("*/service_metrics.csv"))]
    for path in candidates:
        for row in read_csv_rows(path):
            if row.get("service_class") == service and row.get("metric") == metric:
                return row.get(column) or row.get("latest") or row.get("value") or ""
    return ""


def load_or_build_summary(result_dir: Path) -> List[Dict[str, Any]]:
    summary_path = result_dir / "summary.csv"
    existing = read_csv_rows(summary_path)
    if existing:
        return [dict(row) for row in existing]
    rows: List[Dict[str, Any]] = []
    for mode, mode_dir in mode_dirs(result_dir):
        status = load_json(mode_dir / "mode_status.json")
        summary = load_json(mode_dir / "summary_metrics.json")
        policy = load_json(mode_dir / "policy_decision_summary.json")
        risk = risk_stats(mode_dir)
        rows.append(
            {
                "mode": mode,
                "status": status.get("status", "failed"),
                "duration_seconds": status.get("duration_seconds", ""),
                "sla_checks": summary.get("sla_checks", 0),
                "sla_violations": summary.get("sla_violations", 0),
                "sla_violation_rate": summary.get("sla_violation_rate", 0.0),
                "control_latency_avg_ms": metric_value(mode_dir, "real_time_control", "latency_avg_ms"),
                "control_latency_max_ms": metric_value(mode_dir, "real_time_control", "latency_max_ms", "max"),
                "control_loss_percent": metric_value(mode_dir, "real_time_control", "loss_percent"),
                "data_throughput_bps": metric_value(mode_dir, "high_throughput_data", "throughput_bps"),
                "sensor_delivery_ratio_percent": metric_value(mode_dir, "sensor_telemetry", "delivery_ratio_percent"),
                "queue_rule_presence": status.get("queue_rule_presence", queue_presence(mode_dir)),
                "policy_drift_detected": "",
                "policy_decisions": policy.get("decision_count", 0),
                "risk_predictions": risk["risk_predictions"],
                "overall_risk_score_avg": risk["overall_risk_score_avg"],
                "overall_risk_score_max": risk["overall_risk_score_max"],
                "low_risk_events": risk["low_risk_events"],
                "medium_risk_events": risk["medium_risk_events"],
                "high_risk_events": risk["high_risk_events"],
                "selected_policy_actions": "",
                "enforcement_path": "",
                "policy_applied_count": policy_applied_count(mode_dir),
            }
        )
    return rows


def risk_level_is_high(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return float(value) >= 2
    return str(value).strip().lower() == "high"


def risk_prediction_rows(mode_dir: Path) -> List[Dict[str, Any]]:
    path = mode_dir / "risk_inference" / "risk_predictions.jsonl"
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payload["_line_number"] = line_number
                rows.append(payload)
    return rows


def risk_stats(mode_dir: Path) -> Dict[str, Any]:
    rows = risk_prediction_rows(mode_dir)
    levels = Counter(str(row.get("overall_risk_level", "unknown")).lower() for row in rows)
    scores = [as_float(row.get("overall_risk_score")) for row in rows]
    scores = [value for value in scores if value is not None]
    actions = Counter(str(row.get("recommended_policy_action", "")) for row in rows if row.get("recommended_policy_action"))
    return {
        "risk_predictions": len(rows),
        "overall_risk_score_avg": mean(scores) if scores else "",
        "overall_risk_score_max": max(scores) if scores else "",
        "low_risk_events": levels.get("low", 0) + levels.get("0", 0),
        "medium_risk_events": levels.get("medium", 0) + levels.get("1", 0),
        "high_risk_events": levels.get("high", 0) + levels.get("2", 0),
        "recommended_policy_actions": json.dumps(dict(actions), sort_keys=True),
    }


def policy_records(mode_dir: Path) -> List[Dict[str, Any]]:
    records = []
    candidates = sorted((mode_dir / "policy").glob("*.jsonl"))
    for path in candidates:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict) and payload.get("event_type") in {"policy_decision", "policy_cycle"}:
                    records.append(payload)
    return records


def policy_applied_count(mode_dir: Path) -> int:
    count = 0
    for record in policy_records(mode_dir):
        if record.get("applied") is True:
            count += 1
        elif isinstance(record.get("enforcement_result"), dict) and record["enforcement_result"].get("applied") is True:
            count += 1
    return count


def queue_presence(mode_dir: Path) -> str:
    for path in (mode_dir / "ovs_flows_after.txt", mode_dir / "queue_counters_after.txt", mode_dir / "queue_counter_after.txt"):
        if path.exists() and "set_queue" in path.read_text(encoding="utf-8", errors="ignore"):
            return "present"
    return "absent"


def parse_queue_counters(path: Path) -> Dict[str, int]:
    counters = defaultdict(int)
    if not path.exists():
        return dict(counters)
    text = path.read_text(encoding="utf-8", errors="ignore")
    for match in re.finditer(r"queue\s+(\d+).*?bytes=(\d+).*?pkts=(\d+)", text):
        queue_id, byte_count, packet_count = match.groups()
        counters[f"queue_{queue_id}_bytes"] += int(byte_count)
        counters[f"queue_{queue_id}_packets"] += int(packet_count)
    return dict(counters)


def build_sla_violation_comparison(summary_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fifo_rate = 0.0
    for row in summary_rows:
        if row.get("mode") == "fifo":
            fifo_rate = as_float(row.get("sla_violation_rate")) or 0.0
            break
    rows = []
    for row in summary_rows:
        rate = as_float(row.get("sla_violation_rate")) or 0.0
        rows.append(
            {
                "mode": row.get("mode", ""),
                "sla_checks": row.get("sla_checks", 0),
                "sla_violations": row.get("sla_violations", 0),
                "sla_violation_rate": rate,
                "sla_violation_reduction_vs_fifo": ((fifo_rate - rate) / fifo_rate) if fifo_rate else "",
            }
        )
    return rows


def build_risk_prediction_summary(result_dir: Path) -> List[Dict[str, Any]]:
    rows = []
    for mode, mode_dir in mode_dirs(result_dir):
        predictions = risk_prediction_rows(mode_dir)
        levels = Counter(str(row.get("overall_risk_level", "unknown")).lower() for row in predictions)
        scores = [as_float(row.get("overall_risk_score")) for row in predictions]
        scores = [value for value in scores if value is not None]
        actions = Counter(str(row.get("recommended_policy_action", "")) for row in predictions if row.get("recommended_policy_action"))
        rows.append(
            {
                "mode": mode,
                "risk_predictions": len(predictions),
                "overall_risk_score_avg": mean(scores) if scores else "",
                "overall_risk_score_max": max(scores) if scores else "",
                "low_risk_events": levels.get("low", 0) + levels.get("0", 0),
                "medium_risk_events": levels.get("medium", 0) + levels.get("1", 0),
                "high_risk_events": levels.get("high", 0) + levels.get("2", 0),
                "recommended_policy_actions": json.dumps(dict(actions), sort_keys=True),
            }
        )
    return rows


def build_policy_action_summary(result_dir: Path) -> List[Dict[str, Any]]:
    rows = []
    for mode, mode_dir in mode_dirs(result_dir):
        records = policy_records(mode_dir)
        actions = Counter()
        applied_by_action = Counter()
        enforcement_path = ""
        for record in records:
            if isinstance(record.get("decisions"), list):
                for decision in record["decisions"]:
                    if isinstance(decision, dict):
                        action = str(decision.get("selected_policy_action") or decision.get("recommended_action") or "unknown")
                        actions[action] += 1
                if isinstance(record.get("enforcement_result"), dict):
                    if record["enforcement_result"].get("enforcement_path"):
                        enforcement_path = str(record["enforcement_result"].get("enforcement_path"))
                    if record["enforcement_result"].get("applied") is True:
                        for action in actions:
                            applied_by_action[action] += 1
            else:
                action = str(record.get("selected_policy_action") or record.get("state_after_decision") or record.get("recommended_action") or "unknown")
                actions[action] += 1
                if record.get("applied") is True:
                    applied_by_action[action] += 1
                if record.get("enforcement_path"):
                    enforcement_path = str(record.get("enforcement_path"))
        for action, count in sorted(actions.items()):
            rows.append(
                {
                    "mode": mode,
                    "policy_action": action,
                    "decision_count": count,
                    "applied_count": applied_by_action.get(action, 0),
                    "enforcement_path": enforcement_path,
                }
            )
        if not records:
            rows.append({"mode": mode, "policy_action": "", "decision_count": 0, "applied_count": 0, "enforcement_path": ""})
    return rows


def build_queue_enforcement_summary(result_dir: Path) -> List[Dict[str, Any]]:
    rows = []
    for mode, mode_dir in mode_dirs(result_dir):
        before = parse_queue_counters(mode_dir / "queue_counter_before.txt")
        after = parse_queue_counters(mode_dir / "queue_counter_after.txt")
        row = {
            "mode": mode,
            "queue_rule_presence": queue_presence(mode_dir),
        }
        for queue_id in ("1", "2", "3"):
            for metric in ("packets", "bytes"):
                key = f"queue_{queue_id}_{metric}"
                row[f"{key}_before"] = before.get(key, 0)
                row[f"{key}_after"] = after.get(key, 0)
                row[f"{key}_delta"] = after.get(key, 0) - before.get(key, 0)
        rows.append(row)
    return rows


def main() -> int:
    result_dir = resolve_input(parse_args().input)
    if not result_dir.exists():
        raise FileNotFoundError(f"Result directory not found: {result_dir}")

    summary_rows = load_or_build_summary(result_dir)
    write_csv(result_dir / "dt_risk_summary.csv", summary_rows, SUMMARY_FIELDS)
    write_csv(result_dir / "sla_violation_comparison.csv", build_sla_violation_comparison(summary_rows), ["mode", "sla_checks", "sla_violations", "sla_violation_rate", "sla_violation_reduction_vs_fifo"])
    write_csv(
        result_dir / "risk_prediction_summary.csv",
        build_risk_prediction_summary(result_dir),
        [
            "mode",
            "risk_predictions",
            "overall_risk_score_avg",
            "overall_risk_score_max",
            "low_risk_events",
            "medium_risk_events",
            "high_risk_events",
            "recommended_policy_actions",
        ],
    )
    write_csv(result_dir / "policy_action_summary.csv", build_policy_action_summary(result_dir), ["mode", "policy_action", "decision_count", "applied_count", "enforcement_path"])
    queue_rows = build_queue_enforcement_summary(result_dir)
    queue_fields = ["mode", "queue_rule_presence"]
    for queue_id in ("1", "2", "3"):
        for metric in ("packets", "bytes"):
            queue_fields.extend([f"queue_{queue_id}_{metric}_before", f"queue_{queue_id}_{metric}_after", f"queue_{queue_id}_{metric}_delta"])
    write_csv(result_dir / "queue_enforcement_summary.csv", queue_rows, queue_fields)

    print(f"[dt-risk-analysis] wrote analysis CSVs under {result_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
