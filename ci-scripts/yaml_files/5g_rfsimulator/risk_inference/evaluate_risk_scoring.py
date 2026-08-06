#!/usr/bin/env python3
"""Evaluate deterministic risk scoring outputs."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize deterministic SLA risk scoring predictions.")
    parser.add_argument("--input", type=Path, default=Path("logs/risk_inference/risk_predictions.jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path("logs/risk_inference"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_predictions(args.input)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "risk_inference_summary.csv", risk_summary(rows), ["prediction_count", "overall_risk_score_avg", "overall_risk_score_max", "low_risk_events", "medium_risk_events", "high_risk_events", "recommended_policy_actions"])
    write_csv(args.output_dir / "service_risk_summary.csv", service_summary(rows), ["service", "prediction_count", "risk_score_avg", "risk_score_max", "low_risk_events", "medium_risk_events", "high_risk_events"])
    write_csv(args.output_dir / "high_risk_events.csv", high_risk_events(rows), ["timestamp", "overall_risk_score", "overall_risk_level", "recommended_policy_action", "explanation"])
    print(f"[risk-inference-eval] wrote summaries under {args.output_dir}")
    return 0


def read_predictions(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def risk_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scores = [_float(row.get("overall_risk_score")) for row in rows]
    scores = [score for score in scores if score is not None]
    levels = Counter(str(row.get("overall_risk_level", "unknown")).lower() for row in rows)
    actions = Counter(str(row.get("recommended_policy_action", "")) for row in rows if row.get("recommended_policy_action"))
    return [
        {
            "prediction_count": len(rows),
            "overall_risk_score_avg": mean(scores) if scores else "",
            "overall_risk_score_max": max(scores) if scores else "",
            "low_risk_events": levels.get("low", 0),
            "medium_risk_events": levels.get("medium", 0),
            "high_risk_events": levels.get("high", 0),
            "recommended_policy_actions": json.dumps(dict(actions), sort_keys=True),
        }
    ]


def service_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        service_risks = row.get("service_risks") if isinstance(row.get("service_risks"), dict) else {}
        for service, record in service_risks.items():
            if isinstance(record, dict):
                buckets[str(service)].append(record)
    output: List[Dict[str, Any]] = []
    for service, records in sorted(buckets.items()):
        scores = [_float(record.get("risk_score")) for record in records]
        scores = [score for score in scores if score is not None]
        levels = Counter(str(record.get("risk_level", "unknown")).lower() for record in records)
        output.append(
            {
                "service": service,
                "prediction_count": len(records),
                "risk_score_avg": mean(scores) if scores else "",
                "risk_score_max": max(scores) if scores else "",
                "low_risk_events": levels.get("low", 0),
                "medium_risk_events": levels.get("medium", 0),
                "high_risk_events": levels.get("high", 0),
            }
        )
    return output


def high_risk_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for row in rows:
        if str(row.get("overall_risk_level", "")).lower() != "high":
            continue
        explanation = row.get("explanation")
        output.append(
            {
                "timestamp": row.get("timestamp", ""),
                "overall_risk_score": row.get("overall_risk_score", ""),
                "overall_risk_level": row.get("overall_risk_level", ""),
                "recommended_policy_action": row.get("recommended_policy_action", ""),
                "explanation": "; ".join(str(item) for item in explanation) if isinstance(explanation, list) else str(explanation or ""),
            }
        )
    return output


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: Iterable[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())

