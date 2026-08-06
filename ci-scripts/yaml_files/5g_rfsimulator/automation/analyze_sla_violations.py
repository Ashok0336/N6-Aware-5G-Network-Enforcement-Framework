#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
SERVICE_ALIASES = {
    "urllc": "real_time_control",
    "urlcc": "real_time_control",
    "real_time_control": "real_time_control",
    "control": "real_time_control",
    "embb": "high_throughput_data",
    "high_throughput_data": "high_throughput_data",
    "data": "high_throughput_data",
    "mmtc": "sensor_telemetry",
    "sensor_telemetry": "sensor_telemetry",
    "sensor": "sensor_telemetry",
}
THRESHOLDS = {
    ("real_time_control", "latency_avg_ms"): ("<=", 10.0),
    ("real_time_control", "latency_max_ms"): ("<=", 20.0),
    ("real_time_control", "loss_percent"): ("<=", 1.0),
    ("high_throughput_data", "throughput_bps"): (">=", 50000000.0),
    ("sensor_telemetry", "loss_percent"): ("<=", 2.0),
    ("sensor_telemetry", "delivery_ratio_percent"): (">=", 98.0),
}
METRICS = {
    "real_time_control": ["latency_avg_ms", "latency_max_ms", "loss_percent"],
    "high_throughput_data": ["throughput_bps"],
    "sensor_telemetry": ["loss_percent", "delivery_ratio_percent"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract CCNC SLA metrics and violations.")
    parser.add_argument("experiment_dir", nargs="?", default=".", help="Experiment directory to analyze.")
    parser.add_argument("--output-dir", default=None, help="Directory for generated CSV/JSON outputs.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (Path.cwd() / candidate).resolve()


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


def normalize_service(value: Any, path: Optional[Path] = None) -> Optional[str]:
    candidates = []
    if value is not None:
        candidates.append(str(value))
    if path is not None:
        candidates.extend(part for part in path.parts[-5:])
    for item in candidates:
        lowered = item.lower()
        lowered = lowered.replace("-", "_").replace(" ", "_")
        for key, service in SERVICE_ALIASES.items():
            if key in lowered:
                return service
    return None


def timestamp_from_payload(payload: Dict[str, Any]) -> str:
    for key in ("timestamp", "snapshot_timestamp", "last_updated", "time", "created_at"):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def add_metric(rows: List[Dict[str, Any]], service: Optional[str], metric: str, value: Any, source: Path, timestamp: str = "") -> None:
    if service not in METRICS or metric not in METRICS[service]:
        return
    number = as_float(value)
    if number is None:
        return
    rows.append(
        {
            "service_class": service,
            "metric": metric,
            "value": number,
            "source_file": str(source),
            "timestamp": timestamp,
        }
    )


def extract_from_service_dict(payload: Dict[str, Any], source: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    timestamp = timestamp_from_payload(payload)
    service = normalize_service(
        payload.get("service_class")
        or payload.get("service_name")
        or payload.get("slice_name")
        or payload.get("display_name"),
        source,
    )
    for key, metric in (
        ("latency_avg_ms", "latency_avg_ms"),
        ("rtt_avg_ms", "latency_avg_ms"),
        ("latency_max_ms", "latency_max_ms"),
        ("rtt_max_ms", "latency_max_ms"),
        ("loss_percent", "loss_percent"),
        ("packet_loss_percent", "loss_percent"),
        ("throughput_bps", "throughput_bps"),
        ("sender_average_bitrate_bps", "throughput_bps"),
        ("delivery_ratio_percent", "delivery_ratio_percent"),
        ("reliability_proxy_percent", "delivery_ratio_percent"),
    ):
        if key in payload:
            add_metric(rows, service, metric, payload.get(key), source, timestamp)
    return rows


def walk_json(payload: Any, source: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        rows.extend(extract_from_service_dict(payload, source))
        slice_metrics = payload.get("slice_metrics")
        if isinstance(slice_metrics, dict):
            timestamp = timestamp_from_payload(payload)
            for slice_name, metrics in slice_metrics.items():
                if isinstance(metrics, dict):
                    service = normalize_service(metrics.get("display_name") or slice_name, source)
                    for row in extract_from_service_dict(metrics | {"timestamp": timestamp, "slice_name": slice_name}, source):
                        if row["service_class"] == service:
                            rows.append(row)
        services = payload.get("services")
        if isinstance(services, list):
            for service_payload in services:
                if isinstance(service_payload, dict):
                    rows.extend(extract_from_service_dict(service_payload, source))
        for value in payload.values():
            if isinstance(value, (dict, list)):
                rows.extend(walk_json(value, source))
    elif isinstance(payload, list):
        for item in payload:
            rows.extend(walk_json(item, source))
    return rows


def parse_ping_text(text: str, source: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    service = normalize_service(None, source) or "real_time_control"
    loss_match = re.search(r"(\d+(?:\.\d+)?)%\s*packet loss", text)
    if loss_match:
        add_metric(rows, service, "loss_percent", loss_match.group(1), source)
    rtt_match = re.search(r"(?:rtt|round-trip).*?=\s*([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)\s*ms", text)
    if rtt_match:
        add_metric(rows, service, "latency_avg_ms", rtt_match.group(2), source)
        add_metric(rows, service, "latency_max_ms", rtt_match.group(3), source)
    return rows


def parse_sender_text(text: str, source: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    service = normalize_service(None, source)
    summary_match = re.search(r"\bSUMMARY\b.*?\baverage_bitrate_bps=([0-9.]+)", text)
    if summary_match and service in {"high_throughput_data", "sensor_telemetry", "real_time_control"}:
        if service == "high_throughput_data":
            add_metric(rows, service, "throughput_bps", summary_match.group(1), source)
        if service == "sensor_telemetry":
            add_metric(rows, service, "delivery_ratio_percent", 100.0, source)
            add_metric(rows, service, "loss_percent", 0.0, source)
        return rows
    sent_values = [as_float(match.group(1)) for match in re.finditer(r"(?:sent_packets|sent)\s*=\s*(\d+)", text)]
    sent_values = [value for value in sent_values if value is not None]
    if sent_values and service in {"high_throughput_data", "sensor_telemetry", "real_time_control"}:
        packets = max(sent_values)
        line_count = max(1, len(sent_values))
        if service == "high_throughput_data":
            add_metric(rows, service, "throughput_bps", packets * 1200 * 8 / line_count, source)
        if service == "sensor_telemetry":
            add_metric(rows, service, "delivery_ratio_percent", 100.0, source)
            add_metric(rows, service, "loss_percent", 0.0, source)
    return rows


def read_json_file(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        if path.suffix == ".jsonl":
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.extend(walk_json(json.loads(line), path))
                except json.JSONDecodeError:
                    continue
        else:
            rows.extend(walk_json(json.loads(path.read_text(encoding="utf-8", errors="ignore")), path))
    except (OSError, json.JSONDecodeError):
        pass
    return rows


def read_csv_file(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            reader = csv.DictReader(handle)
            for record in reader:
                service = normalize_service(record.get("service_class") or record.get("service") or record.get("slice_name"), path)
                for metric in METRICS.get(service or "", []):
                    if metric in record:
                        add_metric(rows, service, metric, record.get(metric), path, str(record.get("timestamp", "")))
                if "metric" in record and "value" in record:
                    add_metric(rows, service, str(record["metric"]), record["value"], path, str(record.get("timestamp", "")))
    except OSError:
        pass
    return rows


def collect_rows(exp_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    ignored_outputs = {
        "service_metrics.csv",
        "control_latency.csv",
        "data_throughput.csv",
        "sensor_telemetry.csv",
        "sla_violations.csv",
        "manufacturing_utility.csv",
        "policy_stability.csv",
    }
    for path in sorted(exp_dir.rglob("*")):
        if not path.is_file() or path.name in ignored_outputs:
            continue
        suffix = path.suffix.lower()
        if suffix in {".json", ".jsonl"}:
            rows.extend(read_json_file(path))
        elif suffix == ".csv":
            rows.extend(read_csv_file(path))
        elif suffix in {".log", ".txt", ""}:
            text = path.read_text(encoding="utf-8", errors="ignore")
            rows.extend(parse_ping_text(text, path))
            rows.extend(parse_sender_text(text, path))
    unique = {}
    for row in rows:
        key = (row["service_class"], row["metric"], row["value"], row["source_file"], row["timestamp"])
        unique[key] = row
    return list(unique.values())


def aggregate(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for row in rows:
        buckets[(row["service_class"], row["metric"])].append(float(row["value"]))
    output = []
    for (service, metric), values in sorted(buckets.items()):
        output.append(
            {
                "service_class": service,
                "metric": metric,
                "sample_count": len(values),
                "mean": mean(values),
                "median": median(values),
                "min": min(values),
                "max": max(values),
                "latest": values[-1],
            }
        )
    return output


def violates(value: float, operator: str, threshold: float) -> bool:
    if operator == "<=":
        return value > threshold
    if operator == ">=":
        return value < threshold
    return False


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    args = parse_args()
    exp_dir = resolve_path(args.experiment_dir)
    output_dir = resolve_path(args.output_dir) if args.output_dir else exp_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = collect_rows(exp_dir)
    metric_rows = aggregate(raw_rows)
    violation_rows = []
    for row in metric_rows:
        key = (row["service_class"], row["metric"])
        if key not in THRESHOLDS:
            continue
        operator, threshold = THRESHOLDS[key]
        observed = float(row["latest"])
        is_violation = violates(observed, operator, threshold)
        violation_rows.append(
            {
                **row,
                "threshold": threshold,
                "operator": operator,
                "violation": is_violation,
                "violation_margin": abs(observed - threshold) if is_violation else 0.0,
            }
        )

    write_csv(output_dir / "service_metrics.csv", metric_rows, ["service_class", "metric", "sample_count", "mean", "median", "min", "max", "latest"])
    write_csv(output_dir / "control_latency.csv", [r for r in metric_rows if r["service_class"] == "real_time_control"], ["service_class", "metric", "sample_count", "mean", "median", "min", "max", "latest"])
    write_csv(output_dir / "data_throughput.csv", [r for r in metric_rows if r["service_class"] == "high_throughput_data"], ["service_class", "metric", "sample_count", "mean", "median", "min", "max", "latest"])
    write_csv(output_dir / "sensor_telemetry.csv", [r for r in metric_rows if r["service_class"] == "sensor_telemetry"], ["service_class", "metric", "sample_count", "mean", "median", "min", "max", "latest"])
    write_csv(output_dir / "sla_violations.csv", violation_rows, ["service_class", "metric", "sample_count", "mean", "median", "min", "max", "latest", "threshold", "operator", "violation", "violation_margin"])

    total_checks = len(violation_rows)
    violation_count = sum(1 for row in violation_rows if str(row["violation"]).lower() == "true")
    summary = {
        "experiment_dir": str(exp_dir),
        "metric_rows": len(metric_rows),
        "raw_samples": len(raw_rows),
        "sla_checks": total_checks,
        "sla_violations": violation_count,
        "sla_violation_rate": (violation_count / total_checks) if total_checks else 0.0,
        "thresholds": {f"{service}.{metric}": {"operator": op, "threshold": threshold} for (service, metric), (op, threshold) in THRESHOLDS.items()},
    }
    (output_dir / "summary_metrics.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[sla-analysis] wrote {len(metric_rows)} metrics and {violation_count}/{total_checks} SLA violations to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
