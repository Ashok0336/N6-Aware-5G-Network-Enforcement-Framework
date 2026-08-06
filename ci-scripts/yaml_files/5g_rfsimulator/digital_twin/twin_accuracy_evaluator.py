#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional


SERVICE_MAP = {"urllc": "real_time_control", "embb": "high_throughput_data", "mmtc": "sensor_telemetry"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare telemetry metrics with digital twin snapshots.")
    parser.add_argument("experiment_dir", nargs="?", default=".", help="Experiment directory.")
    parser.add_argument("--telemetry-dir", default=None, help="Optional raw telemetry directory.")
    parser.add_argument("--snapshot-dir", default=None, help="Optional twin snapshot directory.")
    parser.add_argument("--max-age-seconds", type=float, default=5.0, help="Maximum timestamp difference allowed for a matched sample.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()


def normalize_service(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("-", "_").replace(" ", "_")
    return SERVICE_MAP.get(text, text)


def as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def read_json_records(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        if path.suffix == ".jsonl":
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    yield payload
        else:
            payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
            if isinstance(payload, dict):
                yield payload
    except (OSError, json.JSONDecodeError):
        return


def telemetry_metrics(base: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(base.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".json", ".jsonl"}:
            continue
        for payload in read_json_records(path):
            timestamp = payload.get("timestamp") or payload.get("last_updated") or payload.get("snapshot_timestamp")
            slices = payload.get("slice_metrics", {})
            if not isinstance(slices, dict):
                continue
            for slice_name, metrics in slices.items():
                if not isinstance(metrics, dict):
                    continue
                service = normalize_service(metrics.get("display_name") or slice_name)
                for source_key, metric_name in (
                    ("latency_avg_ms", "latency_avg_ms"),
                    ("latency_max_ms", "latency_max_ms"),
                    ("loss_percent", "loss_percent"),
                    ("throughput_bps", "throughput_bps"),
                    ("sender_average_bitrate_bps", "throughput_bps"),
                    ("delivery_ratio_percent", "delivery_ratio_percent"),
                ):
                    value = as_float(metrics.get(source_key))
                    if value is not None:
                        rows.append({"service_class": service, "metric_name": metric_name, "metric_value": value, "timestamp": timestamp, "source_file": str(path)})
    return rows


def snapshot_metrics(snapshot_dir: Path) -> List[Dict[str, Any]]:
    csv_path = snapshot_dir / "twin_snapshot_metrics.csv"
    rows: List[Dict[str, Any]] = []
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                value = as_float(row.get("metric_value"))
                if value is None:
                    continue
                rows.append(
                    {
                        "service_class": normalize_service(row.get("service_class")),
                        "metric_name": row.get("metric_name"),
                        "metric_value": value,
                        "timestamp": row.get("twin_timestamp") or row.get("snapshot_timestamp"),
                        "snapshot_timestamp": row.get("snapshot_timestamp"),
                        "source_file": row.get("source_file", ""),
                    }
                )
        return rows

    for path in sorted(snapshot_dir.rglob("twin_snapshots.jsonl")):
        for payload in read_json_records(path):
            timestamp = payload.get("twin_timestamp") or payload.get("snapshot_timestamp")
            for service in payload.get("services", []):
                if not isinstance(service, dict):
                    continue
                service_class = normalize_service(service.get("service_name") or service.get("slice_name"))
                for source_key, metric_name in (
                    ("latency_avg_ms", "latency_avg_ms"),
                    ("latency_max_ms", "latency_max_ms"),
                    ("packet_loss_percent", "loss_percent"),
                    ("throughput_bps", "throughput_bps"),
                ):
                    value = as_float(service.get(source_key))
                    if value is not None:
                        rows.append({"service_class": service_class, "metric_name": metric_name, "metric_value": value, "timestamp": timestamp, "snapshot_timestamp": payload.get("snapshot_timestamp"), "source_file": str(path)})
    return rows


def best_match(telemetry: Dict[str, Any], candidates: List[Dict[str, Any]], max_age_seconds: float) -> Optional[Dict[str, Any]]:
    service = telemetry["service_class"]
    metric = telemetry["metric_name"]
    telemetry_time = parse_time(telemetry.get("timestamp"))
    if telemetry_time is None:
        return None

    valid_matches = []
    for row in candidates:
        if row["service_class"] != service or row["metric_name"] != metric:
            continue
        twin_time = parse_time(row.get("timestamp"))
        if twin_time is None:
            continue
        staleness = abs((twin_time - telemetry_time).total_seconds())
        if staleness <= max_age_seconds:
            valid_matches.append((staleness, row))
    if not valid_matches:
        return None
    return min(valid_matches, key=lambda item: item[0])[1]


def main() -> int:
    args = parse_args()
    exp_dir = resolve_path(args.experiment_dir)
    telemetry_dir = resolve_path(args.telemetry_dir) if args.telemetry_dir else exp_dir
    snapshot_dir = resolve_path(args.snapshot_dir) if args.snapshot_dir else exp_dir
    exp_dir.mkdir(parents=True, exist_ok=True)

    telemetry_rows = telemetry_metrics(telemetry_dir)
    twin_rows = snapshot_metrics(snapshot_dir)
    matches = []
    for telemetry_row in telemetry_rows:
        telemetry_time = parse_time(telemetry_row.get("timestamp"))
        if telemetry_time is None:
            continue
        twin_row = best_match(telemetry_row, twin_rows, args.max_age_seconds)
        if twin_row is None:
            continue
        twin_time = parse_time(twin_row.get("timestamp"))
        if twin_time is None:
            continue
        staleness = abs((twin_time - telemetry_time).total_seconds())
        if staleness > args.max_age_seconds:
            continue
        observed = float(telemetry_row["metric_value"])
        twin = float(twin_row["metric_value"])
        absolute_error = abs(twin - observed)
        relative_error = absolute_error / abs(observed) if observed else (0.0 if twin == 0 else 1.0)
        matches.append(
            {
                "service_class": telemetry_row["service_class"],
                "metric_name": telemetry_row["metric_name"],
                "telemetry_value": observed,
                "twin_value": twin,
                "absolute_error": absolute_error,
                "relative_error": relative_error,
                "staleness_seconds": staleness,
                "telemetry_timestamp": telemetry_row.get("timestamp", ""),
                "twin_timestamp": twin_row.get("timestamp", ""),
            }
        )

    fields = ["service_class", "metric_name", "telemetry_value", "twin_value", "absolute_error", "relative_error", "staleness_seconds", "telemetry_timestamp", "twin_timestamp"]
    with (exp_dir / "twin_accuracy.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(matches)

    rel = [row["relative_error"] for row in matches]
    stale = [row["staleness_seconds"] for row in matches]
    summary = {
        "matched_samples": len(matches),
        "mean_relative_error": mean(rel) if rel else None,
        "median_relative_error": median(rel) if rel else None,
        "mean_staleness_seconds": mean(stale) if stale else None,
        "median_staleness_seconds": median(stale) if stale else None,
    }
    (exp_dir / "twin_accuracy_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[twin-accuracy] matched {len(matches)} samples in {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
