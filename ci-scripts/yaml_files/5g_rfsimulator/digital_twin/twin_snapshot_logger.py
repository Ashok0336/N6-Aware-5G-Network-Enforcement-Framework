#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "logs/digital_twin/twin_state.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Log CCNC digital twin snapshots.")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE), help="Digital twin state JSONL file.")
    parser.add_argument("--output-dir", default=".", help="Output directory for snapshot logs.")
    parser.add_argument("--interval", type=float, default=2.0, help="Polling interval in seconds.")
    parser.add_argument("--duration", type=float, default=0.0, help="Duration in seconds. Zero logs one snapshot.")
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def latest_json_line(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    latest = None
    try:
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
                    latest = payload
    except OSError:
        return None
    return latest


def service_name(payload: Dict[str, Any]) -> str:
    name = str(payload.get("service_class") or payload.get("service_name") or payload.get("display_name") or payload.get("slice_name") or "")
    lowered = name.lower()
    if lowered in {"urllc", "real_time_control"}:
        return "real_time_control"
    if lowered in {"embb", "high_throughput_data"}:
        return "high_throughput_data"
    if lowered in {"mmtc", "sensor_telemetry"}:
        return "sensor_telemetry"
    return name or "unknown"


def metric_rows(snapshot: Dict[str, Any], source_file: Path, snapshot_time: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for service in snapshot.get("services", []):
        if not isinstance(service, dict):
            continue
        normalized = service_name(service)
        twin_time = service.get("timestamp") or snapshot.get("last_updated") or ""
        for source_key, metric_name in (
            ("latency_avg_ms", "latency_avg_ms"),
            ("latency_max_ms", "latency_max_ms"),
            ("packet_loss_percent", "loss_percent"),
            ("loss_percent", "loss_percent"),
            ("throughput_bps", "throughput_bps"),
            ("delivery_ratio_percent", "delivery_ratio_percent"),
        ):
            value = service.get(source_key)
            if value is None:
                continue
            rows.append(
                {
                    "snapshot_timestamp": snapshot_time,
                    "twin_timestamp": twin_time,
                    "service_class": normalized,
                    "metric_name": metric_name,
                    "metric_value": value,
                    "source_file": str(source_file),
                }
            )
    return rows


def write_metrics(path: Path, rows: Iterable[Dict[str, Any]], append: bool) -> None:
    fields = ["snapshot_timestamp", "twin_timestamp", "service_class", "metric_name", "metric_value", "source_file"]
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if mode == "w":
            writer.writeheader()
        writer.writerows(rows)


def log_once(state_file: Path, output_dir: Path) -> bool:
    snapshot_time = utc_now()
    payload = latest_json_line(state_file)
    if payload is None:
        payload = {"services": [], "queues": [], "ovs_status": {}, "onos_status": {}, "last_updated": None}
    event = {
        "event_type": "twin_snapshot",
        "snapshot_timestamp": snapshot_time,
        "twin_timestamp": payload.get("last_updated"),
        "services": payload.get("services", []),
        "network": {
            "queues": payload.get("queues", []),
            "ovs_status": payload.get("ovs_status", {}),
            "onos_status": payload.get("onos_status", {}),
        },
        "source_file": str(state_file),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "twin_snapshots.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    write_metrics(output_dir / "twin_snapshot_metrics.csv", metric_rows(payload, state_file, snapshot_time), append=True)
    return True


def main() -> int:
    args = parse_args()
    state_file = resolve_path(args.state_file)
    output_dir = resolve_path(args.output_dir)
    if args.duration <= 0:
        log_once(state_file, output_dir)
    else:
        end = time.monotonic() + args.duration
        while time.monotonic() <= end:
            log_once(state_file, output_dir)
            time.sleep(max(0.1, args.interval))
    print(f"[twin-snapshot-logger] snapshots written to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
