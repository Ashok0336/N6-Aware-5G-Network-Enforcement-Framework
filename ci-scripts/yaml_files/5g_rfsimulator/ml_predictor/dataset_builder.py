#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
FEATURE_FIELDS = [
    "timestamp",
    "urllc_latency_avg_ms",
    "urllc_latency_max_ms",
    "embb_throughput_bps",
    "ovs_queue_1_throughput_bps",
    "ovs_queue_2_throughput_bps",
    "ovs_queue_3_throughput_bps",
    "ovs_controller_connected",
    "onos_ok",
]
LABEL_FIELDS = ["urllc_sla_violation", "embb_congestion_risk"]


def log(message: str) -> None:
    print(f"[ml-predictor] {message}", flush=True)


def load_config(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    if not path.exists():
        return config
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            config[key.strip()] = _coerce_scalar(value.strip())
    return config


def _coerce_scalar(value: str) -> Any:
    if value in {"", "null", "None"}:
        return None
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value.strip("'\"")


def resolve_config_path(value: Optional[str]) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def resolve_data_path(value: Any, config_path: Path) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


def load_twin_states(path: Path) -> List[Dict[str, Any]]:
    states: List[Dict[str, Any]] = []
    if not path.exists():
        log(f"no twin state file found at {path}")
        return states
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                log(f"skipping malformed twin state line: {exc}")
                continue
            if isinstance(payload, dict):
                states.append(payload)
    return states


def latest_twin_state(path: Path) -> Optional[Dict[str, Any]]:
    latest: Optional[Dict[str, Any]] = None
    for state in load_twin_states(path):
        latest = state
    return latest


def extract_features(state: Dict[str, Any]) -> Dict[str, Any]:
    services = {
        str(item.get("slice_name")): item
        for item in state.get("services", [])
        if isinstance(item, dict)
    }
    queues = {
        str(item.get("queue_id")): item
        for item in state.get("queues", [])
        if isinstance(item, dict)
    }
    urllc = services.get("urllc", {})
    embb = services.get("embb", {})
    ovs_status = state.get("ovs_status", {}) if isinstance(state.get("ovs_status"), dict) else {}
    onos_status = state.get("onos_status", {}) if isinstance(state.get("onos_status"), dict) else {}

    return {
        "timestamp": state.get("last_updated") or _first_timestamp(state),
        "urllc_latency_avg_ms": _number_or_empty(urllc.get("latency_avg_ms")),
        "urllc_latency_max_ms": _number_or_empty(urllc.get("latency_max_ms")),
        "embb_throughput_bps": _number_or_empty(embb.get("throughput_bps")),
        "ovs_queue_1_throughput_bps": _number_or_empty(_queue_throughput(queues.get("1", {}))),
        "ovs_queue_2_throughput_bps": _number_or_empty(_queue_throughput(queues.get("2", {}))),
        "ovs_queue_3_throughput_bps": _number_or_empty(_queue_throughput(queues.get("3", {}))),
        "ovs_controller_connected": _bool_as_int(ovs_status.get("controller_connected")),
        "onos_ok": _bool_as_int(onos_status.get("ok")),
    }


def extract_row(
    state: Dict[str, Any],
    urllc_latency_threshold_ms: float = 20.0,
    embb_throughput_threshold_bps: float = 85000000.0,
) -> Dict[str, Any]:
    row = extract_features(state)
    urllc_latency = _to_float(row.get("urllc_latency_avg_ms"))
    embb_throughput = _to_float(row.get("embb_throughput_bps"))
    row["urllc_sla_violation"] = int(
        urllc_latency is not None and urllc_latency > urllc_latency_threshold_ms
    )
    row["embb_congestion_risk"] = int(
        embb_throughput is not None and embb_throughput > embb_throughput_threshold_bps
    )
    return row


def build_dataset(
    twin_state_path: Path,
    dataset_output_path: Path,
    urllc_latency_threshold_ms: float = 20.0,
    embb_throughput_threshold_bps: float = 85000000.0,
) -> int:
    states = load_twin_states(twin_state_path)
    dataset_output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = FEATURE_FIELDS + LABEL_FIELDS
    with dataset_output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for state in states:
            writer.writerow(
                extract_row(
                    state,
                    urllc_latency_threshold_ms=urllc_latency_threshold_ms,
                    embb_throughput_threshold_bps=embb_throughput_threshold_bps,
                )
            )
    return len(states)


def _first_timestamp(state: Dict[str, Any]) -> Optional[str]:
    for section in ("services", "queues"):
        values = state.get(section)
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict) and item.get("timestamp"):
                return str(item.get("timestamp"))
    return None


def _queue_throughput(queue: Dict[str, Any]) -> Any:
    throughput = queue.get("throughput_bps") if isinstance(queue, dict) else None
    return throughput


def _number_or_empty(value: Any) -> Any:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return numeric


def _to_float(value: Any) -> Optional[float]:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_as_int(value: Any) -> int:
    return int(value is True)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a CSV ML dataset from Digital Twin JSONL state.")
    parser.add_argument("--config", help="Path to ml_predictor/config.yaml.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    twin_state_path = resolve_data_path(
        config.get("twin_state_path", "../logs/digital_twin/twin_state.jsonl"),
        config_path,
    )
    dataset_output_path = resolve_data_path(
        config.get("dataset_output_path", "../logs/ml_predictor/dataset.csv"),
        config_path,
    )
    rows = build_dataset(
        twin_state_path=twin_state_path,
        dataset_output_path=dataset_output_path,
        urllc_latency_threshold_ms=float(config.get("urllc_latency_threshold_ms", 20)),
        embb_throughput_threshold_bps=float(config.get("embb_throughput_threshold_bps", 85000000)),
    )
    log(f"read twin states from {twin_state_path}")
    log(f"wrote {rows} dataset rows to {dataset_output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

