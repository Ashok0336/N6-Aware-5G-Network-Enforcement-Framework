#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Union

try:
    from .machine_state import MachineTwinState
except ImportError:
    from machine_state import MachineTwinState


CSV_FIELDS = [
    "timestamp",
    "octoprint_reachable",
    "printer_operational",
    "availability",
    "printer_state_text",
    "job_state",
    "manufacturing_phase",
    "job_progress_percent",
    "nozzle_actual_c",
    "bed_actual_c",
    "real_time_control_criticality",
    "high_throughput_data_criticality",
    "sensor_telemetry_criticality",
    "api_error",
]


def write_state(
    state: MachineTwinState,
    jsonl_path: Union[str, Path],
    latest_json_path: Union[str, Path],
    metrics_csv_path: Union[str, Path],
) -> None:
    append_jsonl(state, jsonl_path)
    write_latest_json(state, latest_json_path)
    append_metrics_csv(state, metrics_csv_path)


def append_jsonl(state: MachineTwinState, path: Union[str, Path]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(state.to_dict(), sort_keys=True) + "\n")
    return output_path


def write_latest_json(state: MachineTwinState, path: Union[str, Path]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(state.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")
    return output_path


def append_metrics_csv(state: MachineTwinState, path: Union[str, Path]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    exists = output_path.exists() and output_path.stat().st_size > 0
    with output_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(_metrics_row(state))
    return output_path


def _metrics_row(state: MachineTwinState) -> dict[str, object]:
    criticality = state.service_criticality or {}
    return {
        "timestamp": state.timestamp,
        "octoprint_reachable": state.octoprint_reachable,
        "printer_operational": state.printer_operational,
        "availability": state.availability,
        "printer_state_text": state.printer_state_text,
        "job_state": state.job_state,
        "manufacturing_phase": state.manufacturing_phase,
        "job_progress_percent": state.job_progress_percent,
        "nozzle_actual_c": state.nozzle_actual_c,
        "bed_actual_c": state.bed_actual_c,
        "real_time_control_criticality": criticality.get("real_time_control"),
        "high_throughput_data_criticality": criticality.get("high_throughput_data"),
        "sensor_telemetry_criticality": criticality.get("sensor_telemetry"),
        "api_error": state.api_error,
    }
