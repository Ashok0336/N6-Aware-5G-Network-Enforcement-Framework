#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

try:
    from .manufacturing_twin_sync import build_machine_twin_state
except ImportError:
    from manufacturing_twin_sync import build_machine_twin_state


SAMPLE_DIR = Path(__file__).resolve().parent / "sample_states"

EXPECTED = {
    "octoprint_unreachable.json": (
        "octoprint_unreachable",
        {
            "real_time_control": "unknown",
            "high_throughput_data": "unknown",
            "sensor_telemetry": "unknown",
        },
    ),
    "printer_offline.json": (
        "printer_offline",
        {
            "real_time_control": "unknown",
            "high_throughput_data": "unknown",
            "sensor_telemetry": "unknown",
        },
    ),
    "idle_operational.json": (
        "idle",
        {
            "real_time_control": "low",
            "high_throughput_data": "low",
            "sensor_telemetry": "low",
        },
    ),
    "print_initialization.json": (
        "print_initialization",
        {
            "real_time_control": "critical",
            "high_throughput_data": "low",
            "sensor_telemetry": "medium",
        },
    ),
    "active_printing.json": (
        "active_printing",
        {
            "real_time_control": "critical",
            "high_throughput_data": "low",
            "sensor_telemetry": "high",
        },
    ),
    "print_completion.json": (
        "print_completion",
        {
            "real_time_control": "high",
            "high_throughput_data": "low",
            "sensor_telemetry": "medium",
        },
    ),
    "paused.json": (
        "paused",
        {
            "real_time_control": "critical",
            "high_throughput_data": "low",
            "sensor_telemetry": "critical",
        },
    ),
    "machine_error.json": (
        "machine_error",
        {
            "real_time_control": "critical",
            "high_throughput_data": "low",
            "sensor_telemetry": "critical",
        },
    ),
}


def main() -> int:
    config = {
        "machine_id": "ender3_01",
        "machine_type": "Ender-3",
        "controller": "OctoPrint",
    }
    passed = 0
    for filename, (expected_phase, expected_criticality) in EXPECTED.items():
        raw_state = _load_sample(filename)
        state = build_machine_twin_state(raw_state, config)
        assert state.manufacturing_phase == expected_phase, (
            f"{filename}: expected phase {expected_phase}, "
            f"got {state.manufacturing_phase}"
        )
        assert state.service_criticality == expected_criticality, (
            f"{filename}: expected criticality {expected_criticality}, "
            f"got {state.service_criticality}"
        )
        passed += 1
        print(f"PASS {filename} -> {state.manufacturing_phase}")
    print(f"PASS {passed} manufacturing twin phase sample(s)")
    return 0


def _load_sample(filename: str) -> Dict[str, object]:
    path = SAMPLE_DIR / filename
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert isinstance(payload, dict), f"{path} must contain a JSON object"
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
