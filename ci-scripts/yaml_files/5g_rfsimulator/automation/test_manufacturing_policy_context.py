#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

TESTBED_DIR = Path(__file__).resolve().parents[1]

from policy_manager import (
    build_manufacturing_policy_context,
    get_machine_service_criticality,
    get_manufacturing_phase,
    is_machine_twin_fresh,
    read_manufacturing_twin_state,
)

if str(TESTBED_DIR) not in sys.path:
    sys.path.insert(0, str(TESTBED_DIR))
from manufacturing_twin.manufacturing_twin_sync import build_machine_twin_state


SAMPLE_DIR = TESTBED_DIR / "manufacturing_twin/sample_states"


def main() -> int:
    test_active_printing_protects_control_and_sensor()
    test_idle_does_not_force_emergency_policy()
    test_printer_offline_does_not_crash()
    test_missing_file_does_not_crash()
    print("PASS manufacturing policy context tests")
    return 0


def test_active_printing_protects_control_and_sensor() -> None:
    machine_twin = _fresh_sample("active_printing.json")
    context = build_manufacturing_policy_context(
        machine_twin,
        enabled=True,
        latest_path="mock-active",
        max_age_seconds=10,
    )
    assert context["status"] == "fresh"
    assert get_manufacturing_phase(machine_twin) == "active_printing"
    assert is_machine_twin_fresh(machine_twin, 10)
    assert set(context["protected_service_classes"]) == {
        "real_time_control",
        "sensor_telemetry",
    }
    criticality = get_machine_service_criticality(machine_twin)
    assert criticality["real_time_control"] == "critical"
    assert criticality["sensor_telemetry"] == "high"


def test_idle_does_not_force_emergency_policy() -> None:
    machine_twin = _fresh_sample("idle_operational.json")
    context = build_manufacturing_policy_context(
        machine_twin,
        enabled=True,
        latest_path="mock-idle",
        max_age_seconds=10,
    )
    assert context["status"] == "fresh"
    assert context["phase"] == "idle"
    assert context["protected_service_classes"] == []


def test_printer_offline_does_not_crash() -> None:
    machine_twin = _fresh_sample("printer_offline.json")
    context = build_manufacturing_policy_context(
        machine_twin,
        enabled=True,
        latest_path="mock-offline",
        max_age_seconds=10,
    )
    assert context["status"] == "fresh"
    assert context["phase"] == "printer_offline"
    assert context["protected_service_classes"] == []


def test_missing_file_does_not_crash() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        missing_path = Path(tmpdir) / "missing_latest_machine_twin_state.json"
        machine_twin = read_manufacturing_twin_state(missing_path)
    context = build_manufacturing_policy_context(
        machine_twin,
        enabled=True,
        latest_path=missing_path,
        max_age_seconds=10,
    )
    assert machine_twin is None
    assert context["status"] == "missing_or_stale"
    assert context["fresh"] is False
    assert context["protected_service_classes"] == []


def _fresh_sample(filename: str) -> dict[str, object]:
    path = SAMPLE_DIR / filename
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    payload["timestamp"] = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    state = build_machine_twin_state(
        payload,
        {
            "machine_id": "ender3_01",
            "machine_type": "Ender-3",
            "controller": "OctoPrint",
        },
    )
    return state.to_dict()


if __name__ == "__main__":
    raise SystemExit(main())
