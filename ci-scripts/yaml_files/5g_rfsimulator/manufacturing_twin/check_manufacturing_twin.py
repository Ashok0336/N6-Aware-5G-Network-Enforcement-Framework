#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_LATEST_PATH = (
    Path(__file__).resolve().parents[1]
    / "logs/manufacturing_twin/latest_machine_twin_state.json"
)
REQUIRED_FIELDS = [
    "timestamp",
    "manufacturing_phase",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Check manufacturing twin readiness")
    parser.add_argument(
        "--latest-path",
        type=Path,
        default=DEFAULT_LATEST_PATH,
        help="Path to latest_machine_twin_state.json",
    )
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        default=None,
        help="Optional maximum acceptable state age in seconds",
    )
    parser.add_argument(
        "--require-printer-operational",
        action="store_true",
        help="Fail if OctoPrint is reachable but the printer is offline/non-operational",
    )
    args = parser.parse_args()

    state, failures = load_and_validate(
        args.latest_path,
        args.max_age_seconds,
        require_printer_operational=args.require_printer_operational,
    )
    print_summary(state, failures)
    return 0 if not failures else 1


def load_and_validate(
    path: Path,
    max_age_seconds: float | None,
    *,
    require_printer_operational: bool = False,
) -> tuple[Dict[str, Any], List[str]]:
    latest_path = path.expanduser()
    failures: List[str] = []

    if not latest_path.exists():
        return {}, [f"latest state file does not exist: {latest_path}"]
    if not latest_path.is_file():
        return {}, [f"latest state path is not a file: {latest_path}"]

    try:
        with latest_path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
    except json.JSONDecodeError as exc:
        return {}, [f"latest state file is not valid JSON: {exc}"]
    except OSError as exc:
        return {}, [f"could not read latest state file: {exc}"]

    if not isinstance(state, dict):
        return {}, ["latest state JSON must be an object"]

    for field in REQUIRED_FIELDS:
        if not state.get(field):
            failures.append(f"missing required field: {field}")
    if state.get("octoprint_reachable") is not True:
        failures.append("octoprint_reachable is not true")
    if require_printer_operational and state.get("printer_operational") is not True:
        failures.append("printer_operational is not true")

    age = calculate_age_seconds(state.get("timestamp"))
    if age is None:
        failures.append("timestamp is not parseable")
    else:
        state["state_age_seconds"] = age
        if max_age_seconds is not None and age > max_age_seconds:
            failures.append(
                f"state is stale: {age:.3f}s old, max {max_age_seconds:.3f}s"
            )

    return state, failures


def calculate_age_seconds(timestamp: Any) -> float | None:
    if not timestamp:
        return None
    try:
        value = str(timestamp)
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        observed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - observed).total_seconds())


def print_summary(state: Dict[str, Any], failures: List[str]) -> None:
    readiness = "FAIL" if failures else "PASS"
    fields = {
        "machine_id": state.get("machine_id"),
        "octoprint_url": state.get("octoprint_url"),
        "octoprint_reachable": state.get("octoprint_reachable"),
        "printer_operational": state.get("printer_operational"),
        "availability": state.get("availability"),
        "printer_state_text": state.get("printer_state_text"),
        "job_state": state.get("job_state"),
        "manufacturing_phase": state.get("manufacturing_phase"),
        "job_progress_percent": state.get("job_progress_percent"),
        "nozzle_actual_c": state.get("nozzle_actual_c"),
        "bed_actual_c": state.get("bed_actual_c"),
        "service_criticality": state.get("service_criticality"),
        "state_age_seconds": _format_age(state.get("state_age_seconds")),
        "api_error": state.get("api_error"),
        "readiness": readiness,
    }
    for key, value in fields.items():
        print(f"{key}: {value}")
    if failures:
        print("failures:")
        for failure in failures:
            print(f"- {failure}")


def _format_age(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        return f"{value:.3f}"
    return None


if __name__ == "__main__":
    raise SystemExit(main())
