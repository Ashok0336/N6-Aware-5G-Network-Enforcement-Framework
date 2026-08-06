#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Dict


LOW = "low"
MEDIUM = "medium"
HIGH = "high"
CRITICAL = "critical"
UNKNOWN = "unknown"


def map_phase(state_dict: Dict[str, Any]) -> str:
    if state_dict.get("octoprint_reachable") is False:
        return "octoprint_unreachable"

    printer_state_text = _printer_state_text(state_dict)
    job_state = str(state_dict.get("job_state") or _job_state(state_dict) or "")
    progress = _number(_progress_value(state_dict))
    state_text = printer_state_text.lower()

    if job_state.lower() == "offline" or state_dict.get("printer_operational") is False:
        return "printer_offline"
    if "error" in state_text:
        return "machine_error"
    if "paused" in state_text:
        return "paused"
    if "printing" in state_text:
        if progress is not None and progress < 5.0:
            return "print_initialization"
        if progress is not None and progress >= 95.0:
            return "print_completion"
        return "active_printing"
    if "operational" in state_text:
        return "idle"
    return "unknown"


def map_service_criticality(phase: str) -> Dict[str, str]:
    if phase == "job_upload":
        return {
            "high_throughput_data": CRITICAL,
            "real_time_control": MEDIUM,
            "sensor_telemetry": LOW,
        }
    if phase == "print_initialization":
        return {
            "real_time_control": CRITICAL,
            "sensor_telemetry": MEDIUM,
            "high_throughput_data": LOW,
        }
    if phase == "active_printing":
        return {
            "real_time_control": CRITICAL,
            "sensor_telemetry": HIGH,
            "high_throughput_data": LOW,
        }
    if phase == "print_completion":
        return {
            "real_time_control": HIGH,
            "sensor_telemetry": MEDIUM,
            "high_throughput_data": LOW,
        }
    if phase in {"paused", "machine_error"}:
        return {
            "real_time_control": CRITICAL,
            "sensor_telemetry": CRITICAL,
            "high_throughput_data": LOW,
        }
    if phase == "idle":
        return {
            "real_time_control": LOW,
            "sensor_telemetry": LOW,
            "high_throughput_data": LOW,
        }
    if phase in {"octoprint_unreachable", "printer_offline"}:
        return {
            "real_time_control": UNKNOWN,
            "sensor_telemetry": UNKNOWN,
            "high_throughput_data": UNKNOWN,
        }
    return {
        "real_time_control": UNKNOWN,
        "sensor_telemetry": UNKNOWN,
        "high_throughput_data": UNKNOWN,
    }


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _job_file(job: Dict[str, Any]) -> str | None:
    file_info = job.get("file") if isinstance(job.get("file"), dict) else {}
    name = file_info.get("display") or file_info.get("name") or file_info.get("path")
    return None if name is None else str(name)


def _printer_state_text(state_dict: Dict[str, Any]) -> str:
    if state_dict.get("printer_state_text") is not None:
        return str(state_dict.get("printer_state_text") or "")
    printer = state_dict.get("printer") if isinstance(state_dict.get("printer"), dict) else {}
    state = printer.get("state") if isinstance(printer.get("state"), dict) else {}
    return str(state.get("text") or "")


def _job_state(state_dict: Dict[str, Any]) -> str | None:
    job = state_dict.get("job") if isinstance(state_dict.get("job"), dict) else {}
    return None if job.get("state") is None else str(job.get("state"))


def _progress_value(state_dict: Dict[str, Any]) -> Any:
    if state_dict.get("job_progress_percent") is not None:
        return state_dict.get("job_progress_percent")
    job = state_dict.get("job") if isinstance(state_dict.get("job"), dict) else {}
    progress = job.get("progress") if isinstance(job.get("progress"), dict) else {}
    return progress.get("completion")
