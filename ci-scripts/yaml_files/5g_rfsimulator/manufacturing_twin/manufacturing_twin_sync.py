#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from .machine_state import MachineTwinState
    from .manufacturing_twin_store import write_state
    from .octoprint_adapter import OctoPrintAdapter
    from .phase_mapper import map_phase, map_service_criticality
except ImportError:
    from machine_state import MachineTwinState
    from manufacturing_twin_store import write_state
    from octoprint_adapter import OctoPrintAdapter
    from phase_mapper import map_phase, map_service_criticality


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import yaml

        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
            return payload if isinstance(payload, dict) else {}
    except ImportError:
        return _load_simple_yaml(path)


def _load_simple_yaml(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    current_section: Optional[str] = None
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            if line.startswith("  ") and current_section:
                key, value = _split_yaml_scalar(line.strip())
                config.setdefault(current_section, {})[key] = value
                continue
            key, value = _split_yaml_scalar(line.strip())
            if value is None:
                current_section = key
                config[current_section] = {}
            else:
                current_section = None
                config[key] = value
    return config


def _split_yaml_scalar(line: str) -> tuple[str, Any]:
    key, _, value = line.partition(":")
    value = value.strip()
    if not value:
        return key.strip(), None
    try:
        return key.strip(), int(value)
    except ValueError:
        pass
    try:
        return key.strip(), float(value)
    except ValueError:
        return key.strip(), value.strip("'\"")


def build_machine_twin_state(raw_state: Dict[str, Any], config: Dict[str, Any]) -> MachineTwinState:
    printer = raw_state.get("printer") if isinstance(raw_state.get("printer"), dict) else {}
    job = raw_state.get("job") if isinstance(raw_state.get("job"), dict) else {}
    printer_state = printer.get("state") if isinstance(printer.get("state"), dict) else {}
    flags = printer_state.get("flags") if isinstance(printer_state.get("flags"), dict) else {}
    progress = job.get("progress") if isinstance(job.get("progress"), dict) else {}
    octoprint_reachable = _bool_or_default(
        raw_state.get("octoprint_reachable"),
        raw_state.get("version") is not None,
    )
    job_state = _first_string(raw_state.get("job_state"), job.get("state"))
    printer_state_text = _first_string(raw_state.get("printer_state_text"), printer_state.get("text"))
    printer_operational = _bool_or_default(
        raw_state.get("printer_operational"),
        _infer_printer_operational(flags, job_state, printer_state_text),
    )
    normalized_raw = dict(raw_state)
    normalized_raw.update(
        {
            "octoprint_reachable": octoprint_reachable,
            "printer_operational": printer_operational,
            "printer_state_text": printer_state_text,
            "job_state": job_state,
            "api_error": raw_state.get("api_error") or raw_state.get("error"),
        }
    )
    phase = map_phase(normalized_raw)

    return MachineTwinState(
        timestamp=utc_timestamp(),
        machine_id=str(config.get("machine_id", "ender3_01")),
        machine_type=str(config.get("machine_type", "Ender-3")),
        controller=str(config.get("controller", "OctoPrint")),
        octoprint_url=str(raw_state.get("octoprint_url") or ""),
        octoprint_reachable=octoprint_reachable,
        printer_operational=printer_operational,
        availability=str(raw_state.get("availability") or "unknown"),
        printer_state_text=printer_state_text,
        job_state=job_state,
        job_file=_job_file(job),
        job_progress_percent=_number(progress.get("completion")),
        print_time_seconds=_number(progress.get("printTime")),
        print_time_left_seconds=_number(progress.get("printTimeLeft")),
        nozzle_actual_c=_temperature(printer, "tool0", "actual"),
        nozzle_target_c=_temperature(printer, "tool0", "target"),
        bed_actual_c=_temperature(printer, "bed", "actual"),
        bed_target_c=_temperature(printer, "bed", "target"),
        manufacturing_phase=phase,
        service_criticality=map_service_criticality(phase),
        state_age_seconds=0.0,
        api_error=_string_or_none(normalized_raw.get("api_error")),
        raw_octoprint=normalized_raw,
    )


def run_once(
    config: Dict[str, Any],
    output_dir: Optional[Path],
    write_outputs: bool,
    mock_file: Optional[Path] = None,
) -> MachineTwinState:
    raw_state = load_mock_state(mock_file) if mock_file else OctoPrintAdapter().read_state()
    state = build_machine_twin_state(raw_state, config)
    if write_outputs:
        paths = output_paths(config, output_dir)
        write_state(state, paths["jsonl_path"], paths["latest_json_path"], paths["metrics_csv_path"])
    return state


def load_mock_state(path: Path) -> Dict[str, Any]:
    with path.expanduser().open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"mock file must contain a JSON object: {path}")
    raw_octoprint = payload.get("raw_octoprint")
    if isinstance(raw_octoprint, dict):
        return raw_octoprint
    return payload


def output_paths(config: Dict[str, Any], output_dir: Optional[Path]) -> Dict[str, Path]:
    outputs = config.get("outputs") if isinstance(config.get("outputs"), dict) else {}
    base = Path(__file__).resolve().parents[1]
    if output_dir:
        output_base = output_dir.expanduser().resolve()
        return {
            "jsonl_path": output_base / "machine_twin_state.jsonl",
            "latest_json_path": output_base / "latest_machine_twin_state.json",
            "metrics_csv_path": output_base / "machine_twin_metrics.csv",
        }
    return {
        "jsonl_path": _resolve_repo_path(outputs.get("jsonl_path"), base),
        "latest_json_path": _resolve_repo_path(outputs.get("latest_json_path"), base),
        "metrics_csv_path": _resolve_repo_path(outputs.get("metrics_csv_path"), base),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Telemetry-only Ender-3 manufacturing twin sync")
    parser.add_argument("--once", action="store_true", help="Read one sample and print JSON to stdout")
    parser.add_argument("--interval", type=float, default=None, help="Override polling interval in seconds")
    parser.add_argument("--output-dir", type=Path, default=None, help="Override output directory")
    parser.add_argument("--mock-file", type=Path, default=None, help="Read raw OctoPrint/mock JSON instead of calling OctoPrint")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to config.yaml")
    args = parser.parse_args()

    config = load_config(args.config.expanduser().resolve())
    interval = args.interval if args.interval is not None else float(config.get("poll_interval_seconds", 2))

    if args.once:
        state = run_once(config, args.output_dir, write_outputs=True, mock_file=args.mock_file)
        print(json.dumps(state.to_dict(), indent=2, sort_keys=True))
        return 0

    while True:
        state = run_once(config, args.output_dir, write_outputs=True, mock_file=args.mock_file)
        print(
            f"[manufacturing-twin] {state.timestamp} {state.availability} "
            f"{state.manufacturing_phase} {state.printer_state_text}",
            flush=True,
        )
        time.sleep(interval)


def _resolve_repo_path(value: Any, repo_root: Path) -> Path:
    candidate = Path(str(value or "logs/manufacturing_twin/machine_twin_state.jsonl")).expanduser()
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _temperature(printer: Dict[str, Any], section: str, field: str) -> float | None:
    temperature = printer.get("temperature") if isinstance(printer.get("temperature"), dict) else {}
    section_data = temperature.get(section) if isinstance(temperature.get(section), dict) else {}
    return _number(section_data.get(field))


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _first_string(*values: Any) -> str | None:
    for value in values:
        if value is not None:
            return str(value)
    return None


def _bool_or_default(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    if value is None:
        return default
    return bool(value)


def _infer_printer_operational(
    flags: Dict[str, Any],
    job_state: str | None,
    printer_state_text: str | None,
) -> bool:
    if flags.get("operational") is True:
        return True
    if flags.get("operational") is False:
        return False
    if str(job_state or "").lower() == "offline":
        return False
    state_text = str(printer_state_text or "").lower()
    if any(token in state_text for token in ("operational", "printing", "paused")):
        return True
    return False


def _job_file(job: Dict[str, Any]) -> str | None:
    file_info = job.get("file") if isinstance(job.get("file"), dict) else {}
    name = file_info.get("display") or file_info.get("name") or file_info.get("path")
    return None if name is None else str(name)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[manufacturing-twin] stopped", file=sys.stderr)
        raise SystemExit(130)
