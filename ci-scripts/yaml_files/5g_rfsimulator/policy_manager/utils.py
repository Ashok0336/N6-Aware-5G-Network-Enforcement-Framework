from __future__ import annotations

import csv
import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Any, Dict

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_structured_file(path: Path) -> Dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a top-level mapping/object.")
    return loaded


def run_command(args: list[str], timeout_seconds: float = 10.0) -> Dict[str, Any]:
    try:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "return_code": None,
            "stdout": "",
            "stderr": "",
            "error": f"command timed out after {timeout_seconds}s",
            "command": args,
        }
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "ok": False,
            "return_code": None,
            "stdout": "",
            "stderr": "",
            "error": str(exc),
            "command": args,
        }
    return {
        "ok": completed.returncode == 0,
        "return_code": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "error": "" if completed.returncode == 0 else (completed.stderr.strip() or completed.stdout.strip()),
        "command": args,
    }


def normalize_path(base_dir: Path, value: Any) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def parse_size_token(text: str) -> int | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        pass
    units = {
        "b": 1,
        "kb": 1_000,
        "mb": 1_000_000,
        "gb": 1_000_000_000,
        "tb": 1_000_000_000_000,
        "kib": 1024,
        "mib": 1024 ** 2,
        "gib": 1024 ** 3,
        "tib": 1024 ** 4,
    }
    number = ""
    unit = ""
    for char in cleaned:
        if char.isdigit() or char == ".":
            number += char
        else:
            unit += char
    unit = unit.strip().lower()
    if not number:
        return None
    factor = units.get(unit)
    if factor is None:
        return None
    return int(float(number) * factor)


def parse_usage_pair(text: Any) -> Dict[str, Any]:
    raw = str(text or "").strip()
    result: Dict[str, Any] = {"raw": raw}
    if "/" not in raw:
        return result
    used, total = [part.strip() for part in raw.split("/", 1)]
    result["used_bytes"] = parse_size_token(used)
    result["total_bytes"] = parse_size_token(total)
    return result


def profile_signature(profile: Dict[str, Any]) -> str:
    return json.dumps(profile, sort_keys=True)


def merge_queue_profile(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = json.loads(json.dumps(base))
    if overlay.get("parent_max_rate_bps") is not None:
        merged["parent_max_rate_bps"] = int(overlay["parent_max_rate_bps"])
    for queue_id, queue_cfg in dict(overlay.get("queues", {})).items():
        merged.setdefault("queues", {}).setdefault(str(queue_id), {})
        for key in ("min_rate_bps", "max_rate_bps"):
            if queue_cfg.get(key) is not None:
                merged["queues"][str(queue_id)][key] = int(queue_cfg[key])
    return merged


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    ensure_directory(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def append_csv_row(path: Path, row: Dict[str, Any], fieldnames: list[str]) -> None:
    ensure_directory(path.parent)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def parse_ovs_map(text: str) -> Dict[str, str]:
    stripped = str(text or "").strip()
    if stripped in {"", "[]", "{}"}:
        return {}
    if stripped.startswith("{") and stripped.endswith("}"):
        stripped = stripped[1:-1].strip()
    result: Dict[str, str] = {}
    for item in stripped.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        result[key.strip().strip('"')] = value.strip().strip('"')
    return result
