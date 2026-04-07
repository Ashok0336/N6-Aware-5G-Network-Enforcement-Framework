from __future__ import annotations

import csv
import datetime as dt
import json
import re
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


def coerce_bool(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, float) and value in {0.0, 1.0}:
        return bool(int(value))
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean value, got {value!r}.")


def load_structured_file(path: Path) -> Dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    stripped = raw_text.lstrip("\ufeff").strip()
    if not stripped:
        raise ValueError(f"{path} is empty. Expected a JSON or YAML mapping/object.")

    loaded: Any = None
    json_error: Exception | None = None
    yaml_error: Exception | None = None
    json_first = _should_try_json_first(path, stripped)

    if json_first:
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError as exc:
            json_error = exc

    if loaded is None:
        try:
            loaded = _load_yaml_text(raw_text, path)
        except Exception as exc:
            yaml_error = exc

    if loaded is None and not json_first:
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError as exc:
            json_error = exc

    if loaded is None:
        raise ValueError(_format_structured_file_error(path, json_error, yaml_error))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a top-level mapping/object.")
    return loaded


def _should_try_json_first(path: Path, stripped: str) -> bool:
    return path.suffix.lower() == ".json" or stripped[:1] in {"{", "["}


def _load_yaml_text(raw_text: str, path: Path) -> Any:
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = _load_basic_yaml(raw_text, path)
    return loaded


def _format_structured_file_error(
    path: Path,
    json_error: Exception | None,
    yaml_error: Exception | None,
) -> str:
    details = [f"Failed to parse {path} as JSON or YAML."]
    if json_error is not None:
        details.append(f"JSON parser error: {json_error}")
    if yaml_error is not None:
        details.append(f"YAML parser error: {yaml_error}")
    if yaml is None:
        details.append(
            "PyYAML is not installed; the built-in YAML fallback only supports basic mappings, lists, and scalars."
        )
    return " ".join(details)


def _load_basic_yaml(raw_text: str, path: Path) -> Any:
    lines = _tokenize_basic_yaml(raw_text, path)
    if not lines:
        return {}
    loaded, next_index = _parse_yaml_block(lines, 0, lines[0][0], path)
    if next_index != len(lines):
        line_number = lines[next_index][2]
        raise ValueError(f"Unexpected trailing content at line {line_number}.")
    return loaded


def _tokenize_basic_yaml(raw_text: str, path: Path) -> list[tuple[int, str, int]]:
    lines: list[tuple[int, str, int]] = []
    for line_number, raw_line in enumerate(raw_text.splitlines(), start=1):
        line = raw_line.rstrip()
        if not line.strip():
            continue
        leading_prefix = line[: len(line) - len(line.lstrip(" \t"))]
        if "\t" in leading_prefix:
            raise ValueError(
                f"{path}:{line_number}: tabs are not supported in YAML indentation."
            )
        cleaned = _strip_yaml_comment(line).rstrip()
        if not cleaned.strip():
            continue
        indent = len(cleaned) - len(cleaned.lstrip(" "))
        lines.append((indent, cleaned[indent:], line_number))
    return lines


def _strip_yaml_comment(line: str) -> str:
    quote: str | None = None
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if quote == '"' and char == "\\":
            escaped = True
            continue
        if char in {'"', "'"}:
            if quote == char:
                quote = None
            elif quote is None:
                quote = char
            continue
        if char == "#" and quote is None:
            if index == 0 or line[index - 1].isspace():
                return line[:index]
    return line


def _parse_yaml_block(
    lines: list[tuple[int, str, int]],
    start_index: int,
    indent: int,
    path: Path,
) -> tuple[Any, int]:
    if start_index >= len(lines):
        return {}, start_index
    line_indent, text, line_number = lines[start_index]
    if line_indent != indent:
        raise ValueError(
            f"{path}:{line_number}: expected indentation level {indent}, found {line_indent}."
        )
    if text == "-" or text.startswith("- "):
        return _parse_yaml_list(lines, start_index, indent, path)
    return _parse_yaml_mapping(lines, start_index, indent, path)


def _parse_yaml_mapping(
    lines: list[tuple[int, str, int]],
    start_index: int,
    indent: int,
    path: Path,
) -> tuple[Dict[str, Any], int]:
    mapping: Dict[str, Any] = {}
    index = start_index
    while index < len(lines):
        line_indent, text, line_number = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise ValueError(
                f"{path}:{line_number}: unexpected indentation inside mapping."
            )
        if text == "-" or text.startswith("- "):
            raise ValueError(
                f"{path}:{line_number}: list item found where a mapping entry was expected."
            )
        key, value_text = _split_yaml_key_value(text, path, line_number)
        if value_text:
            mapping[key] = _parse_yaml_scalar(value_text)
            index += 1
            continue
        next_index = index + 1
        if next_index >= len(lines) or lines[next_index][0] <= indent:
            mapping[key] = None
            index = next_index
            continue
        value, index = _parse_yaml_block(lines, next_index, lines[next_index][0], path)
        mapping[key] = value
    return mapping, index


def _parse_yaml_list(
    lines: list[tuple[int, str, int]],
    start_index: int,
    indent: int,
    path: Path,
) -> tuple[list[Any], int]:
    items: list[Any] = []
    index = start_index
    while index < len(lines):
        line_indent, text, line_number = lines[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise ValueError(
                f"{path}:{line_number}: unexpected indentation inside list."
            )
        if text == "-":
            item, index = _parse_list_item_block(lines, index, indent, path)
            items.append(item)
            continue
        if not text.startswith("- "):
            break
        remainder = text[2:].strip()
        key_value = _try_split_yaml_key_value(remainder)
        if key_value is None:
            items.append(_parse_yaml_scalar(remainder))
            index += 1
            continue

        key, value_text = key_value
        item: Dict[str, Any] = {}
        index += 1
        if value_text:
            item[key] = _parse_yaml_scalar(value_text)
        else:
            if index >= len(lines) or lines[index][0] <= indent:
                item[key] = None
            else:
                child, index = _parse_yaml_block(lines, index, lines[index][0], path)
                item[key] = child
        if index < len(lines) and lines[index][0] > indent:
            continuation, index = _parse_yaml_block(lines, index, lines[index][0], path)
            if not isinstance(continuation, dict):
                continuation_line = lines[index - 1][2]
                raise ValueError(
                    f"{path}:{continuation_line}: list item continuation must be a mapping."
                )
            item.update(continuation)
        items.append(item)
    return items, index


def _parse_list_item_block(
    lines: list[tuple[int, str, int]],
    start_index: int,
    indent: int,
    path: Path,
) -> tuple[Any, int]:
    index = start_index + 1
    if index >= len(lines) or lines[index][0] <= indent:
        return None, index
    return _parse_yaml_block(lines, index, lines[index][0], path)


def _split_yaml_key_value(text: str, path: Path, line_number: int) -> tuple[str, str]:
    result = _try_split_yaml_key_value(text)
    if result is None:
        raise ValueError(
            f"{path}:{line_number}: expected a 'key: value' mapping entry."
        )
    return result


def _try_split_yaml_key_value(text: str) -> tuple[str, str] | None:
    quote: str | None = None
    escaped = False
    for index, char in enumerate(text):
        if escaped:
            escaped = False
            continue
        if quote == '"' and char == "\\":
            escaped = True
            continue
        if char in {'"', "'"}:
            if quote == char:
                quote = None
            elif quote is None:
                quote = char
            continue
        if char == ":" and quote is None:
            next_char = text[index + 1] if index + 1 < len(text) else ""
            if next_char and not next_char.isspace():
                continue
            key = text[:index].strip()
            value = text[index + 1 :].strip()
            if not key:
                return None
            return key, value
    return None


def _parse_yaml_scalar(text: str) -> Any:
    value = text.strip()
    if value == "":
        return ""
    if value.startswith('"') and value.endswith('"'):
        return json.loads(value)
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1].replace("''", "'")
    lowered = value.lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    if lowered in {"null", "none", "~"}:
        return None
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?(?:\d+\.\d*|\d*\.\d+)", value):
        return float(value)
    return value


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
