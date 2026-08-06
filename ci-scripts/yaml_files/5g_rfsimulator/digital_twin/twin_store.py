#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Deque, List, Union

try:
    from .twin_state import NetworkTwinState
except ImportError:
    from twin_state import NetworkTwinState


DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "../logs/digital_twin/twin_state.jsonl"
DEFAULT_LATEST_STATE_NAME = "latest_twin_state.json"


def _resolve_path(path: Union[str, Path, None]) -> Path:
    if path is None:
        return DEFAULT_STATE_PATH.resolve()
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return (Path(__file__).resolve().parent / candidate).resolve()


def append_state(state: NetworkTwinState, path: Union[str, Path, None] = None) -> Path:
    output_path = _resolve_path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = state.to_dict()
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    latest_path = output_path.parent / DEFAULT_LATEST_STATE_NAME
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def load_recent_states(path: Union[str, Path, None] = None, limit: int = 100) -> List[NetworkTwinState]:
    input_path = _resolve_path(path)
    if limit <= 0 or not input_path.exists():
        return []

    recent: Deque[NetworkTwinState] = deque(maxlen=limit)
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                recent.append(NetworkTwinState.from_dict(payload))
    return list(recent)
