from __future__ import annotations

import json
from typing import Any, Dict, List

from policy_manager.utils import parse_usage_pair, run_command


class DockerStatsCollector:
    def __init__(self, config: Dict[str, Any], command_timeout_seconds: float) -> None:
        self.container_names = [str(item) for item in config.get("containers", [])]
        self.command_timeout_seconds = command_timeout_seconds

    def collect(self) -> Dict[str, Any]:
        if not self.container_names:
            return {"containers": {}, "ok": True}

        command = [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            *self.container_names,
        ]
        result = run_command(command, timeout_seconds=self.command_timeout_seconds)
        containers: Dict[str, Any] = {}
        for line in str(result.get("stdout", "")).splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            name = str(payload.get("Name") or payload.get("Container") or "")
            if not name:
                continue
            containers[name] = {
                "cpu_percent": _parse_percent(payload.get("CPUPerc")),
                "memory_percent": _parse_percent(payload.get("MemPerc")),
                "memory": parse_usage_pair(payload.get("MemUsage")),
                "network": parse_usage_pair(payload.get("NetIO")),
                "block_io": parse_usage_pair(payload.get("BlockIO")),
                "pids": _parse_int(payload.get("PIDs")),
                "raw": payload,
            }
        return {
            "ok": bool(result.get("ok")),
            "containers": containers,
            "error": result.get("error", ""),
            "stdout": result.get("stdout", ""),
            "stderr": result.get("stderr", ""),
        }


def _parse_percent(value: Any) -> float | None:
    text = str(value or "").strip().rstrip("%")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None
