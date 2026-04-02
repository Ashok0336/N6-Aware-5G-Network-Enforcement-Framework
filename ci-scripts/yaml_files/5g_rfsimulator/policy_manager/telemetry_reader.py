from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class TelemetryReader:
    def __init__(self, telemetry_dir: Path, telemetry_glob: str) -> None:
        self.telemetry_dir = telemetry_dir
        self.telemetry_glob = telemetry_glob
        self.last_signature = ""

    def read_latest_snapshot(self) -> Dict[str, Any] | None:
        if not self.telemetry_dir.exists():
            return None
        files = sorted(self.telemetry_dir.glob(self.telemetry_glob), key=lambda item: item.stat().st_mtime)
        if not files:
            return None
        latest = files[-1]
        last_line = self._read_last_line(latest)
        if not last_line:
            return None
        payload = json.loads(last_line)
        signature = f"{latest}:{payload.get('timestamp')}:{payload.get('snapshot_index')}"
        if signature == self.last_signature:
            return None
        self.last_signature = signature
        payload["_telemetry_file"] = str(latest.resolve())
        return payload

    def _read_last_line(self, path: Path) -> str:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            block = min(size, 131072)
            handle.seek(max(0, size - block))
            text = handle.read().decode("utf-8", errors="replace")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines[-1] if lines else ""
