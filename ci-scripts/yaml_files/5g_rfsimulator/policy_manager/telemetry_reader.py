from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict


class TelemetryReader:
    def __init__(self, telemetry_dir: Path, telemetry_glob: str) -> None:
        self.telemetry_dir = telemetry_dir
        self.telemetry_glob = telemetry_glob
        self.last_signature = ""

    def read_latest_snapshot(self) -> Dict[str, Any] | None:
        for attempt in range(3):
            payload = self._read_latest_valid_snapshot()
            if payload is not None:
                return payload
            if attempt < 2:
                time.sleep(0.2)
        return None

    def _read_latest_valid_snapshot(self) -> Dict[str, Any] | None:
        latest = self._latest_file()
        if latest is None:
            return None

        for line in reversed(self._read_recent_lines(latest)):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[telemetry-reader][WARN] skipping invalid telemetry line in {latest}: {exc}",
                    file=sys.stderr,
                )
                continue
            if not isinstance(payload, dict):
                print(
                    f"[telemetry-reader][WARN] skipping non-object telemetry line in {latest}",
                    file=sys.stderr,
                )
                continue
            return self._deduplicate(latest, payload)
        return None

    def _deduplicate(self, latest: Path, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        signature = f"{latest}:{payload.get('timestamp')}:{payload.get('snapshot_index')}"
        if signature == self.last_signature:
            return None
        self.last_signature = signature
        payload["_telemetry_file"] = str(latest.resolve())
        return payload

    def _latest_file(self) -> Path | None:
        if not self.telemetry_dir.exists():
            return None
        files = sorted(self.telemetry_dir.glob(self.telemetry_glob), key=lambda item: item.stat().st_mtime)
        if not files:
            return None
        return files[-1]

    def _read_recent_lines(self, path: Path) -> list[str]:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            block = min(size, 8 * 1024 * 1024)
            handle.seek(max(0, size - block))
            text = handle.read().decode("utf-8", errors="replace")
        return [line.strip() for line in text.splitlines() if line.strip()]
