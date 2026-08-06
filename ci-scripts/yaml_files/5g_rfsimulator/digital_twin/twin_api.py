#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from .twin_store import DEFAULT_STATE_PATH, load_recent_states
except ImportError:
    from twin_store import DEFAULT_STATE_PATH, load_recent_states


class TwinRequestHandler(BaseHTTPRequestHandler):
    state_path: Path = DEFAULT_STATE_PATH.resolve()

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json({"ok": True, "service": "digital_twin"})
            return
        if self.path == "/state/latest":
            latest = self._latest_state()
            if latest is None:
                self._send_json({"state": None, "error": "no twin state available"}, status=404)
                return
            self._send_json(latest)
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[digital-twin-api] {self.address_string()} {fmt % args}", flush=True)

    def _latest_state(self) -> Optional[Dict[str, Any]]:
        states = load_recent_states(self.state_path, limit=1)
        if not states:
            return None
        return states[-1].to_dict()

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HTTP API for the Digital Twin state log.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host.")
    parser.add_argument("--port", type=int, default=8096, help="Bind port.")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH.resolve()), help="Twin JSONL state path.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    TwinRequestHandler.state_path = Path(args.state_path).expanduser().resolve()
    server = ThreadingHTTPServer((args.host, args.port), TwinRequestHandler)
    print(
        f"[digital-twin-api] listening on http://{args.host}:{args.port} "
        f"state_path={TwinRequestHandler.state_path}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[digital-twin-api] stopped", flush=True)
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

