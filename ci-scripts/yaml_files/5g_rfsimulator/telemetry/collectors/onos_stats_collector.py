from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from typing import Any, Dict, List


class OnosStatsCollector:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.base_url = str(config.get("base_url", "http://192.168.71.160:8181")).rstrip("/")
        self.devices_path = str(config.get("devices_path", "/onos/v1/devices"))
        self.username = str(config.get("username", "onos"))
        self.password = str(config.get("password", "rocks"))
        self.timeout_seconds = float(config.get("timeout_seconds", 5))

    def collect(self) -> Dict[str, Any]:
        devices_result = self._request_json(self.devices_path)
        devices = []
        if devices_result["ok"] and isinstance(devices_result.get("data"), dict):
            devices = devices_result["data"].get("devices", [])
        available_devices = [
            device for device in devices if isinstance(device, dict) and device.get("available") is True
        ]

        flows_by_device: Dict[str, Any] = {}
        if available_devices:
            for device in available_devices:
                device_id = str(device.get("id") or "")
                if not device_id:
                    continue
                flow_result = self._request_json(f"/onos/v1/flows/{device_id}")
                flow_payload = flow_result.get("data") if isinstance(flow_result.get("data"), dict) else {}
                flows_by_device[device_id] = {
                    "ok": flow_result.get("ok", False),
                    "flow_count": len(flow_payload.get("flows", [])) if isinstance(flow_payload, dict) else 0,
                    "flows": flow_payload.get("flows", []) if isinstance(flow_payload, dict) else [],
                    "error": flow_result.get("error", ""),
                }

        return {
            "ok": bool(devices_result.get("ok")),
            "device_count": len(devices),
            "available_device_count": len(available_devices),
            "devices": devices,
            "flows_by_device": flows_by_device,
            "error": devices_result.get("error", ""),
        }

    def _request_json(self, path: str) -> Dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        url = self.base_url + path
        request = urllib.request.Request(url, method="GET")
        auth_value = f"{self.username}:{self.password}".encode("utf-8")
        request.add_header("Authorization", f"Basic {base64.b64encode(auth_value).decode('ascii')}")
        request.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8", errors="replace")
                data = json.loads(body) if body.strip() else {}
                return {"ok": True, "status": getattr(response, "status", 200), "data": data, "error": ""}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return {"ok": False, "status": exc.code, "data": {}, "error": body or str(exc)}
        except urllib.error.URLError as exc:
            return {"ok": False, "status": None, "data": {}, "error": str(exc.reason)}
        except Exception as exc:  # pragma: no cover - defensive
            return {"ok": False, "status": None, "data": {}, "error": str(exc)}
