#!/usr/bin/env python3
"""Reusable ONOS REST client helpers for the N6 slicing automation scripts."""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


def _body_excerpt(payload: Optional[str], limit: int = 600) -> str:
    text = (payload or "").strip()
    return text[:limit]


class OnosClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: float = 5.0,
        dry_run: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.dry_run = dry_run

    def _make_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def request(
        self, method: str, path: str, payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = self._make_url(path)
        request_body_text = json.dumps(payload, sort_keys=True) if payload is not None else ""
        audit = {
            "request": {
                "method": method.upper(),
                "url": url,
                "payload": payload,
                "payload_excerpt": _body_excerpt(request_body_text),
                "auth_username": self.username,
                "dry_run": self.dry_run,
            }
        }

        if self.dry_run:
            audit["response"] = {
                "ok": True,
                "status": None,
                "body_excerpt": "",
                "dry_run": True,
            }
            return {
                "ok": True,
                "status": None,
                "data": None,
                "body_text": "",
                "error": "",
                "audit": audit,
                "dry_run": True,
            }

        body_bytes = request_body_text.encode("utf-8") if request_body_text else None
        request = urllib.request.Request(url=url, data=body_bytes, method=method.upper())
        auth_value = f"{self.username}:{self.password}"
        encoded_auth = base64.b64encode(auth_value.encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {encoded_auth}")
        request.add_header("Accept", "application/json")
        if body_bytes is not None:
            request.add_header("Content-Type", "application/json")

        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                body_text = response.read().decode("utf-8", errors="replace")
                duration_ms = int((time.monotonic() - started) * 1000)
                parsed = None
                if body_text.strip():
                    try:
                        parsed = json.loads(body_text)
                    except json.JSONDecodeError:
                        parsed = None
                audit["response"] = {
                    "ok": True,
                    "status": getattr(response, "status", 200),
                    "duration_ms": duration_ms,
                    "body_excerpt": _body_excerpt(body_text),
                }
                return {
                    "ok": True,
                    "status": getattr(response, "status", 200),
                    "data": parsed,
                    "body_text": body_text,
                    "error": "",
                    "audit": audit,
                    "dry_run": False,
                }
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            duration_ms = int((time.monotonic() - started) * 1000)
            audit["response"] = {
                "ok": False,
                "status": exc.code,
                "duration_ms": duration_ms,
                "body_excerpt": _body_excerpt(body_text),
                "error": str(exc.reason),
            }
            return {
                "ok": False,
                "status": exc.code,
                "data": None,
                "body_text": body_text,
                "error": str(exc.reason),
                "audit": audit,
                "dry_run": False,
            }
        except urllib.error.URLError as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            audit["response"] = {
                "ok": False,
                "status": None,
                "duration_ms": duration_ms,
                "body_excerpt": "",
                "error": str(exc.reason),
            }
            return {
                "ok": False,
                "status": None,
                "data": None,
                "body_text": "",
                "error": str(exc.reason),
                "audit": audit,
                "dry_run": False,
            }
        except Exception as exc:  # pragma: no cover - defensive
            duration_ms = int((time.monotonic() - started) * 1000)
            audit["response"] = {
                "ok": False,
                "status": None,
                "duration_ms": duration_ms,
                "body_excerpt": "",
                "error": str(exc),
            }
            return {
                "ok": False,
                "status": None,
                "data": None,
                "body_text": "",
                "error": str(exc),
                "audit": audit,
                "dry_run": False,
            }

    def check_reachability(self, devices_path: str = "/onos/v1/devices") -> Dict[str, Any]:
        return self.request("GET", devices_path)

    def get_devices(self, devices_path: str = "/onos/v1/devices") -> Dict[str, Any]:
        result = self.request("GET", devices_path)
        devices = []
        if result["ok"] and isinstance(result.get("data"), dict):
            devices = result["data"].get("devices", [])
        result["devices"] = devices if isinstance(devices, list) else []
        return result

    def get_available_device(self, devices_path: str = "/onos/v1/devices") -> Dict[str, Any]:
        result = self.get_devices(devices_path=devices_path)
        if not result["ok"]:
            return result

        available_devices = [
            device
            for device in result.get("devices", [])
            if isinstance(device, dict) and device.get("available") is True
        ]
        if not available_devices:
            result["ok"] = False
            result["error"] = "No available ONOS devices were reported."
            return result

        result["available_device"] = available_devices[0]
        return result

    def get_device_ports(self, device_id: str) -> Dict[str, Any]:
        result = self.request("GET", f"/onos/v1/devices/{device_id}/ports")
        ports = []
        if result["ok"] and isinstance(result.get("data"), dict):
            ports = result["data"].get("ports", [])
        result["ports"] = ports if isinstance(ports, list) else []
        return result

    def resolve_port_numbers(
        self, device_id: str, upf_port_name: str, edn_port_name: str
    ) -> Dict[str, Any]:
        result = self.get_device_ports(device_id)
        if not result["ok"]:
            return result

        resolved: Dict[str, Optional[str]] = {"upf_port": None, "edn_port": None}
        for port in result.get("ports", []):
            if not isinstance(port, dict):
                continue
            annotations = port.get("annotations") or {}
            port_name = (
                annotations.get("portName")
                or annotations.get("name")
                or annotations.get("ifName")
                or ""
            )
            if port_name == upf_port_name:
                resolved["upf_port"] = str(port.get("port"))
            if port_name == edn_port_name:
                resolved["edn_port"] = str(port.get("port"))

        result.update(resolved)
        if not result.get("upf_port") or not result.get("edn_port"):
            result["ok"] = False
            result["error"] = (
                f"Could not resolve ONOS port numbers for {upf_port_name} and {edn_port_name}."
            )
        return result

    def get_flows(self, device_id: str) -> Dict[str, Any]:
        result = self.request("GET", f"/onos/v1/flows/{device_id}")
        flows = []
        if result["ok"] and isinstance(result.get("data"), dict):
            flows = result["data"].get("flows", [])
        result["flows"] = flows if isinstance(flows, list) else []
        return result

    @staticmethod
    def build_udp_queue_flow(
        device_id: str,
        upf_port: str,
        edn_port: str,
        udp_port: int,
        queue_id: int,
        priority: int,
    ) -> Dict[str, Any]:
        return {
            "priority": priority,
            "timeout": 0,
            "isPermanent": True,
            "deviceId": device_id,
            "treatment": {
                "instructions": [
                    {"type": "SET_QUEUE", "queueId": queue_id},
                    {"type": "OUTPUT", "port": str(edn_port)},
                ]
            },
            "selector": {
                "criteria": [
                    {"type": "IN_PORT", "port": str(upf_port)},
                    {"type": "ETH_TYPE", "ethType": "0x0800"},
                    {"type": "IP_PROTO", "protocol": 17},
                    {"type": "UDP_DST", "udpPort": int(udp_port)},
                ]
            },
        }

    @staticmethod
    def build_reverse_flow(device_id: str, edn_port: str, upf_port: str, priority: int) -> Dict[str, Any]:
        return {
            "priority": priority,
            "timeout": 0,
            "isPermanent": True,
            "deviceId": device_id,
            "treatment": {"instructions": [{"type": "OUTPUT", "port": str(upf_port)}]},
            "selector": {
                "criteria": [
                    {"type": "IN_PORT", "port": str(edn_port)},
                    {"type": "ETH_TYPE", "ethType": "0x0800"},
                ]
            },
        }

    @staticmethod
    def build_forward_flow(device_id: str, upf_port: str, edn_port: str, priority: int) -> Dict[str, Any]:
        return {
            "priority": priority,
            "timeout": 0,
            "isPermanent": True,
            "deviceId": device_id,
            "treatment": {"instructions": [{"type": "OUTPUT", "port": str(edn_port)}]},
            "selector": {
                "criteria": [
                    {"type": "IN_PORT", "port": str(upf_port)},
                    {"type": "ETH_TYPE", "ethType": "0x0800"},
                ]
            },
        }

    @staticmethod
    def build_arp_flow(device_id: str, in_port: str, out_port: str, priority: int) -> Dict[str, Any]:
        return {
            "priority": priority,
            "timeout": 0,
            "isPermanent": True,
            "deviceId": device_id,
            "treatment": {"instructions": [{"type": "OUTPUT", "port": str(out_port)}]},
            "selector": {
                "criteria": [
                    {"type": "IN_PORT", "port": str(in_port)},
                    {"type": "ETH_TYPE", "ethType": "0x0806"},
                ]
            },
        }

    def _flow_matches_spec(self, flow: Dict[str, Any], spec: Dict[str, Any]) -> bool:
        selector = flow.get("selector", {}).get("criteria", [])
        treatment = flow.get("treatment", {}).get("instructions", [])
        if not isinstance(selector, list) or not isinstance(treatment, list):
            return False

        def selector_value(selector_type: str, key: str) -> Optional[str]:
            for item in selector:
                if isinstance(item, dict) and item.get("type") == selector_type:
                    if key in item:
                        return str(item.get(key))
            return None

        def instruction_value(instruction_type: str, key: str) -> Optional[str]:
            for item in treatment:
                if isinstance(item, dict) and item.get("type") == instruction_type:
                    if key in item:
                        return str(item.get(key))
            return None

        spec_selector = spec.get("selector", {}).get("criteria", [])
        spec_treatment = spec.get("treatment", {}).get("instructions", [])
        for item in spec_selector:
            if not isinstance(item, dict):
                continue
            selector_type = item.get("type")
            if selector_type == "IN_PORT" and selector_value("IN_PORT", "port") != str(item.get("port")):
                return False
            if selector_type == "ETH_TYPE" and selector_value("ETH_TYPE", "ethType") != str(item.get("ethType")):
                return False
            if selector_type == "IP_PROTO" and selector_value("IP_PROTO", "protocol") != str(item.get("protocol")):
                return False
            if selector_type == "UDP_DST" and selector_value("UDP_DST", "udpPort") != str(item.get("udpPort")):
                return False
        for item in spec_treatment:
            if not isinstance(item, dict):
                continue
            instruction_type = item.get("type")
            if instruction_type == "SET_QUEUE" and instruction_value("SET_QUEUE", "queueId") != str(item.get("queueId")):
                return False
            if instruction_type == "OUTPUT" and instruction_value("OUTPUT", "port") != str(item.get("port")):
                return False
        if str(flow.get("priority")) != str(spec.get("priority")):
            return False
        return True

    def post_flow(self, device_id: str, flow_payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", f"/onos/v1/flows/{device_id}", payload=flow_payload)

    def ensure_slice_queue_flows(
        self,
        devices_path: str,
        upf_port_name: str,
        edn_port_name: str,
        flow_rules: List[Dict[str, Any]],
        base_forward_flow_priority: int,
        reverse_flow_priority: int,
        arp_flow_priority: int,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        audits: List[Dict[str, Any]] = []
        installed: List[Dict[str, Any]] = []
        already_present: List[Dict[str, Any]] = []
        errors: List[str] = []

        if self.dry_run:
            planned_flows = [
                {
                    "udp_port": int(rule["udp_port"]),
                    "queue_id": int(rule["queue_id"]),
                    "priority": int(rule["priority"]),
                    "upf_port_name": upf_port_name,
                    "edn_port_name": edn_port_name,
                }
                for rule in flow_rules
            ]
            return {
                "ok": True,
                "error": "",
                "device_id": "dry-run-device",
                "upf_port": upf_port_name,
                "edn_port": edn_port_name,
                "audits": audits,
                "installed_flows": planned_flows,
                "existing_flows": [],
                "dry_run": True,
                "planned_only": True,
            }

        device_result = self.get_available_device(devices_path=devices_path)
        audits.append(device_result.get("audit", {}))
        if not device_result["ok"]:
            return {
                "ok": False,
                "error": device_result.get("error", "Failed to discover ONOS device."),
                "audits": audits,
                "installed_flows": installed,
                "existing_flows": already_present,
                "dry_run": self.dry_run,
            }

        device = device_result.get("available_device") or {}
        device_id = str(device.get("id", ""))
        ports_result = self.resolve_port_numbers(device_id, upf_port_name, edn_port_name)
        audits.append(ports_result.get("audit", {}))
        if not ports_result["ok"]:
            return {
                "ok": False,
                "error": ports_result.get("error", "Failed to resolve ONOS ports."),
                "audits": audits,
                "installed_flows": installed,
                "existing_flows": already_present,
                "dry_run": self.dry_run,
            }

        upf_port = str(ports_result["upf_port"])
        edn_port = str(ports_result["edn_port"])
        desired_flows = [
            self.build_udp_queue_flow(
                device_id=device_id,
                upf_port=upf_port,
                edn_port=edn_port,
                udp_port=int(rule["udp_port"]),
                queue_id=int(rule["queue_id"]),
                priority=int(rule["priority"]),
            )
            for rule in flow_rules
        ]
        desired_flows.append(
            self.build_forward_flow(
                device_id=device_id,
                upf_port=upf_port,
                edn_port=edn_port,
                priority=base_forward_flow_priority,
            )
        )
        desired_flows.append(
            self.build_reverse_flow(device_id=device_id, edn_port=edn_port, upf_port=upf_port, priority=reverse_flow_priority)
        )
        desired_flows.append(
            self.build_arp_flow(device_id=device_id, in_port=upf_port, out_port=edn_port, priority=arp_flow_priority)
        )
        desired_flows.append(
            self.build_arp_flow(device_id=device_id, in_port=edn_port, out_port=upf_port, priority=arp_flow_priority)
        )

        flows_result = self.get_flows(device_id)
        audits.append(flows_result.get("audit", {}))
        existing_flows = flows_result.get("flows", []) if flows_result["ok"] else []
        if not flows_result["ok"] and not self.dry_run:
            return {
                "ok": False,
                "error": flows_result.get("error", "Failed to query existing ONOS flows."),
                "audits": audits,
                "installed_flows": installed,
                "existing_flows": already_present,
                "dry_run": self.dry_run,
            }

        for flow_payload in desired_flows:
            if not force_refresh and any(
                self._flow_matches_spec(flow, flow_payload)
                for flow in existing_flows
                if isinstance(flow, dict)
            ):
                already_present.append(flow_payload)
                continue
            post_result = self.post_flow(device_id, flow_payload)
            audits.append(post_result.get("audit", {}))
            if post_result["ok"]:
                installed.append(flow_payload)
                continue
            errors.append(
                post_result.get("error")
                or post_result.get("body_text")
                or "Unknown ONOS flow-install error."
            )

        return {
            "ok": not errors,
            "error": "; ".join(errors),
            "device_id": device_id,
            "upf_port": upf_port,
            "edn_port": edn_port,
            "audits": audits,
            "installed_flows": installed,
            "existing_flows": already_present,
            "dry_run": self.dry_run,
        }
