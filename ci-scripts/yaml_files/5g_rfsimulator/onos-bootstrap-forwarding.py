#!/usr/bin/env python3
import base64
import json
import os
import sys
import urllib.error
import urllib.request


def env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


ONOS_BASE_URL = env("ONOS_BASE_URL", f"http://{env('ONOS_HOST', '192.168.71.160')}:{env('ONOS_REST_PORT', '8181')}")
ONOS_AUTH = env("ONOS_AUTH", "onos:rocks")
OVS_HOST = env("OVS_HOST", env("OVS_CONTAINER_IP", ""))
UPF_PORT_NAME = env("V_UPF_HOST", env("OVS_UPF_PORT_NAME", "v-upf-host"))
EDN_PORT_NAME = env("V_EDN_HOST", env("OVS_EDN_PORT_NAME", "v-edn-host"))
BASE_FORWARD_PRIORITY = int(env("N6_BASE_FORWARD_PRIORITY", "5000"))
ARP_FORWARD_PRIORITY = int(env("N6_ARP_FORWARD_PRIORITY", "45000"))
REVERSE_FORWARD_PRIORITY = int(env("N6_REVERSE_FORWARD_PRIORITY", str(BASE_FORWARD_PRIORITY)))

USERNAME, PASSWORD = (ONOS_AUTH.split(":", 1) + [""])[:2]


def log(message: str) -> None:
    print(f"[onos-bootstrap] {message}")


def normalize_eth_type(value) -> str:
    text = str(value).strip().lower()
    if not text:
        return text
    try:
        return hex(int(text, 16))
    except ValueError:
        return text


def normalize_item(item):
    if not isinstance(item, dict):
        return ()

    normalized = []
    for key, value in item.items():
        if key == "ethType":
            value = normalize_eth_type(value)
        normalized.append((str(key), str(value)))
    return tuple(sorted(normalized))


def request_json(method: str, path: str, payload=None):
    url = f"{ONOS_BASE_URL.rstrip('/')}{path}"
    headers = {"Accept": "application/json"}
    if USERNAME:
      token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
      headers["Authorization"] = f"Basic {token}"
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, headers=headers, data=data, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read()
        status = response.status
    if not body:
        return status, {}
    return status, json.loads(body.decode("utf-8"))


def resolve_device():
    _, devices_payload = request_json("GET", "/onos/v1/devices")
    devices = devices_payload.get("devices", [])
    available = [device for device in devices if device.get("available") is True]
    if OVS_HOST:
        for device in available:
            annotations = device.get("annotations") or {}
            if annotations.get("managementAddress") == OVS_HOST:
                return device
    return available[0] if available else None


def resolve_ports(device_id: str):
    _, ports_payload = request_json("GET", f"/onos/v1/devices/{device_id}/ports")
    ports = ports_payload.get("ports", [])

    def find_port(target_name: str):
        for port in ports:
            annotations = port.get("annotations") or {}
            for candidate in (
                annotations.get("portName"),
                annotations.get("name"),
                annotations.get("ifName"),
            ):
                if candidate == target_name:
                    return str(port.get("port"))
        return ""

    return find_port(UPF_PORT_NAME), find_port(EDN_PORT_NAME)


def build_forward_flow(device_id: str, in_port: str, out_port: str, priority: int, eth_type: str):
    return {
        "priority": priority,
        "timeout": 0,
        "isPermanent": True,
        "deviceId": device_id,
        "treatment": {"instructions": [{"type": "OUTPUT", "port": str(out_port)}]},
        "selector": {
            "criteria": [
                {"type": "IN_PORT", "port": str(in_port)},
                {"type": "ETH_TYPE", "ethType": eth_type},
            ]
        },
    }


def flow_matches(flow: dict, desired: dict) -> bool:
    if str(flow.get("priority")) != str(desired.get("priority")):
        return False

    flow_selector = flow.get("selector", {}).get("criteria", [])
    desired_selector = desired.get("selector", {}).get("criteria", [])
    flow_treatment = flow.get("treatment", {}).get("instructions", [])
    desired_treatment = desired.get("treatment", {}).get("instructions", [])

    def normalize(items):
        normalized = []
        for item in items:
            normalized_item = normalize_item(item)
            if normalized_item:
                normalized.append(normalized_item)
        return sorted(normalized)

    return normalize(flow_selector) == normalize(desired_selector) and normalize(flow_treatment) == normalize(desired_treatment)


def flow_is_bootstrap_candidate(flow: dict) -> bool:
    selector = flow.get("selector", {}).get("criteria", [])
    treatment = flow.get("treatment", {}).get("instructions", [])
    eth_type = ""
    in_port = ""
    output_port = ""
    set_queue_present = False

    for item in selector:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "ETH_TYPE":
            eth_type = normalize_eth_type(item.get("ethType", ""))
        if item.get("type") == "IN_PORT":
            in_port = str(item.get("port", ""))

    for item in treatment:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "SET_QUEUE":
            set_queue_present = True
        if item.get("type") == "OUTPUT":
            output_port = str(item.get("port", ""))

    priority = int(flow.get("priority", 0))
    return (
        flow.get("appId") == "org.onosproject.rest"
        and not set_queue_present
        and bool(in_port)
        and bool(output_port)
        and (
            (priority == BASE_FORWARD_PRIORITY and eth_type == "0x800")
            or (priority == ARP_FORWARD_PRIORITY and eth_type == "0x806")
        )
    )


def ensure_flow(device_id: str, desired_flow: dict, existing_flows: list[dict]) -> bool:
    if any(flow_matches(flow, desired_flow) for flow in existing_flows if isinstance(flow, dict)):
        return False
    request_json("POST", f"/onos/v1/flows/{device_id}", payload=desired_flow)
    return True


def delete_flow(device_id: str, flow_id: str) -> None:
    request_json("DELETE", f"/onos/v1/flows/{device_id}/{flow_id}")


def main() -> int:
    try:
        device = resolve_device()
        if not device:
            log("WARN: no available ONOS device yet; skipping bootstrap flow install for this iteration.")
            return 0

        device_id = str(device.get("id", ""))
        upf_port, edn_port = resolve_ports(device_id)
        if not upf_port or not edn_port:
            log(
                f"WARN: could not resolve ONOS ports for {UPF_PORT_NAME} and {EDN_PORT_NAME}; "
                "skipping bootstrap flow install for this iteration."
            )
            return 0

        _, flows_payload = request_json("GET", f"/onos/v1/flows/{device_id}")
        existing_flows = flows_payload.get("flows", [])
        desired_flows = [
            build_forward_flow(device_id, upf_port, edn_port, BASE_FORWARD_PRIORITY, "0x0800"),
            build_forward_flow(device_id, edn_port, upf_port, REVERSE_FORWARD_PRIORITY, "0x0800"),
            build_forward_flow(device_id, upf_port, edn_port, ARP_FORWARD_PRIORITY, "0x0806"),
            build_forward_flow(device_id, edn_port, upf_port, ARP_FORWARD_PRIORITY, "0x0806"),
        ]

        deleted = 0
        for flow in existing_flows:
            if not isinstance(flow, dict) or not flow_is_bootstrap_candidate(flow):
                continue
            if any(flow_matches(flow, desired) for desired in desired_flows):
                continue
            flow_id = str(flow.get("id", ""))
            if not flow_id:
                continue
            delete_flow(device_id, flow_id)
            deleted += 1

        if deleted:
            _, flows_payload = request_json("GET", f"/onos/v1/flows/{device_id}")
            existing_flows = flows_payload.get("flows", [])

        installed = 0
        for desired in desired_flows:
            installed += 1 if ensure_flow(device_id, desired, existing_flows) else 0

        if installed or deleted:
            log(
                f"Ensured {installed} ONOS-managed bootstrap N6 flow(s) on {device_id} "
                f"and removed {deleted} stale flow(s) "
                f"(upf_port={upf_port}, edn_port={edn_port})."
            )
        else:
            log(f"Bootstrap ONOS flows already present on {device_id}.")
        return 0
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log(f"ERROR: ONOS REST request failed with HTTP {exc.code}: {body}")
        return 1
    except urllib.error.URLError as exc:
        log(f"ERROR: could not reach ONOS REST at {ONOS_BASE_URL}: {exc}")
        return 1
    except Exception as exc:  # pragma: no cover - defensive logging for bootstrap path
        log(f"ERROR: unexpected bootstrap flow failure: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
