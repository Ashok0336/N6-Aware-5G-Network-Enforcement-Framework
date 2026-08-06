#!/usr/bin/env python3
"""Feature extraction for deterministic Shadow N6 SLA risk scoring."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
SERVICES = ["real_time_control", "high_throughput_data", "sensor_telemetry"]
DEFAULT_EXPECTED_QUEUE_MAPPING = {
    "high_throughput_data": {"protocol": "UDP", "port": 5201, "queue_id": "1"},
    "real_time_control": {"protocol": "UDP", "port": 5202, "queue_id": "2"},
    "sensor_telemetry": {"protocol": "UDP", "port": 5203, "queue_id": "3"},
}
SLICE_TO_SERVICE = {
    "urllc": "real_time_control",
    "embb": "high_throughput_data",
    "mmtc": "sensor_telemetry",
    "ultra_reliable_low_latency": "real_time_control",
    "enhanced_mobile_broadband": "high_throughput_data",
    "massive_machine_type": "sensor_telemetry",
}


@dataclass
class TwinFeatures:
    timestamp: str
    source_path: str
    onos: Dict[str, Any]
    ovs: Dict[str, Any]
    queues: Dict[str, Dict[str, float]]
    services: Dict[str, Dict[str, Optional[float]]]
    queue_rule_presence: Dict[str, str]
    queue_rule_presence_overall: str
    expected_queue_mapping: Dict[str, Dict[str, Any]]
    actual_queue_mapping: Dict[str, Optional[str]]
    policy_drift: Optional[bool]
    missing_fields: List[str] = field(default_factory=list)
    previous: Optional["TwinFeatures"] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "source_path": self.source_path,
            "onos": self.onos,
            "ovs": self.ovs,
            "queues": self.queues,
            "services": self.services,
            "queue_rule_presence": self.queue_rule_presence,
            "queue_rule_presence_overall": self.queue_rule_presence_overall,
            "expected_queue_mapping": self.expected_queue_mapping,
            "actual_queue_mapping": self.actual_queue_mapping,
            "policy_drift": self.policy_drift,
            "missing_fields": self.missing_fields,
        }


def load_twin_features(
    twin_state_path: Optional[Path] = None,
    expected_queue_mapping: Optional[Dict[str, Dict[str, Any]]] = None,
) -> TwinFeatures:
    mapping = expected_queue_mapping or DEFAULT_EXPECTED_QUEUE_MAPPING
    payload, source_path, previous_payload = load_latest_twin_state(twin_state_path)
    return extract_twin_features(payload, source_path, mapping, previous_payload=previous_payload)


def load_latest_twin_state(twin_state_path: Optional[Path] = None) -> Tuple[Dict[str, Any], str, Optional[Dict[str, Any]]]:
    candidates: List[Path] = []
    if twin_state_path:
        candidates.append(_resolve_path(twin_state_path))
    candidates.extend(
        [
            ROOT / "logs/digital_twin/latest_twin_state.json",
            ROOT / "logs/digital_twin/twin_state.jsonl",
        ]
    )
    for path in candidates:
        payload, previous = _read_state_file(path)
        if payload is not None:
            return payload, str(path), previous
    return {}, "", None


def extract_twin_features(
    payload: Dict[str, Any],
    source_path: str,
    expected_queue_mapping: Dict[str, Dict[str, Any]],
    previous_payload: Optional[Dict[str, Any]] = None,
) -> TwinFeatures:
    missing: List[str] = []
    timestamp = str(payload.get("timestamp") or payload.get("last_updated") or "")
    if not timestamp:
        missing.append("timestamp_or_last_updated")
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    onos_status = _dict(payload.get("onos_status"))
    if not onos_status:
        missing.append("onos_status")
    onos = {
        "ok": _truthy_or_none(_first(onos_status, "ok", "healthy", "connected")),
        "device_count": _number(_first(onos_status, "device_count", "devices", "available_devices")),
    }

    ovs_status = _dict(payload.get("ovs_status"))
    if not ovs_status:
        missing.append("ovs_status")
    ovs = {
        "ok": _truthy_or_none(_first(ovs_status, "ok", "healthy")),
        "controller_connected": _truthy_or_none(_first(ovs_status, "controller_connected", "connected")),
        "bridge_name": _first(ovs_status, "bridge_name", "bridge", "br_name"),
    }

    queues = _extract_queue_counters(payload, missing)
    services = _extract_service_metrics(payload, missing)
    queue_rules = _extract_queue_rule_presence(payload, expected_queue_mapping)
    extra_missing = payload.get("_risk_missing_fields")
    if isinstance(extra_missing, list):
        missing.extend(str(item) for item in extra_missing)
    queue_rule_presence_overall = _overall_queue_rule_presence(payload, queue_rules)
    actual_mapping = _extract_actual_queue_mapping(payload, expected_queue_mapping)
    policy_drift = _extract_policy_drift(payload)

    previous = None
    if previous_payload:
        previous = extract_twin_features(previous_payload, source_path, expected_queue_mapping, previous_payload=None)

    return TwinFeatures(
        timestamp=timestamp,
        source_path=source_path,
        onos=onos,
        ovs=ovs,
        queues=queues,
        services=services,
        queue_rule_presence=queue_rules,
        queue_rule_presence_overall=queue_rule_presence_overall,
        expected_queue_mapping=expected_queue_mapping,
        actual_queue_mapping=actual_mapping,
        policy_drift=policy_drift,
        missing_fields=sorted(set(missing)),
        previous=previous,
    )


def twin_state_age_seconds(timestamp: str) -> Optional[float]:
    parsed = _parse_time(timestamp)
    if parsed is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _extract_queue_counters(payload: Dict[str, Any], missing: List[str]) -> Dict[str, Dict[str, float]]:
    queues = {queue_id: {"packets": 0.0, "bytes": 0.0, "packet_rate_pps": 0.0, "throughput_bps": 0.0} for queue_id in ("1", "2", "3")}
    found = False

    for item in _as_list(payload.get("queues")):
        queue_id = str(_first(item, "queue_id", "id", "queue") or "")
        if queue_id in queues:
            _merge_queue_record(queues[queue_id], item)
            found = True

    queue_counters = payload.get("queue_counters")
    if isinstance(queue_counters, dict):
        for queue_id, record in queue_counters.items():
            queue_id = str(queue_id).replace("queue_", "")
            if queue_id in queues and isinstance(record, dict):
                _merge_queue_record(queues[queue_id], record)
                found = True
    elif isinstance(queue_counters, list):
        for record in queue_counters:
            if isinstance(record, dict):
                queue_id = str(_first(record, "queue_id", "id", "queue") or "")
                if queue_id in queues:
                    _merge_queue_record(queues[queue_id], record)
                    found = True

    if not found:
        missing.append("queues_or_queue_counters")
    return queues


def _extract_service_metrics(payload: Dict[str, Any], missing: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    services = {service: _empty_service_metrics() for service in SERVICES}
    found = set()

    for service in _as_list(payload.get("services")):
        name = _service_name(service)
        if name in services:
            services[name].update(_metrics_from_record(service))
            found.add(name)

    service_metrics = payload.get("service_metrics")
    if isinstance(service_metrics, dict):
        for key, record in service_metrics.items():
            name = SLICE_TO_SERVICE.get(str(key), str(key))
            if name in services and isinstance(record, dict):
                services[name].update(_metrics_from_record(record))
                found.add(name)
    elif isinstance(service_metrics, list):
        for record in service_metrics:
            if isinstance(record, dict):
                name = _service_name(record)
                if name in services:
                    services[name].update(_metrics_from_record(record))
                    found.add(name)

    for service in SERVICES:
        if service not in found:
            missing.append(f"service_metrics.{service}")
    return services


def _extract_queue_rule_presence(payload: Dict[str, Any], mapping: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    rules = payload.get("queue_rules")
    presence = {service: "unknown" for service in SERVICES}

    if isinstance(rules, dict):
        for service, expected in mapping.items():
            record = rules.get(service) or rules.get(str(expected.get("port"))) or rules.get(str(expected.get("queue_id")))
            if isinstance(record, dict):
                value = _first(record, "presence", "status", "queue_rule_presence")
                if value is not None:
                    presence[service] = _normalize_presence(value)
                elif any(key in record for key in ("present", "installed", "exists", "ok")):
                    presence[service] = "all_present" if _truthy(_first(record, "present", "installed", "exists", "ok")) else "missing"
            elif record is not None:
                presence[service] = _normalize_presence(record)
    elif isinstance(rules, list):
        for record in rules:
            if not isinstance(record, dict):
                continue
            service = _service_name(record)
            queue_id = str(_first(record, "queue_id", "queue") or "")
            for expected_service, expected in mapping.items():
                if service == expected_service or queue_id == str(expected.get("queue_id")):
                    if any(key in record for key in ("present", "installed", "exists", "ok")):
                        presence[expected_service] = "all_present" if _truthy(_first(record, "present", "installed", "exists", "ok")) else "missing"
                    else:
                        presence[expected_service] = _normalize_presence(_first(record, "presence", "status", "queue_rule_presence", default="unknown"))

    verification = _dict(payload.get("policy_verification_state"))
    verification_presence = _normalize_presence(verification.get("queue_rule_presence")) if verification else "unknown"
    if verification_presence in {"missing", "partial"} and all(value == "unknown" for value in presence.values()):
        return {service: verification_presence for service in SERVICES}
    if verification_presence == "all_present":
        for service, value in list(presence.items()):
            if value == "unknown":
                presence[service] = "all_present"

    if payload.get("queue_rules_status") is not None and all(value == "unknown" for value in presence.values()):
        status = _normalize_presence(payload.get("queue_rules_status"))
        presence = {service: status for service in SERVICES}

    if all(value == "unknown" for value in presence.values()):
        missing = payload.setdefault("_risk_missing_fields", [])
        if isinstance(missing, list):
            missing.append("queue_rules")
    return presence


def _overall_queue_rule_presence(payload: Dict[str, Any], presence: Dict[str, str]) -> str:
    verification = _dict(payload.get("policy_verification_state"))
    if verification.get("queue_rule_presence") is not None:
        return _normalize_presence(verification.get("queue_rule_presence"))
    if payload.get("queue_rules_status") is not None:
        return _normalize_presence(payload.get("queue_rules_status"))
    values = set(presence.values())
    if values == {"all_present"}:
        return "all_present"
    if "missing" in values and len(values) == 1:
        return "missing"
    if "missing" in values or "partial" in values:
        return "partial"
    return "unknown"


def _extract_actual_queue_mapping(payload: Dict[str, Any], mapping: Dict[str, Dict[str, Any]]) -> Dict[str, Optional[str]]:
    actual = {service: None for service in SERVICES}
    candidates = [_dict(payload.get("actual_queue_mapping")), _dict(payload.get("intended_policy_state")), _dict(payload.get("policy_verification_state"))]
    for candidate in candidates:
        for service, expected in mapping.items():
            record = candidate.get(service) or candidate.get(str(expected.get("port")))
            if isinstance(record, dict):
                value = _first(record, "actual_queue_id", "queue_id", "queue", "installed_queue_id")
                if value is not None:
                    actual[service] = str(value)
            elif record is not None:
                actual[service] = str(record)
    return actual


def _extract_policy_drift(payload: Dict[str, Any]) -> Optional[bool]:
    for key in ("policy_verification_state", "intended_policy_state"):
        state = payload.get(key)
        if isinstance(state, dict):
            for field in ("policy_drift_detected", "drift_detected", "policy_drift", "mismatch", "out_of_sync"):
                if field in state:
                    if state.get(field) is None:
                        return None
                    return _truthy(state.get(field))
    return None


def _read_state_file(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not path.exists():
        return None, None
    try:
        if path.suffix == ".jsonl":
            records = [payload for payload in _iter_jsonl(path) if isinstance(payload, dict)]
            if not records:
                return None, None
            return records[-1], records[-2] if len(records) > 1 else None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, None
    return (payload, None) if isinstance(payload, dict) else (None, None)


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    yield payload
    except OSError:
        return


def _resolve_path(path: Path) -> Path:
    expanded = path.expanduser()
    return expanded if expanded.is_absolute() else (ROOT / expanded)


def _merge_queue_record(target: Dict[str, float], record: Dict[str, Any]) -> None:
    target["packets"] = _number(_first(record, "packets", "packets_total", "tx_packets", "packet_count")) or target["packets"]
    target["bytes"] = _number(_first(record, "bytes", "bytes_total", "tx_bytes", "byte_count")) or target["bytes"]
    target["packet_rate_pps"] = _number(_first(record, "packet_rate_pps", "pps")) or target["packet_rate_pps"]
    target["throughput_bps"] = _number(_first(record, "throughput_bps", "bitrate_bps")) or target["throughput_bps"]


def _metrics_from_record(record: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        "latency_avg_ms": _number(_first(record, "latency_avg_ms", "latency_ms", "avg_latency_ms")),
        "latency_max_ms": _number(_first(record, "latency_max_ms", "max_latency_ms")),
        "jitter_ms": _number(_first(record, "jitter_ms", "latency_jitter_ms")),
        "throughput_bps": _number(_first(record, "throughput_bps", "flow_throughput_bps", "sender_average_bitrate_bps")),
        "packet_loss_percent": _number(_first(record, "packet_loss_percent", "loss_percent")),
        "delivery_ratio_percent": _number(_first(record, "delivery_ratio_percent", "reliability_proxy_percent")),
    }


def _empty_service_metrics() -> Dict[str, Optional[float]]:
    return {
        "latency_avg_ms": None,
        "latency_max_ms": None,
        "jitter_ms": None,
        "throughput_bps": None,
        "packet_loss_percent": None,
        "delivery_ratio_percent": None,
    }


def _service_name(record: Dict[str, Any]) -> str:
    raw = str(_first(record, "service_name", "service_class", "service", default="") or "")
    return SLICE_TO_SERVICE.get(raw, raw)


def _normalize_presence(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"all_present", "present", "installed", "true", "ok", "1", "yes"}:
        return "all_present"
    if text in {"partial", "partially_present"}:
        return "partial"
    if text in {"missing", "absent", "false", "0", "no"}:
        return "missing"
    return "unknown"


def _parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _first(record: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(record, dict) and key in record and record[key] not in (None, ""):
            return record[key]
    return default


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    return str(value).strip().lower() in {"true", "1", "yes", "ok", "connected", "present", "installed", "active"}


def _truthy_or_none(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    return _truthy(value)
