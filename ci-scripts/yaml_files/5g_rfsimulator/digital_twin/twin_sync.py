#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from .twin_state import NetworkTwinState, QueueState, ServiceState
    from .twin_store import append_state
except ImportError:
    from twin_state import NetworkTwinState, QueueState, ServiceState
    from twin_store import append_state


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
DEFAULT_SERVICE_NAMES = {
    "embb": "enhanced_mobile_broadband",
    "urllc": "ultra_reliable_low_latency",
    "mmtc": "massive_machine_type",
}
SERVICE_TO_SLICE = {
    "high_throughput_data": "embb",
    "real_time_control": "urllc",
    "sensor_telemetry": "mmtc",
}
SLICE_TO_SERVICE = {value: key for key, value in SERVICE_TO_SLICE.items()}
EXPECTED_QUEUE_MAPPING = {
    "high_throughput_data": {"protocol": "UDP", "port": 5201, "queue_id": "1"},
    "real_time_control": {"protocol": "UDP", "port": 5202, "queue_id": "2"},
    "sensor_telemetry": {"protocol": "UDP", "port": 5203, "queue_id": "3"},
}


def log(message: str) -> None:
    print(f"[digital-twin] {message}", flush=True)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_config(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    if not path.exists():
        return config
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            config[key.strip()] = _coerce_scalar(value.strip())
    return config


def _coerce_scalar(value: str) -> Any:
    if value in {"", "null", "None"}:
        return None
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value.strip("'\"")


def resolve_config_path(value: Optional[str]) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return DEFAULT_CONFIG_PATH.resolve()


def resolve_data_path(value: Any, config_path: Path) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


def newest_telemetry_file(telemetry_dir: Path, telemetry_glob: str) -> Optional[Path]:
    patterns = [telemetry_glob]
    if telemetry_glob == "telemetry_*.jsonl":
        patterns.append("closed_loop_telemetry_*.jsonl")
    files = []
    for pattern in patterns:
        files.extend(path for path in telemetry_dir.glob(pattern) if path.is_file())
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def load_latest_jsonl_record(path: Path) -> Optional[Dict[str, Any]]:
    latest: Optional[Dict[str, Any]] = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                log(f"skipping malformed telemetry line in {path}: {exc}")
                continue
            if isinstance(payload, dict) and payload.get("event_type") != "collector_stop":
                latest = payload
    return latest


def build_network_twin_state(telemetry: Dict[str, Any]) -> NetworkTwinState:
    timestamp = str(telemetry.get("timestamp") or utc_timestamp())
    slice_metrics = telemetry.get("slice_metrics")
    service_metrics = telemetry.get("service_metrics")

    if isinstance(slice_metrics, dict) and slice_metrics:
        services = [
            _service_from_slice_metric(slice_name, metrics, timestamp)
            for slice_name, metrics in sorted(slice_metrics.items())
            if isinstance(metrics, dict)
        ]
        queues = [
            _queue_from_slice_metric(slice_name, metrics, timestamp)
            for slice_name, metrics in sorted(slice_metrics.items())
            if isinstance(metrics, dict)
        ]
    elif isinstance(service_metrics, dict) and service_metrics:
        services = [
            _service_from_legacy_metric(service_name, metrics, timestamp)
            for service_name, metrics in sorted(service_metrics.items())
            if isinstance(metrics, dict)
        ]
        queues = _queues_from_legacy_telemetry(telemetry, service_metrics, timestamp)
    else:
        services, queues = _states_from_flat_metrics(telemetry, timestamp)

    return NetworkTwinState(
        services=services,
        queues=queues,
        ovs_status=_ovs_status(telemetry),
        onos_status=_onos_status(telemetry),
        last_updated=timestamp,
        queue_rules=_queue_rules(telemetry),
        queue_rules_status=_queue_rules_status(telemetry),
        queue_counters=_queue_counters(queues),
        service_metrics=_service_metrics(services),
        intended_policy_state=_intended_policy_state(),
        policy_verification_state=_policy_verification_state(telemetry),
    )


def _service_from_slice_metric(
    slice_name: str, metrics: Dict[str, Any], timestamp: str
) -> ServiceState:
    service_name = str(metrics.get("display_name") or DEFAULT_SERVICE_NAMES.get(slice_name, slice_name))
    return ServiceState(
        service_name=service_name,
        slice_name=slice_name,
        latency_avg_ms=_number(metrics.get("latency_avg_ms")),
        latency_max_ms=_number(metrics.get("latency_max_ms")),
        jitter_ms=_number(metrics.get("jitter_ms")),
        packet_loss_percent=_number(_first_present(metrics, "loss_percent", "packet_loss_percent")),
        throughput_bps=_number(metrics.get("throughput_bps")),
        sla_violation_risk=_sla_violation_risk(metrics),
        timestamp=timestamp,
    )


def _queue_from_slice_metric(slice_name: str, metrics: Dict[str, Any], timestamp: str) -> QueueState:
    queue_id = metrics.get("queue_id")
    return QueueState(
        queue_id=None if queue_id is None else str(queue_id),
        slice_name=slice_name,
        packets_total=_number(_first_present(metrics, "queue_packets_total", "ovs_queue_packets_total")),
        bytes_total=_number(_first_present(metrics, "queue_bytes_total", "ovs_queue_bytes_total")),
        packet_rate_pps=_number(
            _first_present(metrics, "queue_packets_per_second", "ovs_flow_packet_rate_pps", "flow_packet_rate_pps")
        ),
        throughput_bps=_number(
            _first_present(metrics, "queue_throughput_bps", "flow_throughput_bps", "ovs_flow_throughput_bps")
        ),
        timestamp=timestamp,
    )


def _service_from_legacy_metric(
    service_name: str, metrics: Dict[str, Any], timestamp: str
) -> ServiceState:
    slice_name = _legacy_slice_name(service_name)
    return ServiceState(
        service_name=service_name,
        slice_name=slice_name,
        latency_avg_ms=_number(_first_present(metrics, "latency_avg_ms", "urllc_latency_avg_ms")),
        latency_max_ms=_number(_first_present(metrics, "latency_max_ms", "urllc_latency_max_ms")),
        jitter_ms=_number(metrics.get("jitter_ms")),
        packet_loss_percent=_number(_first_present(metrics, "loss_percent", "packet_loss_percent")),
        throughput_bps=_number(_first_present(metrics, "throughput_bps", "embb_throughput_bps")),
        sla_violation_risk=_sla_violation_risk(metrics),
        timestamp=timestamp,
    )


def _queues_from_legacy_telemetry(
    telemetry: Dict[str, Any], service_metrics: Dict[str, Any], timestamp: str
) -> List[QueueState]:
    queue_lookup = _legacy_queue_lookup(telemetry)
    queues: List[QueueState] = []
    for service_name, metrics in sorted(service_metrics.items()):
        if not isinstance(metrics, dict):
            continue
        queue_ids = metrics.get("queue_ids")
        queue_id = None
        if isinstance(queue_ids, list) and queue_ids:
            queue_id = str(queue_ids[0])
        elif metrics.get("queue_id") is not None:
            queue_id = str(metrics.get("queue_id"))
        queue_metrics = queue_lookup.get(queue_id or "", {})
        queues.append(
            QueueState(
                queue_id=queue_id,
                slice_name=_legacy_slice_name(service_name),
                packets_total=_number(
                    _first_present(queue_metrics, "packets_total", "pkts", "ovs_queue_packets_total")
                ),
                bytes_total=_number(
                    _first_present(queue_metrics, "bytes_total", "bytes", "ovs_queue_bytes_total")
                ),
                packet_rate_pps=_number(
                    _first_present(queue_metrics, "packet_rate_pps", "packets_per_second")
                ),
                throughput_bps=_number(
                    _first_present(queue_metrics, "throughput_bps", "bytes_per_second")
                ),
                timestamp=timestamp,
            )
        )
    return queues


def _legacy_queue_lookup(telemetry: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    queue_stats = _nested_dict(telemetry, "ovs_queue_statistics")
    lookup: Dict[str, Dict[str, Any]] = {}
    raw_stats = queue_stats.get("queue_stats")
    if isinstance(raw_stats, list):
        for item in raw_stats:
            if not isinstance(item, dict):
                continue
            queue_id = item.get("queue_id")
            if queue_id is not None:
                lookup[str(queue_id)] = item
    return lookup


def _legacy_slice_name(service_name: str) -> str:
    mapping = {
        "high_throughput_data": "embb",
        "real_time_control": "urllc",
        "sensor_telemetry": "mmtc",
    }
    return mapping.get(service_name, service_name)


def _states_from_flat_metrics(
    telemetry: Dict[str, Any], timestamp: str
) -> Tuple[List[ServiceState], List[QueueState]]:
    service = ServiceState(
        service_name="ultra_reliable_low_latency",
        slice_name="urllc",
        latency_avg_ms=_number(telemetry.get("urllc_latency_avg_ms")),
        latency_max_ms=_number(telemetry.get("urllc_latency_max_ms")),
        jitter_ms=_number(telemetry.get("urllc_jitter_ms")),
        packet_loss_percent=_number(
            _first_present(telemetry, "urllc_packet_loss_percent", "urllc_loss_percent")
        ),
        throughput_bps=_number(_first_present(telemetry, "urllc_throughput_bps", "embb_throughput_bps")),
        sla_violation_risk=_sla_violation_risk(telemetry),
        timestamp=timestamp,
    )
    embb_throughput = _number(telemetry.get("embb_throughput_bps"))
    services = [service]
    if embb_throughput is not None:
        services.append(
            ServiceState(
                service_name="enhanced_mobile_broadband",
                slice_name="embb",
                throughput_bps=embb_throughput,
                sla_violation_risk=None,
                timestamp=timestamp,
            )
        )

    queue = QueueState(
        queue_id=None if telemetry.get("ovs_queue_id") is None else str(telemetry.get("ovs_queue_id")),
        slice_name=str(telemetry.get("slice_name") or "unknown"),
        packets_total=_number(telemetry.get("ovs_queue_packets_total")),
        bytes_total=_number(telemetry.get("ovs_queue_bytes_total")),
        packet_rate_pps=_number(telemetry.get("ovs_flow_packet_rate_pps")),
        throughput_bps=_number(telemetry.get("ovs_flow_throughput_bps")),
        timestamp=timestamp,
    )
    return services, [queue]


def _ovs_status(telemetry: Dict[str, Any]) -> Dict[str, Any]:
    ovs = _nested_dict(telemetry, "telemetry", "ovs")
    if ovs:
        controller = ovs.get("controller") if isinstance(ovs.get("controller"), dict) else {}
        summary = ovs.get("validation_summary") if isinstance(ovs.get("validation_summary"), dict) else {}
        return {
            "ok": ovs.get("ok"),
            "bridge_name": ovs.get("bridge_name") or summary.get("bridge_name"),
            "controller_connected": controller.get("is_connected"),
            "controller_target": controller.get("target"),
            "queue_configured_slices": summary.get("queue_configured_slices"),
            "flow_rule_present_slices": summary.get("flow_rule_present_slices"),
        }
    legacy = _nested_dict(telemetry, "ovs_bridge_status")
    if legacy:
        return {
            "ok": legacy.get("exists"),
            "bridge_name": legacy.get("bridge_name"),
            "controller_connected": legacy.get("controller_connected"),
            "controller_target": _first_from_list(legacy.get("controller_targets")),
            "configured_ports": legacy.get("configured_ports"),
        }
    return {
        "ok": telemetry.get("ovs_ok"),
        "bridge_name": telemetry.get("ovs_bridge_name"),
        "controller_connected": telemetry.get("ovs_controller_connected"),
        "controller_target": telemetry.get("ovs_controller_target"),
    }


def _onos_status(telemetry: Dict[str, Any]) -> Dict[str, Any]:
    onos = _nested_dict(telemetry, "telemetry", "onos")
    if onos:
        return {
            "ok": onos.get("ok"),
            "device_count": onos.get("device_count"),
            "available_device_count": onos.get("available_device_count"),
        }
    legacy = _nested_dict(telemetry, "onos_reachability")
    if legacy:
        return {
            "ok": legacy.get("reachable"),
            "device_count": None,
            "available_device_count": None,
            "url": legacy.get("url"),
            "error": legacy.get("error"),
        }
    return {
        "ok": telemetry.get("onos_ok"),
        "device_count": telemetry.get("onos_device_count"),
        "available_device_count": telemetry.get("onos_available_device_count"),
    }


def _service_metrics(services: List[ServiceState]) -> Dict[str, Dict[str, Any]]:
    metrics: Dict[str, Dict[str, Any]] = {}
    for service in services:
        service_name = SLICE_TO_SERVICE.get(service.slice_name, service.service_name)
        metrics[service_name] = {
            "latency_avg_ms": service.latency_avg_ms,
            "latency_max_ms": service.latency_max_ms,
            "jitter_ms": service.jitter_ms,
            "packet_loss_percent": service.packet_loss_percent,
            "throughput_bps": service.throughput_bps,
            "sla_violation_risk": service.sla_violation_risk,
            "timestamp": service.timestamp,
        }
    return metrics


def _queue_counters(queues: List[QueueState]) -> Dict[str, Dict[str, Any]]:
    counters: Dict[str, Dict[str, Any]] = {}
    for queue in queues:
        if queue.queue_id is None:
            continue
        counters[str(queue.queue_id)] = {
            "queue_id": str(queue.queue_id),
            "slice_name": queue.slice_name,
            "service_name": SLICE_TO_SERVICE.get(queue.slice_name, queue.slice_name),
            "packets_total": queue.packets_total,
            "bytes_total": queue.bytes_total,
            "packet_rate_pps": queue.packet_rate_pps,
            "throughput_bps": queue.throughput_bps,
            "timestamp": queue.timestamp,
        }
    return counters


def _intended_policy_state() -> Dict[str, Any]:
    return {
        "enforcement_path": "ONOS_QUEUE_APP",
        "expected_queue_mapping": EXPECTED_QUEUE_MAPPING,
    }


def _policy_verification_state(telemetry: Dict[str, Any]) -> Dict[str, Any]:
    presence = _queue_rule_presence(telemetry)
    return {
        "queue_rule_presence": presence,
        "policy_drift_detected": None if presence == "unknown" else presence != "all_present",
        "enforcement_path": _enforcement_path(telemetry),
    }


def _queue_rules_status(telemetry: Dict[str, Any]) -> str:
    return _queue_rule_presence(telemetry)


def _queue_rules(telemetry: Dict[str, Any]) -> Dict[str, Any]:
    presence = _queue_rule_presence(telemetry)
    if presence == "unknown":
        return {}
    summary = _nested_dict(telemetry, "telemetry", "ovs", "validation_summary")
    configured = summary.get("queue_configured_slices")
    flow_present = summary.get("flow_rule_present_slices")
    rules: Dict[str, Any] = {}
    for service, expected in EXPECTED_QUEUE_MAPPING.items():
        slice_name = SERVICE_TO_SLICE[service]
        present = _slice_present(configured, slice_name) and _slice_present(flow_present, slice_name)
        rules[service] = {
            "protocol": expected["protocol"],
            "port": expected["port"],
            "queue_id": expected["queue_id"],
            "present": present,
        }
    return rules


def _queue_rule_presence(telemetry: Dict[str, Any]) -> str:
    summary = _nested_dict(telemetry, "telemetry", "ovs", "validation_summary")
    configured = summary.get("queue_configured_slices")
    flow_present = summary.get("flow_rule_present_slices")
    if configured is None or flow_present is None:
        return "unknown"
    statuses = []
    for slice_name in SERVICE_TO_SLICE.values():
        statuses.append(_slice_present(configured, slice_name) and _slice_present(flow_present, slice_name))
    if all(statuses):
        return "all_present"
    if any(statuses):
        return "partial"
    return "missing"


def _slice_present(value: Any, slice_name: str) -> bool:
    if isinstance(value, dict):
        return bool(value.get(slice_name))
    if isinstance(value, list):
        return slice_name in {str(item) for item in value}
    return False


def _enforcement_path(telemetry: Dict[str, Any]) -> str:
    explicit = telemetry.get("enforcement_path")
    if explicit:
        return str(explicit)
    text = json.dumps(telemetry).upper()
    if "ONOS_QUEUE_APP" in text:
        return "ONOS_QUEUE_APP"
    return "unknown"


def _nested_dict(payload: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _first_present(payload: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _first_from_list(value: Any) -> Any:
    if isinstance(value, list) and value:
        return value[0]
    return None


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sla_violation_risk(metrics: Dict[str, Any]) -> Optional[str]:
    explicit = metrics.get("sla_violation_risk")
    if explicit is not None:
        return str(explicit)
    loss = _number(_first_present(metrics, "loss_percent", "packet_loss_percent"))
    latency = _number(_first_present(metrics, "latency_avg_ms", "urllc_latency_avg_ms"))
    if loss is not None and loss > 1.0:
        return "high"
    if latency is not None and latency > 10.0:
        return "medium"
    if loss is not None or latency is not None:
        return "low"
    return None


def sync_once(config: Dict[str, Any], config_path: Path) -> Optional[NetworkTwinState]:
    telemetry_dir = resolve_data_path(config.get("telemetry_dir", "../logs/telemetry"), config_path)
    telemetry_glob = str(config.get("telemetry_glob", "telemetry_*.jsonl"))
    output_path = resolve_data_path(
        config.get("twin_output_path", "../logs/digital_twin/twin_state.jsonl"),
        config_path,
    )

    telemetry_path = newest_telemetry_file(telemetry_dir, telemetry_glob)
    if telemetry_path is None:
        log(f"no telemetry files found yet in {telemetry_dir} matching {telemetry_glob}")
        return None

    record = load_latest_jsonl_record(telemetry_path)
    if record is None:
        log(f"no telemetry records found yet in {telemetry_path}")
        return None

    state = build_network_twin_state(record)
    log(f"loaded telemetry from {telemetry_path}")
    written_path = append_state(state, output_path)
    log(f"wrote twin state to {written_path}")
    return state


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synchronize telemetry into the 6G Digital Twin state log.")
    parser.add_argument("--config", help="Path to digital_twin/config.yaml.")
    parser.add_argument("--once", action="store_true", help="Write one twin state record and exit.")
    parser.add_argument("--interval", type=float, help="Continuously update every N seconds.")
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    interval = args.interval
    if interval is None:
        interval = float(config.get("sync_interval_seconds", 2))

    if args.once:
        sync_once(config, config_path)
        return 0

    log(f"starting sync loop interval_seconds={interval}")
    try:
        while True:
            sync_once(config, config_path)
            time.sleep(interval)
    except KeyboardInterrupt:
        log("stopped")
        return 0


if __name__ == "__main__":
    sys.exit(main())
