#!/usr/bin/env python3
"""Deterministic SLA risk rules for Shadow N6 services."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

try:
    from .risk_features import SERVICES, TwinFeatures
except ImportError:
    from risk_features import SERVICES, TwinFeatures


def component_scores(features: TwinFeatures, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for service in SERVICES:
        explanations: List[str] = []
        scores = {
            "sla_margin_risk": _sla_margin_risk(service, features.services.get(service, {}), config, explanations),
            "trend_risk": _trend_risk(service, features, explanations),
            "queue_pressure_risk": _queue_pressure_risk(service, features, config, explanations),
            "enforcement_mismatch_risk": _enforcement_mismatch_risk(service, features, explanations),
        }
        results[service] = {"components": scores, "explanation": explanations}
    return results


def _sla_margin_risk(service: str, metrics: Dict[str, Optional[float]], config: Dict[str, Any], explanations: List[str]) -> float:
    thresholds = config.get("sla_thresholds", {}).get(service, {})
    candidates: List[float] = []

    if service == "real_time_control":
        candidates.extend(
            [
                _upper_risk(metrics.get("latency_avg_ms"), thresholds.get("latency_avg_ms"), "average latency", explanations),
                _upper_risk(metrics.get("latency_max_ms"), thresholds.get("latency_max_ms"), "maximum latency", explanations),
                _upper_risk(metrics.get("jitter_ms"), thresholds.get("jitter_ms"), "jitter", explanations),
                _upper_risk(metrics.get("packet_loss_percent"), thresholds.get("packet_loss_percent"), "packet loss", explanations),
            ]
        )
    elif service == "high_throughput_data":
        candidates.extend(
            [
                _lower_risk(metrics.get("throughput_bps"), thresholds.get("throughput_bps"), "throughput", explanations),
                _upper_risk(metrics.get("packet_loss_percent"), thresholds.get("packet_loss_percent"), "packet loss", explanations),
            ]
        )
    elif service == "sensor_telemetry":
        candidates.extend(
            [
                _lower_risk(metrics.get("delivery_ratio_percent"), thresholds.get("delivery_ratio_percent"), "delivery ratio", explanations),
                _upper_risk(metrics.get("packet_loss_percent"), thresholds.get("packet_loss_percent"), "packet loss", explanations),
            ]
        )
    return _clamp(max(candidates) if candidates else 0.0)


def _trend_risk(service: str, features: TwinFeatures, explanations: List[str]) -> float:
    if not features.previous:
        return 0.0
    current = features.services.get(service, {})
    previous = features.previous.services.get(service, {})
    candidates = [
        _positive_delta_risk(previous.get("latency_avg_ms"), current.get("latency_avg_ms"), 10.0, "latency trend", explanations),
        _positive_delta_risk(previous.get("jitter_ms"), current.get("jitter_ms"), 5.0, "jitter trend", explanations),
        _positive_delta_risk(previous.get("packet_loss_percent"), current.get("packet_loss_percent"), 1.0, "packet loss trend", explanations),
        _negative_delta_risk(previous.get("throughput_bps"), current.get("throughput_bps"), 5000000.0, "throughput trend", explanations),
        _negative_delta_risk(previous.get("delivery_ratio_percent"), current.get("delivery_ratio_percent"), 2.0, "delivery trend", explanations),
    ]
    return _clamp(max(candidates) if candidates else 0.0)


def _queue_pressure_risk(service: str, features: TwinFeatures, config: Dict[str, Any], explanations: List[str]) -> float:
    queue_id = str(features.expected_queue_mapping.get(service, {}).get("queue_id", ""))
    queue = features.queues.get(queue_id, {})
    pressure = config.get("queue_pressure", {})
    byte_risk = _scale(queue.get("bytes", 0.0), pressure.get("bytes_medium", 1000000), pressure.get("bytes_high", 10000000))
    packet_risk = _scale(queue.get("packets", 0.0), pressure.get("packets_medium", 1000), pressure.get("packets_high", 10000))
    risk = max(byte_risk, packet_risk)
    if risk >= 0.67:
        explanations.append(f"queue {queue_id} pressure is high")
    elif risk >= 0.34:
        explanations.append(f"queue {queue_id} pressure is elevated")
    return _clamp(risk)


def _enforcement_mismatch_risk(service: str, features: TwinFeatures, explanations: List[str]) -> float:
    penalties: List[Tuple[float, str]] = []
    expected_queue = str(features.expected_queue_mapping.get(service, {}).get("queue_id", ""))
    actual_queue = features.actual_queue_mapping.get(service)
    queue = features.queues.get(expected_queue, {})
    traffic_present = _service_has_traffic(features.services.get(service, {}))

    if features.onos.get("ok") is False:
        penalties.append((0.40, "ONOS status is not OK"))
    if features.ovs.get("controller_connected") is False:
        penalties.append((0.40, "OVS controller is disconnected"))
    queue_rule_presence = features.queue_rule_presence.get(service, "unknown")
    if queue_rule_presence == "missing":
        penalties.append((0.45, f"queue rule for {service} is missing"))
    elif queue_rule_presence == "partial":
        penalties.append((0.25, f"queue rule for {service} is partially present"))
    if actual_queue is not None and actual_queue != expected_queue:
        penalties.append((0.55, f"expected queue {expected_queue} but actual queue is {actual_queue}"))
    if features.policy_drift is True:
        penalties.append((0.45, "policy verification reports drift"))
    if traffic_present and (queue.get("packets", 0.0) <= 0 and queue.get("bytes", 0.0) <= 0):
        penalties.append((0.35, f"traffic is present but queue {expected_queue} counter is not increasing"))

    for _, reason in penalties:
        explanations.append(reason)
    return _clamp(sum(score for score, _ in penalties))


def _upper_risk(value: Optional[float], threshold: Any, label: str, explanations: List[str]) -> float:
    threshold_value = _number(threshold)
    if value is None or threshold_value is None or threshold_value <= 0:
        return 0.0
    ratio = value / threshold_value
    risk = _risk_from_ratio(ratio)
    if risk >= 0.67:
        explanations.append(f"{label} exceeds or is near SLA threshold")
    return risk


def _lower_risk(value: Optional[float], threshold: Any, label: str, explanations: List[str]) -> float:
    threshold_value = _number(threshold)
    if value is None or threshold_value is None or threshold_value <= 0:
        return 0.0
    ratio = value / threshold_value
    if ratio >= 1.0:
        return 0.0
    risk = _clamp(1.0 - ratio)
    if ratio < 0.75:
        explanations.append(f"{label} is below expected threshold")
    return risk


def _risk_from_ratio(ratio: float) -> float:
    if ratio < 0.70:
        return 0.0
    if ratio < 1.0:
        return (ratio - 0.70) / 0.30 * 0.66
    return min(1.0, 0.67 + (ratio - 1.0) * 0.33)


def _positive_delta_risk(previous: Optional[float], current: Optional[float], high_delta: float, label: str, explanations: List[str]) -> float:
    if previous is None or current is None or current <= previous:
        return 0.0
    risk = _clamp((current - previous) / high_delta)
    if risk >= 0.34:
        explanations.append(f"{label} is worsening")
    return risk


def _negative_delta_risk(previous: Optional[float], current: Optional[float], high_delta: float, label: str, explanations: List[str]) -> float:
    if previous is None or current is None or current >= previous:
        return 0.0
    risk = _clamp((previous - current) / high_delta)
    if risk >= 0.34:
        explanations.append(f"{label} is worsening")
    return risk


def _scale(value: Optional[float], medium: Any, high: Any) -> float:
    current = value or 0.0
    medium_value = _number(medium) or 0.0
    high_value = _number(high) or medium_value
    if current <= 0 or high_value <= 0:
        return 0.0
    if current <= medium_value:
        return 0.33 * (current / medium_value) if medium_value else 0.0
    if current >= high_value:
        return 1.0
    return 0.34 + ((current - medium_value) / (high_value - medium_value)) * 0.66


def _service_has_traffic(metrics: Dict[str, Optional[float]]) -> bool:
    return any((metrics.get(key) or 0.0) > 0 for key in ("throughput_bps", "latency_avg_ms", "delivery_ratio_percent"))


def _number(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
