#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


Number = Optional[float]


@dataclass
class ServiceState:
    service_name: str
    slice_name: str
    latency_avg_ms: Number = None
    latency_max_ms: Number = None
    jitter_ms: Number = None
    packet_loss_percent: Number = None
    throughput_bps: Number = None
    sla_violation_risk: Optional[str] = None
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ServiceState":
        return cls(**payload)


@dataclass
class QueueState:
    queue_id: Optional[str]
    slice_name: str
    packets_total: Number = None
    bytes_total: Number = None
    packet_rate_pps: Number = None
    throughput_bps: Number = None
    timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "QueueState":
        return cls(**payload)


@dataclass
class NetworkTwinState:
    services: List[ServiceState] = field(default_factory=list)
    queues: List[QueueState] = field(default_factory=list)
    ovs_status: Dict[str, Any] = field(default_factory=dict)
    onos_status: Dict[str, Any] = field(default_factory=dict)
    last_updated: Optional[str] = None
    queue_rules: Dict[str, Any] = field(default_factory=dict)
    queue_rules_status: str = "unknown"
    queue_counters: Dict[str, Any] = field(default_factory=dict)
    service_metrics: Dict[str, Any] = field(default_factory=dict)
    intended_policy_state: Dict[str, Any] = field(default_factory=dict)
    policy_verification_state: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "services": [service.to_dict() for service in self.services],
            "queues": [queue.to_dict() for queue in self.queues],
            "ovs_status": self.ovs_status,
            "onos_status": self.onos_status,
            "last_updated": self.last_updated,
            "timestamp": self.last_updated,
            "queue_rules": self.queue_rules,
            "queue_rules_status": self.queue_rules_status,
            "queue_counters": self.queue_counters,
            "service_metrics": self.service_metrics,
            "intended_policy_state": self.intended_policy_state,
            "policy_verification_state": self.policy_verification_state,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "NetworkTwinState":
        return cls(
            services=[
                ServiceState.from_dict(item)
                for item in payload.get("services", [])
                if isinstance(item, dict)
            ],
            queues=[
                QueueState.from_dict(item)
                for item in payload.get("queues", [])
                if isinstance(item, dict)
            ],
            ovs_status=dict(payload.get("ovs_status", {})),
            onos_status=dict(payload.get("onos_status", {})),
            last_updated=payload.get("last_updated") or payload.get("timestamp"),
            queue_rules=dict(payload.get("queue_rules", {})) if isinstance(payload.get("queue_rules"), dict) else {},
            queue_rules_status=str(payload.get("queue_rules_status") or "unknown"),
            queue_counters=dict(payload.get("queue_counters", {})) if isinstance(payload.get("queue_counters"), dict) else {},
            service_metrics=dict(payload.get("service_metrics", {})) if isinstance(payload.get("service_metrics"), dict) else {},
            intended_policy_state=dict(payload.get("intended_policy_state", {})) if isinstance(payload.get("intended_policy_state"), dict) else {},
            policy_verification_state=dict(payload.get("policy_verification_state", {})) if isinstance(payload.get("policy_verification_state"), dict) else {},
        )
