from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SliceDecision:
    slice_name: str
    display_name: str
    recommended_action: str
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    healthy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slice_name": self.slice_name,
            "display_name": self.display_name,
            "recommended_action": self.recommended_action,
            "reasons": list(self.reasons),
            "metrics": dict(self.metrics),
            "healthy": self.healthy,
        }


@dataclass
class PolicyCycle:
    timestamp: str
    dry_run_only: bool
    decisions: List[SliceDecision]
    target_profile: Dict[str, Any]
    active_actions: List[str]
    telemetry_reference: Dict[str, Any]
    enforcement_result: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "dry_run_only": self.dry_run_only,
            "decisions": [decision.to_dict() for decision in self.decisions],
            "target_profile": self.target_profile,
            "active_actions": list(self.active_actions),
            "telemetry_reference": dict(self.telemetry_reference),
            "enforcement_result": dict(self.enforcement_result),
        }
