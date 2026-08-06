#!/usr/bin/env python3
"""Combine deterministic risk components into service and overall scores."""

from __future__ import annotations

from typing import Any, Dict, List


DEFAULT_WEIGHTS = {
    "sla_margin_weight": 0.40,
    "trend_weight": 0.25,
    "queue_pressure_weight": 0.20,
    "enforcement_mismatch_weight": 0.15,
}


def score_prediction(
    component_results: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    valid_for_policy: bool = True,
    queue_rule_presence: str = "unknown",
    policy_drift_detected: Any = None,
    controller_available: bool = True,
) -> Dict[str, Any]:
    weights = {**DEFAULT_WEIGHTS, **config.get("risk_weights", {})}
    service_risks: Dict[str, Dict[str, Any]] = {}
    explanations: List[str] = []

    for service, result in component_results.items():
        components = result.get("components", {})
        score = (
            components.get("sla_margin_risk", 0.0) * float(weights["sla_margin_weight"])
            + components.get("trend_risk", 0.0) * float(weights["trend_weight"])
            + components.get("queue_pressure_risk", 0.0) * float(weights["queue_pressure_weight"])
            + components.get("enforcement_mismatch_risk", 0.0) * float(weights["enforcement_mismatch_weight"])
        )
        score = _round_score(score)
        level = risk_level(score, config)
        service_risks[service] = {
            "risk_score": score,
            "risk_level": level,
            "components": {key: _round_score(value) for key, value in components.items()},
            "explanation": result.get("explanation", []),
        }
        explanations.extend(f"{service}: {item}" for item in result.get("explanation", []))

    overall_score = _round_score(max((record["risk_score"] for record in service_risks.values()), default=0.0))
    overall_level = risk_level(overall_score, config)
    return {
        "overall_risk_score": overall_score,
        "overall_risk_level": overall_level,
        "service_risks": service_risks,
        "recommended_policy_action": recommended_policy_action(
            overall_level,
            valid_for_policy=valid_for_policy,
            queue_rule_presence=queue_rule_presence,
            policy_drift_detected=policy_drift_detected,
            controller_available=controller_available,
        ),
        "action_reason": action_reason(
            overall_level,
            valid_for_policy=valid_for_policy,
            queue_rule_presence=queue_rule_presence,
            policy_drift_detected=policy_drift_detected,
            controller_available=controller_available,
        ),
        "explanation": explanations[:20],
    }


def risk_level(score: float, config: Dict[str, Any]) -> str:
    boundaries = config.get("risk_boundaries", {})
    low_max = float(boundaries.get("low_max", 0.33))
    medium_max = float(boundaries.get("medium_max", 0.66))
    if score <= low_max:
        return "low"
    if score <= medium_max:
        return "medium"
    return "high"


def recommended_policy_action(
    level: str,
    valid_for_policy: bool = True,
    queue_rule_presence: str = "unknown",
    policy_drift_detected: Any = None,
    controller_available: bool = True,
) -> str:
    if not valid_for_policy:
        return "PRESERVE_EXISTING_POLICY_BEHAVIOR"
    if not controller_available:
        return "PRESERVE_EXISTING_POLICY_BEHAVIOR"
    if str(queue_rule_presence) == "all_present" and policy_drift_detected is False:
        return {
            "low": "MAINTAIN_CURRENT_POLICY",
            "medium": "MAINTAIN_CURRENT_POLICY_WITH_MONITORING",
            "high": "VERIFY_QUEUE_RULES",
        }.get(level, "MAINTAIN_CURRENT_POLICY_WITH_MONITORING")
    if str(queue_rule_presence) in {"partial", "missing"} or policy_drift_detected is True:
        return {
            "low": "VERIFY_QUEUE_RULES",
            "medium": "VERIFY_QUEUE_RULES",
            "high": "VERIFY_OR_REINSTALL_QUEUE_RULES",
        }.get(level, "VERIFY_QUEUE_RULES")
    return {
        "low": "MAINTAIN_CURRENT_POLICY",
        "medium": "VERIFY_QUEUE_RULES",
        "high": "VERIFY_OR_REINSTALL_QUEUE_RULES",
    }.get(level, "VERIFY_QUEUE_RULES")


def action_reason(
    level: str,
    valid_for_policy: bool = True,
    queue_rule_presence: str = "unknown",
    policy_drift_detected: Any = None,
    controller_available: bool = True,
) -> str:
    if not valid_for_policy:
        return "prediction_not_valid_for_policy"
    if not controller_available:
        return "controller_unavailable"
    if str(queue_rule_presence) == "all_present" and policy_drift_detected is False:
        if level == "high":
            return "high_sla_risk_with_verified_queue_rules"
        if level == "medium":
            return "medium_sla_risk_with_verified_queue_rules_monitor_only"
        return "low_sla_risk_with_verified_queue_rules"
    if str(queue_rule_presence) in {"partial", "missing"}:
        return f"{level}_sla_risk_with_{queue_rule_presence}_queue_rules"
    if policy_drift_detected is True:
        return f"{level}_sla_risk_with_policy_drift"
    return f"{level}_sla_risk_with_unknown_queue_rule_state"


def _round_score(value: float) -> float:
    return round(max(0.0, min(1.0, float(value))), 4)
