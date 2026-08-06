from __future__ import annotations

from typing import Any, Dict, List

from policy_manager.models import SliceDecision
from policy_manager.utils import merge_queue_profile


SERVICE_CLASS_TO_SLICE = {
    "real_time_control": "urllc",
    "high_throughput_data": "embb",
    "sensor_telemetry": "mmtc",
}

PHASE_SERVICE_CLASSES = {
    "active_printing": {"real_time_control", "sensor_telemetry"},
    "job_upload": {"high_throughput_data"},
    "paused": {"real_time_control", "sensor_telemetry"},
    "machine_error": {"real_time_control", "sensor_telemetry"},
}


class DecisionEngine:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.slices = dict(config.get("slices", {}))
        queue_profiles = dict(config.get("queue_profiles", {}))
        self.baseline_profile = dict(queue_profiles.get("baseline", {}))
        self.overlays = dict(queue_profiles.get("overlays", {}))

    def evaluate(
        self,
        snapshot: Dict[str, Any],
        machine_context: Dict[str, Any] | None = None,
        risk_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        slice_metrics = dict(snapshot.get("slice_metrics", {}))
        decisions: List[SliceDecision] = []
        active_actions: List[str] = []
        urllc_healthy = True

        urllc_metrics = dict(slice_metrics.get("urllc", {}))
        urllc_decision = self._evaluate_urllc(urllc_metrics)
        decisions.append(urllc_decision)
        urllc_healthy = urllc_decision.healthy
        if urllc_decision.recommended_action != "MAINTAIN_CURRENT_POLICY":
            active_actions.append(urllc_decision.recommended_action)

        embb_metrics = dict(slice_metrics.get("embb", {}))
        embb_decision = self._evaluate_embb(embb_metrics, urllc_healthy=urllc_healthy)
        decisions.append(embb_decision)
        if embb_decision.recommended_action != "MAINTAIN_CURRENT_POLICY":
            active_actions.append(embb_decision.recommended_action)

        mmtc_metrics = dict(slice_metrics.get("mmtc", {}))
        mmtc_decision = self._evaluate_mmtc(mmtc_metrics)
        decisions.append(mmtc_decision)
        if mmtc_decision.recommended_action != "MAINTAIN_CURRENT_POLICY":
            active_actions.append(mmtc_decision.recommended_action)

        if machine_context and machine_context.get("enabled") and machine_context.get("fresh"):
            self._apply_machine_context(decisions, active_actions, machine_context)
        if risk_context and risk_context.get("enabled"):
            self._apply_risk_context(decisions, active_actions, risk_context)

        target_profile = dict(self.baseline_profile)
        for action_name in active_actions:
            overlay = dict(self.overlays.get(action_name, {}))
            target_profile = merge_queue_profile(target_profile, overlay)

        return {
            "decisions": decisions,
            "active_actions": active_actions,
            "target_profile": target_profile,
        }

    def _apply_risk_context(
        self,
        decisions: List[SliceDecision],
        active_actions: List[str],
        risk_context: Dict[str, Any],
    ) -> None:
        selected_action = str(
            risk_context.get("selected_policy_action") or "PRESERVE_EXISTING_POLICY_BEHAVIOR"
        )
        for decision in decisions:
            decision.selected_policy_action = selected_action
            decision.metrics["risk_inference_status"] = risk_context.get("status")
            decision.metrics["valid_for_policy"] = risk_context.get("valid_for_policy")
            decision.metrics["overall_risk_score"] = risk_context.get("overall_risk_score")
            decision.metrics["overall_risk_level"] = risk_context.get("overall_risk_level")
            decision.metrics["selected_policy_action"] = selected_action
            decision.metrics["action_reason"] = risk_context.get("action_reason")
            decision.metrics["enforcement_churn_guard_applied"] = risk_context.get("enforcement_churn_guard_applied")
            if selected_action == "APPLY_REAL_TIME_CONTROL_PROTECTION" and decision.slice_name == "urllc":
                decision.recommended_action = selected_action
                decision.healthy = False
                decision.reasons.append(
                    "DT-risk-assisted policy selected APPLY_REAL_TIME_CONTROL_PROTECTION: "
                    "verify UDP destination port 5202 maps to queue 2, protect queue 2, "
                    "and cap high-throughput queue 1 through ONOS_QUEUE_APP."
                )
            elif selected_action == "RESTORE_DATA_THROUGHPUT_SHARE" and decision.slice_name == "embb":
                decision.recommended_action = selected_action
                decision.healthy = False
                decision.reasons.append(
                    "DT-risk-assisted policy selected RESTORE_DATA_THROUGHPUT_SHARE: "
                    "increase queue 1 share while keeping queue 2 protected through ONOS_QUEUE_APP."
                )
            elif selected_action == "PROTECT_SENSOR_TELEMETRY" and decision.slice_name == "mmtc":
                decision.recommended_action = selected_action
                decision.healthy = False
                decision.reasons.append(
                    "DT-risk-assisted policy selected PROTECT_SENSOR_TELEMETRY: "
                    "protect queue 3 without removing queue 2 protection through ONOS_QUEUE_APP."
                )
            elif selected_action == "BALANCED_SERVICE_ASSURANCE":
                decision.recommended_action = selected_action
                decision.healthy = False
                decision.reasons.append(
                    "DT-risk-assisted policy selected BALANCED_SERVICE_ASSURANCE: "
                    "balance queue 1 throughput, queue 2 protection, and queue 3 stability through ONOS_QUEUE_APP."
                )

        if not risk_context.get("fresh") or risk_context.get("valid_for_policy") is not True:
            return
        if selected_action in {"", "MAINTAIN_CURRENT_POLICY", "MAINTAIN_CURRENT_POLICY_WITH_MONITORING", "PRESERVE_EXISTING_POLICY_BEHAVIOR"}:
            return
        if selected_action not in active_actions:
            active_actions.append(selected_action)

    def _apply_machine_context(
        self,
        decisions: List[SliceDecision],
        active_actions: List[str],
        machine_context: Dict[str, Any],
    ) -> None:
        phase = str(machine_context.get("phase") or "unknown")
        service_classes = PHASE_SERVICE_CLASSES.get(phase, set())
        criticality = machine_context.get("service_criticality", {})
        for service_class in sorted(service_classes):
            slice_name = SERVICE_CLASS_TO_SLICE.get(service_class)
            if not slice_name:
                continue
            decision = next((item for item in decisions if item.slice_name == slice_name), None)
            if decision is None:
                continue
            slice_cfg = dict(self.slices.get(slice_name, {}))
            action = str(slice_cfg.get("action_name", decision.recommended_action))
            decision.recommended_action = action
            decision.healthy = False
            decision.metrics["manufacturing_phase"] = phase
            decision.metrics["machine_availability"] = machine_context.get("availability")
            decision.metrics["printer_state_text"] = machine_context.get("printer_state_text")
            decision.metrics["job_progress_percent"] = machine_context.get("job_progress_percent")
            decision.metrics["machine_service_criticality"] = criticality
            decision.reasons.append(
                "Manufacturing twin phase "
                f"{phase} requires verifying the {service_class} queue "
                f"(criticality={criticality.get(service_class, 'unspecified')})."
            )
            if action != "MAINTAIN_CURRENT_POLICY" and action not in active_actions:
                active_actions.append(action)

    def _evaluate_urllc(self, metrics: Dict[str, Any]) -> SliceDecision:
        cfg = dict(self.slices.get("urllc", {}))
        sla = dict(cfg.get("sla", {}))
        reasons: List[str] = []
        healthy = True
        latency_avg = _to_float(metrics.get("latency_avg_ms"))
        jitter = _to_float(metrics.get("jitter_ms"))
        loss = _to_float(metrics.get("loss_percent"))
        traffic_active = bool(metrics.get("traffic_active"))

        if latency_avg is not None and latency_avg > _to_float(sla.get("max_latency_ms"), default=20.0):
            reasons.append(f"URLLC average latency {latency_avg:.2f} ms exceeds threshold.")
            healthy = False
        if jitter is not None and jitter > _to_float(sla.get("max_jitter_ms"), default=5.0):
            reasons.append(f"URLLC jitter {jitter:.2f} ms exceeds threshold.")
            healthy = False
        if loss is not None and loss > _to_float(sla.get("max_loss_percent"), default=1.0):
            reasons.append(f"URLLC loss {loss:.2f}% exceeds threshold.")
            healthy = False
        if traffic_active and metrics.get("flow_rule_present") is False:
            reasons.append("URLLC queue-selection flow is missing while traffic is active.")
            healthy = False

        return SliceDecision(
            slice_name="urllc",
            display_name=str(cfg.get("display_name", "URLLC")),
            recommended_action=str(cfg.get("action_name", "INCREASE_URLLC_PROTECTION"))
            if not healthy
            else "MAINTAIN_CURRENT_POLICY",
            reasons=reasons or ["URLLC telemetry is within SLA."],
            metrics=metrics,
            healthy=healthy,
        )

    def _evaluate_embb(self, metrics: Dict[str, Any], urllc_healthy: bool) -> SliceDecision:
        cfg = dict(self.slices.get("embb", {}))
        sla = dict(cfg.get("sla", {}))
        reasons: List[str] = []
        healthy = True
        traffic_active = bool(metrics.get("traffic_active"))
        throughput = _first_float(
            metrics.get("throughput_bps"),
            metrics.get("flow_throughput_bps"),
            metrics.get("sender_average_bitrate_bps"),
        )
        throughput_threshold = _to_float(sla.get("min_throughput_bps"), default=60_000_000.0)
        if traffic_active and throughput is not None and throughput < throughput_threshold:
            healthy = False
            reasons.append(
                f"eMBB throughput {throughput:.0f} bps is below threshold {throughput_threshold:.0f} bps."
            )
        if traffic_active and metrics.get("flow_rule_present") is False and urllc_healthy:
            healthy = False
            reasons.append("eMBB queue-selection flow is missing while traffic is active.")
        elif not traffic_active:
            reasons.append("No active eMBB traffic was observed in the latest telemetry window.")
        if not urllc_healthy:
            healthy = True
            reasons.append("URLLC is degraded, so eMBB boosting is intentionally suppressed.")

        return SliceDecision(
            slice_name="embb",
            display_name=str(cfg.get("display_name", "eMBB")),
            recommended_action=str(cfg.get("action_name", "INCREASE_EMBB_BANDWIDTH_SHARE"))
            if (not healthy and urllc_healthy)
            else "MAINTAIN_CURRENT_POLICY",
            reasons=reasons
            or [
                "eMBB throughput is within SLA."
                if traffic_active and urllc_healthy
                else "eMBB throughput is within SLA or URLLC has priority."
            ],
            metrics=metrics,
            healthy=healthy or not urllc_healthy,
        )

    def _evaluate_mmtc(self, metrics: Dict[str, Any]) -> SliceDecision:
        cfg = dict(self.slices.get("mmtc", {}))
        sla = dict(cfg.get("sla", {}))
        reasons: List[str] = []
        healthy = True
        traffic_active = bool(metrics.get("traffic_active"))
        delivery_ratio = _first_float(
            metrics.get("delivery_ratio_percent"),
            metrics.get("reliability_proxy_percent"),
        )
        loss = _to_float(metrics.get("loss_percent"))
        delivery_threshold = _to_float(sla.get("min_delivery_ratio_percent"), default=98.0)
        loss_threshold = _to_float(sla.get("max_loss_percent"), default=2.0)

        if traffic_active and delivery_ratio is not None and delivery_ratio < delivery_threshold:
            reasons.append(
                f"mMTC delivery ratio {delivery_ratio:.2f}% is below threshold {delivery_threshold:.2f}%."
            )
            healthy = False
        if traffic_active and loss is not None and loss > loss_threshold:
            reasons.append(f"mMTC loss {loss:.2f}% exceeds threshold {loss_threshold:.2f}%.")
            healthy = False
        if traffic_active and metrics.get("flow_rule_present") is False:
            reasons.append("mMTC queue-selection flow is missing while traffic is active.")
            healthy = False
        if not traffic_active:
            reasons.append("No active mMTC traffic was observed in the latest telemetry window.")

        return SliceDecision(
            slice_name="mmtc",
            display_name=str(cfg.get("display_name", "mMTC")),
            recommended_action=str(cfg.get("action_name", "INCREASE_MMTC_RESERVED_RATE"))
            if not healthy
            else "MAINTAIN_CURRENT_POLICY",
            reasons=reasons
            or [
                "mMTC telemetry is within SLA."
                if traffic_active and (delivery_ratio is not None or loss is not None)
                else "mMTC telemetry is within SLA or delivery ratio is unavailable."
            ],
            metrics=metrics,
            healthy=healthy,
        )


def _to_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_float(*values: Any) -> float | None:
    for value in values:
        numeric = _to_float(value)
        if numeric is not None:
            return numeric
    return None
