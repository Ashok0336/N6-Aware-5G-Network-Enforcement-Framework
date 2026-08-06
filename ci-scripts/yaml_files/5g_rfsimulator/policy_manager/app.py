#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from policy_manager.config import load_policy_config
from policy_manager.decision_engine import DecisionEngine
from policy_manager.models import PolicyCycle
from policy_manager.onos_client import OnosClient
from policy_manager.ovs_client import OvsClient
from policy_manager.prometheus_exporter import PolicyPrometheusExporter
from policy_manager.telemetry_reader import TelemetryReader
from policy_manager.utils import (
    append_csv_row,
    append_jsonl,
    coerce_bool,
    ensure_directory,
    profile_signature,
    utc_timestamp,
)


CSV_FIELDS = [
    "timestamp",
    "dry_run_only",
    "embb_action",
    "urllc_action",
    "mmtc_action",
    "active_actions",
    "applied",
    "enforcement_mode",
    "reason_summary",
    "telemetry_file",
]


def read_manufacturing_twin_state(path: Path | str) -> Optional[Dict[str, Any]]:
    latest_path = Path(path).expanduser()
    if not latest_path.exists() or not latest_path.is_file():
        return None
    try:
        with latest_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def read_risk_prediction(path: Path | str) -> Optional[Dict[str, Any]]:
    prediction_path = Path(path).expanduser()
    if not prediction_path.exists() or not prediction_path.is_file():
        return None
    try:
        with prediction_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def get_manufacturing_phase(machine_twin: Optional[Dict[str, Any]]) -> str:
    if not isinstance(machine_twin, dict):
        return "unknown"
    return str(machine_twin.get("manufacturing_phase") or "unknown")


def get_service_criticality(machine_twin: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(machine_twin, dict):
        return {}
    criticality = machine_twin.get("service_criticality")
    if not isinstance(criticality, dict):
        return {}
    return {str(key): str(value) for key, value in criticality.items()}


def build_manufacturing_twin_context(config: Dict[str, Any]) -> Dict[str, Any]:
    enabled = bool(config.get("manufacturing_twin_enabled"))
    path = Path(str(config.get("manufacturing_twin_latest_path", "")))
    context: Dict[str, Any] = {
        "enabled": enabled,
        "fresh": False,
        "latest_path": str(path),
        "phase": "disabled" if not enabled else "unknown",
        "availability": None,
        "printer_state_text": None,
        "job_progress_percent": None,
        "service_criticality": {},
        "state_age_seconds": None,
        "error": None,
    }
    if not enabled:
        return context
    machine_twin = read_manufacturing_twin_state(path)
    if machine_twin is None:
        context["error"] = "latest_machine_twin_state_unavailable"
        return context
    age = _timestamp_age_seconds(machine_twin.get("timestamp"))
    context.update(
        {
            "phase": get_manufacturing_phase(machine_twin),
            "availability": machine_twin.get("availability"),
            "printer_state_text": machine_twin.get("printer_state_text"),
            "job_progress_percent": machine_twin.get("job_progress_percent"),
            "service_criticality": get_service_criticality(machine_twin),
            "state_age_seconds": age,
        }
    )
    if age is None:
        context["error"] = "machine_twin_timestamp_unparseable"
        return context
    max_age = float(config.get("manufacturing_twin_max_age_seconds", 10))
    if age > max_age:
        context["error"] = f"machine_twin_stale:{age:.3f}s>{max_age:.3f}s"
        return context
    context["fresh"] = True
    return context


def build_risk_inference_context(config: Dict[str, Any]) -> Dict[str, Any]:
    enabled = bool(config.get("risk_inference_enabled"))
    path = Path(str(config.get("risk_prediction_path", "")))
    context: Dict[str, Any] = {
        "enabled": enabled,
        "status": "disabled" if not enabled else "missing",
        "fresh": False,
        "valid_for_policy": False,
        "latest_path": str(path),
        "overall_risk_score": None,
        "overall_risk_level": "disabled" if not enabled else "unknown",
        "recommended_policy_action": None,
        "selected_policy_action": "PRESERVE_EXISTING_POLICY_BEHAVIOR" if enabled else None,
        "action_reason": None,
        "enforcement_churn_guard_applied": False,
        "data_quality_status": [],
        "queue_rule_presence": None,
        "policy_drift_detected": None,
        "state_age_seconds": None,
        "error": None,
    }
    if not enabled:
        return context

    prediction = read_risk_prediction(path)
    if prediction is None:
        context["error"] = "risk_prediction_unavailable"
        return context

    age = _timestamp_age_seconds(prediction.get("timestamp"))
    context.update(
        {
            "status": str(prediction.get("inference_status") or "unknown"),
            "valid_for_policy": bool(prediction.get("valid_for_policy") is True),
            "overall_risk_score": _to_float(prediction.get("overall_risk_score")),
            "overall_risk_level": _normalize_risk_level(prediction.get("overall_risk_level")),
            "recommended_policy_action": prediction.get("recommended_policy_action"),
            "action_reason": prediction.get("action_reason"),
            "enforcement_churn_guard_applied": bool(prediction.get("enforcement_churn_guard_applied") is True),
            "data_quality_status": prediction.get("data_quality_status") or [],
            "queue_rule_presence": prediction.get("queue_rule_presence"),
            "policy_drift_detected": prediction.get("policy_drift_detected"),
            "state_age_seconds": age,
        }
    )
    if age is None:
        context["status"] = "stale_prediction_file"
        context["error"] = "risk_prediction_timestamp_unparseable"
        return context
    max_age = float(config.get("risk_max_age_seconds", 10))
    if age > max_age:
        context["status"] = "stale_prediction_file"
        context["error"] = f"risk_prediction_stale:{age:.3f}s>{max_age:.3f}s"
        return context
    if not context["valid_for_policy"]:
        context["selected_policy_action"] = "PRESERVE_EXISTING_POLICY_BEHAVIOR"
        context["error"] = f"risk_prediction_not_valid_for_policy:{context['status']}"
        return context

    selected_action = _dt_risk_assisted_action(prediction, config)
    if not selected_action:
        selected_action = str(context.get("recommended_policy_action") or "").strip()
        if not selected_action:
            selected_action = _risk_action_for_level(
                str(context["overall_risk_level"]),
                low_action=str(config.get("risk_low_action", "MAINTAIN_CURRENT_POLICY")),
                medium_action=str(config.get("risk_medium_action", "APPLY_REAL_TIME_CONTROL_PROTECTION")),
                high_action=str(config.get("risk_high_action", "APPLY_REAL_TIME_CONTROL_PROTECTION")),
            )
    context["selected_policy_action"] = selected_action
    action_reasons = {
        "APPLY_REAL_TIME_CONTROL_PROTECTION": "dt_risk_assisted_real_time_control_protection",
        "RESTORE_DATA_THROUGHPUT_SHARE": "dt_risk_assisted_restore_data_throughput_share",
        "PROTECT_SENSOR_TELEMETRY": "dt_risk_assisted_protect_sensor_telemetry",
        "BALANCED_SERVICE_ASSURANCE": "dt_risk_assisted_balanced_service_assurance",
    }
    if selected_action in action_reasons:
        context["action_reason"] = action_reasons[selected_action]
    context["fresh"] = True
    context["status"] = "fresh"
    return context


def apply_risk_action_cooldown(
    context: Dict[str, Any],
    *,
    last_action: Optional[str],
    last_action_monotonic: float,
    now_monotonic: float,
    cooldown_seconds: float,
) -> Dict[str, Any]:
    updated = dict(context)
    selected = str(updated.get("selected_policy_action") or "")
    if (
        updated.get("enabled")
        and updated.get("valid_for_policy") is True
        and selected in {
            "VERIFY_QUEUE_RULES",
            "VERIFY_OR_REINSTALL_QUEUE_RULES",
            "APPLY_REAL_TIME_CONTROL_PROTECTION",
            "RESTORE_DATA_THROUGHPUT_SHARE",
            "PROTECT_SENSOR_TELEMETRY",
            "BALANCED_SERVICE_ASSURANCE",
        }
        and last_action == selected
        and cooldown_seconds > 0
        and (now_monotonic - last_action_monotonic) < cooldown_seconds
    ):
        updated["selected_policy_action"] = "MAINTAIN_CURRENT_POLICY_WITH_MONITORING"
        updated["enforcement_churn_guard_applied"] = True
        updated["action_reason"] = "recent_verification_cooldown"
    return updated


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Closed-loop rule-based Slice Policy Manager for the N6 slicing testbed."
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("config.yaml")),
        help="Path to the policy-manager config file.",
    )
    parser.add_argument("--once", action="store_true", help="Process the latest telemetry snapshot once and exit.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Force dry-run mode.")
    mode.add_argument("--active", action="store_true", help="Enable active enforcement mode.")
    mode.add_argument("--live", action="store_true", help="Enable live enforcement mode.")
    return parser


def _timestamp_age_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return max(0.0, (dt.datetime.now(dt.timezone.utc) - parsed).total_seconds())


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_risk_level(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"low", "medium", "high"}:
        return text
    numeric = _to_float(value)
    if numeric is not None:
        if numeric >= 0.67:
            return "high"
        if numeric >= 0.34:
            return "medium"
        return "low"
    return "unknown"


def _risk_action_for_level(
    level: str,
    *,
    low_action: str,
    medium_action: str,
    high_action: str,
) -> str:
    if level == "high":
        return high_action
    if level == "medium":
        return medium_action
    return low_action


def _dt_risk_assisted_action(prediction: Dict[str, Any], config: Dict[str, Any]) -> str:
    service_risks = prediction.get("service_risks")
    if not isinstance(service_risks, dict):
        return ""
    rtc = service_risks.get("real_time_control") if isinstance(service_risks.get("real_time_control"), dict) else {}
    data = service_risks.get("high_throughput_data") if isinstance(service_risks.get("high_throughput_data"), dict) else {}
    sensor = service_risks.get("sensor_telemetry") if isinstance(service_risks.get("sensor_telemetry"), dict) else {}
    rtc_level = _normalize_risk_level(rtc.get("risk_level"))
    data_level = _normalize_risk_level(data.get("risk_level"))
    sensor_level = _normalize_risk_level(sensor.get("risk_level"))
    components = rtc.get("components") if isinstance(rtc.get("components"), dict) else {}
    data_components = data.get("components") if isinstance(data.get("components"), dict) else {}
    sensor_components = sensor.get("components") if isinstance(sensor.get("components"), dict) else {}
    trend_risk = _to_float(components.get("trend_risk")) or 0.0
    explanations = " ".join(str(item).lower() for item in rtc.get("explanation", []) if item is not None)
    latency_trend_increasing = trend_risk > 0.0 or "latency trend" in explanations
    if rtc_level in {"medium", "high"} or latency_trend_increasing:
        return str(config.get("risk_real_time_control_action", "APPLY_REAL_TIME_CONTROL_PROTECTION"))
    data_needs_share = data_level in {"medium", "high"} or (_to_float(data_components.get("sla_margin_risk")) or 0.0) > 0.0
    sensor_needs_share = sensor_level in {"medium", "high"} or (_to_float(sensor_components.get("sla_margin_risk")) or 0.0) > 0.0
    if data_needs_share and sensor_needs_share:
        return str(config.get("risk_balanced_action", "BALANCED_SERVICE_ASSURANCE"))
    if data_needs_share:
        return str(config.get("risk_high_throughput_data_action", "RESTORE_DATA_THROUGHPUT_SHARE"))
    if sensor_needs_share:
        return str(config.get("risk_sensor_telemetry_action", "PROTECT_SENSOR_TELEMETRY"))
    return ""


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = Path(args.config).resolve()
    try:
        config = load_policy_config(config_path)
    except Exception as exc:
        print(f"[policy][ERROR] failed to load config {config_path}: {exc}")
        return 1
    cli_live = bool(args.active or args.live)
    mode_source = "cli(--dry-run)" if args.dry_run else "cli(--live)" if cli_live else "config"
    dry_run_only = (
        True
        if args.dry_run
        else False
        if cli_live
        else coerce_bool(config["dry_run_only"], field_name="policy_manager.dry_run_only")
    )
    exporter = PolicyPrometheusExporter(
        metrics_http_host=str(config["metrics_http_host"]),
        metrics_http_port=int(config["metrics_http_port"]),
    )

    telemetry_reader = TelemetryReader(
        telemetry_dir=Path(str(config["telemetry_dir"])),
        telemetry_glob=str(config["telemetry_glob"]),
    )
    decision_engine = DecisionEngine(config)
    onos_cfg = dict(config["onos"])
    onos_client = OnosClient(
        base_url=str(onos_cfg["base_url"]),
        username=str(onos_cfg["username"]),
        password=str(onos_cfg["password"]),
        timeout_seconds=float(onos_cfg["timeout_seconds"]),
        dry_run=dry_run_only,
    )
    ovs_cfg = dict(config["ovs"])
    ovs_client = OvsClient(
        container_name=str(ovs_cfg["container_name"]),
        bridge_name=str(ovs_cfg["bridge_name"]),
        egress_port_name=str(ovs_cfg["egress_port_name"]),
        dry_run_only=dry_run_only,
    )

    log_dir = ensure_directory(Path(str(config["log_dir"])))
    stamp = time.strftime("%Y%m%d_%H%M%S")
    log_prefix = str(config.get("log_prefix", "closed_loop_policy"))
    jsonl_path = log_dir / f"{log_prefix}_{stamp}.jsonl"
    csv_path = log_dir / f"{log_prefix}_{stamp}.csv"
    last_applied_signature = ""
    last_apply_monotonic = 0.0
    last_risk_action = ""
    last_risk_action_monotonic = 0.0
    forwarding_continuity_confirmed = False
    last_forwarding_state = "unknown"

    print(f"[policy] config={config_path}")
    print(f"[policy] mode={'dry-run' if dry_run_only else 'live'}")
    print(f"[policy] mode_source={mode_source}")
    print(f"[policy] dry_run_only={str(dry_run_only).lower()}")
    print(f"[policy] telemetry_dir={config['telemetry_dir']}")
    print(
        "[policy] "
        f"manufacturing_twin_enabled={str(bool(config.get('manufacturing_twin_enabled'))).lower()} "
        f"manufacturing_twin_latest_path={config.get('manufacturing_twin_latest_path')}"
    )
    print(
        "[policy] "
        f"risk_inference_enabled={str(bool(config.get('risk_inference_enabled'))).lower()} "
        f"risk_prediction_path={config.get('risk_prediction_path')}"
    )
    print(f"[policy] decisions_jsonl={jsonl_path}")
    print(f"[policy] decisions_csv={csv_path}")
    try:
        exporter.start()
    except RuntimeError as exc:
        print(f"[policy][ERROR] {exc}")
        return 1
    print(
        "[policy] Policy Manager metrics endpoint listening on "
        f"{exporter.metrics_bind_label}"
    )

    while True:
        snapshot = telemetry_reader.read_latest_snapshot()
        if snapshot is None:
            if args.once:
                print("[policy] no telemetry snapshot available yet.")
                return 1
            time.sleep(float(config["poll_interval_seconds"]))
            continue

        machine_context = build_manufacturing_twin_context(config)
        risk_context = build_risk_inference_context(config)
        now_monotonic = time.monotonic()
        risk_context = apply_risk_action_cooldown(
            risk_context,
            last_action=last_risk_action,
            last_action_monotonic=last_risk_action_monotonic,
            now_monotonic=now_monotonic,
            cooldown_seconds=float(config.get("risk_action_cooldown_seconds", 10)),
        )
        if (
            risk_context.get("fresh")
            and risk_context.get("valid_for_policy") is True
            and risk_context.get("enforcement_churn_guard_applied") is not True
            and str(risk_context.get("selected_policy_action") or "")
            in {
                "VERIFY_QUEUE_RULES",
                "VERIFY_OR_REINSTALL_QUEUE_RULES",
                "APPLY_REAL_TIME_CONTROL_PROTECTION",
                "RESTORE_DATA_THROUGHPUT_SHARE",
                "PROTECT_SENSOR_TELEMETRY",
                "BALANCED_SERVICE_ASSURANCE",
            }
        ):
            last_risk_action = str(risk_context.get("selected_policy_action"))
            last_risk_action_monotonic = now_monotonic
        evaluation = decision_engine.evaluate(
            snapshot,
            machine_context=machine_context,
            risk_context=risk_context,
        )
        decisions = list(evaluation["decisions"])
        target_profile = dict(evaluation["target_profile"])
        active_actions = list(evaluation["active_actions"])
        enforcement_required = _has_non_maintain_action(decisions, active_actions)
        target_signature = profile_signature(target_profile)
        enforcement_result = {
            "applied": False,
            "mode": "dry-run" if dry_run_only else "live",
            "onos": {},
            "ovs": {},
            "forwarding_state": "dry-run" if dry_run_only else "not_rechecked",
            "qos_state": "planned_only" if dry_run_only else "not_reapplied",
            "queue_assignment_state": "planned_only" if dry_run_only else "not_reapplied",
            "forwarding_check_result": None,
            "applied_reason": "dry_run_only is enabled." if dry_run_only else "profile unchanged or cooldown active.",
            "reason": "dry_run_only is enabled." if dry_run_only else "profile unchanged or cooldown active.",
        }

        if dry_run_only and enforcement_required:
            enforcement_result.update(
                {
                    "applied": False,
                    "reason": "dry_run",
                    "applied_reason": "dry_run",
                    "enforcement_path": "ONOS_QUEUE_APP",
                    "action": "INSTALL_OR_VERIFY_QUEUE_RULES",
                }
            )
            print(
                "[policy] "
                f"phase={machine_context.get('phase')} "
                f"machine_state={machine_context.get('printer_state_text') or machine_context.get('availability') or 'unknown'} "
                f"service_criticality={machine_context.get('service_criticality')} "
                "applied=false enforcement_path=ONOS_QUEUE_APP reason=dry_run"
            )

        if not dry_run_only and enforcement_required:
            cooldown_seconds = float(config["decision_cooldown_seconds"])
            if target_signature != last_applied_signature and (time.monotonic() - last_apply_monotonic) >= cooldown_seconds:
                slice_flow_rules = [
                    {
                        "name": slice_name,
                        "udp_port": int(slice_cfg["udp_port"]),
                        "queue_id": int(slice_cfg["queue_id"]),
                        "priority": _default_flow_priority(slice_name),
                    }
                    for slice_name, slice_cfg in dict(config["slices"]).items()
                ]
                force_onos_refresh = bool(config["force_onos_flow_refresh"])
                onos_enabled = bool(config["ensure_onos_slice_flows"])
                onos_result: Dict[str, Any] = {}
                forwarding_check_result: Optional[Dict[str, Any]] = None
                forwarding_state = "not_required"
                forwarding_ready = True
                onos_refresh_required = onos_enabled and (force_onos_refresh or not forwarding_continuity_confirmed)

                if onos_refresh_required:
                    onos_result = onos_client.ensure_baseline_slice_flows(
                        devices_path=str(onos_cfg["devices_path"]),
                        upf_port_name=str(onos_cfg["upf_port_name"]),
                        edn_port_name=str(onos_cfg["edn_port_name"]),
                        slice_flow_rules=slice_flow_rules,
                        base_forward_flow_priority=5000,
                        reverse_flow_priority=20000,
                        arp_flow_priority=45000,
                        force_refresh=force_onos_refresh,
                    )
                    if onos_result.get("ok"):
                        forwarding_state = _derive_forwarding_state(onos_result)
                    else:
                        forwarding_check_result = ovs_client.check_forwarding_continuity(
                            upf_port_name=str(onos_cfg["upf_port_name"])
                        )
                        if forwarding_check_result.get("ok"):
                            forwarding_state = "temporarily_unavailable_existing_baseline"
                        else:
                            forwarding_state = "missing"
                            forwarding_ready = False
                elif onos_enabled:
                    onos_result = _build_forwarding_skip_result(
                        slice_flow_rules=slice_flow_rules,
                        reason="Forwarding continuity was confirmed earlier, so ONOS refresh was skipped for this cycle.",
                    )
                    forwarding_state = "already_present"

                ovs_qos_result = ovs_client.apply_queue_profile(target_profile)
                ovs_queue_result = ovs_client.install_or_verify_onos_queue_rules()
                ovs_result = {
                    "ok": bool(ovs_qos_result.get("ok")) and bool(ovs_queue_result.get("ok")),
                    "qos_result": ovs_qos_result,
                    "queue_assignment_result": ovs_queue_result,
                }
                qos_state = "applied" if ovs_qos_result.get("ok") else "failed"
                queue_assignment_state = "ensured" if ovs_queue_result.get("ok") else "failed"
                applied = bool(ovs_result.get("ok")) and forwarding_ready
                applied_reason = _compose_applied_reason(
                    qos_ok=bool(ovs_qos_result.get("ok")),
                    queue_assignment_ok=bool(ovs_queue_result.get("ok")),
                    forwarding_state=forwarding_state,
                )
                enforcement_result = {
                    "applied": applied,
                    "mode": "live",
                    "onos": onos_result,
                    "ovs": ovs_result,
                    "forwarding_state": forwarding_state,
                    "qos_state": qos_state,
                    "queue_assignment_state": queue_assignment_state,
                    "forwarding_check_result": forwarding_check_result,
                    "enforcement_path": "ONOS_QUEUE_APP",
                    "action": "INSTALL_OR_VERIFY_QUEUE_RULES",
                    "enforcement_status": ovs_queue_result.get("enforcement_status", ""),
                    "enforcement_error": ovs_queue_result.get("enforcement_error", ""),
                    "applied_reason": applied_reason,
                    "reason": applied_reason,
                    "manufacturing_phase": machine_context.get("phase"),
                    "machine_state": machine_context.get("printer_state_text")
                    or machine_context.get("availability"),
                    "machine_service_criticality": machine_context.get("service_criticality"),
                }
                print(
                    "[policy] "
                    f"forwarding_state={forwarding_state} "
                    f"ovs_qos_ok={bool(ovs_qos_result.get('ok'))} "
                    f"ovs_queue_assignment_ok={bool(ovs_queue_result.get('ok'))} "
                    f"onos_queue_ops_skipped={bool(onos_result.get('queue_operations_skipped'))}"
                )
                print(
                    "[policy] "
                    f"applied={applied} "
                    f"phase={machine_context.get('phase')} "
                    f"machine_state={machine_context.get('printer_state_text') or machine_context.get('availability') or 'unknown'} "
                    f"service_criticality={machine_context.get('service_criticality')} "
                    "enforcement_path=ONOS_QUEUE_APP "
                    "action=INSTALL_OR_VERIFY_QUEUE_RULES "
                    f"enforcement_status={ovs_queue_result.get('enforcement_status', '')}"
                )
                if not applied:
                    print(
                        "[policy][ERROR] "
                        f"enforcement_error={ovs_queue_result.get('enforcement_error') or applied_reason}"
                    )
                if forwarding_ready:
                    forwarding_continuity_confirmed = True
                    last_forwarding_state = forwarding_state
                elif onos_enabled:
                    forwarding_continuity_confirmed = False
                    last_forwarding_state = forwarding_state
                if applied:
                    last_applied_signature = target_signature
                    last_apply_monotonic = time.monotonic()
            else:
                enforcement_result["forwarding_state"] = (
                    last_forwarding_state if forwarding_continuity_confirmed else "not_rechecked"
                )
                enforcement_result["applied_reason"] = "profile unchanged or cooldown not elapsed."
                enforcement_result["reason"] = enforcement_result["applied_reason"]
                enforcement_result["enforcement_path"] = "ONOS_QUEUE_APP"
                enforcement_result["action"] = "INSTALL_OR_VERIFY_QUEUE_RULES"
                print(
                    "[policy] "
                    f"phase={machine_context.get('phase')} "
                    f"machine_state={machine_context.get('printer_state_text') or machine_context.get('availability') or 'unknown'} "
                    f"service_criticality={machine_context.get('service_criticality')} "
                    "applied=false enforcement_path=ONOS_QUEUE_APP "
                    "reason=profile unchanged or cooldown not elapsed."
                )
        elif not dry_run_only:
            enforcement_result.update(
                {
                    "applied": False,
                    "reason": "maintain_current_n6_policy",
                    "applied_reason": "maintain_current_n6_policy",
                }
            )
            print(
                "[policy] "
                f"phase={machine_context.get('phase')} "
                f"machine_state={machine_context.get('printer_state_text') or machine_context.get('availability') or 'unknown'} "
                f"service_criticality={machine_context.get('service_criticality')} "
                "applied=false reason=maintain_current_n6_policy"
            )

        enforcement_result.update(
            {
                "manufacturing_twin_enabled": bool(machine_context.get("enabled")),
                "manufacturing_phase": machine_context.get("phase"),
                "machine_availability": machine_context.get("availability"),
                "printer_state_text": machine_context.get("printer_state_text"),
                "job_progress_percent": machine_context.get("job_progress_percent"),
                "machine_service_criticality": machine_context.get("service_criticality"),
                "machine_state": machine_context.get("printer_state_text")
                or machine_context.get("availability"),
                "risk_inference_enabled": bool(risk_context.get("enabled")),
                "risk_inference_status": risk_context.get("status"),
                "valid_for_policy": bool(risk_context.get("valid_for_policy")),
                "overall_risk_score": risk_context.get("overall_risk_score"),
                "overall_risk_level": risk_context.get("overall_risk_level"),
                "recommended_policy_action": risk_context.get("recommended_policy_action"),
                "selected_policy_action": risk_context.get("selected_policy_action"),
                "action_reason": risk_context.get("action_reason"),
                "enforcement_churn_guard_applied": bool(risk_context.get("enforcement_churn_guard_applied")),
            }
        )

        cycle = PolicyCycle(
            timestamp=utc_timestamp(),
            dry_run_only=dry_run_only,
            decisions=decisions,
            target_profile=target_profile,
            active_actions=active_actions,
            telemetry_reference={
                "telemetry_timestamp": snapshot.get("timestamp"),
                "snapshot_index": snapshot.get("snapshot_index"),
                "telemetry_file": snapshot.get("_telemetry_file"),
            },
            enforcement_result=enforcement_result,
        )

        payload = {
            "event_type": "policy_cycle",
            **cycle.to_dict(),
            "manufacturing_twin_enabled": bool(machine_context.get("enabled")),
            "manufacturing_phase": machine_context.get("phase"),
            "machine_availability": machine_context.get("availability"),
            "printer_state_text": machine_context.get("printer_state_text"),
            "job_progress_percent": machine_context.get("job_progress_percent"),
            "machine_service_criticality": machine_context.get("service_criticality"),
            "machine_state": machine_context.get("printer_state_text")
            or machine_context.get("availability"),
            "machine_twin_fresh": bool(machine_context.get("fresh")),
            "machine_twin_state_age_seconds": machine_context.get("state_age_seconds"),
            "machine_twin_latest_path": machine_context.get("latest_path"),
            "machine_twin_error": machine_context.get("error"),
            "risk_inference_enabled": bool(risk_context.get("enabled")),
            "risk_inference_status": risk_context.get("status"),
            "valid_for_policy": bool(risk_context.get("valid_for_policy")),
            "overall_risk_score": risk_context.get("overall_risk_score"),
            "overall_risk_level": risk_context.get("overall_risk_level"),
            "recommended_policy_action": risk_context.get("recommended_policy_action"),
            "selected_policy_action": risk_context.get("selected_policy_action"),
            "action_reason": risk_context.get("action_reason"),
            "enforcement_churn_guard_applied": bool(risk_context.get("enforcement_churn_guard_applied")),
            "risk_prediction_fresh": bool(risk_context.get("fresh")),
            "risk_prediction_state_age_seconds": risk_context.get("state_age_seconds"),
            "risk_prediction_path": risk_context.get("latest_path"),
            "risk_inference_error": risk_context.get("error"),
            "risk_inference_data_quality_status": risk_context.get("data_quality_status"),
            "queue_rule_presence": risk_context.get("queue_rule_presence"),
            "policy_drift_detected": risk_context.get("policy_drift_detected"),
            "enforcement_path": enforcement_result.get("enforcement_path"),
            "enforcement_status": enforcement_result.get("enforcement_status")
            or enforcement_result.get("reason"),
            "applied": bool(enforcement_result.get("applied")),
            "decision_reason": _decision_reason_with_machine_context(decisions, machine_context),
        }
        try:
            exporter.update_cycle(
                cycle_timestamp=cycle.timestamp,
                dry_run_only=dry_run_only,
                decisions=decisions,
                enforcement_result=enforcement_result,
            )
        except Exception as exc:
            print(f"[policy][WARN] failed to update Prometheus metrics: {exc}")
        append_jsonl(jsonl_path, payload)
        append_csv_row(
            csv_path,
            {
                "timestamp": cycle.timestamp,
                "dry_run_only": cycle.dry_run_only,
                "embb_action": _find_action(decisions, "embb"),
                "urllc_action": _find_action(decisions, "urllc"),
                "mmtc_action": _find_action(decisions, "mmtc"),
                "active_actions": ",".join(active_actions),
                "applied": enforcement_result.get("applied"),
                "enforcement_mode": enforcement_result.get("mode"),
                "reason_summary": " | ".join(_flatten_reasons(decisions)),
                "telemetry_file": snapshot.get("_telemetry_file"),
            },
            CSV_FIELDS,
        )
        print(
            "[policy] "
            f"ts={cycle.timestamp} "
            f"urllc={_find_action(decisions, 'urllc')} "
            f"embb={_find_action(decisions, 'embb')} "
            f"mmtc={_find_action(decisions, 'mmtc')} "
            f"phase={machine_context.get('phase')} "
            f"machine_state={machine_context.get('printer_state_text') or machine_context.get('availability') or 'unknown'} "
            f"service_criticality={machine_context.get('service_criticality')} "
            f"risk={risk_context.get('status')}/{risk_context.get('overall_risk_level')}/{risk_context.get('overall_risk_score')} "
            f"valid_for_policy={str(bool(risk_context.get('valid_for_policy'))).lower()} "
            f"selected_policy_action={risk_context.get('selected_policy_action') or 'none'} "
            f"action_reason={risk_context.get('action_reason') or 'none'} "
            f"enforcement_churn_guard_applied={str(bool(risk_context.get('enforcement_churn_guard_applied'))).lower()} "
            f"applied={enforcement_result.get('applied')} "
            f"enforcement_path={enforcement_result.get('enforcement_path') or 'none'}"
        )
        if args.once:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        time.sleep(float(config["poll_interval_seconds"]))


def _find_action(decisions: List[Any], slice_name: str) -> str:
    for decision in decisions:
        if getattr(decision, "slice_name", "") == slice_name:
            return str(getattr(decision, "recommended_action", ""))
    return ""


def _has_non_maintain_action(decisions: List[Any], active_actions: List[str]) -> bool:
    maintain = {"", "MAINTAIN_CURRENT_POLICY", "MAINTAIN_CURRENT_POLICY_WITH_MONITORING", "maintain_current_n6_policy"}
    for action in active_actions:
        if str(action).strip() not in maintain:
            return True
    for decision in decisions:
        action = str(getattr(decision, "recommended_action", "")).strip()
        if action and action not in maintain:
            return True
    return False


def _flatten_reasons(decisions: List[Any]) -> List[str]:
    results: List[str] = []
    for decision in decisions:
        display = getattr(decision, "display_name", getattr(decision, "slice_name", "slice"))
        reasons = getattr(decision, "reasons", [])
        results.append(f"{display}: {', '.join(str(reason) for reason in reasons)}")
    return results


def _decision_reason_with_machine_context(
    decisions: List[Any],
    machine_context: Dict[str, Any],
) -> str:
    reason = " | ".join(_flatten_reasons(decisions))
    if machine_context.get("enabled"):
        machine_state = machine_context.get("printer_state_text") or machine_context.get("availability") or "unknown"
        reason = (
            f"{reason} | Manufacturing twin context: "
            f"phase={machine_context.get('phase')}, machine_state={machine_state}."
        )
    return reason


def _build_forwarding_skip_result(*, slice_flow_rules: List[Dict[str, Any]], reason: str) -> Dict[str, Any]:
    return {
        "ok": True,
        "scope": "forwarding_only",
        "refresh_skipped": True,
        "queue_operations_attempted": False,
        "queue_operations_skipped": True,
        "queue_operation_reason": (
            "ONOS REST queue instructions are skipped; OVS installs per-slice queue assignment flows directly."
        ),
        "skipped_queue_rules": list(slice_flow_rules),
        "error": "",
        "reason": reason,
    }


def _derive_forwarding_state(onos_result: Dict[str, Any]) -> str:
    if onos_result.get("installed_forwarding_flows"):
        return "freshly_installed"
    if onos_result.get("existing_forwarding_flows"):
        return "already_present"
    return "verified"


def _compose_applied_reason(
    *,
    qos_ok: bool,
    queue_assignment_ok: bool,
    forwarding_state: str,
) -> str:
    if not qos_ok:
        return "OVS QoS profile update failed."
    if not queue_assignment_ok:
        return "OVS queue-assignment flow update failed."
    if forwarding_state == "missing":
        return "Forwarding continuity could not be confirmed."
    if forwarding_state == "freshly_installed":
        return "OVS QoS and queue assignment succeeded, and ONOS forwarding flows were refreshed successfully."
    if forwarding_state == "already_present":
        return "OVS QoS and queue assignment succeeded, and forwarding continuity was already present."
    if forwarding_state == "temporarily_unavailable_existing_baseline":
        return (
            "OVS QoS and queue assignment succeeded, and forwarding continuity was already present despite a temporary "
            "ONOS refresh failure."
        )
    if forwarding_state == "not_required":
        return "OVS QoS and queue assignment succeeded without requiring ONOS forwarding management."
    return "OVS QoS and queue assignment succeeded, and forwarding continuity was verified."


def _default_flow_priority(slice_name: str) -> int:
    if slice_name == "urllc":
        return 50000
    if slice_name == "embb":
        return 40000
    if slice_name == "mmtc":
        return 30000
    return 20000


if __name__ == "__main__":
    sys.exit(main())
