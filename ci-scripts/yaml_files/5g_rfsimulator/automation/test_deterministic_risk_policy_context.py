#!/usr/bin/env python3
"""Deterministic policy-manager checks for DT risk advisory integration."""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from pathlib import Path

from policy_manager import (
    DEFAULT_RISK_HIGH_ACTION,
    DEFAULT_RISK_LOW_ACTION,
    DEFAULT_RISK_MEDIUM_ACTION,
    PolicyManager,
    build_risk_inference_policy_context,
    read_risk_prediction,
)


def utc_timestamp(age_seconds: float = 0.0) -> str:
    stamp = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_seconds)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def prediction(level: str, *, age_seconds: float = 0.0, valid_for_policy: bool = True) -> dict:
    score = {"low": 0.1, "medium": 0.5, "high": 0.9}[level]
    action = {
        "low": DEFAULT_RISK_LOW_ACTION,
        "medium": DEFAULT_RISK_MEDIUM_ACTION,
        "high": DEFAULT_RISK_HIGH_ACTION,
    }[level]
    return {
        "timestamp": utc_timestamp(age_seconds),
        "inference_method": "deterministic_sla_risk_scoring",
        "inference_status": "ok" if valid_for_policy else "stale_twin_state",
        "valid_for_policy": valid_for_policy,
        "overall_risk_score": score,
        "overall_risk_level": level,
        "service_risks": {
            "real_time_control": {"risk_level": level, "risk_score": score},
            "high_throughput_data": {"risk_level": level, "risk_score": score},
            "sensor_telemetry": {"risk_level": level, "risk_score": score},
        },
        "recommended_policy_action": action if valid_for_policy else "PRESERVE_EXISTING_POLICY_BEHAVIOR",
        "data_quality_status": ["ok"] if valid_for_policy else ["stale_twin_state"],
        "queue_rule_presence": "all_present" if valid_for_policy else "unknown",
        "policy_drift_detected": False if valid_for_policy else None,
    }


def context(payload: dict | None, *, enabled: bool = True, max_age_seconds: float = 10.0) -> dict:
    return build_risk_inference_policy_context(
        payload,
        enabled=enabled,
        latest_path="unused.json",
        max_age_seconds=max_age_seconds,
        high_action=DEFAULT_RISK_HIGH_ACTION,
        medium_action=DEFAULT_RISK_MEDIUM_ACTION,
        low_action=DEFAULT_RISK_LOW_ACTION,
    )


def test_risk_inference_disabled_preserves_old_behavior() -> None:
    ctx = context(prediction("high"), enabled=False)
    assert ctx["enabled"] is False
    assert ctx["status"] == "disabled"
    assert ctx["fresh"] is False
    assert ctx["valid_for_policy"] is False


def test_missing_prediction_file_does_not_crash(tmp_dir: Path) -> None:
    missing = tmp_dir / "missing_prediction.json"
    ctx = context(read_risk_prediction(missing))
    assert ctx["status"] == "missing"
    assert ctx["fresh"] is False


def test_stale_prediction_file_does_not_crash() -> None:
    ctx = context(prediction("high", age_seconds=30.0), max_age_seconds=10.0)
    assert ctx["status"] == "stale"
    assert ctx["fresh"] is False


def test_valid_for_policy_false_preserves_existing_behavior() -> None:
    ctx = context(prediction("high", valid_for_policy=False))
    assert ctx["status"] == "stale_twin_state"
    assert ctx["valid_for_policy"] is False
    assert ctx["fresh"] is False


def test_low_risk_maps_to_maintain_or_verify() -> None:
    ctx = context(prediction("low"))
    assert ctx["fresh"] is True
    assert ctx["selected_policy_action"] in {DEFAULT_RISK_LOW_ACTION, DEFAULT_RISK_MEDIUM_ACTION}


def test_medium_risk_maps_to_verify() -> None:
    ctx = context(prediction("medium"))
    assert ctx["fresh"] is True
    assert ctx["selected_policy_action"] == DEFAULT_RISK_MEDIUM_ACTION


def test_high_risk_maps_to_verify_or_reinstall() -> None:
    ctx = context(prediction("high"))
    assert ctx["fresh"] is True
    assert ctx["selected_policy_action"] == DEFAULT_RISK_HIGH_ACTION


def test_enforcement_path_remains_onos_queue_app() -> None:
    manager = PolicyManager.__new__(PolicyManager)
    manager.current_manufacturing_context = {}
    manager.current_risk_context = context(prediction("high"))
    manager.dry_run_only = False
    manager.run_id = "deterministic-risk-policy-test"
    manager.service_state = {"real_time_control": {}}

    record = PolicyManager.build_decision_record(
        manager,
        service_class="real_time_control",
        service_cfg={"policy_name": "real_time_control_guard"},
        telemetry_reference={},
        condition="risk_inference_high_real_time_control_queue_protection",
        metrics={},
        explanation="Deterministic risk advisory high-risk test.",
        recommended_action=DEFAULT_RISK_HIGH_ACTION,
        decision_state="new",
        is_new_decision=True,
        state_after_decision=DEFAULT_RISK_HIGH_ACTION,
        candidate_action=None,
        cooldown_status=None,
    )
    assert record["enforcement_path"] == "ONOS_QUEUE_APP"
    assert record["enforcement_status"] == "pending_ONOS_QUEUE_APP"
    assert record["applied"] is True
    assert record["risk_inference_enabled"] is True
    assert record["valid_for_policy"] is True
    assert record["selected_policy_action"] == DEFAULT_RISK_HIGH_ACTION


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="deterministic-risk-policy-") as tmp:
        tmp_dir = Path(tmp)
        test_risk_inference_disabled_preserves_old_behavior()
        test_missing_prediction_file_does_not_crash(tmp_dir)
        test_stale_prediction_file_does_not_crash()
        test_valid_for_policy_false_preserves_existing_behavior()
        test_low_risk_maps_to_maintain_or_verify()
        test_medium_risk_maps_to_verify()
        test_high_risk_maps_to_verify_or_reinstall()
        test_enforcement_path_remains_onos_queue_app()
    print("PASS deterministic risk policy context tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
