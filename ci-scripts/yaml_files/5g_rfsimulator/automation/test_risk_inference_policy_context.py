#!/usr/bin/env python3
"""Offline checks for risk inference context in the deterministic policy manager."""

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
    is_risk_prediction_fresh,
    read_risk_prediction,
)


def utc_timestamp(age_seconds: float = 0.0) -> str:
    stamp = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_seconds)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def prediction_payload(level: str, *, age_seconds: float = 0.0) -> dict:
    return {
        "timestamp": utc_timestamp(age_seconds),
        "inference_status": "ok",
        "valid_for_policy": True,
        "overall_risk_score": {"low": 0.1, "medium": 0.5, "high": 0.9}[level],
        "overall_risk_level": level,
        "service_risks": {
            "real_time_control": {"risk_level": level},
            "high_throughput_data": {"risk_level": "low"},
            "sensor_telemetry": {"risk_level": level if level != "low" else "low"},
        },
        "recommended_policy_action": f"{level}_risk_queue_policy",
    }


def write_prediction(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_context(payload: dict | None, *, max_age_seconds: float = 10.0) -> dict:
    return build_risk_inference_policy_context(
        payload,
        enabled=True,
        latest_path="unused.json",
        max_age_seconds=max_age_seconds,
        high_action=DEFAULT_RISK_HIGH_ACTION,
        medium_action=DEFAULT_RISK_MEDIUM_ACTION,
        low_action=DEFAULT_RISK_LOW_ACTION,
    )


def test_missing_prediction_file_does_not_crash(tmp_dir: Path) -> None:
    missing_path = tmp_dir / "missing_prediction.json"
    prediction = read_risk_prediction(missing_path)
    context = build_context(prediction)
    assert prediction is None
    assert context["status"] == "missing"
    assert context["fresh"] is False


def test_stale_prediction_file_does_not_crash(tmp_dir: Path) -> None:
    stale_path = tmp_dir / "stale_prediction.json"
    write_prediction(stale_path, prediction_payload("high", age_seconds=30.0))
    prediction = read_risk_prediction(stale_path)
    context = build_context(prediction, max_age_seconds=10.0)
    assert prediction is not None
    assert is_risk_prediction_fresh(prediction, 10.0) is False
    assert context["status"] == "stale"
    assert context["fresh"] is False


def test_low_risk_maps_to_maintain_or_verify() -> None:
    context = build_context(prediction_payload("low"))
    assert context["status"] == "fresh"
    assert context["overall_risk_level"] == "low"
    assert context["selected_policy_action"] in {
        DEFAULT_RISK_LOW_ACTION,
        DEFAULT_RISK_MEDIUM_ACTION,
    }


def test_medium_risk_maps_to_verify() -> None:
    context = build_context(prediction_payload("medium"))
    assert context["status"] == "fresh"
    assert context["overall_risk_level"] == "medium"
    assert context["selected_policy_action"] == DEFAULT_RISK_MEDIUM_ACTION


def test_high_risk_maps_to_verify_or_reinstall() -> None:
    context = build_context(prediction_payload("high"))
    assert context["status"] == "fresh"
    assert context["overall_risk_level"] == "high"
    assert context["selected_policy_action"] == DEFAULT_RISK_HIGH_ACTION


def test_enforcement_path_remains_onos_queue_app() -> None:
    manager = PolicyManager.__new__(PolicyManager)
    manager.current_manufacturing_context = {}
    manager.current_risk_context = build_context(prediction_payload("high"))
    manager.dry_run_only = False
    manager.run_id = "risk-inference-policy-context-test"
    manager.service_state = {"real_time_control": {}}

    record = PolicyManager.build_decision_record(
        manager,
        service_class="real_time_control",
        service_cfg={"policy_name": "real_time_control_guard"},
        telemetry_reference={},
        condition="risk_inference_high_real_time_control_queue_protection",
        metrics={},
        explanation="Predictive SLA Risk Inference Engine high-risk test.",
        recommended_action="INCREASE_CONTROL_PRIORITY",
        decision_state="new",
        is_new_decision=True,
        state_after_decision="INCREASE_CONTROL_PRIORITY",
        candidate_action="INCREASE_CONTROL_PRIORITY",
        cooldown_status=None,
    )
    assert record["enforcement_path"] == "ONOS_QUEUE_APP"
    assert record["applied"] is True
    assert record["risk_inference_enabled"] is True


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="risk-policy-context-") as tmp:
        tmp_dir = Path(tmp)
        test_missing_prediction_file_does_not_crash(tmp_dir)
        test_stale_prediction_file_does_not_crash(tmp_dir)
        test_low_risk_maps_to_maintain_or_verify()
        test_medium_risk_maps_to_verify()
        test_high_risk_maps_to_verify_or_reinstall()
        test_enforcement_path_remains_onos_queue_app()
    print("PASS risk inference policy context tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
