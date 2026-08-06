#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path

TESTBED_DIR = Path(__file__).resolve().parents[1]
if str(TESTBED_DIR) not in sys.path:
    sys.path.insert(0, str(TESTBED_DIR))

from policy_manager.app import apply_risk_action_cooldown, build_risk_inference_context
from policy_manager.config import load_policy_config
from policy_manager.decision_engine import DecisionEngine
from policy_manager.ovs_client import OvsClient
from risk_inference.risk_score import recommended_policy_action


CONFIG_PATH = TESTBED_DIR / "policy_manager/config.yaml"


def utc_timestamp(age_seconds: float = 0.0) -> str:
    stamp = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_seconds)
    return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def prediction(level: str, *, age_seconds: float = 0.0, valid_for_policy: bool = True) -> dict:
    action = {
        "low": "MAINTAIN_CURRENT_POLICY",
        "medium": "VERIFY_QUEUE_RULES",
        "high": "VERIFY_OR_REINSTALL_QUEUE_RULES",
    }[level]
    return {
        "timestamp": utc_timestamp(age_seconds),
        "inference_status": "ok" if valid_for_policy else "stale_twin_state",
        "valid_for_policy": valid_for_policy,
        "overall_risk_score": {"low": 0.1, "medium": 0.5, "high": 0.9}[level],
        "overall_risk_level": level,
        "recommended_policy_action": action if valid_for_policy else "PRESERVE_EXISTING_POLICY_BEHAVIOR",
        "action_reason": f"{level}_test",
        "enforcement_churn_guard_applied": False,
    }


def write_prediction(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def config_for(path: Path, *, enabled: bool = True, max_age_seconds: float = 10.0) -> dict:
    return {
        "risk_inference_enabled": enabled,
        "risk_prediction_path": str(path),
        "risk_max_age_seconds": max_age_seconds,
        "risk_low_action": "MAINTAIN_CURRENT_POLICY",
        "risk_medium_action": "VERIFY_QUEUE_RULES",
        "risk_high_action": "VERIFY_OR_REINSTALL_QUEUE_RULES",
        "risk_action_cooldown_seconds": 10.0,
    }


def test_disabled_risk_inference_preserves_old_behavior(tmp_dir: Path) -> None:
    ctx = build_risk_inference_context(config_for(tmp_dir / "risk.json", enabled=False))
    assert ctx["enabled"] is False
    assert ctx["status"] == "disabled"
    assert ctx["fresh"] is False


def test_missing_prediction_file_does_not_crash(tmp_dir: Path) -> None:
    ctx = build_risk_inference_context(config_for(tmp_dir / "missing.json"))
    assert ctx["status"] == "missing"
    assert ctx["fresh"] is False


def test_stale_prediction_file_does_not_crash(tmp_dir: Path) -> None:
    path = tmp_dir / "stale.json"
    write_prediction(path, prediction("high", age_seconds=30.0))
    ctx = build_risk_inference_context(config_for(path, max_age_seconds=10.0))
    assert ctx["status"] == "stale_prediction_file"
    assert ctx["fresh"] is False


def test_valid_for_policy_false_preserves_existing_behavior(tmp_dir: Path) -> None:
    path = tmp_dir / "invalid.json"
    write_prediction(path, prediction("high", valid_for_policy=False))
    ctx = build_risk_inference_context(config_for(path))
    assert ctx["status"] == "stale_twin_state"
    assert ctx["valid_for_policy"] is False
    assert ctx["selected_policy_action"] == "PRESERVE_EXISTING_POLICY_BEHAVIOR"


def test_low_risk_maps_to_maintain_or_verify(tmp_dir: Path) -> None:
    path = tmp_dir / "low.json"
    write_prediction(path, prediction("low"))
    ctx = build_risk_inference_context(config_for(path))
    assert ctx["selected_policy_action"] in {"MAINTAIN_CURRENT_POLICY", "VERIFY_QUEUE_RULES"}


def test_medium_risk_maps_to_verify(tmp_dir: Path) -> None:
    path = tmp_dir / "medium.json"
    write_prediction(path, prediction("medium"))
    ctx = build_risk_inference_context(config_for(path))
    assert ctx["selected_policy_action"] == "VERIFY_QUEUE_RULES"


def test_high_risk_maps_to_verify_or_reinstall(tmp_dir: Path) -> None:
    path = tmp_dir / "high.json"
    write_prediction(path, prediction("high"))
    ctx = build_risk_inference_context(config_for(path))
    assert ctx["selected_policy_action"] == "VERIFY_OR_REINSTALL_QUEUE_RULES"


def test_medium_risk_all_present_no_drift_maps_to_monitoring() -> None:
    action = recommended_policy_action(
        "medium",
        valid_for_policy=True,
        queue_rule_presence="all_present",
        policy_drift_detected=False,
        controller_available=True,
    )
    assert action == "MAINTAIN_CURRENT_POLICY_WITH_MONITORING"


def test_medium_risk_missing_rules_maps_to_verify() -> None:
    action = recommended_policy_action(
        "medium",
        valid_for_policy=True,
        queue_rule_presence="missing",
        policy_drift_detected=False,
        controller_available=True,
    )
    assert action == "VERIFY_QUEUE_RULES"


def test_high_risk_missing_rules_maps_to_reinstall() -> None:
    action = recommended_policy_action(
        "high",
        valid_for_policy=True,
        queue_rule_presence="missing",
        policy_drift_detected=False,
        controller_available=True,
    )
    assert action == "VERIFY_OR_REINSTALL_QUEUE_RULES"


def test_cooldown_prevents_repeated_verify_queue_rules(tmp_dir: Path) -> None:
    path = tmp_dir / "medium.json"
    payload = prediction("medium")
    payload["recommended_policy_action"] = "VERIFY_QUEUE_RULES"
    write_prediction(path, payload)
    ctx = build_risk_inference_context(config_for(path))
    guarded = apply_risk_action_cooldown(
        ctx,
        last_action="VERIFY_QUEUE_RULES",
        last_action_monotonic=100.0,
        now_monotonic=105.0,
        cooldown_seconds=10.0,
    )
    assert guarded["selected_policy_action"] == "MAINTAIN_CURRENT_POLICY_WITH_MONITORING"
    assert guarded["enforcement_churn_guard_applied"] is True
    assert guarded["action_reason"] == "recent_verification_cooldown"


def test_ccnc_disable_manufacturing_twin_runtime_config() -> None:
    previous = os.environ.get("CCNC_DISABLE_MANUFACTURING_TWIN")
    previous_enabled = os.environ.get("MANUFACTURING_TWIN_ENABLED")
    try:
        os.environ["MANUFACTURING_TWIN_ENABLED"] = "true"
        os.environ["CCNC_DISABLE_MANUFACTURING_TWIN"] = "true"
        cfg = load_policy_config(CONFIG_PATH)
        assert cfg["manufacturing_twin_enabled"] is False
    finally:
        _restore_env("CCNC_DISABLE_MANUFACTURING_TWIN", previous)
        _restore_env("MANUFACTURING_TWIN_ENABLED", previous_enabled)


def test_decision_engine_keeps_slice_decisions_and_adds_selected_action(tmp_dir: Path) -> None:
    path = tmp_dir / "high.json"
    write_prediction(path, prediction("high"))
    cfg = load_policy_config(CONFIG_PATH)
    risk_context = build_risk_inference_context(config_for(path))
    evaluation = DecisionEngine(cfg).evaluate({"slice_metrics": {}}, risk_context=risk_context)
    assert "VERIFY_OR_REINSTALL_QUEUE_RULES" in evaluation["active_actions"]
    assert {decision.slice_name for decision in evaluation["decisions"]} == {"urllc", "embb", "mmtc"}
    assert all(decision.selected_policy_action == "VERIFY_OR_REINSTALL_QUEUE_RULES" for decision in evaluation["decisions"])


def test_enforcement_path_remains_onos_queue_app() -> None:
    result = OvsClient(
        container_name="ovs",
        bridge_name="br-n6",
        egress_port_name="v-edn-host",
        dry_run_only=True,
    ).install_or_verify_onos_queue_rules()
    assert result["enforcement_path"] == "ONOS_QUEUE_APP"
    assert result["action"] == "INSTALL_OR_VERIFY_QUEUE_RULES"


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="policy-risk-context-") as tmp:
        tmp_dir = Path(tmp)
        test_disabled_risk_inference_preserves_old_behavior(tmp_dir)
        test_missing_prediction_file_does_not_crash(tmp_dir)
        test_stale_prediction_file_does_not_crash(tmp_dir)
        test_valid_for_policy_false_preserves_existing_behavior(tmp_dir)
        test_low_risk_maps_to_maintain_or_verify(tmp_dir)
        test_medium_risk_maps_to_verify(tmp_dir)
        test_high_risk_maps_to_verify_or_reinstall(tmp_dir)
        test_medium_risk_all_present_no_drift_maps_to_monitoring()
        test_medium_risk_missing_rules_maps_to_verify()
        test_high_risk_missing_rules_maps_to_reinstall()
        test_cooldown_prevents_repeated_verify_queue_rules(tmp_dir)
        test_ccnc_disable_manufacturing_twin_runtime_config()
        test_decision_engine_keeps_slice_decisions_and_adds_selected_action(tmp_dir)
        test_enforcement_path_remains_onos_queue_app()
    print("PASS package deterministic risk policy context tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
