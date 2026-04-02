#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from policy_manager.config import load_policy_config
from policy_manager.decision_engine import DecisionEngine
from policy_manager.models import PolicyCycle
from policy_manager.onos_client import OnosClient
from policy_manager.ovs_client import OvsClient
from policy_manager.telemetry_reader import TelemetryReader
from policy_manager.utils import append_csv_row, append_jsonl, ensure_directory, profile_signature, utc_timestamp


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
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    config_path = Path(args.config).resolve()
    config = load_policy_config(config_path)
    dry_run_only = True if args.dry_run else False if args.active else bool(config["dry_run_only"])

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

    print(f"[policy] config={config_path}")
    print(f"[policy] mode={'dry-run' if dry_run_only else 'active'}")
    print(f"[policy] telemetry_dir={config['telemetry_dir']}")
    print(f"[policy] decisions_jsonl={jsonl_path}")
    print(f"[policy] decisions_csv={csv_path}")

    while True:
        snapshot = telemetry_reader.read_latest_snapshot()
        if snapshot is None:
            if args.once:
                print("[policy] no telemetry snapshot available yet.")
                return 1
            time.sleep(float(config["poll_interval_seconds"]))
            continue

        evaluation = decision_engine.evaluate(snapshot)
        decisions = list(evaluation["decisions"])
        target_profile = dict(evaluation["target_profile"])
        active_actions = list(evaluation["active_actions"])
        target_signature = profile_signature(target_profile)
        enforcement_result = {
            "applied": False,
            "mode": "dry-run" if dry_run_only else "active",
            "onos": {},
            "ovs": {},
            "reason": "dry_run_only is enabled." if dry_run_only else "profile unchanged or cooldown active.",
        }

        if not dry_run_only:
            cooldown_seconds = float(config["decision_cooldown_seconds"])
            if target_signature != last_applied_signature and (time.monotonic() - last_apply_monotonic) >= cooldown_seconds:
                onos_result = {}
                if bool(config["ensure_onos_slice_flows"]):
                    slice_flow_rules = [
                        {
                            "name": slice_name,
                            "udp_port": int(slice_cfg["udp_port"]),
                            "queue_id": int(slice_cfg["queue_id"]),
                            "priority": _default_flow_priority(slice_name),
                        }
                        for slice_name, slice_cfg in dict(config["slices"]).items()
                    ]
                    onos_result = onos_client.ensure_baseline_slice_flows(
                        devices_path=str(onos_cfg["devices_path"]),
                        upf_port_name=str(onos_cfg["upf_port_name"]),
                        edn_port_name=str(onos_cfg["edn_port_name"]),
                        slice_flow_rules=slice_flow_rules,
                        base_forward_flow_priority=5000,
                        reverse_flow_priority=20000,
                        arp_flow_priority=45000,
                        force_refresh=bool(config["force_onos_flow_refresh"]),
                    )
                ovs_result = ovs_client.apply_queue_profile(target_profile)
                applied = bool(ovs_result.get("ok")) and (not onos_result or bool(onos_result.get("ok")))
                enforcement_result = {
                    "applied": applied,
                    "mode": "active",
                    "onos": onos_result,
                    "ovs": ovs_result,
                    "reason": "applied target profile" if applied else "enforcement call failed",
                }
                if applied:
                    last_applied_signature = target_signature
                    last_apply_monotonic = time.monotonic()
            else:
                enforcement_result["reason"] = "profile unchanged or cooldown not elapsed."

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
        }
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
            f"applied={enforcement_result.get('applied')}"
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


def _flatten_reasons(decisions: List[Any]) -> List[str]:
    results: List[str] = []
    for decision in decisions:
        display = getattr(decision, "display_name", getattr(decision, "slice_name", "slice"))
        reasons = getattr(decision, "reasons", [])
        results.append(f"{display}: {', '.join(str(reason) for reason in reasons)}")
    return results


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
