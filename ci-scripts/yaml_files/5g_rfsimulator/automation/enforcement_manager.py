#!/usr/bin/env python3
"""Live enforcement manager for applying policy-manager decisions to ONOS and OVS."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

from onos_client import OnosClient
from service_mapping_utils import apply_service_mapping


DEFAULT_CONFIG_NAME = "enforcement_config.yaml"
DEFAULT_POLICY_LOG_DIR = "../logs/policy"
DEFAULT_POLICY_LOG_GLOB = "policy_decisions_*.jsonl"
DEFAULT_ENFORCEMENT_LOG_DIR = "../logs/enforcement"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_COOLDOWN_SECONDS = 10
DEFAULT_ACTION_MAINTAIN = "MAINTAIN_CURRENT_POLICY"
DEFAULT_ACTION_RESTORE = "RESTORE_DEFAULT_POLICY"


def utc_timestamp() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def print_console(level: str, message: str) -> None:
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[enforcement][{stamp}][{level}] {message}", flush=True)


def text_excerpt(value: Any, limit: int = 240) -> str:
    return str(value or "").strip()[:limit]


def normalize_path(base_dir: Path, value: str) -> str:
    return str((base_dir / value).resolve())


def parse_timestamp(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def load_config(config_path: Path) -> Dict[str, Any]:
    raw_text = config_path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {config_path} must define a top-level mapping/object.")
    return loaded


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def split_top_level(text: str, delimiter: str = ",") -> List[str]:
    items: List[str] = []
    depth = 0
    current: List[str] = []
    quote: Optional[str] = None
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            quote = char
            current.append(char)
            continue
        if char in "{[(":
            depth += 1
        elif char in "}])" and depth > 0:
            depth -= 1
        if char == delimiter and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def parse_ovs_map(text: str) -> Dict[str, str]:
    stripped = text.strip()
    if stripped in {"", "[]", "{}"}:
        return {}
    if stripped.startswith("{") and stripped.endswith("}"):
        stripped = stripped[1:-1].strip()
    result: Dict[str, str] = {}
    for item in split_top_level(stripped):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        clean_key = key.strip().strip('"')
        clean_value = value.strip().strip('"')
        result[clean_key] = clean_value
    return result


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True)


def profile_signature(profile: Dict[str, Any]) -> str:
    return canonical_json(profile)


def normalize_queue_profile(raw_profile: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_profile, dict):
        raise ValueError("Queue profile must be a mapping/object.")
    parent_max = to_int(raw_profile.get("parent_max_rate_bps"))
    queues = raw_profile.get("queues", {})
    if not isinstance(queues, dict):
        raise ValueError("Queue profile 'queues' must be a mapping/object.")
    normalized_queues: Dict[str, Dict[str, int]] = {}
    for queue_id, queue_cfg in queues.items():
        if not isinstance(queue_cfg, dict):
            raise ValueError(f"Queue profile entry for queue {queue_id} must be a mapping/object.")
        min_rate = to_int(queue_cfg.get("min_rate_bps"))
        max_rate = to_int(queue_cfg.get("max_rate_bps"))
        if min_rate is None or max_rate is None:
            raise ValueError(f"Queue profile for queue {queue_id} requires min_rate_bps and max_rate_bps.")
        normalized_queues[str(queue_id)] = {
            "min_rate_bps": min_rate,
            "max_rate_bps": max_rate,
        }
    if parent_max is None:
        raise ValueError("Queue profile requires parent_max_rate_bps.")
    return {
        "parent_max_rate_bps": parent_max,
        "queues": normalized_queues,
    }


def normalize_queue_overlay(raw_overlay: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_overlay, dict):
        raise ValueError("Queue overlay must be a mapping/object.")
    normalized: Dict[str, Any] = {}
    if "parent_max_rate_bps" in raw_overlay and raw_overlay["parent_max_rate_bps"] is not None:
        parent_rate = to_int(raw_overlay["parent_max_rate_bps"])
        if parent_rate is None:
            raise ValueError("Queue overlay parent_max_rate_bps must be numeric.")
        normalized["parent_max_rate_bps"] = parent_rate

    queues = raw_overlay.get("queues", {})
    if queues is not None and not isinstance(queues, dict):
        raise ValueError("Queue overlay 'queues' must be a mapping/object.")
    normalized_queues: Dict[str, Dict[str, int]] = {}
    for queue_id, queue_cfg in dict(queues or {}).items():
        if not isinstance(queue_cfg, dict):
            raise ValueError(f"Queue overlay entry for queue {queue_id} must be a mapping/object.")
        entry: Dict[str, int] = {}
        if "min_rate_bps" in queue_cfg and queue_cfg["min_rate_bps"] is not None:
            min_rate = to_int(queue_cfg["min_rate_bps"])
            if min_rate is None:
                raise ValueError(f"Queue overlay min_rate_bps for queue {queue_id} must be numeric.")
            entry["min_rate_bps"] = min_rate
        if "max_rate_bps" in queue_cfg and queue_cfg["max_rate_bps"] is not None:
            max_rate = to_int(queue_cfg["max_rate_bps"])
            if max_rate is None:
                raise ValueError(f"Queue overlay max_rate_bps for queue {queue_id} must be numeric.")
            entry["max_rate_bps"] = max_rate
        if entry:
            normalized_queues[str(queue_id)] = entry
    normalized["queues"] = normalized_queues
    return normalized


def merge_queue_profile(base_profile: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base_profile)
    if "parent_max_rate_bps" in overlay and overlay["parent_max_rate_bps"] is not None:
        merged["parent_max_rate_bps"] = to_int(overlay["parent_max_rate_bps"])
    overlay_queues = overlay.get("queues", {})
    if isinstance(overlay_queues, dict):
        for queue_id, queue_cfg in overlay_queues.items():
            merged["queues"].setdefault(str(queue_id), {})
            if not isinstance(queue_cfg, dict):
                continue
            for key in ("min_rate_bps", "max_rate_bps"):
                if key in queue_cfg and queue_cfg[key] is not None:
                    merged["queues"][str(queue_id)][key] = to_int(queue_cfg[key])
    return normalize_queue_profile(merged)


def normalize_config(config_path: Path, raw_config: Dict[str, Any]) -> Dict[str, Any]:
    base_dir = config_path.parent
    config = apply_service_mapping(base_dir, dict(raw_config))
    enforcement = dict(config.get("enforcement", {}))
    env_onos_host = os.getenv("ONOS_HOST", "").strip() or os.getenv("ONOS_REST_HOST", "").strip()
    env_onos_rest_port = os.getenv("ONOS_REST_PORT", "").strip() or "8181"
    default_onos_base_url = (
        f"http://{env_onos_host}:{env_onos_rest_port}" if env_onos_host else "http://192.168.71.160:8181"
    )
    env_onos_base_url = os.getenv("ONOS_BASE_URL", "").strip()
    env_onos_username = os.getenv("ONOS_USERNAME", "").strip()
    env_onos_password = os.getenv("ONOS_PASSWORD", "").strip()
    env_ovs_container_name = os.getenv("OVS_CONTAINER_NAME", "").strip()
    env_ovs_bridge_name = os.getenv("OVS_BRIDGE_NAME", "").strip()

    enforcement.setdefault("policy_log_dir", DEFAULT_POLICY_LOG_DIR)
    enforcement.setdefault("policy_log_glob", DEFAULT_POLICY_LOG_GLOB)
    enforcement.setdefault("log_dir", DEFAULT_ENFORCEMENT_LOG_DIR)
    enforcement.setdefault("polling_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
    enforcement.setdefault("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS)
    enforcement.setdefault("dry_run", True)
    enforcement.setdefault("bootstrap_from_existing_policy_logs", True)
    enforcement.setdefault("ensure_onos_slice_flows", True)
    enforcement.setdefault("force_onos_flow_refresh", False)
    enforcement.setdefault("read_current_ovs_profile_on_startup", True)

    onos_cfg = dict(enforcement.get("onos", {}))
    onos_cfg.setdefault("base_url", config.get("onos_base_url", default_onos_base_url))
    onos_cfg.setdefault("devices_path", "/onos/v1/devices")
    onos_cfg.setdefault("username", config.get("onos_username", "onos"))
    onos_cfg.setdefault("password", config.get("onos_password", "rocks"))
    onos_cfg.setdefault("timeout_seconds", 5)
    onos_cfg.setdefault("upf_port_name", "v-upf-host")
    onos_cfg.setdefault("edn_port_name", "v-edn-host")
    if env_onos_base_url:
        onos_cfg["base_url"] = env_onos_base_url
    if env_onos_username:
        onos_cfg["username"] = env_onos_username
    if env_onos_password:
        onos_cfg["password"] = env_onos_password
    onos_cfg.setdefault(
        "slice_flow_rules",
        [
            {"name": "embb", "udp_port": 5201, "queue_id": 1, "priority": 40000},
            {"name": "urllc", "udp_port": 5202, "queue_id": 2, "priority": 50000},
            {"name": "mmtc", "udp_port": 5203, "queue_id": 3, "priority": 30000},
        ],
    )
    onos_cfg.setdefault("base_forward_flow_priority", 5000)
    onos_cfg.setdefault("reverse_flow_priority", 20000)
    onos_cfg.setdefault("arp_flow_priority", 45000)
    enforcement["onos"] = onos_cfg

    ovs_cfg = dict(enforcement.get("ovs", {}))
    ovs_cfg.setdefault("container_name", config.get("ovs_container_name", "ovs"))
    ovs_cfg.setdefault("bridge_name", config.get("ovs_bridge_name", "br-n6"))
    ovs_cfg.setdefault("egress_port_name", "v-edn-host")
    if env_ovs_container_name:
        ovs_cfg["container_name"] = env_ovs_container_name
    if env_ovs_bridge_name:
        ovs_cfg["bridge_name"] = env_ovs_bridge_name
    enforcement["ovs"] = ovs_cfg

    queue_profiles = dict(enforcement.get("queue_profiles", {}))
    queue_profiles.setdefault(
        "baseline",
        {
            "parent_max_rate_bps": 120000000,
            "queues": {
                "1": {"min_rate_bps": 50000000, "max_rate_bps": 100000000},
                "2": {"min_rate_bps": 10000000, "max_rate_bps": 20000000},
                "3": {"min_rate_bps": 1000000, "max_rate_bps": 5000000},
            },
        },
    )
    queue_profiles.setdefault(
        "action_overlays",
        {
            "INCREASE_CONTROL_PRIORITY": {
                "queues": {
                    "2": {"min_rate_bps": 20000000, "max_rate_bps": 40000000}
                }
            },
            "TIGHTEN_DATA_SHAPING": {
                "queues": {
                    "1": {"min_rate_bps": 30000000, "max_rate_bps": 60000000}
                }
            },
            "PROTECT_SENSOR_MIN_BW": {
                "queues": {
                    "3": {"min_rate_bps": 5000000, "max_rate_bps": 10000000}
                }
            },
        },
    )
    enforcement["queue_profiles"] = queue_profiles

    enforcement["policy_log_dir"] = normalize_path(base_dir, str(enforcement["policy_log_dir"]))
    enforcement["log_dir"] = normalize_path(base_dir, str(enforcement["log_dir"]))
    enforcement["polling_interval_seconds"] = float(enforcement["polling_interval_seconds"])
    enforcement["cooldown_seconds"] = float(enforcement["cooldown_seconds"])
    enforcement["dry_run"] = bool(enforcement["dry_run"])
    enforcement["bootstrap_from_existing_policy_logs"] = bool(
        enforcement["bootstrap_from_existing_policy_logs"]
    )
    enforcement["ensure_onos_slice_flows"] = bool(enforcement["ensure_onos_slice_flows"])
    enforcement["force_onos_flow_refresh"] = bool(enforcement["force_onos_flow_refresh"])
    enforcement["read_current_ovs_profile_on_startup"] = bool(
        enforcement["read_current_ovs_profile_on_startup"]
    )

    onos_cfg["timeout_seconds"] = float(onos_cfg["timeout_seconds"])
    onos_cfg["base_forward_flow_priority"] = int(onos_cfg["base_forward_flow_priority"])
    onos_cfg["reverse_flow_priority"] = int(onos_cfg["reverse_flow_priority"])
    onos_cfg["arp_flow_priority"] = int(onos_cfg["arp_flow_priority"])
    onos_cfg["slice_flow_rules"] = [
        {
            "name": str(rule.get("name", f"rule-{index}")),
            "udp_port": int(rule["udp_port"]),
            "queue_id": int(rule["queue_id"]),
            "priority": int(rule["priority"]),
        }
        for index, rule in enumerate(ensure_list(onos_cfg.get("slice_flow_rules")))
        if isinstance(rule, dict)
    ]

    queue_profiles["baseline"] = normalize_queue_profile(queue_profiles["baseline"])
    normalized_overlays: Dict[str, Dict[str, Any]] = {}
    for action_name, overlay in dict(queue_profiles["action_overlays"]).items():
        normalized_overlays[str(action_name)] = normalize_queue_overlay(overlay)
    queue_profiles["action_overlays"] = normalized_overlays

    config["enforcement"] = enforcement
    return config


class EnforcementManager:
    def __init__(
        self,
        config_path: Path,
        config: Dict[str, Any],
        dry_run_override: Optional[bool],
        run_once: bool = False,
        restore_default: bool = False,
    ) -> None:
        self.config_path = config_path.resolve()
        self.config = config
        self.enforcement_cfg = dict(config["enforcement"])
        self.run_once = run_once
        self.stop_requested = False
        self.received_signal_name: Optional[str] = None
        self.warned_keys: set[str] = set()
        self.policy_log_dir = Path(str(self.enforcement_cfg["policy_log_dir"]))
        self.policy_log_glob = str(self.enforcement_cfg["policy_log_glob"])
        self.log_dir = Path(str(self.enforcement_cfg["log_dir"]))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.polling_interval_seconds = float(self.enforcement_cfg["polling_interval_seconds"])
        self.cooldown_seconds = float(self.enforcement_cfg["cooldown_seconds"])
        self.dry_run = (
            bool(dry_run_override)
            if dry_run_override is not None
            else bool(self.enforcement_cfg["dry_run"])
        )
        self.default_action = str(self.config.get("default_policy_action", DEFAULT_ACTION_MAINTAIN))
        self.restore_action = str(self.config.get("restore_policy_action", DEFAULT_ACTION_RESTORE))
        self.service_classes = sorted(dict(self.config.get("service_classes", {})).keys())
        self.service_actions: Dict[str, str] = {
            service_class: self.default_action for service_class in self.service_classes
        }
        self.file_state: Dict[str, Dict[str, int]] = {}
        self.pending_profile: Optional[Dict[str, Any]] = None
        self.pending_signature: Optional[str] = None
        self.pending_context: Optional[Dict[str, Any]] = None
        self.pending_since_monotonic: Optional[float] = None
        self.pending_hold_logged = False
        self.last_apply_monotonic: Optional[float] = None
        self.last_processed_policy_reference: Optional[Dict[str, Any]] = None

        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = f"enforcement-{stamp}-{os.getpid()}"
        self.log_path = self.log_dir / f"enforcement_{stamp}.jsonl"
        self.log_handle = self.log_path.open("a", encoding="utf-8")

        queue_profiles = dict(self.enforcement_cfg["queue_profiles"])
        self.baseline_profile = normalize_queue_profile(queue_profiles["baseline"])
        self.action_overlays = dict(queue_profiles["action_overlays"])
        self.current_profile = copy.deepcopy(self.baseline_profile)
        self.current_signature = profile_signature(self.current_profile)

        self.ovs_cfg = dict(self.enforcement_cfg["ovs"])
        self.onos_cfg = dict(self.enforcement_cfg["onos"])
        self.onos_client = OnosClient(
            base_url=str(self.onos_cfg["base_url"]),
            username=str(self.onos_cfg["username"]),
            password=str(self.onos_cfg["password"]),
            timeout_seconds=float(self.onos_cfg["timeout_seconds"]),
            dry_run=self.dry_run,
        )
        self.restore_default = restore_default

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        if not self.stop_requested:
            self.received_signal_name = signal.Signals(signum).name
        self.stop_requested = True

    def warn_once(self, key: str, message: str) -> None:
        if key in self.warned_keys:
            return
        self.warned_keys.add(key)
        print_console("WARN", message)

    def write_jsonl(self, payload: Dict[str, Any]) -> None:
        self.log_handle.write(json.dumps(payload, sort_keys=True) + "\n")
        self.log_handle.flush()

    def mode_label(self) -> str:
        return "dry-run" if self.dry_run else "live"

    def reset_service_actions_to_default(self) -> None:
        for service_class in self.service_classes:
            self.service_actions[service_class] = self.default_action

    def build_manual_restore_context(self) -> Dict[str, Any]:
        return {
            "source": "manual_rollback",
            "decision_count": 0,
            "policy_references": [],
            "manual_action": self.restore_action,
            "target_service_classes": list(self.service_classes),
            "explanation": "Manual rollback requested; restoring the baseline queue profile.",
        }

    def run(self) -> int:
        self.install_signal_handlers()
        print_console("INFO", f"Using config: {self.config_path}")
        print_console("INFO", f"Watching policy directory: {self.policy_log_dir}")
        print_console(
            "INFO",
            f"Writing enforcement logs to: {self.log_path} (dry_run={self.dry_run})",
        )

        self.write_jsonl(
            {
                "event_type": "enforcement_manager_start",
                "timestamp": utc_timestamp(),
                "run_id": self.run_id,
                "config_path": str(self.config_path),
                "log_path": str(self.log_path),
                "dry_run": self.dry_run,
                "mode": self.mode_label(),
                "bridge_name": self.ovs_cfg["bridge_name"],
                "egress_port_name": self.ovs_cfg["egress_port_name"],
                "no_oai_core_changes": True,
                "external_enforcement_only": True,
            }
        )

        if self.enforcement_cfg["read_current_ovs_profile_on_startup"] and not self.dry_run:
            current_profile_result = self.read_current_queue_profile()
            if current_profile_result["ok"] and isinstance(current_profile_result.get("profile"), dict):
                self.current_profile = current_profile_result["profile"]
                self.current_signature = profile_signature(self.current_profile)
            else:
                self.warn_once(
                    "ovs-current-profile-fallback",
                    "Could not read the current OVS queue profile on startup; assuming the default baseline profile.",
                )

        try:
            if self.restore_default:
                self.reset_service_actions_to_default()
                self.schedule_reconcile(self.build_manual_restore_context())
                self.last_apply_monotonic = None
                self.reconcile_or_hold()
            elif self.enforcement_cfg["bootstrap_from_existing_policy_logs"]:
                bootstrap_result = self.bootstrap_service_state()
                if bootstrap_result["context"] is not None:
                    self.schedule_reconcile(bootstrap_result["context"])
                    self.reconcile_or_hold()

            while not self.stop_requested:
                new_decisions = self.read_new_policy_decisions()
                if new_decisions:
                    batch_context = self.apply_policy_decision_batch(new_decisions)
                    if batch_context is not None:
                        self.schedule_reconcile(batch_context)
                        self.reconcile_or_hold()
                else:
                    self.reconcile_or_hold()

                if self.run_once:
                    break
                self.sleep_with_stop(self.polling_interval_seconds)
        except KeyboardInterrupt:
            print_console("INFO", "Interrupted by Ctrl+C.")
            exit_code = 0
        except Exception as exc:  # pragma: no cover - defensive
            exit_code = 1
            trace = traceback.format_exc()
            print_console("ERROR", f"Enforcement manager hit an unexpected error: {exc}")
            self.write_jsonl(
                {
                    "event_type": "enforcement_manager_error",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "error": str(exc),
                    "traceback": trace,
                    "dry_run": self.dry_run,
                    "mode": self.mode_label(),
                }
            )
        else:
            exit_code = 0
        finally:
            if self.received_signal_name:
                print_console(
                    "INFO",
                    f"Received {self.received_signal_name}; stopping after the current cycle.",
                )
            self.write_jsonl(
                {
                    "event_type": "enforcement_manager_stop",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "last_processed_policy_reference": self.last_processed_policy_reference,
                    "stopped_cleanly": exit_code == 0,
                    "dry_run": self.dry_run,
                    "mode": self.mode_label(),
                }
            )
            self.log_handle.close()
            print_console("INFO", f"Enforcement manager stopped. Final log file: {self.log_path}")
        return exit_code

    def sleep_with_stop(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.stop_requested:
            chunk = min(0.25, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def discover_policy_files(self) -> List[Path]:
        if not self.policy_log_dir.exists():
            self.warn_once(
                "missing-policy-dir",
                f"Policy log directory does not exist yet: {self.policy_log_dir}",
            )
            return []
        return sorted(self.policy_log_dir.glob(self.policy_log_glob))

    def bootstrap_service_state(self) -> Dict[str, Any]:
        latest_by_service: Dict[str, Tuple[Tuple[str, str, int], Dict[str, Any], Dict[str, Any]]] = {}
        for path in self.discover_policy_files():
            resolved = str(path.resolve())
            line_number = 0
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                while True:
                    line = handle.readline()
                    if not line:
                        break
                    line_number += 1
                    payload = self.parse_policy_line(line, resolved, line_number)
                    if payload is None:
                        continue
                    decision, reference = payload
                    service_class = decision.get("service_class")
                    if not service_class:
                        continue
                    ordering = (
                        str(decision.get("timestamp") or ""),
                        resolved,
                        line_number,
                    )
                    latest_by_service[str(service_class)] = (ordering, decision, reference)
                self.file_state[resolved] = {"offset": handle.tell(), "line_number": line_number}

        if not latest_by_service:
            return {"context": None}

        applied_refs: List[Dict[str, Any]] = []
        changed_services: List[str] = []
        for service_class in sorted(latest_by_service):
            _, decision, reference = latest_by_service[service_class]
            if self.update_service_state_from_decision(decision):
                applied_refs.append(reference)
                changed_services.append(service_class)
            self.last_processed_policy_reference = reference

        return {
            "context": {
                "source": "bootstrap",
                "decision_count": len(applied_refs),
                "policy_references": applied_refs,
                "target_service_classes": sorted(set(changed_services)),
            }
        }

    def parse_policy_line(
        self, line: str, resolved_path: str, line_number: int
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        stripped = line.strip()
        if not stripped:
            return None
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            self.warn_once(
                f"policy-json-error:{resolved_path}:{line_number}",
                f"Skipping malformed policy JSON at {resolved_path}:{line_number}: {exc}",
            )
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("event_type") != "policy_decision":
            return None
        reference = {
            "policy_file": resolved_path,
            "policy_line_number": line_number,
            "policy_timestamp": payload.get("timestamp"),
            "policy_run_id": payload.get("run_id"),
            "service_class": payload.get("service_class"),
            "recommended_action": payload.get("recommended_action"),
            "state_after_decision": payload.get("state_after_decision"),
            "decision_state": payload.get("decision_state"),
            "detected_condition": payload.get("detected_condition"),
            "latest_telemetry_snapshot_reference": payload.get("latest_telemetry_snapshot_reference"),
        }
        return payload, reference

    def read_new_policy_decisions(self) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
        collected: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for path in self.discover_policy_files():
            resolved = str(path.resolve())
            state = self.file_state.setdefault(resolved, {"offset": 0, "line_number": 0})
            try:
                file_size = path.stat().st_size
            except FileNotFoundError:
                continue
            if file_size < state["offset"]:
                state["offset"] = 0
                state["line_number"] = 0

            with path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(state["offset"])
                while True:
                    line = handle.readline()
                    if not line:
                        state["offset"] = handle.tell()
                        break
                    state["line_number"] += 1
                    payload = self.parse_policy_line(line, resolved, state["line_number"])
                    state["offset"] = handle.tell()
                    if payload is None:
                        continue
                    collected.append(payload)

        collected.sort(
            key=lambda item: (
                str(item[0].get("timestamp") or ""),
                str(item[1].get("policy_file") or ""),
                int(item[1].get("policy_line_number") or 0),
            )
        )
        return collected

    def apply_policy_decision_batch(
        self, decisions: List[Tuple[Dict[str, Any], Dict[str, Any]]]
    ) -> Optional[Dict[str, Any]]:
        changed_refs: List[Dict[str, Any]] = []
        changed_services: List[str] = []
        for decision, reference in decisions:
            if self.update_service_state_from_decision(decision):
                changed_refs.append(reference)
                if reference.get("service_class"):
                    changed_services.append(str(reference["service_class"]))
            self.last_processed_policy_reference = reference
        if not changed_refs:
            return None
        return {
            "source": "policy_decision_batch",
            "decision_count": len(changed_refs),
            "policy_references": changed_refs,
            "target_service_classes": sorted(set(changed_services)),
        }

    def update_service_state_from_decision(self, decision: Dict[str, Any]) -> bool:
        service_class = str(decision.get("service_class") or "")
        if not service_class:
            return False
        desired_state = str(
            decision.get("state_after_decision")
            or decision.get("recommended_action")
            or self.default_action
        )
        if desired_state == self.restore_action:
            desired_state = self.default_action
        previous_state = self.service_actions.get(service_class, self.default_action)
        self.service_actions[service_class] = desired_state
        return previous_state != desired_state

    def schedule_reconcile(self, context: Dict[str, Any]) -> None:
        profile, active_actions = self.compose_target_profile()
        signature = profile_signature(profile)
        if signature == self.current_signature and self.pending_signature is None:
            self.log_enforcement_record(
                status="noop",
                context=context,
                previous_profile=self.current_profile,
                target_profile=profile,
                active_actions=active_actions,
                flow_result=None,
                ovs_result=None,
                explanation="The computed live queue profile already matches the current effective profile.",
                cooldown_status=None,
            )
            return
        self.pending_profile = profile
        self.pending_signature = signature
        self.pending_context = context
        self.pending_since_monotonic = time.monotonic()
        self.pending_hold_logged = False

    def compose_target_profile(self) -> Tuple[Dict[str, Any], Dict[str, str]]:
        profile = copy.deepcopy(self.baseline_profile)
        active_actions: Dict[str, str] = {}
        for service_class in sorted(self.service_actions):
            action = self.service_actions.get(service_class, self.default_action)
            overlay = self.action_overlays.get(action)
            if overlay:
                profile = merge_queue_profile(profile, overlay)
                active_actions[service_class] = action
        return profile, active_actions

    def reconcile_or_hold(self) -> None:
        if self.pending_profile is None or self.pending_signature is None:
            return
        if self.pending_signature == self.current_signature:
            self.pending_profile = None
            self.pending_signature = None
            self.pending_context = None
            self.pending_since_monotonic = None
            self.pending_hold_logged = False
            return

        now_monotonic = time.monotonic()
        elapsed_since_apply = (
            None if self.last_apply_monotonic is None else now_monotonic - self.last_apply_monotonic
        )
        if elapsed_since_apply is not None and elapsed_since_apply < self.cooldown_seconds:
            remaining = self.cooldown_seconds - elapsed_since_apply
            if not self.pending_hold_logged:
                self.pending_hold_logged = True
                self.log_enforcement_record(
                    status="cooldown_hold",
                    context=self.pending_context,
                    previous_profile=self.current_profile,
                    target_profile=self.pending_profile,
                    active_actions=self.compose_target_profile()[1],
                    flow_result=None,
                    ovs_result=None,
                    explanation=(
                        f"Deferring live enforcement update until the {self.cooldown_seconds:.1f}s "
                        f"cooldown expires."
                    ),
                    cooldown_status={
                        "cooldown_seconds": self.cooldown_seconds,
                        "remaining_seconds": remaining,
                    },
                )
            return

        self.apply_pending_profile()

    def apply_pending_profile(self) -> None:
        if self.pending_profile is None or self.pending_signature is None:
            return

        active_actions = self.compose_target_profile()[1]
        flow_result = None
        if self.enforcement_cfg["ensure_onos_slice_flows"]:
            flow_result = self.onos_client.ensure_slice_queue_flows(
                devices_path=str(self.onos_cfg["devices_path"]),
                upf_port_name=str(self.onos_cfg["upf_port_name"]),
                edn_port_name=str(self.onos_cfg["edn_port_name"]),
                flow_rules=list(self.onos_cfg["slice_flow_rules"]),
                base_forward_flow_priority=int(self.onos_cfg["base_forward_flow_priority"]),
                reverse_flow_priority=int(self.onos_cfg["reverse_flow_priority"]),
                arp_flow_priority=int(self.onos_cfg["arp_flow_priority"]),
                force_refresh=bool(self.enforcement_cfg["force_onos_flow_refresh"]),
            )

        ovs_result = self.apply_queue_profile(self.pending_profile)

        status = "applied"
        explanation = "Applied the composed queue profile driven by the latest policy-manager decisions."
        if self.dry_run:
            status = "dry_run"
            explanation = (
                "Dry-run mode is active, so the enforcement manager only planned the queue and ONOS actions."
            )
        elif not ovs_result["ok"]:
            status = "failed"
            explanation = "The OVS queue update failed, so the live queue profile was not changed."
        elif flow_result is not None and not flow_result.get("ok", False):
            status = "applied_with_warnings"
            explanation = (
                "The OVS queue profile was updated, but ONOS flow verification or refresh reported a warning."
            )

        if ovs_result["ok"]:
            self.current_profile = copy.deepcopy(self.pending_profile)
            self.current_signature = self.pending_signature
            self.last_apply_monotonic = time.monotonic()

        self.log_enforcement_record(
            status=status,
            context=self.pending_context,
            previous_profile=self.current_profile if not ovs_result["ok"] else ovs_result.get("previous_profile"),
            target_profile=self.pending_profile,
            active_actions=active_actions,
            flow_result=flow_result,
            ovs_result=ovs_result,
            explanation=explanation,
            cooldown_status=None,
        )

        if ovs_result["ok"]:
            print_console(
                "INFO",
                f"Applied enforcement profile (dry_run={self.dry_run}) with active actions: "
                f"{active_actions or {'baseline': self.default_action}}",
            )
            self.pending_profile = None
            self.pending_signature = None
            self.pending_context = None
            self.pending_since_monotonic = None
            self.pending_hold_logged = False

    def collect_target_service_classes(
        self, context: Optional[Dict[str, Any]], active_actions: Dict[str, str]
    ) -> List[str]:
        targets: List[str] = []
        if isinstance(context, dict):
            for key in ("target_service_classes",):
                for value in ensure_list(context.get(key)):
                    if value:
                        targets.append(str(value))
            for reference in ensure_list(context.get("policy_references")):
                if isinstance(reference, dict) and reference.get("service_class"):
                    targets.append(str(reference["service_class"]))
        targets.extend(sorted(active_actions))
        if not targets:
            targets = list(self.service_classes)
        return sorted(set(targets))

    def collect_action_names(
        self, context: Optional[Dict[str, Any]], active_actions: Dict[str, str]
    ) -> List[str]:
        actions: List[str] = []
        if isinstance(context, dict):
            if context.get("manual_action"):
                actions.append(str(context["manual_action"]))
            for reference in ensure_list(context.get("policy_references")):
                if not isinstance(reference, dict):
                    continue
                decision_action = (
                    reference.get("state_after_decision")
                    or reference.get("recommended_action")
                    or None
                )
                if decision_action:
                    actions.append(str(decision_action))
        if not actions:
            actions.extend(str(action) for action in active_actions.values())
        if not actions:
            actions.append(self.default_action)
        return sorted(set(actions))

    def collect_operations(
        self,
        flow_result: Optional[Dict[str, Any]],
        ovs_result: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        operations: List[Dict[str, Any]] = []
        mode = self.mode_label()

        if isinstance(ovs_result, dict):
            for command in ensure_list(ovs_result.get("planned_commands")):
                if command:
                    operations.append(
                        {
                            "type": "ovs-vsctl",
                            "mode": mode,
                            "planned": True,
                            "command": str(command),
                        }
                    )
            result_entries = []
            if isinstance(ovs_result.get("result"), dict):
                result_entries.append(ovs_result["result"])
            for item in ensure_list(ovs_result.get("results")):
                if isinstance(item, dict):
                    result_entries.append(item)
            for result in result_entries:
                operations.append(
                    {
                        "type": "ovs-vsctl",
                        "mode": mode,
                        "planned": False,
                        "command": " ".join(str(part) for part in ensure_list(result.get("command"))),
                        "return_code": result.get("return_code"),
                        "ok": result.get("ok"),
                        "stdout_excerpt": text_excerpt(result.get("stdout"), limit=240),
                        "stderr_excerpt": text_excerpt(result.get("stderr"), limit=240),
                    }
                )

        if isinstance(flow_result, dict):
            for audit in ensure_list(flow_result.get("audits")):
                if not isinstance(audit, dict) or not audit:
                    continue
                operations.append(
                    {
                        "type": "onos-rest",
                        "mode": mode,
                        "planned": bool(flow_result.get("dry_run")),
                        "request": audit.get("request"),
                        "response": audit.get("response"),
                    }
                )
            if flow_result.get("planned_only"):
                operations.append(
                    {
                        "type": "onos-rest",
                        "mode": mode,
                        "planned": True,
                        "operation": "ensure_slice_queue_flows",
                        "details": flow_result.get("installed_flows", []),
                    }
                )
        if not operations:
            operations.append(
                {
                    "type": "none",
                    "mode": mode,
                    "planned": False,
                    "operation": "no_network_change",
                }
            )
        return operations

    def log_enforcement_record(
        self,
        status: str,
        context: Optional[Dict[str, Any]],
        previous_profile: Optional[Dict[str, Any]],
        target_profile: Optional[Dict[str, Any]],
        active_actions: Dict[str, str],
        flow_result: Optional[Dict[str, Any]],
        ovs_result: Optional[Dict[str, Any]],
        explanation: str,
        cooldown_status: Optional[Dict[str, Any]],
    ) -> None:
        target_service_classes = self.collect_target_service_classes(context, active_actions)
        action_names = self.collect_action_names(context, active_actions)
        source_references = []
        if isinstance(context, dict):
            source_references = [
                reference
                for reference in ensure_list(context.get("policy_references"))
                if isinstance(reference, dict)
            ]
        operations = self.collect_operations(flow_result, ovs_result)
        self.write_jsonl(
            {
                "event_type": "enforcement_action",
                "timestamp": utc_timestamp(),
                "run_id": self.run_id,
                "status": status,
                "result_status": status,
                "mode": self.mode_label(),
                "source_decision_reference": source_references[0] if len(source_references) == 1 else None,
                "source_decision_references": source_references,
                "action_name": action_names[0] if len(action_names) == 1 else "MULTI_ACTION",
                "action_names": action_names,
                "target_service_class": (
                    target_service_classes[0]
                    if len(target_service_classes) == 1
                    else "MULTI_SERVICE_CLASS"
                ),
                "target_service_classes": target_service_classes,
                "trigger_context": context,
                "service_actions": dict(self.service_actions),
                "active_actions": active_actions,
                "previous_profile": previous_profile,
                "target_profile": target_profile,
                "operations": operations,
                "onos_result": flow_result,
                "ovs_result": ovs_result,
                "explanation": explanation,
                "dry_run": self.dry_run,
                "cooldown_status": cooldown_status,
            }
        )

    def run_command(self, args: List[str]) -> Dict[str, Any]:
        command_name = args[0] if args else ""
        if not command_name:
            return {"ok": False, "command": args, "error": "empty command", "stdout": "", "stderr": ""}
        if shutil.which(command_name) is None:
            return {
                "ok": False,
                "command": args,
                "error": f"Required host command '{command_name}' is not available.",
                "stdout": "",
                "stderr": "",
            }
        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "command": args,
                "error": "command timed out after 15s",
                "stdout": "",
                "stderr": "",
            }
        except Exception as exc:  # pragma: no cover - defensive
            return {
                "ok": False,
                "command": args,
                "error": str(exc),
                "stdout": "",
                "stderr": "",
            }
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        return {
            "ok": completed.returncode == 0,
            "command": args,
            "return_code": completed.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "error": "" if completed.returncode == 0 else (stderr or stdout or "command failed"),
        }

    def docker_exec(self, *args: str) -> Dict[str, Any]:
        return self.run_command(["docker", "exec", str(self.ovs_cfg["container_name"]), *args])

    def get_port_qos_uuid(self) -> Dict[str, Any]:
        result = self.docker_exec("ovs-vsctl", "get", "port", str(self.ovs_cfg["egress_port_name"]), "qos")
        if not result["ok"]:
            return result
        qos_uuid = result["stdout"].strip()
        if qos_uuid in {"", "[]", "{}"}:
            result["ok"] = False
            result["error"] = "No QoS object is attached to the egress port."
            return result
        result["qos_uuid"] = qos_uuid
        return result

    def read_current_queue_profile(self) -> Dict[str, Any]:
        qos_result = self.get_port_qos_uuid()
        if not qos_result["ok"]:
            return {"ok": False, "error": qos_result.get("error"), "audit": [qos_result]}

        qos_uuid = str(qos_result["qos_uuid"])
        parent_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "other-config")
        queues_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "queues")
        audits = [qos_result, parent_result, queues_result]
        if not parent_result["ok"] or not queues_result["ok"]:
            return {"ok": False, "error": "Failed to read current OVS QoS settings.", "audit": audits}

        parent_map = parse_ovs_map(parent_result["stdout"])
        queue_map = parse_ovs_map(queues_result["stdout"])
        profile = {
            "parent_max_rate_bps": to_int(parent_map.get("max-rate")) or self.baseline_profile["parent_max_rate_bps"],
            "queues": {},
        }
        for queue_id, queue_uuid in queue_map.items():
            queue_result = self.docker_exec("ovs-vsctl", "get", "queue", queue_uuid, "other-config")
            audits.append(queue_result)
            if not queue_result["ok"]:
                return {"ok": False, "error": f"Failed to read queue {queue_id} settings.", "audit": audits}
            queue_cfg = parse_ovs_map(queue_result["stdout"])
            min_rate = to_int(queue_cfg.get("min-rate"))
            max_rate = to_int(queue_cfg.get("max-rate"))
            if min_rate is None or max_rate is None:
                return {
                    "ok": False,
                    "error": f"Queue {queue_id} does not expose min-rate/max-rate settings.",
                    "audit": audits,
                }
            profile["queues"][str(queue_id)] = {
                "min_rate_bps": min_rate,
                "max_rate_bps": max_rate,
            }
        return {"ok": True, "profile": normalize_queue_profile(profile), "audit": audits}

    def create_queue_profile(self, target_profile: Dict[str, Any]) -> Dict[str, Any]:
        queue_ids = sorted(target_profile["queues"], key=lambda item: int(item))
        command = [
            "docker",
            "exec",
            str(self.ovs_cfg["container_name"]),
            "ovs-vsctl",
            "--if-exists",
            "clear",
            "port",
            str(self.ovs_cfg["egress_port_name"]),
            "qos",
            "--",
            "set",
            "port",
            str(self.ovs_cfg["egress_port_name"]),
            "qos=@newqos",
            "--",
            "--id=@newqos",
            "create",
            "qos",
            "type=linux-htb",
            f"other-config:max-rate={target_profile['parent_max_rate_bps']}",
        ]
        for queue_id in queue_ids:
            command.append(f"queues:{queue_id}=@q{queue_id}")
        for queue_id in queue_ids:
            queue_cfg = target_profile["queues"][queue_id]
            command.extend(
                [
                    "--",
                    f"--id=@q{queue_id}",
                    "create",
                    "queue",
                    f"other-config:min-rate={queue_cfg['min_rate_bps']}",
                    f"other-config:max-rate={queue_cfg['max_rate_bps']}",
                ]
            )

        if self.dry_run:
            return {
                "ok": True,
                "mode": "create",
                "planned_commands": [" ".join(command)],
                "previous_profile": copy.deepcopy(self.current_profile),
            }

        result = self.run_command(command)
        return {
            "ok": result["ok"],
            "mode": "create",
            "result": result,
            "previous_profile": copy.deepcopy(self.current_profile),
        }

    def update_existing_queue_profile(
        self, target_profile: Dict[str, Any], qos_uuid: str, queue_uuid_map: Dict[str, str]
    ) -> Dict[str, Any]:
        commands: List[List[str]] = [
            [
                "docker",
                "exec",
                str(self.ovs_cfg["container_name"]),
                "ovs-vsctl",
                "set",
                "qos",
                qos_uuid,
                f"other-config:max-rate={target_profile['parent_max_rate_bps']}",
            ]
        ]
        for queue_id, queue_cfg in sorted(target_profile["queues"].items(), key=lambda item: int(item[0])):
            queue_uuid = queue_uuid_map.get(str(queue_id))
            if not queue_uuid:
                return self.create_queue_profile(target_profile)
            commands.append(
                [
                    "docker",
                    "exec",
                    str(self.ovs_cfg["container_name"]),
                    "ovs-vsctl",
                    "set",
                    "queue",
                    queue_uuid,
                    f"other-config:min-rate={queue_cfg['min_rate_bps']}",
                    f"other-config:max-rate={queue_cfg['max_rate_bps']}",
                ]
            )

        if self.dry_run:
            return {
                "ok": True,
                "mode": "update",
                "planned_commands": [" ".join(command) for command in commands],
                "previous_profile": copy.deepcopy(self.current_profile),
            }

        results = [self.run_command(command) for command in commands]
        return {
            "ok": all(result["ok"] for result in results),
            "mode": "update",
            "results": results,
            "previous_profile": copy.deepcopy(self.current_profile),
        }

    def apply_queue_profile(self, target_profile: Dict[str, Any]) -> Dict[str, Any]:
        qos_result = self.get_port_qos_uuid()
        if not qos_result["ok"]:
            return self.create_queue_profile(target_profile)

        qos_uuid = str(qos_result["qos_uuid"])
        queue_map_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "queues")
        if not queue_map_result["ok"]:
            return self.create_queue_profile(target_profile)
        queue_uuid_map = parse_ovs_map(queue_map_result["stdout"])
        return self.update_existing_queue_profile(target_profile, qos_uuid, queue_uuid_map)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply policy-manager decisions to live OVS queues and optional ONOS flow refresh."
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name(DEFAULT_CONFIG_NAME)),
        help="Path to the shared policy/enforcement config file.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process the current policy logs once and exit.",
    )
    parser.add_argument(
        "--restore-default",
        action="store_true",
        help="Ignore policy decisions and immediately restore the baseline/default queue profile.",
    )
    dry_group = parser.add_mutually_exclusive_group()
    dry_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Force dry-run mode even if the config enables live enforcement.",
    )
    dry_group.add_argument(
        "--live",
        action="store_true",
        help="Force live enforcement mode even if the config default is dry-run.",
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print_console("ERROR", f"Config file not found: {config_path}")
        return 1

    try:
        raw_config = load_config(config_path)
        config = normalize_config(config_path.resolve(), raw_config)
    except Exception as exc:
        print_console("ERROR", f"Failed to load config from {config_path}: {exc}")
        return 1

    dry_run_override: Optional[bool]
    if args.dry_run:
        dry_run_override = True
    elif args.live:
        dry_run_override = False
    else:
        dry_run_override = None

    manager = EnforcementManager(
        config_path=config_path,
        config=config,
        dry_run_override=dry_run_override,
        run_once=args.once,
        restore_default=args.restore_default,
    )
    return manager.run()


if __name__ == "__main__":
    sys.exit(main())
