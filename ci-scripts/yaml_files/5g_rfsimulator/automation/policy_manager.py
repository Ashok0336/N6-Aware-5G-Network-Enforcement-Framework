#!/usr/bin/env python3
"""Slice Policy Manager for threshold-based policy recommendations."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from service_mapping_utils import apply_service_mapping

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

try:
    from prometheus_client import Counter, Gauge, start_http_server
except ImportError as exc:  # pragma: no cover - dependency checked at runtime
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]
    PROMETHEUS_IMPORT_ERROR: Optional[ImportError] = exc
else:
    PROMETHEUS_IMPORT_ERROR = None


DEFAULT_CONFIG_NAME = "policy_config.yaml"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_TELEMETRY_DIR = "../logs/telemetry"
DEFAULT_TELEMETRY_GLOB = "telemetry_*.jsonl"
DEFAULT_POLICY_LOG_DIR = "../logs/policy"
DEFAULT_DECISION_COOLDOWN_SECONDS = 10
DEFAULT_UNCHANGED_LOG_INTERVAL_SECONDS = 30
DEFAULT_ACTION_MAINTAIN = "MAINTAIN_CURRENT_POLICY"
DEFAULT_ACTION_RESTORE = "RESTORE_DEFAULT_POLICY"
DEFAULT_METRICS_HTTP_HOST = "0.0.0.0"
DEFAULT_METRICS_HTTP_PORT = 8001
DEFAULT_MANUFACTURING_TWIN_LATEST_PATH = "logs/manufacturing_twin/latest_machine_twin_state.json"
DEFAULT_MANUFACTURING_TWIN_MAX_AGE_SECONDS = 10.0
DEFAULT_RISK_PREDICTION_PATH = "logs/risk_inference/latest_risk_prediction.json"
DEFAULT_RISK_MAX_AGE_SECONDS = 10.0
DEFAULT_RISK_HIGH_ACTION = "VERIFY_OR_REINSTALL_QUEUE_RULES"
DEFAULT_RISK_MEDIUM_ACTION = "VERIFY_QUEUE_RULES"
DEFAULT_RISK_LOW_ACTION = "MAINTAIN_CURRENT_POLICY"

SLICE_NAME_BY_SERVICE_CLASS = {
    "real_time_control": "urllc",
    "high_throughput_data": "embb",
    "sensor_telemetry": "mmtc",
}

MANUFACTURING_PHASE_SERVICE_CLASSES = {
    "print_initialization": {"real_time_control"},
    "active_printing": {"real_time_control", "sensor_telemetry"},
    "print_completion": {"real_time_control"},
    "job_upload": {"high_throughput_data"},
    "paused": {"real_time_control", "sensor_telemetry"},
    "machine_error": {"real_time_control", "sensor_telemetry"},
}

RISK_LEVEL_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
}

RISK_FIELD_BY_SERVICE_CLASS = {
    "real_time_control": "real_time_control_risk",
    "high_throughput_data": "high_throughput_data_risk",
    "sensor_telemetry": "sensor_telemetry_risk",
}


def utc_timestamp() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def print_console(level: str, message: str) -> None:
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[policy-manager][{stamp}][{level}] {message}", flush=True)


def normalize_path(base_dir: Path, value: str) -> str:
    return str((base_dir / value).resolve())


def normalize_manufacturing_twin_path(base_dir: Path, value: str) -> str:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    if candidate.parts and candidate.parts[0] == "logs":
        return str((base_dir.parent / candidate).resolve())
    return normalize_path(base_dir, value)


def normalize_repo_artifact_path(base_dir: Path, value: str) -> str:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    if candidate.parts and candidate.parts[0] in {"logs", "digital_twin", "risk_inference"}:
        return str((base_dir.parent / candidate).resolve())
    return normalize_path(base_dir, value)


def load_config(config_path: Path) -> Dict[str, Any]:
    raw_text = config_path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {config_path} must define a top-level mapping/object.")
    return loaded


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


def timestamp_age_seconds(value: Any, now: Optional[dt.datetime] = None) -> Optional[float]:
    parsed = parse_timestamp(str(value)) if value is not None else None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    reference = now or dt.datetime.now(dt.timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=dt.timezone.utc)
    return max(0.0, (reference - parsed).total_seconds())


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def coerce_bool(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, float) and value in {0.0, 1.0}:
        return bool(int(value))
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean value, got {value!r}.")


def prometheus_value(value: Any) -> float:
    numeric = to_float(value)
    if numeric is None:
        return float("nan")
    if math.isfinite(numeric):
        return numeric
    return float("nan")


def first_numeric(*values: Any) -> Optional[float]:
    for value in values:
        numeric_value = to_float(value)
        if numeric_value is not None:
            return numeric_value
    return None


def list_of_strings(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def matches_keywords(path: Optional[str], keywords: Iterable[str]) -> bool:
    if not path:
        return False
    lowered_path = path.lower()
    lowered_keywords = [keyword.lower() for keyword in keywords if str(keyword).strip()]
    if not lowered_keywords:
        return True
    return any(keyword in lowered_path for keyword in lowered_keywords)


def strip_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


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


def is_machine_twin_fresh(machine_twin: Optional[Dict[str, Any]], max_age_seconds: float) -> bool:
    if not isinstance(machine_twin, dict):
        return False
    age = timestamp_age_seconds(machine_twin.get("timestamp"))
    return age is not None and age <= max_age_seconds


def is_risk_prediction_fresh(
    prediction: Optional[Dict[str, Any]],
    max_age_seconds: float,
) -> bool:
    if not isinstance(prediction, dict):
        return False
    age = timestamp_age_seconds(prediction.get("timestamp"))
    return age is not None and age <= max_age_seconds


def normalize_risk_level(value: Any) -> str:
    if isinstance(value, bool):
        return "high" if value else "low"
    numeric_value = to_float(value)
    if numeric_value is not None:
        if numeric_value >= 2:
            return "high"
        if numeric_value >= 1:
            return "medium"
        return "low"
    text = str(value or "").strip().lower()
    if text in RISK_LEVEL_ORDER:
        return text
    if text in {"true", "yes", "risk"}:
        return "high"
    if text in {"false", "no", "none"}:
        return "low"
    return "unknown"


def risk_level_rank(value: Any) -> int:
    return RISK_LEVEL_ORDER.get(normalize_risk_level(value), -1)


def risk_is_at_least(value: Any, minimum: str) -> bool:
    return risk_level_rank(value) >= RISK_LEVEL_ORDER[minimum]


def selected_action_for_risk_level(
    overall_risk_level: str,
    *,
    high_action: str,
    medium_action: str,
    low_action: str,
) -> str:
    if overall_risk_level == "high":
        return high_action
    if overall_risk_level == "medium":
        return medium_action
    return low_action


def build_risk_inference_policy_context(
    prediction: Optional[Dict[str, Any]],
    *,
    enabled: bool = True,
    latest_path: Path | str = "",
    max_age_seconds: float = DEFAULT_RISK_MAX_AGE_SECONDS,
    high_action: str = DEFAULT_RISK_HIGH_ACTION,
    medium_action: str = DEFAULT_RISK_MEDIUM_ACTION,
    low_action: str = DEFAULT_RISK_LOW_ACTION,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "enabled": enabled,
        "status": "disabled" if not enabled else "missing",
        "fresh": False,
        "valid_for_policy": False,
        "latest_path": str(latest_path),
        "prediction": prediction if isinstance(prediction, dict) else None,
        "overall_risk_level": "disabled" if not enabled else "unknown",
        "overall_risk_score": None,
        "real_time_control_risk": "unknown",
        "high_throughput_data_risk": "unknown",
        "sensor_telemetry_risk": "unknown",
        "recommended_policy_action": None,
        "selected_policy_action": None,
        "inference_status": "disabled" if not enabled else "missing",
        "data_quality_status": [],
        "queue_rule_presence": None,
        "policy_drift_detected": None,
        "state_age_seconds": None,
        "error": None,
        "service_risk_levels": {},
    }
    if not enabled:
        return context
    if not isinstance(prediction, dict):
        context["error"] = "risk_prediction_unavailable"
        return context

    age = timestamp_age_seconds(prediction.get("timestamp"))
    inference_status = str(prediction.get("inference_status") or "unknown")
    valid_for_policy = bool(prediction.get("valid_for_policy") is True)
    overall_level = normalize_risk_level(prediction.get("overall_risk_level"))
    service_levels = risk_service_levels(prediction)
    selected_action = selected_action_for_risk_level(
        overall_level,
        high_action=high_action,
        medium_action=medium_action,
        low_action=low_action,
    )
    context.update(
        {
            "overall_risk_level": overall_level,
            "overall_risk_score": to_float(prediction.get("overall_risk_score")),
            "real_time_control_risk": service_levels["real_time_control"],
            "high_throughput_data_risk": service_levels["high_throughput_data"],
            "sensor_telemetry_risk": service_levels["sensor_telemetry"],
            "recommended_policy_action": prediction.get("recommended_policy_action"),
            "selected_policy_action": selected_action,
            "inference_status": inference_status,
            "valid_for_policy": valid_for_policy,
            "data_quality_status": prediction.get("data_quality_status") or [],
            "queue_rule_presence": prediction.get("queue_rule_presence"),
            "policy_drift_detected": prediction.get("policy_drift_detected"),
            "state_age_seconds": age,
            "service_risk_levels": service_levels,
        }
    )
    if age is None:
        context["status"] = "stale"
        context["error"] = "risk_prediction_timestamp_unparseable"
        return context
    if age > max_age_seconds:
        context["status"] = "stale"
        context["error"] = f"risk_prediction_stale:{age:.3f}s>{max_age_seconds:.3f}s"
        return context
    if not valid_for_policy:
        context["status"] = inference_status
        context["error"] = f"risk_prediction_not_valid_for_policy:{inference_status}"
        return context
    context["fresh"] = True
    context["status"] = "fresh"
    return context


def risk_service_levels(prediction: Dict[str, Any]) -> Dict[str, str]:
    service_risks = prediction.get("service_risks")
    levels: Dict[str, str] = {}
    for service_class in SLICE_NAME_BY_SERVICE_CLASS:
        level = None
        if isinstance(service_risks, dict):
            record = service_risks.get(service_class)
            if isinstance(record, dict):
                level = record.get("risk_level")
            else:
                level = record
        if level is None:
            level = prediction.get(RISK_FIELD_BY_SERVICE_CLASS.get(service_class, ""))
        levels[service_class] = normalize_risk_level(level)
    return levels


def get_manufacturing_phase(machine_twin: Optional[Dict[str, Any]]) -> str:
    if not isinstance(machine_twin, dict):
        return "unknown"
    phase = machine_twin.get("manufacturing_phase")
    return str(phase) if phase else "unknown"


def get_machine_service_criticality(machine_twin: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(machine_twin, dict):
        return {}
    criticality = machine_twin.get("service_criticality")
    if not isinstance(criticality, dict):
        return {}
    return {str(key): str(value) for key, value in criticality.items()}


def get_service_criticality(machine_twin: Optional[Dict[str, Any]]) -> Dict[str, str]:
    return get_machine_service_criticality(machine_twin)


def service_classes_for_manufacturing_phase(phase: str) -> set[str]:
    return set(MANUFACTURING_PHASE_SERVICE_CLASSES.get(str(phase), set()))


def build_manufacturing_policy_context(
    machine_twin: Optional[Dict[str, Any]],
    *,
    enabled: bool = True,
    latest_path: Path | str = "",
    max_age_seconds: float = DEFAULT_MANUFACTURING_TWIN_MAX_AGE_SECONDS,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "enabled": enabled,
        "status": "disabled" if not enabled else "missing_or_stale",
        "fresh": False,
        "latest_path": str(latest_path),
        "state": machine_twin if isinstance(machine_twin, dict) else None,
        "phase": "disabled" if not enabled else "unknown",
        "availability": None,
        "printer_state_text": None,
        "job_state": None,
        "job_progress_percent": None,
        "service_criticality": {},
        "state_age_seconds": None,
        "error": None,
        "protected_service_classes": [],
    }
    if not enabled:
        return context
    if not isinstance(machine_twin, dict):
        context["error"] = "latest_machine_twin_state_unavailable"
        return context

    age = timestamp_age_seconds(machine_twin.get("timestamp"))
    phase = get_manufacturing_phase(machine_twin)
    context.update(
        {
            "phase": phase,
            "availability": machine_twin.get("availability"),
            "printer_state_text": machine_twin.get("printer_state_text"),
            "job_state": machine_twin.get("job_state"),
            "job_progress_percent": machine_twin.get("job_progress_percent"),
            "service_criticality": get_machine_service_criticality(machine_twin),
            "state_age_seconds": age,
            "protected_service_classes": sorted(service_classes_for_manufacturing_phase(phase)),
        }
    )
    if age is None:
        context["error"] = "machine_twin_timestamp_unparseable"
        return context
    if age > max_age_seconds:
        context["error"] = f"machine_twin_stale:{age:.3f}s>{max_age_seconds:.3f}s"
        return context
    context["fresh"] = True
    context["status"] = "fresh"
    return context


def normalize_config(config_path: Path, raw_config: Dict[str, Any]) -> Dict[str, Any]:
    base_dir = config_path.parent
    config = dict(raw_config)
    env_metrics_http_host = os.getenv("POLICY_MANAGER_METRICS_HOST", "").strip()
    env_metrics_http_port = os.getenv("POLICY_MANAGER_METRICS_PORT", "").strip()
    env_manufacturing_twin_enabled = os.getenv("MANUFACTURING_TWIN_ENABLED", "").strip()
    env_manufacturing_twin_latest_path = os.getenv("MANUFACTURING_TWIN_LATEST_PATH", "").strip()
    env_manufacturing_twin_max_age_seconds = os.getenv("MANUFACTURING_TWIN_MAX_AGE_SECONDS", "").strip()
    env_risk_inference_enabled = os.getenv("DT_RISK_INFERENCE_ENABLED", "").strip()
    env_risk_prediction_path = os.getenv("DT_RISK_PREDICTION_PATH", "").strip()
    env_risk_max_age_seconds = os.getenv("DT_RISK_MAX_AGE_SECONDS", "").strip()
    env_risk_high_action = os.getenv("DT_RISK_HIGH_ACTION", "").strip()
    env_risk_medium_action = os.getenv("DT_RISK_MEDIUM_ACTION", "").strip()
    env_risk_low_action = os.getenv("DT_RISK_LOW_ACTION", "").strip()

    config.setdefault("polling_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
    config.setdefault("telemetry_dir", DEFAULT_TELEMETRY_DIR)
    config.setdefault("telemetry_glob", DEFAULT_TELEMETRY_GLOB)
    config.setdefault("log_dir", DEFAULT_POLICY_LOG_DIR)
    config.setdefault("metrics_http_host", DEFAULT_METRICS_HTTP_HOST)
    config.setdefault("metrics_http_port", DEFAULT_METRICS_HTTP_PORT)
    config.setdefault("decision_cooldown_seconds", DEFAULT_DECISION_COOLDOWN_SECONDS)
    config.setdefault("emit_unchanged_decisions", True)
    config.setdefault(
        "unchanged_decision_log_interval_seconds",
        DEFAULT_UNCHANGED_LOG_INTERVAL_SECONDS,
    )
    config.setdefault("default_policy_action", DEFAULT_ACTION_MAINTAIN)
    config.setdefault("restore_policy_action", DEFAULT_ACTION_RESTORE)
    config.setdefault("manufacturing_twin_enabled", False)
    config.setdefault("manufacturing_twin_latest_path", DEFAULT_MANUFACTURING_TWIN_LATEST_PATH)
    config.setdefault("manufacturing_twin_max_age_seconds", DEFAULT_MANUFACTURING_TWIN_MAX_AGE_SECONDS)
    config.setdefault("risk_inference_enabled", False)
    config.setdefault("risk_prediction_path", DEFAULT_RISK_PREDICTION_PATH)
    config.setdefault("risk_max_age_seconds", DEFAULT_RISK_MAX_AGE_SECONDS)
    config.setdefault("risk_high_action", DEFAULT_RISK_HIGH_ACTION)
    config.setdefault("risk_medium_action", DEFAULT_RISK_MEDIUM_ACTION)
    config.setdefault("risk_low_action", DEFAULT_RISK_LOW_ACTION)
    config.setdefault(
        "service_classes",
        {
            "real_time_control": {
                "policy_name": "real_time_control_guard",
                "trigger_action_name": "INCREASE_CONTROL_PRIORITY",
                "traffic_udp_port": 5202,
                "ping_log_keywords": ["urllc"],
                "iperf_log_keywords": ["urllc"],
                "latency_avg_threshold_ms": 20.0,
                "latency_max_threshold_ms": 35.0,
                "packet_loss_threshold_percent": 1.0,
            },
            "high_throughput_data": {
                "policy_name": "high_throughput_guard",
                "trigger_action_name": "TIGHTEN_DATA_SHAPING",
                "traffic_udp_port": 5201,
                "iperf_log_keywords": ["embb"],
                "monitored_queue_ids": ["1"],
                "throughput_threshold_bps": 60000000,
                "queue_occupancy_threshold_bytes_per_second": 4000000,
                "queue_packets_threshold_per_second": 500.0,
            },
            "sensor_telemetry": {
                "policy_name": "sensor_telemetry_guard",
                "trigger_action_name": "PROTECT_SENSOR_MIN_BW",
                "traffic_udp_port": 5203,
                "iperf_log_keywords": ["mmtc"],
                "monitored_interface_names": ["v-edn-host"],
                "loss_threshold_percent": 2.0,
                "delivery_ratio_threshold_percent": 98.0,
                "drop_rate_threshold_percent": 0.5,
            },
        },
    )
    config = apply_service_mapping(base_dir, config)
    enforcement = dict(config.get("enforcement", {}))
    enforcement.setdefault("dry_run", True)
    enforcement["dry_run"] = coerce_bool(
        enforcement["dry_run"],
        field_name="enforcement.dry_run",
    )
    config["enforcement"] = enforcement

    if env_metrics_http_host:
        config["metrics_http_host"] = env_metrics_http_host
    if env_metrics_http_port:
        config["metrics_http_port"] = env_metrics_http_port
    if env_manufacturing_twin_enabled:
        config["manufacturing_twin_enabled"] = env_manufacturing_twin_enabled
    if env_manufacturing_twin_latest_path:
        config["manufacturing_twin_latest_path"] = env_manufacturing_twin_latest_path
    if env_manufacturing_twin_max_age_seconds:
        config["manufacturing_twin_max_age_seconds"] = env_manufacturing_twin_max_age_seconds
    if env_risk_inference_enabled:
        config["risk_inference_enabled"] = env_risk_inference_enabled
    if env_risk_prediction_path:
        config["risk_prediction_path"] = env_risk_prediction_path
    if env_risk_max_age_seconds:
        config["risk_max_age_seconds"] = env_risk_max_age_seconds
    if env_risk_high_action:
        config["risk_high_action"] = env_risk_high_action
    if env_risk_medium_action:
        config["risk_medium_action"] = env_risk_medium_action
    if env_risk_low_action:
        config["risk_low_action"] = env_risk_low_action

    config["polling_interval_seconds"] = float(config["polling_interval_seconds"])
    config["metrics_http_host"] = str(config["metrics_http_host"])
    config["metrics_http_port"] = int(config["metrics_http_port"])
    config["decision_cooldown_seconds"] = float(config["decision_cooldown_seconds"])
    config["emit_unchanged_decisions"] = coerce_bool(
        config["emit_unchanged_decisions"],
        field_name="emit_unchanged_decisions",
    )
    config["unchanged_decision_log_interval_seconds"] = float(
        config["unchanged_decision_log_interval_seconds"]
    )
    config["telemetry_dir"] = normalize_path(base_dir, str(config["telemetry_dir"]))
    config["log_dir"] = normalize_path(base_dir, str(config["log_dir"]))
    config["manufacturing_twin_enabled"] = coerce_bool(
        config["manufacturing_twin_enabled"],
        field_name="manufacturing_twin_enabled",
    )
    config["manufacturing_twin_latest_path"] = normalize_manufacturing_twin_path(
        base_dir,
        str(config["manufacturing_twin_latest_path"]),
    )
    config["manufacturing_twin_max_age_seconds"] = float(
        config["manufacturing_twin_max_age_seconds"]
    )
    config["risk_inference_enabled"] = coerce_bool(
        config["risk_inference_enabled"],
        field_name="risk_inference_enabled",
    )
    config["risk_prediction_path"] = normalize_repo_artifact_path(
        base_dir,
        str(config["risk_prediction_path"]),
    )
    config["risk_max_age_seconds"] = float(config["risk_max_age_seconds"])
    config["risk_high_action"] = str(config["risk_high_action"])
    config["risk_medium_action"] = str(config["risk_medium_action"])
    config["risk_low_action"] = str(config["risk_low_action"])

    normalized_services: Dict[str, Dict[str, Any]] = {}
    for service_class, service_cfg in config.get("service_classes", {}).items():
        if not isinstance(service_cfg, dict):
            raise ValueError(f"service_classes.{service_class} must be a mapping/object.")
        entry = dict(service_cfg)
        entry.setdefault("policy_name", f"{service_class}_policy")
        entry.setdefault("trigger_action_name", DEFAULT_ACTION_MAINTAIN)
        entry.setdefault("ping_log_keywords", [])
        entry.setdefault("iperf_log_keywords", [])
        entry.setdefault("monitored_queue_ids", [])
        entry.setdefault("monitored_interface_names", [])
        entry["ping_log_keywords"] = list_of_strings(entry.get("ping_log_keywords"))
        entry["iperf_log_keywords"] = list_of_strings(entry.get("iperf_log_keywords"))
        entry["monitored_queue_ids"] = list_of_strings(entry.get("monitored_queue_ids"))
        entry["monitored_interface_names"] = list_of_strings(
            entry.get("monitored_interface_names")
        )

        for numeric_key in (
            "traffic_udp_port",
            "latency_avg_threshold_ms",
            "latency_max_threshold_ms",
            "packet_loss_threshold_percent",
            "throughput_threshold_bps",
            "queue_occupancy_threshold_bytes_per_second",
            "queue_packets_threshold_per_second",
            "loss_threshold_percent",
            "delivery_ratio_threshold_percent",
            "drop_rate_threshold_percent",
        ):
            if numeric_key in entry and entry[numeric_key] is not None:
                numeric_value = to_float(entry[numeric_key])
                if numeric_value is None:
                    raise ValueError(
                        f"service_classes.{service_class}.{numeric_key} must be numeric."
                    )
                entry[numeric_key] = numeric_value
        normalized_services[str(service_class)] = entry

    config["service_classes"] = normalized_services
    return config


class PolicyManager:
    def __init__(
        self,
        config_path: Path,
        config: Dict[str, Any],
        run_once: bool = False,
        dry_run_override: Optional[bool] = None,
    ) -> None:
        self.config_path = config_path.resolve()
        self.config = config
        self.run_once = run_once
        self.enforcement_cfg = dict(config.get("enforcement", {}))
        self.dry_run_only = (
            dry_run_override
            if dry_run_override is not None
            else self.enforcement_cfg["dry_run"]
        )
        self.polling_interval_seconds = float(config["polling_interval_seconds"])
        self.metrics_http_host = str(config["metrics_http_host"])
        self.metrics_http_port = int(config["metrics_http_port"])
        self.telemetry_dir = Path(str(config["telemetry_dir"]))
        self.telemetry_glob = str(config["telemetry_glob"])
        self.log_dir = Path(str(config["log_dir"]))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.decision_cooldown_seconds = float(config["decision_cooldown_seconds"])
        self.emit_unchanged_decisions = bool(config["emit_unchanged_decisions"])
        self.unchanged_log_interval_seconds = float(
            config["unchanged_decision_log_interval_seconds"]
        )
        self.manufacturing_twin_enabled = bool(config["manufacturing_twin_enabled"])
        self.manufacturing_twin_latest_path = Path(str(config["manufacturing_twin_latest_path"]))
        self.manufacturing_twin_max_age_seconds = float(
            config["manufacturing_twin_max_age_seconds"]
        )
        self.risk_inference_enabled = bool(config["risk_inference_enabled"])
        self.risk_prediction_path = Path(str(config["risk_prediction_path"]))
        self.risk_max_age_seconds = float(config["risk_max_age_seconds"])
        self.risk_high_action = str(config["risk_high_action"])
        self.risk_medium_action = str(config["risk_medium_action"])
        self.risk_low_action = str(config["risk_low_action"])
        self.default_action = str(config["default_policy_action"])
        self.restore_action = str(config["restore_policy_action"])
        self.service_configs = dict(config["service_classes"])
        self.stop_requested = False
        self.received_signal_name: Optional[str] = None
        self.warned_keys: set[str] = set()
        self.file_state: Dict[str, Dict[str, int]] = {}
        self.previous_snapshot: Optional[Dict[str, Any]] = None
        self.previous_snapshot_time: Optional[dt.datetime] = None
        self.last_processed_telemetry_reference: Optional[Dict[str, Any]] = None
        self.current_manufacturing_context: Dict[str, Any] = {
            "enabled": self.manufacturing_twin_enabled,
            "status": "disabled" if not self.manufacturing_twin_enabled else "missing_or_stale",
            "fresh": False,
            "phase": "disabled" if not self.manufacturing_twin_enabled else "unknown",
            "availability": None,
            "printer_state_text": None,
            "job_state": None,
            "job_progress_percent": None,
            "service_criticality": {},
            "state_age_seconds": None,
        }
        self.current_risk_context: Dict[str, Any] = {
            "enabled": self.risk_inference_enabled,
            "status": "disabled" if not self.risk_inference_enabled else "missing",
            "fresh": False,
            "valid_for_policy": False,
            "overall_risk_level": "disabled" if not self.risk_inference_enabled else "unknown",
            "overall_risk_score": None,
            "real_time_control_risk": "unknown",
            "high_throughput_data_risk": "unknown",
            "sensor_telemetry_risk": "unknown",
            "recommended_policy_action": None,
            "selected_policy_action": None,
            "inference_status": "disabled" if not self.risk_inference_enabled else "missing",
            "data_quality_status": [],
            "queue_rule_presence": None,
            "policy_drift_detected": None,
            "state_age_seconds": None,
        }
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = f"policy-manager-{stamp}-{os.getpid()}"
        self.log_path = self.log_dir / f"policy_decisions_{stamp}.jsonl"
        self.log_handle = self.log_path.open("a", encoding="utf-8")
        self.metrics_path = "/metrics"
        self.metrics_access_url = f"http://127.0.0.1:{self.metrics_http_port}{self.metrics_path}"
        self.metrics_bind_label = f"{self.metrics_http_host}:{self.metrics_http_port}"
        self.metrics_server_started = False
        self.service_state: Dict[str, Dict[str, Any]] = {}
        for service_class in self.service_configs:
            self.service_state[service_class] = {
                "effective_action": self.default_action,
                "pending_target_state": None,
                "pending_condition": None,
                "pending_since": None,
                "pending_logged": False,
                "last_emitted_action": None,
                "last_emitted_state": None,
                "last_emitted_condition": None,
                "last_emitted_at": None,
                "last_decision_state": None,
            }
        self._register_prometheus_metrics()

    def _register_prometheus_metrics(self) -> None:
        if Gauge is None or Counter is None:
            return
        self.prometheus_policy_cycle_total = Counter(
            "policy_cycle_total",
            "Total number of policy evaluation cycles processed by the policy manager.",
        )
        self.prometheus_policy_action_applied_total = Counter(
            "policy_action_applied_total",
            "Total number of new policy actions applied to the policy manager state machine.",
            ["slice", "service_class", "policy_name", "recommended_action"],
        )
        self.prometheus_policy_dry_run_mode = Gauge(
            "policy_dry_run_mode",
            "Dry-run mode indicator for the policy manager. 1 means dry-run, 0 means active.",
        )
        self.prometheus_policy_last_decision_timestamp = Gauge(
            "policy_last_decision_timestamp",
            "Unix timestamp of the last emitted policy decision for each slice.",
            ["slice", "service_class", "policy_name"],
        )
        self.prometheus_slice_policy_state = Gauge(
            "slice_policy_state",
            "Current policy state per slice. The active label set has value 1.",
            [
                "slice",
                "service_class",
                "policy_name",
                "recommended_action",
                "state_after_decision",
                "decision_state",
            ],
        )

    def start_metrics_server(self) -> None:
        if self.metrics_server_started:
            return
        if PROMETHEUS_IMPORT_ERROR is not None or start_http_server is None:
            raise RuntimeError(
                "prometheus_client is required for the policy manager metrics endpoint. "
                "Install it before running policy_manager.py."
            ) from PROMETHEUS_IMPORT_ERROR
        start_http_server(self.metrics_http_port, addr=self.metrics_http_host)
        self.metrics_server_started = True
        print_console(
            "INFO",
            f"Prometheus metrics available at {self.metrics_access_url} "
            f"(bind {self.metrics_bind_label}).",
        )

    def resolve_slice_name(self, service_class: str) -> str:
        return SLICE_NAME_BY_SERVICE_CLASS.get(service_class, service_class)

    def update_policy_state_metrics(self) -> None:
        if Gauge is None or Counter is None:
            raise RuntimeError(
                "prometheus_client is required for Prometheus metrics export."
            )
        self.prometheus_policy_dry_run_mode.set(1.0 if self.dry_run_only else 0.0)
        self.prometheus_slice_policy_state.clear()
        self.prometheus_policy_last_decision_timestamp.clear()
        for service_class, service_cfg in self.service_configs.items():
            state = self.service_state.get(service_class, {})
            if not isinstance(state, dict):
                continue
            slice_name = self.resolve_slice_name(service_class)
            policy_name = str(service_cfg.get("policy_name") or service_class)
            recommended_action = str(
                state.get("last_emitted_action")
                or state.get("effective_action")
                or self.default_action
            )
            state_after_decision = str(
                state.get("last_emitted_state")
                or state.get("effective_action")
                or self.default_action
            )
            decision_state = str(state.get("last_decision_state") or "initial")
            self.prometheus_slice_policy_state.labels(
                slice=slice_name,
                service_class=service_class,
                policy_name=policy_name,
                recommended_action=recommended_action,
                state_after_decision=state_after_decision,
                decision_state=decision_state,
            ).set(1.0)
            last_emitted_at = state.get("last_emitted_at")
            timestamp_value = (
                last_emitted_at.timestamp()
                if isinstance(last_emitted_at, dt.datetime)
                else float("nan")
            )
            self.prometheus_policy_last_decision_timestamp.labels(
                slice=slice_name,
                service_class=service_class,
                policy_name=policy_name,
            ).set(timestamp_value)

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

    def run(self) -> int:
        self.install_signal_handlers()
        print_console("INFO", f"Using config: {self.config_path}")
        print_console("INFO", f"Operating mode: {'dry-run' if self.dry_run_only else 'active'}")
        print_console("INFO", f"Watching telemetry directory: {self.telemetry_dir}")
        print_console(
            "INFO",
            f"Manufacturing twin enabled={str(self.manufacturing_twin_enabled).lower()} "
            f"latest_path={self.manufacturing_twin_latest_path}",
        )
        print_console(
            "INFO",
            f"Risk inference enabled={str(self.risk_inference_enabled).lower()} "
            f"prediction_path={self.risk_prediction_path}",
        )
        print_console(
            "INFO",
            "Policy decisions are emitted for enforcement_manager.py; OVS/ONOS updates happen in the enforcement layer.",
        )
        print_console("INFO", f"Writing policy decisions to: {self.log_path}")

        self.write_jsonl(
            {
                "event_type": "policy_manager_start",
                "timestamp": utc_timestamp(),
                "run_id": self.run_id,
                "config_path": str(self.config_path),
                "log_path": str(self.log_path),
                "dry_run_only": self.dry_run_only,
                "mode": "dry-run" if self.dry_run_only else "active",
                "no_live_enforcement": self.dry_run_only,
                "external_enforcement_only": True,
                "manufacturing_twin_enabled": self.manufacturing_twin_enabled,
                "manufacturing_twin_latest_path": str(self.manufacturing_twin_latest_path),
                "risk_inference_enabled": self.risk_inference_enabled,
                "risk_prediction_path": str(self.risk_prediction_path),
            }
        )

        exit_code = 0
        try:
            self.start_metrics_server()
            self.update_policy_state_metrics()
            while not self.stop_requested:
                processed_any = False
                for snapshot, telemetry_reference in self.read_new_snapshots():
                    processed_any = True
                    self.process_snapshot(snapshot, telemetry_reference)
                if self.run_once:
                    if not processed_any:
                        print_console("WARN", "No new telemetry snapshots were available to evaluate.")
                    break
                self.sleep_with_stop(self.polling_interval_seconds)
        except KeyboardInterrupt:
            print_console("INFO", "Interrupted by Ctrl+C.")
        except Exception as exc:  # pragma: no cover - defensive
            exit_code = 1
            trace = traceback.format_exc()
            print_console("ERROR", f"Policy manager hit an unexpected error: {exc}")
            self.write_jsonl(
                {
                    "event_type": "policy_manager_error",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "error": str(exc),
                    "traceback": trace,
                    "dry_run_only": self.dry_run_only,
                    "mode": "dry-run" if self.dry_run_only else "active",
                }
            )
        finally:
            if self.received_signal_name:
                print_console(
                    "INFO",
                    f"Received {self.received_signal_name}; stopping after the current cycle.",
                )
            self.write_jsonl(
                {
                    "event_type": "policy_manager_stop",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "last_telemetry_reference": self.last_processed_telemetry_reference,
                    "stopped_cleanly": exit_code == 0,
                    "dry_run_only": self.dry_run_only,
                    "mode": "dry-run" if self.dry_run_only else "active",
                }
            )
            self.log_handle.close()
            print_console("INFO", f"Policy manager stopped. Final log file: {self.log_path}")
        return exit_code

    def sleep_with_stop(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.stop_requested:
            chunk = min(0.25, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def discover_telemetry_files(self) -> List[Path]:
        if not self.telemetry_dir.exists():
            self.warn_once(
                "missing-telemetry-dir",
                f"Telemetry directory does not exist yet: {self.telemetry_dir}",
            )
            return []
        return sorted(self.telemetry_dir.glob(self.telemetry_glob))

    def read_new_snapshots(self) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
        discovered = self.discover_telemetry_files()
        collected: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

        for path in discovered:
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
                    position_before = handle.tell()
                    line = handle.readline()
                    if not line:
                        state["offset"] = handle.tell()
                        break
                    state["line_number"] += 1
                    stripped = line.strip()
                    if not stripped:
                        state["offset"] = handle.tell()
                        continue
                    try:
                        payload = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        self.warn_once(
                            f"jsonl-parse-error:{resolved}:{state['line_number']}",
                            f"Skipping malformed telemetry JSON at {resolved}:{state['line_number']}: {exc}",
                        )
                        state["offset"] = handle.tell()
                        continue
                    if not isinstance(payload, dict):
                        state["offset"] = handle.tell()
                        continue
                    if payload.get("event_type") != "snapshot":
                        state["offset"] = handle.tell()
                        continue

                    telemetry_reference = {
                        "telemetry_file": resolved,
                        "telemetry_line_number": state["line_number"],
                        "telemetry_timestamp": payload.get("timestamp"),
                        "telemetry_run_id": payload.get("run_id"),
                        "telemetry_sample_index": payload.get("sample_index"),
                        "file_offset_start": position_before,
                    }
                    collected.append((payload, telemetry_reference))
                    state["offset"] = handle.tell()

        collected.sort(
            key=lambda item: (
                item[1].get("telemetry_timestamp") or "",
                item[1].get("telemetry_file") or "",
                int(item[1].get("telemetry_line_number") or 0),
            )
        )
        return collected

    def process_snapshot(
        self, snapshot: Dict[str, Any], telemetry_reference: Dict[str, Any]
    ) -> None:
        if hasattr(self, "prometheus_policy_cycle_total"):
            self.prometheus_policy_cycle_total.inc()
        snapshot_time = parse_timestamp(snapshot.get("timestamp"))
        delta_seconds = None
        if snapshot_time is not None and self.previous_snapshot_time is not None:
            elapsed = (snapshot_time - self.previous_snapshot_time).total_seconds()
            if elapsed > 0:
                delta_seconds = elapsed

        context = self.build_context(snapshot, delta_seconds)
        self.current_manufacturing_context = dict(context.get("manufacturing_twin", {}))
        self.current_risk_context = dict(context.get("risk_inference", {}))

        decision_count = 0
        for service_class, service_cfg in self.service_configs.items():
            evaluation = self.evaluate_service(
                service_class=service_class,
                service_cfg=service_cfg,
                snapshot=snapshot,
                context=context,
            )
            decision = self.resolve_decision(
                service_class=service_class,
                service_cfg=service_cfg,
                evaluation=evaluation,
                telemetry_reference=telemetry_reference,
                snapshot_time=snapshot_time,
            )
            if decision is not None:
                decision_count += 1
                self.write_jsonl(decision)
                print_console("INFO", str(decision.get("decision_log_summary")))
                if (
                    decision.get("is_new_decision")
                    and hasattr(self, "prometheus_policy_action_applied_total")
                ):
                    self.prometheus_policy_action_applied_total.labels(
                        slice=self.resolve_slice_name(service_class),
                        service_class=service_class,
                        policy_name=str(service_cfg.get("policy_name") or service_class),
                        recommended_action=str(
                            decision.get("recommended_action") or self.default_action
                        ),
                    ).inc()

        self.previous_snapshot = snapshot
        self.previous_snapshot_time = snapshot_time
        self.last_processed_telemetry_reference = telemetry_reference
        try:
            self.update_policy_state_metrics()
        except Exception as exc:
            self.warn_once(
                f"policy-prometheus-update:{type(exc).__name__}",
                f"Failed to update policy Prometheus metrics: {exc}",
            )
        print_console(
            "INFO",
            f"Evaluated telemetry sample {telemetry_reference.get('telemetry_sample_index')} "
            f"from {Path(str(telemetry_reference.get('telemetry_file', 'unknown'))).name}; "
            f"emitted {decision_count} decision record(s).",
        )

    def build_context(self, snapshot: Dict[str, Any], delta_seconds: Optional[float]) -> Dict[str, Any]:
        context = {
            "delta_seconds": delta_seconds,
            "queue_rates": self.compute_queue_rates(snapshot, delta_seconds),
            "interface_rates": self.compute_interface_rates(snapshot, delta_seconds),
            "flow_rates": self.compute_flow_rates(snapshot, delta_seconds),
            "manufacturing_twin": self.build_manufacturing_twin_context(),
            "risk_inference": self.build_risk_inference_context(),
        }
        return context

    def build_risk_inference_context(self) -> Dict[str, Any]:
        if not self.risk_inference_enabled:
            return build_risk_inference_policy_context(
                None,
                enabled=False,
                latest_path=self.risk_prediction_path,
                max_age_seconds=self.risk_max_age_seconds,
                high_action=self.risk_high_action,
                medium_action=self.risk_medium_action,
                low_action=self.risk_low_action,
            )

        prediction = read_risk_prediction(self.risk_prediction_path)
        context = build_risk_inference_policy_context(
            prediction,
            enabled=True,
            latest_path=self.risk_prediction_path,
            max_age_seconds=self.risk_max_age_seconds,
            high_action=self.risk_high_action,
            medium_action=self.risk_medium_action,
            low_action=self.risk_low_action,
        )
        if context.get("status") in {"missing", "stale"}:
            self.warn_once(
                f"risk-inference-{context.get('status')}",
                f"risk_inference_status={context.get('status')} latest_path={self.risk_prediction_path}",
            )
        return context

    def build_manufacturing_twin_context(self) -> Dict[str, Any]:
        if not self.manufacturing_twin_enabled:
            return build_manufacturing_policy_context(
                None,
                enabled=False,
                latest_path=self.manufacturing_twin_latest_path,
                max_age_seconds=self.manufacturing_twin_max_age_seconds,
            )

        machine_twin = read_manufacturing_twin_state(self.manufacturing_twin_latest_path)
        context = build_manufacturing_policy_context(
            machine_twin,
            enabled=True,
            latest_path=self.manufacturing_twin_latest_path,
            max_age_seconds=self.manufacturing_twin_max_age_seconds,
        )
        if context.get("status") == "missing_or_stale":
            self.warn_once(
                "manufacturing-twin-missing-or-stale",
                f"manufacturing_twin_status=missing_or_stale latest_path={self.manufacturing_twin_latest_path}",
            )
        return context

    def compute_queue_rates(
        self, snapshot: Dict[str, Any], delta_seconds: Optional[float]
    ) -> Dict[Tuple[str, str], Dict[str, Optional[float]]]:
        rates: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}
        if not self.previous_snapshot or not delta_seconds:
            return rates

        current_stats = (
            snapshot.get("ovs_queue_statistics", {}).get("queue_stats", [])
            if isinstance(snapshot.get("ovs_queue_statistics"), dict)
            else []
        )
        previous_stats = (
            self.previous_snapshot.get("ovs_queue_statistics", {}).get("queue_stats", [])
            if isinstance(self.previous_snapshot.get("ovs_queue_statistics"), dict)
            else []
        )

        previous_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for record in previous_stats:
            if not isinstance(record, dict):
                continue
            key = (str(record.get("port")), str(record.get("queue_id")))
            previous_map[key] = record

        for record in current_stats:
            if not isinstance(record, dict):
                continue
            key = (str(record.get("port")), str(record.get("queue_id")))
            previous_record = previous_map.get(key)
            if not previous_record:
                continue
            current_bytes = to_float(record.get("bytes"))
            current_packets = to_float(record.get("packets"))
            current_errors = to_float(record.get("errors"))
            previous_bytes = to_float(previous_record.get("bytes"))
            previous_packets = to_float(previous_record.get("packets"))
            previous_errors = to_float(previous_record.get("errors"))
            if None in {
                current_bytes,
                current_packets,
                current_errors,
                previous_bytes,
                previous_packets,
                previous_errors,
            }:
                continue
            delta_bytes = current_bytes - previous_bytes
            delta_packets = current_packets - previous_packets
            delta_errors = current_errors - previous_errors
            if delta_bytes < 0 or delta_packets < 0 or delta_errors < 0:
                continue
            rates[key] = {
                "bytes_per_second": delta_bytes / delta_seconds,
                "packets_per_second": delta_packets / delta_seconds,
                "errors_per_second": delta_errors / delta_seconds,
            }
        return rates

    def compute_interface_rates(
        self, snapshot: Dict[str, Any], delta_seconds: Optional[float]
    ) -> Dict[str, Dict[str, Optional[float]]]:
        rates: Dict[str, Dict[str, Optional[float]]] = {}
        if not self.previous_snapshot or not delta_seconds:
            return rates

        current_stats = snapshot.get("interface_statistics", {})
        previous_stats = self.previous_snapshot.get("interface_statistics", {})
        if not isinstance(current_stats, dict) or not isinstance(previous_stats, dict):
            return rates

        for interface_name, current_record in current_stats.items():
            if not isinstance(current_record, dict) or not current_record.get("available"):
                continue
            previous_record = previous_stats.get(interface_name)
            if not isinstance(previous_record, dict) or not previous_record.get("available"):
                continue

            current_tx_bytes = to_float(current_record.get("tx_bytes"))
            current_tx_packets = to_float(current_record.get("tx_packets"))
            current_tx_dropped = to_float(current_record.get("tx_dropped"))
            previous_tx_bytes = to_float(previous_record.get("tx_bytes"))
            previous_tx_packets = to_float(previous_record.get("tx_packets"))
            previous_tx_dropped = to_float(previous_record.get("tx_dropped"))
            if None in {
                current_tx_bytes,
                current_tx_packets,
                current_tx_dropped,
                previous_tx_bytes,
                previous_tx_packets,
                previous_tx_dropped,
            }:
                continue
            delta_tx_bytes = current_tx_bytes - previous_tx_bytes
            delta_tx_packets = current_tx_packets - previous_tx_packets
            delta_tx_dropped = current_tx_dropped - previous_tx_dropped
            if delta_tx_bytes < 0 or delta_tx_packets < 0 or delta_tx_dropped < 0:
                continue

            delivered_packets = delta_tx_packets
            attempted_packets = delivered_packets + delta_tx_dropped
            drop_rate_percent = (
                (delta_tx_dropped / attempted_packets) * 100.0 if attempted_packets > 0 else None
            )
            rates[str(interface_name)] = {
                "tx_bits_per_second": (delta_tx_bytes * 8.0) / delta_seconds,
                "tx_packets_per_second": delta_tx_packets / delta_seconds,
                "tx_drops_per_second": delta_tx_dropped / delta_seconds,
                "drop_rate_percent": drop_rate_percent,
            }
        return rates

    def compute_flow_rates(
        self, snapshot: Dict[str, Any], delta_seconds: Optional[float]
    ) -> Dict[str, Dict[str, Optional[float]]]:
        rates: Dict[str, Dict[str, Optional[float]]] = {}
        if not self.previous_snapshot or not delta_seconds:
            return rates

        current_flows = (
            snapshot.get("ovs_flow_counters", {}).get("flows", [])
            if isinstance(snapshot.get("ovs_flow_counters"), dict)
            else []
        )
        previous_flows = (
            self.previous_snapshot.get("ovs_flow_counters", {}).get("flows", [])
            if isinstance(self.previous_snapshot.get("ovs_flow_counters"), dict)
            else []
        )

        current_totals = self.aggregate_flow_totals_by_udp_port(current_flows)
        previous_totals = self.aggregate_flow_totals_by_udp_port(previous_flows)

        for udp_port, current_total in current_totals.items():
            previous_total = previous_totals.get(udp_port)
            if not previous_total:
                continue
            delta_bytes = current_total["n_bytes"] - previous_total["n_bytes"]
            delta_packets = current_total["n_packets"] - previous_total["n_packets"]
            if delta_bytes < 0 or delta_packets < 0:
                continue
            rates[udp_port] = {
                "bits_per_second": (delta_bytes * 8.0) / delta_seconds,
                "packets_per_second": delta_packets / delta_seconds,
            }
        return rates

    def aggregate_flow_totals_by_udp_port(
        self, flows: Any
    ) -> Dict[str, Dict[str, float]]:
        totals: Dict[str, Dict[str, float]] = {}
        if not isinstance(flows, list):
            return totals
        for flow in flows:
            if not isinstance(flow, dict):
                continue
            udp_port = flow.get("tp_dst")
            bytes_value = to_float(flow.get("n_bytes"))
            packets_value = to_float(flow.get("n_packets"))
            if udp_port is None or bytes_value is None or packets_value is None:
                continue
            key = str(udp_port)
            totals.setdefault(key, {"n_bytes": 0.0, "n_packets": 0.0})
            totals[key]["n_bytes"] += bytes_value
            totals[key]["n_packets"] += packets_value
        return totals

    def find_matching_log(
        self, records: Any, keywords: Iterable[str]
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(records, list):
            return None
        for record in records:
            if not isinstance(record, dict):
                continue
            if matches_keywords(record.get("path"), keywords):
                return record
        return None

    def get_snapshot_service_metrics(
        self, snapshot: Dict[str, Any], service_class: str
    ) -> Dict[str, Any]:
        service_metrics = snapshot.get("service_metrics", {})
        if not isinstance(service_metrics, dict):
            return {}
        metrics = service_metrics.get(service_class, {})
        return metrics if isinstance(metrics, dict) else {}

    def evaluate_service(
        self,
        service_class: str,
        service_cfg: Dict[str, Any],
        snapshot: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        risk_evaluation = self.evaluate_risk_inference_context(
            service_class,
            service_cfg,
            context,
        )
        if risk_evaluation is not None:
            return risk_evaluation
        manufacturing_evaluation = self.evaluate_manufacturing_twin_context(
            service_class,
            service_cfg,
            context,
        )
        if manufacturing_evaluation is not None:
            return manufacturing_evaluation
        if service_class == "real_time_control":
            return self.evaluate_real_time_control(service_cfg, snapshot, context)
        if service_class == "high_throughput_data":
            return self.evaluate_high_throughput_data(service_cfg, snapshot, context)
        if service_class == "sensor_telemetry":
            return self.evaluate_sensor_telemetry(service_cfg, snapshot, context)
        return {
            "target_state": None,
            "condition": f"{service_class}_unsupported",
            "evaluated_metrics": {},
            "explanation": f"No evaluator is defined for service class '{service_class}'.",
        }

    def evaluate_risk_inference_context(
        self,
        service_class: str,
        service_cfg: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        risk_context = context.get("risk_inference", {})
        if not isinstance(risk_context, dict):
            return None
        if (
            not risk_context.get("enabled")
            or not risk_context.get("fresh")
            or risk_context.get("valid_for_policy") is not True
        ):
            return None

        overall_level = normalize_risk_level(risk_context.get("overall_risk_level"))
        service_risks = risk_context.get("service_risk_levels", {})
        service_level = (
            normalize_risk_level(service_risks.get(service_class))
            if isinstance(service_risks, dict)
            else "unknown"
        )
        selected_policy_action = str(
            risk_context.get("selected_policy_action") or self.risk_low_action
        )
        if overall_level == "low" and not risk_is_at_least(service_level, "medium"):
            return None
        if overall_level == "unknown" and not risk_is_at_least(service_level, "medium"):
            return None

        action_phrase = (
            "verify or reinstall"
            if selected_policy_action == self.risk_high_action
            else "verify"
        )
        metrics = strip_none_values(
            {
                "risk_inference_status": risk_context.get("status"),
                "valid_for_policy": risk_context.get("valid_for_policy"),
                "overall_risk_score": risk_context.get("overall_risk_score"),
                "overall_risk_level": overall_level,
                "service_risk_level": service_level,
                "real_time_control_risk": risk_context.get("real_time_control_risk"),
                "high_throughput_data_risk": risk_context.get("high_throughput_data_risk"),
                "sensor_telemetry_risk": risk_context.get("sensor_telemetry_risk"),
                "recommended_policy_action": risk_context.get("recommended_policy_action"),
                "selected_policy_action": selected_policy_action,
                "data_quality_status": risk_context.get("data_quality_status"),
                "queue_rule_presence": risk_context.get("queue_rule_presence"),
                "policy_drift_detected": risk_context.get("policy_drift_detected"),
                "state_age_seconds": risk_context.get("state_age_seconds"),
                "latest_path": risk_context.get("latest_path"),
            }
        )
        return {
            "target_state": selected_policy_action,
            "condition": f"risk_inference_{overall_level}_{service_class}_queue_protection",
            "evaluated_metrics": metrics,
            "explanation": (
                "Predictive SLA Risk Inference Engine reported "
                f"overall_risk_level={overall_level} and {service_class}_risk={service_level}; "
                f"the deterministic policy manager should {action_phrase} queue rules through ONOS_QUEUE_APP."
            ),
        }

    def evaluate_manufacturing_twin_context(
        self,
        service_class: str,
        service_cfg: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        machine_context = context.get("manufacturing_twin", {})
        if not isinstance(machine_context, dict):
            return None
        if not machine_context.get("enabled") or not machine_context.get("fresh"):
            return None

        phase = str(machine_context.get("phase") or "unknown")
        protected_service_classes = service_classes_for_manufacturing_phase(phase)
        if service_class not in protected_service_classes:
            return None

        service_criticality = machine_context.get("service_criticality", {})
        criticality = (
            service_criticality.get(service_class)
            if isinstance(service_criticality, dict)
            else None
        )
        metrics = strip_none_values(
            {
                "manufacturing_phase": phase,
                "machine_availability": machine_context.get("availability"),
                "printer_state_text": machine_context.get("printer_state_text"),
                "job_state": machine_context.get("job_state"),
                "job_progress_percent": machine_context.get("job_progress_percent"),
                "service_criticality": criticality,
                "machine_service_criticality": service_criticality,
                "state_age_seconds": machine_context.get("state_age_seconds"),
                "latest_path": machine_context.get("latest_path"),
            }
        )
        return {
            "target_state": service_cfg["trigger_action_name"],
            "condition": f"manufacturing_twin_{phase}_{service_class}_protection",
            "evaluated_metrics": metrics,
            "explanation": (
                f"Manufacturing twin phase {phase} requires verifying the "
                f"{service_class} queue because its machine criticality is {criticality or 'unspecified'}."
            ),
        }

    def evaluate_real_time_control(
        self,
        service_cfg: Dict[str, Any],
        snapshot: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        service_metrics = self.get_snapshot_service_metrics(snapshot, "real_time_control")
        traffic_refs = snapshot.get("traffic_log_references", {})
        ping_record = self.find_matching_log(
            traffic_refs.get("latest_ping_logs", []), service_cfg.get("ping_log_keywords", [])
        )
        iperf_record = self.find_matching_log(
            traffic_refs.get("latest_iperf_logs", []), service_cfg.get("iperf_log_keywords", [])
        )

        ping_summary = ping_record.get("parsed_summary", {}) if isinstance(ping_record, dict) else {}
        iperf_summary = (
            iperf_record.get("parsed_summary", {}) if isinstance(iperf_record, dict) else {}
        )

        latency_avg = first_numeric(
            service_metrics.get("latency_avg_ms"),
            service_metrics.get("latency_ms"),
            ping_summary.get("rtt_avg_ms"),
        )
        latency_max = first_numeric(
            service_metrics.get("latency_max_ms"),
            ping_summary.get("rtt_max_ms"),
        )
        packet_loss_percent = first_numeric(
            service_metrics.get("packet_loss_percent"),
            ping_summary.get("packet_loss_percent"),
        )
        jitter_ms = first_numeric(
            service_metrics.get("jitter_ms"),
            iperf_summary.get("sum_jitter_ms")
            or iperf_summary.get("sum_received_jitter_ms")
            or iperf_summary.get("sum_sent_jitter_ms")
        )

        metrics = strip_none_values(
            {
                "latency_ms": latency_avg,
                "latency_avg_ms": latency_avg,
                "latency_max_ms": latency_max,
                "packet_loss_percent": packet_loss_percent,
                "jitter_ms": jitter_ms,
                "queue_bytes": first_numeric(service_metrics.get("queue_bytes")),
                "queue_packets": first_numeric(service_metrics.get("queue_packets")),
                "flow_packets_total": first_numeric(service_metrics.get("flow_packets_total")),
                "ping_log_path": service_metrics.get("ping_log_path")
                or (ping_record.get("path") if isinstance(ping_record, dict) else None),
                "iperf_log_path": service_metrics.get("iperf_log_path")
                or (iperf_record.get("path") if isinstance(iperf_record, dict) else None),
                "sources": service_metrics.get("sources"),
            }
        )
        numeric_metrics_available = any(
            value is not None
            for value in (latency_avg, latency_max, packet_loss_percent, jitter_ms)
        )

        threshold_reasons: List[str] = []
        avg_threshold = to_float(service_cfg.get("latency_avg_threshold_ms"))
        max_threshold = to_float(service_cfg.get("latency_max_threshold_ms"))
        loss_threshold = to_float(service_cfg.get("packet_loss_threshold_percent"))
        if avg_threshold is not None and latency_avg is not None and latency_avg > avg_threshold:
            threshold_reasons.append(
                f"average latency {latency_avg:.2f} ms exceeded {avg_threshold:.2f} ms"
            )
        if max_threshold is not None and latency_max is not None and latency_max > max_threshold:
            threshold_reasons.append(
                f"peak latency {latency_max:.2f} ms exceeded {max_threshold:.2f} ms"
            )
        if loss_threshold is not None and packet_loss_percent is not None and packet_loss_percent > loss_threshold:
            threshold_reasons.append(
                f"packet loss {packet_loss_percent:.2f}% exceeded {loss_threshold:.2f}%"
            )

        if threshold_reasons:
            return {
                "target_state": service_cfg["trigger_action_name"],
                "condition": "real_time_control_latency_degraded",
                "evaluated_metrics": metrics,
                "explanation": (
                    "Real-time control protection is recommended because "
                    + "; ".join(threshold_reasons)
                    + "."
                ),
            }

        if numeric_metrics_available:
            return {
                "target_state": self.default_action,
                "condition": "real_time_control_healthy",
                "evaluated_metrics": metrics,
                "explanation": (
                    "Real-time control metrics are within threshold, so the current policy can remain in place."
                ),
            }

        return {
            "target_state": None,
            "condition": "real_time_control_insufficient_telemetry",
            "evaluated_metrics": strip_none_values(
                {
                    "ping_log_path": service_metrics.get("ping_log_path"),
                    "iperf_log_path": service_metrics.get("iperf_log_path"),
                    "sources": service_metrics.get("sources"),
                }
            ),
            "explanation": (
                "No matching real-time control latency or jitter evidence was available, so the manager will hold its current recommendation."
            ),
        }

    def evaluate_high_throughput_data(
        self,
        service_cfg: Dict[str, Any],
        snapshot: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        service_metrics = self.get_snapshot_service_metrics(snapshot, "high_throughput_data")
        traffic_refs = snapshot.get("traffic_log_references", {})
        iperf_record = self.find_matching_log(
            traffic_refs.get("latest_iperf_logs", []), service_cfg.get("iperf_log_keywords", [])
        )
        iperf_summary = (
            iperf_record.get("parsed_summary", {}) if isinstance(iperf_record, dict) else {}
        )

        configured_port = str(int(service_cfg.get("traffic_udp_port")))
        flow_rate = context.get("flow_rates", {}).get(configured_port, {})
        throughput_mbps = first_numeric(service_metrics.get("throughput_mbps"))
        throughput_bps = first_numeric(
            service_metrics.get("throughput_bps"),
            throughput_mbps * 1_000_000.0 if throughput_mbps is not None else None,
            iperf_summary.get("sum_bits_per_second")
            or iperf_summary.get("sum_sent_bits_per_second")
            or iperf_summary.get("sum_received_bits_per_second"),
            flow_rate.get("bits_per_second"),
        )
        throughput_mbps = (
            throughput_bps / 1_000_000.0 if throughput_bps is not None else throughput_mbps
        )
        retransmits = first_numeric(
            service_metrics.get("retransmits"),
            iperf_summary.get("sum_retransmits")
            or iperf_summary.get("sum_sent_retransmits")
            or iperf_summary.get("sum_received_retransmits"),
        )

        queue_rate = self.aggregate_queue_rates(
            context.get("queue_rates", {}),
            list_of_strings(service_cfg.get("monitored_queue_ids")),
        )
        queue_bytes_per_second = first_numeric(
            service_metrics.get("queue_bytes_per_second"),
            queue_rate.get("bytes_per_second"),
        )
        queue_packets_per_second = first_numeric(
            service_metrics.get("queue_packets_per_second"),
            queue_rate.get("packets_per_second"),
        )
        queue_bytes = first_numeric(
            service_metrics.get("queue_bytes"),
            service_metrics.get("queue_occupancy_bytes"),
        )

        metrics = strip_none_values(
            {
                "throughput_mbps": throughput_mbps,
                "throughput_bps": throughput_bps,
                "retransmits": retransmits,
                "queue_bytes": queue_bytes,
                "queue_occupancy_bytes_per_second_proxy": queue_bytes_per_second,
                "queue_packets_per_second": queue_packets_per_second,
                "iperf_log_path": service_metrics.get("iperf_log_path")
                or (iperf_record.get("path") if isinstance(iperf_record, dict) else None),
                "flow_udp_port": configured_port,
                "queue_ids": service_metrics.get("queue_ids"),
                "sources": service_metrics.get("sources"),
            }
        )
        numeric_metrics_available = any(
            value is not None
            for value in (
                throughput_mbps,
                throughput_bps,
                retransmits,
                queue_bytes,
                queue_bytes_per_second,
                queue_packets_per_second,
            )
        )

        threshold_reasons: List[str] = []
        throughput_threshold = to_float(service_cfg.get("throughput_threshold_bps"))
        queue_threshold = to_float(
            service_cfg.get("queue_occupancy_threshold_bytes_per_second")
        )
        packet_rate_threshold = to_float(service_cfg.get("queue_packets_threshold_per_second"))
        if throughput_threshold is not None and throughput_bps is not None and throughput_bps > throughput_threshold:
            threshold_reasons.append(
                f"throughput {throughput_bps:.2f} bps exceeded {throughput_threshold:.2f} bps"
            )
        if queue_threshold is not None and queue_bytes_per_second is not None and queue_bytes_per_second > queue_threshold:
            threshold_reasons.append(
                f"queue pressure proxy {queue_bytes_per_second:.2f} B/s exceeded {queue_threshold:.2f} B/s"
            )
        if packet_rate_threshold is not None and queue_packets_per_second is not None and queue_packets_per_second > packet_rate_threshold:
            threshold_reasons.append(
                f"queue packet rate {queue_packets_per_second:.2f} pkt/s exceeded {packet_rate_threshold:.2f} pkt/s"
            )

        if threshold_reasons:
            return {
                "target_state": service_cfg["trigger_action_name"],
                "condition": "high_throughput_data_pressure_detected",
                "evaluated_metrics": metrics,
                "explanation": (
                    "High-throughput data shaping should be tightened because "
                    + "; ".join(threshold_reasons)
                    + "."
                ),
            }

        if numeric_metrics_available:
            return {
                "target_state": self.default_action,
                "condition": "high_throughput_data_healthy",
                "evaluated_metrics": metrics,
                "explanation": (
                    "High-throughput data demand is within threshold, so the current data policy can remain unchanged."
                ),
            }

        return {
            "target_state": None,
            "condition": "high_throughput_data_insufficient_telemetry",
            "evaluated_metrics": strip_none_values(
                {
                    "iperf_log_path": service_metrics.get("iperf_log_path"),
                    "queue_ids": service_metrics.get("queue_ids"),
                    "sources": service_metrics.get("sources"),
                }
            ),
            "explanation": (
                "No matching high-throughput data metrics were available, so the manager will hold its current recommendation."
            ),
        }

    def evaluate_sensor_telemetry(
        self,
        service_cfg: Dict[str, Any],
        snapshot: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        service_metrics = self.get_snapshot_service_metrics(snapshot, "sensor_telemetry")
        traffic_refs = snapshot.get("traffic_log_references", {})
        iperf_record = self.find_matching_log(
            traffic_refs.get("latest_iperf_logs", []), service_cfg.get("iperf_log_keywords", [])
        )
        iperf_summary = (
            iperf_record.get("parsed_summary", {}) if isinstance(iperf_record, dict) else {}
        )

        loss_percent = first_numeric(
            service_metrics.get("packet_loss_percent"),
            iperf_summary.get("sum_lost_percent")
            or iperf_summary.get("sum_sent_lost_percent")
            or iperf_summary.get("sum_received_lost_percent"),
        )
        delivery_ratio_percent = first_numeric(
            service_metrics.get("packet_delivery_ratio_percent"),
            service_metrics.get("success_ratio_percent"),
            self.compute_delivery_ratio_percent(iperf_summary),
        )
        interface_drop_rate = first_numeric(
            service_metrics.get("drop_rate_percent"),
            self.aggregate_interface_drop_rate(
                context.get("interface_rates", {}),
                list_of_strings(service_cfg.get("monitored_interface_names")),
            ),
        )
        rx_packets = first_numeric(service_metrics.get("rx_packets"))
        tx_packets = first_numeric(service_metrics.get("tx_packets"))

        metrics = strip_none_values(
            {
                "loss_percent": loss_percent,
                "delivery_ratio_percent": delivery_ratio_percent,
                "drop_rate_percent": interface_drop_rate,
                "rx_packets": rx_packets,
                "tx_packets": tx_packets,
                "iperf_log_path": service_metrics.get("iperf_log_path")
                or (iperf_record.get("path") if isinstance(iperf_record, dict) else None),
                "interface_names": service_metrics.get("interface_names"),
                "sources": service_metrics.get("sources"),
            }
        )
        numeric_metrics_available = any(
            value is not None
            for value in (
                loss_percent,
                delivery_ratio_percent,
                interface_drop_rate,
                rx_packets,
                tx_packets,
            )
        )

        threshold_reasons: List[str] = []
        loss_threshold = to_float(service_cfg.get("loss_threshold_percent"))
        delivery_threshold = to_float(service_cfg.get("delivery_ratio_threshold_percent"))
        drop_rate_threshold = to_float(service_cfg.get("drop_rate_threshold_percent"))
        if loss_threshold is not None and loss_percent is not None and loss_percent > loss_threshold:
            threshold_reasons.append(
                f"loss {loss_percent:.2f}% exceeded {loss_threshold:.2f}%"
            )
        if delivery_threshold is not None and delivery_ratio_percent is not None and delivery_ratio_percent < delivery_threshold:
            threshold_reasons.append(
                f"delivery ratio {delivery_ratio_percent:.2f}% fell below {delivery_threshold:.2f}%"
            )
        if drop_rate_threshold is not None and interface_drop_rate is not None and interface_drop_rate > drop_rate_threshold:
            threshold_reasons.append(
                f"drop rate {interface_drop_rate:.2f}% exceeded {drop_rate_threshold:.2f}%"
            )

        if threshold_reasons:
            return {
                "target_state": service_cfg["trigger_action_name"],
                "condition": "sensor_telemetry_reliability_degraded",
                "evaluated_metrics": metrics,
                "explanation": (
                    "Sensor telemetry minimum-bandwidth protection is recommended because "
                    + "; ".join(threshold_reasons)
                    + "."
                ),
            }

        if numeric_metrics_available:
            return {
                "target_state": self.default_action,
                "condition": "sensor_telemetry_healthy",
                "evaluated_metrics": metrics,
                "explanation": (
                    "Sensor telemetry reliability is within threshold, so the current policy can remain in place."
                ),
            }

        return {
            "target_state": None,
            "condition": "sensor_telemetry_insufficient_telemetry",
            "evaluated_metrics": strip_none_values(
                {
                    "iperf_log_path": service_metrics.get("iperf_log_path"),
                    "interface_names": service_metrics.get("interface_names"),
                    "sources": service_metrics.get("sources"),
                }
            ),
            "explanation": (
                "No matching sensor telemetry reliability evidence was available, so the manager will hold its current recommendation."
            ),
        }

    def aggregate_queue_rates(
        self, queue_rates: Any, queue_ids: List[str]
    ) -> Dict[str, Optional[float]]:
        totals = {"bytes_per_second": 0.0, "packets_per_second": 0.0}
        found = False
        if not isinstance(queue_rates, dict):
            return {}
        for key, record in queue_rates.items():
            if not isinstance(record, dict):
                continue
            queue_id = ""
            if isinstance(key, tuple) and len(key) == 2:
                queue_id = str(key[1])
            if queue_ids and queue_id not in queue_ids:
                continue
            bytes_per_second = to_float(record.get("bytes_per_second"))
            packets_per_second = to_float(record.get("packets_per_second"))
            if bytes_per_second is None and packets_per_second is None:
                continue
            totals["bytes_per_second"] += bytes_per_second or 0.0
            totals["packets_per_second"] += packets_per_second or 0.0
            found = True
        return totals if found else {}

    def aggregate_interface_drop_rate(
        self, interface_rates: Any, interface_names: List[str]
    ) -> Optional[float]:
        if not isinstance(interface_rates, dict):
            return None
        selected = interface_names or list(interface_rates.keys())
        total_attempted = 0.0
        total_drops = 0.0
        for interface_name in selected:
            record = interface_rates.get(interface_name)
            if not isinstance(record, dict):
                continue
            tx_packets_per_second = to_float(record.get("tx_packets_per_second"))
            tx_drops_per_second = to_float(record.get("tx_drops_per_second"))
            if tx_packets_per_second is None or tx_drops_per_second is None:
                continue
            total_attempted += tx_packets_per_second + tx_drops_per_second
            total_drops += tx_drops_per_second
        if total_attempted <= 0:
            return None
        return (total_drops / total_attempted) * 100.0

    def compute_delivery_ratio_percent(self, iperf_summary: Any) -> Optional[float]:
        if not isinstance(iperf_summary, dict):
            return None
        sent_bytes = to_float(
            iperf_summary.get("sum_sent_bytes") or iperf_summary.get("sum_bytes")
        )
        received_bytes = to_float(
            iperf_summary.get("sum_received_bytes") or iperf_summary.get("sum_bytes")
        )
        if sent_bytes is not None and received_bytes is not None and sent_bytes > 0:
            ratio = (received_bytes / sent_bytes) * 100.0
            if ratio > 100.0:
                return 100.0
            return ratio
        loss_percent = to_float(
            iperf_summary.get("sum_lost_percent")
            or iperf_summary.get("sum_sent_lost_percent")
            or iperf_summary.get("sum_received_lost_percent")
        )
        if loss_percent is None:
            return None
        return max(0.0, 100.0 - loss_percent)

    def resolve_decision(
        self,
        service_class: str,
        service_cfg: Dict[str, Any],
        evaluation: Dict[str, Any],
        telemetry_reference: Dict[str, Any],
        snapshot_time: Optional[dt.datetime],
    ) -> Optional[Dict[str, Any]]:
        now = snapshot_time or dt.datetime.now(dt.timezone.utc)
        state = self.service_state[service_class]
        stable_action = str(state.get("effective_action") or self.default_action)
        target_state = evaluation.get("target_state")
        condition = str(evaluation.get("condition") or "unknown_condition")
        metrics = evaluation.get("evaluated_metrics", {})
        explanation = str(evaluation.get("explanation") or "")

        # This manager emits stabilized decisions for the external enforcement
        # manager. It does not talk to ONOS or OVS directly.
        if state.get("last_emitted_action") is None:
            if target_state is None:
                recommended_action = stable_action
                state_after = stable_action
                decision_state = "new"
                is_new = True
                explanation = explanation + " Initializing the manager in hold mode."
            elif str(target_state) == self.default_action and stable_action != self.default_action:
                recommended_action = self.restore_action
                state_after = self.default_action
                state["effective_action"] = self.default_action
                decision_state = "new"
                is_new = True
            else:
                recommended_action = str(target_state)
                state_after = str(target_state)
                state["effective_action"] = state_after
                decision_state = "new"
                is_new = True
            return self.build_decision_record(
                service_class=service_class,
                service_cfg=service_cfg,
                telemetry_reference=telemetry_reference,
                condition=condition,
                metrics=metrics,
                explanation=explanation,
                recommended_action=recommended_action,
                decision_state=decision_state,
                is_new_decision=is_new,
                state_after_decision=state_after,
                candidate_action=None,
                cooldown_status=None,
            )

        last_emitted_at = state.get("last_emitted_at")
        unchanged_due = self.emit_unchanged_decisions and (
            last_emitted_at is None
            or (now - last_emitted_at).total_seconds() >= self.unchanged_log_interval_seconds
        )

        if target_state is None:
            state["pending_target_state"] = None
            state["pending_condition"] = None
            state["pending_since"] = None
            state["pending_logged"] = False
            if condition != state.get("last_emitted_condition") or unchanged_due:
                return self.build_decision_record(
                    service_class=service_class,
                    service_cfg=service_cfg,
                    telemetry_reference=telemetry_reference,
                    condition=condition,
                    metrics=metrics,
                    explanation=explanation,
                    recommended_action=stable_action,
                    decision_state="unchanged",
                    is_new_decision=False,
                    state_after_decision=stable_action,
                    candidate_action=None,
                    cooldown_status=None,
                )
            return None

        target_state = str(target_state)
        if target_state == stable_action:
            state["pending_target_state"] = None
            state["pending_condition"] = None
            state["pending_since"] = None
            state["pending_logged"] = False
            if condition != state.get("last_emitted_condition") or unchanged_due:
                return self.build_decision_record(
                    service_class=service_class,
                    service_cfg=service_cfg,
                    telemetry_reference=telemetry_reference,
                    condition=condition,
                    metrics=metrics,
                    explanation=explanation,
                    recommended_action=stable_action,
                    decision_state="unchanged",
                    is_new_decision=False,
                    state_after_decision=stable_action,
                    candidate_action=None,
                    cooldown_status=None,
                )
            return None

        pending_target = state.get("pending_target_state")
        if pending_target != target_state:
            state["pending_target_state"] = target_state
            state["pending_condition"] = condition
            state["pending_since"] = now
            state["pending_logged"] = False

        pending_since = state.get("pending_since") or now
        cooldown_elapsed = (now - pending_since).total_seconds()
        candidate_action = (
            self.restore_action
            if target_state == self.default_action and stable_action != self.default_action
            else target_state
        )

        if cooldown_elapsed >= self.decision_cooldown_seconds:
            state["pending_target_state"] = None
            state["pending_condition"] = None
            state["pending_since"] = None
            state["pending_logged"] = False
            state_after = self.default_action if candidate_action == self.restore_action else target_state
            state["effective_action"] = state_after
            return self.build_decision_record(
                service_class=service_class,
                service_cfg=service_cfg,
                telemetry_reference=telemetry_reference,
                condition=condition,
                metrics=metrics,
                explanation=explanation,
                recommended_action=candidate_action,
                decision_state="new",
                is_new_decision=True,
                state_after_decision=state_after,
                candidate_action=None,
                cooldown_status={
                    "cooldown_seconds": self.decision_cooldown_seconds,
                    "cooldown_elapsed_seconds": cooldown_elapsed,
                },
            )

        if state.get("pending_logged"):
            return None

        state["pending_logged"] = True
        hold_explanation = (
            explanation
            + f" Candidate action {candidate_action} is being held until the {self.decision_cooldown_seconds:.1f}s cooldown expires."
        )
        return self.build_decision_record(
            service_class=service_class,
            service_cfg=service_cfg,
            telemetry_reference=telemetry_reference,
            condition=f"{condition}_cooldown_hold",
            metrics=metrics,
            explanation=hold_explanation,
            recommended_action=stable_action,
            decision_state="unchanged",
            is_new_decision=False,
            state_after_decision=stable_action,
            candidate_action=candidate_action,
            cooldown_status={
                "cooldown_seconds": self.decision_cooldown_seconds,
                "cooldown_elapsed_seconds": cooldown_elapsed,
                "pending_since": pending_since.isoformat(timespec="seconds"),
                "pending_target_state": target_state,
            },
        )

    def build_decision_record(
        self,
        service_class: str,
        service_cfg: Dict[str, Any],
        telemetry_reference: Dict[str, Any],
        condition: str,
        metrics: Any,
        explanation: str,
        recommended_action: str,
        decision_state: str,
        is_new_decision: bool,
        state_after_decision: str,
        candidate_action: Optional[str],
        cooldown_status: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        machine_context = self.current_manufacturing_context or {}
        risk_context = self.current_risk_context or {}
        manufacturing_twin_enabled = bool(machine_context.get("enabled"))
        manufacturing_twin_status = str(machine_context.get("status") or "disabled")
        manufacturing_phase = str(machine_context.get("phase") or "disabled")
        machine_availability = machine_context.get("availability")
        printer_state_text = machine_context.get("printer_state_text")
        job_state = machine_context.get("job_state")
        job_progress_percent = machine_context.get("job_progress_percent")
        machine_service_criticality = machine_context.get("service_criticality", {})
        machine_state = printer_state_text or machine_availability
        risk_inference_enabled = bool(risk_context.get("enabled"))
        risk_inference_status = str(risk_context.get("status") or "disabled")
        valid_for_policy = bool(risk_context.get("valid_for_policy"))
        overall_risk_score = risk_context.get("overall_risk_score")
        overall_risk_level = str(risk_context.get("overall_risk_level") or "disabled")
        real_time_control_risk = risk_context.get("real_time_control_risk")
        high_throughput_data_risk = risk_context.get("high_throughput_data_risk")
        sensor_telemetry_risk = risk_context.get("sensor_telemetry_risk")
        risk_recommended_policy_action = risk_context.get("recommended_policy_action")
        selected_policy_action = (
            risk_context.get("selected_policy_action")
            if risk_inference_enabled and valid_for_policy
            else recommended_action
        )
        decision_reason = explanation
        if manufacturing_twin_enabled:
            phase_reason = (
                f" Manufacturing twin context: phase={manufacturing_phase}, "
                f"machine_state={machine_state or 'unknown'}."
            )
            if f"phase={manufacturing_phase}" not in decision_reason:
                decision_reason += phase_reason
        if risk_inference_enabled:
            risk_reason = (
                " Predictive SLA Risk Inference Engine context: "
                f"risk_inference_status={risk_inference_status}, "
                f"valid_for_policy={str(valid_for_policy).lower()}, "
                f"overall_risk_score={overall_risk_score}, "
                f"overall_risk_level={overall_risk_level}, "
                f"selected_policy_action={selected_policy_action or 'none'}."
            )
            if f"risk_inference_status={risk_inference_status}" not in decision_reason:
                decision_reason += risk_reason
        applied = bool(is_new_decision and not self.dry_run_only)
        enforcement_path = (
            "ONOS_QUEUE_APP"
            if applied or risk_inference_enabled
            else None
        )
        enforcement_status = "pending_ONOS_QUEUE_APP" if applied else "not_applied"
        log_summary = (
            f"service_class={service_class} "
            f"risk_inference_enabled={str(risk_inference_enabled).lower()} "
            f"risk_inference_status={risk_inference_status} "
            f"valid_for_policy={str(valid_for_policy).lower()} "
            f"overall_risk_score={overall_risk_score} "
            f"overall_risk_level={overall_risk_level} "
            f"real_time_control_risk={real_time_control_risk} "
            f"high_throughput_data_risk={high_throughput_data_risk} "
            f"sensor_telemetry_risk={sensor_telemetry_risk} "
            f"recommended_policy_action={risk_recommended_policy_action or 'none'} "
            f"selected_policy_action={selected_policy_action or 'none'} "
            f"manufacturing_twin_status={manufacturing_twin_status} "
            f"phase={manufacturing_phase} "
            f"machine_state={machine_state or 'unknown'} "
            f"service_criticality={machine_service_criticality} "
            f"enforcement_status={enforcement_status} "
            f"applied={str(applied).lower()}"
        )
        if enforcement_path:
            log_summary += f" enforcement_path={enforcement_path}"
        record = {
            "event_type": "policy_decision",
            "timestamp": utc_timestamp(),
            "run_id": self.run_id,
            "service_class": service_class,
            "policy_name": service_cfg.get("policy_name"),
            "latest_telemetry_snapshot_reference": telemetry_reference,
            "evaluated_metrics": metrics if isinstance(metrics, dict) else {},
            "detected_condition": condition,
            "recommended_action": recommended_action,
            "candidate_action": candidate_action,
            "state_after_decision": state_after_decision,
            "decision_state": decision_state,
            "is_new_decision": is_new_decision,
            "explanation": explanation,
            "decision_reason": decision_reason,
            "dry_run_only": self.dry_run_only,
            "mode": "dry-run" if self.dry_run_only else "active",
            "no_live_enforcement": self.dry_run_only,
            "external_enforcement_only": True,
            "cooldown_status": cooldown_status,
            "risk_inference_enabled": risk_inference_enabled,
            "risk_inference_status": risk_inference_status,
            "valid_for_policy": valid_for_policy,
            "overall_risk_score": overall_risk_score,
            "overall_risk_level": overall_risk_level,
            "real_time_control_risk": real_time_control_risk,
            "high_throughput_data_risk": high_throughput_data_risk,
            "sensor_telemetry_risk": sensor_telemetry_risk,
            "recommended_policy_action": risk_recommended_policy_action,
            "selected_policy_action": selected_policy_action,
            "risk_inference_data_quality_status": risk_context.get("data_quality_status"),
            "queue_rule_presence": risk_context.get("queue_rule_presence"),
            "policy_drift_detected": risk_context.get("policy_drift_detected"),
            "risk_prediction_fresh": bool(risk_context.get("fresh")),
            "risk_prediction_state_age_seconds": risk_context.get("state_age_seconds"),
            "risk_prediction_path": risk_context.get("latest_path"),
            "risk_inference_error": risk_context.get("error"),
            "manufacturing_twin_enabled": manufacturing_twin_enabled,
            "manufacturing_twin_status": manufacturing_twin_status,
            "manufacturing_phase": manufacturing_phase,
            "machine_availability": machine_availability,
            "printer_state_text": printer_state_text,
            "job_state": job_state,
            "job_progress_percent": job_progress_percent,
            "service_criticality": machine_service_criticality,
            "machine_service_criticality": machine_service_criticality,
            "machine_state": machine_state,
            "machine_twin_fresh": bool(machine_context.get("fresh")),
            "machine_twin_state_age_seconds": machine_context.get("state_age_seconds"),
            "machine_twin_latest_path": machine_context.get("latest_path"),
            "machine_twin_error": machine_context.get("error"),
            "applied": applied,
            "enforcement_path": enforcement_path,
            "enforcement_status": enforcement_status,
            "decision_log_summary": log_summary,
        }
        service_state = self.service_state[service_class]
        service_state["last_emitted_action"] = recommended_action
        service_state["last_emitted_state"] = state_after_decision
        service_state["last_emitted_condition"] = condition
        service_state["last_emitted_at"] = parse_timestamp(record["timestamp"])
        service_state["last_decision_state"] = decision_state
        return record


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Slice Policy Manager that reads telemetry JSONL and logs recommendations."
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name(DEFAULT_CONFIG_NAME)),
        help="Path to the policy manager config file.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process the currently available telemetry snapshots once and exit.",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help=f"Prometheus metrics HTTP port override. Default comes from config or {DEFAULT_METRICS_HTTP_PORT}.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Force dry-run mode even if the config enables active enforcement.",
    )
    mode.add_argument(
        "--active",
        "--live",
        dest="active",
        action="store_true",
        help="Force active/live mode even if the config defaults to dry-run.",
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
    if args.metrics_port is not None:
        config["metrics_http_port"] = int(args.metrics_port)

    dry_run_override: Optional[bool]
    if args.dry_run:
        dry_run_override = True
    elif args.active:
        dry_run_override = False
    else:
        dry_run_override = None

    manager = PolicyManager(
        config_path=config_path,
        config=config,
        run_once=args.once,
        dry_run_override=dry_run_override,
    )
    return manager.run()


if __name__ == "__main__":
    sys.exit(main())
