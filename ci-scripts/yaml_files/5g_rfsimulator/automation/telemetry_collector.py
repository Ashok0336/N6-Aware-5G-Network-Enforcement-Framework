#!/usr/bin/env python3
"""Periodic telemetry collector for the OAI + ONOS + OVS N6 slicing testbed."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from service_mapping_utils import apply_service_mapping

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


DEFAULT_CONFIG_NAME = "config.yaml"
DEFAULT_LOG_DIR = "../logs/telemetry"
DEFAULT_TRAFFIC_LOG_DIR = "../logs"
DEFAULT_POLL_INTERVAL_SECONDS = 2
DEFAULT_COMMAND_TIMEOUT_SECONDS = 5

PORT_DESC_RE = re.compile(r"^\s*(LOCAL|\d+)(?:\(([^)]+)\))?:\s*(.*)$")
RX_PORT_STATS_RE = re.compile(
    r"^\s*(LOCAL|\d+):\s+rx pkts=(\d+), bytes=(\d+), drop=(\d+), errs=(\d+), frame=(\d+), over=(\d+), crc=(\d+)"
)
TX_PORT_STATS_RE = re.compile(r"^\s*tx pkts=(\d+), bytes=(\d+), drop=(\d+), errs=(\d+), coll=(\d+)")
QUEUE_STATS_RE = re.compile(
    r"port\s+(\S+),\s+queue\s+(\S+),\s+bytes\s+(\d+),\s+pkts\s+(\d+),\s+errors\s+(\d+),\s+duration\s+([0-9.]+)s"
)
PING_SUMMARY_RE = re.compile(
    r"(\d+)\s+packets transmitted,\s+(\d+)\s+(?:packets )?received,.*?(\d+(?:\.\d+)?)%\s+packet loss"
)
PING_REPLY_RTT_RE = re.compile(r"time=(\d+(?:\.\d+)?)\s*ms")
PING_RTT_RE = re.compile(
    r"(?:round-trip|rtt) min/avg/max/(?:mdev|stddev)\s*=\s*([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)\s*ms"
)
SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMGTP]?i?B)\s*$", re.IGNORECASE)
BITRATE_RE = re.compile(
    r"^\s*([0-9]*\.?[0-9]+)\s*([KMGTP]?)(?:bits/sec|bit/s)\s*$",
    re.IGNORECASE,
)
IPERF_UDP_TEXT_RE = re.compile(
    r"(?P<transfer>[0-9]*\.?[0-9]+)\s+(?P<transfer_unit>[KMGTP]?Bytes?)\s+"
    r"(?P<bitrate>[0-9]*\.?[0-9]+)\s+(?P<bitrate_unit>[KMGTP]?bits/sec)\s+"
    r"(?P<jitter>[0-9]*\.?[0-9]+)\s+ms\s+"
    r"(?P<lost>\d+)/(?P<packets>\d+)\s+\((?P<lost_percent>[0-9]*\.?[0-9]+)%\)"
    r"(?:\s+(?P<role>sender|receiver))?",
    re.IGNORECASE,
)
IPERF_TCP_TEXT_RE = re.compile(
    r"(?P<transfer>[0-9]*\.?[0-9]+)\s+(?P<transfer_unit>[KMGTP]?Bytes?)\s+"
    r"(?P<bitrate>[0-9]*\.?[0-9]+)\s+(?P<bitrate_unit>[KMGTP]?bits/sec)"
    r"(?:\s+(?P<retransmits>\d+))?"
    r"(?:\s+(?P<role>sender|receiver))?",
    re.IGNORECASE,
)
UDP_SENDER_SUMMARY_RE = re.compile(r"^SUMMARY\s+(.*)$", re.MULTILINE)


def utc_timestamp() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def print_console(level: str, message: str) -> None:
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[telemetry][{stamp}][{level}] {message}", flush=True)


def parse_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if text.lower() in {"none", "null"}:
        return None
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            return text
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            return text
    return text


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
        if char in "[{(":
            depth += 1
        elif char in "]})" and depth > 0:
            depth -= 1
        if char == delimiter and depth == 0:
            part = "".join(current).strip()
            if part:
                items.append(part)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def parse_ovs_value(text: str) -> Any:
    value = text.strip()
    if not value:
        return ""
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        result: Dict[str, Any] = {}
        for item in split_top_level(inner):
            if "=" not in item:
                result[item.strip()] = True
                continue
            key, raw = item.split("=", 1)
            result[key.strip()] = parse_ovs_value(raw)
        return result
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [parse_ovs_value(item) for item in split_top_level(inner)]
    return parse_scalar(value)


def parse_ovs_records(text: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            if current:
                records.append(current)
                current = {}
            continue
        if ":" not in line:
            continue
        key, raw_value = line.split(":", 1)
        current[key.strip()] = parse_ovs_value(raw_value)
    if current:
        records.append(current)
    return records


def parse_human_size(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    cleaned = text.strip()
    if cleaned in {"", "0", "0B"}:
        return 0
    match = SIZE_RE.match(cleaned)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).upper()
    factors = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
    }
    factor = factors.get(unit)
    if factor is None:
        return None
    return int(number * factor)


def parse_usage_pair(text: Optional[str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"raw": text or ""}
    if not text or "/" not in text:
        return result
    used, limit = [part.strip() for part in text.split("/", 1)]
    result["used"] = used
    result["limit"] = limit
    result["used_bytes"] = parse_human_size(used)
    result["limit_bytes"] = parse_human_size(limit)
    return result


def parse_percent(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    stripped = text.strip().rstrip("%")
    try:
        return float(stripped)
    except ValueError:
        return None


def compact_dict(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def list_of_strings(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


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


def parse_human_bitrate(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    match = BITRATE_RE.match(text.strip())
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).upper()
    factors = {
        "": 1.0,
        "K": 1000.0,
        "M": 1000.0**2,
        "G": 1000.0**3,
        "T": 1000.0**4,
        "P": 1000.0**5,
    }
    return number * factors.get(unit, 1.0)


def normalize_path(base_dir: Path, value: str) -> str:
    return str((base_dir / value).resolve())


def load_config(config_path: Path) -> Dict[str, Any]:
    raw_text = config_path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {config_path} must define a top-level mapping/object.")
    return loaded


def normalize_config(config_path: Path, raw_config: Dict[str, Any]) -> Dict[str, Any]:
    base_dir = config_path.parent
    config = dict(raw_config)
    env_onos_host = os.getenv("ONOS_HOST", "").strip() or os.getenv("ONOS_REST_HOST", "").strip()
    env_onos_rest_port = os.getenv("ONOS_REST_PORT", "").strip() or "8181"
    default_onos_base = (
        f"http://{env_onos_host}:{env_onos_rest_port}" if env_onos_host else "http://192.168.71.160:8181"
    )
    env_onos_devices_url = os.getenv("ONOS_DEVICES_URL", "").strip()
    env_onos_base_url = os.getenv("ONOS_BASE_URL", "").strip()
    env_onos_username = os.getenv("ONOS_USERNAME", "").strip()
    env_onos_password = os.getenv("ONOS_PASSWORD", "").strip()
    env_ovs_container_name = os.getenv("OVS_CONTAINER_NAME", "").strip()
    env_ovs_bridge_name = os.getenv("OVS_BRIDGE_NAME", "").strip()

    config.setdefault("polling_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
    config.setdefault("command_timeout_seconds", DEFAULT_COMMAND_TIMEOUT_SECONDS)
    config.setdefault("ovs_container_name", "ovs")
    config.setdefault("ovs_bridge_name", "br-n6")
    config.setdefault("monitored_ports", ["v-upf-host", "v-edn-host"])
    config.setdefault("monitored_interfaces", ["v-upf-host", "v-edn-host"])
    config.setdefault("log_dir", DEFAULT_LOG_DIR)
    config.setdefault(
        "optional_container_names",
        [
            "ovs",
            "onos",
            "rfsim5g-oai-upf",
            "rfsim5g-oai-ext-dn",
            "rfsim5g-oai-nr-ue",
            "rfsim5g-oai-nr-ue2",
            "rfsim5g-oai-nr-ue3",
        ],
    )
    config.setdefault("onos_base_url", default_onos_base.rstrip("/") + "/onos/v1/devices")
    config.setdefault("onos_username", "onos")
    config.setdefault("onos_password", "rocks")
    config.setdefault("onos_timeout_seconds", 3)
    if env_ovs_container_name:
        config["ovs_container_name"] = env_ovs_container_name
    if env_ovs_bridge_name:
        config["ovs_bridge_name"] = env_ovs_bridge_name
    if env_onos_devices_url:
        config["onos_base_url"] = env_onos_devices_url
    elif env_onos_base_url:
        config["onos_base_url"] = env_onos_base_url.rstrip("/") + "/onos/v1/devices"
    if env_onos_username:
        config["onos_username"] = env_onos_username
    if env_onos_password:
        config["onos_password"] = env_onos_password
    config.setdefault("traffic_log_search_dirs", [DEFAULT_TRAFFIC_LOG_DIR])
    config.setdefault("traffic_log_globs", ["iperf3-*.json", "iperf3-*.log", "ping-*.log"])
    config.setdefault(
        "service_classes",
        {
            "real_time_control": {
                "udp_port": 5202,
                "queue_ids": ["2"],
                "interface_names": ["v-edn-host"],
                "container_names": ["rfsim5g-oai-nr-ue2"],
                "log_search_dirs": [DEFAULT_TRAFFIC_LOG_DIR],
                "ping_log_keywords": ["urllc", "control"],
                "iperf_log_keywords": ["urllc", "control"],
            },
            "high_throughput_data": {
                "udp_port": 5201,
                "queue_ids": ["1"],
                "interface_names": ["v-edn-host"],
                "container_names": ["rfsim5g-oai-nr-ue"],
                "log_search_dirs": [DEFAULT_TRAFFIC_LOG_DIR],
                "ping_log_keywords": ["embb", "data"],
                "iperf_log_keywords": ["embb", "data"],
            },
            "sensor_telemetry": {
                "udp_port": 5203,
                "queue_ids": ["3"],
                "interface_names": ["v-edn-host"],
                "container_names": ["rfsim5g-oai-nr-ue3"],
                "log_search_dirs": [DEFAULT_TRAFFIC_LOG_DIR],
                "ping_log_keywords": ["mmtc", "sensor"],
                "iperf_log_keywords": ["mmtc", "sensor"],
            },
        },
    )
    config = apply_service_mapping(base_dir, config)

    config["polling_interval_seconds"] = float(config["polling_interval_seconds"])
    config["command_timeout_seconds"] = int(config["command_timeout_seconds"])
    config["onos_timeout_seconds"] = float(config["onos_timeout_seconds"])
    config["monitored_ports"] = [str(item) for item in config.get("monitored_ports", [])]
    config["monitored_interfaces"] = [
        str(item) for item in config.get("monitored_interfaces", [])
    ]
    config["optional_container_names"] = [
        str(item) for item in config.get("optional_container_names", [])
    ]
    config["traffic_log_globs"] = [str(item) for item in config.get("traffic_log_globs", [])]

    config["log_dir"] = normalize_path(base_dir, str(config["log_dir"]))
    config["traffic_log_search_dirs"] = [
        normalize_path(base_dir, str(item))
        for item in config.get("traffic_log_search_dirs", [])
    ]

    normalized_service_classes: Dict[str, Dict[str, Any]] = {}
    for service_name, service_cfg in config.get("service_classes", {}).items():
        if not isinstance(service_cfg, dict):
            raise ValueError(f"service_classes.{service_name} must be a mapping/object.")
        entry = dict(service_cfg)
        entry.setdefault("udp_port", None)
        entry.setdefault("queue_ids", [])
        entry.setdefault("interface_names", [])
        entry.setdefault("container_names", [])
        entry.setdefault("log_search_dirs", config["traffic_log_search_dirs"])
        entry.setdefault("ping_log_keywords", [])
        entry.setdefault("iperf_log_keywords", [])
        entry["queue_ids"] = list_of_strings(entry.get("queue_ids"))
        entry["interface_names"] = list_of_strings(entry.get("interface_names"))
        entry["container_names"] = list_of_strings(entry.get("container_names"))
        entry["ping_log_keywords"] = list_of_strings(entry.get("ping_log_keywords"))
        entry["iperf_log_keywords"] = list_of_strings(entry.get("iperf_log_keywords"))
        entry["log_search_dirs"] = [
            normalize_path(base_dir, str(item))
            for item in list_of_strings(entry.get("log_search_dirs"))
        ]
        if entry.get("udp_port") is not None:
            entry["udp_port"] = int(entry["udp_port"])
        normalized_service_classes[str(service_name)] = entry
    config["service_classes"] = normalized_service_classes

    return config


def mask_config(config: Dict[str, Any]) -> Dict[str, Any]:
    masked = dict(config)
    if masked.get("onos_password"):
        masked["onos_password"] = "***"
    return masked


def command_failure(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "command": result.get("command"),
        "return_code": result.get("return_code"),
        "duration_ms": result.get("duration_ms"),
        "error": result.get("error"),
        "stderr": result.get("stderr"),
        "stdout_excerpt": result.get("stdout_excerpt"),
    }


class TelemetryCollector:
    def __init__(self, config_path: Path, config: Dict[str, Any], run_once: bool = False) -> None:
        self.config_path = config_path.resolve()
        self.config = config
        self.run_once = run_once
        self.polling_interval_seconds = float(config["polling_interval_seconds"])
        self.command_timeout_seconds = int(config["command_timeout_seconds"])
        self.ovs_container_name = str(config["ovs_container_name"])
        self.ovs_bridge_name = str(config["ovs_bridge_name"])
        self.monitored_ports = list(dict.fromkeys(config.get("monitored_ports", [])))
        self.monitored_interfaces = list(
            dict.fromkeys(config.get("monitored_interfaces", []))
        )
        self.optional_container_names = list(
            dict.fromkeys(config.get("optional_container_names", []))
        )
        self.service_configs = dict(config.get("service_classes", {}))
        self.log_dir = Path(str(config["log_dir"]))
        self.log_dir.mkdir(parents=True, exist_ok=True)
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = f"telemetry-{stamp}-{os.getpid()}"
        self.log_path = self.log_dir / f"telemetry_{stamp}.jsonl"
        self.log_handle = self.log_path.open("a", encoding="utf-8")
        self.stop_requested = False
        self.received_signal_name: Optional[str] = None
        self.warned_keys: set[str] = set()
        self.sample_index = 0

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

    def run_command(
        self, args: List[str], timeout_seconds: Optional[float] = None
    ) -> Dict[str, Any]:
        command_name = args[0] if args else ""
        if not command_name:
            return {
                "ok": False,
                "command": "",
                "return_code": None,
                "duration_ms": 0,
                "stdout": "",
                "stderr": "",
                "stdout_excerpt": "",
                "error": "empty command",
            }
        if shutil.which(command_name) is None:
            message = f"Required host command '{command_name}' is not available."
            self.warn_once(f"missing-command:{command_name}", message)
            return {
                "ok": False,
                "command": args,
                "return_code": None,
                "duration_ms": 0,
                "stdout": "",
                "stderr": "",
                "stdout_excerpt": "",
                "error": message,
            }

        started = time.monotonic()
        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout_seconds or self.command_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            message = f"Command timed out after {timeout_seconds or self.command_timeout_seconds}s"
            self.warn_once(f"timeout:{' '.join(args[:3])}", f"{message}: {' '.join(args)}")
            stdout = (exc.stdout or "").strip()
            stderr = (exc.stderr or "").strip()
            return {
                "ok": False,
                "command": args,
                "return_code": None,
                "duration_ms": duration_ms,
                "stdout": stdout,
                "stderr": stderr,
                "stdout_excerpt": stdout[:400],
                "error": message,
            }
        except Exception as exc:  # pragma: no cover - defensive
            duration_ms = int((time.monotonic() - started) * 1000)
            self.warn_once(
                f"command-exception:{command_name}",
                f"Command execution failed for {' '.join(args)}: {exc}",
            )
            return {
                "ok": False,
                "command": args,
                "return_code": None,
                "duration_ms": duration_ms,
                "stdout": "",
                "stderr": "",
                "stdout_excerpt": "",
                "error": str(exc),
            }

        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        result = {
            "ok": completed.returncode == 0,
            "command": args,
            "return_code": completed.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stdout": stdout,
            "stderr": stderr,
            "stdout_excerpt": stdout[:400],
            "error": "" if completed.returncode == 0 else (stderr or stdout or "command failed"),
        }
        return result

    def run_ovs_command(self, *args: str) -> Dict[str, Any]:
        return self.run_command(["docker", "exec", self.ovs_container_name, *args])

    def run(self) -> int:
        self.install_signal_handlers()
        print_console("INFO", f"Using config: {self.config_path}")
        print_console("INFO", f"Writing telemetry JSONL to: {self.log_path}")
        print_console(
            "INFO",
            f"Polling every {self.polling_interval_seconds:.1f}s for bridge {self.ovs_bridge_name}.",
        )

        self.write_jsonl(
            {
                "event_type": "collector_start",
                "timestamp": utc_timestamp(),
                "run_id": self.run_id,
                "hostname": socket.gethostname(),
                "config_path": str(self.config_path),
                "log_path": str(self.log_path),
                "config": mask_config(self.config),
            }
        )

        exit_code = 0
        try:
            while not self.stop_requested:
                started = time.monotonic()
                self.sample_index += 1
                snapshot = self.collect_snapshot()
                self.write_jsonl(snapshot)
                available_port_count = sum(
                    1
                    for item in snapshot.get("ovs_port_statistics", [])
                    if isinstance(item, dict) and item.get("available") and item.get("port_id")
                )
                print_console(
                    "INFO",
                    f"Captured sample #{self.sample_index} with "
                    f"{available_port_count} OVS port snapshots.",
                )
                if self.run_once:
                    break
                elapsed = time.monotonic() - started
                sleep_for = max(0.0, self.polling_interval_seconds - elapsed)
                self.sleep_with_stop(sleep_for)
        except KeyboardInterrupt:
            print_console("INFO", "Interrupted by Ctrl+C.")
        except Exception as exc:  # pragma: no cover - defensive
            exit_code = 1
            trace = traceback.format_exc()
            print_console("ERROR", f"Collector hit an unexpected error: {exc}")
            self.write_jsonl(
                {
                    "event_type": "collector_error",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "sample_index": self.sample_index,
                    "error": str(exc),
                    "traceback": trace,
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
                    "event_type": "collector_stop",
                    "timestamp": utc_timestamp(),
                    "run_id": self.run_id,
                    "sample_index": self.sample_index,
                    "stopped_cleanly": exit_code == 0,
                }
            )
            self.log_handle.close()
            print_console("INFO", f"Collector stopped. Final log file: {self.log_path}")
        return exit_code

    def sleep_with_stop(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.stop_requested:
            chunk = min(0.25, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def collect_snapshot(self) -> Dict[str, Any]:
        started = time.monotonic()
        ovs_bridge_status = self.collect_ovs_bridge_status()
        ovs_port_statistics = self.collect_ovs_port_statistics()
        ovs_queue_statistics = self.collect_ovs_queue_statistics()
        ovs_flow_counters = self.collect_ovs_flow_counters()
        interface_statistics = self.collect_interface_statistics()
        container_statistics = self.collect_container_statistics()
        onos_reachability = self.collect_onos_reachability()
        traffic_catalog = self.discover_traffic_logs()
        snapshot = {
            "event_type": "snapshot",
            "timestamp": utc_timestamp(),
            "run_id": self.run_id,
            "sample_index": self.sample_index,
            "collector": {
                "polling_interval_seconds": self.polling_interval_seconds,
                "command_timeout_seconds": self.command_timeout_seconds,
                "hostname": socket.gethostname(),
            },
            "ovs_bridge_status": ovs_bridge_status,
            "ovs_port_statistics": ovs_port_statistics,
            "ovs_queue_statistics": ovs_queue_statistics,
            "ovs_flow_counters": ovs_flow_counters,
            "interface_statistics": interface_statistics,
            "container_statistics": container_statistics,
            "onos_reachability": onos_reachability,
            "traffic_log_references": self.build_traffic_log_references(traffic_catalog),
            "service_metrics": self.collect_service_metrics(
                traffic_catalog=traffic_catalog,
                ovs_queue_statistics=ovs_queue_statistics,
                interface_statistics=interface_statistics,
                ovs_flow_counters=ovs_flow_counters,
            ),
        }
        snapshot["collection_duration_ms"] = int((time.monotonic() - started) * 1000)
        return snapshot

    def collect_ovs_bridge_status(self) -> Dict[str, Any]:
        bridge = self.ovs_bridge_name
        show_result = self.run_ovs_command("ovs-vsctl", "show")
        controller_result = self.run_ovs_command("ovs-vsctl", "get-controller", bridge)
        ports_result = self.run_ovs_command("ovs-vsctl", "list-ports", bridge)
        exists_result = self.run_ovs_command("ovs-vsctl", "br-exists", bridge)

        errors: List[Dict[str, Any]] = []
        for result in (show_result, controller_result, ports_result):
            if not result["ok"]:
                errors.append(command_failure(result))

        status = {
            "bridge_name": bridge,
            "ovs_container_name": self.ovs_container_name,
            "exists": exists_result["ok"],
            "controller_targets": controller_result["stdout"].splitlines()
            if controller_result["ok"] and controller_result["stdout"]
            else [],
            "configured_ports": ports_result["stdout"].splitlines()
            if ports_result["ok"] and ports_result["stdout"]
            else [],
            "controller_connected": "is_connected: true" in show_result["stdout"],
            "bridge_present_in_show": bridge in show_result["stdout"],
            "errors": errors,
        }
        if not exists_result["ok"] and exists_result.get("error"):
            status["bridge_exists_check_error"] = command_failure(exists_result)
        return status

    def parse_port_descriptions(self, text: str) -> Dict[str, Dict[str, Any]]:
        ports: Dict[str, Dict[str, Any]] = {}
        current: Optional[Dict[str, Any]] = None
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line or line.startswith("OFPST_PORT_DESC"):
                continue
            match = PORT_DESC_RE.match(line)
            if match:
                port_id, name, rest = match.groups()
                current = {
                    "port_id": port_id,
                    "name": name or ("LOCAL" if port_id == "LOCAL" else port_id),
                }
                if rest:
                    current["description"] = rest
                ports[port_id] = current
                continue
            if current is None:
                continue
            stripped = line.strip()
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            current[key.strip().lower().replace(" ", "_")] = value.strip()
        return ports

    def parse_port_counters(self, text: str) -> Dict[str, Dict[str, Any]]:
        counters: Dict[str, Dict[str, Any]] = {}
        current_port_id: Optional[str] = None
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            rx_match = RX_PORT_STATS_RE.match(line)
            if rx_match:
                current_port_id = rx_match.group(1)
                counters[current_port_id] = {
                    "rx": {
                        "packets": int(rx_match.group(2)),
                        "bytes": int(rx_match.group(3)),
                        "dropped": int(rx_match.group(4)),
                        "errors": int(rx_match.group(5)),
                        "frame_errors": int(rx_match.group(6)),
                        "overrun_errors": int(rx_match.group(7)),
                        "crc_errors": int(rx_match.group(8)),
                    }
                }
                continue
            tx_match = TX_PORT_STATS_RE.match(line)
            if tx_match and current_port_id:
                counters.setdefault(current_port_id, {})["tx"] = {
                    "packets": int(tx_match.group(1)),
                    "bytes": int(tx_match.group(2)),
                    "dropped": int(tx_match.group(3)),
                    "errors": int(tx_match.group(4)),
                    "collisions": int(tx_match.group(5)),
                }
        return counters

    def collect_ovs_port_statistics(self) -> List[Dict[str, Any]]:
        desc_result = self.run_ovs_command(
            "ovs-ofctl", "-O", "OpenFlow13", "dump-ports-desc", self.ovs_bridge_name
        )
        stats_result = self.run_ovs_command(
            "ovs-ofctl", "-O", "OpenFlow13", "dump-ports", self.ovs_bridge_name
        )

        if not desc_result["ok"] and not stats_result["ok"]:
            return [
                {
                    "available": False,
                    "errors": [command_failure(desc_result), command_failure(stats_result)],
                }
            ]

        descriptions = self.parse_port_descriptions(desc_result["stdout"]) if desc_result["ok"] else {}
        counters = self.parse_port_counters(stats_result["stdout"]) if stats_result["ok"] else {}
        port_ids = sorted(
            set(descriptions) | set(counters),
            key=lambda item: (item == "LOCAL", int(item) if item.isdigit() else 10**9),
        )

        port_entries: List[Dict[str, Any]] = []
        for port_id in port_ids:
            entry = {
                "available": True,
                "port_id": port_id,
                "name": descriptions.get(port_id, {}).get("name", port_id),
                "is_monitored": descriptions.get(port_id, {}).get("name", port_id)
                in self.monitored_ports,
            }
            entry.update(descriptions.get(port_id, {}))
            entry.update(counters.get(port_id, {}))
            port_entries.append(entry)

        errors: List[Dict[str, Any]] = []
        if not desc_result["ok"]:
            errors.append(command_failure(desc_result))
        if not stats_result["ok"]:
            errors.append(command_failure(stats_result))
        if errors:
            port_entries.append({"available": False, "errors": errors})
        return port_entries

    def parse_queue_stats(self, text: str) -> List[Dict[str, Any]]:
        queues: List[Dict[str, Any]] = []
        for line in text.splitlines():
            match = QUEUE_STATS_RE.search(line)
            if not match:
                continue
            queues.append(
                {
                    "port": match.group(1),
                    "queue_id": match.group(2),
                    "bytes": int(match.group(3)),
                    "packets": int(match.group(4)),
                    "errors": int(match.group(5)),
                    "duration_seconds": float(match.group(6)),
                }
            )
        return queues

    def collect_ovs_queue_statistics(self) -> Dict[str, Any]:
        queue_stats_result = self.run_ovs_command(
            "ovs-ofctl", "queue-stats", self.ovs_bridge_name
        )
        qos_result = self.run_ovs_command("ovs-vsctl", "list", "qos")
        queue_result = self.run_ovs_command("ovs-vsctl", "list", "queue")

        errors: List[Dict[str, Any]] = []
        for result in (queue_stats_result, qos_result, queue_result):
            if not result["ok"]:
                errors.append(command_failure(result))

        return {
            "available": queue_stats_result["ok"] or qos_result["ok"] or queue_result["ok"],
            "queue_stats_available": queue_stats_result["ok"],
            "queue_stats": self.parse_queue_stats(queue_stats_result["stdout"])
            if queue_stats_result["ok"]
            else [],
            "qos_config": parse_ovs_records(qos_result["stdout"]) if qos_result["ok"] else [],
            "queue_config": parse_ovs_records(queue_result["stdout"])
            if queue_result["ok"]
            else [],
            "errors": errors,
        }

    def parse_flow_dump(self, text: str) -> List[Dict[str, Any]]:
        flows: List[Dict[str, Any]] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or "n_packets=" not in line or "actions=" not in line:
                continue
            header, actions = line.split(" actions=", 1)
            flow: Dict[str, Any] = {"actions": [part.strip() for part in actions.split(",")]}
            match_fields: List[str] = []
            for part in split_top_level(header):
                token = part.strip()
                if "=" in token:
                    key, value = token.split("=", 1)
                    clean_key = key.strip()
                    clean_value = value.strip()
                    if clean_key in {"n_packets", "n_bytes", "table", "priority", "idle_age", "hard_age"}:
                        flow[clean_key] = int(clean_value)
                    elif clean_key == "duration" and clean_value.endswith("s"):
                        flow["duration_seconds"] = float(clean_value[:-1])
                    else:
                        flow[clean_key] = clean_value
                else:
                    match_fields.append(token)
            flow["match_fields"] = [item for item in match_fields if item]
            flow["raw"] = line
            flows.append(flow)
        return flows

    def collect_ovs_flow_counters(self) -> Dict[str, Any]:
        result = self.run_ovs_command(
            "ovs-ofctl", "-O", "OpenFlow13", "dump-flows", self.ovs_bridge_name
        )
        if not result["ok"]:
            return {
                "available": False,
                "flows": [],
                "errors": [command_failure(result)],
            }
        flows = self.parse_flow_dump(result["stdout"])
        return {
            "available": True,
            "flow_count": len(flows),
            "total_packets": sum(int(flow.get("n_packets", 0)) for flow in flows),
            "total_bytes": sum(int(flow.get("n_bytes", 0)) for flow in flows),
            "flows": flows,
        }

    def collect_interface_statistics(self) -> Dict[str, Dict[str, Any]]:
        stats: Dict[str, Dict[str, Any]] = {}
        for interface_name in self.monitored_interfaces:
            interface_path = Path("/sys/class/net") / interface_name
            if not interface_path.exists():
                self.warn_once(
                    f"missing-interface:{interface_name}",
                    f"Interface '{interface_name}' is not currently present on the host.",
                )
                stats[interface_name] = {
                    "available": False,
                    "error": "interface_not_found",
                }
                continue
            try:
                stat_dir = interface_path / "statistics"
                stats[interface_name] = {
                    "available": True,
                    "operstate": (interface_path / "operstate").read_text(encoding="utf-8").strip(),
                    "mtu": int((interface_path / "mtu").read_text(encoding="utf-8").strip()),
                    "rx_bytes": int((stat_dir / "rx_bytes").read_text(encoding="utf-8").strip()),
                    "rx_packets": int((stat_dir / "rx_packets").read_text(encoding="utf-8").strip()),
                    "rx_dropped": int((stat_dir / "rx_dropped").read_text(encoding="utf-8").strip()),
                    "rx_errors": int((stat_dir / "rx_errors").read_text(encoding="utf-8").strip()),
                    "tx_bytes": int((stat_dir / "tx_bytes").read_text(encoding="utf-8").strip()),
                    "tx_packets": int((stat_dir / "tx_packets").read_text(encoding="utf-8").strip()),
                    "tx_dropped": int((stat_dir / "tx_dropped").read_text(encoding="utf-8").strip()),
                    "tx_errors": int((stat_dir / "tx_errors").read_text(encoding="utf-8").strip()),
                }
            except Exception as exc:
                stats[interface_name] = {
                    "available": False,
                    "error": str(exc),
                }
        return stats

    def parse_docker_json_lines(self, text: str) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                entries.append(parsed)
        return entries

    def collect_container_statistics(self) -> Dict[str, Any]:
        ps_result = self.run_command(["docker", "ps", "-a", "--format", "{{json .}}"])
        stats_result = self.run_command(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"]
        )

        containers: Dict[str, Dict[str, Any]] = {}
        errors: List[Dict[str, Any]] = []

        if ps_result["ok"]:
            for entry in self.parse_docker_json_lines(ps_result["stdout"]):
                name = entry.get("Names")
                if not name:
                    continue
                containers[str(name)] = {
                    "container_name": str(name),
                    "container_id": entry.get("ID"),
                    "state": entry.get("State"),
                    "status": entry.get("Status"),
                    "running_for": entry.get("RunningFor"),
                    "networks": entry.get("Networks"),
                }
        else:
            errors.append(command_failure(ps_result))

        live_stats: Dict[str, Dict[str, Any]] = {}
        if stats_result["ok"]:
            for entry in self.parse_docker_json_lines(stats_result["stdout"]):
                name = entry.get("Name")
                if not name:
                    continue
                live_stats[str(name)] = {
                    "cpu_percent": parse_percent(entry.get("CPUPerc")),
                    "memory_percent": parse_percent(entry.get("MemPerc")),
                    "memory_usage": parse_usage_pair(entry.get("MemUsage")),
                    "network_io": parse_usage_pair(entry.get("NetIO")),
                    "block_io": parse_usage_pair(entry.get("BlockIO")),
                    "pids": parse_scalar(str(entry.get("PIDs", ""))),
                    "raw": entry,
                }
        else:
            errors.append(command_failure(stats_result))

        if self.optional_container_names:
            target_names = list(self.optional_container_names)
        else:
            target_names = sorted(set(containers) | set(live_stats))

        combined: Dict[str, Any] = {}
        for name in target_names:
            record = dict(containers.get(name, {"container_name": name, "state": "not_found"}))
            record["stats_available"] = name in live_stats
            if name in live_stats:
                record.update(live_stats[name])
            combined[name] = record

        return {
            "available": ps_result["ok"] or stats_result["ok"],
            "containers": combined,
            "errors": errors,
        }

    def collect_onos_reachability(self) -> Dict[str, Any]:
        base_url = str(self.config.get("onos_base_url", "")).strip()
        if not base_url:
            return {"enabled": False}

        request = urllib.request.Request(base_url)
        auth_value = f"{self.config.get('onos_username', '')}:{self.config.get('onos_password', '')}"
        encoded_auth = base64.b64encode(auth_value.encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {encoded_auth}")
        request.add_header("Accept", "application/json")

        started = time.monotonic()
        try:
            with urllib.request.urlopen(
                request, timeout=float(self.config.get("onos_timeout_seconds", 3))
            ) as response:
                body = response.read().decode("utf-8", errors="replace")
                duration_ms = int((time.monotonic() - started) * 1000)
                payload = json.loads(body) if body else {}
                devices = payload.get("devices", []) if isinstance(payload, dict) else []
                available_devices = [
                    item.get("id")
                    for item in devices
                    if isinstance(item, dict) and item.get("available") is True
                ]
                return {
                    "enabled": True,
                    "reachable": True,
                    "http_status": getattr(response, "status", 200),
                    "response_time_ms": duration_ms,
                    "device_count": len(devices),
                    "available_device_count": len(available_devices),
                    "available_device_ids": available_devices,
                    "url": base_url,
                }
        except urllib.error.HTTPError as exc:
            return {
                "enabled": True,
                "reachable": False,
                "http_status": exc.code,
                "error": exc.reason,
                "url": base_url,
            }
        except urllib.error.URLError as exc:
            return {
                "enabled": True,
                "reachable": False,
                "error": str(exc.reason),
                "url": base_url,
            }
        except Exception as exc:
            return {
                "enabled": True,
                "reachable": False,
                "error": str(exc),
                "url": base_url,
            }

    def parse_ping_log(self, path: Path) -> Dict[str, Any]:
        text = path.read_text(encoding="utf-8", errors="replace")
        summary: Dict[str, Any] = {}
        packet_match = PING_SUMMARY_RE.search(text)
        rtt_match = PING_RTT_RE.search(text)
        reply_rtts = [float(match.group(1)) for match in PING_REPLY_RTT_RE.finditer(text)]
        if packet_match:
            summary["packets_transmitted"] = int(packet_match.group(1))
            summary["packets_received"] = int(packet_match.group(2))
            summary["packet_loss_percent"] = float(packet_match.group(3))
        elif reply_rtts:
            summary["packets_received"] = len(reply_rtts)
        if rtt_match:
            summary["rtt_min_ms"] = float(rtt_match.group(1))
            summary["rtt_avg_ms"] = float(rtt_match.group(2))
            summary["rtt_max_ms"] = float(rtt_match.group(3))
            summary["rtt_mdev_ms"] = float(rtt_match.group(4))
        elif reply_rtts:
            summary["rtt_min_ms"] = min(reply_rtts)
            summary["rtt_avg_ms"] = sum(reply_rtts) / len(reply_rtts)
            summary["rtt_max_ms"] = max(reply_rtts)
        return summary

    def parse_iperf_json_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}

        start_block = payload.get("start", {})
        test_start = start_block.get("test_start", {}) if isinstance(start_block, dict) else {}
        end_block = payload.get("end", {}) if isinstance(payload, dict) else {}

        summary["protocol"] = test_start.get("protocol")
        summary["duration_seconds"] = test_start.get("duration")
        summary["target_bitrate_bps"] = test_start.get("target_bitrate")

        for key in ("sum", "sum_received", "sum_sent"):
            section = end_block.get(key)
            if isinstance(section, dict):
                summary[f"{key}_bytes"] = section.get("bytes")
                summary[f"{key}_bits_per_second"] = section.get("bits_per_second")
                summary[f"{key}_jitter_ms"] = section.get("jitter_ms")
                summary[f"{key}_lost_packets"] = section.get("lost_packets")
                summary[f"{key}_packets"] = section.get("packets")
                summary[f"{key}_lost_percent"] = section.get("lost_percent")
                summary[f"{key}_retransmits"] = section.get("retransmits")
        return summary

    def parse_iperf_text_log(self, text: str) -> Dict[str, Any]:
        for raw_line in reversed(text.splitlines()):
            line = raw_line.strip()
            if not line:
                continue
            udp_match = IPERF_UDP_TEXT_RE.search(line)
            if udp_match:
                bits_per_second = parse_human_bitrate(
                    f"{udp_match.group('bitrate')} {udp_match.group('bitrate_unit')}"
                )
                return compact_dict(
                    {
                        "protocol": "UDP",
                        "sum_bits_per_second": bits_per_second,
                        "sum_jitter_ms": float(udp_match.group("jitter")),
                        "sum_lost_packets": int(udp_match.group("lost")),
                        "sum_packets": int(udp_match.group("packets")),
                        "sum_lost_percent": float(udp_match.group("lost_percent")),
                    }
                )
            tcp_match = IPERF_TCP_TEXT_RE.search(line)
            if tcp_match:
                bits_per_second = parse_human_bitrate(
                    f"{tcp_match.group('bitrate')} {tcp_match.group('bitrate_unit')}"
                )
                retransmits = tcp_match.group("retransmits")
                return compact_dict(
                    {
                        "protocol": "TCP",
                        "sum_bits_per_second": bits_per_second,
                        "sum_retransmits": int(retransmits) if retransmits is not None else None,
                    }
                )
        return {}

    def parse_iperf_log(self, path: Path) -> Dict[str, Any]:
        text = path.read_text(encoding="utf-8", errors="replace")
        stripped = text.lstrip()
        if path.suffix.lower() == ".json" or stripped.startswith("{"):
            return self.parse_iperf_json_payload(json.loads(text))
        return self.parse_iperf_text_log(text)

    def parse_udp_sender_log(self, path: Path) -> Dict[str, Any]:
        text = path.read_text(encoding="utf-8", errors="replace")
        matches = UDP_SENDER_SUMMARY_RE.findall(text)
        if not matches:
            return {}
        summary_line = matches[-1]
        summary: Dict[str, Any] = {}
        for token in summary_line.split():
            if "=" not in token:
                continue
            key, raw_value = token.split("=", 1)
            value = parse_scalar(raw_value)
            summary[str(key)] = value
        if str(summary.get("type") or "") != "udp_sender":
            return {}
        packets_sent = to_float(summary.get("packets_sent"))
        bytes_sent = to_float(summary.get("bytes_sent"))
        duration_seconds = to_float(summary.get("duration_seconds"))
        if (
            summary.get("average_bitrate_bps") is None
            and bytes_sent is not None
            and duration_seconds is not None
            and duration_seconds > 0
        ):
            summary["average_bitrate_bps"] = (bytes_sent * 8.0) / duration_seconds
        if (
            summary.get("packet_rate_per_second") is None
            and packets_sent is not None
            and duration_seconds is not None
            and duration_seconds > 0
        ):
            summary["packet_rate_per_second"] = packets_sent / duration_seconds
        return summary

    def discover_traffic_logs(self) -> Dict[str, Any]:
        search_dir_values = list(self.config.get("traffic_log_search_dirs", []))
        for service_cfg in self.service_configs.values():
            for search_dir in service_cfg.get("log_search_dirs", []):
                if search_dir not in search_dir_values:
                    search_dir_values.append(search_dir)
        search_dirs = [Path(path) for path in search_dir_values]
        patterns = [str(item) for item in self.config.get("traffic_log_globs", [])]

        iperf_logs: List[Dict[str, Any]] = []
        ping_logs: List[Dict[str, Any]] = []
        udp_sender_logs: List[Dict[str, Any]] = []
        seen_paths: set[Path] = set()

        for search_dir in search_dirs:
            if not search_dir.exists():
                continue
            for pattern in patterns:
                for path in search_dir.rglob(pattern):
                    if not path.is_file():
                        continue
                    resolved = path.resolve()
                    if resolved in seen_paths:
                        continue
                    seen_paths.add(resolved)
                    stat = path.stat()
                    record: Dict[str, Any] = {
                        "path": str(resolved),
                        "basename": resolved.name,
                        "size_bytes": stat.st_size,
                        "modified_at": dt.datetime.fromtimestamp(
                            stat.st_mtime, tz=dt.timezone.utc
                        )
                        .isoformat(timespec="seconds")
                        .replace("+00:00", "Z"),
                    }
                    try:
                        ping_summary = self.parse_ping_log(path)
                        if ping_summary:
                            record["parsed_summary"] = ping_summary
                            ping_logs.append(record)
                            continue
                    except Exception as exc:
                        record["parse_error"] = str(exc)

                    try:
                        udp_sender_summary = self.parse_udp_sender_log(path)
                        if udp_sender_summary:
                            record["parsed_summary"] = udp_sender_summary
                            udp_sender_logs.append(record)
                            continue
                    except Exception as exc:
                        record["parse_error"] = str(exc)

                    try:
                        iperf_summary = self.parse_iperf_log(path)
                        if iperf_summary:
                            record["parsed_summary"] = iperf_summary
                            iperf_logs.append(record)
                    except Exception as exc:
                        if "parse_error" not in record:
                            record["parse_error"] = str(exc)

        for catalog in (iperf_logs, ping_logs, udp_sender_logs):
            catalog.sort(key=lambda item: item["modified_at"], reverse=True)

        return {
            "search_dirs": [str(path) for path in search_dirs],
            "iperf_logs": iperf_logs,
            "ping_logs": ping_logs,
            "udp_sender_logs": udp_sender_logs,
        }

    def record_matches_service(
        self,
        record: Any,
        keywords: List[str],
        search_dirs: Optional[List[str]] = None,
        log_names: Optional[List[str]] = None,
    ) -> bool:
        if not isinstance(record, dict):
            return False
        path = str(record.get("path") or "")
        if not path:
            return False
        lowered_path = path.lower()
        lowered_dirs = [str(Path(item).resolve()).lower() for item in (search_dirs or [])]
        lowered_keywords = [str(item).lower() for item in keywords if str(item).strip()]
        lowered_names = [str(item).lower() for item in (log_names or []) if str(item).strip()]
        basename = Path(path).name.lower()
        if lowered_dirs and not any(lowered_path.startswith(prefix) for prefix in lowered_dirs):
            return False
        if lowered_names and basename in lowered_names:
            return True
        if lowered_keywords and any(keyword in lowered_path for keyword in lowered_keywords):
            return True
        if not lowered_names and not lowered_keywords:
            return True
        return False

    def find_matching_logs(
        self,
        records: Any,
        keywords: List[str],
        search_dirs: Optional[List[str]] = None,
        log_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not isinstance(records, list):
            return []
        return [
            record
            for record in records
            if self.record_matches_service(record, keywords, search_dirs, log_names)
        ]

    def find_matching_log(
        self,
        records: Any,
        keywords: List[str],
        search_dirs: Optional[List[str]] = None,
        log_names: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        matches = self.find_matching_logs(records, keywords, search_dirs, log_names)
        return matches[0] if matches else None

    def build_traffic_log_references(self, traffic_catalog: Dict[str, Any]) -> Dict[str, Any]:
        iperf_logs = list(traffic_catalog.get("iperf_logs", []))
        ping_logs = list(traffic_catalog.get("ping_logs", []))
        udp_sender_logs = list(traffic_catalog.get("udp_sender_logs", []))
        service_matches: Dict[str, Dict[str, Any]] = {}
        for service_name, service_cfg in self.service_configs.items():
            search_dirs = list_of_strings(service_cfg.get("log_search_dirs"))
            ping_records = self.find_matching_logs(
                ping_logs,
                list_of_strings(service_cfg.get("ping_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("ping_log_names")),
            )
            iperf_records = self.find_matching_logs(
                iperf_logs,
                list_of_strings(service_cfg.get("iperf_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("iperf_log_names")),
            )
            udp_sender_records = self.find_matching_logs(
                udp_sender_logs,
                list_of_strings(service_cfg.get("udp_sender_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("udp_sender_log_names")),
            )
            service_matches[service_name] = compact_dict(
                {
                    "ping_log_path": ping_records[0].get("path") if ping_records else None,
                    "ping_log_modified_at": ping_records[0].get("modified_at") if ping_records else None,
                    "ping_log_count": len(ping_records),
                    "ping_log_paths": [record.get("path") for record in ping_records],
                    "iperf_log_path": iperf_records[0].get("path") if iperf_records else None,
                    "iperf_log_modified_at": iperf_records[0].get("modified_at") if iperf_records else None,
                    "iperf_log_count": len(iperf_records),
                    "iperf_log_paths": [record.get("path") for record in iperf_records],
                    "udp_sender_log_path": udp_sender_records[0].get("path")
                    if udp_sender_records
                    else None,
                    "udp_sender_log_modified_at": udp_sender_records[0].get("modified_at")
                    if udp_sender_records
                    else None,
                    "udp_sender_log_count": len(udp_sender_records),
                    "udp_sender_log_paths": [record.get("path") for record in udp_sender_records],
                }
            )

        return {
            "search_dirs": list(traffic_catalog.get("search_dirs", [])),
            "iperf_logs_found": len(iperf_logs),
            "ping_logs_found": len(ping_logs),
            "udp_sender_logs_found": len(udp_sender_logs),
            "latest_iperf_logs": iperf_logs[:10],
            "latest_ping_logs": ping_logs[:10],
            "latest_udp_sender_logs": udp_sender_logs[:10],
            "service_matches": service_matches,
        }

    def aggregate_queue_snapshot(
        self, ovs_queue_statistics: Dict[str, Any], queue_ids: List[str]
    ) -> Dict[str, Any]:
        queue_stats = ovs_queue_statistics.get("queue_stats", [])
        if not isinstance(queue_stats, list):
            return {}
        totals = {
            "queue_bytes": 0,
            "queue_packets": 0,
            "queue_errors": 0,
            "queue_duration_seconds": 0.0,
        }
        matched_queue_ids: List[str] = []
        matched = False
        for record in queue_stats:
            if not isinstance(record, dict):
                continue
            queue_id = str(record.get("queue_id"))
            if queue_ids and queue_id not in queue_ids:
                continue
            matched = True
            matched_queue_ids.append(queue_id)
            totals["queue_bytes"] += int(record.get("bytes", 0))
            totals["queue_packets"] += int(record.get("packets", 0))
            totals["queue_errors"] += int(record.get("errors", 0))
            totals["queue_duration_seconds"] += float(record.get("duration_seconds", 0.0))
        if not matched:
            return {}
        totals["queue_ids"] = sorted(set(matched_queue_ids), key=str)
        totals["queue_occupancy_bytes"] = totals["queue_bytes"]
        return totals

    def aggregate_interface_counters(
        self, interface_statistics: Dict[str, Any], interface_names: List[str]
    ) -> Dict[str, Any]:
        if not isinstance(interface_statistics, dict):
            return {}
        selected_names = interface_names or list(interface_statistics.keys())
        totals = {
            "rx_bytes": 0,
            "rx_packets": 0,
            "rx_dropped": 0,
            "tx_bytes": 0,
            "tx_packets": 0,
            "tx_dropped": 0,
        }
        matched_interfaces: List[str] = []
        matched = False
        for interface_name in selected_names:
            record = interface_statistics.get(interface_name)
            if not isinstance(record, dict) or not record.get("available"):
                continue
            matched = True
            matched_interfaces.append(str(interface_name))
            for key in totals:
                totals[key] += int(record.get(key, 0))
        if not matched:
            return {}
        totals["interface_names"] = matched_interfaces
        attempted_packets = totals["tx_packets"] + totals["tx_dropped"]
        if attempted_packets > 0:
            totals["drop_rate_percent"] = (totals["tx_dropped"] / attempted_packets) * 100.0
        return totals

    def aggregate_flow_totals(
        self, ovs_flow_counters: Dict[str, Any], udp_port: Optional[int]
    ) -> Dict[str, Any]:
        if udp_port is None:
            return {}
        flows = ovs_flow_counters.get("flows", [])
        if not isinstance(flows, list):
            return {}
        totals = {"flow_bytes_total": 0, "flow_packets_total": 0}
        matched = False
        for flow in flows:
            if not isinstance(flow, dict):
                continue
            if str(flow.get("tp_dst")) != str(udp_port):
                continue
            matched = True
            totals["flow_bytes_total"] += int(flow.get("n_bytes", 0))
            totals["flow_packets_total"] += int(flow.get("n_packets", 0))
        return totals if matched else {}

    def compute_delivery_ratio_percent(self, iperf_summary: Dict[str, Any]) -> Optional[float]:
        sent_packets = iperf_summary.get("sum_sent_packets") or iperf_summary.get("sum_packets")
        lost_packets = iperf_summary.get("sum_sent_lost_packets") or iperf_summary.get("sum_lost_packets")
        sent_value = parse_scalar(str(sent_packets)) if sent_packets is not None else None
        lost_value = parse_scalar(str(lost_packets)) if lost_packets is not None else None
        if isinstance(sent_value, int) and isinstance(lost_value, int) and sent_value > 0:
            delivered = max(0, sent_value - lost_value)
            return (delivered / sent_value) * 100.0
        sent_bytes = iperf_summary.get("sum_sent_bytes") or iperf_summary.get("sum_bytes")
        received_bytes = iperf_summary.get("sum_received_bytes") or iperf_summary.get("sum_bytes")
        if isinstance(sent_bytes, (int, float)) and isinstance(received_bytes, (int, float)) and sent_bytes > 0:
            return min(100.0, (float(received_bytes) / float(sent_bytes)) * 100.0)
        loss_percent = parse_percent(
            str(
                iperf_summary.get("sum_lost_percent")
                or iperf_summary.get("sum_sent_lost_percent")
                or iperf_summary.get("sum_received_lost_percent")
                or ""
            )
        )
        if loss_percent is None:
            return None
        return max(0.0, 100.0 - loss_percent)

    def normalize_ue_label_for_container(self, container_name: str) -> Optional[str]:
        if not container_name:
            return None
        if container_name.endswith("-ue"):
            return "ue1"
        match = re.search(r"-ue(\d+)$", container_name)
        if not match:
            return None
        return f"ue{int(match.group(1))}"

    def extract_ue_label_from_path(self, path: str) -> Optional[str]:
        match = re.search(r"ue(\d+)", Path(path).name, re.IGNORECASE)
        if not match:
            return None
        return f"ue{int(match.group(1))}"

    def build_service_ue_container_map(self, service_cfg: Dict[str, Any]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for container_name in list_of_strings(service_cfg.get("container_names")):
            ue_label = self.normalize_ue_label_for_container(container_name)
            if ue_label:
                mapping[ue_label] = container_name
        return mapping

    def merge_per_ue_metrics(self, *sources: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for source in sources:
            for ue_label, metrics in source.items():
                merged.setdefault(ue_label, {})
                merged[ue_label].update(metrics)
        return merged

    def aggregate_ping_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        transmitted_total = 0.0
        received_total = 0.0
        latency_weight = 0.0
        latency_weighted_total = 0.0
        latency_max_values: List[float] = []
        per_ue_metrics: Dict[str, Dict[str, Any]] = {}

        for index, record in enumerate(records, start=1):
            summary = record.get("parsed_summary", {})
            if not isinstance(summary, dict):
                continue
            ue_label = self.extract_ue_label_from_path(str(record.get("path") or "")) or f"ue{index}"
            transmitted = to_float(summary.get("packets_transmitted"))
            received = to_float(summary.get("packets_received"))
            latency_avg = to_float(summary.get("rtt_avg_ms"))
            latency_max = to_float(summary.get("rtt_max_ms"))
            packet_loss = to_float(summary.get("packet_loss_percent"))
            weight = received if received and received > 0 else 1.0
            if transmitted is not None:
                transmitted_total += transmitted
            if received is not None:
                received_total += received
            if latency_avg is not None:
                latency_weighted_total += latency_avg * weight
                latency_weight += weight
            if latency_max is not None:
                latency_max_values.append(latency_max)
            per_ue_metrics[ue_label] = compact_dict(
                {
                    "ue_label": ue_label,
                    "ping_log_path": record.get("path"),
                    "ping_log_modified_at": record.get("modified_at"),
                    "latency_avg_ms": latency_avg,
                    "latency_max_ms": latency_max,
                    "packet_loss_percent": packet_loss,
                    "packets_transmitted": transmitted,
                    "packets_received": received,
                }
            )

        packet_loss_percent = None
        if transmitted_total > 0:
            packet_loss_percent = ((transmitted_total - received_total) / transmitted_total) * 100.0
        return compact_dict(
            {
                "latency_avg_ms": (latency_weighted_total / latency_weight)
                if latency_weight > 0
                else None,
                "latency_max_ms": max(latency_max_values) if latency_max_values else None,
                "packet_loss_percent": packet_loss_percent,
                "ping_packets_transmitted": transmitted_total if transmitted_total > 0 else None,
                "ping_packets_received": received_total if received_total > 0 else None,
                "per_ue_metrics": per_ue_metrics,
            }
        )

    def aggregate_iperf_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        throughput_bps_total = 0.0
        throughput_found = False
        retransmits_total = 0.0
        retransmits_found = False
        jitter_values: List[float] = []
        lost_packets_total = 0.0
        packets_total = 0.0
        bytes_sent_total = 0.0
        bytes_received_total = 0.0
        per_ue_metrics: Dict[str, Dict[str, Any]] = {}

        for index, record in enumerate(records, start=1):
            summary = record.get("parsed_summary", {})
            if not isinstance(summary, dict):
                continue
            ue_label = self.extract_ue_label_from_path(str(record.get("path") or "")) or f"ue{index}"
            throughput_bps = to_float(
                summary.get("sum_bits_per_second")
                or summary.get("sum_sent_bits_per_second")
                or summary.get("sum_received_bits_per_second")
            )
            retransmits = to_float(
                summary.get("sum_retransmits")
                or summary.get("sum_sent_retransmits")
                or summary.get("sum_received_retransmits")
            )
            jitter_ms = to_float(
                summary.get("sum_jitter_ms")
                or summary.get("sum_sent_jitter_ms")
                or summary.get("sum_received_jitter_ms")
            )
            lost_packets = to_float(
                summary.get("sum_lost_packets") or summary.get("sum_sent_lost_packets")
            )
            packets = to_float(summary.get("sum_packets") or summary.get("sum_sent_packets"))
            sent_bytes = to_float(summary.get("sum_sent_bytes") or summary.get("sum_bytes"))
            received_bytes = to_float(
                summary.get("sum_received_bytes") or summary.get("sum_bytes")
            )
            if throughput_bps is not None:
                throughput_bps_total += throughput_bps
                throughput_found = True
            if retransmits is not None:
                retransmits_total += retransmits
                retransmits_found = True
            if jitter_ms is not None:
                jitter_values.append(jitter_ms)
            if lost_packets is not None:
                lost_packets_total += lost_packets
            if packets is not None:
                packets_total += packets
            if sent_bytes is not None:
                bytes_sent_total += sent_bytes
            if received_bytes is not None:
                bytes_received_total += received_bytes
            per_ue_metrics[ue_label] = compact_dict(
                {
                    "ue_label": ue_label,
                    "iperf_log_path": record.get("path"),
                    "iperf_log_modified_at": record.get("modified_at"),
                    "throughput_bps": throughput_bps,
                    "throughput_mbps": throughput_bps / 1_000_000.0
                    if throughput_bps is not None
                    else None,
                    "retransmits": retransmits,
                    "jitter_ms": jitter_ms,
                    "packet_loss_percent": to_float(
                        summary.get("sum_lost_percent")
                        or summary.get("sum_sent_lost_percent")
                        or summary.get("sum_received_lost_percent")
                    ),
                }
            )

        packet_loss_percent = None
        if packets_total > 0:
            packet_loss_percent = (lost_packets_total / packets_total) * 100.0
        delivery_ratio_percent = None
        if bytes_sent_total > 0:
            delivery_ratio_percent = min(100.0, (bytes_received_total / bytes_sent_total) * 100.0)
        elif packet_loss_percent is not None:
            delivery_ratio_percent = max(0.0, 100.0 - packet_loss_percent)

        return compact_dict(
            {
                "throughput_bps": throughput_bps_total if throughput_found else None,
                "throughput_mbps": (throughput_bps_total / 1_000_000.0)
                if throughput_found
                else None,
                "retransmits": retransmits_total if retransmits_found else None,
                "jitter_ms": (sum(jitter_values) / len(jitter_values)) if jitter_values else None,
                "packet_loss_percent": packet_loss_percent,
                "packet_delivery_ratio_percent": delivery_ratio_percent,
                "success_ratio_percent": delivery_ratio_percent,
                "per_ue_metrics": per_ue_metrics,
            }
        )

    def aggregate_udp_sender_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        packets_total = 0.0
        bytes_total = 0.0
        bitrate_total = 0.0
        bitrate_found = False
        packet_rate_total = 0.0
        packet_rate_found = False
        per_ue_metrics: Dict[str, Dict[str, Any]] = {}

        for index, record in enumerate(records, start=1):
            summary = record.get("parsed_summary", {})
            if not isinstance(summary, dict):
                continue
            ue_label = self.extract_ue_label_from_path(str(record.get("path") or "")) or f"ue{index}"
            packets_sent = to_float(summary.get("packets_sent"))
            bytes_sent = to_float(summary.get("bytes_sent"))
            bitrate_bps = to_float(summary.get("average_bitrate_bps"))
            packet_rate = to_float(summary.get("packet_rate_per_second"))
            if packets_sent is not None:
                packets_total += packets_sent
            if bytes_sent is not None:
                bytes_total += bytes_sent
            if bitrate_bps is not None:
                bitrate_total += bitrate_bps
                bitrate_found = True
            if packet_rate is not None:
                packet_rate_total += packet_rate
                packet_rate_found = True
            per_ue_metrics[ue_label] = compact_dict(
                {
                    "ue_label": ue_label,
                    "udp_sender_log_path": record.get("path"),
                    "udp_sender_log_modified_at": record.get("modified_at"),
                    "packets_sent": packets_sent,
                    "bytes_sent": bytes_sent,
                    "average_bitrate_bps": bitrate_bps,
                    "packet_rate_per_second": packet_rate,
                }
            )

        return compact_dict(
            {
                "sender_packets_sent": packets_total if packets_total > 0 else None,
                "sender_bytes_sent": bytes_total if bytes_total > 0 else None,
                "sender_average_bitrate_bps": bitrate_total if bitrate_found else None,
                "sender_average_bitrate_mbps": (bitrate_total / 1_000_000.0)
                if bitrate_found
                else None,
                "sender_packet_rate_per_second": packet_rate_total if packet_rate_found else None,
                "per_ue_metrics": per_ue_metrics,
            }
        )

    def collect_service_metrics(
        self,
        traffic_catalog: Dict[str, Any],
        ovs_queue_statistics: Dict[str, Any],
        interface_statistics: Dict[str, Any],
        ovs_flow_counters: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        service_metrics: Dict[str, Dict[str, Any]] = {}
        iperf_logs = traffic_catalog.get("iperf_logs", [])
        ping_logs = traffic_catalog.get("ping_logs", [])
        udp_sender_logs = traffic_catalog.get("udp_sender_logs", [])

        for service_name, service_cfg in self.service_configs.items():
            search_dirs = list_of_strings(service_cfg.get("log_search_dirs"))
            ping_records = self.find_matching_logs(
                ping_logs,
                list_of_strings(service_cfg.get("ping_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("ping_log_names")),
            )
            iperf_records = self.find_matching_logs(
                iperf_logs,
                list_of_strings(service_cfg.get("iperf_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("iperf_log_names")),
            )
            udp_sender_records = self.find_matching_logs(
                udp_sender_logs,
                list_of_strings(service_cfg.get("udp_sender_log_keywords")),
                search_dirs,
                list_of_strings(service_cfg.get("udp_sender_log_names")),
            )
            ping_summary = self.aggregate_ping_records(ping_records)
            iperf_summary = self.aggregate_iperf_records(iperf_records)
            udp_sender_summary = self.aggregate_udp_sender_records(udp_sender_records)
            queue_metrics = self.aggregate_queue_snapshot(
                ovs_queue_statistics, list_of_strings(service_cfg.get("queue_ids"))
            )
            interface_metrics = self.aggregate_interface_counters(
                interface_statistics, list_of_strings(service_cfg.get("interface_names"))
            )
            flow_metrics = self.aggregate_flow_totals(
                ovs_flow_counters, service_cfg.get("udp_port")
            )
            per_ue_metrics = self.merge_per_ue_metrics(
                ping_summary.get("per_ue_metrics", {}),
                iperf_summary.get("per_ue_metrics", {}),
                udp_sender_summary.get("per_ue_metrics", {}),
            )
            ue_container_map = self.build_service_ue_container_map(service_cfg)
            for ue_label, metrics in per_ue_metrics.items():
                if ue_label in ue_container_map:
                    metrics.setdefault("container_name", ue_container_map[ue_label])

            latency_avg_ms = ping_summary.get("latency_avg_ms")
            latency_max_ms = ping_summary.get("latency_max_ms")
            packet_loss_percent = (
                ping_summary.get("packet_loss_percent")
                if service_name == "real_time_control"
                else iperf_summary.get("sum_lost_percent")
                or iperf_summary.get("sum_sent_lost_percent")
                or iperf_summary.get("sum_received_lost_percent")
                or iperf_summary.get("packet_loss_percent")
            )
            jitter_ms = (
                iperf_summary.get("sum_jitter_ms")
                or iperf_summary.get("sum_received_jitter_ms")
                or iperf_summary.get("sum_sent_jitter_ms")
                or iperf_summary.get("jitter_ms")
            )
            throughput_bps = (
                iperf_summary.get("sum_bits_per_second")
                or iperf_summary.get("sum_sent_bits_per_second")
                or iperf_summary.get("sum_received_bits_per_second")
                or iperf_summary.get("throughput_bps")
                or udp_sender_summary.get("sender_average_bitrate_bps")
            )
            throughput_mbps = (
                float(throughput_bps) / 1_000_000.0
                if isinstance(throughput_bps, (int, float))
                else None
            )
            delivery_ratio_percent = (
                iperf_summary.get("packet_delivery_ratio_percent")
                or iperf_summary.get("success_ratio_percent")
                or self.compute_delivery_ratio_percent(iperf_summary)
            )
            retransmits = (
                iperf_summary.get("sum_retransmits")
                or iperf_summary.get("sum_sent_retransmits")
                or iperf_summary.get("sum_received_retransmits")
                or iperf_summary.get("retransmits")
            )

            record = compact_dict(
                {
                    "service_class": service_name,
                    "udp_port": service_cfg.get("udp_port"),
                    "queue_ids": queue_metrics.get("queue_ids") or list_of_strings(service_cfg.get("queue_ids")),
                    "interface_names": interface_metrics.get("interface_names")
                    or list_of_strings(service_cfg.get("interface_names")),
                    "container_names": list_of_strings(service_cfg.get("container_names")),
                    "latency_ms": latency_avg_ms,
                    "latency_avg_ms": latency_avg_ms,
                    "latency_max_ms": latency_max_ms,
                    "packet_loss_percent": packet_loss_percent,
                    "jitter_ms": jitter_ms,
                    "throughput_bps": throughput_bps,
                    "throughput_mbps": throughput_mbps,
                    "retransmits": retransmits,
                    "queue_bytes": queue_metrics.get("queue_bytes"),
                    "queue_occupancy_bytes": queue_metrics.get("queue_occupancy_bytes"),
                    "queue_packets": queue_metrics.get("queue_packets"),
                    "queue_errors": queue_metrics.get("queue_errors"),
                    "queue_duration_seconds": queue_metrics.get("queue_duration_seconds"),
                    "packet_delivery_ratio_percent": delivery_ratio_percent,
                    "success_ratio_percent": delivery_ratio_percent,
                    "sender_packets_sent": udp_sender_summary.get("sender_packets_sent"),
                    "sender_bytes_sent": udp_sender_summary.get("sender_bytes_sent"),
                    "sender_average_bitrate_bps": udp_sender_summary.get("sender_average_bitrate_bps"),
                    "sender_average_bitrate_mbps": udp_sender_summary.get("sender_average_bitrate_mbps"),
                    "sender_packet_rate_per_second": udp_sender_summary.get("sender_packet_rate_per_second"),
                    "rx_bytes": interface_metrics.get("rx_bytes"),
                    "rx_packets": interface_metrics.get("rx_packets"),
                    "rx_dropped": interface_metrics.get("rx_dropped"),
                    "tx_bytes": interface_metrics.get("tx_bytes"),
                    "tx_packets": interface_metrics.get("tx_packets"),
                    "tx_dropped": interface_metrics.get("tx_dropped"),
                    "drop_rate_percent": interface_metrics.get("drop_rate_percent"),
                    "flow_bytes_total": flow_metrics.get("flow_bytes_total"),
                    "flow_packets_total": flow_metrics.get("flow_packets_total"),
                    "ping_log_path": ping_records[0].get("path") if ping_records else None,
                    "ping_log_modified_at": ping_records[0].get("modified_at") if ping_records else None,
                    "iperf_log_path": iperf_records[0].get("path") if iperf_records else None,
                    "iperf_log_modified_at": iperf_records[0].get("modified_at") if iperf_records else None,
                    "udp_sender_log_path": udp_sender_records[0].get("path")
                    if udp_sender_records
                    else None,
                    "udp_sender_log_modified_at": udp_sender_records[0].get("modified_at")
                    if udp_sender_records
                    else None,
                    "matched_ping_log_count": len(ping_records),
                    "matched_iperf_log_count": len(iperf_records),
                    "matched_udp_sender_log_count": len(udp_sender_records),
                    "per_ue_metrics": per_ue_metrics,
                }
            )
            sources: List[str] = []
            if ping_records:
                sources.append("ping_log")
            if iperf_records:
                sources.append("iperf_log")
            if udp_sender_records:
                sources.append("udp_sender_log")
            if queue_metrics:
                sources.append("ovs_queue_stats")
            if interface_metrics:
                sources.append("interface_counters")
            if flow_metrics:
                sources.append("ovs_flow_counters")
            record["sources"] = sources
            record["available"] = any(
                key in record
                for key in (
                    "latency_ms",
                    "packet_loss_percent",
                    "jitter_ms",
                    "throughput_bps",
                    "throughput_mbps",
                    "queue_bytes",
                    "packet_delivery_ratio_percent",
                    "sender_packets_sent",
                    "rx_packets",
                    "tx_packets",
                )
            )
            service_metrics[service_name] = record
        return service_metrics


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect OVS, container, ONOS, and traffic-log telemetry into JSONL."
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name(DEFAULT_CONFIG_NAME)),
        help="Path to the telemetry config file.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect a single snapshot and exit.",
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

    collector = TelemetryCollector(config_path=config_path, config=config, run_once=args.once)
    return collector.run()


if __name__ == "__main__":
    sys.exit(main())
