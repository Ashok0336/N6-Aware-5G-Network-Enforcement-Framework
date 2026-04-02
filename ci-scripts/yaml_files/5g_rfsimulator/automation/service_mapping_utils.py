#!/usr/bin/env python3
"""Helpers for loading and applying the shared multi-UE service mapping."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


def load_yaml_or_json(path: Path) -> Dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(raw_text)
    else:
        loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {path} must define a top-level mapping/object.")
    return loaded


def list_of_strings(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)] if str(value).strip() else []


def unique_strings(values: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def normalize_path_list(base_dir: Path, values: Any) -> List[str]:
    return [str((base_dir / value).resolve()) for value in list_of_strings(values)]


def infer_ue_label(container_name: str) -> Optional[str]:
    if not container_name:
        return None
    if container_name.endswith("-ue"):
        return "ue1"
    match = re.search(r"-ue(\d+)$", container_name)
    if not match:
        return None
    return f"ue{int(match.group(1))}"


def normalize_service_mapping(mapping_path: Path) -> Dict[str, Any]:
    raw_mapping = load_yaml_or_json(mapping_path)
    base_dir = mapping_path.parent
    defaults = dict(raw_mapping.get("defaults", {}))
    defaults["log_search_dirs"] = normalize_path_list(
        base_dir, defaults.get("log_search_dirs", [])
    )

    services_raw = raw_mapping.get("service_classes", raw_mapping.get("services", {}))
    if not isinstance(services_raw, dict):
        raise ValueError("service_mapping.service_classes must be a mapping/object.")

    normalized_services: Dict[str, Dict[str, Any]] = {}
    for service_name, service_cfg in services_raw.items():
        if not isinstance(service_cfg, dict):
            raise ValueError(f"service_mapping.service_classes.{service_name} must be an object.")
        entry = dict(service_cfg)
        entry.setdefault("service_class", service_name)
        entry.setdefault("ue_bindings", [])
        entry.setdefault("log_file_names", [])
        entry.setdefault("ping_log_file_names", [])
        entry.setdefault("iperf_log_file_names", [])
        entry.setdefault("udp_sender_log_file_names", [])
        entry.setdefault("target_ports", [])
        entry.setdefault("log_search_dirs", defaults.get("log_search_dirs", []))

        ue_bindings: List[Dict[str, Any]] = []
        for index, raw_binding in enumerate(entry.get("ue_bindings", [])):
            if not isinstance(raw_binding, dict):
                continue
            container_name = str(
                raw_binding.get("container_name")
                or raw_binding.get("ue_container")
                or raw_binding.get("container")
                or ""
            ).strip()
            if not container_name:
                continue
            ue_label = str(raw_binding.get("ue_label") or "").strip()
            if not ue_label:
                ue_label = infer_ue_label(container_name) or f"ue{index + 1}"
            binding = {
                "container_name": container_name,
                "ue_label": ue_label,
            }
            log_file_name = str(raw_binding.get("log_file_name") or "").strip()
            if log_file_name:
                binding["log_file_name"] = log_file_name
            auxiliary = unique_strings(
                list_of_strings(raw_binding.get("auxiliary_log_file_names"))
            )
            if auxiliary:
                binding["auxiliary_log_file_names"] = auxiliary
            ue_bindings.append(binding)

        ue_containers = unique_strings(
            [binding["container_name"] for binding in ue_bindings]
            + list_of_strings(entry.get("ue_containers") or entry.get("container_names"))
        )
        log_file_names = unique_strings(
            list_of_strings(entry.get("log_file_names"))
            + list_of_strings(entry.get("ping_log_file_names"))
            + list_of_strings(entry.get("iperf_log_file_names"))
            + list_of_strings(entry.get("udp_sender_log_file_names"))
            + [
                str(binding.get("log_file_name") or "")
                for binding in ue_bindings
                if str(binding.get("log_file_name") or "").strip()
            ]
            + [
                item
                for binding in ue_bindings
                for item in list_of_strings(binding.get("auxiliary_log_file_names"))
            ]
        )
        target_ports = []
        for raw_port in list_of_strings(entry.get("target_ports")):
            try:
                target_ports.append(int(raw_port))
            except ValueError:
                continue

        queue_id = entry.get("queue_id")
        queue_id_value: Optional[str]
        if queue_id is None or str(queue_id).strip() == "":
            queue_id_value = None
        else:
            queue_id_value = str(queue_id)

        normalized_services[str(service_name)] = {
            "service_class": str(entry.get("service_class") or service_name),
            "target_ip": str(
                entry.get("target_ip") or defaults.get("target_ip") or ""
            ).strip(),
            "target_ports": target_ports,
            "queue_id": queue_id_value,
            "queue_profile_name": str(
                entry.get("queue_profile_name")
                or entry.get("queue_profile")
                or ""
            ).strip(),
            "ue_bindings": ue_bindings,
            "ue_containers": ue_containers,
            "log_file_names": log_file_names,
            "ping_log_file_names": unique_strings(
                list_of_strings(entry.get("ping_log_file_names"))
            ),
            "iperf_log_file_names": unique_strings(
                list_of_strings(entry.get("iperf_log_file_names"))
            ),
            "udp_sender_log_file_names": unique_strings(
                list_of_strings(entry.get("udp_sender_log_file_names"))
            ),
            "log_search_dirs": normalize_path_list(base_dir, entry.get("log_search_dirs")),
            "traffic_profile": dict(entry.get("traffic_profile", {}))
            if isinstance(entry.get("traffic_profile"), dict)
            else {},
        }
    return {
        "mapping_path": str(mapping_path.resolve()),
        "defaults": defaults,
        "service_classes": normalized_services,
    }


def apply_service_mapping(base_dir: Path, config: Dict[str, Any]) -> Dict[str, Any]:
    config = dict(config)
    mapping_path_value = str(config.get("service_mapping_path") or "").strip()
    if not mapping_path_value:
        return config

    mapping_path = (base_dir / mapping_path_value).resolve()
    mapping = normalize_service_mapping(mapping_path)
    config["service_mapping_path"] = str(mapping_path)
    config["service_mapping"] = mapping

    service_classes = dict(config.get("service_classes", {}))
    traffic_dirs = unique_strings(
        list_of_strings(config.get("traffic_log_search_dirs"))
        + list_of_strings(mapping.get("defaults", {}).get("log_search_dirs"))
    )
    if traffic_dirs:
        config["traffic_log_search_dirs"] = traffic_dirs

    optional_container_names = unique_strings(
        list_of_strings(config.get("optional_container_names"))
        + (
            [str(mapping.get("defaults", {}).get("ext_dn_container"))]
            if str(mapping.get("defaults", {}).get("ext_dn_container") or "").strip()
            else []
        )
    )

    for service_name, service_mapping in mapping.get("service_classes", {}).items():
        entry = dict(service_classes.get(service_name, {}))
        ue_containers = list_of_strings(service_mapping.get("ue_containers"))
        if ue_containers:
            entry["container_names"] = ue_containers
            entry["ue_container_names"] = ue_containers
            optional_container_names = unique_strings(optional_container_names + ue_containers)

        queue_id = str(service_mapping.get("queue_id") or "").strip()
        if queue_id:
            entry["queue_id"] = int(queue_id)
            entry["queue_ids"] = [queue_id]
            entry["monitored_queue_ids"] = [queue_id]

        target_ports = service_mapping.get("target_ports", [])
        if isinstance(target_ports, list) and target_ports:
            first_port = int(target_ports[0])
            entry["target_ports"] = [int(item) for item in target_ports]
            entry["udp_port"] = first_port
            entry["traffic_udp_port"] = first_port

        target_ip = str(service_mapping.get("target_ip") or "").strip()
        if target_ip:
            entry["target_ip"] = target_ip

        log_search_dirs = list_of_strings(service_mapping.get("log_search_dirs"))
        if log_search_dirs:
            entry["log_search_dirs"] = log_search_dirs

        for source_key, target_key in (
            ("log_file_names", "log_file_names"),
            ("ping_log_file_names", "ping_log_names"),
            ("iperf_log_file_names", "iperf_log_names"),
            ("udp_sender_log_file_names", "udp_sender_log_names"),
        ):
            values = list_of_strings(service_mapping.get(source_key))
            if values:
                entry[target_key] = values

        queue_profile_name = str(service_mapping.get("queue_profile_name") or "").strip()
        if queue_profile_name:
            entry["queue_profile_name"] = queue_profile_name

        traffic_profile = service_mapping.get("traffic_profile", {})
        if isinstance(traffic_profile, dict) and traffic_profile:
            entry["traffic_profile"] = dict(traffic_profile)

        service_classes[service_name] = entry

    config["service_classes"] = service_classes
    if optional_container_names:
        config["optional_container_names"] = optional_container_names
    return config
