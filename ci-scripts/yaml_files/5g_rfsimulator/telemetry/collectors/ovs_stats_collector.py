from __future__ import annotations

import re
import time
from typing import Any, Dict, Optional

from policy_manager.utils import parse_ovs_map, run_command


PORT_RX_RE = re.compile(
    r"^\s*port\s+(?P<port>\S+):\s+rx pkts=(?P<pkts>\d+), bytes=(?P<bytes>\d+), drop=(?P<drop>\d+), errs=(?P<errs>\d+), frame=(?P<frame>\d+), over=(?P<over>\d+), crc=(?P<crc>\d+)"
)
PORT_TX_RE = re.compile(
    r"^\s*tx pkts=(?P<pkts>\d+), bytes=(?P<bytes>\d+), drop=(?P<drop>\d+), errs=(?P<errs>\d+), coll=(?P<coll>\d+)"
)
QUEUE_RE = re.compile(
    r"^\s*port\s+(?P<port>\S+)\s+queue\s+(?P<queue>\S+):\s+bytes=(?P<bytes>\d+),\s*pkts=(?P<pkts>\d+),\s*errors=(?P<errors>\d+),\s*duration=(?P<duration>[0-9.]+)s"
)


class OvsStatsCollector:
    def __init__(
        self,
        config: Dict[str, Any],
        command_timeout_seconds: float,
        *,
        slices: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.container_name = str(config.get("container_name", "ovs"))
        self.bridge_name = str(config.get("bridge_name", "br-n6"))
        self.upf_port_name = str(config.get("upf_port_name", "v-upf-host"))
        self.egress_port_name = str(config.get("egress_port_name", "v-edn-host"))
        self.command_timeout_seconds = command_timeout_seconds
        self.slice_definitions = self._normalize_slice_definitions(slices or {})
        self.previous_slice_totals: Dict[str, Dict[str, int]] = {}
        self.previous_port_totals: Dict[str, Dict[str, int]] = {}
        self.previous_queue_totals: Dict[str, Dict[str, int]] = {}
        self.previous_timestamp: float | None = None

    def collect(self) -> Dict[str, Any]:
        current_time = time.monotonic()
        elapsed = current_time - self.previous_timestamp if self.previous_timestamp is not None else None

        flow_dump = self._docker_exec("ovs-ofctl", "-O", "OpenFlow13", "dump-flows", self.bridge_name)
        port_dump = self._docker_exec("ovs-ofctl", "-O", "OpenFlow13", "dump-ports", self.bridge_name)
        queue_dump = self._docker_exec("ovs-ofctl", "-O", "OpenFlow13", "queue-stats", self.bridge_name)
        controller_dump = self._docker_exec("ovs-vsctl", "list", "controller")
        port_map = self._resolve_port_numbers()
        qos_state = self._read_qos_configuration(port_map)

        slice_flows = self._parse_slice_flows(flow_dump.get("stdout", ""), elapsed, port_map)
        port_stats = self._parse_port_stats(port_dump.get("stdout", ""), elapsed, port_map)
        queue_stats = self._parse_queue_stats(queue_dump.get("stdout", ""), elapsed, port_map)
        slice_queue_validation = self._build_slice_queue_validation(queue_stats, qos_state, port_map)
        validation_summary = self._build_validation_summary(slice_flows, slice_queue_validation, port_map)
        controller_state = {
            "target": self._docker_exec("ovs-vsctl", "get-controller", self.bridge_name).get("stdout", "").strip(),
            "is_connected": "is_connected        : true" in controller_dump.get("stdout", "")
            or "is_connected : true" in controller_dump.get("stdout", ""),
            "details": controller_dump.get("stdout", ""),
        }

        self.previous_timestamp = current_time
        return {
            "ok": bool(flow_dump.get("ok")) and bool(port_dump.get("ok")) and bool(queue_dump.get("ok")),
            "bridge_name": self.bridge_name,
            "upf_port_name": self.upf_port_name,
            "egress_port_name": self.egress_port_name,
            "port_name_to_ofport": {name: info["ofport"] for name, info in port_map.items()},
            "ofport_to_port_name": {info["ofport"]: name for name, info in port_map.items()},
            "controller": controller_state,
            "flow_dump": flow_dump.get("stdout", ""),
            "port_dump": port_dump.get("stdout", ""),
            "queue_dump": queue_dump.get("stdout", ""),
            "qos": qos_state,
            "slice_flows": slice_flows.get("by_udp_port", {}),
            "slice_flow_validation": slice_flows.get("by_slice", {}),
            "ports": port_stats,
            "queues": queue_stats,
            "slice_queue_validation": slice_queue_validation,
            "validation_summary": validation_summary,
        }

    def _docker_exec(self, *args: str) -> Dict[str, Any]:
        return run_command(
            ["docker", "exec", self.container_name, *args],
            timeout_seconds=self.command_timeout_seconds,
        )

    def _normalize_slice_definitions(self, slices: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        normalized: Dict[str, Dict[str, Any]] = {}
        for slice_name, raw_cfg in dict(slices).items():
            cfg = dict(raw_cfg or {})
            udp_port = str(cfg.get("udp_port") or "").strip()
            queue_id = str(cfg.get("queue_id") or "").strip()
            if not udp_port or not queue_id:
                continue
            normalized[str(slice_name)] = {
                "slice_name": str(slice_name),
                "display_name": str(cfg.get("display_name") or slice_name),
                "udp_port": udp_port,
                "queue_id": queue_id,
            }
        return normalized

    def _resolve_port_numbers(self) -> Dict[str, Dict[str, Any]]:
        mapping: Dict[str, Dict[str, Any]] = {}
        for port_name in (self.upf_port_name, self.egress_port_name):
            result = self._docker_exec("ovs-vsctl", "get", "Interface", port_name, "ofport")
            raw_ofport = str(result.get("stdout", "")).strip().strip('"')
            if not result.get("ok") or raw_ofport in {"", "[]", "{}"}:
                continue
            try:
                ofport = str(int(raw_ofport))
            except ValueError:
                continue
            mapping[port_name] = {"ofport": ofport}
        return mapping

    def _read_qos_configuration(self, port_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "ok": False,
            "port_name": self.egress_port_name,
            "port_ofport": str(port_map.get(self.egress_port_name, {}).get("ofport", "")),
            "queues": {},
            "error": "",
        }
        qos_result = self._docker_exec("ovs-vsctl", "get", "port", self.egress_port_name, "qos")
        result["qos_result"] = qos_result
        if not qos_result.get("ok"):
            result["error"] = qos_result.get("error", "Failed to query the OVS QoS object.")
            return result

        qos_uuid = str(qos_result.get("stdout", "")).strip()
        if qos_uuid in {"", "[]", "{}"}:
            result["error"] = "No QoS object is attached to the egress port."
            return result

        parent_result = self._docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "other-config")
        queues_result = self._docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "queues")
        result["parent_result"] = parent_result
        result["queues_result"] = queues_result
        result["qos_uuid"] = qos_uuid
        if not parent_result.get("ok") or not queues_result.get("ok"):
            result["error"] = "Failed to query the OVS QoS queue configuration."
            return result

        parent_map = parse_ovs_map(parent_result.get("stdout", ""))
        queue_map = parse_ovs_map(queues_result.get("stdout", ""))
        result["parent_max_rate_bps"] = _to_int(parent_map.get("max-rate"))
        queues: Dict[str, Dict[str, Any]] = {}
        for queue_id, queue_uuid in queue_map.items():
            queue_result = self._docker_exec("ovs-vsctl", "get", "queue", queue_uuid, "other-config")
            queue_other_config = parse_ovs_map(queue_result.get("stdout", "")) if queue_result.get("ok") else {}
            queues[str(queue_id)] = {
                "uuid": str(queue_uuid),
                "min_rate_bps": _to_int(queue_other_config.get("min-rate")),
                "max_rate_bps": _to_int(queue_other_config.get("max-rate")),
                "query_ok": bool(queue_result.get("ok")),
                "error": queue_result.get("error", ""),
            }
        result["queues"] = queues
        result["ok"] = True
        return result

    def _parse_slice_flows(
        self,
        text: str,
        elapsed: float | None,
        port_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        upf_ofport = str(port_map.get(self.upf_port_name, {}).get("ofport", ""))
        edn_ofport = str(port_map.get(self.egress_port_name, {}).get("ofport", ""))
        parsed_entries = []
        for line in text.splitlines():
            if "actions=" not in line:
                continue
            udp_port = _extract(line, r"(?:tp_dst|udp_dst)=(\d+)")
            if not udp_port:
                continue
            parsed_entries.append(
                {
                    "line": line.strip(),
                    "udp_port": udp_port,
                    "queue_id": _extract(line, r"set_queue:(\d+)") or "",
                    "priority": _to_int(_extract(line, r"priority=(\d+)")),
                    "in_port": _extract(line, r"in_port=([^,\s]+)"),
                    "output_port": _extract(line, r"actions=(?:set_queue:\d+,)?output:([^,\s]+)"),
                    "packets_total": int(_extract(line, r"n_packets=(\d+)") or "0"),
                    "bytes_total": int(_extract(line, r"n_bytes=(\d+)") or "0"),
                    "duration_seconds": _to_float(_extract(line, r"duration=([0-9.]+)s")),
                }
            )

        by_slice: Dict[str, Dict[str, Any]] = {}
        by_udp_port: Dict[str, Dict[str, Any]] = {}
        for slice_name, slice_cfg in self.slice_definitions.items():
            udp_port = str(slice_cfg["udp_port"])
            expected_queue_id = str(slice_cfg["queue_id"])
            selector_entries = [entry for entry in parsed_entries if entry["udp_port"] == udp_port]
            matching_entries = [
                entry
                for entry in selector_entries
                if (not upf_ofport or entry.get("in_port") == upf_ofport)
                and (not edn_ofport or entry.get("output_port") == edn_ofport)
                and entry.get("queue_id") == expected_queue_id
            ]
            summary = self._summarize_flow_entries(
                slice_key=slice_name,
                slice_cfg=slice_cfg,
                selector_entries=selector_entries,
                matching_entries=matching_entries,
                elapsed=elapsed,
                upf_ofport=upf_ofport,
                edn_ofport=edn_ofport,
            )
            by_slice[slice_name] = summary
            by_udp_port[udp_port] = {
                "slice_name": slice_name,
                "display_name": slice_cfg["display_name"],
                "queue_id": summary.get("queue_id"),
                "packets_total": summary.get("packets_total"),
                "bytes_total": summary.get("bytes_total"),
                "packet_rate_pps": summary.get("packet_rate_pps"),
                "throughput_bps": summary.get("throughput_bps"),
                "rule_present": summary.get("rule_present"),
                "counters_nonzero": summary.get("counters_nonzero"),
                "counters_increasing": summary.get("counters_increasing"),
                "rate_source": summary.get("rate_source"),
            }
        return {"by_slice": by_slice, "by_udp_port": by_udp_port}

    def _summarize_flow_entries(
        self,
        *,
        slice_key: str,
        slice_cfg: Dict[str, Any],
        selector_entries: list[Dict[str, Any]],
        matching_entries: list[Dict[str, Any]],
        elapsed: float | None,
        upf_ofport: str,
        edn_ofport: str,
    ) -> Dict[str, Any]:
        packets_total = sum(int(entry.get("packets_total", 0)) for entry in matching_entries)
        bytes_total = sum(int(entry.get("bytes_total", 0)) for entry in matching_entries)
        duration_seconds = max(
            (float(entry.get("duration_seconds") or 0.0) for entry in matching_entries),
            default=0.0,
        )
        previous = self.previous_slice_totals.get(slice_key, {})
        packets_per_second, packet_rate_source, packet_counter_reset = _derive_rate(
            current_total=packets_total,
            previous_total=_to_int(previous.get("packets_total")),
            elapsed=elapsed,
            duration_seconds=duration_seconds,
        )
        bytes_per_second, byte_rate_source, byte_counter_reset = _derive_rate(
            current_total=bytes_total,
            previous_total=_to_int(previous.get("bytes_total")),
            elapsed=elapsed,
            duration_seconds=duration_seconds,
        )
        throughput_bps = bytes_per_second * 8.0 if bytes_per_second is not None else None
        observed_queue_ids = sorted({str(entry.get("queue_id") or "") for entry in selector_entries if entry.get("queue_id")})
        observed_priorities = sorted(
            {
                int(entry.get("priority"))
                for entry in selector_entries
                if entry.get("priority") is not None
            }
        )

        self.previous_slice_totals[slice_key] = {
            "packets_total": packets_total,
            "bytes_total": bytes_total,
        }

        return {
            "slice_name": slice_cfg["slice_name"],
            "display_name": slice_cfg["display_name"],
            "udp_port": int(slice_cfg["udp_port"]),
            "expected_queue_id": int(slice_cfg["queue_id"]),
            "expected_in_port_name": self.upf_port_name,
            "expected_in_port_ofport": upf_ofport or None,
            "expected_output_port_name": self.egress_port_name,
            "expected_output_port_ofport": edn_ofport or None,
            "rule_present": bool(matching_entries),
            "matching_flow_count": len(matching_entries),
            "matching_flow_lines": [str(entry["line"]) for entry in matching_entries],
            "selector_flow_count": len(selector_entries),
            "selector_flow_lines": [str(entry["line"]) for entry in selector_entries],
            "queue_id": int(slice_cfg["queue_id"]),
            "observed_queue_ids": observed_queue_ids,
            "queue_id_matches_expected": str(slice_cfg["queue_id"]) in observed_queue_ids,
            "observed_priorities": observed_priorities,
            "packets_total": packets_total,
            "bytes_total": bytes_total,
            "packet_rate_pps": packets_per_second,
            "throughput_bps": throughput_bps,
            "rate_source": byte_rate_source or packet_rate_source,
            "counters_nonzero": bool(packets_total > 0 or bytes_total > 0),
            "counters_increasing": bool(
                (packets_per_second is not None and packets_per_second > 0)
                or (bytes_per_second is not None and bytes_per_second > 0)
            ),
            "counter_reset_detected": packet_counter_reset or byte_counter_reset,
        }

    def _parse_port_stats(
        self,
        text: str,
        elapsed: float | None,
        port_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        ofport_to_name = {info["ofport"]: name for name, info in port_map.items()}
        stats: Dict[str, Any] = {}
        current_port = ""
        for line in text.splitlines():
            rx_match = PORT_RX_RE.match(line)
            if rx_match:
                current_port = rx_match.group("port")
                port_name = ofport_to_name.get(current_port, current_port)
                stats[port_name] = {
                    "ofport": current_port,
                    "rx_packets_total": int(rx_match.group("pkts")),
                    "rx_bytes_total": int(rx_match.group("bytes")),
                    "rx_drop_total": int(rx_match.group("drop")),
                }
                continue
            tx_match = PORT_TX_RE.match(line)
            if tx_match and current_port:
                port_name = ofport_to_name.get(current_port, current_port)
                stats.setdefault(port_name, {})["tx_packets_total"] = int(tx_match.group("pkts"))
                stats.setdefault(port_name, {})["tx_bytes_total"] = int(tx_match.group("bytes"))
                stats.setdefault(port_name, {})["tx_drop_total"] = int(tx_match.group("drop"))

        for port_name, values in stats.items():
            previous = self.previous_port_totals.get(port_name, {})
            rx_bytes_per_second, _, _ = _derive_rate(
                current_total=int(values.get("rx_bytes_total", 0)),
                previous_total=_to_int(previous.get("rx_bytes_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            tx_bytes_per_second, _, _ = _derive_rate(
                current_total=int(values.get("tx_bytes_total", 0)),
                previous_total=_to_int(previous.get("tx_bytes_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            rx_packets_per_second, _, _ = _derive_rate(
                current_total=int(values.get("rx_packets_total", 0)),
                previous_total=_to_int(previous.get("rx_packets_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            tx_packets_per_second, _, _ = _derive_rate(
                current_total=int(values.get("tx_packets_total", 0)),
                previous_total=_to_int(previous.get("tx_packets_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            rx_drops_per_second, _, _ = _derive_rate(
                current_total=int(values.get("rx_drop_total", 0)),
                previous_total=_to_int(previous.get("rx_drop_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            tx_drops_per_second, _, _ = _derive_rate(
                current_total=int(values.get("tx_drop_total", 0)),
                previous_total=_to_int(previous.get("tx_drop_total")),
                elapsed=elapsed,
                duration_seconds=None,
            )
            values["rx_bytes_per_second"] = rx_bytes_per_second
            values["tx_bytes_per_second"] = tx_bytes_per_second
            values["rx_packets_per_second"] = rx_packets_per_second
            values["tx_packets_per_second"] = tx_packets_per_second
            values["rx_drops_per_second"] = rx_drops_per_second
            values["tx_drops_per_second"] = tx_drops_per_second

            attempted_tx_packets = int(values.get("tx_packets_total", 0)) + int(values.get("tx_drop_total", 0))
            if attempted_tx_packets > 0:
                values["tx_drop_rate_percent"] = (
                    int(values.get("tx_drop_total", 0)) / attempted_tx_packets
                ) * 100.0
            self.previous_port_totals[port_name] = {
                "rx_bytes_total": int(values.get("rx_bytes_total", 0)),
                "tx_bytes_total": int(values.get("tx_bytes_total", 0)),
                "rx_packets_total": int(values.get("rx_packets_total", 0)),
                "tx_packets_total": int(values.get("tx_packets_total", 0)),
                "rx_drop_total": int(values.get("rx_drop_total", 0)),
                "tx_drop_total": int(values.get("tx_drop_total", 0)),
            }
        return stats

    def _parse_queue_stats(
        self,
        text: str,
        elapsed: float | None,
        port_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        ofport_to_name = {info["ofport"]: name for name, info in port_map.items()}
        stats: Dict[str, Any] = {}
        for line in text.splitlines():
            match = QUEUE_RE.match(line)
            if not match:
                continue
            port = match.group("port")
            port_name = ofport_to_name.get(port, port)
            queue_id = match.group("queue")
            bytes_total = int(match.group("bytes"))
            packets_total = int(match.group("pkts"))
            duration_seconds = float(match.group("duration"))
            previous = self.previous_queue_totals.get(f"{port_name}:{queue_id}", {})
            bytes_per_second, byte_rate_source, byte_counter_reset = _derive_rate(
                current_total=bytes_total,
                previous_total=_to_int(previous.get("bytes_total")),
                elapsed=elapsed,
                duration_seconds=duration_seconds,
            )
            packets_per_second, packet_rate_source, packet_counter_reset = _derive_rate(
                current_total=packets_total,
                previous_total=_to_int(previous.get("packets_total")),
                elapsed=elapsed,
                duration_seconds=duration_seconds,
            )
            entry = {
                "ofport": port,
                "port_name": port_name,
                "bytes_total": bytes_total,
                "packets_total": packets_total,
                "errors_total": int(match.group("errors")),
                "duration_seconds": duration_seconds,
                "bytes_per_second": bytes_per_second,
                "packets_per_second": packets_per_second,
                "rate_source": byte_rate_source or packet_rate_source,
                "counters_nonzero": bool(bytes_total > 0 or packets_total > 0),
                "counters_increasing": bool(
                    (bytes_per_second is not None and bytes_per_second > 0)
                    or (packets_per_second is not None and packets_per_second > 0)
                ),
                "counter_reset_detected": byte_counter_reset or packet_counter_reset,
            }
            stats.setdefault(port_name, {})[queue_id] = entry
            self.previous_queue_totals[f"{port_name}:{queue_id}"] = {
                "bytes_total": bytes_total,
                "packets_total": packets_total,
            }
        return stats

    def _build_slice_queue_validation(
        self,
        queue_stats: Dict[str, Any],
        qos_state: Dict[str, Any],
        port_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        egress_ofport = str(port_map.get(self.egress_port_name, {}).get("ofport", ""))
        qos_queues = dict(qos_state.get("queues", {}))
        for slice_name, slice_cfg in self.slice_definitions.items():
            queue_id = str(slice_cfg["queue_id"])
            queue_metrics = self._lookup_queue_metrics(queue_stats, self.egress_port_name, egress_ofport, queue_id)
            queue_config = dict(qos_queues.get(queue_id, {}))
            counters_nonzero = bool(queue_metrics.get("counters_nonzero"))
            counters_increasing = bool(queue_metrics.get("counters_increasing"))
            configured = bool(queue_config)
            validation_status = "missing"
            if configured and (counters_nonzero or counters_increasing):
                validation_status = "active"
            elif configured:
                validation_status = "configured_idle"
            results[slice_name] = {
                "slice_name": slice_cfg["slice_name"],
                "display_name": slice_cfg["display_name"],
                "udp_port": int(slice_cfg["udp_port"]),
                "queue_id": int(queue_id),
                "port_name": self.egress_port_name,
                "port_ofport": egress_ofport or None,
                "configured": configured,
                "queue_uuid": queue_config.get("uuid"),
                "min_rate_bps": queue_config.get("min_rate_bps"),
                "max_rate_bps": queue_config.get("max_rate_bps"),
                "bytes_total": _to_int(queue_metrics.get("bytes_total")),
                "packets_total": _to_int(queue_metrics.get("packets_total")),
                "bytes_per_second": _to_float(queue_metrics.get("bytes_per_second")),
                "packets_per_second": _to_float(queue_metrics.get("packets_per_second")),
                "rate_source": queue_metrics.get("rate_source"),
                "counters_nonzero": counters_nonzero,
                "counters_increasing": counters_increasing,
                "validation_status": validation_status,
            }
        return results

    def _build_validation_summary(
        self,
        slice_flows: Dict[str, Dict[str, Any]],
        slice_queue_validation: Dict[str, Any],
        port_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        by_slice = dict(slice_flows.get("by_slice", {}))
        flow_counter_slices_nonzero = [
            slice_name
            for slice_name, metrics in by_slice.items()
            if isinstance(metrics, dict) and metrics.get("counters_nonzero")
        ]
        flow_counter_slices_increasing = [
            slice_name
            for slice_name, metrics in by_slice.items()
            if isinstance(metrics, dict) and metrics.get("counters_increasing")
        ]
        flow_rule_present_slices = [
            slice_name
            for slice_name, metrics in by_slice.items()
            if isinstance(metrics, dict) and metrics.get("rule_present")
        ]
        queue_counter_slices_nonzero = [
            slice_name
            for slice_name, metrics in slice_queue_validation.items()
            if isinstance(metrics, dict) and metrics.get("counters_nonzero")
        ]
        queue_counter_slices_increasing = [
            slice_name
            for slice_name, metrics in slice_queue_validation.items()
            if isinstance(metrics, dict) and metrics.get("counters_increasing")
        ]
        queue_configured_slices = [
            slice_name
            for slice_name, metrics in slice_queue_validation.items()
            if isinstance(metrics, dict) and metrics.get("configured")
        ]
        return {
            "bridge_name": self.bridge_name,
            "upf_port_name": self.upf_port_name,
            "upf_port_ofport": str(port_map.get(self.upf_port_name, {}).get("ofport", "")) or None,
            "egress_port_name": self.egress_port_name,
            "egress_port_ofport": str(port_map.get(self.egress_port_name, {}).get("ofport", "")) or None,
            "slice_to_queue_map": {
                slice_name: int(cfg["queue_id"]) for slice_name, cfg in self.slice_definitions.items()
            },
            "slice_to_udp_port_map": {
                slice_name: int(cfg["udp_port"]) for slice_name, cfg in self.slice_definitions.items()
            },
            "flow_rule_present_slices": flow_rule_present_slices,
            "flow_counter_slices_nonzero": flow_counter_slices_nonzero,
            "flow_counter_slices_increasing": flow_counter_slices_increasing,
            "queue_configured_slices": queue_configured_slices,
            "queue_counter_slices_nonzero": queue_counter_slices_nonzero,
            "queue_counter_slices_increasing": queue_counter_slices_increasing,
        }

    def _lookup_queue_metrics(
        self,
        queue_stats: Dict[str, Any],
        egress_port_name: str,
        egress_port_ofport: str,
        queue_id: str,
    ) -> Dict[str, Any]:
        for port_key in (egress_port_name, egress_port_ofport):
            if not port_key:
                continue
            port_queues = queue_stats.get(port_key, {})
            if isinstance(port_queues, dict):
                queue_metrics = port_queues.get(queue_id, {})
                if isinstance(queue_metrics, dict) and queue_metrics:
                    return queue_metrics
        return {}


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1) if match else ""


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _derive_rate(
    *,
    current_total: int,
    previous_total: Optional[int],
    elapsed: Optional[float],
    duration_seconds: Optional[float],
) -> tuple[Optional[float], Optional[str], bool]:
    counter_reset_detected = False
    if elapsed is not None and elapsed > 0 and previous_total is not None:
        delta = current_total - previous_total
        if delta >= 0:
            return delta / elapsed, "delta", False
        counter_reset_detected = True
    if duration_seconds is not None and duration_seconds > 0:
        return current_total / duration_seconds, "average_duration", counter_reset_detected
    return None, None, counter_reset_detected
