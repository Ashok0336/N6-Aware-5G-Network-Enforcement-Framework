from __future__ import annotations

import re
import time
from typing import Any, Dict, Tuple

from policy_manager.utils import run_command


PORT_RX_RE = re.compile(
    r"^\s*port\s+(?P<port>\S+):\s+rx pkts=(?P<pkts>\d+), bytes=(?P<bytes>\d+), drop=(?P<drop>\d+), errs=(?P<errs>\d+), frame=(?P<frame>\d+), over=(?P<over>\d+), crc=(?P<crc>\d+)"
)
PORT_TX_RE = re.compile(
    r"^\s*tx pkts=(?P<pkts>\d+), bytes=(?P<bytes>\d+), drop=(?P<drop>\d+), errs=(?P<errs>\d+), coll=(?P<coll>\d+)"
)
QUEUE_RE = re.compile(
    r"port\s+(?P<port>\S+)\s+queue\s+(?P<queue>\S+):\s+bytes=(?P<bytes>\d+), pkts=(?P<pkts>\d+), errors=(?P<errors>\d+), duration=(?P<duration>[0-9.]+)s"
)


class OvsStatsCollector:
    def __init__(self, config: Dict[str, Any], command_timeout_seconds: float) -> None:
        self.container_name = str(config.get("container_name", "ovs"))
        self.bridge_name = str(config.get("bridge_name", "br-n6"))
        self.upf_port_name = str(config.get("upf_port_name", "v-upf-host"))
        self.egress_port_name = str(config.get("egress_port_name", "v-edn-host"))
        self.command_timeout_seconds = command_timeout_seconds
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

        slice_flows = self._parse_slice_flows(flow_dump.get("stdout", ""), elapsed)
        port_stats = self._parse_port_stats(port_dump.get("stdout", ""), elapsed, port_map)
        queue_stats = self._parse_queue_stats(queue_dump.get("stdout", ""), elapsed, port_map)
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
            "port_name_to_ofport": {name: info["ofport"] for name, info in port_map.items()},
            "controller": controller_state,
            "flow_dump": flow_dump.get("stdout", ""),
            "port_dump": port_dump.get("stdout", ""),
            "queue_dump": queue_dump.get("stdout", ""),
            "slice_flows": slice_flows,
            "ports": port_stats,
            "queues": queue_stats,
        }

    def _docker_exec(self, *args: str) -> Dict[str, Any]:
        return run_command(
            ["docker", "exec", self.container_name, *args],
            timeout_seconds=self.command_timeout_seconds,
        )

    def _resolve_port_numbers(self) -> Dict[str, Dict[str, Any]]:
        mapping: Dict[str, Dict[str, Any]] = {}
        for port_name in (self.upf_port_name, self.egress_port_name):
            result = self._docker_exec("ovs-vsctl", "get", "Interface", port_name, "ofport")
            ofport = str(result.get("stdout", "")).strip()
            if ofport:
                mapping[port_name] = {"ofport": ofport}
        return mapping

    def _parse_slice_flows(self, text: str, elapsed: float | None) -> Dict[str, Any]:
        flow_lines = [line.strip() for line in text.splitlines() if "n_packets=" in line and "actions=" in line]
        slice_totals: Dict[str, Dict[str, int]] = {}
        for line in flow_lines:
            udp_port = _extract(line, r"(?:tp_dst|udp_dst)=(\d+)")
            if not udp_port:
                continue
            bytes_total = int(_extract(line, r"n_bytes=(\d+)") or "0")
            packets_total = int(_extract(line, r"n_packets=(\d+)") or "0")
            queue_id = _extract(line, r"set_queue:(\d+)") or ""
            entry = slice_totals.setdefault(udp_port, {"packets_total": 0, "bytes_total": 0, "queue_id": int(queue_id) if queue_id else None})
            entry["packets_total"] += packets_total
            entry["bytes_total"] += bytes_total
            if queue_id and not entry.get("queue_id"):
                entry["queue_id"] = int(queue_id)

        results: Dict[str, Any] = {}
        for udp_port, totals in slice_totals.items():
            previous = self.previous_slice_totals.get(udp_port, {})
            packet_rate = None
            throughput_bps = None
            if elapsed and elapsed > 0 and previous:
                packet_rate = (totals["packets_total"] - int(previous.get("packets_total", 0))) / elapsed
                throughput_bps = ((totals["bytes_total"] - int(previous.get("bytes_total", 0))) * 8) / elapsed
            results[udp_port] = {
                **totals,
                "packet_rate_pps": packet_rate,
                "throughput_bps": throughput_bps,
            }
            self.previous_slice_totals[udp_port] = {
                "packets_total": totals["packets_total"],
                "bytes_total": totals["bytes_total"],
            }
        return results

    def _parse_port_stats(
        self, text: str, elapsed: float | None, port_map: Dict[str, Dict[str, Any]]
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
            if elapsed and elapsed > 0 and previous:
                values["rx_bytes_per_second"] = (
                    int(values.get("rx_bytes_total", 0)) - int(previous.get("rx_bytes_total", 0))
                ) / elapsed
                values["tx_bytes_per_second"] = (
                    int(values.get("tx_bytes_total", 0)) - int(previous.get("tx_bytes_total", 0))
                ) / elapsed
                values["rx_packets_per_second"] = (
                    int(values.get("rx_packets_total", 0)) - int(previous.get("rx_packets_total", 0))
                ) / elapsed
                values["tx_packets_per_second"] = (
                    int(values.get("tx_packets_total", 0)) - int(previous.get("tx_packets_total", 0))
                ) / elapsed
                values["rx_drops_per_second"] = (
                    int(values.get("rx_drop_total", 0)) - int(previous.get("rx_drop_total", 0))
                ) / elapsed
                values["tx_drops_per_second"] = (
                    int(values.get("tx_drop_total", 0)) - int(previous.get("tx_drop_total", 0))
                ) / elapsed
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
        self, text: str, elapsed: float | None, port_map: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        ofport_to_name = {info["ofport"]: name for name, info in port_map.items()}
        stats: Dict[str, Any] = {}
        for line in text.splitlines():
            match = QUEUE_RE.search(line)
            if not match:
                continue
            port = match.group("port")
            port_name = ofport_to_name.get(port, port)
            queue_id = match.group("queue")
            entry = {
                "ofport": port,
                "bytes_total": int(match.group("bytes")),
                "packets_total": int(match.group("pkts")),
                "errors_total": int(match.group("errors")),
                "duration_seconds": float(match.group("duration")),
            }
            previous = self.previous_queue_totals.get(f"{port_name}:{queue_id}", {})
            if elapsed and elapsed > 0 and previous:
                entry["bytes_per_second"] = (
                    entry["bytes_total"] - int(previous.get("bytes_total", 0))
                ) / elapsed
                entry["packets_per_second"] = (
                    entry["packets_total"] - int(previous.get("packets_total", 0))
                ) / elapsed
            stats.setdefault(port_name, {})[queue_id] = entry
            self.previous_queue_totals[f"{port_name}:{queue_id}"] = {
                "bytes_total": entry["bytes_total"],
                "packets_total": entry["packets_total"],
            }
        return stats


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1) if match else ""
