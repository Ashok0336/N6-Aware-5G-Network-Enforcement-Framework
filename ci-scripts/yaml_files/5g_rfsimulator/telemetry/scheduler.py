from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from policy_manager.utils import ensure_directory, load_structured_file, utc_timestamp
from telemetry.collectors.docker_stats_collector import DockerStatsCollector
from telemetry.collectors.iperf_collector import IperfCollector
from telemetry.collectors.onos_stats_collector import OnosStatsCollector
from telemetry.collectors.ovs_stats_collector import OvsStatsCollector
from telemetry.collectors.ping_collector import PingCollector
from telemetry.prometheus_exporter import (
    DEFAULT_METRICS_HTTP_HOST,
    DEFAULT_METRICS_HTTP_PORT,
)


class TelemetryScheduler:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path.resolve()
        raw_config = load_structured_file(self.config_path)
        telemetry_cfg = dict(raw_config.get("telemetry", {}))
        self.telemetry_cfg = telemetry_cfg
        self.poll_interval_seconds = float(telemetry_cfg.get("poll_interval_seconds", 5))
        self.command_timeout_seconds = float(telemetry_cfg.get("command_timeout_seconds", 8))
        self.metrics_http_host = str(
            telemetry_cfg.get("metrics_http_host", DEFAULT_METRICS_HTTP_HOST)
        )
        self.metrics_http_port = int(
            telemetry_cfg.get("metrics_http_port", DEFAULT_METRICS_HTTP_PORT)
        )
        self.output_dir = ensure_directory(
            self._resolve_path(telemetry_cfg.get("output_dir", "../logs/telemetry"))
        )
        self.latest_snapshot_path = self._resolve_path(
            telemetry_cfg.get("latest_snapshot_path", "../logs/telemetry/closed_loop_latest.json")
        )
        self.file_prefix = str(telemetry_cfg.get("file_prefix", "closed_loop_telemetry"))
        self.slices = dict(telemetry_cfg.get("slices", {}))
        self.ovs_cfg = dict(telemetry_cfg.get("ovs", {}))
        stamp = time.strftime("%Y%m%d_%H%M%S")
        self.run_id = f"telemetry-{stamp}"
        self.output_path = self.output_dir / f"{self.file_prefix}_{stamp}.jsonl"
        self.snapshot_index = 0

        self.ovs_collector = OvsStatsCollector(
            dict(telemetry_cfg.get("ovs", {})),
            command_timeout_seconds=self.command_timeout_seconds,
        )
        self.onos_collector = OnosStatsCollector(dict(telemetry_cfg.get("onos", {})))
        self.ping_collector = PingCollector(
            dict(telemetry_cfg.get("ping", {})),
            command_timeout_seconds=self.command_timeout_seconds,
        )
        self.iperf_collector = IperfCollector(
            dict(telemetry_cfg.get("iperf", {})),
            slices=self.slices,
            config_path=self.config_path,
        )
        self.docker_collector = DockerStatsCollector(
            dict(telemetry_cfg.get("docker", {})),
            command_timeout_seconds=self.command_timeout_seconds,
        )

    def _resolve_path(self, value: Any) -> Path:
        path = Path(str(value))
        if path.is_absolute():
            return path
        return (self.config_path.parent / path).resolve()

    def collect_once(self) -> Dict[str, Any]:
        started = time.monotonic()
        ovs = self.ovs_collector.collect()
        onos = self.onos_collector.collect()
        ping = self.ping_collector.collect()
        iperf = self.iperf_collector.collect()
        docker = self.docker_collector.collect()
        slice_metrics = self._build_slice_metrics(ovs=ovs, ping=ping, iperf=iperf)

        snapshot = {
            "event_type": "telemetry_snapshot",
            "timestamp": utc_timestamp(),
            "run_id": self.run_id,
            "snapshot_index": self.snapshot_index,
            "config_path": str(self.config_path),
            "telemetry": {
                "ovs": ovs,
                "onos": onos,
                "ping": ping,
                "iperf": iperf,
                "docker": docker,
            },
            "slice_metrics": slice_metrics,
        }
        snapshot["collection_duration_ms"] = int((time.monotonic() - started) * 1000)
        self.snapshot_index += 1
        self._write_snapshot(snapshot)
        return snapshot

    def _build_slice_metrics(
        self, ovs: Dict[str, Any], ping: Dict[str, Any], iperf: Dict[str, Any]
    ) -> Dict[str, Any]:
        ping_by_slice: Dict[str, Dict[str, Any]] = {}
        for probe in dict(ping.get("probes", {})).values():
            if not isinstance(probe, dict):
                continue
            slice_name = str(probe.get("slice_name") or "").strip()
            if slice_name:
                ping_by_slice[slice_name] = probe

        slice_metrics: Dict[str, Any] = {}
        slice_flows = dict(ovs.get("slice_flows", {}))
        queue_stats = dict(ovs.get("queues", {}))
        port_stats = dict(ovs.get("ports", {}))
        port_map = dict(ovs.get("port_name_to_ofport", {}))
        egress_port_name = str(self.ovs_cfg.get("egress_port_name", "v-edn-host"))
        egress_port_ofport = str(port_map.get(egress_port_name, ""))
        per_slice_iperf = dict(iperf.get("slices", {}))

        for slice_name, slice_cfg in self.slices.items():
            udp_port = str(slice_cfg.get("udp_port"))
            queue_id = str(slice_cfg.get("queue_id"))
            iperf_metrics = dict(per_slice_iperf.get(slice_name, {}))
            flow_metrics = dict(slice_flows.get(udp_port, {}))
            ping_metrics = dict(ping_by_slice.get(slice_name, {}))
            queue_metrics = self._lookup_queue_metrics(
                queue_stats, egress_port_name, egress_port_ofport, queue_id
            )
            egress_port_metrics = self._lookup_port_metrics(
                port_stats, egress_port_name, egress_port_ofport
            )

            sender_delta_packets = _to_float(iperf_metrics.get("sender_packets_delta"))
            sender_packet_rate_pps = _first_not_none(
                _to_float(iperf_metrics.get("sender_packet_rate_pps")),
                (sender_delta_packets / self.poll_interval_seconds)
                if sender_delta_packets is not None and self.poll_interval_seconds > 0
                else None,
            )
            flow_packets_per_second = _to_float(flow_metrics.get("packet_rate_pps"))
            queue_packets_per_second = _to_float(queue_metrics.get("packets_per_second"))
            traffic_active = any(
                _is_positive(value)
                for value in (
                    sender_delta_packets,
                    sender_packet_rate_pps,
                    flow_packets_per_second,
                    queue_packets_per_second,
                )
            ) or (slice_name == "urllc" and bool(ping_metrics.get("ok")))

            throughput_bps, throughput_source = self._select_throughput_bps(
                iperf_metrics=iperf_metrics,
                flow_metrics=flow_metrics,
            )
            delivery_ratio_percent, delivery_ratio_source = self._select_delivery_ratio_percent(
                iperf_metrics=iperf_metrics,
                flow_metrics=flow_metrics,
                queue_metrics=queue_metrics,
                sender_packet_rate_pps=sender_packet_rate_pps,
                traffic_active=traffic_active,
            )
            loss_percent = (
                _to_float(ping_metrics.get("loss_percent"))
                if ping_metrics
                else _to_float(iperf_metrics.get("loss_percent"))
            )
            if loss_percent is None and delivery_ratio_percent is not None:
                loss_percent = max(0.0, 100.0 - delivery_ratio_percent)

            slice_metrics[slice_name] = {
                "display_name": slice_cfg.get("display_name", slice_name),
                "udp_port": int(slice_cfg.get("udp_port", 0)),
                "queue_id": int(slice_cfg.get("queue_id", 0)),
                "traffic_active": traffic_active,
                "throughput_bps": throughput_bps,
                "throughput_source": throughput_source,
                "sender_packets_total": iperf_metrics.get("sender_packets_total"),
                "sender_packets_delta": sender_delta_packets,
                "sender_bytes_total": iperf_metrics.get("sender_bytes_total"),
                "sender_bytes_delta": iperf_metrics.get("sender_bytes_delta"),
                "sender_packet_rate_pps": sender_packet_rate_pps,
                "sender_average_bitrate_bps": iperf_metrics.get("sender_average_bitrate_bps"),
                "flow_packets_total": flow_metrics.get("packets_total"),
                "flow_packet_rate_pps": flow_packets_per_second,
                "flow_throughput_bps": flow_metrics.get("throughput_bps"),
                "queue_bytes_per_second": _to_float(queue_metrics.get("bytes_per_second")),
                "queue_packets_per_second": queue_packets_per_second,
                "queue_bytes_total": _to_float(queue_metrics.get("bytes_total")),
                "latency_avg_ms": ping_metrics.get("rtt_avg_ms"),
                "latency_max_ms": ping_metrics.get("rtt_max_ms"),
                "jitter_ms": ping_metrics.get("jitter_ms") or iperf_metrics.get("jitter_ms"),
                "loss_percent": loss_percent,
                "delivery_ratio_percent": delivery_ratio_percent,
                "delivery_ratio_source": delivery_ratio_source,
                "reliability_proxy_percent": delivery_ratio_percent
                if delivery_ratio_source
                in {"ovs_flow_vs_sender", "ovs_queue_vs_sender", "sender_continuity_proxy"}
                else None,
                "egress_port_tx_drop_rate_percent": _to_float(
                    egress_port_metrics.get("tx_drop_rate_percent")
                ),
                "sources": {
                    "ping": ping_metrics.get("ok"),
                    "iperf": iperf_metrics.get("source"),
                    "ovs_flow": bool(flow_metrics),
                    "queue": bool(queue_metrics),
                    "throughput": throughput_source,
                    "delivery_ratio": delivery_ratio_source,
                },
            }
        return slice_metrics

    def _write_snapshot(self, snapshot: Dict[str, Any]) -> None:
        with self.output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot, sort_keys=True) + "\n")
        ensure_directory(self.latest_snapshot_path.parent)
        self.latest_snapshot_path.write_text(
            json.dumps(snapshot, sort_keys=True, indent=2), encoding="utf-8"
        )

    def _select_throughput_bps(
        self, iperf_metrics: Dict[str, Any], flow_metrics: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[str]]:
        # Prefer active sender deltas first, then dataplane counters, then the sender's own summary average.
        candidates = (
            (_to_float(iperf_metrics.get("iperf_throughput_bps")), "iperf_text"),
            (_to_float(iperf_metrics.get("estimated_throughput_bps")), "udp_sender_delta"),
            (_to_float(flow_metrics.get("throughput_bps")), "ovs_flow_counter"),
            (_to_float(iperf_metrics.get("sender_average_bitrate_bps")), "udp_sender_summary_average"),
        )
        for value, source in candidates:
            if value is not None:
                return value, source
        return None, None

    def _select_delivery_ratio_percent(
        self,
        iperf_metrics: Dict[str, Any],
        flow_metrics: Dict[str, Any],
        queue_metrics: Dict[str, Any],
        sender_packet_rate_pps: Optional[float],
        traffic_active: bool,
    ) -> Tuple[Optional[float], Optional[str]]:
        explicit_loss = _to_float(iperf_metrics.get("loss_percent"))
        if explicit_loss is not None:
            return max(0.0, 100.0 - explicit_loss), "iperf_loss_percent"
        if not traffic_active or sender_packet_rate_pps is None or sender_packet_rate_pps <= 0:
            return None, None

        flow_packet_rate_pps = _to_float(flow_metrics.get("packet_rate_pps"))
        if flow_packet_rate_pps is not None and flow_packet_rate_pps > 0:
            return (
                _clamp_percent((flow_packet_rate_pps / sender_packet_rate_pps) * 100.0),
                "ovs_flow_vs_sender",
            )

        queue_packet_rate_pps = _to_float(queue_metrics.get("packets_per_second"))
        if queue_packet_rate_pps is not None and queue_packet_rate_pps > 0:
            return (
                _clamp_percent((queue_packet_rate_pps / sender_packet_rate_pps) * 100.0),
                "ovs_queue_vs_sender",
            )

        # When receiver-side evidence is unavailable, keep a clearly-labeled sender continuity proxy
        # so the policy loop can still reason about mMTC health during active UDP traffic.
        return 100.0, "sender_continuity_proxy"

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

    def _lookup_port_metrics(
        self, port_stats: Dict[str, Any], egress_port_name: str, egress_port_ofport: str
    ) -> Dict[str, Any]:
        for port_key in (egress_port_name, egress_port_ofport):
            if not port_key:
                continue
            metrics = port_stats.get(port_key, {})
            if isinstance(metrics, dict) and metrics:
                return metrics
        return {}


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_not_none(*values: Any) -> Optional[float]:
    for value in values:
        numeric = _to_float(value)
        if numeric is not None:
            return numeric
    return None


def _is_positive(value: Any) -> bool:
    numeric = _to_float(value)
    return bool(numeric is not None and numeric > 0)


def _clamp_percent(value: float) -> float:
    return max(0.0, min(100.0, float(value)))
