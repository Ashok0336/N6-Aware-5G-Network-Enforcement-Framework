from __future__ import annotations

import math
from typing import Any, Dict, Optional

try:
    from prometheus_client import Counter, Gauge, start_http_server
except ImportError as exc:  # pragma: no cover - dependency checked at runtime
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]
    PROMETHEUS_IMPORT_ERROR: Optional[ImportError] = exc
else:
    PROMETHEUS_IMPORT_ERROR = None


DEFAULT_METRICS_HTTP_HOST = "0.0.0.0"
DEFAULT_METRICS_HTTP_PORT = 8000
METRICS_PATH = "/metrics"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _prometheus_value(value: Any) -> float:
    numeric = _to_float(value)
    if numeric is None:
        return float("nan")
    if math.isfinite(numeric):
        return numeric
    return float("nan")


class TelemetryPrometheusExporter:
    def __init__(self, metrics_http_host: str, metrics_http_port: int, bridge_name: str) -> None:
        self.metrics_http_host = str(metrics_http_host or DEFAULT_METRICS_HTTP_HOST)
        self.metrics_http_port = int(metrics_http_port or DEFAULT_METRICS_HTTP_PORT)
        self.bridge_name = str(bridge_name or "br-n6")
        self.metrics_path = METRICS_PATH
        self.metrics_access_url = f"http://127.0.0.1:{self.metrics_http_port}{self.metrics_path}"
        self.metrics_bind_label = f"{self.metrics_http_host}:{self.metrics_http_port}"
        self.server_started = False
        self._register_metrics()

    def _register_metrics(self) -> None:
        if Gauge is None or Counter is None:
            return
        self.prometheus_sample_counter = Counter(
            "telemetry_collector_samples_total",
            "Total number of telemetry snapshots collected by the telemetry collector.",
        )
        self.prometheus_metric_update_error_counter = Counter(
            "telemetry_collector_metric_update_errors_total",
            "Total number of Prometheus metric update errors seen by the telemetry collector.",
        )
        self.prometheus_collection_duration_ms = Gauge(
            "telemetry_collection_duration_ms",
            "Duration of the most recent telemetry collection cycle in milliseconds.",
        )
        self.prometheus_urllc_latency_avg_ms = Gauge(
            "urllc_latency_avg_ms",
            "Average URLLC latency in milliseconds.",
            ["slice"],
        )
        self.prometheus_urllc_latency_max_ms = Gauge(
            "urllc_latency_max_ms",
            "Maximum URLLC latency in milliseconds.",
            ["slice"],
        )
        self.prometheus_urllc_jitter_ms = Gauge(
            "urllc_jitter_ms",
            "URLLC jitter in milliseconds.",
            ["slice"],
        )
        self.prometheus_urllc_loss_percent = Gauge(
            "urllc_loss_percent",
            "URLLC packet loss percentage.",
            ["slice"],
        )
        self.prometheus_embb_throughput_bps = Gauge(
            "embb_throughput_bps",
            "eMBB throughput in bits per second.",
            ["slice"],
        )
        self.prometheus_embb_packet_rate_pps = Gauge(
            "embb_packet_rate_pps",
            "eMBB packet rate in packets per second.",
            ["slice"],
        )
        self.prometheus_mmtc_delivery_ratio_percent = Gauge(
            "mmtc_delivery_ratio_percent",
            "mMTC delivery ratio percentage.",
            ["slice"],
        )
        self.prometheus_mmtc_packet_rate_pps = Gauge(
            "mmtc_packet_rate_pps",
            "mMTC packet rate in packets per second.",
            ["slice"],
        )
        self.prometheus_ovs_flow_packets_total = Gauge(
            "ovs_flow_packets_total",
            "Observed OVS flow packet totals from the latest telemetry snapshot.",
            ["slice", "udp_port", "device_id"],
        )
        self.prometheus_ovs_flow_bytes_total = Gauge(
            "ovs_flow_bytes_total",
            "Observed OVS flow byte totals from the latest telemetry snapshot.",
            ["slice", "udp_port", "device_id"],
        )
        self.prometheus_ovs_queue_bytes_total = Gauge(
            "ovs_queue_bytes_total",
            "Observed OVS queue byte totals from the latest telemetry snapshot.",
            ["slice", "queue_id", "device_id"],
        )
        self.prometheus_ovs_queue_packets_total = Gauge(
            "ovs_queue_packets_total",
            "Observed OVS queue packet totals from the latest telemetry snapshot.",
            ["slice", "queue_id", "device_id"],
        )
        self.prometheus_container_cpu_percent = Gauge(
            "container_cpu_percent",
            "Container CPU usage percentage from docker stats.",
            ["container_name"],
        )
        self.prometheus_container_memory_bytes = Gauge(
            "container_memory_bytes",
            "Container memory usage in bytes from docker stats.",
            ["container_name"],
        )

    def start(self) -> None:
        if self.server_started:
            return
        if PROMETHEUS_IMPORT_ERROR is not None or start_http_server is None:
            raise RuntimeError(
                "prometheus_client is required for the telemetry metrics endpoint. "
                "Install it before running automation/run_telemetry.sh."
            ) from PROMETHEUS_IMPORT_ERROR
        start_http_server(self.metrics_http_port, addr=self.metrics_http_host)
        self.server_started = True

    def record_update_error(self) -> None:
        if hasattr(self, "prometheus_metric_update_error_counter"):
            self.prometheus_metric_update_error_counter.inc()

    def update(self, snapshot: Dict[str, Any]) -> None:
        if Gauge is None or Counter is None:
            raise RuntimeError(
                "prometheus_client is required for Prometheus metrics export."
            )

        self.prometheus_sample_counter.inc()
        self.prometheus_collection_duration_ms.set(
            _prometheus_value(snapshot.get("collection_duration_ms"))
        )

        slice_metrics = snapshot.get("slice_metrics", {})
        if not isinstance(slice_metrics, dict):
            slice_metrics = {}
        urllc_metrics = dict(slice_metrics.get("urllc", {}))
        embb_metrics = dict(slice_metrics.get("embb", {}))
        mmtc_metrics = dict(slice_metrics.get("mmtc", {}))

        self.prometheus_urllc_latency_avg_ms.labels(slice="urllc").set(
            _prometheus_value(urllc_metrics.get("latency_avg_ms"))
        )
        self.prometheus_urllc_latency_max_ms.labels(slice="urllc").set(
            _prometheus_value(urllc_metrics.get("latency_max_ms"))
        )
        self.prometheus_urllc_jitter_ms.labels(slice="urllc").set(
            _prometheus_value(urllc_metrics.get("jitter_ms"))
        )
        self.prometheus_urllc_loss_percent.labels(slice="urllc").set(
            _prometheus_value(urllc_metrics.get("loss_percent"))
        )
        self.prometheus_embb_throughput_bps.labels(slice="embb").set(
            _prometheus_value(embb_metrics.get("throughput_bps"))
        )
        self.prometheus_embb_packet_rate_pps.labels(slice="embb").set(
            _prometheus_value(
                embb_metrics.get("sender_packet_rate_pps")
                or embb_metrics.get("flow_packet_rate_pps")
            )
        )
        self.prometheus_mmtc_delivery_ratio_percent.labels(slice="mmtc").set(
            _prometheus_value(mmtc_metrics.get("delivery_ratio_percent"))
        )
        self.prometheus_mmtc_packet_rate_pps.labels(slice="mmtc").set(
            _prometheus_value(
                mmtc_metrics.get("sender_packet_rate_pps")
                or mmtc_metrics.get("flow_packet_rate_pps")
            )
        )

        device_id = self._resolve_device_id_label(snapshot)
        self.prometheus_ovs_flow_packets_total.clear()
        self.prometheus_ovs_flow_bytes_total.clear()
        self.prometheus_ovs_queue_bytes_total.clear()
        self.prometheus_ovs_queue_packets_total.clear()

        flow_packets_total = 0.0
        flow_bytes_total = 0.0
        queue_bytes_total = 0.0
        queue_packets_total = 0.0

        queue_totals = self._collect_slice_queue_totals(snapshot, slice_metrics)
        for slice_name, metrics in slice_metrics.items():
            if not isinstance(metrics, dict):
                continue
            udp_port = str(metrics.get("udp_port") or "unknown")
            queue_id = str(metrics.get("queue_id") or "unknown")
            flow_packets = _to_float(metrics.get("flow_packets_total"))
            flow_bytes = _to_float(metrics.get("flow_bytes_total"))
            queue_bytes = _to_float(queue_totals.get(slice_name, {}).get("bytes_total"))
            queue_packets = _to_float(queue_totals.get(slice_name, {}).get("packets_total"))
            if queue_bytes is None:
                queue_bytes = _to_float(metrics.get("queue_bytes_total"))

            self.prometheus_ovs_flow_packets_total.labels(
                slice=str(slice_name),
                udp_port=udp_port,
                device_id=device_id,
            ).set(_prometheus_value(flow_packets))
            self.prometheus_ovs_flow_bytes_total.labels(
                slice=str(slice_name),
                udp_port=udp_port,
                device_id=device_id,
            ).set(_prometheus_value(flow_bytes))
            self.prometheus_ovs_queue_bytes_total.labels(
                slice=str(slice_name),
                queue_id=queue_id,
                device_id=device_id,
            ).set(_prometheus_value(queue_bytes))
            self.prometheus_ovs_queue_packets_total.labels(
                slice=str(slice_name),
                queue_id=queue_id,
                device_id=device_id,
            ).set(_prometheus_value(queue_packets))

            if flow_packets is not None:
                flow_packets_total += flow_packets
            if flow_bytes is not None:
                flow_bytes_total += flow_bytes
            if queue_bytes is not None:
                queue_bytes_total += queue_bytes
            if queue_packets is not None:
                queue_packets_total += queue_packets

        self.prometheus_ovs_flow_packets_total.labels(
            slice="all",
            udp_port="all",
            device_id=device_id,
        ).set(_prometheus_value(flow_packets_total))
        self.prometheus_ovs_flow_bytes_total.labels(
            slice="all",
            udp_port="all",
            device_id=device_id,
        ).set(_prometheus_value(flow_bytes_total))
        self.prometheus_ovs_queue_bytes_total.labels(
            slice="all",
            queue_id="all",
            device_id=device_id,
        ).set(_prometheus_value(queue_bytes_total))
        self.prometheus_ovs_queue_packets_total.labels(
            slice="all",
            queue_id="all",
            device_id=device_id,
        ).set(_prometheus_value(queue_packets_total))

        self.prometheus_container_cpu_percent.clear()
        self.prometheus_container_memory_bytes.clear()
        telemetry = snapshot.get("telemetry", {})
        docker = telemetry.get("docker", {}) if isinstance(telemetry, dict) else {}
        containers = docker.get("containers", {}) if isinstance(docker, dict) else {}
        if isinstance(containers, dict):
            for container_name, container_metrics in containers.items():
                if not isinstance(container_metrics, dict):
                    continue
                memory = container_metrics.get("memory", {})
                used_bytes = memory.get("used_bytes") if isinstance(memory, dict) else None
                self.prometheus_container_cpu_percent.labels(
                    container_name=str(container_name)
                ).set(_prometheus_value(container_metrics.get("cpu_percent")))
                self.prometheus_container_memory_bytes.labels(
                    container_name=str(container_name)
                ).set(_prometheus_value(used_bytes))

    def _collect_slice_queue_totals(
        self, snapshot: Dict[str, Any], slice_metrics: Dict[str, Any]
    ) -> Dict[str, Dict[str, float]]:
        telemetry = snapshot.get("telemetry", {})
        ovs = telemetry.get("ovs", {}) if isinstance(telemetry, dict) else {}
        queues = ovs.get("queues", {}) if isinstance(ovs, dict) else {}
        results: Dict[str, Dict[str, float]] = {}
        if not isinstance(queues, dict):
            return results

        for slice_name, metrics in slice_metrics.items():
            if not isinstance(metrics, dict):
                continue
            queue_id = str(metrics.get("queue_id") or "")
            if not queue_id:
                continue
            bytes_total = 0.0
            packets_total = 0.0
            found = False
            for port_queues in queues.values():
                if not isinstance(port_queues, dict):
                    continue
                queue_metrics = port_queues.get(queue_id)
                if not isinstance(queue_metrics, dict):
                    continue
                queue_bytes = _to_float(queue_metrics.get("bytes_total"))
                queue_packets = _to_float(queue_metrics.get("packets_total"))
                if queue_bytes is not None:
                    bytes_total += queue_bytes
                    found = True
                if queue_packets is not None:
                    packets_total += queue_packets
                    found = True
            if found:
                results[str(slice_name)] = {
                    "bytes_total": bytes_total,
                    "packets_total": packets_total,
                }
        return results

    def _resolve_device_id_label(self, snapshot: Dict[str, Any]) -> str:
        telemetry = snapshot.get("telemetry", {})
        onos = telemetry.get("onos", {}) if isinstance(telemetry, dict) else {}
        if isinstance(onos, dict):
            devices = onos.get("devices", [])
            if isinstance(devices, list):
                for device in devices:
                    if not isinstance(device, dict):
                        continue
                    if device.get("available") is True and device.get("id"):
                        return str(device["id"])
        ovs = telemetry.get("ovs", {}) if isinstance(telemetry, dict) else {}
        if isinstance(ovs, dict) and ovs.get("bridge_name"):
            return str(ovs["bridge_name"])
        return self.bridge_name
