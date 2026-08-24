from __future__ import annotations

import re
from typing import Any, Dict, List

from policy_manager.utils import run_command


PING_SUMMARY_RE = re.compile(
    r"(?P<tx>\d+)\s+packets transmitted,\s+(?P<rx>\d+)\s+(?:packets )?received,.*?(?P<loss>[0-9.]+)%\s+packet loss"
)
PING_REPLY_RTT_RE = re.compile(r"time=(?P<rtt>[0-9.]+)\s*ms")
PING_RTT_RE = re.compile(
    r"(?:round-trip|rtt) min/avg/max/(?:mdev|stddev)\s*=\s*(?P<min>[0-9.]+)/(?P<avg>[0-9.]+)/(?P<max>[0-9.]+)/(?P<jitter>[0-9.]+)\s*ms"
)


class PingCollector:
    def __init__(self, config: Dict[str, Any], command_timeout_seconds: float) -> None:
        self.probes = list(config.get("probes", []))
        self.command_timeout_seconds = command_timeout_seconds

    def collect(self) -> Dict[str, Any]:
        probes: Dict[str, Any] = {}
        for probe in self.probes:
            if not isinstance(probe, dict):
                continue
            name = str(probe.get("name") or "unnamed_probe")
            container_name = str(probe.get("container_name") or "").strip()
            interface_name = str(probe.get("interface_name") or "").strip()
            target_ip = str(probe.get("target_ip") or "").strip()
            count = int(probe.get("count", 3))
            timeout_seconds = int(probe.get("timeout_seconds", 1))
            if not container_name or not interface_name or not target_ip:
                probes[name] = {
                    "ok": False,
                    "error": "Probe is missing container_name, interface_name, or target_ip.",
                    "slice_name": probe.get("slice_name"),
                }
                continue

            result = run_command(
                [
                    "docker",
                    "exec",
                    container_name,
                    "ping",
                    "-I",
                    interface_name,
                    "-c",
                    str(count),
                    "-W",
                    str(timeout_seconds),
                    target_ip,
                ],
                timeout_seconds=self.command_timeout_seconds,
            )
            probes[name] = self._parse_probe_result(probe, result)
        return {"probes": probes}

    def _parse_probe_result(self, probe: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        combined_output = "\n".join(
            part for part in (result.get("stdout", ""), result.get("stderr", "")) if str(part).strip()
        )
        transmitted = received = None
        loss_percent = None
        rtt_values: List[float] = [
            float(match.group("rtt")) for match in PING_REPLY_RTT_RE.finditer(combined_output)
        ]
        summary_match = PING_SUMMARY_RE.search(combined_output)
        if summary_match:
            transmitted = int(summary_match.group("tx"))
            received = int(summary_match.group("rx"))
            loss_percent = float(summary_match.group("loss"))

        rtt_match = PING_RTT_RE.search(combined_output)
        rtt_stats: Dict[str, Any] = {}
        if rtt_match:
            rtt_stats = {
                "rtt_min_ms": float(rtt_match.group("min")),
                "rtt_avg_ms": float(rtt_match.group("avg")),
                "rtt_max_ms": float(rtt_match.group("max")),
                "jitter_ms": float(rtt_match.group("jitter")),
            }

        return {
            "ok": bool(result.get("ok")),
            "slice_name": probe.get("slice_name"),
            "container_name": probe.get("container_name"),
            "interface_name": probe.get("interface_name"),
            "target_ip": probe.get("target_ip"),
            "transmitted": transmitted,
            "received": received,
            "loss_percent": loss_percent,
            "reply_rtts_ms": rtt_values,
            **rtt_stats,
            "stdout": result.get("stdout", ""),
            "stderr": result.get("stderr", ""),
            "error": result.get("error", ""),
        }
