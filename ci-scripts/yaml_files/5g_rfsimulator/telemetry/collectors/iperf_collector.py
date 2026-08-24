from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


IPERF_UDP_RE = re.compile(
    r"(?P<transfer>[0-9]*\.?[0-9]+)\s+(?P<transfer_unit>[KMGTP]?Bytes?)\s+"
    r"(?P<bitrate>[0-9]*\.?[0-9]+)\s+(?P<bitrate_unit>[KMGTP]?bits/sec)\s+"
    r"(?P<jitter>[0-9]*\.?[0-9]+)\s+ms\s+"
    r"(?P<lost>\d+)/(?P<packets>\d+)\s+\((?P<lost_percent>[0-9.]+)%\)"
)
IPERF_TCP_RE = re.compile(
    r"(?P<transfer>[0-9]*\.?[0-9]+)\s+(?P<transfer_unit>[KMGTP]?Bytes?)\s+"
    r"(?P<bitrate>[0-9]*\.?[0-9]+)\s+(?P<bitrate_unit>[KMGTP]?bits/sec)"
)
SENT_PACKETS_RE = re.compile(r"sent_packets=(\d+)")
SENT_RE = re.compile(r"sent=(\d+)")
UDP_SENDER_SUMMARY_RE = re.compile(r"^SUMMARY\s+(?P<body>.*)$", re.MULTILINE)


class IperfCollector:
    def __init__(self, config: Dict[str, Any], slices: Dict[str, Any], config_path: Path) -> None:
        self.log_search_dirs = [
            (config_path.parent / str(path)).resolve()
            if not Path(str(path)).is_absolute()
            else Path(str(path))
            for path in config.get("log_search_dirs", [])
        ]
        self.file_globs = [str(item) for item in config.get("file_globs", ["*.log"])]
        self.slices = slices
        self.previous_counts: Dict[str, Dict[str, Any]] = {}
        self.last_collection_time: float | None = None

    def collect(self) -> Dict[str, Any]:
        now = time.monotonic()
        elapsed = now - self.last_collection_time if self.last_collection_time is not None else None
        self.last_collection_time = now

        slice_results: Dict[str, Any] = {}
        for slice_name, slice_cfg in self.slices.items():
            files = self._discover_files(slice_cfg)
            slice_results[slice_name] = self._collect_slice(slice_name, slice_cfg, files, elapsed)
        return {"slices": slice_results}

    def _discover_files(self, slice_cfg: Dict[str, Any]) -> List[Path]:
        file_names = {str(item) for item in slice_cfg.get("file_names", [])}
        keywords = [str(item).lower() for item in slice_cfg.get("keywords", [])]
        discovered: List[Path] = []
        for search_dir in self.log_search_dirs:
            if not search_dir.exists():
                continue
            for pattern in self.file_globs:
                for path in search_dir.rglob(pattern):
                    if not path.is_file():
                        continue
                    if path.name in file_names:
                        discovered.append(path)
                        continue
                    lowered = path.name.lower()
                    if keywords and any(keyword in lowered for keyword in keywords):
                        discovered.append(path)
        unique: Dict[str, Path] = {str(path.resolve()): path.resolve() for path in discovered}
        return sorted(unique.values())

    def _collect_slice(
        self, slice_name: str, slice_cfg: Dict[str, Any], files: Iterable[Path], elapsed: float | None
    ) -> Dict[str, Any]:
        payload_bytes = int(slice_cfg.get("payload_bytes", 0))
        results: Dict[str, Any] = {
            "files": [str(path) for path in files],
            "sender_packets_total": 0,
            "sender_bytes_total": 0,
            "sender_packets_delta": None,
            "sender_bytes_delta": None,
            "sender_packet_rate_pps": None,
            "sender_average_bitrate_bps": None,
            "estimated_throughput_bps": None,
            "iperf_throughput_bps": None,
            "jitter_ms": None,
            "loss_percent": None,
        }
        total_delta_packets = 0
        total_delta_bytes = 0
        have_delta_packets = False
        have_delta_bytes = False
        summary_bitrate_total = 0.0
        summary_packet_rate_total = 0.0
        summary_bitrate_found = False
        summary_packet_rate_found = False
        per_file: Dict[str, Any] = {}
        best_iperf: Dict[str, float] | None = None

        for path in files:
            text = _read_tail(path)
            path_key = str(path.resolve())
            sender_total = _extract_counter(text)
            sender_summary = _extract_udp_sender_summary(text)
            file_entry: Dict[str, Any] = {}
            sender_bytes_total = _to_int(sender_summary.get("bytes_sent"))
            sender_average_bitrate_bps = _to_float(sender_summary.get("average_bitrate_bps"))
            sender_packet_rate_pps = _to_float(sender_summary.get("packet_rate_per_second"))
            if sender_total is None:
                sender_total = _to_int(sender_summary.get("packets_sent"))
            if sender_total is not None:
                file_entry["sender_packets_total"] = sender_total
                results["sender_packets_total"] += sender_total
            if sender_bytes_total is not None:
                file_entry["sender_bytes_total"] = sender_bytes_total
                results["sender_bytes_total"] += sender_bytes_total
            if sender_average_bitrate_bps is not None:
                file_entry["sender_average_bitrate_bps"] = sender_average_bitrate_bps
                summary_bitrate_total += sender_average_bitrate_bps
                summary_bitrate_found = True
            if sender_packet_rate_pps is not None:
                file_entry["sender_packet_rate_pps"] = sender_packet_rate_pps
                summary_packet_rate_total += sender_packet_rate_pps
                summary_packet_rate_found = True

            previous = self.previous_counts.get(path_key)
            delta_packets = None
            delta_bytes = None
            if previous and elapsed and elapsed > 0:
                if sender_total is not None and previous.get("sender_packets_total") is not None:
                    delta_packets = max(0, sender_total - int(previous.get("sender_packets_total", 0)))
                    file_entry["sender_packets_delta"] = delta_packets
                    file_entry["packet_rate_pps"] = delta_packets / elapsed
                    total_delta_packets += delta_packets
                    have_delta_packets = True
                if sender_bytes_total is not None and previous.get("sender_bytes_total") is not None:
                    delta_bytes = max(0, sender_bytes_total - int(previous.get("sender_bytes_total", 0)))
                    file_entry["sender_bytes_delta"] = delta_bytes
                    total_delta_bytes += delta_bytes
                    have_delta_bytes = True
                if delta_bytes is not None:
                    file_entry["estimated_throughput_bps"] = (delta_bytes * 8) / elapsed
                elif delta_packets is not None:
                    file_entry["estimated_throughput_bps"] = (delta_packets * payload_bytes * 8) / elapsed

            if sender_total is not None or sender_bytes_total is not None:
                self.previous_counts[path_key] = {
                    "sender_packets_total": sender_total,
                    "sender_bytes_total": sender_bytes_total,
                }

            iperf_metrics = _extract_iperf_metrics(text)
            if iperf_metrics:
                file_entry.update(iperf_metrics)
                if best_iperf is None or iperf_metrics.get("throughput_bps", 0) > best_iperf.get("throughput_bps", 0):
                    best_iperf = iperf_metrics
            per_file[path_key] = file_entry

        if have_delta_packets and elapsed and elapsed > 0:
            results["sender_packets_delta"] = total_delta_packets
            results["sender_packet_rate_pps"] = total_delta_packets / elapsed
        if have_delta_bytes and elapsed and elapsed > 0:
            results["sender_bytes_delta"] = total_delta_bytes
            results["estimated_throughput_bps"] = (total_delta_bytes * 8) / elapsed
        elif have_delta_packets and elapsed and elapsed > 0:
            results["estimated_throughput_bps"] = (total_delta_packets * payload_bytes * 8) / elapsed
        if summary_bitrate_found:
            results["sender_average_bitrate_bps"] = summary_bitrate_total
        if summary_packet_rate_found and results["sender_packet_rate_pps"] is None:
            results["sender_packet_rate_pps"] = summary_packet_rate_total
        if best_iperf:
            results["iperf_throughput_bps"] = best_iperf.get("throughput_bps")
            results["jitter_ms"] = best_iperf.get("jitter_ms")
            results["loss_percent"] = best_iperf.get("loss_percent")
        results["per_file"] = per_file
        results["source"] = (
            "iperf_text"
            if best_iperf
            else "sender_delta"
            if (have_delta_packets or have_delta_bytes)
            else "sender_summary"
            if (
                results["sender_packets_total"] > 0
                or results["sender_bytes_total"] > 0
                or summary_bitrate_found
                or summary_packet_rate_found
            )
            else "none"
        )
        return results


def _read_tail(path: Path, max_bytes: int = 262144) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _extract_counter(text: str) -> int | None:
    values = [int(match.group(1)) for match in SENT_PACKETS_RE.finditer(text)]
    if values:
        return max(values)
    sent_values = [int(match.group(1)) for match in SENT_RE.finditer(text)]
    if sent_values:
        return max(sent_values)
    return None


def _extract_iperf_metrics(text: str) -> Dict[str, float] | None:
    udp_matches = list(IPERF_UDP_RE.finditer(text))
    if udp_matches:
        match = udp_matches[-1]
        return {
            "throughput_bps": _bitrate_to_bps(match.group("bitrate"), match.group("bitrate_unit")),
            "jitter_ms": float(match.group("jitter")),
            "loss_percent": float(match.group("lost_percent")),
        }
    tcp_matches = list(IPERF_TCP_RE.finditer(text))
    if tcp_matches:
        match = tcp_matches[-1]
        return {
            "throughput_bps": _bitrate_to_bps(match.group("bitrate"), match.group("bitrate_unit")),
        }
    return None


def _extract_udp_sender_summary(text: str) -> Dict[str, Any]:
    matches = UDP_SENDER_SUMMARY_RE.findall(text)
    if not matches:
        return {}
    summary: Dict[str, Any] = {}
    for token in matches[-1].split():
        if "=" not in token:
            continue
        key, raw_value = token.split("=", 1)
        summary[str(key)] = _parse_scalar(raw_value)
    if str(summary.get("type") or "") != "udp_sender":
        return {}
    packets_sent = _to_float(summary.get("packets_sent"))
    bytes_sent = _to_float(summary.get("bytes_sent"))
    duration_seconds = _to_float(summary.get("duration_seconds"))
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


def _parse_scalar(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if text.lower() in {"none", "null"}:
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)
    return text


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bitrate_to_bps(value: str, unit: str) -> float:
    base = float(value)
    cleaned = unit.lower()
    factor = 1.0
    if cleaned.startswith("k"):
        factor = 1_000.0
    elif cleaned.startswith("m"):
        factor = 1_000_000.0
    elif cleaned.startswith("g"):
        factor = 1_000_000_000.0
    elif cleaned.startswith("t"):
        factor = 1_000_000_000_000.0
    return base * factor
