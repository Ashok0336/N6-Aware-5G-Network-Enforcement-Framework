#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Optional, Tuple


MODES = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]
SERVICE_CLASSES = ["real_time_control", "high_throughput_data", "sensor_telemetry"]
SLICE_TO_SERVICE = {"urllc": "real_time_control", "embb": "high_throughput_data", "mmtc": "sensor_telemetry"}
SERVICE_TO_SLICE = {value: key for key, value in SLICE_TO_SERVICE.items()}

RTC_AVG_LATENCY_MS = 10.0
RTC_P95_LATENCY_MS = 20.0
RTC_LOSS_PERCENT = 1.0
DATA_THROUGHPUT_BPS = 50_000_000.0
SENSOR_DELIVERY_PERCENT = 98.0
DEFAULT_DURATION_SECONDS = 180.0
DEFAULT_WINDOW_SECONDS = 10.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Window-level CCNC SLA analyzer.")
    parser.add_argument("result_dir", nargs="?", help="Mode directory or batch directory.")
    parser.add_argument("--batch-root", default=None, help="Batch directory containing per-mode subdirectories.")
    parser.add_argument("--window-seconds", type=float, default=DEFAULT_WINDOW_SECONDS)
    parser.add_argument("--duration-seconds", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    if not args.result_dir and not args.batch_root:
        parser.error("provide result_dir or --batch-root")
    return args


def resolve_path(value: str) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()


def parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def as_float(value: Any) -> Optional[float]:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def percentile(values: List[float], pct: float) -> Optional[float]:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * pct
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return clean[lower]
    return clean[lower] + (clean[upper] - clean[lower]) * (rank - lower)


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def mode_dirs(result_dir: Path) -> List[Tuple[str, Path]]:
    found = [(mode, result_dir / mode) for mode in MODES if (result_dir / mode).is_dir()]
    if found:
        return found
    return [(result_dir.name, result_dir)]


def load_status(mode_dir: Path) -> Dict[str, Any]:
    try:
        return json.loads((mode_dir / "mode_status.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def first_number(record: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = as_float(record.get(key))
        if value is not None:
            return value
    return None


def service_metrics_from_payload(payload: Dict[str, Any], service: str) -> Dict[str, Any]:
    service_metrics = payload.get("service_metrics")
    if isinstance(service_metrics, dict) and isinstance(service_metrics.get(service), dict):
        return service_metrics[service]
    slice_metrics = payload.get("slice_metrics")
    slice_name = SERVICE_TO_SLICE[service]
    if isinstance(slice_metrics, dict) and isinstance(slice_metrics.get(slice_name), dict):
        return slice_metrics[slice_name]
    return {}


def extract_raw_rtts(payload: Dict[str, Any]) -> List[float]:
    probes = (
        payload.get("telemetry", {})
        .get("ping", {})
        .get("probes", {})
    )
    if not isinstance(probes, dict):
        return []
    values: List[float] = []
    for probe in probes.values():
        if not isinstance(probe, dict):
            continue
        if str(probe.get("slice_name") or "").lower() not in {"urllc", "real_time_control", ""}:
            continue
        for item in probe.get("reply_rtts_ms") or []:
            number = as_float(item)
            if number is not None:
                values.append(number)
    return values


def collect_samples(mode_dir: Path, duration_seconds: float) -> List[Dict[str, Any]]:
    telemetry_paths = sorted((mode_dir / "telemetry").glob("*.jsonl"))
    telemetry_paths += sorted(mode_dir.glob("closed_loop_telemetry_*.jsonl"))
    records: List[Dict[str, Any]] = []
    for path in telemetry_paths:
        records.extend(iter_jsonl(path))
    records = [record for record in records if record.get("event_type") != "collector_stop"]
    traffic_samples = samples_from_traffic_logs(mode_dir)
    if records:
        return samples_from_telemetry(records, duration_seconds) + traffic_samples
    return samples_from_summary_csvs(mode_dir, duration_seconds) + traffic_samples


def samples_from_traffic_logs(mode_dir: Path) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    for path in sorted((mode_dir / "traffic" / "high_throughput_data").glob("*.log")):
        samples.extend(data_samples_from_sender_log(path))
    return samples


def data_samples_from_sender_log(path: Path) -> List[Dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return []
    payload_bytes: Optional[float] = None
    burst_interval: Optional[float] = None
    last_packets: Optional[float] = None
    sample_index = 0
    samples: List[Dict[str, Any]] = []
    for line in lines:
        if line.startswith("START "):
            payload_bytes = key_value_float(line, "payload_bytes")
            burst_interval = key_value_float(line, "burst_interval_seconds")
            continue
        match = re.search(r"\bsent_packets=(\d+)", line)
        if not match or payload_bytes is None or burst_interval is None or burst_interval <= 0:
            continue
        packets = float(match.group(1))
        if last_packets is None:
            last_packets = packets
            sample_index += 1
            continue
        delta_packets = max(0.0, packets - last_packets)
        last_packets = packets
        throughput_bps = (delta_packets * payload_bytes * 8.0) / burst_interval
        samples.append(
            {
                "time_seconds": sample_index * burst_interval,
                "data_throughput_bps": throughput_bps,
                "data_sample_source": str(path),
            }
        )
        sample_index += 1
    return samples


def key_value_float(text: str, key: str) -> Optional[float]:
    match = re.search(rf"\b{re.escape(key)}=([^\s]+)", text)
    return as_float(match.group(1)) if match else None


def samples_from_telemetry(records: List[Dict[str, Any]], duration_seconds: float) -> List[Dict[str, Any]]:
    timestamps = [parse_time(item.get("timestamp") or item.get("last_updated")) for item in records]
    first_ts = next((item for item in timestamps if item is not None), None)
    samples: List[Dict[str, Any]] = []
    for index, payload in enumerate(records):
        timestamp = timestamps[index]
        if first_ts and timestamp:
            t = max(0.0, (timestamp - first_ts).total_seconds())
        else:
            t = duration_seconds * index / max(1, len(records) - 1)
        rtc = service_metrics_from_payload(payload, "real_time_control")
        data = service_metrics_from_payload(payload, "high_throughput_data")
        sensor = service_metrics_from_payload(payload, "sensor_telemetry")
        samples.append(
            {
                "time_seconds": t,
                "rtc_avg_latency_ms": first_number(rtc, "latency_avg_ms", "avg_latency_ms", "rtt_avg_ms"),
                "rtc_max_latency_ms": first_number(rtc, "latency_max_ms", "max_latency_ms", "rtt_max_ms"),
                "rtc_loss_percent": first_number(rtc, "loss_percent", "packet_loss_percent"),
                "rtc_raw_latency_samples_ms": extract_raw_rtts(payload),
                "data_throughput_bps": first_number(data, "throughput_bps", "flow_throughput_bps", "sender_average_bitrate_bps"),
                "sensor_delivery_ratio_percent": first_number(sensor, "delivery_ratio_percent", "reliability_proxy_percent"),
                "sensor_loss_percent": first_number(sensor, "loss_percent", "packet_loss_percent"),
            }
        )
    return samples


def samples_from_summary_csvs(mode_dir: Path, duration_seconds: float) -> List[Dict[str, Any]]:
    metric_rows: Dict[Tuple[str, str], Dict[str, str]] = {}
    for file_name in ("control_latency.csv", "data_throughput.csv", "sensor_telemetry.csv", "service_metrics.csv"):
        for row in read_csv_rows(mode_dir / file_name):
            service = row.get("service_class", "")
            metric = row.get("metric", "")
            if service and metric:
                metric_rows[(service, metric)] = row

    samples: List[Dict[str, Any]] = []
    metric_to_key = {
        ("real_time_control", "latency_avg_ms"): "rtc_avg_latency_ms",
        ("real_time_control", "latency_max_ms"): "rtc_max_latency_ms",
        ("real_time_control", "loss_percent"): "rtc_loss_percent",
        ("high_throughput_data", "throughput_bps"): "data_throughput_bps",
        ("sensor_telemetry", "delivery_ratio_percent"): "sensor_delivery_ratio_percent",
        ("sensor_telemetry", "loss_percent"): "sensor_loss_percent",
    }
    for metric_id, sample_key in metric_to_key.items():
        row = metric_rows.get(metric_id)
        if not row:
            continue
        value = as_float(row.get("mean"))
        count = int(as_float(row.get("sample_count")) or 0)
        if value is None or count <= 0:
            continue
        for index in range(count):
            t = duration_seconds * index / max(1, count - 1)
            samples.append({"time_seconds": t, sample_key: value})
    return samples


def values(rows: List[Dict[str, Any]], key: str) -> List[float]:
    return [value for value in (as_float(row.get(key)) for row in rows) if value is not None]


def raw_latency_values(rows: List[Dict[str, Any]]) -> List[float]:
    all_values: List[float] = []
    for row in rows:
        for value in row.get("rtc_raw_latency_samples_ms") or []:
            number = as_float(value)
            if number is not None:
                all_values.append(number)
    return all_values


def data_throughput_for_window(rows: List[Dict[str, Any]]) -> Optional[float]:
    traffic_by_source: Dict[str, List[float]] = defaultdict(list)
    telemetry_values: List[float] = []
    for row in rows:
        value = as_float(row.get("data_throughput_bps"))
        if value is None:
            continue
        source = str(row.get("data_sample_source") or "")
        if source:
            traffic_by_source[source].append(value)
        else:
            telemetry_values.append(value)
    if traffic_by_source:
        return sum(mean(values_) for values_ in traffic_by_source.values() if values_)
    if telemetry_values:
        return mean(telemetry_values)
    return None


def severity_upper(value: Optional[float], threshold: float) -> float:
    if value is None:
        return 0.0
    return max(0.0, (value - threshold) / threshold)


def severity_lower(value: Optional[float], threshold: float) -> float:
    if value is None:
        return 0.0
    return max(0.0, (threshold - value) / threshold)


def service_rate(rows: List[Dict[str, Any]], service: str) -> float:
    valid = [row for row in rows if row["service_class"] == service and row["valid_window"]]
    if not valid:
        return 0.0
    return sum(1 for row in valid if row["sla_violation"]) / len(valid)


def summarize_mode(
    mode: str,
    mode_dir: Path,
    window_seconds: float,
    duration_seconds: float,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    status = load_status(mode_dir)
    run_id = str(status.get("run_id") or status.get("mode") or mode)
    duration = float(status.get("duration_seconds") or duration_seconds)
    samples = collect_samples(mode_dir, duration)
    windows: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        t = as_float(sample.get("time_seconds")) or 0.0
        if duration > 0:
            t = min(max(0.0, t), max(0.0, duration - 1e-9))
        windows[int(t // window_seconds)].append(sample)

    total_windows = int(math.ceil(duration / window_seconds))
    by_mode: List[Dict[str, Any]] = []
    by_metric: List[Dict[str, Any]] = []
    severity_rows: List[Dict[str, Any]] = []
    by_service: List[Dict[str, Any]] = []
    summary_values: Dict[str, List[float]] = defaultdict(list)
    metric_counts = Counter()
    service_counts = Counter()
    violation_severities: List[float] = []

    valid_windows = 0
    violating_windows = 0
    missing_control = 0
    missing_data = 0
    missing_sensor = 0

    for window_index in range(total_windows):
        rows = windows.get(window_index, [])
        rtc_avg_values = values(rows, "rtc_avg_latency_ms")
        rtc_max_values = values(rows, "rtc_max_latency_ms")
        rtc_loss_values = values(rows, "rtc_loss_percent")
        rtc_raw_values = raw_latency_values(rows)
        data_values = values(rows, "data_throughput_bps")
        sensor_delivery_values = values(rows, "sensor_delivery_ratio_percent")
        sensor_loss_values = values(rows, "sensor_loss_percent")

        rtc_valid = bool(rtc_avg_values or rtc_max_values or rtc_loss_values or rtc_raw_values)
        data_valid = bool(data_values)
        sensor_valid = bool(sensor_delivery_values or sensor_loss_values)
        valid_window = rtc_valid or data_valid or sensor_valid
        if not rtc_valid:
            missing_control += 1
        if not data_valid:
            missing_data += 1
        if not sensor_valid:
            missing_sensor += 1

        rtc_avg = mean(rtc_avg_values) if rtc_avg_values else None
        rtc_max = max(rtc_max_values) if rtc_max_values else (max(rtc_raw_values) if rtc_raw_values else None)
        rtc_p95 = percentile(rtc_raw_values, 0.95) if rtc_raw_values else None
        p95_available = rtc_p95 is not None
        rtc_loss = max(rtc_loss_values) if rtc_loss_values else None
        data_throughput = data_throughput_for_window(rows)
        sensor_delivery = (
            mean(sensor_delivery_values)
            if sensor_delivery_values
            else (100.0 - max(sensor_loss_values) if sensor_loss_values else None)
        )

        metric_specs = [
            ("rtc_avg_latency", "real_time_control", rtc_avg, RTC_AVG_LATENCY_MS, "upper"),
            ("rtc_p95_latency", "real_time_control", rtc_p95, RTC_P95_LATENCY_MS, "upper"),
            ("rtc_loss", "real_time_control", rtc_loss, RTC_LOSS_PERCENT, "upper"),
            ("data_throughput", "high_throughput_data", data_throughput, DATA_THROUGHPUT_BPS, "lower"),
            ("sensor_delivery", "sensor_telemetry", sensor_delivery, SENSOR_DELIVERY_PERCENT, "lower"),
        ]
        severities: Dict[str, float] = {}
        metric_validity: Dict[str, bool] = {}
        for metric, service, observed, threshold, direction in metric_specs:
            metric_validity[metric] = observed is not None
            if observed is None:
                severities[metric] = 0.0
            elif direction == "upper":
                severities[metric] = severity_upper(observed, threshold)
            else:
                severities[metric] = severity_lower(observed, threshold)

        flags = {name: metric_validity[name] and severity > 0.0 for name, severity in severities.items()}
        service_validity = {
            "real_time_control": rtc_valid,
            "high_throughput_data": data_valid,
            "sensor_telemetry": sensor_valid,
        }
        service_flags = {
            "real_time_control": flags["rtc_avg_latency"] or flags["rtc_p95_latency"] or flags["rtc_loss"],
            "high_throughput_data": flags["data_throughput"],
            "sensor_telemetry": flags["sensor_delivery"],
        }
        violates = valid_window and any(service_flags[service] for service, valid in service_validity.items() if valid)
        max_severity = max((severities[name] for name, valid in metric_validity.items() if valid), default=0.0)

        if valid_window:
            valid_windows += 1
            violation_severities.append(max_severity)
            if violates:
                violating_windows += 1
        for metric_name, flag in flags.items():
            if flag:
                metric_counts[metric_name] += 1
        for service, service_valid in service_validity.items():
            if service_valid:
                service_counts[f"{service}_valid"] += 1
                if service_flags[service]:
                    service_counts[f"{service}_violating"] += 1

        metric_values = {
            "rtc_avg_latency_ms": rtc_avg,
            "rtc_max_latency_ms": rtc_max,
            "rtc_p95_latency_ms": rtc_p95,
            "rtc_loss_percent": rtc_loss,
            "data_throughput_mbps": data_throughput / 1_000_000.0 if data_throughput is not None else None,
            "sensor_delivery_ratio_percent": sensor_delivery,
        }
        if valid_window:
            for key, value in metric_values.items():
                if value is not None:
                    summary_values[key].append(float(value))

        by_mode.append(
            {
                "mode": mode,
                "run_id": run_id,
                "window_index": window_index,
                "window_start_seconds": window_index * window_seconds,
                "window_end_seconds": min(duration, (window_index + 1) * window_seconds),
                "valid_window": valid_window,
                "real_time_control_valid": rtc_valid,
                "high_throughput_data_valid": data_valid,
                "sensor_telemetry_valid": sensor_valid,
                **metric_values,
                "p95_available": p95_available,
                "sla_violation": violates if valid_window else "",
                "violation_severity": max_severity if valid_window else "",
            }
        )
        for metric, service, observed, threshold, direction in metric_specs:
            by_metric.append(
                {
                    "mode": mode,
                    "run_id": run_id,
                    "window_index": window_index,
                    "service_class": service,
                    "metric": metric,
                    "valid_metric": metric_validity[metric],
                    "observed_value": observed,
                    "threshold": threshold,
                    "threshold_direction": direction,
                    "violation": flags[metric],
                    "severity": severities[metric] if metric_validity[metric] else "",
                }
            )
        for service in SERVICE_CLASSES:
            service_severity = max(
                [severities[name] for name, svc, *_ in metric_specs if svc == service and metric_validity[name]],
                default=0.0,
            )
            by_service.append(
                {
                    "mode": mode,
                    "run_id": run_id,
                    "window_index": window_index,
                    "service_class": service,
                    "valid_window": service_validity[service],
                    "sla_violation": service_flags[service] if service_validity[service] else "",
                    "violation_severity": service_severity if service_validity[service] else "",
                }
            )
        severity_rows.append(
            {
                "mode": mode,
                "run_id": run_id,
                "window_index": window_index,
                "valid_window": valid_window,
                "violation_severity": max_severity if valid_window else "",
                "dominant_metric": max(
                    [name for name, valid in metric_validity.items() if valid],
                    key=lambda name: severities[name],
                    default="",
                ),
            }
        )

    missing_windows = total_windows - valid_windows
    summary = {
        "mode": mode,
        "run_id": run_id,
        "total_windows": total_windows,
        "valid_windows": valid_windows,
        "missing_windows": missing_windows,
        "missing_control_windows": missing_control,
        "missing_data_windows": missing_data,
        "missing_sensor_windows": missing_sensor,
        "violating_windows": violating_windows,
        "window_sla_violation_rate": violating_windows / valid_windows if valid_windows else "",
        "avg_violation_duration_seconds": window_seconds if violating_windows else 0.0,
        "total_violation_duration_seconds": violating_windows * window_seconds,
        "avg_violation_severity": mean(violation_severities) if violation_severities else "",
        "max_violation_severity": max(violation_severities) if violation_severities else "",
        "rtc_avg_latency_violation_windows": metric_counts["rtc_avg_latency"],
        "rtc_p95_latency_violation_windows": metric_counts["rtc_p95_latency"],
        "rtc_loss_violation_windows": metric_counts["rtc_loss"],
        "data_throughput_violation_windows": metric_counts["data_throughput"],
        "sensor_delivery_violation_windows": metric_counts["sensor_delivery"],
        "real_time_control_window_sla_violation_rate": (
            service_counts["real_time_control_violating"] / service_counts["real_time_control_valid"]
            if service_counts["real_time_control_valid"] else ""
        ),
        "high_throughput_data_window_sla_violation_rate": (
            service_counts["high_throughput_data_violating"] / service_counts["high_throughput_data_valid"]
            if service_counts["high_throughput_data_valid"] else ""
        ),
        "sensor_telemetry_window_sla_violation_rate": (
            service_counts["sensor_telemetry_violating"] / service_counts["sensor_telemetry_valid"]
            if service_counts["sensor_telemetry_valid"] else ""
        ),
        "weighted_window_sla_violation_rate": weighted_rate(service_counts),
        "rtc_avg_latency_ms_mean": average_or_blank(summary_values["rtc_avg_latency_ms"]),
        "rtc_max_latency_ms_mean": average_or_blank(summary_values["rtc_max_latency_ms"]),
        "rtc_p95_latency_ms_mean": average_or_blank(summary_values["rtc_p95_latency_ms"]),
        "p95_available": bool(summary_values["rtc_p95_latency_ms"]),
        "rtc_loss_percent_mean": average_or_blank(summary_values["rtc_loss_percent"]),
        "data_throughput_mbps_mean": average_or_blank(summary_values["data_throughput_mbps"]),
        "sensor_delivery_ratio_percent_mean": average_or_blank(summary_values["sensor_delivery_ratio_percent"]),
    }
    return summary, by_mode, by_metric, severity_rows, by_service


def average_or_blank(values_: List[float]) -> Any:
    return mean(values_) if values_ else ""


def weighted_rate(service_counts: Counter) -> Any:
    numer = sum(service_counts[f"{service}_violating"] for service in SERVICE_CLASSES)
    denom = sum(service_counts[f"{service}_valid"] for service in SERVICE_CLASSES)
    return numer / denom if denom else ""


def count_jsonl(path: Path) -> int:
    return sum(1 for _ in iter_jsonl(path))


def policy_counts(mode_dir: Path) -> Tuple[int, int]:
    path = mode_dir / "policy_decisions.jsonl"
    decisions = 0
    applied = 0
    for row in iter_jsonl(path):
        decisions += 1
        enforcement = row.get("enforcement_result")
        if isinstance(enforcement, dict) and enforcement.get("applied") is True:
            applied += 1
    return decisions, applied


def enforcement_path(mode: str, mode_dir: Path) -> str:
    if mode == "fifo":
        return "FIFO_NO_QUEUE_RULES"
    if (mode_dir / "policy_decisions.jsonl").exists() and count_jsonl(mode_dir / "policy_decisions.jsonl") > 0:
        return "ONOS_QUEUE_APP"
    return "STATIC_OVS_QUEUE_RULES" if mode == "static_qos" else "ONOS_QUEUE_APP"


def mean_std_rows(summaries: List[Dict[str, Any]], directories: Dict[str, Path]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in summaries:
        grouped[str(row["mode"])].append(row)
    rows: List[Dict[str, Any]] = []
    for mode in MODES:
        items = grouped.get(mode, [])
        if not items:
            continue
        mode_dir = directories[mode]
        decisions, applied = policy_counts(mode_dir)
        risk_predictions = count_jsonl(mode_dir / "risk_predictions.jsonl")

        def vals(key: str) -> List[float]:
            return [value for value in (as_float(item.get(key)) for item in items) if value is not None]

        def avg(key: str) -> Any:
            numbers = vals(key)
            return mean(numbers) if numbers else ""

        def std(key: str) -> Any:
            numbers = vals(key)
            return pstdev(numbers) if len(numbers) > 1 else 0.0 if numbers else ""

        rows.append(
            {
                "mode": mode,
                "valid_windows_mean": avg("valid_windows"),
                "missing_windows_mean": avg("missing_windows"),
                "unweighted_window_sla_violation_rate_mean": avg("window_sla_violation_rate"),
                "unweighted_window_sla_violation_rate_std": std("window_sla_violation_rate"),
                "real_time_control_window_sla_violation_rate_mean": avg("real_time_control_window_sla_violation_rate"),
                "high_throughput_data_window_sla_violation_rate_mean": avg("high_throughput_data_window_sla_violation_rate"),
                "sensor_telemetry_window_sla_violation_rate_mean": avg("sensor_telemetry_window_sla_violation_rate"),
                "weighted_window_sla_violation_rate_mean": avg("weighted_window_sla_violation_rate"),
                "rtc_avg_latency_ms_mean": avg("rtc_avg_latency_ms_mean"),
                "rtc_max_latency_ms_mean": avg("rtc_max_latency_ms_mean"),
                "rtc_p95_latency_ms_mean": avg("rtc_p95_latency_ms_mean"),
                "p95_available": any(bool(item.get("p95_available")) for item in items),
                "data_throughput_mbps_mean": avg("data_throughput_mbps_mean"),
                "sensor_delivery_ratio_percent_mean": avg("sensor_delivery_ratio_percent_mean"),
                "risk_predictions_mean": risk_predictions,
                "policy_decisions_mean": decisions,
                "policy_applied_count_mean": applied,
                "enforcement_path": enforcement_path(mode, mode_dir),
            }
        )
    return rows


def service_mean_std_rows(by_service: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in by_service:
        grouped[(str(row["mode"]), str(row["service_class"]))].append(row)
    rows: List[Dict[str, Any]] = []
    for mode in MODES:
        for service in SERVICE_CLASSES:
            items = grouped.get((mode, service), [])
            if not items:
                continue
            valid = [row for row in items if row.get("valid_window") is True]
            violating = [row for row in valid if row.get("sla_violation") is True]
            severities = [as_float(row.get("violation_severity")) for row in valid]
            clean_severities = [value for value in severities if value is not None]
            rows.append(
                {
                    "mode": mode,
                    "service_class": service,
                    "total_windows": len(items),
                    "valid_windows": len(valid),
                    "missing_windows": len(items) - len(valid),
                    "violating_windows": len(violating),
                    "window_sla_violation_rate_mean": len(violating) / len(valid) if valid else "",
                    "window_sla_violation_rate_std": 0.0 if valid else "",
                    "avg_violation_severity_mean": mean(clean_severities) if clean_severities else "",
                }
            )
    return rows


def main() -> int:
    args = parse_args()
    result_dir = resolve_path(args.batch_root or args.result_dir)
    output_dir = resolve_path(args.output_dir) if args.output_dir else result_dir
    summaries: List[Dict[str, Any]] = []
    by_mode: List[Dict[str, Any]] = []
    by_metric: List[Dict[str, Any]] = []
    severity: List[Dict[str, Any]] = []
    by_service: List[Dict[str, Any]] = []
    directories: Dict[str, Path] = {}

    for mode, directory in mode_dirs(result_dir):
        summary, mode_rows, metric_rows, severity_rows, service_rows = summarize_mode(
            mode, directory, args.window_seconds, args.duration_seconds
        )
        summaries.append(summary)
        by_mode.extend(mode_rows)
        by_metric.extend(metric_rows)
        severity.extend(severity_rows)
        by_service.extend(service_rows)
        directories[mode] = directory

    summary_fields = [
        "mode", "run_id", "total_windows", "valid_windows", "missing_windows",
        "missing_control_windows", "missing_data_windows", "missing_sensor_windows",
        "violating_windows", "window_sla_violation_rate", "avg_violation_duration_seconds",
        "total_violation_duration_seconds", "avg_violation_severity", "max_violation_severity",
        "rtc_avg_latency_violation_windows", "rtc_p95_latency_violation_windows",
        "rtc_loss_violation_windows", "data_throughput_violation_windows",
        "sensor_delivery_violation_windows", "real_time_control_window_sla_violation_rate",
        "high_throughput_data_window_sla_violation_rate", "sensor_telemetry_window_sla_violation_rate",
        "weighted_window_sla_violation_rate", "rtc_avg_latency_ms_mean", "rtc_max_latency_ms_mean",
        "rtc_p95_latency_ms_mean", "p95_available", "rtc_loss_percent_mean",
        "data_throughput_mbps_mean", "sensor_delivery_ratio_percent_mean",
    ]
    by_mode_fields = [
        "mode", "run_id", "window_index", "window_start_seconds", "window_end_seconds",
        "valid_window", "real_time_control_valid", "high_throughput_data_valid", "sensor_telemetry_valid",
        "rtc_avg_latency_ms", "rtc_max_latency_ms", "rtc_p95_latency_ms", "p95_available",
        "rtc_loss_percent", "data_throughput_mbps", "sensor_delivery_ratio_percent",
        "sla_violation", "violation_severity",
    ]
    by_metric_fields = [
        "mode", "run_id", "window_index", "service_class", "metric", "valid_metric",
        "observed_value", "threshold", "threshold_direction", "violation", "severity",
    ]
    severity_fields = ["mode", "run_id", "window_index", "valid_window", "violation_severity", "dominant_metric"]
    service_fields = ["mode", "run_id", "window_index", "service_class", "valid_window", "sla_violation", "violation_severity"]
    mean_fields = [
        "mode", "valid_windows_mean", "missing_windows_mean",
        "unweighted_window_sla_violation_rate_mean", "unweighted_window_sla_violation_rate_std",
        "real_time_control_window_sla_violation_rate_mean",
        "high_throughput_data_window_sla_violation_rate_mean",
        "sensor_telemetry_window_sla_violation_rate_mean",
        "weighted_window_sla_violation_rate_mean", "rtc_avg_latency_ms_mean",
        "rtc_max_latency_ms_mean", "rtc_p95_latency_ms_mean", "p95_available",
        "data_throughput_mbps_mean", "sensor_delivery_ratio_percent_mean",
        "risk_predictions_mean", "policy_decisions_mean", "policy_applied_count_mean",
        "enforcement_path",
    ]
    service_mean_fields = [
        "mode", "service_class", "total_windows", "valid_windows", "missing_windows",
        "violating_windows", "window_sla_violation_rate_mean",
        "window_sla_violation_rate_std", "avg_violation_severity_mean",
    ]
    weighted_fields = [
        "mode", "run_id", "total_windows", "valid_windows", "missing_windows",
        "window_sla_violation_rate", "weighted_window_sla_violation_rate",
        "real_time_control_window_sla_violation_rate",
        "high_throughput_data_window_sla_violation_rate",
        "sensor_telemetry_window_sla_violation_rate",
    ]

    write_csv(output_dir / "window_sla_summary.csv", summaries, summary_fields)
    write_csv(output_dir / "window_sla_by_mode.csv", by_mode, by_mode_fields)
    write_csv(output_dir / "window_sla_by_metric.csv", by_metric, by_metric_fields)
    write_csv(output_dir / "window_sla_severity.csv", severity, severity_fields)
    write_csv(output_dir / "window_sla_by_service_class.csv", by_service, service_fields)
    write_csv(output_dir / "window_sla_by_service_class_mean_std.csv", service_mean_std_rows(by_service), service_mean_fields)
    write_csv(output_dir / "window_sla_mean_std_summary.csv", mean_std_rows(summaries, directories), mean_fields)
    write_csv(output_dir / "window_sla_weighted_summary.csv", summaries, weighted_fields)
    print(f"[window-sla] wrote {len(summaries)} mode summary row(s) to {output_dir / 'window_sla_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
