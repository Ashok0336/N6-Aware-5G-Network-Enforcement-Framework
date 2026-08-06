#!/usr/bin/env python3
"""Plot DT-risk CCNC result summaries with matplotlib."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_GLOB = ROOT / "logs/experiments/ccnc"
MODES = ["fifo", "static_qos", "n6_only", "dt_only", "dt_risk_assisted"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot DT-risk CCNC result summaries.")
    parser.add_argument("--input", type=Path, default=None, help="Result directory. Default: latest logs/experiments/ccnc/dt_risk_*")
    return parser.parse_args()


def resolve_input(path: Optional[Path]) -> Path:
    if path is not None:
        candidate = path.expanduser()
        return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    candidates = sorted(DEFAULT_RESULTS_GLOB.glob("dt_risk_*"), key=lambda item: item.stat().st_mtime if item.exists() else 0)
    if not candidates:
        raise FileNotFoundError(f"No dt_risk_* result directories found under {DEFAULT_RESULTS_GLOB}")
    return candidates[-1].resolve()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def ordered_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    by_mode = {row.get("mode", ""): row for row in rows}
    return [by_mode[mode] for mode in MODES if mode in by_mode]


def mode_label(mode: str) -> str:
    return {
        "fifo": "FIFO",
        "static_qos": "Static QoS",
        "n6_only": "N6 Only",
        "dt_only": "DT Only",
        "dt_risk_assisted": "DT Risk",
    }.get(mode, mode)


def setup_matplotlib():
    try:
        import matplotlib
    except ModuleNotFoundError:
        print("matplotlib is required for plotting. Install with: sudo apt install python3-matplotlib or pip install matplotlib")
        raise SystemExit(0)

    matplotlib.use("Agg")
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        print("matplotlib is required for plotting. Install with: sudo apt install python3-matplotlib or pip install matplotlib")
        raise SystemExit(0)

    plt.rcParams.update(
        {
            "font.size": 8,
            "axes.labelsize": 8,
            "axes.titlesize": 8,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "legend.fontsize": 7,
            "figure.figsize": (3.5, 2.35),
            "figure.dpi": 300,
            "savefig.dpi": 300,
        }
    )
    return plt


def save_figure(fig: Any, result_dir: Path, stem: str) -> None:
    fig.tight_layout(pad=0.4)
    fig.savefig(result_dir / f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(result_dir / f"{stem}.pdf", dpi=300, bbox_inches="tight")


def bar_plot(plt: Any, result_dir: Path, rows: List[Dict[str, str]], value_key: str, ylabel: str, stem: str) -> None:
    rows = ordered_rows(rows)
    if not rows:
        return
    labels = [mode_label(str(row.get("mode", ""))) for row in rows]
    values = [as_float(row.get(value_key)) for row in rows]
    fig, ax = plt.subplots()
    ax.bar(labels, values)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    save_figure(fig, result_dir, stem)
    plt.close(fig)


def risk_events_plot(plt: Any, result_dir: Path, rows: List[Dict[str, str]]) -> None:
    rows = ordered_rows(rows)
    if not rows:
        return
    labels = [mode_label(str(row.get("mode", ""))) for row in rows]
    event_keys = ["low_risk_events", "medium_risk_events", "high_risk_events"]
    x = list(range(len(rows)))
    bottom = [0.0 for _ in rows]
    fig, ax = plt.subplots()
    for key in event_keys:
        values = [as_float(row.get(key)) for row in rows]
        ax.bar(x, values, bottom=bottom, label=key.replace("_", " "))
        bottom = [bottom[i] + values[i] for i in range(len(values))]
    ax.set_ylabel("Risk events")
    ax.set_xticks(x, labels, rotation=25)
    ax.grid(axis="y", linewidth=0.4, alpha=0.5)
    ax.legend(frameon=False, ncol=1)
    save_figure(fig, result_dir, "risk_events_by_mode")
    plt.close(fig)


def read_risk_events(result_dir: Path) -> List[Tuple[str, int, float]]:
    events: List[Tuple[str, int, float]] = []
    mode_dir = result_dir / "dt_risk_assisted"
    path = mode_dir / "risk_inference" / "risk_predictions.jsonl"
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for idx, line in enumerate(handle, start=1):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            level = payload.get("overall_risk_level", 0)
            if str(level).lower() == "high":
                numeric_level = 2
            elif str(level).lower() == "medium":
                numeric_level = 1
            else:
                numeric_level = int(as_float(level))
            events.append((str(payload.get("timestamp", idx)), idx, float(numeric_level)))
    return events


def risk_timeline_plot(plt: Any, result_dir: Path) -> None:
    events = read_risk_events(result_dir)
    if not events:
        return
    x = [event[1] for event in events]
    y = [event[2] for event in events]
    fig, ax = plt.subplots()
    ax.step(x, y, where="post")
    ax.scatter(x, y, s=8)
    ax.set_xlabel("Prediction sample")
    ax.set_ylabel("Risk level")
    ax.set_yticks([0, 1, 2], ["Low", "Medium", "High"])
    ax.grid(linewidth=0.4, alpha=0.5)
    save_figure(fig, result_dir, "risk_events_timeline")
    plt.close(fig)


def main() -> int:
    result_dir = resolve_input(parse_args().input)
    if not (result_dir / "dt_risk_summary.csv").exists():
        import subprocess
        import sys

        subprocess.run([sys.executable, str(Path(__file__).with_name("analyze_dt_risk_results.py")), "--input", str(result_dir)], check=True)

    plt = setup_matplotlib()
    summary_rows = read_csv_rows(result_dir / "dt_risk_summary.csv")

    bar_plot(plt, result_dir, summary_rows, "sla_violation_rate", "SLA violation rate", "sla_violation_rate_by_mode")
    bar_plot(plt, result_dir, summary_rows, "control_latency_avg_ms", "Average control latency (ms)", "control_latency_avg_by_mode")
    bar_plot(plt, result_dir, summary_rows, "control_latency_max_ms", "Maximum control latency (ms)", "control_latency_max_by_mode")
    bar_plot(plt, result_dir, summary_rows, "sensor_delivery_ratio_percent", "Sensor delivery ratio (%)", "sensor_delivery_ratio_by_mode")
    risk_events_plot(plt, result_dir, summary_rows)
    bar_plot(plt, result_dir, summary_rows, "policy_applied_count", "Applied policy count", "policy_applied_count_by_mode")
    bar_plot(plt, result_dir, summary_rows, "overall_risk_score_avg", "Average overall risk score", "overall_risk_score_by_mode")
    risk_timeline_plot(plt, result_dir)

    print(f"[dt-risk-plot] wrote figures under {result_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
