#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from telemetry.scheduler import TelemetryScheduler


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Closed-loop telemetry collector for the OAI + ONOS + OVS N6 slicing testbed."
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("config.yaml")),
        help="Path to the telemetry config file.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect exactly one telemetry snapshot and exit.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Stop after N iterations. Zero means run forever.",
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()
    scheduler = TelemetryScheduler(Path(args.config))
    iteration = 0
    print(f"[telemetry] config={scheduler.config_path}")
    print(f"[telemetry] output={scheduler.output_path}")
    try:
        while True:
            snapshot = scheduler.collect_once()
            print(
                "[telemetry] snapshot="
                f"{snapshot['snapshot_index']} timestamp={snapshot['timestamp']} "
                f"urllc_latency={snapshot['slice_metrics'].get('urllc', {}).get('latency_avg_ms')} "
                f"embb_throughput={snapshot['slice_metrics'].get('embb', {}).get('throughput_bps')}"
            )
            iteration += 1
            if args.once or (args.iterations and iteration >= args.iterations):
                return 0
            time.sleep(scheduler.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
