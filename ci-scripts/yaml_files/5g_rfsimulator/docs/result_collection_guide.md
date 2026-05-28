# Result Collection Guide

This guide describes the one-click result collector for the OAI + ONOS + OVS N6 slicing testbed.

## Methods

- `fifo`: no explicit QoS queues or slice queue rules. The script removes OVS QoS and Queue objects, removes UDP `set_queue` rules for ports `5201`, `5202`, and `5203`, and keeps basic N6 forwarding on `br-n6`.
- `static_ovs`: fixed OVS queue treatment with eMBB queue 1 at 50-100 Mbps, URLLC queue 2 at 10-20 Mbps, and mMTC queue 3 at 1-5 Mbps.
- `static_slicing`: fixed partition slicing with eMBB queue 1 at 70 Mbps, URLLC queue 2 at 20 Mbps, and mMTC queue 3 at 10 Mbps.
- `proposed_closed_loop`: starts `automation/run_closed_loop.sh --active` for each run so the telemetry and policy loops can adapt enforcement decisions.

## How to Run

From `ci-scripts/yaml_files/5g_rfsimulator`:

```bash
chmod +x automation/run_all_results_experiments.sh automation/check_result_readiness.sh
./automation/check_result_readiness.sh
./automation/run_all_results_experiments.sh
```

The default matrix is:

- Methods: `fifo`, `static_ovs`, `static_slicing`, `proposed_closed_loop`
- Loads: `load_30`, `load_60`, `load_80`, `load_95`
- Repetitions: `RUNS=1`
- Duration: `DURATION_SECONDS=60`

To run ten repetitions for paper collection:

```bash
RUNS=10 ./automation/run_all_results_experiments.sh
```

For a shorter smoke test:

```bash
DURATION_SECONDS=15 RUNS=1 ./automation/run_all_results_experiments.sh
```

## Result Layout

Results are written under `results_real/`:

```text
results_real/
  fifo/load_30/run_1/
  fifo/load_60/run_1/
  fifo/load_80/run_1/
  fifo/load_95/run_1/
  static_ovs/...
  static_slicing/...
  proposed_closed_loop/...
```

Each run folder contains:

- `urllc_ue1_ping.log` and `urllc_ue2_ping.log`
- `run_all_traffic.log` from the framework traffic generator
- `traffic_logs/`, copied from `logs/traffic/`
- `prometheus_metrics_snapshot.txt` from `http://localhost:8000/metrics`
- `policy_metrics_snapshot.txt` from `http://localhost:8001/metrics`
- `filtered_metrics.txt` with key paper metrics such as `urllc_latency_avg_ms`, `urllc_jitter_ms`, `embb_throughput_bps`, `mmtc_delivery_ratio_percent`, `ovs_queue_packets_total`, `ovs_queue_bytes_total`, and policy metrics when present
- `ovs_flows.log`, `ovs_queues.log`, `ovs_bridge_show.log`, and `docker_status.log`
- latest telemetry JSONL files under `telemetry_jsonl/`
- latest policy JSONL files under `policy_jsonl/`
- `run_summary.txt`

Direct `iperf3` over the OAI UE tunnel may time out because `iperf3` requires a TCP control connection before UDP data transfer. For that reason, direct per-UE `iperf3` JSON files are not used as the primary success condition. The primary experimental data is collected from `automation/run_all_traffic.sh`, framework logs under `logs/traffic/`, telemetry JSONL files, policy JSONL files, Prometheus snapshots, and OVS flow and queue counters.

## Industrial UE Mapping

- UE1: robotic arm control, URLLC latency measured by ping and low-rate UDP on port `5202`
- UE2: AGV or mobile robot control, URLLC latency measured by ping and low-rate UDP on port `5202`
- UE3: machine vision camera 1, eMBB UDP on port `5201`
- UE4: machine vision camera 2, eMBB UDP on port `5201`
- UE5: AR/VR operator support, eMBB UDP on port `5201`
- UE6: 3D printer monitoring, eMBB UDP on port `5201`
- UE7: temperature sensor, mMTC UDP on port `5203`
- UE8: vibration sensor, mMTC UDP on port `5203`
- UE9: energy or machine-health telemetry, mMTC UDP on port `5203`
- UE10: inventory or environmental sensor, mMTC UDP on port `5203`

## Preliminary Paper Graphs

Use one row per run folder. The most useful first-pass plots are:

- URLLC latency and jitter by method and load from `urllc_ue1_ping.log`, `urllc_ue2_ping.log`, and telemetry metrics.
- eMBB throughput by method and load from `traffic_logs/`, telemetry JSONL files, OVS counters, and `embb_throughput_bps`.
- mMTC delivery ratio by method and load from `traffic_logs/`, telemetry JSONL files, and `mmtc_delivery_ratio_percent`.
- Queue packet and byte counters by method from `ovs_queues.log` and `filtered_metrics.txt`.
- Policy activity for the closed-loop method from `policy_metrics_snapshot.txt` and `policy_jsonl/`.

For publication-quality plots, aggregate all `RUNS=10` repetitions by method and load, then show mean with confidence intervals or error bars. Keep `fifo` as the uncontrolled baseline, `static_ovs` and `static_slicing` as fixed-resource baselines, and `proposed_closed_loop` as the adaptive method.
