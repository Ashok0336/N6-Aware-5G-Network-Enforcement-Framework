# Adaptive N6 Slicing Automation

This folder keeps the existing closed-loop N6 slicing architecture intact:

```text
UEs -> gNB -> 5G Core -> UPF -> N6 -> OVS
                           ^
                          ONOS
                           ^
                 Slice Policy Manager
                           ^
                       Telemetry
```

The update in this folder does not redesign that architecture and does not modify OAI core internals, gNB internals, or the N6 enforcement design. It only prepares the existing framework for reproducible 10-UE experiments.

## Why 10 UEs

The paper-ready experiment setup now uses 10 UE containers so each service class is represented by a small multi-UE population instead of a single prototype flow:

- `real_time_control`: `UE1`, `UE2`
- `high_throughput_data`: `UE3`, `UE4`, `UE5`, `UE6`
- `sensor_telemetry`: `UE7`, `UE8`, `UE9`, `UE10`

This keeps the original 3-class N6 enforcement model while making the traffic mix more realistic for final experiment runs.

## Shared Mapping

The shared experiment mapping lives in:

- `service_mapping.yaml`

It defines, per service class:

- service class name
- UE container bindings
- target IP and target port
- queue/profile association
- expected log file names
- sender profile used by the traffic scripts

Current UE-to-service mapping:

| Service class | UE containers | Target port | Queue |
| --- | --- | --- | --- |
| `real_time_control` | `rfsim5g-oai-nr-ue`, `rfsim5g-oai-nr-ue2` | `5202` | `2` |
| `high_throughput_data` | `rfsim5g-oai-nr-ue3`, `rfsim5g-oai-nr-ue4`, `rfsim5g-oai-nr-ue5`, `rfsim5g-oai-nr-ue6` | `5201` | `1` |
| `sensor_telemetry` | `rfsim5g-oai-nr-ue7`, `rfsim5g-oai-nr-ue8`, `rfsim5g-oai-nr-ue9`, `rfsim5g-oai-nr-ue10` | `5203` | `3` |

## Files

- `service_mapping.yaml`: shared 10-UE service map
- `service_mapping_utils.py`: loader used by telemetry, policy, and enforcement configs
- `config.yaml`: telemetry config with multi-UE service bindings
- `policy_config.yaml`: policy config with service-aware multi-UE thresholds and bindings
- `enforcement_config.yaml`: enforcement config annotated with the same service map
- `telemetry_collector.py`: read-only collector with multi-UE service aggregation
- `policy_manager.py`: threshold-based dry-run policy manager
- `enforcement_manager.py`: external N6 enforcement manager
- `traffic_common.sh`: shared UDP sink/sender helpers
- `run_control_traffic.sh`: UE1 and UE2 ping plus low-rate UDP control traffic
- `run_data_traffic.sh`: UE3 to UE6 bulk UDP traffic
- `run_sensor_traffic.sh`: UE7 to UE10 low-rate periodic UDP traffic
- `run_all_traffic.sh`: launches the full 10-UE mix together
- `experiment_common.sh`: shared experiment directory and runtime-config helper
- `run_baseline_experiment.sh`: telemetry plus traffic only
- `run_static_experiment.sh`: restore/hold the baseline queue profile, then run telemetry plus traffic
- `run_adaptive_experiment.sh`: telemetry plus policy manager plus enforcement manager plus traffic
- `run_telemetry.sh`, `run_policy_manager.sh`, `run_adaptive_control.sh`, `rollback_default_policy.sh`: existing direct helpers retained for single-component use

## Traffic Model

The traffic runners follow the requested service behavior:

- `real_time_control`: `UE1` and `UE2` run periodic `ping` for latency visibility and a low-rate UDP control probe on port `5202`
- `high_throughput_data`: `UE3` to `UE6` run concurrent bulk UDP senders on port `5201`
- `sensor_telemetry`: `UE7` to `UE10` run concurrent low-rate periodic UDP senders on port `5203`

The traffic scripts stay outside OAI and ONOS. They only use `docker exec` in the existing UE and Ext-DN containers.

## Log Layout

Each experiment creates a results directory under:

- `../logs/experiments/<mode>_<timestamp>/`

Expected subdirectories:

- `traffic/`
- `telemetry/`
- `policy/`
- `enforcement/`
- `runtime_configs/`
- `metadata/`

Traffic logs are separated by service class and UE. Typical files are:

- `traffic/real_time_control/control_ue1.log`
- `traffic/real_time_control/control_ue2.log`
- `traffic/real_time_control/control_ue1_udp.log`
- `traffic/real_time_control/control_ue2_udp.log`
- `traffic/high_throughput_data/data_ue3.log`
- `traffic/high_throughput_data/data_ue4.log`
- `traffic/high_throughput_data/data_ue5.log`
- `traffic/high_throughput_data/data_ue6.log`
- `traffic/sensor_telemetry/sensor_ue7.log`
- `traffic/sensor_telemetry/sensor_ue8.log`
- `traffic/sensor_telemetry/sensor_ue9.log`
- `traffic/sensor_telemetry/sensor_ue10.log`

Runtime-generated configs for each experiment are written to `runtime_configs/` so the exact run inputs are preserved alongside the results.

## Telemetry Aggregation

The telemetry collector now supports multi-UE service-aware aggregation.

What changed:

- it loads `service_mapping.yaml` through `service_mapping_utils.py`
- it discovers service logs from experiment-specific traffic directories
- it matches multiple logs per service instead of only one log per service
- it parses:
  - `ping` logs
  - existing iperf-style logs when present
  - UDP sender summary logs produced by the new traffic runners
- it emits one `service_metrics.<service_class>` block per service with:
  - aggregated service-level metrics
  - per-UE metrics when a UE-specific log is available
  - matched log counts and paths

Examples of service-level aggregation:

- `real_time_control`: service-average RTT, service-maximum RTT, aggregated control loss, per-UE ping evidence
- `high_throughput_data`: summed offered throughput across `UE3` to `UE6`, queue counters, flow totals, per-UE sender summaries
- `sensor_telemetry`: aggregated sender packet/byte counts, interface drop metrics, flow totals, per-UE sender summaries

The collector still keeps the older generic fields such as:

- `ovs_queue_statistics`
- `ovs_flow_counters`
- `interface_statistics`
- `container_statistics`
- `traffic_log_references`

This preserves single-UE compatibility where older log patterns are still used.

## Policy Behavior

The Slice Policy Manager logic is not redesigned.

The minimal policy-side updates are:

- load the shared mapping so service bindings stay consistent with telemetry and traffic scripts
- evaluate the aggregated `service_metrics` blocks produced by telemetry
- use thresholds sized for the 4-UE `high_throughput_data` aggregate instead of the old single-UE prototype

`real_time_control` and `sensor_telemetry` thresholds remain based on latency/loss/drop behavior, while `high_throughput_data` thresholds were raised to reflect the new aggregate offered load from `UE3` to `UE6`.

## Experiment Modes

### Baseline

What it does:

- creates an experiment results directory
- writes runtime config copies
- starts telemetry only
- starts the 10-UE traffic mix
- does not start the policy manager
- does not start the enforcement manager

Run it:

```bash
cd ci-scripts/yaml_files/5g_rfsimulator/automation
./run_baseline_experiment.sh --duration 60
```

### Static

What it does:

- creates an experiment results directory
- writes runtime config copies
- restores the baseline/default queue profile once through the existing enforcement manager
- starts telemetry
- starts the 10-UE traffic mix
- does not start the policy manager loop
- does not keep adaptive enforcement running

Run it:

```bash
cd ci-scripts/yaml_files/5g_rfsimulator/automation
./run_static_experiment.sh --live --duration 60
```

For a planned-only static dry run:

```bash
./run_static_experiment.sh --dry-run --duration 60
```

### Adaptive

What it does:

- creates an experiment results directory
- writes runtime config copies
- starts telemetry
- starts the policy manager
- starts the enforcement manager
- starts the same 10-UE traffic mix
- keeps the existing external closed loop intact

Run it:

```bash
cd ci-scripts/yaml_files/5g_rfsimulator/automation
./run_adaptive_experiment.sh --live --duration 60
```

For a no-network-change adaptive rehearsal:

```bash
./run_adaptive_experiment.sh --dry-run --duration 60
```

## Direct Traffic Commands

If you only want to generate traffic without the experiment wrappers:

```bash
./run_control_traffic.sh --duration 60
./run_data_traffic.sh --duration 60
./run_sensor_traffic.sh --duration 60
./run_all_traffic.sh --duration 60
```

## Notes

- The `static` mode assumption in this folder is: re-apply and hold the baseline queue profile without starting the adaptive policy loop.
- The signal handlers in telemetry, policy, and enforcement were adjusted so the new experiment wrappers can stop them cleanly without reentrant console-print errors.
- No ML or RL was added.
- No topology changes were added.
- No OAI core or gNB internals were modified.
