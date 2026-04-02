# Policy Manager Guide

## Files

- `policy_manager/config.yaml`: thresholds, queue overlays, ONOS/OVS endpoints, and dry-run default
- `policy_manager/app.py`: main rule-based loop
- `policy_manager/decision_engine.py`: threshold evaluation logic
- `policy_manager/telemetry_reader.py`: latest-snapshot reader
- `policy_manager/onos_client.py`: ONOS slice-flow helper
- `policy_manager/ovs_client.py`: OVS queue-profile helper

## Default Mode

The policy manager starts in dry-run mode by default.

That means:

- telemetry is read normally
- decisions are generated normally
- JSONL and CSV logs are written normally
- no queue profile changes are pushed to OVS
- no ONOS flow refresh is pushed unless active mode is explicitly enabled

## Run Once

Use this to validate the current telemetry snapshot and inspect the planned decision:

```bash
cd ci-scripts/yaml_files/5g_rfsimulator
./automation/run_policy_loop.sh --once --dry-run
```

## Continuous Dry-Run Mode

```bash
./automation/run_policy_loop.sh --dry-run
```

The first telemetry snapshot primarily initializes the delta-based counters used for throughput, queue-rate, and delivery-ratio estimates. In practice that means the second telemetry cycle is when `eMBB` throughput and `mMTC` delivery-ratio decisions become meaningful.

## Active Enforcement Mode

```bash
./automation/run_policy_loop.sh --active
```

In active mode the policy manager:

- re-ensures ONOS slice rules for UDP ports `5201`, `5202`, and `5203`
- updates the existing OVS queue profile on `v-edn-host`

## Validation

After telemetry and the policy loop have run at least once:

```bash
./scripts/check_closed_loop.sh
```

That script checks:

- the latest telemetry snapshot exists
- OVS, ONOS, ping, iperf, Docker, and slice metrics are present
- the latest policy JSONL exists
- the latest policy CSV exists
- the latest policy cycle contains `embb`, `urllc`, and `mmtc` decisions
