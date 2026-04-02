# Closed-Loop N6 Slicing Design

This repository now exposes the full closed-loop control stack above the already working N6 data plane:

```text
UEs -> gNB -> Core -> UPF -> N6 -> OVS -> ext-DN
                                     ^
                                   ONOS
                                     ^
                          Slice Policy Manager
                                     ^
                                 Telemetry
```

## Design Goals

- Keep the current working N6 forwarding path intact.
- Preserve OVS + ONOS based external N6 enforcement.
- Add only the missing telemetry and policy layers.
- Default to `dry_run_only=true` so policy decisions can be validated safely before live enforcement.

## Runtime Components

### Telemetry

The new telemetry package runs on the host and samples the running testbed every few seconds. It collects:

- OVS flow counters from `ovs-ofctl dump-flows br-n6`
- OVS port statistics from `ovs-ofctl dump-ports br-n6`
- OVS queue statistics from `ovs-ofctl queue-stats br-n6`
- ONOS device and flow state from the REST API
- N6 ping latency and jitter using `ext-DN -> UPF` probes over `dn0`
- iperf-style throughput evidence and sender counters from existing experiment log files
- Docker CPU/memory/network/block I/O statistics

Snapshots are written to:

- `logs/telemetry/closed_loop_telemetry_<timestamp>.jsonl`
- `logs/telemetry/closed_loop_latest.json`

### Slice Policy Manager

The new policy manager is a rule-based controller. It reads telemetry snapshots, evaluates SLA thresholds, logs one decision record per policy cycle, and only applies changes when active mode is explicitly enabled.

Decisions are written to:

- `logs/policy/closed_loop_policy_<timestamp>.jsonl`
- `logs/policy/closed_loop_policy_<timestamp>.csv`

## Initial Slice Model

- `eMBB` -> UDP port `5201` -> queue `1`
- `URLLC` -> UDP port `5202` -> queue `2`
- `mMTC` -> UDP port `5203` -> queue `3`

## Initial Rule Set

- If URLLC latency, jitter, or loss exceeds threshold, increase URLLC protection by raising queue 2 guarantees.
- If eMBB throughput falls below threshold and URLLC is healthy, increase eMBB bandwidth share by raising queue 1 guarantees.
- If mMTC delivery ratio falls below threshold, increase the reserved rate for queue 3.

## Active Enforcement Scope

Active mode does not redesign the switch behavior. It keeps the working forwarding model and only:

- re-ensures ONOS slice flows exist
- updates the OVS HTB queue profile attached to `v-edn-host`

The bootstrap ARP and IPv4 N6 forwarding rules remain intact.
