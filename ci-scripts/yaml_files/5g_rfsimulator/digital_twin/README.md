# Digital Twin

This module is the first 6G prototype extension for the OAI + ONOS + OVS N6 testbed. It mirrors the latest telemetry snapshot into a compact Digital Twin state model without changing OAI core code, Docker Compose, ONOS, OVS, telemetry, or policy-manager logic.

The first version is intentionally file-based and dependency-free. It reads JSONL telemetry that the existing collectors already write, converts service and queue metrics into typed Python dataclasses, and appends the resulting network state to a Digital Twin JSONL log.

## How It Fits

- OAI, ONOS, and OVS keep running exactly as they do today.
- Existing telemetry writes live testbed observations under `../logs/telemetry/`.
- `twin_sync.py` reads the newest telemetry JSONL record and builds a `NetworkTwinState`.
- `twin_store.py` appends each twin state to `../logs/digital_twin/twin_state.jsonl`.
- `twin_api.py` exposes the latest stored twin state over a small standard-library HTTP API.

## Files Read

- `../logs/telemetry/telemetry_*.jsonl` by default
- `config.yaml` for the telemetry directory, glob, output path, and sync interval

The parser accepts missing metrics and stores them as JSON `null`. It also understands the newer nested `slice_metrics` shape when that appears in telemetry records.

## Files Written

- `../logs/digital_twin/twin_state.jsonl`

The output directory is created automatically if it does not already exist.

## Run Once

From `ci-scripts/yaml_files/5g_rfsimulator/digital_twin/`:

```bash
python3 twin_sync.py --once
```

## Run Continuously

From `ci-scripts/yaml_files/5g_rfsimulator/digital_twin/`:

```bash
python3 twin_sync.py --interval 2
```

Or use the automation launcher:

```bash
cd ../automation
./run_digital_twin.sh
```

## Run The API

```bash
python3 twin_api.py --host 127.0.0.1 --port 8096
```

Endpoints:

- `GET /health`
- `GET /state/latest`

## Example Output

Each line in `../logs/digital_twin/twin_state.jsonl` is one JSON object:

```json
{"last_updated":"2026-06-08T18:50:12.255Z","onos_status":{"available_device_count":1,"device_count":1,"ok":true},"ovs_status":{"bridge_name":"br-n6","controller_connected":true,"controller_target":"tcp:192.168.71.160:6653","flow_rule_present_slices":[],"ok":true,"queue_configured_slices":["embb","urllc","mmtc"]},"queues":[{"bytes_total":0.0,"packet_rate_pps":0.0,"packets_total":0.0,"queue_id":"1","slice_name":"embb","throughput_bps":0.0,"timestamp":"2026-06-08T18:50:12.255Z"}],"services":[{"jitter_ms":null,"latency_avg_ms":null,"latency_max_ms":null,"packet_loss_percent":null,"service_name":"eMBB","sla_violation_risk":null,"slice_name":"embb","throughput_bps":0.0,"timestamp":"2026-06-08T18:50:12.255Z"}]}
```

