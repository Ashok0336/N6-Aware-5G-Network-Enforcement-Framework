# AI Agent Decision Engine

This module is a dry-run decision engine for the 6G prototype loop. It reads the latest Digital Twin state and ML prediction, smooths OVS/ONOS health over the last three twin samples, then appends an advisory decision record.

It does not call ONOS, OVS, Docker, the policy manager, or any enforcement API. Decisions are written only to JSONL until live enforcement is explicitly enabled later.

## Inputs

- `../logs/digital_twin/twin_state.jsonl`
- `../logs/ml_predictor/predictions.jsonl`

## Output

- `../logs/ai_agent/agent_decisions.jsonl`

## Supported Actions

- `HOLD_ACTION`
- `MAINTAIN_CURRENT_POLICY`
- `INCREASE_CONTROL_PRIORITY`
- `TIGHTEN_DATA_SHAPING`
- `PROTECT_SENSOR_MIN_BW`

## Decision Rules

- If OVS controller connectivity or ONOS health is unstable for the majority of the last three Digital Twin samples, select `HOLD_ACTION`.
- If the ML predictor reports high URLLC SLA risk, select `INCREASE_CONTROL_PRIORITY`.
- If the ML predictor reports high eMBB congestion risk, select `TIGHTEN_DATA_SHAPING`.
- If the Digital Twin reports high sensor telemetry risk, select `PROTECT_SENSOR_MIN_BW`.
- Otherwise select `MAINTAIN_CURRENT_POLICY`.

Every output includes `dry_run: true`, `decision_only: true`, and `enforcement_performed: false`.

## Run Once

From `ci-scripts/yaml_files/5g_rfsimulator/`:

```bash
python3 ai_agent/decision_agent.py --once
```

## Run Continuously

```bash
python3 ai_agent/decision_agent.py --interval 2
```

Or:

```bash
cd automation
./run_ai_agent.sh
```
