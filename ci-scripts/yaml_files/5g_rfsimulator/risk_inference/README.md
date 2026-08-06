# Deterministic Predictive SLA Risk Inference Engine for the Shadow N6 Digital Twin

This module computes deterministic SLA risk from the Shadow N6 Digital Twin. It does not train a model and does not control ONOS or OVS directly.

The output is advisory until policy manager integration is added. The risk engine does not install, remove, or modify ONOS/OVS queue rules.

The scorer combines four components for each service:

- SLA margin risk
- Short-term trend risk
- Queue pressure risk
- Enforcement mismatch risk

Services and expected N6 queues:

- `high_throughput_data`: UDP `5201`, queue `1`
- `real_time_control`: UDP `5202`, queue `2`
- `sensor_telemetry`: UDP `5203`, queue `3`

Risk scores are in the `0.0` to `1.0` range:

- `0.00` to `0.33`: `low`
- `0.34` to `0.66`: `medium`
- `0.67` to `1.00`: `high`

Queue-rule data quality matters:

- `unknown` means the twin did not include inspected queue-rule telemetry.
- `missing` means queue-rule telemetry was inspected and the expected rule was not found.
- `partial` means only some expected queue rules were found.
- `all_present` means the expected rules were observed.

Unknown queue state is neutral for enforcement mismatch risk. It is reported through `missing_fields` and `data_quality_status`, but it is not treated as a confirmed missing queue rule.

Stale twin states are not valid for policy decisions. If the latest twin state is older than `max_twin_age_seconds`, prediction output uses:

```json
{
  "inference_status": "stale_twin_state",
  "valid_for_policy": false,
  "recommended_policy_action": "PRESERVE_EXISTING_POLICY_BEHAVIOR"
}
```

Run a single prediction from the repository's `5g_rfsimulator` directory:

```bash
python3 -m risk_inference.predict_risk
```

Run periodically:

```bash
bash risk_inference/run_risk_inference.sh --duration 60 --interval 2
```

The predictor reads the latest twin state from:

1. a path passed with `--twin-state-path`
2. `logs/digital_twin/latest_twin_state.json`
3. the latest line in `logs/digital_twin/twin_state.jsonl`

Outputs:

- `logs/risk_inference/latest_risk_prediction.json`
- `logs/risk_inference/risk_predictions.jsonl`

Evaluate accumulated predictions:

```bash
python3 risk_inference/evaluate_risk_scoring.py
```

Evaluation writes:

- `risk_inference_summary.csv`
- `service_risk_summary.csv`
- `high_risk_events.csv`
