# ML Predictor

This module is the first ML layer on top of the Digital Twin. It reads `logs/digital_twin/twin_state.jsonl`, converts twin state into a compact CSV dataset, stores a dependency-free threshold baseline model, and writes risk predictions as JSONL.

It does not modify telemetry, Digital Twin sync, policy manager, ONOS, OVS, OAI core code, or Docker Compose.

## Files Read

- `../logs/digital_twin/twin_state.jsonl`
- `models/baseline_model.json` when predicting

## Files Written

- `../logs/ml_predictor/dataset.csv`
- `models/baseline_model.json`
- `../logs/ml_predictor/predictions.jsonl`

## Dataset Features

- `urllc_latency_avg_ms`
- `urllc_latency_max_ms`
- `embb_throughput_bps`
- `ovs_queue_1_throughput_bps`
- `ovs_queue_2_throughput_bps`
- `ovs_queue_3_throughput_bps`
- `ovs_controller_connected`
- `onos_ok`

## Labels

- `urllc_sla_violation`: `1` when `urllc_latency_avg_ms > 20`, otherwise `0`
- `embb_congestion_risk`: `1` when `embb_throughput_bps > 85000000`, otherwise `0`

Missing numeric values are left blank in the CSV and treated as non-risk during threshold prediction.

## Build Dataset

From `ci-scripts/yaml_files/5g_rfsimulator/`:

```bash
python3 ml_predictor/dataset_builder.py
```

## Train Baseline Model

```bash
python3 ml_predictor/train_model.py
```

## Predict Once

```bash
python3 ml_predictor/predict_state.py --once
```

## Predict Continuously

```bash
python3 ml_predictor/predict_state.py --interval 2
```

Or use:

```bash
cd automation
./run_ml_predictor.sh
```

