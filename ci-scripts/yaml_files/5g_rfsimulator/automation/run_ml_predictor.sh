#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTBED_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${TESTBED_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${TESTBED_DIR}/.venv/bin/activate"
elif [[ -f "${TESTBED_DIR}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${TESTBED_DIR}/venv/bin/activate"
fi

mkdir -p "${TESTBED_DIR}/logs/ml_predictor"
cd "${TESTBED_DIR}"

if [[ ! -f "${TESTBED_DIR}/ml_predictor/models/baseline_model.json" ]]; then
  python3 ml_predictor/dataset_builder.py
  python3 ml_predictor/train_model.py
fi

echo "[ml-predictor] testbed directory: ${TESTBED_DIR}"
exec python3 ml_predictor/predict_state.py --interval 2
