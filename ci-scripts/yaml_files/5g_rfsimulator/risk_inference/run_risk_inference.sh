#!/usr/bin/env bash
set -euo pipefail

DURATION=""
INTERVAL="2"
TWIN_STATE_PATH=""
OUTPUT_DIR="logs/risk_inference"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --interval)
      INTERVAL="$2"
      shift 2
      ;;
    --twin-state-path)
      TWIN_STATE_PATH="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "[risk-inference][ERROR] unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

START_SECONDS="$(date +%s)"

while true; do
  CMD=(python3 -m risk_inference.predict_risk --output-dir "$OUTPUT_DIR")
  if [[ -n "$TWIN_STATE_PATH" ]]; then
    CMD+=(--twin-state-path "$TWIN_STATE_PATH")
  fi
  "${CMD[@]}"

  if [[ -n "$DURATION" ]]; then
    NOW_SECONDS="$(date +%s)"
    if (( NOW_SECONDS - START_SECONDS >= DURATION )); then
      break
    fi
  fi
  sleep "$INTERVAL"
done

