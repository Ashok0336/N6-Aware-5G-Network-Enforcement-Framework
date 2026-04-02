#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"
RUN_TAG="$(date +%Y%m%d_%H%M%S)"
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

EXT_DN_CONT="${EXT_DN_CONT:-${EXT_DN_CONTAINER_NAME}}"
UE1_CONT="${UE1_CONT:-rfsim5g-oai-nr-ue}"
UE2_CONT="${UE2_CONT:-rfsim5g-oai-nr-ue2}"
UE3_CONT="${UE3_CONT:-rfsim5g-oai-nr-ue3}"
DURATION="${DURATION:-20}"
EXT_DN_IP="${EXT_DN_IP:-${EXT_DN_TARGET_IP}}"

ensure_iperf3() {
  local c="$1"
  if ! docker exec "$c" bash -lc "command -v iperf3 >/dev/null 2>&1"; then
    docker exec "$c" bash -lc "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y iperf3 >/dev/null"
  fi
  docker exec "$c" bash -lc "command -v iperf3 >/dev/null 2>&1"
}

echo "[traffic] Ensuring iperf3 availability..."
ensure_iperf3 "$EXT_DN_CONT"
ensure_iperf3 "$UE1_CONT"
ensure_iperf3 "$UE2_CONT"
ensure_iperf3 "$UE3_CONT"

echo "[traffic] Starting Ext-DN iperf3 UDP servers on 5201/5202/5203..."
docker exec "$EXT_DN_CONT" bash -lc "pkill -f 'iperf3 -s -p 5201' 2>/dev/null || true; pkill -f 'iperf3 -s -p 5202' 2>/dev/null || true; pkill -f 'iperf3 -s -p 5203' 2>/dev/null || true"
docker exec "$EXT_DN_CONT" bash -lc "nohup iperf3 -s -p 5201 >/tmp/iperf3-s-5201.log 2>&1 &"
docker exec "$EXT_DN_CONT" bash -lc "nohup iperf3 -s -p 5202 >/tmp/iperf3-s-5202.log 2>&1 &"
docker exec "$EXT_DN_CONT" bash -lc "nohup iperf3 -s -p 5203 >/tmp/iperf3-s-5203.log 2>&1 &"
sleep 2

echo "[traffic] URLLC ping test from UE2..."
docker exec "$UE2_CONT" bash -lc "ping -c 5 ${EXT_DN_IP}" | tee "${LOG_DIR}/ping-urllc-${RUN_TAG}.log"

echo "[traffic] Running UDP iperf3 clients (20s each)..."
docker exec "$UE1_CONT" bash -lc "iperf3 -u -c ${EXT_DN_IP} -p 5201 -t ${DURATION} -b 80M -J" > "${LOG_DIR}/iperf3-embb-${RUN_TAG}.json"
docker exec "$UE2_CONT" bash -lc "iperf3 -u -c ${EXT_DN_IP} -p 5202 -t ${DURATION} -b 20M -J" > "${LOG_DIR}/iperf3-urllc-${RUN_TAG}.json"
docker exec "$UE3_CONT" bash -lc "iperf3 -u -c ${EXT_DN_IP} -p 5203 -t ${DURATION} -b 5M -J" > "${LOG_DIR}/iperf3-mmtc-${RUN_TAG}.json"

echo "[traffic] Completed. Logs:"
echo "  ${LOG_DIR}/iperf3-embb-${RUN_TAG}.json"
echo "  ${LOG_DIR}/iperf3-urllc-${RUN_TAG}.json"
echo "  ${LOG_DIR}/iperf3-mmtc-${RUN_TAG}.json"
echo "  ${LOG_DIR}/ping-urllc-${RUN_TAG}.log"
