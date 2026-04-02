#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

APP_DIR="$PWD/onos-slice-queue-app"
APP_JAR="$APP_DIR/target/onos-slice-queue-app-1.0.0.jar"
ONOS_CONT="${ONOS_CONT:-${ONOS_CONTAINER_NAME}}"
ONOS_APP_CLI="${ONOS_APP_CLI:-/root/onos/bin/onos-app}"

echo "[onos-app] Building ONOS slice queue app with Maven Docker image..."
docker run --rm -v "$PWD/onos-slice-queue-app":/app -w /app maven:3.9-eclipse-temurin-11 mvn -q clean package

if [[ ! -f "$APP_JAR" ]]; then
  echo "[onos-app] ERROR: expected JAR not found: $APP_JAR" >&2
  exit 1
fi

echo "[onos-app] Copying app to ONOS container (${ONOS_CONT})..."
docker cp "$APP_JAR" "${ONOS_CONT}:/tmp/onos-slice-queue-app-1.0.0.jar"

echo "[onos-app] Installing app via Karaf onos-app..."
docker exec "$ONOS_CONT" /bin/bash -lc "
set -euo pipefail
if ! command -v onos-app >/dev/null 2>&1 && [ -x '${ONOS_APP_CLI}' ]; then
  export PATH=\$(dirname '${ONOS_APP_CLI}'):\${PATH}
fi
command -v onos-app >/dev/null 2>&1 || { echo '[onos-app] ERROR: onos-app CLI not found in container.' >&2; exit 1; }
onos-app localhost install! /tmp/onos-slice-queue-app-1.0.0.jar
"

echo "[onos-app] Installed. Verify flows with:"
echo "docker exec ${OVS_CONTAINER_NAME} ovs-ofctl -O OpenFlow13 dump-flows ${OVS_BRIDGE_NAME} | egrep \"set_queue|tp_dst=5201|tp_dst=5202|tp_dst=5203\""
