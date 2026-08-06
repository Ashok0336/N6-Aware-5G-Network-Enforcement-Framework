#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
# shellcheck disable=SC1091
source "${HERE}/testbed-env.sh"

APP_DIR="${HERE}/onos-slice-queue-app"
ONOS_CONT="${ONOS_CONT:-${ONOS_CONTAINER_NAME:-onos}}"
OVS_CONTAINER_NAME="${OVS_CONTAINER_NAME:-ovs}"
OVS_BRIDGE_NAME="${OVS_BRIDGE_NAME:-br-n6}"
KARAF_CLIENT="${KARAF_CLIENT:-/root/onos/apache-karaf-4.2.14/bin/client}"
KARAF_USER="${KARAF_USER:-karaf}"
KARAF_PASSWORD="${KARAF_PASSWORD:-karaf}"
APP_MATCH_REGEX="${APP_MATCH_REGEX:-org\\.oai\\.slicequeue|onos-slice-queue|ONOS Slice Queue App}"
APP_ID="${APP_ID:-org.oai.slicequeue}"
ARTIFACT_NAME=""
REMOTE_ARTIFACT="/tmp/onos-slice-queue-app-1.0.0.jar"
BUNDLE_ID=""
INSTALL_OUTPUT=""

build_app() {
  echo "[onos-app] Building ONOS slice queue app..."
  if command -v mvn >/dev/null 2>&1; then
    mvn -f "${APP_DIR}/pom.xml" clean package
  else
    docker run --rm -v "${APP_DIR}:/app" -w /app maven:3.9-eclipse-temurin-11 mvn clean package
  fi
}

select_artifact() {
  local jar
  jar="$(find "${APP_DIR}/target" -maxdepth 1 -type f -name 'onos-slice-queue-app-*.jar' | sort | tail -n1 || true)"
  if [[ -n "$jar" ]]; then
    ARTIFACT_NAME="$jar"
  else
    echo "[onos-app] ERROR: no OSGi JAR artifact found under ${APP_DIR}/target" >&2
    exit 1
  fi
}

karaf() {
  local command="$*"
  docker exec "$ONOS_CONT" bash -lc "printf '%s\n' '${command}' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b"
}

karaf_quiet() {
  local command="$*"
  docker exec "$ONOS_CONT" bash -lc "printf '%s\n' '${command}' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b" >/dev/null 2>&1
}

matching_bundles() {
  docker exec "$ONOS_CONT" bash -lc "printf 'bundle:list\n' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b 2>/dev/null | grep -iE '${APP_MATCH_REGEX}' || true"
}

matching_bundle_ids() {
  matching_bundles | sed -n 's/^[^0-9]*\([0-9][0-9]*\).*/\1/p'
}

remove_stale_bundles() {
  local bundle_ids
  bundle_ids="$(matching_bundle_ids)"
  if [[ -z "$bundle_ids" ]]; then
    echo "[onos-app] No stale ${APP_ID} bundles found."
    return 0
  fi

  local bundle_id=""
  for bundle_id in $bundle_ids; do
    echo "[onos-app] Removing stale bundle id=${bundle_id}"
    karaf_quiet "bundle:stop ${bundle_id}" || true
    karaf_quiet "bundle:uninstall ${bundle_id}" || true
  done
}

install_artifact() {
  echo "[onos-app] Copying $(basename "$ARTIFACT_NAME") to ONOS container ${ONOS_CONT}:${REMOTE_ARTIFACT}..."
  docker cp "$ARTIFACT_NAME" "${ONOS_CONT}:${REMOTE_ARTIFACT}"

  echo "[onos-app] Installing and starting OSGi bundle through Karaf..."
  local output
  output="$(karaf "bundle:install -s file:${REMOTE_ARTIFACT}")"
  INSTALL_OUTPUT="$output"
  echo "$output"
  BUNDLE_ID="$(echo "$output" | sed -n 's/.*Bundle ID:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | tail -n1)"
  if [[ -z "$BUNDLE_ID" ]]; then
    BUNDLE_ID="$(matching_bundle_ids | tail -n1)"
  fi
  if [[ -z "$BUNDLE_ID" ]]; then
    echo "[onos-app] ERROR: could not determine installed bundle ID." >&2
    return 1
  fi
  echo "[onos-app] Installed bundle id=${BUNDLE_ID}"
}

verify_ds_activation_log() {
  if echo "$INSTALL_OUTPUT" | grep -q "Registered appId name=${APP_ID}" \
    && echo "$INSTALL_OUTPUT" | grep -q "Installing slice queue rules"; then
    echo "[onos-app] DS activation log observed in Karaf install output."
    return 0
  fi

  if docker logs "$ONOS_CONT" --tail=400 2>/dev/null \
    | grep -q "Registered appId name=${APP_ID}" \
    && docker logs "$ONOS_CONT" --tail=400 2>/dev/null \
    | grep -q "Installing slice queue rules"; then
    echo "[onos-app] DS activation log observed in ONOS container logs."
    return 0
  fi

  echo "[onos-app] ERROR: SliceQueueApp DS activation log was not observed." >&2
  return 1
}

bundle_is_active() {
  [[ -n "$BUNDLE_ID" ]] || return 1
  karaf "bundle:list" 2>/dev/null | grep -Eq "^[[:space:]]*${BUNDLE_ID}[[:space:]]*[|│].*Active"
}

wait_for_bundle_active() {
  for _ in {1..30}; do
    if bundle_is_active; then
      echo "[onos-app] Bundle id=${BUNDLE_ID} is Active."
      return 0
    fi
    sleep 1
  done
  return 1
}

show_status() {
  echo "[onos-app] ONOS app status:"
  docker exec "$ONOS_CONT" bash -lc "printf 'apps -s\n' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b 2>/dev/null | grep -iE \"slice|queue|oai\" || true"
  echo "[onos-app] Karaf bundle status:"
  docker exec "$ONOS_CONT" bash -lc "printf 'bundle:list\n' | '${KARAF_CLIENT}' -u '${KARAF_USER}' -p '${KARAF_PASSWORD}' -b 2>/dev/null | grep -iE \"slice|queue|oai\" || true"
}

wait_for_set_queue_flows() {
  for _ in {1..30}; do
    local flows
    flows="$(docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true)"
    if echo "$flows" | grep -q "set_queue:1" \
      && echo "$flows" | grep -q "set_queue:2" \
      && echo "$flows" | grep -q "set_queue:3"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

print_failure_diagnostics() {
  echo "[onos-app] Matching Karaf bundles:"
  matching_bundles || true
  echo "[onos-app] SCR components matching slice/queue/oai:"
  karaf "scr:list" 2>/dev/null | grep -iE "slice|queue|oai" || true
  echo "[onos-app] Component list matching slice/queue/oai:"
  karaf "component:list" 2>/dev/null | grep -iE "slice|queue|oai" || true
  echo "[onos-app] Recent ONOS logs for slice queue activation:"
  docker logs "$ONOS_CONT" --tail=200 2>/dev/null | grep -iE "slice|queue|oai|activate|exception|error" || true
  echo "[onos-app] OVS dump-flows ${OVS_BRIDGE_NAME}:"
  docker exec "${OVS_CONTAINER_NAME}" ovs-ofctl -O OpenFlow13 dump-flows "${OVS_BRIDGE_NAME}" 2>/dev/null || true
}

build_app
select_artifact
remove_stale_bundles
install_artifact
show_status

if ! wait_for_bundle_active; then
  echo "[onos-app] ERROR: bundle id=${BUNDLE_ID:-unknown} did not become Active." >&2
  print_failure_diagnostics
  exit 1
fi

if ! verify_ds_activation_log; then
  print_failure_diagnostics
  exit 1
fi

echo "[onos-app] Waiting for ONOS-installed queue rules to appear in OVS..."
if wait_for_set_queue_flows; then
  echo "[onos-app] SUCCESS: set_queue:1, set_queue:2, and set_queue:3 are present on ${OVS_BRIDGE_NAME}."
  exit 0
fi

echo "[onos-app] ERROR: queue rules were not observed after ONOS app installation." >&2
print_failure_diagnostics
exit 1
