#!/bin/bash
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

OVS_RECONCILE_INTERVAL_SECONDS="${OVS_RECONCILE_INTERVAL_SECONDS:-5}"
ONOS_CTRL="${ONOS_CTRL:-${ONOS_HOST:-192.168.71.160}:6653}"
LAST_ONOS_REACHABILITY="unknown"

check_onos_listener() {
  local onos_host="${ONOS_CTRL%:*}"
  local onos_port="${ONOS_CTRL##*:}"
  local current_state="unreachable"
  local message=""

  if nc -z -w 1 "${onos_host}" "${onos_port}" >/dev/null 2>&1; then
    current_state="reachable"
    message="[OVS] ONOS listener ${ONOS_CTRL} is reachable."
  else
    message="[OVS] WARN: ONOS listener ${ONOS_CTRL} is not reachable yet. OVS will continue and retry."
  fi

  if [[ "${current_state}" != "${LAST_ONOS_REACHABILITY}" ]]; then
    echo "${message}" | tee -a /var/log/openvswitch/ovs-init.log
    LAST_ONOS_REACHABILITY="${current_state}"
  fi
}

mkdir -p /var/run/openvswitch /var/log/openvswitch /etc/openvswitch

if [ ! -f /etc/openvswitch/conf.db ]; then
  echo "[OVS] Creating /etc/openvswitch/conf.db ..."
  ovsdb-tool create /etc/openvswitch/conf.db /usr/share/openvswitch/vswitch.ovsschema
fi

echo "[OVS] Starting ovsdb-server..."
/usr/sbin/ovsdb-server \
  --remote=punix:/var/run/openvswitch/db.sock \
  --remote=db:Open_vSwitch,Open_vSwitch,manager_options \
  --pidfile --detach \
  --log-file=/var/log/openvswitch/ovsdb-server.log \
  /etc/openvswitch/conf.db

echo "[OVS] Waiting for db.sock..."
for i in $(seq 1 50); do
  [ -S /var/run/openvswitch/db.sock ] && break
  sleep 0.2
done
[ -S /var/run/openvswitch/db.sock ] || { echo "[OVS] ERROR: db.sock not created"; ls -la /var/run/openvswitch; exit 1; }

echo "[OVS] Initializing OVS database..."
/usr/bin/ovs-vsctl --no-wait init

echo "[OVS] Starting ovs-vswitchd..."
/usr/sbin/ovs-vswitchd \
  --pidfile --detach \
  --log-file=/var/log/openvswitch/ovs-vswitchd.log \
  --unixctl=/var/run/openvswitch/ovs-vswitchd.ctl

sleep 2

echo "[OVS] Entering reconcile loop (interval=${OVS_RECONCILE_INTERVAL_SECONDS}s)..."
: > /var/log/openvswitch/ovs-init.log
while true; do
  check_onos_listener
  echo "[OVS] Reconcile iteration started at $(date -Iseconds)" | tee -a /var/log/openvswitch/ovs-init.log
  if bash /ovs-init.sh 2>&1 | tee -a /var/log/openvswitch/ovs-init.log; then
    if python3 /onos-bootstrap-forwarding.py 2>&1 | tee -a /var/log/openvswitch/ovs-init.log; then
      :
    else
      status=$?
      echo "[OVS] WARN: ONOS bootstrap forwarding sync failed with exit code ${status}." | tee -a /var/log/openvswitch/ovs-init.log
    fi
    echo "[OVS] Reconcile iteration completed successfully." | tee -a /var/log/openvswitch/ovs-init.log
  else
    status=$?
    echo "[OVS] WARN: reconcile iteration failed with exit code ${status}." | tee -a /var/log/openvswitch/ovs-init.log
  fi
  sleep "${OVS_RECONCILE_INTERVAL_SECONDS}"
done
