# Validation Guide

This folder provides deterministic post-deployment checks for the OAI + OVS + ONOS 5G slicing testbed.

## Goal

After every clean redeploy, you should be able to answer these questions quickly:

- Are the core containers up?
- Did ext-DN get `dn0`?
- Did UPF get `n6ovs0`?
- Does OVS expose `br-n6`, `v-upf-host`, and `v-edn-host`?
- Is OVS configured for the correct ONOS controller?
- Is ONOS reachable and does it see the OVS device?
- Is the N6 path ready for forwarding?

## Scripts

All scripts live in `scripts/` and return a non-zero exit code on failure.

- `check_containers.sh`: validates the main container states and health statuses.
- `check_extdn.sh`: validates `dn0` and ext-DN N6 routes.
- `check_upf.sh`: validates `n6ovs0` and the expected UPF N6 address.
- `check_ovs.sh`: validates `br-n6`, required bridge ports, and OpenFlow controller state.
- `check_onos.sh`: validates ONOS REST reachability and OVS device visibility.
- `check_n6_forwarding.sh`: validates the live N6 dataplane, including ARP/IPv4 bridge flows and bidirectional reachability.
- `check_n6_path.sh`: runs the ext-DN, UPF, OVS, and ONOS checks and then performs the N6 forwarding test.
- `check_end_to_end.sh`: runs the container check and the full N6 path check.
- `check_all.sh`: one-command alias for `check_end_to_end.sh`.

The composite wrappers keep running after a failing stage so you get a fuller picture in one pass, then return a non-zero exit code at the end if anything failed.

## Recommended sequence

Run this after every clean deployment:

```bash
cd /home/ak/Desktop/openairinterfacE5G_J/ci-scripts/yaml_files/5g_rfsimulator
docker compose -f docker-compose.yaml --env-file .env down -v --remove-orphans
docker compose -f docker-compose.yaml --env-file .env up -d --build
./scripts/check_all.sh
```

## Fast manual sequence

If you want the validation broken into steps:

```bash
./scripts/check_containers.sh
./scripts/check_extdn.sh
./scripts/check_upf.sh
./scripts/check_ovs.sh
./scripts/check_onos.sh
./scripts/check_n6_path.sh
```

## Interpreting failures

- If `check_containers.sh` fails, fix service startup or health first.
- If `check_extdn.sh` fails, inspect `docker logs rfsim5g-oai-ext-dn`.
- If `check_upf.sh` fails, inspect `docker logs rfsim5g-oai-upf`.
- If `check_ovs.sh` fails, inspect `docker exec ovs tail -n 120 /var/log/openvswitch/ovs-init.log`.
- If `check_onos.sh` fails, inspect `docker logs onos`.
- If `check_n6_path.sh` fails because forwarding flows are missing, install policy and re-run:

```bash
./install-slice-flows.sh
./scripts/check_n6_path.sh
```

## Useful extra commands

```bash
./scripts/show-ext-dn-interfaces.sh
./scripts/show-ovs-bridge-status.sh
./scripts/show-openflow-flows.sh
docker exec ovs ovs-vsctl list controller
docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6
curl -s -u onos:rocks http://192.168.71.160:8181/onos/v1/devices | python3 -m json.tool
```
