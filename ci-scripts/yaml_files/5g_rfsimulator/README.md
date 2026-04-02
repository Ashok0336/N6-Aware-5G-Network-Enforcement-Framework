# ONOS-Based 5G N6 Slice Enforcement Testbed using OAI and OVS

This project implements an OAI 5G testbed in which Open vSwitch (OVS) is inserted on the N6 path between the OAI UPF and the external data network (Ext-DN), and ONOS acts as the SDN controller that installs slice-aware forwarding behavior through OpenFlow.

The current prototype enforces slices by mapping UDP destination ports to OVS queues on the Ext-DN-facing egress port:

| Slice | Traffic classifier | OVS queue |
| --- | --- | --- |
| eMBB | UDP destination port `5201` | Queue `1` |
| URLLC | UDP destination port `5202` | Queue `2` |
| mMTC | UDP destination port `5203` | Queue `3` |

> [!IMPORTANT]
> The infrastructure in this folder is functional, but end-to-end slice enforcement still depends on active `set_queue` flow rules being installed in OVS. The main operational risk is ONOS application version alignment: the ONOS runtime currently observed in the container is `3.0.0`, while rebuilding the custom ONOS app requires an ONOS API version that is both compatible with the runtime and actually available from Maven repositories. Do not assume that changing the Maven version to `3.0.0` will build successfully.

## Authoritative deployment path

This folder now has one authoritative deployment path for the OAI + OVS + ONOS N6 slicing testbed:

- Compose file: `docker-compose.yaml`
- Environment file: `.env`
- Deployment command: `docker compose -f docker-compose.yaml --env-file .env up -d --build`
- Validation command: `./scripts/check_all.sh`

The following files are not part of the authoritative testbed startup path:

- `overrides/local-dev.override.yaml`
- `overrides/ue-gdb.override.yaml`
- `flexric/docker/docker-compose.yaml`
- `automation/archive/policy_config_backup.json`

## Overview

This framework combines:

- OAI 5G Core components (`AMF`, `SMF`, `UPF`)
- OAI RF-simulator gNB and UE containers
- OVS on the N6 path
- ONOS as the OpenFlow controller

OVS is placed between the UPF and the external DN so that traffic leaving the user plane toward the data network passes through bridge `br-n6`. ONOS controls that bridge through OpenFlow and selects an egress queue according to the slice classifier. The current implementation is a UDP-port prototype rather than a QFI-aware classifier.

The slice mapping used by this repo is:

| Slice | UDP port | Queue | Intended rate profile |
| --- | --- | --- | --- |
| eMBB | `5201` | `1` | `50-100 Mbps` |
| URLLC | `5202` | `2` | `10-20 Mbps` |
| mMTC | `5203` | `3` | `1-5 Mbps` |

## Architecture

End-to-end traffic path:

```text
UE -> gNB -> OAI Core -> UPF -> OVS (br-n6) -> External DN
```

In the current implementation:

- The UPF-side OVS attachment is interface `n6ovs0`.
- The OVS bridge is `br-n6`.
- The OVS-side bridge ports are `v-upf-host` and `v-edn-host`.
- The Ext-DN-side interface created by OVS is `dn0`.
- ONOS controls OVS through OpenFlow 1.3 on TCP port `6653`.
- Queue shaping is enforced on the OVS egress port toward Ext-DN, which is `v-edn-host`.

### Final interface design

This repository now uses one N6-side interface model only:

- `oai-ext-dn` is attached only to `public_net` by Docker.
- OVS injects the Ext-DN data-plane interface as `dn0`.
- The UPF receives its OVS-facing N6 interface as `n6ovs0`.
- OVS itself also lives on `public_net`, but only for controller reachability; it still uses `pid: host` so it can `nsenter` the UPF and Ext-DN namespaces and reconcile the injected veth pairs.
- The repository uses one canonical ONOS host on `public_net`: `ONOS_HOST=192.168.71.160`. Both ONOS REST and the OVS OpenFlow controller target are derived from that host.
- No Docker-created `eth1` data-plane interface is part of the supported design.

The earlier failure mode came from a mismatch between a one-shot OVS bootstrap and containers that could be restarted independently. After an Ext-DN restart, the old injected veth disappeared with the old namespace, `dn0` was gone, and the container kept waiting forever. The fix in this repo makes OVS continuously reconcile the N6 attachments and makes the Ext-DN and UPF entrypoints fail fast with debug output instead of blocking forever.

Container health checks are intentionally lighter than N6 readiness checks. `oai-ext-dn` is considered live when the container process and its Docker-facing interface are up; `dn0`, route installation, controller connectivity, and forwarding rules are validated separately by `./scripts/verify-n6-readiness.sh`.

The earlier `ping -I dn0 ... 192.168.72.134` style health check was the wrong contract for Docker health. It treated end-to-end N6 dataplane success, which depends on OVS forwarding state and ONOS policy timing, as a container liveness check. The permanent model in this repo keeps Docker health focused on local state such as `dn0`, routes, and bridge presence, while strict reachability moves into operator-facing diagnostics.

The same cleanup was applied to ONOS addressing. Older localhost-based references caused drift between Compose, OVS, and automation. The repo now treats `.env` and `testbed-env.sh` as the single source of truth for `ONOS_HOST`, `ONOS_BASE_URL`, and `ONOS_CTRL`.

The permanent N6 forwarding model now includes four bootstrap forwarding rules for N6:

- ARP: `v-upf-host -> v-edn-host` at priority `45000`
- ARP: `v-edn-host -> v-upf-host` at priority `45000`
- IPv4: `v-upf-host -> v-edn-host` at priority `5000`
- IPv4: `v-edn-host -> v-upf-host` at priority `5000`

`ovs-init.sh` installs these rules locally first so N6 comes up immediately even while ONOS is still converging. `onos-bootstrap-forwarding.py` then mirrors the same rules into ONOS so the controller owns an equivalent forwarding baseline after the switch is discovered. Higher-priority slice queue rules still override the generic IPv4 bootstrap flow when policy enforcement is installed.

| Component | Role in this testbed |
| --- | --- |
| UE | Generates slice-tagged prototype traffic by choosing UDP ports `5201`, `5202`, or `5203` |
| gNB | RF-simulator radio access side for the OAI deployment |
| OAI Core | Provides control plane and user plane, with the UPF forwarding N6 traffic |
| OVS (`br-n6`) | Enforces queue-aware forwarding on the N6 path |
| ONOS | Discovers the OVS bridge as an OpenFlow device and installs policy rules |
| Ext-DN | Receives traffic after queue selection on the OVS egress side |

## Tested environment

The following environment details were observed on the current setup while preparing this README:

| Item | Value |
| --- | --- |
| Host OS | `Ubuntu 24.04.3 LTS` |
| Deployment model | Docker / Docker Compose |
| Main launch command | `docker compose up -d --build` |
| ONOS container image | `onosproject/onos` |
| ONOS runtime observed in container | `3.0.0` |
| ONOS Karaf client path | `/root/onos/apache-karaf-4.2.14/bin/client` |
| OVS container image | `custom-ovs:noble` |
| OVS version observed in container | `2.17.9` |
| OVS container base image | `ubuntu:22.04` in `ovs/Dockerfile` |

> [!IMPORTANT]
> The custom ONOS app may need to be built against a compatible ONOS API version available from Maven repositories, and version alignment must be checked before rebuilding the app.
>
> This repo already shows why that matters:
>
> - The live ONOS runtime is currently `3.0.0`.
> - The current `onos-slice-queue-app/pom.xml` is pinned to ONOS API `2.7.0`.
> - `onos-slice-queue-app/build.log` records a failed build attempt against `3.0.0` because the required ONOS artifacts could not be resolved from Maven.
>
> If the bundle installs but remains `Installed` instead of `Active`, treat that as an ONOS API compatibility problem until proven otherwise.

## Repository structure

The most important files in this framework are:

| Path | Purpose |
| --- | --- |
| `docker-compose.yaml` | The one authoritative deployment definition for the OAI core, gNB, UE, ONOS, OVS, UPF, and Ext-DN services used by this testbed |
| `.env` | Canonical ONOS, OVS, and N6 interface defaults consumed by Compose and helper scripts |
| `testbed-env.sh` | Shared environment loader used by scripts to stay aligned with `.env` |
| `ovs-init.sh` | Creates `br-n6`, wires the UPF and Ext-DN veth pairs, assigns IP addresses, configures queues, and connects OVS to ONOS |
| `ovs-start.sh` | Starts `ovsdb-server`, `ovs-vswitchd`, then runs `ovs-init.sh` |
| `mysql-healthcheck.sh` | Health check used by the MySQL container |
| `install-slice-flows.sh` | Current top-level slice rule installer; tries ONOS REST first and falls back to the custom ONOS app if `SET_QUEUE` is unsupported |
| `deploy-onos-slice-app.sh` | Automates Dockerized Maven build, JAR copy, and ONOS app installation |
| `start-paper-traffic.sh` | Starts `iperf3` traffic for the eMBB, URLLC, and mMTC prototype flows |
| `verify-paper-testbed.sh` | End-to-end verification script for controller connectivity, queues, rules, traffic, and counters |
| `onos-slice-queue-app/` | Custom ONOS bundle that installs `setQueue()` flow rules when REST-based `SET_QUEUE` installation is not supported |
| `install_onos_flows.sh` | Older helper kept in the repo; the current testbed path uses `install-slice-flows.sh` |
| `verify-slicing.sh` | Alternate lightweight verification helper for UDP rule presence and counter growth |
| `scripts/` | Post-deployment verification helpers for Ext-DN interfaces, OVS bridge state, OpenFlow flows, ONOS reachability, and N6 path diagnostics including `check_extdn.sh`, `check_ovs.sh`, `check_onos.sh`, and `check_n6_path.sh` |
| `overrides/` | Explicitly opt-in developer overrides; not part of the authoritative deployment |
| `flexric/docker/docker-compose.yaml` | Separate FlexRIC emulator stack; not part of the OAI + OVS + ONOS N6 slicing deployment |

## Prerequisites

- Docker must be installed and the current user must be able to access the Docker socket.
- Docker Compose must be installed.
- The Docker Compose implementation must support the `interface_name` property used by `docker-compose.yaml`.
- The host must have enough CPU and RAM to run the OAI core, gNB, ONOS, OVS, Ext-DN, and UE containers without repeated healthcheck failures.
- Internet access is required for initial image pulls and if the Dockerized Maven build is used.
- Maven on the host is optional because the repo already supports Dockerized Maven builds.
- `curl`, `python3`, `grep`, and `awk` should be available on the host because the helper scripts use them.

> [!NOTE]
> `start-paper-traffic.sh` installs `iperf3` inside the Ext-DN and UE containers if it is not already present, so package-repository access may also be needed on the first traffic run.

## Deployment steps

### 1. Launch the full stack

Run the deployment from this folder:

```bash
cd ~/Downloads/openairinterfacE5G_J/ci-scripts/yaml_files/5g_rfsimulator
docker compose -f docker-compose.yaml --env-file .env down -v --remove-orphans
docker compose -f docker-compose.yaml --env-file .env up -d --build
```

This is the only supported startup sequence for the main testbed. It brings up the OAI core, gNB, UE containers, ONOS, and the container-networked OVS service defined in this repo.

### 2. Check container status

```bash
docker compose -f docker-compose.yaml --env-file .env ps
```

At minimum, verify that the following services are up and not restarting:

- `rfsim5g-mysql`
- `rfsim5g-oai-amf`
- `rfsim5g-oai-smf`
- `rfsim5g-oai-upf`
- `rfsim5g-oai-ext-dn`
- `rfsim5g-oai-gnb`
- `onos`
- `ovs`

### 3. Verify OVS controller connectivity

```bash
docker exec ovs ovs-vsctl show
docker exec ovs ovs-vsctl get-controller br-n6
```

Expected result:

- `ovs-vsctl show` should contain `is_connected: true`
- `get-controller br-n6` should show the configured ONOS controller endpoint

In the current compose file, OVS is configured to use the canonical ONOS host on `public_net`:

```text
tcp:192.168.71.160:6653
```

### 4. Verify ONOS device discovery

```bash
curl -s -u onos:rocks http://192.168.71.160:8181/onos/v1/devices | python3 -m json.tool
```

To print only the available device IDs:

```bash
curl -s -u onos:rocks http://192.168.71.160:8181/onos/v1/devices | python3 -c 'import sys,json; d=json.load(sys.stdin); print([x["id"] for x in d.get("devices", []) if x.get("available") is True])'
```

Expected result:

- At least one ONOS device should appear as available.
- That device corresponds to the OVS bridge controlled through OpenFlow.

### 5. Install the slice policy

The current repo path is:

```bash
./install-slice-flows.sh
```

What this script does:

- waits for ONOS REST and an available device
- resolves the `v-upf-host` and `v-edn-host` OVS ports
- attempts to install `SET_QUEUE` flows through ONOS REST
- if ONOS REST rejects `SET_QUEUE`, it automatically falls back to `./deploy-onos-slice-app.sh`

If that fallback still does not produce active `set_queue` rules, use the manual ONOS app installation procedure in the next section.

## QoS queues

The queue configuration is created by `ovs-init.sh` on the Ext-DN-facing OVS port `v-edn-host`.

| Queue | Slice | Minimum rate | Maximum rate |
| --- | --- | --- | --- |
| `1` | eMBB | `50 Mbps` | `100 Mbps` |
| `2` | URLLC | `10 Mbps` | `20 Mbps` |
| `3` | mMTC | `1 Mbps` | `5 Mbps` |

The parent QoS object is configured with:

```text
other-config:max-rate=120000000
```

To inspect the queue objects:

```bash
docker exec ovs ovs-vsctl list qos
docker exec ovs ovs-vsctl list queue
```

Queue enforcement happens on the OVS egress side toward Ext-DN, not inside ONOS itself.

## ONOS slice app installation

### Why this may be necessary

Some ONOS REST paths reject flow rules that include `SET_QUEUE`. In that case, you may see an error similar to:

```text
Instruction type SET_QUEUE is not supported
```

That is why this repo includes a custom ONOS bundle in `onos-slice-queue-app/`.

### Recommended manual build and install workflow

From the `5g_rfsimulator` directory:

```bash
cd onos-slice-queue-app
rm -rf target
docker run --rm -v "$PWD":/app -w /app maven:3.9-eclipse-temurin-11 mvn clean package
```

Then copy the JAR into the ONOS container:

```bash
docker cp target/onos-slice-queue-app-1.0.0.jar onos:/tmp/
```

Enter the ONOS Karaf shell:

```bash
docker exec -it onos /root/onos/apache-karaf-4.2.14/bin/client
```

Inside the Karaf shell, install and inspect the bundle:

```text
bundle:install -s file:/tmp/onos-slice-queue-app-1.0.0.jar
bundle:list | grep -i slice
```

> [!IMPORTANT]
> If the bundle remains `Installed` instead of `Active`, there is still an ONOS API version compatibility issue in the app build.

### Practical version-alignment guidance

- The current runtime ONOS version is `3.0.0`.
- The current app `pom.xml` is pinned to `2.7.0`.
- A previous attempt to rebuild against `3.0.0` failed because the needed `onos-api` and `onlab-osgi` artifacts were not found from Maven during the recorded build.

Before changing `onos.version`, verify that the target ONOS API artifacts are actually available from the configured repositories and are compatible with the ONOS runtime you are using.

## Flow rule verification

Use the following command to verify that the expected queue-selection rules are present in OVS:

```bash
docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 | egrep -n "set_queue|tp_dst=5201|tp_dst=5202|tp_dst=5203"
```

Expected output:

- one flow for UDP destination port `5201` that applies `set_queue:1`
- one flow for UDP destination port `5202` that applies `set_queue:2`
- one flow for UDP destination port `5203` that applies `set_queue:3`
- the flows should output toward the Ext-DN-facing port
- after traffic is generated, the `n_packets` and `n_bytes` counters should increase

If the `tp_dst` matches exist but `set_queue` is missing, slice enforcement is not active yet.

## Traffic generation

### Automated traffic script

The repo provides an automated traffic generator:

```bash
./start-paper-traffic.sh
```

What it does:

- ensures `iperf3` is present in Ext-DN and UE containers
- starts UDP `iperf3` servers on Ext-DN ports `5201`, `5202`, and `5203`
- runs a ping test from the URLLC UE
- sends UDP traffic from:
  - `rfsim5g-oai-nr-ue` to port `5201` at `80M`
  - `rfsim5g-oai-nr-ue2` to port `5202` at `20M`
  - `rfsim5g-oai-nr-ue3` to port `5203` at `5M`
- writes logs into `./logs/`

### Manual two-terminal method

You can also test each slice manually using one server terminal and one client terminal. Repeat the pair below for each slice.

Terminal 1, start an `iperf3` server on Ext-DN:

```bash
docker exec -it rfsim5g-oai-ext-dn bash -lc 'iperf3 -s -p 5201'
```

Terminal 2, send eMBB traffic from the UE:

```bash
docker exec -it rfsim5g-oai-nr-ue bash -lc 'iperf3 -u -c 192.168.72.135 -p 5201 -t 20 -b 80M'
```

For URLLC:

```bash
docker exec -it rfsim5g-oai-ext-dn bash -lc 'iperf3 -s -p 5202'
docker exec -it rfsim5g-oai-nr-ue2 bash -lc 'iperf3 -u -c 192.168.72.135 -p 5202 -t 20 -b 20M'
```

For mMTC:

```bash
docker exec -it rfsim5g-oai-ext-dn bash -lc 'iperf3 -s -p 5203'
docker exec -it rfsim5g-oai-nr-ue3 bash -lc 'iperf3 -u -c 192.168.72.135 -p 5203 -t 20 -b 5M'
```

## Final verification

Run the full end-to-end verification script with:

```bash
ONOS_IP=127.0.0.1 ONOS_PORT=8181 ./verify-paper-testbed.sh
```

A `PASS` result means:

- OVS is connected to ONOS
- queues exist on OVS
- the required `set_queue` rules for ports `5201`, `5202`, and `5203` exist
- traffic generation completed
- OVS flow counters increased after traffic

Failure usually means one of the following:

- ONOS never discovered the OVS device
- OVS is not connected to ONOS
- the ONOS app is not active
- the required `set_queue` rules were never installed
- Ext-DN or UPF interfaces were not created correctly
- traffic generation did not actually reach the OVS rules

## Known issues / troubleshooting

| Problem | What it usually means | What to do |
| --- | --- | --- |
| `Instruction type SET_QUEUE is not supported` | The ONOS REST path you are using cannot install `SET_QUEUE` instructions | Use the custom ONOS app workflow in `onos-slice-queue-app/` or run `./deploy-onos-slice-app.sh` |
| Bundle shows `Installed` instead of `Active` | The bundle was copied into ONOS but did not resolve cleanly against the runtime APIs | Rebuild against a compatible ONOS API version and verify Maven artifact availability first |
| ONOS API version mismatch | Runtime ONOS and build-time ONOS dependencies are not aligned | Check the runtime version, the `pom.xml` version, and the Maven repository availability before rebuilding |
| Ext-DN exits because `dn0` is missing | OVS has not finished creating or re-creating the Ext-DN veth attachment | Check `docker logs ovs`, `docker logs rfsim5g-oai-ext-dn`, and run `./scripts/show-ext-dn-interfaces.sh` |
| UPF exits because `n6ovs0` is missing | OVS has not finished creating or re-creating the UPF-side N6 attachment | Check `docker logs ovs`, `docker logs rfsim5g-oai-upf`, and run `./scripts/verify-n6-readiness.sh` |
| Missing `set_queue` rules in OVS | ONOS did not install the expected slice policy | Re-run flow verification, confirm ONOS device discovery, and verify the custom app is `Active` |
| Stale `target/` output or stale JAR | An older or failed build artifact is being reused | Run `rm -rf target` before rebuilding the ONOS app |
| Linux shell versus Karaf shell confusion | Commands are being run in the wrong shell | Run `docker exec ...` from the Linux host shell; run `bundle:install` and `bundle:list` only after entering the Karaf client |
| OVS controller not connected | The controller endpoint is wrong or ONOS is not listening yet on `public_net` | Confirm `docker exec ovs ovs-vsctl get-controller br-n6`, keep `ONOS_CTRL=192.168.71.160:6653`, and verify ONOS is up on `192.168.71.160:6653` |

## Current status

The current status of this testbed is:

- The infrastructure is functional.
- OVS queues and ONOS connectivity are working in the current deployment model.
- The decisive final step for slice enforcement is the presence of active `set_queue` rules.
- Final slice enforcement therefore still depends on successful ONOS app installation, or any other path that results in active `set_queue` rules for UDP ports `5201`, `5202`, and `5203`.

This README does not claim that every environment will already have fully validated slice enforcement out of the box. The repo provides the required plumbing and verification scripts, but the ONOS application activation dependency must be checked explicitly.

## Future work

- Dynamic slice orchestration instead of static prototype rules
- Automatic reinstallation of ONOS-managed flows after controller or switch reconnect
- QFI-based classification instead of the current UDP-port prototype
