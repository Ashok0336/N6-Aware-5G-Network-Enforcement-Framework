# Distributed Three-Machine Deployment

This deployment keeps the OAI control plane, RF-simulated RAN, and N6 enforcement edge on separate physical hosts while preserving the existing OAI container IPs, Docker service names, RFsim addressing, and local N6 datapath.

## Machine Roles

| Role | Host | Physical LAN IP | Services |
| --- | --- | --- | --- |
| CORE | `iot-lab-server` / `iotlab3` | `129.24.28.58` | `mysql`, `oai-amf`, `oai-smf` |
| RAN | `iotlab2` | `129.24.28.56` | `oai-gnb`, `oai-nr-ue` through `oai-nr-ue10` |
| N6 EDGE | `iotlab1` | `129.24.28.222` | `oai-upf`, `oai-ext-dn`, `onos`, `ovs`, `prometheus`, `grafana`, telemetry, Digital Twin, policy-manager |

The gNB and all RFsim UEs must stay together on the RAN machine. The UPF, OVS, and Ext-DN must stay together on the N6 EDGE machine because `ovs-init.sh` uses the local Docker socket and container namespaces to inject `n6ovs0` and `dn0`.

## Required Swarm State

The three hosts must already be in the same Docker Swarm:

| Swarm role | Physical LAN IP |
| --- | --- |
| Manager | `129.24.28.58` |
| Worker | `129.24.28.56` |
| Worker | `129.24.28.222` |

Check node status from a manager:

```bash
docker node ls
```

## Shared Overlay Network

The shared Docker network must already exist before Compose is used:

```text
rfsim5g-oai-public-net
192.168.71.128/26
```

Check it on each host:

```bash
docker network ls | grep rfsim5g
docker network inspect rfsim5g-oai-public-net --format '{{.Driver}} {{.Attachable}}'
```

Expected driver:

```text
overlay
```

Do not recreate this network from Docker Compose. The `public_net` Compose network is external and maps to the existing overlay.

## Addressing Model

The physical `129.24.28.x` addresses are only the Docker Swarm underlay. OAI services continue to communicate through Docker DNS names and the shared overlay addresses:

| Service | Overlay address |
| --- | --- |
| `mysql` | `192.168.71.131` |
| `oai-amf` | `192.168.71.132` |
| `oai-smf` | `192.168.71.133` |
| `oai-upf` | `192.168.71.134` |
| `oai-ext-dn` | `192.168.71.135` |
| `oai-gnb` | `192.168.71.140` |
| `oai-nr-ue` through `oai-nr-ue10` | `192.168.71.150` through `192.168.71.159` |
| `onos` | `192.168.71.160` |
| `ovs` | `192.168.71.161` |

Do not replace these values with physical host IPs in `mini_nonrf_config.yaml`, the gNB config, UE RFsim options, or the N6 scripts.

## Startup Order

Run commands from `ci-scripts/yaml_files/5g_rfsimulator` on the indicated machine. Do not run a full unqualified deployment on every host.

Never run this on all three machines:

```bash
docker compose up -d
```

That would create duplicate network functions. Start only the services assigned to each physical machine.

### 1. CORE: Machine 1

```bash
docker compose -f docker-compose.yaml --env-file .env up -d mysql oai-amf oai-smf
```

### 2. N6 EDGE: Machine 3

Use `--no-deps` so the edge host does not start another SMF because `oai-upf` depends on `oai-smf`.

```bash
docker compose -f docker-compose.yaml --env-file .env up -d --no-deps oai-upf oai-ext-dn onos ovs
```

For monitoring on the edge host:

```bash
docker compose -f docker-compose.yaml --env-file .env --profile monitoring up -d --no-deps prometheus grafana
```

Start telemetry, Digital Twin, and policy-manager components after the core, edge, and initial RAN attachment are healthy, following the existing automation scripts for this repository.

### 3. RAN: Machine 2

Use `--no-deps` so the RAN host does not start another Ext-DN because `oai-gnb` depends on `oai-ext-dn`.

```bash
docker compose -f docker-compose.yaml --env-file .env up -d --no-deps oai-gnb
docker compose -f docker-compose.yaml --env-file .env up -d --no-deps oai-nr-ue
```

Verify one UE registration and PDU session first. Then start the remaining UEs:

```bash
docker compose -f docker-compose.yaml --env-file .env up -d --no-deps \
  oai-nr-ue2 oai-nr-ue3 oai-nr-ue4 oai-nr-ue5 oai-nr-ue6 \
  oai-nr-ue7 oai-nr-ue8 oai-nr-ue9 oai-nr-ue10
```

All UE definitions must continue to use:

```text
--rfsimulator.serveraddr 192.168.71.140
```

## N6 Enforcement Path

N6 enforcement remains entirely on Machine 3:

```text
UPF -> n6ovs0 -> OVS br-n6 -> dn0 -> Ext-DN
```

Detailed interface path:

```text
UPF
 |
 n6ovs0
 192.168.72.134/26
 |
 v-upf-host
 |
 OVS br-n6
 |
 v-edn-host
 |
 dn0
 192.168.72.135/26
 |
 Ext-DN
```

The `192.168.72.128/26` N6 subnet is not a Docker overlay subnet. It is the local N6 datapath between UPF, OVS, and Ext-DN on Machine 3.

## Verification Commands

Docker node status:

```bash
docker node ls
```

Overlay network:

```bash
docker network ls | grep rfsim5g
docker network inspect rfsim5g-oai-public-net --format '{{.Driver}} {{.IPAM.Config}}'
```

Running containers on each host:

```bash
docker compose -f docker-compose.yaml --env-file .env ps
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Networks}}'
```

AMF/gNB N2 connection:

```bash
docker logs rfsim5g-oai-amf 2>&1 | grep -Ei 'ng setup|gnb|sctp|n2'
docker logs rfsim5g-oai-gnb 2>&1 | grep -Ei 'ng setup|amf|sctp|n2'
```

SMF/UPF PFCP N4 association:

```bash
docker logs rfsim5g-oai-smf 2>&1 | grep -Ei 'pfcp|association|n4|upf'
docker logs rfsim5g-oai-upf 2>&1 | grep -Ei 'pfcp|association|n4|smf'
```

gNB/UPF N3:

```bash
docker logs rfsim5g-oai-gnb 2>&1 | grep -Ei 'ngu|gtp|n3|pdu'
docker logs rfsim5g-oai-upf 2>&1 | grep -Ei 'gtp|n3|pdu|session'
```

OVS bridge on Machine 3:

```bash
docker exec ovs ovs-vsctl show
docker exec ovs ovs-vsctl list-ports br-n6
docker exec ovs ovs-ofctl -O OpenFlow13 show br-n6
```

ONOS connection:

```bash
docker exec ovs ovs-vsctl get-controller br-n6
docker exec ovs ovs-vsctl list controller
curl -s -u onos:rocks http://192.168.71.160:8181/onos/v1/devices | python3 -m json.tool
```

OpenFlow flows:

```bash
docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6
curl -s -u onos:rocks http://192.168.71.160:8181/onos/v1/flows | python3 -m json.tool
```

UE registration:

```bash
docker logs rfsim5g-oai-nr-ue 2>&1 | grep -Ei 'registration|registered|pdu session|ip'
docker logs rfsim5g-oai-amf 2>&1 | grep -Ei 'registration|imsi|supi|ue context'
```

UE PDU-session IP:

```bash
docker exec rfsim5g-oai-nr-ue ip -br addr
docker exec rfsim5g-oai-nr-ue ip route
```

UE to Ext-DN connectivity:

```bash
docker exec rfsim5g-oai-ext-dn ip -br addr
docker exec rfsim5g-oai-ext-dn ip route
docker exec rfsim5g-oai-nr-ue ping -c 3 192.168.72.135
```

Repository helper checks, after the selected services are up:

```bash
./scripts/check_containers.sh
./scripts/check_ovs.sh
./scripts/check_onos.sh
./scripts/check_n6_path.sh
./scripts/check_closed_loop.sh
```
