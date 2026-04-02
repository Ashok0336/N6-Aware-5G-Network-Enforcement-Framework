# ONOS Slice Queue App

Use this app when your ONOS REST API rejects `SET_QUEUE` (HTTP 400).

It installs ONOS flow rules with:

- `udp,tp_dst=5201` -> `setQueue(1)` -> output `v-edn-host`
- `udp,tp_dst=5202` -> `setQueue(2)` -> output `v-edn-host`
- `udp,tp_dst=5203` -> `setQueue(3)` -> output `v-edn-host`
- reverse IPv4 flow `v-edn-host -> v-upf-host`

## Build

```bash
cd ci-scripts/yaml_files/5g_rfsimulator/onos-slice-queue-app
mvn clean package
```

## Install on ONOS

Copy bundle into ONOS container and install via Karaf:

```bash
docker cp target/onos-slice-queue-app-1.0.0.jar onos:/tmp/
docker exec -it onos /bin/bash -lc 'onos-app localhost install! /tmp/onos-slice-queue-app-1.0.0.jar'
```

Then verify:

```bash
docker exec ovs ovs-ofctl -O OpenFlow13 dump-flows br-n6 | egrep "tp_dst=5201|tp_dst=5202|tp_dst=5203|set_queue"
```
