FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    openvswitch-switch iproute2 iputils-ping net-tools tcpdump ca-certificates netcat-openbsd \
    docker.io util-linux \
 && rm -rf /var/lib/apt/lists/*

# sanity check: make sure ovs-ctl exists
RUN test -f /usr/share/openvswitch/scripts/ovs-ctl

CMD ["/bin/bash", "-c", "\
  mkdir -p /var/run/openvswitch /var/log/openvswitch && \
  /usr/share/openvswitch/scripts/ovs-ctl start --system-id=random --no-mlockall --ovsdb-server-options='--remote=punix:/var/run/openvswitch/db.sock' && \
  tail -f /var/log/openvswitch/ovs-vswitchd.log"]
