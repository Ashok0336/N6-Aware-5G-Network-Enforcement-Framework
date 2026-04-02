FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    openvswitch-switch iproute2 iputils-ping net-tools tcpdump ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# sanity check: make sure ovs-ctl exists
RUN test -f /usr/share/openvswitch/scripts/ovs-ctl

ENTRYPOINT ["/bin/sh","-c"]
