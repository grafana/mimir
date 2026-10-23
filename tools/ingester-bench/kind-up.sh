#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Sets up a host of its own, such as a temporary droplet with dedicated CPUs, to run the bench in a kind cluster:
#
#   tools/ingester-bench/kind-up.sh
#
# It reserves CPU 0's physical core for the host: Docker's and containerd's daemons, the local registry, and the kind
# node's kubelet and containerd only run on it, and the kubelet's static CPU manager gives the rest out in whole cores
# (kind.yaml). Then run the bench from the host, so that localhost:5001 is the registry:
#
#   IMAGE_REPO=localhost:5001/ingester-bench KUBE_CONTEXT=kind-ingester-bench NAMESPACE=default GUARANTEED=1 \
#     RESULTS=~/results CONFIG=... tools/ingester-bench/run-in-kubernetes.sh
#
# and check-isolation.sh once its pod runs. It needs docker, kind and kubectl, and root for the host's daemons.
set -euo pipefail

cluster="${CLUSTER:-ingester-bench}"
cd "$(git rev-parse --show-toplevel)"

# CPU 0 and its SMT siblings: a whole physical core, as the static CPU manager's full-pcpus-only needs. RESERVED_CPUS
# overrides it, as on a cloud machine that shows no SMT, where CPU 0 alone would leave the daemons a single CPU.
reserved="${RESERVED_CPUS:-$(cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list)}"
echo "reserving CPUs $reserved for the host's daemons"
for unit in docker.service containerd.service; do
  sudo systemctl set-property "$unit" AllowedCPUs="$reserved"
done

if [[ -z "$(docker ps -q -f name='^kind-registry$')" ]]; then
  docker run -d --restart=always --cpuset-cpus="$reserved" -p 127.0.0.1:5001:5000 --network bridge --name kind-registry registry:2
fi

config="$(mktemp)"
sed "s/RESERVED_CPUS/$reserved/" tools/ingester-bench/kind.yaml > "$config"
kind create cluster --name "$cluster" --config "$config"

# The node pulls localhost:5001's images from the registry's container.
for node in $(kind get nodes --name "$cluster"); do
  docker exec "$node" mkdir -p /etc/containerd/certs.d/localhost:5001
  printf '[host."http://kind-registry:5000"]\n' | docker exec -i "$node" cp /dev/stdin /etc/containerd/certs.d/localhost:5001/hosts.toml
  for unit in kubelet.service containerd.service; do
    docker exec "$node" systemctl set-property "$unit" AllowedCPUs="$reserved" ||
      echo "couldn't keep $unit of $node to CPUs $reserved: check-isolation.sh shows whether it matters" >&2
  done
done
if [[ "$(docker inspect -f '{{json .NetworkSettings.Networks.kind}}' kind-registry)" == null ]]; then
  docker network connect kind kind-registry
fi
kubectl --context "kind-$cluster" apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:5001"
EOF
echo "kind cluster kind-$cluster is up, with CPUs $reserved reserved"
