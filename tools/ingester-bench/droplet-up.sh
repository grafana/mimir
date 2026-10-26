#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Sets up a droplet for the bench as the native metadata prototype's first one was, so that a batch's droplets are
# alike: its tools at that droplet's versions, this repository at a commit, and kind-up.sh's cluster with CPUs 0 and 1
# kept for the host's daemons. It prints the tools' versions and the CPU model, which every droplet of a batch has to
# share; a droplet with another model is destroyed and created again.
#
#   HOST=<the droplet's address> COMMIT=<commit> tools/ingester-bench/droplet-up.sh
#
# The first droplet is a g-32vcpu-128gb in lon1, of debian-13-x64, with an Intel Xeon Gold 6140; another like it is
#
#   doctl compute droplet create <name> --size g-32vcpu-128gb --region lon1 --image debian-13-x64 --ssh-keys <key> --wait
#
# It needs root over SSH, and the commit pushed to grafana/mimir's arve/native-metadata-prototype. The droplets are
# destroyed between phases.
set -euo pipefail

: "${HOST:?the address of the droplet}"
: "${COMMIT:?the commit to check out}"
KIND_VERSION="${KIND_VERSION:-v0.33.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.37.1}"
GO_VERSION="${GO_VERSION:-1.27.3}"
RESERVED_CPUS="${RESERVED_CPUS:-0-1}"

ssh root@"$HOST" bash -s -- "$COMMIT" "$KIND_VERSION" "$KUBECTL_VERSION" "$GO_VERSION" "$RESERVED_CPUS" <<'EOF'
set -euo pipefail
commit=$1 kind_version=$2 kubectl_version=$3 go_version=$4 reserved=$5
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq docker.io docker-buildx git make jq python3 tmux curl ca-certificates sysstat sudo >/dev/null
curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${kind_version}/kind-linux-amd64"
curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${kubectl_version}/bin/linux/amd64/kubectl"
chmod +x /usr/local/bin/kind /usr/local/bin/kubectl
rm -rf /usr/local/go
curl -fsSL "https://go.dev/dl/go${go_version}.linux-amd64.tar.gz" | tar -C /usr/local -xz
ln -sf /usr/local/go/bin/go /usr/local/bin/go
if [[ ! -d /root/mimir ]]; then
  git clone -q --filter=blob:none https://github.com/grafana/mimir.git /root/mimir
fi
git -C /root/mimir fetch -q origin arve/native-metadata-prototype
git -C /root/mimir checkout -q --detach "$commit"
if ! kind get clusters 2>/dev/null | grep -qx ingester-bench; then
  (cd /root/mimir && RESERVED_CPUS="$reserved" tools/ingester-bench/kind-up.sh)
fi
docker version --format "docker {{.Server.Version}}"
kind version
kubectl version --client | head -1
go version
lscpu | grep -E "^Model name|^CPU\(s\)|^Thread"
EOF
