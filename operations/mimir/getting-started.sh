#!/usr/bin/env bash
set -e

# Initialise the Tanka.
mkdir jsonnet-example && cd jsonnet-example
tk init --k8s=false

# Install Kubernetes Jsonnet libraries.
# The k8s-alpha library supports Kubernetes versions 1.14+
jb install github.com/jsonnet-libs/k8s-alpha/1.18
cat <<EOF > lib/k.libsonnet
(import "github.com/jsonnet-libs/k8s-alpha/1.18/main.libsonnet") +
(import "github.com/jsonnet-libs/k8s-alpha/1.18/extensions/kausal-shim.libsonnet")
EOF

# Install Mimir jsonnet.
jb install github.com/grafana/mimir/operations/mimir@main

# Use the provided example.
cp vendor/mimir/mimir-manifests.jsonnet.example environments/default/main.jsonnet

# Generate the YAML manifests.
export PAGER=cat
tk show environments/default
