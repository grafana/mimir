#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Initialise the Tanka.
mkdir jsonnet-example && cd jsonnet-example
# Pin Kubernetes 1.29 explicitly; upstream defaults can drop older library versions.
tk init --k8s=false
# Preserve the legacy import aliases used by the release manifests.
printf '%s\n' '{"version":1,"dependencies":[],"legacyImports":true}' > jsonnetfile.json
jb install github.com/jsonnet-libs/k8s-libsonnet/1.29@bf9a62cfd32a58c071b8410bfcdec058475dd25e
printf "%s\n" "import 'github.com/jsonnet-libs/k8s-libsonnet/1.29/main.libsonnet'" > lib/k.libsonnet

# Use this release line's Jsonnet definitions with its example.
jb install github.com/grafana/mimir/operations/mimir@mimir-distributed-release-5.8

# Use the provided example.
cp vendor/mimir/mimir-manifests.jsonnet.example environments/default/main.jsonnet

# Allow CI to capture the rendered manifests without an interactive pager.
export PAGER=cat
tk show --dangerous-allow-redirect environments/default
