# Jsonnet for Mimir on Kubernetes

This folder has the jsonnet for deploying Mimir in Kubernetes. To generate the YAMLs for deploying Mimir follow these instructions.

## Quick start

### 1. Make sure you have `tanka` and `jb` installed

Follow the steps at https://tanka.dev/install. If you have `go` installed locally you can also use:

```console
$ # make sure to be outside of GOPATH or a go.mod project
$ go install github.com/grafana/tanka/cmd/tk@latest
$ go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest
```

### 2. Setup Jsonnet project

Initialise the Tanka, install the Mimir and Kubernetes Jsonnet libraries, and setup an environment based on the provided example.

<!-- prettier-ignore-start -->
[embedmd]:# (./getting-started.sh)
```sh
#!/usr/bin/env bash
set -e

# Initialise the Tanka.
mkdir jsonnet-example && cd jsonnet-example
tk init --k8s=1.18

# Install Mimir jsonnet.
jb install github.com/grafana/mimir/operations/mimir@main

# Use the provided example.
cp vendor/mimir/mimir-manifests.jsonnet.example environments/default/main.jsonnet

# Generate the YAML manifests.
export PAGER=cat
tk show environments/default
```
<!-- prettier-ignore-end -->

### 3. Generate the YAML manifests

```console
$ cd jsonnet-example
$ tk show environments/default
```

To output YAML manifests to `./manifests`, run:

```console
$ cd jsonnet-example
$ tk export manifests environments/default
```

## Configuration

### Anti-affinity

Given the distributed nature of Mimir, both performance and reliability are improved when pods are spread across different nodes: for example, multiple queriers can be processing the same query at the same time, so it's better to distribute them across different nodes, and since losing multiple ingesters can cause data loss, it's also important to have them spread.
This is why, by default, anti-affinity rules are applied to some deployments and statefulsets.
This can become an issue when playing with Mimir in a single-node kubernetes cluster, so anti-affinity can be disabled by enabling the configuration values `_config.<component>_allow_multiple_replicas_on_same_node`.

For example:

```jsonnet
local mimir = 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    // ... default configuration values
    distributor_allow_multiple_replicas_on_same_node: true,
    ingester_allow_multiple_replicas_on_same_node: true,
    ruler_allow_multiple_replicas_on_same_node: true,
    querier_allow_multiple_replicas_on_same_node: true,
    query_frontend_allow_multiple_replicas_on_same_node: true,
  },
}
```
