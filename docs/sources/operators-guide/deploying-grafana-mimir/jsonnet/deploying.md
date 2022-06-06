---
title: "Deploying Grafana Mimir with Jsonnet and Tanka"
menuTitle: "Deploying with Jsonnet"
description: "Learn how to deploy Grafana Mimir on Kubernetes with Jsonnet and Tanka."
weight: 10
---

# Deploying Grafana Mimir with Jsonnet and Tanka

This guide explains how to use [Tanka](https://tanka.dev/) and [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler) to generate Kubernetes YAML manifests from the jsonnet files.

## 1. Make sure you have `tanka` and `jb` installed

Follow the steps at [https://tanka.dev/install](https://tanka.dev/install). If you have `go` installed locally you can also use:

```console
$ # make sure to be outside of GOPATH or a go.mod project
$ go install github.com/grafana/tanka/cmd/tk@latest
$ go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest
```

## 2. Setup a Jsonnet project

Initialise Tanka, install Grafana Mimir and Kubernetes Jsonnet libraries, and setup an environment based on the provided example.

<!-- prettier-ignore-start -->
[embedmd]:# (../../../../../operations/mimir/getting-started.sh)
```sh
#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -e

# Initialise the Tanka.
mkdir jsonnet-example && cd jsonnet-example
tk init --k8s=1.21

# Install Mimir jsonnet.
jb install github.com/grafana/mimir/operations/mimir@main

# Use the provided example.
cp vendor/mimir/mimir-manifests.jsonnet.example environments/default/main.jsonnet

# Generate the YAML manifests.
export PAGER=cat
tk show environments/default
```
<!-- prettier-ignore-end -->

## 3. Generate the Kubernetes YAML manifests

Generate Kubernetes YAML manifests and store them in the `./manifests` directory:

```console
tk export manifests environments/default
```

## 4. Deploy to a Kubernetes cluster

You have two options to deploy the manifests to a Kubernetes cluster:

1. Use `tk apply` command
2. Use `kubectl apply` command

### Use `tk apply` command

Tanka supports commands to show the `diff` and `apply` changes to a Kubernetes cluster:

```console
# Show the difference between your Jsonnet definition and Kubernetes cluster.
tk diff environments/default

# Apply changes to your Kubernetes cluster.
tk apply environments/default
```

> **Note**: before using these commands, you need to configure the environment specification file at `environments/default/spec.json`. You can follow the [Tanka Jsonnet tutorial](https://tanka.dev/tutorial/jsonnet) to learn more on how to use Tanka and configure `spec.json`.

### Use `kubectl apply` command

You generated the Kubernetes manifests and stored them in the `./manifests` directory in the previous step.
You can run the following command to directly apply these manifests to your Kubernetes cluster:

```console
# Review changes that would be applied to your Kubernetes cluster.
kubectl apply --dry-run=client -k manifests/

# Apply changes to your Kubernetes cluster.
kubectl apply -k manifests/
```

> **Note**: the generated Kubernetes manifests create resources in the `default` namespace. To use a different namespace, you can change the `namespace` configuration option in `environments/default/main.jsonnet` and re-generate the Kubernetes manifests.
