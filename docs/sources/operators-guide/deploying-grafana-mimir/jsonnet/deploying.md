---
title: "Deploying Grafana Mimir with Jsonnet and Tanka"
menuTitle: "Deploying with Jsonnet"
description: "Learn how to deploy Grafana Mimir on Kubernetes with Jsonnet and Tanka."
weight: 10
---

# Deploying Grafana Mimir with Jsonnet and Tanka

This guide explains how to use [Tanka](https://tanka.dev/) and [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler) to generate Kubernetes YAML manifests from the jsonnet files.

> **Note**: the instructions in this guide assume that your working directory is the root of the Mimir git repository.

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

```console
$ cd jsonnet-example
$ tk show environments/default
```

To output YAML manifests to `./manifests`, run:

```console
$ cd jsonnet-example
$ tk export manifests environments/default
```
