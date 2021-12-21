# Jsonnet for Mimir on Kubernetes

This folder has the jsonnet for deploying Mimir in Kubernetes. To generate the YAMLs for deploying Mimir follow these instructions.

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
