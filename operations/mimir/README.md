# Jsonnet for Mimir on Kubernetes

This folder has the jsonnet for deploying Mimir in Kubernetes.

To generate the YAMLs for deploying Mimir:

1. Make sure you have tanka and jb installed:

   Follow the steps at https://tanka.dev/install. If you have `go` installed locally you can also use:

    ```console
    $ # make sure to be outside of GOPATH or a go.mod project
    $ GO111MODULE=on go get github.com/grafana/tanka/cmd/tk
    $ GO111MODULE=on go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
    ```

1. Initialise the Tanka, and install the Mimir and Kubernetes Jsonnet libraries.

    ```console
    $ mkdir <name> && cd <name>
    $ tk init --k8s=false
    $ # The k8s-alpha library supports Kubernetes versions 1.14+
    $ jb install github.com/jsonnet-libs/k8s-alpha/1.18
    $ cat <<EOF > lib/k.libsonnet
      (import "github.com/jsonnet-libs/k8s-alpha/1.18/main.libsonnet")
      + (import "github.com/jsonnet-libs/k8s-alpha/1.18/extensions/kausal-shim.libsonnet")
      EOF
    $ jb install github.com/grafana/mimir/operations/mimir@main
    ```

1. Use the example monitoring.jsonnet.example:

    ```console
    $ cp vendor/mimir/mimir-manifests.jsonnet.example environments/default/main.jsonnet
    ```

1. Check what is in the example:

    ```console
    $ cat environments/default/main.jsonnet
    ...
    ```

1. Generate the YAML manifests:

    ```console
    $ tk show environments/default
    ```

   To output YAML manifests to `./manifests`, run:

    ```console
    $ tk export manifests environments/default
    ```
