---
aliases:
  -
description: Learn how to install the Grafana Mimir Helm chart in a custom namespace.
menuTitle: Install in a custom namespace
title: Install the Helm chart in a custom namespace
weight: 20
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Install the Helm chart in a custom namespace

The Grafana Mimir [Helm](https://helm.sh/) chart allows you to configure, install, and upgrade Grafana Mimir within a Kubernetes cluster.

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. They also assume that you know how to install a Kubernetes cluster, and configure and operate it.

It also assumes that you have an understanding of what the `kubectl` command does.

> **Caution:** This procedure is primarily aimed at local or development setups. To set up in a production environment, see [Run Grafana Mimir in production using the Helm chart]({{< relref "prepare-production-environment/run-production-environment-with-helm/" >}}).

### Hardware requirements

- A single Kubernetes node with a minimum of 4 cores and 16GiB RAM

### Software requirements

- Kubernetes 1.20 or higher
- The `kubectl` command for your version of Kubernetes
- Helm 3 or higher

### Verify that you have

- Access to the Kubernetes cluster
- Persistent storage is enabled in the Kubernetes cluster, which has a default storage class set up. You can [change the default StorageClass](https://kubernetes.io/docs/tasks/administer-cluster/change-default-storage-class/).
- DNS service works in the Kubernetes cluster
- An ingress controller is set up in the Kubernetes cluster, for example [ingress-nginx](https://kubernetes.github.io/ingress-nginx/)

> **Note:** Although this is not strictly necessary, if you want to access Mimir from outside of the Kubernetes cluster, you will need an ingress. This procedure assumes you have an ingress controller set up.

## Steps

Using a custom namespace solves problems later on because you do not have to overwrite the default namespace.

1. Create a unique Kubernetes namespace, for example `mimir-test`:

   ```console
   kubectl create namespace mimir-test
   ```

   For more details, see the Kubernetes documentation about [Creating a new namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/#creating-a-new-namespace).

1. Set up a Helm repository using the following commands:

   ```console
   helm repo add grafana https://grafana.github.io/helm-charts
   helm repo update
   ```

   > **Note:** The Helm chart at [https://grafana.github.io/helm-charts](https://grafana.github.io/helm-charts) is a publication of the source code at [**grafana/mimir**](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed).

1. Configure an ingress:

   a. Create a YAML file of Helm values called `custom.yaml`.

   b. Add the following configuration to the file:

   ```yaml
   nginx:
     ingress:
       enabled: true
       ingressClassName: nginx
       hosts:
         - host: <ingress-host>
           paths:
             - path: /
               pathType: Prefix
       tls:
         # empty, disabled.
   ```

   > **Note:** To see all of the configurable parameters for a Helm chart installation, use `helm show values grafana/mimir-distributed`.

   An ingress enables you to externally access a Kubernetes cluster.
   Replace _`<ingress-host>`_ with a suitable hostname that DNS can resolve
   to the external IP address of the Kubernetes cluster.
   For more information, see [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).

   > **Note:** On Linux systems, and if it is not possible for you set up local DNS resolution, you can use the `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` command-line flag to define the _`<ingress-host>`_ local address for the `docker` commands in the examples that follow.

1. Install Grafana Mimir using the Helm chart:

   ```bash
   helm -n mimir-test install mimir grafana/mimir-distributed -f custom.yaml
   ```

   > **Note:** The output of the command contains the write and read URLs necessary for the following steps.

1. Check the statuses of the Mimir pods:

   ```bash
   kubectl -n mimir-test get pods
   ```

   The results look similar to this:

   ```bash
   kubectl -n mimir-test get pods
   NAME                                            READY   STATUS      RESTARTS   AGE
   mimir-minio-78b59f5569-fhlhs                    1/1     Running     0          2m4s
   mimir-nginx-74f8bff8dc-7kr7z                    1/1     Running     0          2m5s
   mimir-distributed-make-bucket-job-z2hc8         0/1     Completed   0          2m4s
   mimir-overrides-exporter-5fd94b745b-htrdr       1/1     Running     0          2m5s
   mimir-query-frontend-68cbbfbfb5-pt2ng           1/1     Running     0          2m5s
   mimir-ruler-56586c9774-28k7h                    1/1     Running     0          2m5s
   mimir-querier-7894f6c5f9-pj9sp                  1/1     Running     0          2m5s
   mimir-querier-7894f6c5f9-cwjf6                  1/1     Running     0          2m4s
   mimir-alertmanager-0                            1/1     Running     0          2m4s
   mimir-distributor-55745599b5-r26kr              1/1     Running     0          2m4s
   mimir-compactor-0                               1/1     Running     0          2m4s
   mimir-store-gateway-0                           1/1     Running     0          2m4s
   mimir-ingester-1                                1/1     Running     0          2m4s
   mimir-ingester-2                                1/1     Running     0          2m4s
   mimir-ingester-0                                1/1     Running     0          2m4s
   ```

1. Wait until all of the pods have a status of `Running` or `Completed`, which might take a few minutes.
