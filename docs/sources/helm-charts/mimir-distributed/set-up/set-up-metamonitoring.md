---
aliases:
  -
description: Learn how to set up metamonitoring of Grafana Mimir of GEM.
menuTitle: Set up metamonitoring
title: Set up metamonitoring
weight: 80
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Set up metamonitoring

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

Grafana Mimir metamonitoring collects metrics or logs, or both,
about Grafana Mimir itself.
In the example that follows, metamonitoring scrapes metrics about
Grafana Mimir itself, and then writes those metrics to the same Grafana Mimir instance.

1. To enable metamonitoring in Grafana Mimir, add the following YAML snippet to your Grafana Mimir `custom.yaml` file:

   ```yaml
   metaMonitoring:
     serviceMonitor:
       enabled: true
     grafanaAgent:
       enabled: true
       installOperator: true
       metrics:
         additionalRemoteWriteConfigs:
           - url: "http://mimir-nginx.mimir-test.svc:80/api/v1/push"
   ```

1. Upgrade Grafana Mimir by using the `helm` command:

   ```bash
   helm -n mimir-test upgrade mimir grafana/mimir-distributed -f custom.yaml
   ```

1. From [Grafana Explore](http://localhost:3000/explore), verify that your metrics are being written to Grafana Mimir, by querying `sum(rate(cortex_ingester_ingested_samples_total[$__rate_interval]))`.
