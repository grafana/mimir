---
aliases:
  -
description: Learn how to configure Prometheus to write to Grafana Mimir.
menuTitle: Configure Prometheus
title: Configure Prometheus to write to Grafana Mimir
weight: 40
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Configure Prometheus to write to Grafana Mimir

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

You can either configure Prometheus to write to Grafana Mimir or [configure Grafana Agent to write to Mimir](#configure-grafana-agent-to-write-to-grafana-mimir). Although you can configure both, you do not need to.

Make a choice based on whether or not you already have a Prometheus server set up:

- For an existing Prometheus server:

  1. Add the following YAML snippet to your Prometheus configuration file:

     ```yaml
     remote_write:
       - url: http://<ingress-host>/api/v1/push
     ```

     In this case, your Prometheus server writes metrics to Grafana Mimir, based on what is defined in the existing `scrape_configs` configuration.

  1. Restart the Prometheus server.

- For a Prometheus server that does not exist yet:

  1. Write the following configuration to a `prometheus.yml` file:

     ```yaml
     remote_write:
       - url: http://<ingress-host>/api/v1/push

     scrape_configs:
       - job_name: prometheus
         honor_labels: true
         static_configs:
           - targets: ["localhost:9090"]
     ```

     In this case, your Prometheus server writes metrics to Grafana Mimir that it scrapes from itself.

  1. Start a Prometheus server by using Docker:

     ```bash
     docker run -p 9090:9090  -v <absolute-path-to>/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
     ```

     > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by the Prometheus server, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.
