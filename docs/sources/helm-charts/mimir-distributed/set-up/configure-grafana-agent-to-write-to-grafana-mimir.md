---
aliases:
  -
description: Learn how to configure Grafana Agent to write to Grafana Mimir.
menuTitle: Configure Grafana Agent
title: Configure Grafana Agent to write to Grafana Mimir
weight: 50
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Configure Grafana Agent to write to Grafana Mimir

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

You can either configure Grafana Agent to write to Grafana Mimir or [configure Prometheus to write to Mimir]({{< relref "configure-prometheus-to-write-to-grafana-mimir" >}}). Although you can configure both, you do not need to.

Make a choice based on whether or not you already have a Grafana Agent set up:

- For an existing Grafana Agent:

  1. Add the following YAML snippet to your Grafana Agent metrics configurations (`metrics.configs`):

     ```yaml
     remote_write:
       - url: http://<ingress-host>/api/v1/push
     ```

     In this case, your Grafana Agent will write metrics to Grafana Mimir, based on what is defined in the existing `metrics.configs.scrape_configs` configuration.

  1. Restart the Grafana Agent.

- For a Grafana Agent that does not exist yet:

  1. Write the following configuration to an `agent.yaml` file:

     ```yaml
     metrics:
       wal_directory: /tmp/grafana-agent/wal

       configs:
         - name: agent
           scrape_configs:
             - job_name: agent
               static_configs:
                 - targets: ["127.0.0.1:12345"]
           remote_write:
             - url: http://<ingress-host>/api/v1/push
     ```

     In this case, your Grafana Agent writes metrics to Grafana Mimir that it scrapes from itself.

  1. Create an empty directory for the write ahead log (WAL) of the Grafana Agent.

  1. Start a Grafana Agent by using Docker:

     ```bash
     docker run \
       -v <absolute-path-to-wal-directory>:/etc/agent/data \
       -v <absolute-path-to>/agent.yaml:/etc/agent/agent.yaml \
       -p 12345:12345 \
       grafana/agent
     ```

     > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by the Grafana Agent, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.
