---
aliases:
  -
description: Learn how to visualize metrics in Grafana.
menuTitle: Visualize metrics in Grafana
title: Visualize metrics in Grafana
weight: 50
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Visualize metrics in Grafana

The Grafana Mimir [Helm](https://helm.sh/) chart allows you to configure, install, and upgrade Grafana Mimir within a Kubernetes cluster.

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. They also assume that you know how to install a Kubernetes cluster, and configure and operate it.

It also assumes that you have an understanding of what the `kubectl` command does.

> **Caution:** This procedure is primarily aimed at local or development setups. To set up in a production environment, see [Run Grafana Mimir in production using the Helm chart]({{< relref "../set-up/prepare-production-environment/run-production-environment-with-helm/" >}}).

## Steps

First install Grafana, and then add Mimir as a Prometheus data source.

1. Start Grafana by using Docker:

   ```bash
   docker run \
     --rm \
     --name=grafana \
     -p 3000:3000 \
     grafana/grafana
   ```

   > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by Grafana, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir cluster, by using the following settings:

   | Field | Value                              |
   | ----- | ---------------------------------- |
   | Name  | Mimir                              |
   | URL   | http://\<ingress-host\>/prometheus |

   To add a data source, see [Add a data source](/docs/grafana/latest/datasources/add-a-data-source/).

1. Verify success:

   You should be able to query metrics in [Grafana Explore](http://localhost:3000/explore),
   as well as create dashboard panels by using your newly configured `Mimir` data source.
   For more information, see [Monitor Grafana Mimir]({{< relref "../set-up/prepare-production-environment/run-production-environment-with-helm/monitor-system-health.md" >}}).
