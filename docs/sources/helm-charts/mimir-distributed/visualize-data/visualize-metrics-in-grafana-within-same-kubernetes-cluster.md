---
aliases:
  -
description: Visualize metrics in Grafana, which is running within the same Kubernetes cluster.
menuTitle: Visualize metrics Grafana within the same Kubernetes cluster
title: Visualize metrics in Grafana that is running within the same Kubernetes cluster
weight: 50
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Visualize metrics in Grafana, which is running within the same Kubernetes cluster

The Grafana Mimir [Helm](https://helm.sh/) chart allows you to configure, install, and upgrade Grafana Mimir within a Kubernetes cluster.

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. They also assume that you know how to install a Kubernetes cluster, and configure and operate it.

It also assumes that you have an understanding of what the `kubectl` command does.

> **Note:** If you are using GEM, make sure to set up an access policy and associated token that has the `metrics:read` scope. For more information, see the [Admin API documentation](https://grafana.com/docs/enterprise-metrics/latest/setup-gem-plugin-grafana/) or [Set up the GEM plugin for Grafana](https://grafana.com/docs/enterprise-metrics/latest/admin-api/).

### Verify that you have

- Access to the Kubernetes cluster
- Persistent storage is enabled in the Kubernetes cluster, which has a default storage class set up. You can [change the default StorageClass](https://kubernetes.io/docs/tasks/administer-cluster/change-default-storage-class/).
- DNS service works in the Kubernetes cluster
- An ingress controller is set up in the Kubernetes cluster, for example [ingress-nginx](https://kubernetes.github.io/ingress-nginx/)

> **Note:** Although this is not strictly necessary, if you want to access Mimir from outside of the Kubernetes cluster, you will need an ingress. This procedure assumes you have an ingress controller set up.

## Steps

1. Install Grafana in the same Kubernetes cluster.

   For details, see [Deploy Grafana on Kubernetes](/docs/grafana/latest/setup-grafana/installation/kubernetes/).

1. Port-forward Grafana to `localhost`, by using the `kubectl` command:

   ```bash
   kubectl port-forward service/grafana 3000:3000
   ```

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir server, by using the following settings:

   | Field | Mimir and GEM values                            |
   | ----- | ----------------------------------------------- |
   | Name  | \<arbitrary-name>                               |
   | URL   | \<internal-prometheus-url>, for example:        |
   |       | http://mimir-nginx.mimir-test.svc:80/prometheus |

   | Field      | GEM-specific values |
   | ---------- | ------------------- |
   | Name       | \<arbitrary-name>   |
   | Basic auth | Enabled             |
   | User       | \*                  |
   | Password   | \<token>            |

   To add a data source, see [Add a data source](/docs/grafana/latest/datasources/add-a-data-source/).

1. Verify success:

   You should be able to query metrics in [Grafana Explore](/docs/grafana/latest/explore/),
   as well as create dashboard panels by using your newly configured `Mimir` data source.
