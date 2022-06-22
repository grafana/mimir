---
title: "Getting started with Grafana Mimir using a Helm chart"
menuTitle: "Getting started using a Helm chart"
description: "Learn how to get started with Grafana Mimir using a Helm chart."
weight: 20
---

# Getting started with Grafana Mimir using a Helm chart

TBD

## Before you begin

Install the following software:

- Either a [Prometheus server](https://prometheus.io/docs/prometheus/latest/installation/) or [Grafana Agent](https://grafana.com/docs/grafana-cloud/agent/#installing-the-grafana-agent).
<!-- TBD, Krajo to figure out: - Verify that you have enough memery overall and number of cores. -->
- Helm 3 or higher
- A DNS service
- K8s 1.10 or higher

  **Note:** We support generalized instructions regardless of any specific K8s flavor. The information that follows assumes that you are able to install K8s, and configure and operate it.

- The kubectl command for your version of K8s

Verify that you have:

- Access to the K8s cluster
- Persistent storage
- An ingress controller set up

  **Note:** Although this is not strictly necessary, if you want to access Mimir from outside of the K8s cluster, you will need an ingress.

## Configure Prometheus to write to Grafana Mimir <!-- Keep this section, but include different information: -->

Add the following YAML snippet to your Prometheus configuration file and restart the Prometheus server:

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

The configuration for a Prometheus server that scrapes itself and writes those metrics to Grafana Mimir looks similar to this:

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push

scrape_configs:
  - job_name: prometheus
    honor_labels: true
    static_configs:
      - targets: ["localhost:9090"]
```

## Configure Grafana Agent to write to Grafana Mimir <!-- Check this section for changes: -->

Add the following YAML snippet to one of your Agent metrics configurations (`metrics.configs`) in your Agent configuration file and restart the Grafana Agent:

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

The configuration for an Agent that scrapes itself for metrics and writes those metrics to Grafana Mimir looks similar to this:

```yaml
server:
  http_listen_port: 12345
  grpc_listen_port: 54321

metrics:
  wal_directory: /tmp/grafana-agent/wal

  configs:
    - name: agent
      scrape_configs:
        - job_name: agent
          static_configs:
            - targets: ["127.0.0.1:12345"]
      remote_write:
        - url: http://localhost:9009/api/v1/push
```

## Install the Helm chart in a custom namespace

This will solve problems later on because you are not overwriting the default namespace.

1. [Create a unique K8s namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/#creating-a-new-namespace).
1. Set up a Helm repository to use:
  `helm repo add grafana`
  `helm repo update`

 See https://grafana.github.io/helm-charts.


## Query data in Grafana

In a new terminal, run a local Grafana server using Docker:

```bash
docker run --rm --name=grafana --network=host grafana/grafana
```

### Add Grafana Mimir as a Prometheus data source

1. In a browser, go to the Grafana server at [http://localhost:3000/datasources](http://localhost:3000/datasources).
1. Sign in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Grafana Mimir server using the following settings:

   | Field | Value                                                                |
   | ----- | -------------------------------------------------------------------- |
   | Name  | Mimir                                                                |
   | URL   | [http://localhost:9009/prometheus](http://localhost:9009/prometheus) |

To add a data source, refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

## Verify success

When you have completed the tasks in this getting started guide, you can query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/)
as well as create dashboard panels using the newly configured Grafana Mimir data source.
