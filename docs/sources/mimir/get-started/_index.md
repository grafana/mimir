---
aliases:
  - getting-started/
description: Learn how to get started with Grafana Mimir.
menuTitle: Get started
title: Get started with Grafana Mimir
weight: 10
---

# Get started with Grafana Mimir

You can get started with Grafana Mimir _imperatively_ or _declaratively_:

- **Imperatively**: The written instructions that follow contain commands to help you start a single Mimir process. You would need to perform the commands again to start another Mimir process.
- **Declaratively**: The following video tutorial uses `docker-compose` to deploy multiple Mimir processes. Therefore, if you want to deploy multiple Mimir processes later, the majority of the configuration work will have already been done.

  {{< vimeo 691947043 >}}

## Before you begin

- Verify that you have installed either a [Prometheus server](https://prometheus.io/docs/prometheus/latest/installation/) or [Grafana Alloy](https://grafana.com/docs/alloy/<ALLOY_VERSION>/set-up/install).
- Verify that you have installed [Docker](https://docs.docker.com/engine/install/).

{{< admonition type="note" >}}
The instructions that follow help you to deploy Grafana Mimir in [Monolithic mode]({{< relref "../references/architecture/deployment-modes#monolithic-mode" >}}).

For information about the different ways to deploy Grafana Mimir, refer to [Grafana Mimir deployment modes]({{< relref "../references/architecture/deployment-modes" >}}).
{{< /admonition >}}

## Download Grafana Mimir

In a terminal, run one of the following commands:

- Using Docker:

  ```bash
  docker pull grafana/mimir:latest
  ```

- Using a local binary:

  Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture, and make it executable.

  For Linux with the AMD64 architecture:

  ```bash
  curl -fLo mimir https://github.com/grafana/mimir/releases/latest/download/mimir-linux-amd64
  chmod +x mimir
  ```

## Start Grafana Mimir

To run Grafana Mimir as a monolith and with local filesystem storage, write the following YAML configuration to a file named `demo.yaml`:

<!-- prettier-ignore-start -->
[embedmd]:# (../../../configurations/demo.yaml)
```yaml
# Do not use this configuration in production.
# It is for demonstration purposes only.
multitenancy_enabled: false

blocks_storage:
  backend: filesystem
  bucket_store:
    sync_dir: /tmp/mimir/tsdb-sync
  filesystem:
    dir: /tmp/mimir/data/tsdb
  tsdb:
    dir: /tmp/mimir/tsdb

compactor:
  data_dir: /tmp/mimir/compactor
  sharding_ring:
    kvstore:
      store: memberlist

distributor:
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: memberlist

ingester:
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: memberlist
    replication_factor: 1

ruler_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir/rules

server:
  http_listen_port: 9009
  log_level: error

store_gateway:
  sharding_ring:
    replication_factor: 1
```
<!-- prettier-ignore-end -->

{{< admonition type="note" >}}
Grafana Mimir includes a system that optionally and anonymously reports non-sensitive, non-personally identifiable information about the running Mimir cluster to a remote statistics server to help Mimir maintainers understand how the open source community runs Mimir.

To opt out, refer to [Disable the anonymous usage statistics reporting]({{< relref "../configure/about-anonymous-usage-statistics-reporting#disable-the-anonymous-usage-statistics-reporting" >}}).
{{< /admonition >}}

## Run Grafana Mimir

In a terminal, run one of the following commands:

- Using Docker:

  ```bash
  docker network create grafanet

  docker run \
    --rm \
    --name mimir \
    --network grafanet \
    --publish 9009:9009 \
    --volume "$(pwd)"/demo.yaml:/etc/mimir/demo.yaml grafana/mimir:latest \
    --config.file=/etc/mimir/demo.yaml
  ```

- Using a local binary:

  ```bash
  ./mimir --config.file=./demo.yaml
  ```

Grafana Mimir listens on port `9009`.

## Configure Prometheus to write to Grafana Mimir

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

## Configure Grafana Alloy to write to Grafana Mimir

Use the `prometheus.remote_write` component in Grafana Alloy to send metrics to Grafana Mimir. For example:

```
prometheus.remote_write "LABEL" {
  endpoint {
    url = http://localhost:9009/api/v1/push

    ...
  }

  ...
}
```

The configuration for Alloy that scrapes itself and writes those metrics to Grafana Mimir looks similar to this:

```
prometheus.exporter.self "self_metrics" {
}

prometheus.scrape "self_scrape" {
  targets    = prometheus.exporter.self.self_metrics.targets
  forward_to = [prometheus.remote_write.mimir.receiver]
}

prometheus.remote_write "mimir" {
  endpoint {
    url = "http://localhost:9009/api/v1/push"
  }
}
```

For more information about setting up Alloy, refer to [prometheus.remote_write](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.remote_write/).

## Monitor Grafana Mimir with the integration for Grafana Cloud

Integrate with Grafana Cloud to monitor the health of your Mimir system. The self-hosted Mimir integration for Grafana Cloud includes dashboards, as well as recording and alerting rules, to help monitor the health of your cluster. This integration uses Grafana Alloy to scrape and send metrics to Mimir.

For more information, refer to [Self-hosted Grafana Mimir integration for Grafana Cloud](https://grafana.com/docs/grafana-cloud/monitor-infrastructure/integrations/integration-reference/integration-mimir).

## Query data in Grafana

In a new terminal, run a local Grafana server using Docker:

```bash
docker run --rm --name=grafana --network=grafanet -p 3000:3000 grafana/grafana
```

### Add Grafana Mimir as a Prometheus data source

1. In a browser, go to the Grafana server at [http://localhost:3000/datasources](http://localhost:3000/datasources).
1. Sign in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Grafana Mimir server using the following settings:

   | Field | Value                                                                                                                                                                           |
   | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
   | Name  | Mimir                                                                                                                                                                           |
   | URL   | [http://mimir:9009/prometheus](http://mimir:9009/prometheus) if you used Docker / [http://localhost:9009/prometheus](http://localhost:9009/prometheus) if you used local binary |

To add a data source, refer to [Add a data source](/docs/grafana/latest/administration/data-source-management/#add-a-data-source).

## Verify success

After you have completed the tasks in this _Get started_ guide, you can query metrics in [Grafana Explore](/docs/grafana/latest/explore/)
as well as create dashboard panels using your newly configured Grafana Mimir data source.
