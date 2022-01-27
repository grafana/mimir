---
title: "Getting started with Grafana Mimir"
linkTitle: "Getting started with Grafana Mimir"
weight: 1
no_section_index_title: true
slug: "getting-started-with-grafana-mimir"
---

# Getting started with Grafana Mimir

## Overview

Grafana Mimir runs as a single process or as multiple microservice processes.
The single process mode is easier to deploy for users wanting to try out Grafana Mimir or develop on it.
The microservices mode allows you to independently scale different services and isolate failures.

These instructions focus on deploying Grafana Mimir as a single process.
Refer to [Architecture](../architecture.md) for more information about the microservices.

This guide assumes you have already installed a Prometheus server or the Grafana Agent.
To install Prometheus, refer to [Prometheus Installation](https://prometheus.io/docs/prometheus/latest/installation/).
To install the Grafana Agent, refer to the [latest release](https://github.com/grafana/agent/releases/latest).

## Install Grafana Mimir

Pull the latest Grafana Mimir Docker image locally:

```bash
export MIMIR_LATEST=$(curl -Ls -o /dev/null -w %{url_effective} https://github.com/grafana/mimir/releases | awk -F / '{ print $NF; }')
docker pull "grafana/mimir:${MIMIR_LATEST}"
```

To install Grafana Mimir locally, download the latest release from GitHub and mark the file as executable.

```bash
curl -LO https://github.com/grafana/mimir/releases/latest/download/mimir
chmod +x mimir
```

## Start Grafana Mimir

To run Grafana Mimir in a single process and with local filesystem storage, write the following configuration YAML to a file called `dev.yaml`.

```yaml
# Configuration for running Grafana Mimir in single process mode.
# This should not be used in production.  It is only for getting started
# and development.
auth_enabled: false

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
  lifecycler:
    address: 127.0.0.1
    ring:
      kvstore:
        store: memberlist
      replication_factor: 1

ruler:
  enable_api: true
  enable_sharding: false

ruler_storage:
  backend: local
  local:
    directory: /tmp/mimir/rules

server:
  http_listen_port: 9009
  log_level: error

store_gateway:
  sharding_ring:
    replication_factor: 1
```

Using Docker, run Grafana Mimir with:

```bash
docker run --rm --name mimir --detach --publish 9009:9009 --volume "$(pwd)"/dev.yaml:/etc/mimir/dev.yaml "grafana/mimir:${MIMIR_LATEST}" --config.file=/etc/mimir/dev.yaml
```

Using a local binary, run Grafana Mimir with:

```bash
./mimir --config.file=./dev.yaml &
```

Grafana Mimir starts in the background, listening on port 9009.

## Configure Prometheus to write to Grafana Mimir

Add the following YAML snippet to your Prometheus configuration file and restart the Prometheus server.

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

The configuration for a Prometheus server that scrapes itself and writes those metrics to Grafana Mimir would look similar to:

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push

scrape_configs:
  - job_name: prometheus
    honor_labels: true
    static_configs:
      - targets: ["localhost:9090"]
```

## Configure the Grafana Agent to write to Grafana Mimir

Add the following YAML snippet to one of your Agent `metrics` `configs` in your Agent configuration file and restart the Grafana Agent.

```yaml
remote_write:
  - url: http://localhost:9009/prometheus/api/v1/push
```

The configuration for an Agent that scrapes itself for metrics and writes those metrics to Grafana Mimir would look similar to:

```yaml
server:
  http_listen_port: 12345

metrics:
  configs:
    - name: agent
      scrape_configs:
        - job_name: agent
          static_configs:
            - targets: ["127.0.0.1:12345"]
      remote_write:
        - url: http://localhost:9009/prometheus/api/v1/push
```

## Query data in Grafana

Run a local Grafana server using Docker:

```bash
docker run --rm --detach --name=grafana --network=host grafana/grafana
```

### Add Grafana Mimir as a Prometheus data source

1. Browse to the Grafana server at http://localhost:3000/datasources.
1. Log in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Grafana Mimir server using the following settings:

| Field | Value                            |
| ----- | -------------------------------- |
| Name  | Mimir                            |
| URL   | http://localhost:9009/prometheus |

For an illustrated guide to adding a data source, refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

## Success

You are now able to query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/)
and create dashboard panels using the newly configured Grafana Mimir data source.
