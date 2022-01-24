---
title: "Getting started with Mimir"
linkTitle: "Getting started with Mimir"
weight: 1
no_section_index_title: true
slug: "getting-started-with-mimir"
---

# Getting started with Mimir

## Overview

Mimir runs as a single process or as multiple microservice processes.
The single process mode is easier to deploy for users wanting to try out Mimir or develop on it.
The microservices mode allows you to independently scale different services and isolate failures.

These instructions focus on deploying Mimir as a single process.
Refer to [Architecture](../architecture.md) for more information about the microservices.

## Install Mimir

To install Mimir, download the latest release from GitHub and mark the file as executable.

```console
$ curl -LO https://github.com/grafana/mimir/releases/latest/download/mimir
$ chmod +x mimir
```

Alternatively, to build Mimir from source, clone the repository and build it with Go.

```console
$ git clone https://github.com/grafana/mimir.git
$ cd mimir
$ go build ./cmd/mimir
```

### Verify the installation

To verify the downloaded binary version, you can run Mimir with the `--version` flag:

```console
$ ./mimir --version
Mimir, version  (branch: , revision: )
  build user:
  build date:
  go version:       go1.17.6
  platform:         linux/amd64
```

## Start Mimir

To run Mimir in a single process and with local filesystem storage, use the [`dev.yaml`](./configuration/dev.yaml) configuration file:

```console
$ curl -LO https://raw.githubusercontent.com/grafana/mimir/main/docs/configuration/dev.yaml
$ ./mimir --config.file=./dev.yaml &
```

Mimir starts in the background, listening on port 9009.

## Configure Prometheus to write to Mimir

If you don't already have Prometheus installed, refer to [Prometheus Installation](https://prometheus.io/docs/prometheus/latest/installation/).

Add the following YAML snippet to your Prometheus configuration file and restart the Prometheus server.

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

The configuration for a Prometheus server that scrapes itself and writes those metrics to Mimir would look similar to:

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push

scrape_configs:
  - job_name: prometheus
    honor_labels: true
    static_configs:
      - targets: ["localhost:9090"]
```

## Configure the Grafana Agent to write to Mimir

If you don't already have the Grafana Agent installed, refer to the [latest release](https://github.com/grafana/agent/releases/latest).

Add the following YAML snippet to one of your Agent `metrics` `configs` in your Agent configuration file and restart the Grafana Agent.

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

The configuration for an Agent that scrapes itself for metrics and writes those metrics to Mimir would look similar to:

```yaml
server:
  http_listen_port: 12345

metrics:
  configs:
    - name: agent
      scrape_configs:
        - job_name: agent
          static_configs:
            - targets: ['127.0.0.1:12345']
      remote_write:
        - url: http://localhost:9009/api/v1/push
```

## Query data in Grafana

Run a local Grafana server using Docker:

```console
$ docker run --rm -d --name=grafana -p 3000:3000 grafana/grafana
```

### Add Mimir as a Prometheus data source:

1. Browse to the Grafana server at http://localhost:3000/datasources.
1. Log in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Mimir server using the following settings:
   Name: Mimir
   URL: http://localhost:9009/api/v1/query
