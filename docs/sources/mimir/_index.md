---
description: A guide to different sections of the documentation for Grafana Mimir.
labels:
  products:
    - OSS
keywords:
  - Grafana Mimir
  - Grafana metrics
  - time series database
  - TSDB
  - Prometheus storage
  - Prometheus remote write
  - metrics storage
  - metrics datastore
  - observability
cascade:
  ALLOY_VERSION: "latest"
menuTitle: Grafana Mimir
title: Grafana Mimir documentation
hero:
  title: Grafana Mimir
  level: 1
  image: /media/docs/mimir/GrafanaLogo_Mimir_icon.png
  width: 100
  height: 100
  description: Grafana Mimir is an open source software project that provides horizontally scalable, highly available, multi-tenant, long-term storage for Prometheus and OpenTelemetry metrics.
cards:
  title_class: pt-0 lh-1s
  items:
    - title: Get started
      href: ./get-started/
      description: Get started quickly with Grafana Mimir's extensive documentation, tutorials, and deployment tooling. Use the monolithic mode to get up and running with just one binary and no added dependencies.
      height: 24
    - title: Set up and configure
      href: ./set-up/
      description: Set up Grafana Mimir with Helm, Puppet, or Jsonnet and Tanka. Then, Configure Grafana Mimir through a YAML-based configuration file or CLI flags.
      height: 24
    - title: Send metric data
      href: ./send/
      description: Configure your data sources to write data to Grafana Mimir. These include such sources as Prometheus, the OpenTelemetry Collector, and Grafana Agent.
      height: 24
    - title: Manage
      href: ./manage/
      description: Whether you're an operator or user, you have some decisions to make and actions to take. Read about exemplars, tools, runbooks, and more to help you take the right decisions and actions for your operation.
    - title: Query metric labels
      href: ./query/
      description: Query metric data from Grafana Mimir through the use of Grafana or the Grafana Mimir HTTP API. Learn how to query Prometheus data from within Mimir.
      height: 24
    - title: Visualize data
      href: ./visualize/
      description: Query, visualize, and explore your metrics using Grafana, an open platform for metrics visualization. It supports multiple data stores including Prometheus. You can also visualize native histograms through Grafana Mimir since they are a Prometheus data type.
      height: 24
---

{{< docs/hero-simple key="hero" >}}

---

## Overview

Grafana Mimir enables users to ingest Prometheus or OpenTelemetry metrics, run queries, create new data through the use of recording rules, and set up alerting rules across multiple tenants to leverage tenant federation. Once deployed, the best-practice dashboards, alerts, and runbooks packaged with Grafana Mimir make it easy to monitor the health of the system.

## Explore

{{< card-grid key="cards" type="simple" >}}
