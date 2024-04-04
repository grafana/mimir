
---
description: Learn about the key benefits and features of Grafana Mimir.
labels:
  products:
    - OSS
menuTitle: Grafana Mimir
title: Grafana Mimir
weight: 1
hero:
  title: Grafana Mimir
  level: 1
  image: /media/docs/mimir/GrafanaLogo_Mimir_icon.png
  width: 100
  height: 100
  description: Grafana Mimir is an open source software that provides horizontally scalable, highly available, multi-tenant, long-term storage for Prometheus.
cards:
  title_class: pt-0 lh-1
  items:
    - title: Get started
      href: ./get-started/
      description: Get started with Grafana Mimir
      height: 24
    - title: Configurations
      href: ./configure/about-configurations/
      description: Learn about configuration options for Grafana Mimir
      height: 24
    - title: Send metric data
      href: ./send/
      description: Configure your data source to write data to Grafana Mimir
      height: 24
    - title: Manage
      href: ./manage/
      description: Explore your options when deploying, configuring and managing Mimir
    - title: Query metric labels
      href: ./query/
      description: Query metric labels with HTTP API endpoints.
      height: 24
    - title: Visualize data
      href: ./visualize/
      description: Use Grafana Explore and PromQL to visualize data in Grafana
      height: 24
---

{{< docs/hero-simple key="hero" >}}

---

## Overview

Experience the capabilities of Grafana Mimir for scalable, cost-effective, and reliable long-term storage of your Prometheus metrics.
With easy installation and global visibility bundled with dashboards, alerts, and runbooks, Mimir simplifies system monitoring.
Its horizontally scalable architecture handles up to 1 billion active time series, while its query engine parallelizes execution for quick processing of aggregated metrics from multiple Prometheus instances.

Utilize data and query isolation among independent teams with Mimir's native multi-tenancy architecture.
Take advantage of affordability and durability with support for various object storage implementations, including AWS S3, Google Cloud Storage, and Azure Blob Storage.

## Explore

{{< card-grid key="cards" type="simple" >}}

