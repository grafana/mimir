---
description: An introduction to Grafana Mimir.
menuTitle: Introduction
title: Introduction to Grafana Mimir
weight: 5
---

# Introduction to Grafana Mimir

Grafana Mimir is an open source software project that provides long-term storage for Prometheus and OpenTelemetry metrics. It allows you to extend Prometheus' capabilities through providing a robust solution for storing and querying metric data at scale.

## Key features of Grafana Mimir

With Grafana Mimir, you can:

- Handle large volumes of time series data through horizontally scaling Grafana Mimir across multiple machines.
- Replicate incoming metrics to make Grafana Mimir highly available and prevent data loss in the event of a failure or outage.
- Isolate data and queries from independent teams in the same Grafana Mimir cluster with multi-tenancy support.
- Store data long-term with Grafana Mimir object storage capabilities.
- Use Prometheus remote write, PromQL, and alerting, as Grafana Mimir is fully compatible with Prometheus.

## Learn about metric data

To learn more about metric data, refer to [About metrics and telemetry](https://grafana.com/docs/mimir/<MIMIR_VERSION>/introduction/about-metrics/).

## Learn about Grafana Mimir architecture

## Get started with Grafana Mimir

To get started running Grafana Mimir, refer to [Get started with Grafana Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/).
