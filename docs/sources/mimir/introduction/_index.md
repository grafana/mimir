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

## About metrics

Metrics provide a high-level picture of the state of a system. Metrics are the foundation of alerts because metrics are numeric values and can be compared against known thresholds. Alerts constantly run in the background and trigger when a value is outside of an expected range. This is typically the first sign that something is going on and is where discovery first starts. Metrics indicate that something is happening.

Metrics tell you how much of something exists, such as how much memory a computer system has available or how many centimeters long a desktop is. In the case of Grafana, metrics are most useful when they are recorded repeatedly over time. This permits us to compare things like how running a program affects the availability of system resources, as shown in the following dashboard.

![Sample visualization dashboard](/media/metrics-explore/visualization_sample.png)

Metrics like these are stored in a time series database (TSDB), like [Prometheus](https://prometheus.io/), by recording a metric and pairing that entry with a time stamp. Each TSDB uses a slightly different [data model](https://prometheus.io/docs/concepts/data_model/), but all combine these two aspects and Grafana and Grafana Cloud can accept their different metrics formats for visualization.

For example, you might be interested in comparing system I/O performance as the number of users increases during a morning while many users in a company come online to start their work days.

A chart showing this change of resource use across time is an example of a visualization. Comparing these time-stamped metrics over time using visualizations makes it quick and easy to see changes to a computer system, especially as events occur.

Grafana and Grafana Cloud offer a variety of visualizations to suit different use cases. See the Grafana documentation on [visualizations](https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/) for more information.

## Get started with Grafana Mimir

To get started with Grafana Mimir, refer to [Get started with Grafana Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/).
