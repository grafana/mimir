---
aliases:
  - ../../../operators-guide/monitor-grafana-mimir/dashboards/writes-resources/
  - ../../../operators-guide/monitoring-grafana-mimir/dashboards/writes-resources/
  - ../../../operators-guide/visualizing-metrics/dashboards/writes-resources/
description: View an example Writes resources dashboard.
menuTitle: Writes resources
title: Grafana Mimir Writes resources dashboard
weight: 200
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Grafana Mimir Writes resources dashboard

The Writes resources dashboard shows CPU, memory, disk, and other resource utilization metrics.
The dashboard isolates each service on the write path into its own section and displays the order in which a write request flows.

This dashboard requires [additional resources metrics](../../requirements/#additional-resources-metrics).

Use this dashboard for the following use cases:

- Monitor the resource utilization of each component involved in the write path of a Mimir cluster.
- Identify which component is experiencing capacity issues.

## Example

The following example shows a Writes resources dashboard from a demo cluster.

![Grafana Mimir writes resources dashboard](mimir-writes-resources.png)
