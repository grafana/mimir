---
title: "Grafana Mimir Writes resources dashboard"
menuTitle: "Writes resources"
description: "View an example Writes resources dashboard."
weight: 200
aliases:
  - ../../visualizing-metrics/dashboards/writes-resources/
---

# Grafana Mimir Writes resources dashboard

The Writes resources dashboard shows CPU, memory, disk, and other resource utilization metrics.
The dashboard isolates each service on the write path into its own section and displays the order in which a write request flows.

This dashboard requires [additional resources metrics]({{< relref "../../requirements.md#additional-resources-metrics" >}}).

## Example

The following example shows a Writes resources dashboard from a demo cluster.

![Grafana Mimir writes resources dashboard](mimir-writes-resources.png)
