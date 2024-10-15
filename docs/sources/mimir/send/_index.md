---
description: Learn how to send metric data to Grafana Mimir.
keywords:
  - send metrics
menuTitle: Send
title: Send metric data to Mimir
weight: 32
---

# Send metric data to Mimir

To send metric data to Mimir:

1. Configure your data source to write to Mimir:
   - If you are using Prometheus, see [Configure Prometheus to write to Mimir]({{< relref "../get-started#configure-prometheus-to-write-to-grafana-mimir" >}}).
   - If you are using the OpenTelemetry Collector, see [Configure the OpenTelemetry Collector to write metrics into Mimir]({{< relref "../configure/configure-otel-collector" >}})
1. [Configure Grafana Alloy to write to Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/#configure-grafana-alloy-to-write-to-grafana-mimir).
1. Upload Prometheus TSDB blocks to Grafana Mimir by using the `backfill` command; see [Backfill]({{< relref "../manage/tools/mimirtool#backfill" >}}).
