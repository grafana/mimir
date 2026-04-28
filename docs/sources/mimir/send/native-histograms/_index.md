---
description: Learn how to send metric data to Grafana Mimir.
keywords:
  - send metrics
  - native histogram
  - prometheus
  - grafana alloy
  - instrumentation
menuTitle: Native histograms
title: Send native histograms to Mimir
weight: 100
---

# Send native histograms to Mimir

Prometheus [native histograms](https://prometheus.io/docs/specs/native_histograms/) are a data type in the Prometheus ecosystem that enable the following use cases:

1. Produce, store, and query a high-resolution histogram of observations. Use native histograms with exponential buckets in your instrumentation for this purpose.
1. Store classic histograms in a more resilient and cost effective way, without changes to instrumentation. Use native histograms with custom buckets for this purpose.

The following topics cover each use case:

{{< section menuTitle="true" >}}
