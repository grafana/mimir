---
title: "Configure native histograms"
menuTitle: "Native histograms"
description: "Learn how to configure Grafana Mimir to ingest and query native histograms."
weight: 160
---

# Configure native histograms

To enable support for ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint, set the flag `-ingester.native-histograms-ingestion-enabled=true` on ingesters.

To enable support for querying native histograms together with [Grafana Mimir query sharding]({{< relref "../references/architecture/query-sharding" >}}), set the flag `-query-frontend.query-result-response-format=protobuf` on query frontends.

> **Note:** Native histograms is an experimental feature of Grafana Mimir.

## Configure Prometheus to write native histograms to Grafana Mimir

To enable experimental support for scraping and ingesting native histograms in Prometheus, [enable the feature](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms) with the flag `--enable-feature=native-histograms`.

To enable Prometheus remote write to send native histograms to Grafana Mimir, add the `send_native_histograms: true` parameter to your remote write configuration, for example:

```yaml
remote_write:
  - url: <your-url>
    send_native_histograms: true
```
