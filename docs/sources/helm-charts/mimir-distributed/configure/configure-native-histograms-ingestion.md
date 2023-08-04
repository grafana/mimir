---
title: "Configure native histograms"
menuTitle: "Native histograms"
description: "Learn how to configure Grafana Mimir to ingest and query native histograms."
---

# Configure native histograms

To enable support for ingesting Prometheus native histograms over the [remote write API](/docs/mimir/{{< param "mimir_docs_version" >}}/references/http-api/#remote-write) endpoint, set the configuration parameter `native_histograms_ingestion_enabled` to true.

To enable support for querying native histograms together with [Grafana Mimir query sharding](/docs/mimir/{{< param "mimir_docs_version" >}}/references/architecture/query-sharding/), set the configuration parameter `query_result_response_format` to `protobuf`.

Example values file:

```yaml
mimir:
  structuredConfig:
    frontend:
      query_result_response_format: protobuf
    limits:
      native_histograms_ingestion_enabled: true
```

> **Note:** Native histograms is an experimental feature of Grafana Mimir.

## Configure Prometheus to write native histograms to Grafana Mimir

To enable experimental support for scraping and ingesting native histograms in Prometheus, [enable the feature](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms) with the flag `--enable-feature=native-histograms`.

To enable Prometheus remote write to send native histograms to Grafana Mimir, add the `send_native_histograms: true` parameter to your remote write configuration, for example:

```yaml
remote_write:
  - url: <your-url>
    send_native_histograms: true
```
