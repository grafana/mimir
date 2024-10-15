---
title: "Configure native histograms"
menuTitle: "Native histograms"
description: "Learn how to configure Grafana Mimir to ingest and query native histograms."
---

# Configure native histograms

To enable support for ingesting Prometheus native histograms over the [remote write API](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/http-api/#remote-write) endpoint, set the configuration parameter `native_histograms_ingestion_enabled` to true.

To enable support for querying native histograms together with [Grafana Mimir query sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/query-sharding/), set the configuration parameter `query_result_response_format` to `protobuf`.

Example values file:

```yaml
mimir:
  structuredConfig:
    frontend:
      query_result_response_format: protobuf
    limits:
      native_histograms_ingestion_enabled: true
```

{{% admonition type="note" %}}
Native histograms is an experimental feature of Grafana Mimir.
{{% /admonition %}}

To configure bucket limits for native histograms, refer to [Configure native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-native-histograms-ingestion/).

To configure Grafana Agent or Prometheus to write native histograms to Grafana Mimir, refer to [Send native histograms to Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/).

To visualize native histograms in Mimir, refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/).
