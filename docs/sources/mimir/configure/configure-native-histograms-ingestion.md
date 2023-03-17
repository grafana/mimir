---
title: "Configure native histograms"
menuTitle: "Native histograms"
description: "Learn how to configure Grafana Mimir to ingest and query native histograms."
weight: 160
---

# Configure native histograms

To enable support for ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api/#remote-write">}}) endpoint, set the flag `-ingester.native-histograms-ingestion-enabled=true` on ingesters.

To enable support for querying native histograms together with [Grafana Mimir query sharding]({{< relref "../operators-guide/architecture/query-sharding">}}), set the flag `-query-frontend.query-result-response-format=protobuf` on query frontends.

**Note:** native histograms is an experimental feature of Grafana Mimir and of [Prometheus](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms).
