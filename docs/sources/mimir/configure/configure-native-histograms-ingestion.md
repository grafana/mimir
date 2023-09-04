---
description: Learn how to configure Grafana Mimir to ingest and query native histograms.
menuTitle: Native histograms
title: Configure native histograms
weight: 160
---

# Configure native histograms

Prometheus native histograms ingestion is an **experimental** feature of Grafana Mimir.

You can configure native histograms ingestion over the Prometheus [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint globally or per tenant.

> **Note:** To enable support for querying native histograms together with [Grafana Mimir query sharding]({{< relref "../references/architecture/query-sharding" >}}), make sure that the flag `-query-frontend.query-result-response-format` is set to its default `protobuf` value on query frontends.

## Configure native histograms globally

To enable ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint for all tenants, set the flag `-ingester.native-histograms-ingestion-enabled=true` on ingesters.

To limit the number of native histogram buckets per sample, set the `-validation.max-native-histogram-buckets` flag on distributors.
The recommended value is 160 which is the default in the [OpenTelemetry SDK](https://opentelemetry.io/docs/specs/otel/metrics/sdk/) for exponential histograms, which are a similar concept in OpenTelemetry.
Samples with more buckets than the limit will be dropped.

## Configure native histograms per tenant

To enable ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api#remote-write" >}}) for a tenant, set the `native_histograms_ingestion_enabled` runtime value to `true`.

To limit the number of native histogram buckets per sample for a tenant, set the `max_native_histogram_buckets` runtime value.
The recommended value is 160 which is the default in the [OpenTelemetry SDK](https://opentelemetry.io/docs/specs/otel/metrics/sdk/) for exponential histograms, which are a similar concept in OpenTelemetry.
Samples with more buckets than the limit will be dropped.

```yaml
overrides:
  tenant1:
    native_histograms_ingestion_enabled: true
    max_native_histogram_buckets: 160
```

## Configure Prometheus to write native histograms to Grafana Mimir

To enable experimental support for scraping and ingesting native histograms in Prometheus, [enable the feature](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms) with the flag `--enable-feature=native-histograms`.

To enable Prometheus remote write to send native histograms to Grafana Mimir, add the `send_native_histograms: true` parameter to your remote write configuration, for example:

```yaml
remote_write:
  - url: <your-url>
    send_native_histograms: true
```
