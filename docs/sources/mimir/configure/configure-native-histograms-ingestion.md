---
description: Learn how to configure Grafana Mimir to ingest and query native histograms.
menuTitle: Native histograms
title: Configure native histograms
weight: 160
---

# Configure native histograms

Prometheus native histograms ingestion is an **experimental** feature of Grafana Mimir.

You can configure native histograms ingestion via the Prometheus [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint globally or per tenant.

{{% admonition type="note" %}}
To enable support for querying native histograms together with [Grafana Mimir query sharding]({{< relref "../references/architecture/query-sharding" >}}), make sure that the flag `-query-frontend.query-result-response-format` is set to its default `protobuf` value on query frontends.
{{% /admonition %}}

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

To learn more about sending native histograms to Mimir or Grafana Cloud Metrics via Grafana Agent or Prometheus,
see [Scrape and send native histograms with Grafana Agent]({{< relref "../send/native-histograms#scrape-and-send-native-histograms-with-grafana-agent" >}}) or
[Scrape and send native histograms with Prometheus]({{< relref "../send/native-histograms#scrape-and-send-native-histograms-with-prometheus" >}}).
