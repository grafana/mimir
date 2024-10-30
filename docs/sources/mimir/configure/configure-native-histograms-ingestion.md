---
description: Learn how to configure Grafana Mimir to ingest and query native histograms.
menuTitle: Native histograms
title: Configure native histograms
weight: 160
---

# Configure native histograms

Native histograms are experimental and subject to change.

You can configure native histograms ingestion via the Prometheus [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint globally or per tenant.

{{% admonition type="note" %}}
To enable support for querying native histograms together with [Grafana Mimir query sharding]({{< relref "../references/architecture/query-sharding" >}}), make sure that the flag `-query-frontend.query-result-response-format` is set to its default `protobuf` value on query frontends.
{{% /admonition %}}

## Configure native histograms globally

To enable ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api#remote-write" >}}) endpoint for all tenants, set the flag `-ingester.native-histograms-ingestion-enabled=true` on the ingesters.

To limit the number of native histogram buckets per sample, set the `-validation.max-native-histogram-buckets` flag on the distributors.
The recommended value is `160` which is the default in the [OpenTelemetry SDK](https://opentelemetry.io/docs/specs/otel/metrics/sdk/) for exponential histograms.
Exponential histograms in OpenTelemetry are a similar concept to Prometheus native histograms.
At the time of ingestion, samples with more buckets than the limit will be scaled down, meaning that the resolution will be reduced and buckets will be merged until either the number of buckets is under the limit or the minimal resolution is reached. The behavior can be changed to dropping such samples by setting the `-validation.reduce-native-histogram-over-max-buckets` option to `false`.

## Configure native histograms per tenant

To enable ingesting Prometheus native histograms over the [remote write API]({{< relref "../references/http-api#remote-write" >}}) for a tenant, set the `native_histograms_ingestion_enabled` runtime value to `true`.

To limit the number of native histogram buckets per sample for a tenant, set the `max_native_histogram_buckets` runtime value.
The recommended value is `160` which is the default in the [OpenTelemetry SDK](https://opentelemetry.io/docs/specs/otel/metrics/sdk/) for exponential histograms.
Exponential histograms in OpenTelemetry are a similar concept to Prometheus native histograms.
At the time of ingestion, samples with more buckets than the limit will be scaled down, meaning that the resolution will be reduced and buckets will be merged until either the number of buckets is under the limit or the minimal resolution is reached. The behavior can be changed to dropping such samples by setting the `-validation.reduce-native-histogram-over-max-buckets` option to `false`.

```yaml
overrides:
  tenant1:
    native_histograms_ingestion_enabled: true
    max_native_histogram_buckets: 160
```

To learn more about sending native histograms to Mimir or Grafana Cloud Metrics via Grafana Alloy or Prometheus,
see [Scrape and send native histograms with Grafana Alloy](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/#scrape-and-send-native-histograms-with-grafana-alloy) or
[Scrape and send native histograms with Prometheus]({{< relref "../send/native-histograms#scrape-and-send-native-histograms-with-prometheus" >}}).
