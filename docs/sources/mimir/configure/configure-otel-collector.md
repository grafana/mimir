---
aliases:
  - ../operators-guide/configure/configure-otel-collector/
description: Learn how to write metrics from OpenTelemetry Collector into Mimir
menuTitle: OpenTelemetry Collector
title: Configure the OpenTelemetry Collector to write metrics into Mimir
weight: 150
---

# Configure the OpenTelemetry Collector to write metrics into Mimir

{{% admonition type="note" %}}
To send OTel data to Grafana Cloud, refer to [Send data using OpenTelemetry Protocol (OTLP)](https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/).
{{% /admonition %}}

When using the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), you can write metrics into Mimir using two options: `otlphttp` (preferred) and `prometheusremotewrite`.

Use each protocol's respective exporter and native Mimir endpoint. For example:

- OTel data: `otlphttp`
- Prometheus data: `prometheusremotewrite`


## OTLP

Mimir supports native OTLP over HTTP. When possible, use this protocol to send OTLP data. To configure the collector to use the OTLP interface, use the [`otlphttp`](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter) exporter:

```yaml
exporters:
  otlphttp:
    endpoint: http://<mimir-endpoint>/otlp
```

Then, enable it in `service.pipelines`:

```yaml
service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., otlphttp]
```

If you want to authenticate using basic auth, use the [`basicauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/basicauthextension) extension:

```yaml
extensions:
  basicauth/otlp:
    client_auth:
      username: username
      password: password

exporters:
  otlphttp:
    auth:
      authenticator: basicauth/otlp
    endpoint: http://<mimir-endpoint>/otlp

service:
  extensions: [basicauth/otlp]
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., otlphttp]
```

## Prometheus remote write

For remote write, use the [`prometheusremotewrite`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusremotewriteexporter) exporter in the Collector:

In the `exporters` section add:

```yaml
exporters:
  prometheusremotewrite:
    endpoint: http://<mimir-endpoint>/api/v1/push
```

Then, enable it in the `service.pipelines` block:

```yaml
service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., prometheusremotewrite]
```

If you want to authenticate using basic auth, use the [`basicauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/basicauthextension) extension:

```yaml
extensions:
  basicauth/prw:
    client_auth:
      username: username
      password: password

exporters:
  prometheusremotewrite:
    auth:
      authenticator: basicauth/prw
    endpoint: http://<mimir-endpoint>/api/v1/push

service:
  extensions: [basicauth/prw]
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., prometheusremotewrite]
```

## Format considerations

We follow the official [OTLP Metric points to Prometheus](https://opentelemetry.io/docs/reference/specification/compatibility/prometheus_and_openmetrics/#otlp-metric-points-to-prometheus) specification.

By default, Grafana Mimir does not accept [OpenTelemetry Exponential Histogram](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram) metrics. For Grafana Mimir to accept them, ingestion of Prometheus Native Histogram metrics must first be enabled following the instructions in [Configure native histogram ingestion]({{< relref "./configure-native-histograms-ingestion" >}}). After this is done, Grafana Mimir will accept OpenTelemetry Exponential Histograms, and convert them into Prometheus Native Histograms following the conventions described in the [Exponential Histograms specification](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/#exponential-histograms).

You might experience the following common issues:

- Dots (.) are converted to \_

  Prometheus metrics do not support `.` and `-` characters in metric or label names. Prometheus converts these characters to `_`.

  For example:

  `requests.duration{http.status_code=500, cloud.region=us-central1}` in OTLP

  `requests_duration{http_status_code=”500”, cloud_region=”us-central1”}` in Prometheus

- Resource attributes are added to the `target_info` metric.

  However, `<service.namespace>/<service.name>` or `<service.name>` (if the namespace is empty), is added as the label `job`, and `service.instance.id` is added as the label `instance` to every metric.

  For details, see the [OpenTelemetry Resource Attributes](https://opentelemetry.io/docs/reference/specification/compatibility/prometheus_and_openmetrics/#resource-attributes) specification.
