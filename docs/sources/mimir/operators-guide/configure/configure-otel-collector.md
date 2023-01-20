---
description: Learn how to write metrics from OpenTelemetry Collector into Mimir
menuTitle: Configure OTel Collector
title: Configure the OpenTelemetry Collector to write metrics into Mimir
weight: 150
---

# Configure the OpenTelemetry Collector to write metrics into Mimir

When using the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), you can write metrics into Mimir via two options: `prometheusremotewrite` and `otlphttp`.

We recommend using the `prometheusremotewrite` exporter when possible because the remote write ingest path is tested and proven at scale.

## Remote Write

For the Remote Write, use the [`prometheusremotewrite`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusremotewriteexporter) exporter in the Collector:

In the `exporters` section add:

```yaml
exporters:
  prometheusremotewrite:
    endpoint: http://<mimir-endpoint>/api/v1/push
```

And enable it in the `service.pipelines`:

```yaml
service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., prometheusremotewrite]
```

If you want to authenticate using basic auth, we recommend the [`basicauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/basicauthextension) extension:

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

## OTLP

Mimir supports native OTLP over HTTP. To configure the collector to use the OTLP interface, you use the [`otlphttp`](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter) exporter:

```yaml
exporters:
  otlphttp:
    endpoint: http://<mimir-endpoint>/otlp
```

And enable it in `service.pipelines`:

```yaml
service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., otlphttp]
```

If you want to authenticate using basic auth, we recommend the [`basicauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/basicauthextension) extension:

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

## Format considerations

We follow the official [OTLP Metric points to Prometheus](https://opentelemetry.io/docs/reference/specification/compatibility/prometheus_and_openmetrics/#otlp-metric-points-to-prometheus) specification.

You might experience the following common issues:

- Dots (.) are converted to \_

  Prometheus metrics do not support `.` and `-` characters in metric or label names. Prometheus converts these characters to `_`.

  For example:

  `requests.duration{http.status_code=500, cloud.region=us-central1}` in OTLP

  `requests_duration{http_status_code=”500”, cloud_region=”us-central1”}` in Prometheus

- Resource attributes are added to the `target_info` metric.

  However, `<service.namespace>/<service.name>` or `<service.name>` (if the namespace is empty), is added as the label `job`, and `service.instance.id` is added as the label `instance` to every metric.

  For details, see the [OpenTelemetry Resource Attributes](https://opentelemetry.io/docs/reference/specification/compatibility/prometheus_and_openmetrics/#resource-attributes) specification.
