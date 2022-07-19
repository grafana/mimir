---
aliases:
  - /docs/mimir/latest/operators-guide/configuring/configuring-otel-collector/
description: Learn how to write data from OpenTelemetry Collector into Mimir
menuTitle: Configuring OTel Collector
title: Configuring OpenTelemetry Collector to write data into Mimir
weight: 150
---

# Configuring OpenTelemetry Collector to write data into Mimir

When using the OpenTelemetry Collector, you can write metrics data into Mimir via two options: `remote_write` and `otlphttp`.

We recommend using the `remote_write` exporter when possible because the remote_write ingest path is tested and proven at scale.

## Remote Write

For the Remote Write, use the [`prometheusremotewrite`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusremotewriteexporter) exporter in the Collector:

In the `exporters` section add:

```yaml
exporters:
  prometheusremotewrite:
    endpoint: http://<mimir-endpoint>/api/prom/push
```

And use the same in the `service.pipelines`:

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
    endpoint: http://<mimir-endpoint>/api/prom/push

service:
  extensions: [basicauth/prw]
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., prometheusremotewrite]
```

## OTLP

We support a native OTLP over HTTP on Mimir. To configure the collector to use the interface, we use the [`otlphttp`](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter) exporter:

```yaml
exporters:
  otlphttp:
    endpoint: http://<mimir-endpoint>/otlp
```

And use the same in the `service.pipelines`:

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
  prometheusremotewrite:
    auth:
      authenticator: basicauth/otlp
    endpoint: http://<mimir-endpoint>/api/prom/push

service:
  extensions: [basicauth/otlp]
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [..., otlphttp]
```
