---
aliases:
  - ../configuring/configuring-tracing/
  - configuring-tracing/
  - ../operators-guide/configure/configure-tracing/
description: Learn how to configure Grafana Mimir to send traces using OpenTelemetry or Jaeger.
menuTitle: Tracing
title: Configure Grafana Mimir tracing
weight: 270
---

# Configure Grafana Mimir tracing

Distributed tracing is a valuable tool for troubleshooting the behavior of Grafana Mimir in production.

Grafana Mimir supports distributed tracing using [OpenTelemetry](https://opentelemetry.io/docs/concepts/signals/traces/) and provides backward compatibility with [Jaeger](https://www.jaegertracing.io/) configuration.

Mimir automatically detects which tracing configuration method to use based on the environment variables you set:

- **OpenTelemetry format**: Uses standard OTel environment variables for modern tracing backends.
- **Jaeger format**: Uses Jaeger-specific environment variables for backward compatibility.

## Dependencies

Set up a tracing backend to collect and store traces from Grafana Mimir:

- **For OpenTelemetry**: Use any OTLP-compatible tracing backend such as Jaeger, Tempo, or cloud-based solutions.
- **For Jaeger**: Set up a Jaeger deployment including either the Jaeger all-in-one binary or a distributed system of agents, collectors, and queriers.

If you run Grafana Mimir on Kubernetes, refer to [Jaeger Kubernetes](https://github.com/jaegertracing/jaeger-kubernetes).

## Configuration

Grafana Mimir automatically detects the tracing configuration method based on the environment variables you set. You can use either OpenTelemetry standard environment variables or Jaeger-specific environment variables.

### OpenTelemetry Configuration (Recommended)

To configure Grafana Mimir with OpenTelemetry format tracing, set any of the following environment variables.

OpenTelemetry configuration follows the standard documentation from [OpenTelemetry SDK Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/general/).

- `OTEL_EXPORTER_OTLP_ENDPOINT`: The OTLP endpoint URL. For example, `http://otlp-endpoint:4318`.
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`: The OTLP traces-specific endpoint URL. This value overrides the previous variable. For example, `http://tempo:4318/v1/traces`.
- `OTEL_TRACES_EXPORTER`: The traces exporter to use. Default: `otlp`.
- `OTEL_TRACES_SAMPLER`: The sampling strategy to use.

#### Traces exporter configuration

The `OTEL_TRACES_EXPORTER` environment variable specifies which trace exporter to use:

- `otlp` (default): OpenTelemetry Protocol exporter.
- `none`: No trace exporter. You can set this value if you want to configure the tracing library to propagate trace context without exporting traces.

{{< admonition type="note" >}}
The `jaeger` exporter option is not available, as it was deprecated by the OpenTelemetry project in 2023. Instead, use the `otlp` exporter, since Jaeger supports OTLP ingestion natively.
{{< /admonition >}}

#### Sampling configuration

Configure sampling behavior using the `OTEL_TRACES_SAMPLER` environment variable:

- `always_on`: Sample all traces
- `always_off`: Sample no traces
- `traceidratio`: Sample traces based on trace ID ratio
- `parentbased_always_on` (default): Sample based on parent span decision. Always sample root spans.
- `parentbased_always_off`: Sample based on parent span decision. Never sample root spans.
- `parentbased_traceidratio`: Sample based on parent span decision, use ratio for root spans
- `jaeger_remote`: Use Jaeger remote sampling
- `parentbased_jaeger_remote`: Use parent-based Jaeger remote sampling

For `jaeger_remote` and `parentbased_jaeger_remote` samplers, you must also set `OTEL_TRACES_SAMPLER_ARG` with the configuration in the format:

```
endpoint=http://localhost:14250,pollingIntervalMs=5000,initialSamplingRate=0.25
```

#### Propagation configuration

Configure trace context propagation using `OTEL_PROPAGATORS`:

- `tracecontext`: W3C Trace Context (default)
- `baggage`: W3C Baggage (default)
- `jaeger`: Jaeger propagation (default)
- `none`: No propagation

Default propagators include `tracecontext`, `baggage`, and `jaeger`.

#### Example OpenTelemetry configuration

```bash
# Basic OTLP configuration
export OTEL_EXPORTER_OTLP_ENDPOINT="http://jaeger:4318"
export OTEL_TRACES_SAMPLER="always_on"

# Advanced configuration with Jaeger remote sampling
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://jaeger:4318/v1/traces"
export OTEL_TRACES_SAMPLER="jaeger_remote"
export OTEL_TRACES_SAMPLER_ARG="endpoint=http://jaeger:14250,pollingIntervalMs=5000,initialSamplingRate=0.25"
export OTEL_PROPAGATORS="tracecontext,baggage,jaeger"
```

### Jaeger configuration (deprecated)

{{< admonition type="caution" >}}
Jaeger tracing configuration using `JAEGER_*` environment variables is deprecated since Mimir 2.17. Use OpenTelemetry configuration instead, as Jaeger supports OTLP ingestion natively.
{{< /admonition >}}

To configure Grafana Mimir with Jaeger format tracing, set any of the following environment variables:

- `JAEGER_AGENT_HOST`: Jaeger agent hostname
- `JAEGER_ENDPOINT`: Jaeger collector endpoint URL
- `JAEGER_SAMPLER_MANAGER_HOST_PORT`: Jaeger sampler manager endpoint

#### Required environment variables

You must specify at least one of the following environment variables to enable Jaeger configuration:

- `JAEGER_AGENT_HOST`
- `JAEGER_ENDPOINT`
- `JAEGER_SAMPLER_MANAGER_HOST_PORT`

#### Sampling configuration

Configure sampling using Jaeger-specific environment variables:

- `JAEGER_SAMPLER_TYPE`: Sampling strategy (`const`, `probabilistic`, `remote`)
- `JAEGER_SAMPLER_PARAM`: Sampling parameter value
- `JAEGER_SAMPLER_MANAGER_HOST_PORT`: Remote sampling endpoint
- `JAEGER_SAMPLING_ENDPOINT`: Alternative remote sampling endpoint

#### Additional Jaeger environment variables

- `JAEGER_AGENT_PORT`: Jaeger agent port (default: `6831`)
- `JAEGER_TAGS`: Additional tags to add to spans (format: `key1=value1,key2=value2`)

#### Example Jaeger configuration

```bash
# Basic agent configuration
export JAEGER_AGENT_HOST="jaeger-agent"
export JAEGER_SAMPLER_TYPE="const"
export JAEGER_SAMPLER_PARAM="1"

# Collector endpoint configuration
export JAEGER_ENDPOINT="http://jaeger-collector:14268/api/traces"
export JAEGER_SAMPLER_TYPE="probabilistic"
export JAEGER_SAMPLER_PARAM="0.1"

# Remote sampling configuration
export JAEGER_AGENT_HOST="jaeger-agent"
export JAEGER_SAMPLER_MANAGER_HOST_PORT="jaeger-agent:5778"
```

For a complete list of Jaeger environment variables, refer to the [Jaeger Client Go documentation](https://github.com/jaegertracing/jaeger-client-go#environment-variables).

### Enabling sampling

Enable sampling in the appropriate components:

- The ingester and ruler self-initiate traces. You should explicitly enable sampling for these components.
- You can enable sampling for the distributor and query-frontend in Grafana Mimir or in an upstream service, like a proxy or gateway running in front of Grafana Mimir

{{< admonition type="note" >}}
Mimir automatically detects the configuration method based on the environment variables present. OpenTelemetry configuration takes precedence if both OTel and Jaeger environment variables are set.
{{< /admonition >}}
