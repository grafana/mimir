---
description: Learn how to collect and send exponential histograms with the OpenTelemetry Collector
keywords:
  - send metrics
  - exponential histogram
  - OpenTelemetry
  - instrumentation
menuTitle: OpenTelemetry exponential histograms
title: Send exponential histograms to Mimir
weight: 200
---

# Send exponential histograms to Mimir

You can collect and send exponential histograms to Mimir with the OpenTelemetry Collector. OpenTelemetry [exponential histograms](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram) are compatible with Prometheus native histograms. The key difference is that exponential histograms store the `min` and `max`  observation values explicitly, whereas native histograms don't. This means that for exponential histograms, you don't need to estimate these values using the 0.0 and 1.0 quantiles.

The OpenTelemetry Collector supports collecting exponential histograms and other compatible data formats, such as native histograms and DataDog sketches, through its receivers and sending them through its exporters.

{{< admonition type="note" >}}
The availability of different receivers and exporters depends on your Collector [distribution](https://opentelemetry.io/docs/concepts/distributions/).
{{< /admonition >}}

You can use the OpenTelemetry (OTLP) protocol to send exponential histograms to Grafana Mimir in their existing format, or you can use the Prometheus remote write protocol to send them as Prometheus native histograms.

The OpenTelemetry SDK supports instrumenting applications in multiple languages. Refer to [Language APIs & SDKs](https://opentelemetry.io/docs/languages/). 

## Instrument an application with the OpenTelemetry SDK using Go

Use the OpenTelemetry SDK version 1.17.0 or later.

1. Set up the OpenTelemetry Collector to handle your metrics data. This includes setting up your resources, meter provider, meter, instruments, and views. Refer to [Metrics](https://opentelemetry.io/docs/languages/go/instrumentation/#metrics) in the OpenTelemetry SDK documentation for Go.
1. To aggregate a histogram instrument as an exponential histogram, include the following view:

   ```
   Aggregation: metric.AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
		}
   ```

   For more information, refer to [Registering Views](https://opentelemetry.io/docs/languages/go/instrumentation/#registering-views) in the OpenTelemetry SDK documentation for Go.

## Migrate from classic histograms

## Bucket boundary calculation