---
description: Learn how to collect and send exponential histograms with the OpenTelemetry Collector
menuTitle: OpenTelemetry exponential histograms
title: Collect and send exponential histograms with the OpenTelemetry Collector
weight: 100
---

# Collect and send exponential histograms with the OpenTelemetry Collector

OpenTelemetry [exponential histograms](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram) are compatible with Prometheus native histograms. The only significant difference is that exponential histograms store the `min` and `max`  observation values explicitly, but native histograms don't, which means you need to estimate the minimum and maximum by using the 0.0 and 1.0 quantile respectively. 

Use the OpenTelemetry Collector to collect and send exponential histograms. The Collector supports collecting exponential histograms or other compatible data formats (for example native histograms, DataDog sketches, etc) through its receivers. The Collector also supports sending the exponential histograms or compatible data formats such as native histograms through its exporters.

Note: the availability of different receivers and exporters depends on your Collector [distribution](https://opentelemetry.io/docs/concepts/distributions/).

Grafana Mimir can receive exponential histograms as is via the OTLP protocol or as Prometheus native histograms over the Prometheus remote write protocol.

OpenTelemetry supports instrumenting applications with the OpenTelemetry SDK. Multiple languages are supported, for the complete list see [Language APIs & SDKs](https://opentelemetry.io/docs/languages/).

## Instrument a Go application with the OpenTelemetry SDK

Use the OpenTelemetry SDK version 1.17.0 or later for Go.

1. Set up the collector to handle your metrics data. This includes setting up your resources, meter provider, meter, instruments, and views. Refer to [Metrics](https://opentelemetry.io/docs/languages/go/instrumentation/#metrics) in the OpenTelemetry SDK documentation for Go.
1. To aggregate a histogram instrument as an exponential histogram, include the following view:

   ```
   Aggregation: metric.AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
		}
   ```

   For more information, refer to [Registering Views](https://opentelemetry.io/docs/languages/go/instrumentation/#registering-views) in the OpenTelemetry SDK documentation for Go.

## Migrate from classic histograms
