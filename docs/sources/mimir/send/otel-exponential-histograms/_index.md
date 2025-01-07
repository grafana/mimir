---
description: Learn how to collect and send exponential histograms with the OpenTelemetry Collector
keywords:
  - send metrics
  - exponential histogram
  - OpenTelemetry
  - instrumentation
menuTitle: OpenTelemetry exponential histograms
title: Send OpenTelemetry exponential histograms to Mimir
weight: 200
---

# Send OpenTelemetry exponential histograms to Mimir

{{% admonition type="note" %}}
Sending OpenTelemetry exponential histograms is an experimental feature of Grafana Mimir.
{{% /admonition %}}

You can collect and send exponential histograms to Mimir with the OpenTelemetry Collector. OpenTelemetry [exponential histograms](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram) are compatible with Prometheus native histograms. The key difference is that exponential histograms store the `min` and `max` observation values explicitly, whereas native histograms don't. This means that for exponential histograms, you don't need to estimate these values using the 0.0 and 1.0 quantiles.

The OpenTelemetry Collector supports collecting exponential histograms and other compatible data formats, including native histograms and Datadog sketches, through its receivers and sending them through its exporters.

{{< admonition type="note" >}}
The availability of different receivers and exporters depends on your OpenTelemetry Collector [distribution](https://opentelemetry.io/docs/concepts/distributions/).
{{< /admonition >}}

You can use the OpenTelemetry protocol (OTLP) over HTTP to send exponential histograms to Grafana Mimir in their existing format, or you can use the Prometheus remote write protocol to send them as Prometheus native histograms.

The OpenTelemetry SDK supports instrumenting applications in multiple languages. Refer to [Language APIs & SDKs](https://opentelemetry.io/docs/languages/) for a complete list.

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

   For more information about views, refer to [Registering Views](https://opentelemetry.io/docs/languages/go/instrumentation/#registering-views) in the OpenTelemetry SDK documentation for Go. For information about view configuration parameters, refer to [Base2 Exponential Bucket Histogram Aggregation](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#base2-exponential-bucket-histogram-aggregation) in the OpenTelemetry Metrics SDK on GitHub.

## Migrate from explicit bucket histograms

To ease the migration process, you can keep the custom bucket definition of an explicit bucket histogram and add a view for the exponential histogram.

1. Start with an existing histogram that uses explicit buckets.
1. Create a view for the exponential histogram. Assign this view a unique name and include the exponential aggregation. This creates a metric with the assigned name and exponential buckets.

   The following example shows how to create a metric called `request_latency_exp` that uses exponential buckets.

   ```
   v := sdkmetric.NewView(sdkmetric.Instrument{
      	Name: "request_latency",
      	Kind: sdkmetric.InstrumentKindHistogram,
      }, sdkmetric.Stream{
   	   Name: "request_latency_exp",
   	   Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, NoMinMax: true, MaxScale: 20},
      })
   ```

   For more information about creating a view, refer to [View](https://opentelemetry.io/docs/specs/otel/metrics/sdk/#view) in the OpenTelemetry Metrics SDK.

1. Modify dashboards to use the exponential histogram metrics. Refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) for more information.

   Use one of the following strategies to update dashboards.

   - (Recommended) Create dashboards with the exponential histogram queries. This solution requires looking at different dashboards for data before and after the migration, until data before the migration is removed due to passing its retention time. You can publish the dashboard when sufficient time has passed to serve users with the new data.
   - Add a dashboard variable to your dashboard to enable switching between explicit bucket histograms and exponential histograms. There isn't support for selectively enabling and disabling queries in Grafana ([issue 79848](https://github.com/grafana/grafana/issues/79848)). As a workaround, add the dashboard variable `latency_metrics`, for example, and assign it a value of either `-1` or `1`. Then, add the following two queries to the panel:

     ```
     <explicit_bucket_query> < ($latency_metrics * +Inf)
     ```

     ```
     <exponential_query> < ($latency_metrics * -Inf)
     ```

     Where `explicit_bucket_query` is the original query and `exponential_query` is the same query using exponential histogram query syntax. This technique is employed in Mimir's dashboards. For an example, refer to the [Overview dashboard](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled/dashboards/mimir-overview.json) in the Mimir repository.

     This solution allows users to switch between the explicit bucket histogram and the exponential histogram without going to a different dashboard.

   - Replace the explicit bucket queries with modified queries. For example, replace:

     ```
     <explicit_bucket_query>
     ```

     with

     ```
     <exponential_query> or <explicit_bucket_query>
     ```

     Where `explicit_bucket_query` is the original query and `exponential_query` is the same query using exponential histogram query syntax.

     {{< admonition type="warning" >}}
     Using the PromQL operator `or` can lead to unexpected results. For example, if a query uses a range of seven days, such as `sum(rate(http_request_duration_seconds[7d]))`, then this query returns a value as soon as there are two exponential histograms samples present before the end time specified in the query. In this case, the seven day rate is calculated from a couple of minutes, rather than seven days, worth of data. This results in an inaccuracy in the graph around the time you started scraping exponential histograms.
     {{< /admonition >}}

1. Begin adding recording rules and alerts to use exponential histograms. Don't remove any existing recording rules and alerts at this time.
1. It's important to keep scraping both explicit bucket and exponential histograms for at least the period of the longest range in your recording rules and alerts, plus one day. This is the minimum amount of time, but it's recommended to keep scraping both data types until you can verify the new rules and alerts.

   For example, if you have an alert that calculates the rate of requests, such as `sum(rate(http_request_duration_seconds[7d]))`, this query looks at the data from the last seven days plus the Prometheus [lookback period](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness). When you start sending exponential histograms, the data isn't there for the entire seven days, and therefore, the results might be unreliable for alerting.

1. After configuring exponential histogram collection, remove the explicit bucket histogram definition, as well as any views that expose explicit buckets.
1. Clean up recording rules and alerts by deleting the explicit bucket histogram version of the rule or alert.

## Bucket boundary calculation

Bucket boundaries for exponential histograms are calculated similarly to those for native histograms. The only difference is that for exponential histograms, bucket offsets are shifted by one, as shown in the following equation.

<!--- LaTeX equation source: {\left( 2^{2^{-schema}} \right)}^{index} < v \leq {\left( 2^{2^{-schema}}\right)}^{index+1} -->

![Positive bucket definition](otel-pos-bucket-def.svg)

For more information, refer to [bucket boundary calculation](https://grafana.com/docs/mimir/next/send/native-histograms/#bucket-boundary-calculation) in the documentation for native histograms.
