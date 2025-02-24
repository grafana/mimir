---
description: Learn how to send metric data to Grafana Mimir.
keywords:
  - send metrics
  - native histogram
  - prometheus
  - grafana alloy
  - instrumentation
menuTitle: Native histograms
title: Send native histograms to Mimir
weight: 100
---

# Send native histograms to Mimir

{{% admonition type="note" %}}
Native histograms are an experimental feature of Grafana Mimir.
{{% /admonition %}}

Prometheus native histograms is a data type in the Prometheus ecosystem that makes it possible to produce, store, and query a high-resolution [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) of observations.

Native histograms are different from classic Prometheus histograms in a number of ways:

- Native histogram bucket boundaries are calculated by a formula that depends on the scale (resolution) of the native histogram, and are not user defined. The calculation produces exponentially increasing bucket boundaries. For details, refer to [Bucket boundary calculation](#bucket-boundary-calculation).
- Native histogram bucket boundaries might change (widen) dynamically if the observations result in too many buckets. For details, refer to [Limit the number of buckets](#limit-the-number-of-buckets).
- Native histogram bucket counters only count observations inside the bucket boundaries, whereas the classic histogram buckets only have an upper bound called `le` and count all observations in the bucket and all lower buckets (cumulative).
- An instance of a native histogram metric only requires a single time series, because the buckets, sum of observations, and the count of observations are stored in a single data type called `native histogram` rather than in separate time series using the `float` data type. Thus, there are no `<metric>_bucket`, `<metric>_sum`, and `<metric>_count` series. There is only `<metric>` time series.
- Querying native histograms via the Prometheus query language (PromQL) uses a different syntax. For details, refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) and [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).

For an introduction to native histograms, watch the [Native Histograms in Prometheus](https://www.youtube.com/watch?v=AcmABV6NCYk) presentation.

## Advantages and disadvantages

There are advantages and disadvantages of using native histograms compared to the classic Prometheus histograms. For more information and a real example, refer to the [Prometheus Native Histograms in Production](https://www.youtube.com/watch?v=TgINvIK9SYc&t=127s) video.

### Advantages

- Simpler instrumentation: you do not need to think about bucket boundaries because they are created automatically.
- Better resolution in practice: custom bucket layouts are usually not high resolution.
- Native histograms are compatible with each other: they have an automatic layout, which makes them easy to combine.
  {{< admonition type="note" >}}
  The operation might scale down an operand to lower resolution to match the other operand.
  {{< /admonition >}}

### Disadvantages

- Observations might be distributed in a way that is not a good fit for the exponential bucket schema, such as sound pressure measured in decibels, which are already logarithmic.
- If converting from an externally represented histogram with specific bucket boundaries, there is generally no precise match with the bucket boundaries of the native histogram, and in which case you need to use interpolation.
- There is no way to set an arbitrary bucket boundary, such as one that is particularly interesting for an SLO definition. Generally, ratios of observations above or below a given threshold have to be estimated by interpolation, rather than being precise in the case for a classic histogram with a configured bucket boundary at a given threshold.

The preceding problems are mitigated by high resolution, which native histograms can provide at a much lower resource cost compared to classic histograms.

## Instrument application with Prometheus client libraries

The following examples have some reasonable defaults to define a new native histogram metric. The examples use the [Go client library](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Histogram) version 1.16 and the [Java client library](https://prometheus.github.io/client_java/api/io/prometheus/metrics/core/metrics/Histogram.Builder.html) 1.0.

{{< admonition type="note" >}}
Native histogram options can be added to existing classic histograms to get both the classic and native histogram at the same time. Refer to [Migrate from classic histograms](#migrate-from-classic-histograms).
{{< /admonition >}}

{{< code >}}

```go
histogram := prometheus.NewHistogram(
   prometheus.HistogramOpts{
      Name: "request_latency_seconds",
      Help: "Histogram of request latency in seconds",
      NativeHistogramBucketFactor: 1.1,
      NativeHistogramMaxBucketNumber: 100,
      NativeHistogramMinResetDuration: 1*time.Hour,
})
```

```java
static final Histogram requestLatency = Histogram.build()
     .name("requests_latency_seconds")
     .help("Histogram of request latency in seconds")
     .nativeOnly()
     .nativeInitialSchema(3)
     .nativeMaxNumberOfBuckets(100)
     .nativeResetDuration(1, TimeUnit.HOURS)
     .register();
```

{{< /code >}}

In Go, the `NativeHistogramBucketFactor` option sets an upper limit of the relative growth from one bucket to the next. The value 1.1 means that a bucket is at most 10% wider than the next smaller bucket. The currently supported values range from `1.0027` or 0.27% up to 65536 or 655%. For more detailed explanation, refer to [Bucket boundary calculation](#bucket-boundary-calculation).

Some of the resulting buckets for factor `1.1` rounded to two decimal places are:

..., (0.84, 0.92], (0.92, 1], (1, 1.09], (1.09, 1.19], (1.19, 1.30], ...

..., (76.1, 83], (83, 91], (91, 99], ...

..., (512, 558], (558, 608], (608, 663], ...

In Java `.nativeInitialSchema` using schema value `3` results in the same bucket boundaries. For more information about the schema supported in Java, consult the documentation for [nativeInitialSchema](<https://prometheus.github.io/client_java/api/io/prometheus/metrics/core/metrics/Histogram.Builder.html#nativeInitialSchema(int)>).

The value of `NativeHistogramMaxBucketNumber`/`nativeMaxNumberOfBuckets` limits the number of buckets produced by the observations. This can be especially useful if the receiver side is limiting the number of buckets that can be sent. For more information about the bucket limit refer to [Limit the number of buckets](#limit-the-number-of-buckets).

The duration in `NativeHistogramMinResetDuration`/`nativeResetDuration` will prohibit automatic counter resets inside that period. Counter resets are related to the bucket limit, for more information refer to [Limit the number of buckets](#limit-the-number-of-buckets).

## Scrape and send native histograms with Prometheus

Use Prometheus version 2.47 or later.

1. To enable scraping native histograms from the application, you need to enable the native histograms feature via a feature flag on the command line:

   ```bash
   prometheus --enable-feature=native-histograms
   ```

   This flag makes Prometheus detect and scrape native histograms and ignore the classic histogram version of metrics that have native histograms defined as well. Classic histograms without native histogram definitions are not affected.

1. To keep scraping the classic histogram version of native histogram metrics, you need to set `always_scrape_classic_histograms` to `true` in your scrape jobs.

   {{< admonition type="note" >}}
   In Prometheus versions earlier than 3.0, the `always_scrape_classic_histograms` setting is called `scrape_classic_histograms`. Use the setting name that corresponds to the version of Prometheus you're using.
   {{< /admonition >}}

   For example, to get both classic and native histograms, use the following configuration:

   ```yaml
   scrape_configs:
     - job_name: myapp
       always_scrape_classic_histograms: true
   ```

   {{< admonition type="note" >}}
   <!-- Issue: https://github.com/prometheus/prometheus/issues/11265 -->

   Native histograms don't have a textual presentation on the application's `/metrics` endpoint. Therefore, Prometheus negotiates a Protobuf protocol transfer in these cases.
   {{< /admonition >}}

   {{< admonition type="note" >}}
   In certain situations, the Protobuf parsing changes the number formatting of
   the `le` labels of conventional histograms and the `quantile` labels of
   summaries. Typically, this happens if the scraped target is instrumented with
   [client_golang](https://github.com/prometheus/client_golang), provided that
   [promhttp.HandlerOpts.EnableOpenMetrics](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/promhttp#HandlerOpts)
   is set to `false`. In such cases, integer label values are represented
   as `quantile="1"` or `le="2"` omitting the zero fractional.
   However, the Protobuf parsing changes the representation to always include a fractional (following the OpenMetrics
   specification), so the examples above become `quantile="1.0"` and `le="2.0"` after
   ingestion into Prometheus. This changes the identity of the metric from what was originally ingested.

   For more information, refer to [Feature Flags Native Histograms](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms) in the Prometheus documentation.
   {{< /admonition >}}

1. To be able to send native histograms to a Prometheus remote write compatible receiver, for example Grafana Cloud Metrics, Mimir, etc, set `send_native_histograms` to `true` in the remote write configuration, for example:

   ```yaml
   remote_write:
     - url: http://.../api/prom/push
       send_native_histograms: true
   ```

## Scrape and send native histograms with Grafana Alloy

Use the latest version of [Grafana Alloy](https://grafana.com/docs/alloy/<ALLOY_VERSION>).

1. To scrape native histograms, set the `scrape_protocols` argument in the `prometheus.scrape` component to specify `PrometheusProto` as the first protocol to negotiate.

   ```
    scrape_protocols = ["PrometheusProto", "OpenMetricsText1.0.0", "OpenMetricsText0.0.1", "PrometheusText0.0.4"]
   ```

1. To scrape classic histograms in addition to native histograms, set `always_scrape_classic_histograms` to `true`.

   {{< admonition type="note" >}}
   In Prometheus versions earlier than 3.0, the `always_scrape_classic_histograms` setting is called `scrape_classic_histograms`. Use the setting name that corresponds to the version of Prometheus you're using.
   {{< /admonition >}}

   ```
   scrape_protocols = ["PrometheusProto", "OpenMetricsText1.0.0", "OpenMetricsText0.0.1", "PrometheusText0.0.4"]
   always_scrape_classic_histograms = true
   ```

   For more information, refer to [prometheus.scrape](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.scrape/) in the Grafana Alloy documentation.

   {{< admonition type="note" >}}
   In certain situations, the Protobuf parsing changes the number formatting of
   the `le` labels of conventional histograms and the `quantile` labels of
   summaries. Typically, this happens if the scraped target is instrumented with
   [client_golang](https://github.com/prometheus/client_golang), provided that
   [promhttp.HandlerOpts.EnableOpenMetrics](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/promhttp#HandlerOpts)
   is set to `false`. In such cases, integer label values are represented
   as `quantile="1"` or `le="2"` omitting the zero fractional.
   However, the Protobuf parsing changes the representation to always include a fractional (following the OpenMetrics
   specification), so the examples above become `quantile="1.0"` and `le="2.0"` after
   ingestion into Prometheus. This changes the identity of the metric from what was originally ingested.

   For more information, refer to [Feature Flags Native Histograms](https://prometheus.io/docs/prometheus/latest/feature_flags/#native-histograms) in the Prometheus documentation.
   {{< /admonition >}}

1. To send native histograms to a Prometheus remote write compatible receiver, such as Grafana Cloud Metrics or Mimir, set the `send_native_histograms` argument to `true` in the `prometheus.remote_write` component. For example:

   ```
   prometheus.remote_write "mimir" {
     endpoint {
       url = "http://.../api/prom/push"
       send_native_histograms = true
     }
   }
   ```

## Migrate from classic histograms

To ease the migration process, you can keep the custom bucket definition of a classic histogram and add native histogram buckets at the same time.

1. Add the native histogram definition to an existing histogram in the instrumentation.
1. If the existing histogram doesn't have buckets defined, add the default buckets to keep the classic histogram.

   Code examples with both classic and native histogram defined for the same metric:

   {{< code >}}

   ```go
   histogram := prometheus.NewHistogram(
      prometheus.HistogramOpts{
          Name: "request_latency_seconds",
          Help: "Histogram of request latency in seconds",
          Buckets: prometheus.DefBuckets,  // If buckets weren't already defined.
          NativeHistogramBucketFactor: 1.1,
          NativeHistogramMaxBucketNumber: 100,
          NativeHistogramMinResetDuration: 1*time.Hour,
   })
   ```

   ```java
   static final Histogram requestLatency = Histogram.build()
      .name("requests_latency_seconds")
      .help("Histogram of request latency in seconds")
      .classicUpperBounds(Histogram.Builder.DEFAULT_CLASSIC_UPPER_BOUNDS)  // If upper bounds weren't already defined.
      .nativeInitialSchema(3)
      .nativeMaxNumberOfBuckets(100)
      .nativeResetDuration(1, TimeUnit.HOURS)
      .register();
   ```

   {{< /code >}}

1. Let Prometheus or Grafana Alloy scrape both classic and native histograms for metrics that have both defined.
1. Send native histograms to remote write, along with the existing classic histograms.
1. Modify dashboards to use the native histogram metrics. Refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) for more information.

   Use one of the following strategies to update dashboards.

   - (Recommended) Add new dashboards with the new native histogram queries. This solution requires looking at different dashboards for data before and after the migration, until data before the migration is removed due to passing its retention time. You can publish the new dashboard when sufficient time has passed to serve users with the new data.
   - Add a dashboard variable to your dashboard to enable switching between classic histograms and native histograms. There isn't support for selectively enabling and disabling queries in Grafana ([issue 79848](https://github.com/grafana/grafana/issues/79848)). As a workaround, add the dashboard variable `latency_metrics`, for example, and assign it a value of either `-1` or `1`. Then, add the following two queries to the panel:

     ```
     (<classic_query>) and on() (vector($latency_metrics) == 1)
     ```

     ```
     (<native_query>) and on() (vector($latency_metrics) == -1)
     ```

     Where `classic_query` is the original query and `native_query` is the same query using native histogram query syntax placed inside parentheses. Mimir dashboards use this technique. For an example, refer to the [Overview dashboard](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled/dashboards/mimir-overview.json) in the Mimir repository.

     This solution allows users to switch between the classic histogram and the native histogram without going to a different dashboard.

   - Replace the existing classic queries with modified queries. For example, replace:

     ```
     <classic_query>
     ```

     with

     ```
     <native_query> or <classic_query>
     ```

     Where `classic_query` is the original query and `native_query` is the same query using native histogram query syntax.

     {{< admonition type="warning" >}}
     Using the PromQL operator `or` can lead to unexpected results. For example, if a query uses a range of seven days, such as `sum(rate(http_request_duration_seconds[7d]))`, then this query returns a value as soon as there are two native histograms samples present before the end time specified in the query. In this case, the seven day rate is calculated from a couple of minutes, rather than seven days, worth of data. This results in an inaccuracy in the graph around the time you started scraping native histograms.
     {{< /admonition >}}

1. Start adding _new_ recording rules and alerts to use native histograms. Do not remove the old recording rules and alerts at this time.
1. It is important to keep scraping both classic and native histograms for at least the period of the longest range in your recording rules and alerts, plus one day. This is the minimum amount of time, but it's recommended to keep scraping both data types until the new rules and alerts can be verified.

   For example, if you have an alert that calculates the rate of requests, such as `sum(rate(http_request_duration_seconds[7d]))`, this query looks at the data from the last seven days plus the Prometheus [lookback period](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness). When you start sending native histograms, the data isn't there for the entire seven days, and therefore, the results might be unreliable for alerting.

1. After configuring native histogram collection, choose one of the following ways to stop collecting classic histograms.

   - Remove the custom bucket definition, `Buckets`/`classicUpperBounds`, from the instrumentation. In Java, also use the `nativeOnly()` option. Refer to the examples in [Instrument application with Prometheus client libraries](#instrument-application-with-prometheus-client-libraries).
   - Drop the classic histogram series with [Prometheus relabeling](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) or [Grafana Alloy prometheus.relabel](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.relabel) at the time of scraping.
   - Stop scraping the classic histogram version of metrics. This option applies to all metrics of a scrape target.

1. Clean up recording rules and alerts by deleting the classic histogram version of the rule or alert.

## Bucket boundary calculation

This section assumes that you are familiar with basic algebra. Native histogram bucket boundaries are calculated from an exponential formula with a base of 2.

Native histogram samples have three different kind of buckets, for any observed value the value is counted towards one kind of bucket.

- A zero bucket, which contains the count of observations whose absolute value is smaller or equal to the zero threshold.

<!--- LaTeX equation source: -threshold \leq v \leq threshold -->

![Zero threshold definition](zero-threshold-def.svg)

- Positive buckets, which contain the count of observations with a positive value that is greater than the lower bound and less or equal to the upper bound of a bucket.

<!--- LaTeX equation source: {\left( 2^{2^{-schema}} \right)}^{index-1} < v \leq {\left( 2^{2^{-schema}}\right)}^{index} -->

![Positive bucket definition](pos-bucket-def.svg)

where the _index_ can be a positive or negative integer resulting in boundaries above 1 and fractions below 1. The _schema_ either directly specified out of `[-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8]` at instrumentation time or it is the largest number chosen from the list in such way that

<!--- LaTeX equation source: 2^{2^{-schema}} <= factor -->

![Factor equation](factor-equation.svg)

for example for factor `1.1`:

<!--- Latex equation source: 2^{2^{-3}}\simeq1.09<=1.1 -->

![Factor 1.1 equation](factor-1.1-equation.svg)

Table of schema to factor:
| _schema_ | _factor_ | | _schema_ | _factor_ |
|----------|----------|--|----------|----------|
| -4 | 65536 | | 3 | 1.0905 |
| -3 | 256 | | 4 | 1.0443 |
| -2 | 16 | | 5 | 1.0219 |
| -1 | 4 | | 6 | 1.0109 |
| 0 | 2 | | 7 | 1.0054 |
| 1 | 1.4142 | | 8 | 1.0027 |
| 2 | 1.1892 |

- Negative buckets, which contain the count of observations with a negative value that is smaller than the upper bound and greater than or equal to the lower bound of a bucket.

<!--- LaTeX equation source: -{\left( 2^{2^{-schema}} \right)}^{index} \leq v < -{\left( 2^{2^{-schema}}\right)}^{index-1} -->

![Negative bucket definition](neg-bucket-def.svg)

where the `schema` is chosen as above.

## Limit the number of buckets

The server scraping or receiving native histograms over remote write may limit the number of native histogram buckets it accepts. The server may reject or downscale (reduce resolution and merge adjacent buckets). Even if that wasn't the case, storing and emitting potentially unlimited number of buckets isn't practical.

The instrumentation libraries of Prometheus have automation to keep the number of buckets down, provided that the maximum bucket number option is used, such as `NativeHistogramMaxBucketNumber` in Go.

After the set maximum is exceeded, the following strategy is enacted:

1. First, if the last reset (or the creation) of the histogram is at least the minimum reset duration ago, then the whole histogram is reset to its initial state (including classic buckets). This only works if the minimum reset duration was set (`NativeHistogramMinResetDuration` in Go).

1. If less time has passed, or if the minimum reset duration is zero, no reset is performed. Instead, the zero threshold is increased sufficiently to reduce the number of buckets to or below the maximum bucket number, but not to more than the maximum zero threshold (`NativeHistogramMaxZeroThreshold` in Go). Thus, if the threshold is at or above the maximum threshold already nothing happens at this step.

1. After that, if the number of buckets still exceeds maximum bucket number, the resolution of the histogram is reduced by doubling the width of all the buckets (up to a growth factor between one bucket to the next of 2^(2^4) = 65536, refer to [Bucket boundary calculation](#bucket-boundary-calculation)).

1. Any increased zero threshold or reduced resolution is reset back to their original values once the minimum reset duration has passed (since the last reset or the creation of the histogram).
