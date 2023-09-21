---
description: Learn how to send metric data to Grafana Mimir.
keywords:
  - send metrics
  - native histogram
  - prometheus
  - grafana agent
  - instrumentation
menuTitle: Native histograms
title: Send native histograms to Mimir
weight: 100
---

# Send native histograms to Mimir

Prometheus native histograms is a data type in the Prometheus ecosystem that enables producing, storing and querying high resolution [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) of observations.

Native histograms are different from classic Prometheus histograms in a number of ways:

1. Native histogram bucket boundaries are calculated with a formula depending on the scale (resolution) of the native histogram and are not user defined. The calculation produces exponentially increasing bucket boundaries. For more detail see [Bucket boundary calculation](#bucket-boundary-calculation).
1. Native histogram bucket boundaries may change (widen) dynamically if observations result in too many buckets. For more detail see [Limiting the number of buckets](#limiting-the-number-of-buckets).
1. Native histogram bucket counters only count observations inside the bucket boundaries, whereas the classic histogram buckets only had an upper bound called `le` and counted all observations in the bucket and all lower buckets (cummulative).
1. An instance of a native histogram metric only requires a single time series, because the buckets, sum of observations, and count of observations are stored in a single new data type - and not in separate time series using the float data type. Thus there are no `<metric>_bucket`, `<metric>_sum`, `<metric>_count` series, only `<metric>`.
1. Querying native histograms via the Prometheus query language (PromQL) uses a different syntax. There are also a couple of new [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).

For an introduction to native histograms, watch the following presentation: [Native Histograms in Prometheus](https://www.youtube.com/watch?v=AcmABV6NCYk).

## Pros and cons

This section describes the pros and cons of using native histograms compared to the classic Prometheus histograms. For more information and real example, see [Prometheus Native Histograms in Production](https://www.youtube.com/watch?v=TgINvIK9SYc&t=127s) video.

### Pros

1. Simpler instrumentation. No need to think about bucket boundaries, they are automatic.
1. Better resolution in practice as custom bucket layouts are usually not that high resolution.
1. Native histograms are compatible with each other due to the automatic layout, thus they can be easily combined. Caveat: the operation might scale down an operand to lower resolution to match the other operand.

### Cons

1. Loses precision in case the observed values are on a logarithmic scale already (e.g. sound pressure measured in decibels).
1. Loses precision if you are converting from an external representation that has stict custom bucket boundaries.
1. No way to set the bucket boundaries, which means that if a query falls between bucket boundaries, the result will be an approximation. In general this should not be a problem due to the much better resolution.

## Use native histograms

### Instrument application with Prometheus client libraries

The following example has some reasonable defaults to define a new native histogram metric.

{{% admonition type="note" %}}
Native histogram options can be added to existing classic histograms to get both the classic and native histogram at the same time. See [Migrate from classic histograms](#migrate-from-classic-histograms).
{{% /admonition %}}

{{< code >}}

```go
histogram := prometheus.NewHistogram(
   prometheus.HistogramOpts{
      Name: "request_latency_seconds",
      Help: "Histogram of request response times.",
      NativeHistogramBucketFactor: 1.1,
      NativeHistogramMaxBucketNumber: 100,
      NativeHistogramMinResetDuration: 1*time.Hour,
})
```

```java
static final Histogram requestLatency = Histogram.build()
     .name("requests_latency_seconds")
     .help("Histogram of request latency in seconds.")
     .withNativeInitialSchema(3)
     .withNativeMaxNumberOfBuckets(100)
     .withNativeResetDuration(1, TimeUnit.HOURS)
     .register();
```

{{< /code >}}

In GO the `NativeHistogramBucketFactor` option sets the maximum expansion rate between neighbouring buckets. The value `1.1` means that buckets grow with at most 10%. The currently supported values range from `1.0027` or 0.27% up to 65536 or 655%. For more detailed explanation see [Bucket boundary calculation](#bucket-boundary-calculation).

Some of the resulting buckets for factor `1.1` rounded to two decimal places are:

..., [0.84, 0.94), [0.92, 1), [1, 1.09), [1.09, 1.19), [1.19, 1.30), ...

..., [76.1, 83), [83, 91), [91, 99), ...

..., [512, 558), [558, 608), [608, 663), ...

In Java `.withNativeInitialSchema` using schema value `3` results in the same bucket boundaries.

The value of `NativeHistogramMaxBucketNumber`/`withNativeMaxNumberOfBuckets` limits the number of buckets produced by the observations. This can be especially useful if the receiver side is limiting the number of buckets that can be sent. For more information about the bucket limit see [Limiting the number of buckets](#limiting-the-number-of-buckets).

The duration in `NativeHistogramMinResetDuration`/`withNativeResetDuration` will prohibit counter resets inside that period. Counter resets are related to the bucket limit, for more information see [Limiting the number of buckets](#limiting-the-number-of-buckets).

### Scrape and send native histograms with Prometheus

Use the latest version of Prometheus.

1. As of version 2.47, you need to enable native histograms feature via a feature flag.

   ```bash
   prometheus --enabled-feature native-histograms
   ```

1. The above flag will make Prometheus detect and scrape native histograms, but ignores classic histogram version of metrics. In case of a migration when a metric is both a classic and native histogram at the same time we recommend setting `scrape_classic_histograms` to `true` in your scrape jobs, for example:

   ```yaml
   scrape_configs:
     - job_name: myapp
       scrape_classic_histograms: true
   ```

   in your scrape jobs, to get both histogram version.

   {{% admonition type="note" %}}
   Native histograms don't have a textual presentation at the moment on the application's `/metrics` endpoint, Prometheus actually negotiates a Protobuf protocol transfer in this case.
   {{% /admonition %}}

1. To be able to send native histograms to a Prometheus remote write compatible receiver, for example Grafana Cloud Metrics, Mimir, etc, set `send_native_histograms` to `true` in the remote write configuration, for example:

   ```yaml
   remote_write:
      - url: http://.../api/prom/push
        send_native_histograms: true
   ```

### Scrape and send native histograms with Grafana Agent

TBD, awaiting agent version 0.37.

## Migrate from classic histograms

It is perfectly possible to keep the custom bucket definition of a classic histogram and add native histogram buckets at the same time. This can ease the migration process, which can look like this in general:

1. Add native histogram definition to an existing histogram in the instrumentation.
1. Let Prometheus or Grafana Agent scrape both classic and native histogram.
1. Send native histograms to remote write - if classic histogram is scraped, it is sent by default.
1. Start modifying the recording rules, alerts, dashboards to use the new native histograms.
1. Once everything works, remove the custom bucket definition from the instrumentation.

Code examples:

{{< code >}}

```go
histogram := prometheus.NewHistogram(
   prometheus.HistogramOpts{
      Name: "request_latency_seconds",
      Help: "Histogram of request response times.",
      Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100},
      NativeHistogramBucketFactor: 1.1,
      NativeHistogramMaxBucketNumber: 100,
      NativeHistogramMinResetDuration: 1*time.Hour,
})
```

```java
static final Histogram requestLatency = Histogram.build()
     .name("requests_latency_seconds")
     .help("Histogram of request latency in seconds.")
     .withClassicBuckets(0.005, 0.01, 0.025, 0,05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, Double.NaN);
     .withNativeInitialSchema(3)
     .withNativeMaxNumberOfBuckets(100)
     .withNativeResetDuration(1, TimeUnit.HOURS)
     .register();
```

{{< /code >}}

## Bucket boundary calculation

This section assumes that you are famliar with basic algebra. Native histogram bucket boundaries are calculated from an exponential formula with a base of 2.

Native histogram samples have three different kind of buckets, for any observed value the value is counted towards one kind of bucket.

- A zero bucket, which contains the count of observations whose absolute value is smaller or equal to the zero threshold.

  [//]: # "LaTeX equation source: -threshold \\leq v \leq threshold"

  ![Zero threshold definition](zero-threshold-def.svg)

- Positive buckets, which contain the count of observations with a positive value that is greater or equal to the lower bound and smaller than the upper bound of a bucket.

  [//]: # "LaTeX equation source: {2^{\\left( 2^{-schema}\right)}}^{index} \leq v < {2^{\left( 2^{-schema}\right)}}^{index+1}"

  ![Positive bucket definition](pos-bucket-def.svg)

  where the _index_ can be a positive or negative integer resulting in boundaries above 1 and fractions bellow 1. The _schema_ either directly specified out of `[-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8]` at instrumentation time or it is the largest number chosen from the list in such way that

  [//]: # "LaTeX equation source: 2^{\\left( 2^{-schema}\right)} <= factor"

  ![Factor equation](factor-equation.svg)

  for example for factor `1.1`:

  [//]: # "Latex equation source: 2^{\\left(2^{-3}\right)}\simeq1.09<=1.1"

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

- Negative buckets, which contain the coint of observations with a negative value that is smaller or equal to the upper bound and larger than the lower bound of a bucket.

  [//]: # "LaTeX equation source: {-2^{\left( 2^{-schema}\right)}}^{index+1} < v \leq {-2^{\left( 2^{-schema}\right)}}^{index}"

  ![Negative bucket definition](neg-bucket-def.svg)

  where the `schema` is choosen as above.

## Limiting the number of buckets

The server scraping or receiving native histograms over remote write may limit the number of native histogram buckets it accepts. The server may reject or downscale (reduce resolution and merge adjacent buckets). Even if that wasn't the case, storing and emitting potentially unlimited buckets isn't practical.

The intrumentation libraries of Prometheus have automation to keep the number of buckets down, provided that the maximum bucket number option is used (e.g. `NativeHistogramMaxBucketNumber` in GO).

Once the set maximum is exceeded, the following strategy is enacted:

1. First, if the last reset (or the creation) of the histogram is at least the minimum reset duration ago, then the whole histogram is reset to its initial state (including classic buckets). This only works if the minimum reset duration was set (`NativeHistogramMinResetDuration` in GO).

1. If less time has passed, or if the minimum reset duration is zero, no reset is performed. Instead, the zero threshold is increased sufficiently to reduce the number of buckets to or below the maximum bucket number, but not to more than the maximum zaro threashold (`NativeHistogramMaxZeroThreshold` in GO). Thus, if the threashold is at or above the maximum threashold already nothing happens at this step.

1. After that, if the number of buckets still exceeds maximum bucket number, the resolution of the histogram is reduced by doubling the width of all the sparse buckets (up to a growth factor between one bucket to the next of 2^(2^4) = 65536, see above in [Bucket boundary calculation](#bucket-boundary-calculation)).
	
1. Any increased zero threshold or reduced resolution is reset back to their original values once the minimum reset duration has passed (since the last reset or the creation of the histogram).
