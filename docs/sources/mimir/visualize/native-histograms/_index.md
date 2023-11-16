---
description: Learn how to visualize native histograms.
keywords:
  - query metrics
  - native histogram
  - prometheus
  - grafana
  - panels
  - explore
  - promql
menuTitle: Native histograms
title: Visualize native histograms
weight: 100
---

# Visualize native histograms

Prometheus native histograms is a data type in the Prometheus ecosystem that makes it possible to produce, store, and query a high-resolution [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) of observations.
To learn more about the native histograms data type and how to start sending native histograms to Grafana Mimir,
refer to [Send native histograms to Mimir]({{< relref "../../send/native-histograms" >}}).

## Prometheus Query Language

The Prometheus Query Language (PromQL) allows you to query native histogram metrics.
PromQL queries of native histograms are different from those of classic histograms.

For more information about PromQL, refer to [Querying Prometheus](https://prometheus.io/docs/prometheus/latest/querying/basics/).

<!--
Given an example, where a histogram metric is named `request_duration_seconds`,
and it has some labels, you can apply an aggregation (`sum`) to the time series of the metric.
Doing so produces a single output.
 -->

### Query your histogram’s count or sum

To query the total count of observations within a histogram, use the following queries:

```PromQL
# Native histograms:
histogram_count(sum(request_duration_seconds))

# Classic histograms:
sum(request_duration_seconds_count)
```

To query the total sum of observed values, use the following query:

```PromQL
# Native histograms:
histogram_sum(sum(request_duration_seconds))

# Previous classic histograms:
sum(request_duration_seconds_sum)
```

### Find rate of observations

To query the rate of all observations, calculated over 5 minute time window, use the following query:

```PromQL
# Native histograms:
histogram_count(sum(rate(request_duration_seconds[5m])))

# Previous classic histograms:
sum(rate(request_duration_seconds_count[5m]))
```

In Grafana the range vector selector of `5m` would be replaced by `$__rate_interval`.

To query the rate of observations below a certain limit like 2 seconds, use the following query:

```PromQL
# Native histograms:
histogram_fraction(0, 2, sum(rate(request_duration_seconds[5m])))
*
histogram_count(sum(rate(request_duration_seconds[5m])))

# Previous classic histograms:
sum(rate(request_duration_seconds_bucket{le="2.5"}[5m]))
```

There are a number of things to note here:

- Native histograms have a new dedicated function to estimate what fraction of the total number of observations fall into a certain interval, in this case [0, 2]. To learn more about the new function, go to [histogram fraction](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_fraction).
- The previous classic histograms had no such function, meaning that in case the bucket boundaries didn't line up with the requested boundaries (like in the example above), you were out of luck and had to either accept the result as is, or do the estimation yourself.

{{% admonition type="note" %}}
Never use the `histogram_fraction` function without including `rate` or `increase` inside it with a suitable range selector. Not having a range like 5 minutes selected will use the current value of the histogram, which will be an accumulated value since the the histogram existed (or was last reset), which is probably not what you want.
{{% /admonition %}}

### Quantiles

To query an upper bound of observation values that 95% of observations fall under, use the following query:

```PromQL
# Native histograms:
histogram_quantile(0.95, sum(rate(request_duration_seconds[5m])))

# Previous classic histograms:
histogram_quantile(0.95, sum by (le) (rate(request_duration_seconds_bucket[5m])))
```

{{% admonition type="note" %}}
Never use the `histogram_quantile` function without including `rate` or `increase` inside it with a suitable range selector. Not having a range like 5 minutes selected will use the current value of the histogram, which will be an accumulated value since the the histogram existed (or was last reset), which is probably not what you want.
{{% /admonition %}}

## Grafana

The two panel types most relevant for native histograms are the [Histogram](/docs/grafana/latest/panels-visualizations/visualizations/histogram/) and [Heatmap](/docs/grafana/latest/panels-visualizations/visualizations/heatmap/) panels.

Regarding [Explore](/docs/grafana/latest/explore/), the functions `histogram_count`, `histogram_sum` and `histogram_quantile` will result in normal floating point series which you can plot as usual. Visualizing native histogram series directly in the explore view is a work in progress.
