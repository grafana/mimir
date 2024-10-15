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

# Classic histograms:
sum(request_duration_seconds_sum)
```

### Find rate of observations

To query the rate of all observations calculated over 5 minute time window, use the following query:

```PromQL
# Native histograms:
histogram_count(sum(rate(request_duration_seconds[5m])))

# Classic histograms:
sum(rate(request_duration_seconds_count[5m]))
```

To query the rate of observations between two values such as `0` and `2` seconds, use the following query:

```PromQL
# Native histograms:
histogram_fraction(0, 2, sum(rate(request_duration_seconds[5m])))
*
histogram_count(sum(rate(request_duration_seconds[5m])))

# Classic histograms:
sum(rate(request_duration_seconds_bucket{le="2.5"}[5m]))
```

There is a native histogram function that estimates the fraction of the total number of observations that fall within a certain interval, such as `[0, 2]`.
For more information, refer to [histogram fraction](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_fraction).

Classic histograms have no such function. Therefore, if the lower and upper bounds of the interval do not line up with the bucket boundaries of a classic histogram,
you have to estimate the fraction manually.

{{% admonition type="note" %}}
Only ever use the `histogram_fraction` function by including `rate` or `increase` inside of it with a suitable range selector.
If you do not specify a range, such as `5m`, the function uses the current value of the histogram.
In that case, the current value is an accumulated value over the lifespan of the histogram or since the histogram was last reset.
{{% /admonition %}}

### Quantiles

To query an upper bound of observation values that 95% of observations fall under, use the following query:

```PromQL
# Native histograms:
histogram_quantile(0.95, sum(rate(request_duration_seconds[5m])))

# Classic histograms:
histogram_quantile(0.95, sum by (le) (rate(request_duration_seconds_bucket[5m])))
```

{{% admonition type="note" %}}
Only ever use the `histogram_quantile` function by including `rate` or `increase` inside of it with a suitable range selector.
If you do not specify a range, such as `5m`, the function uses the current value of the histogram.
In that case, the current value is an accumulated value over the lifespan of the histogram or since the histogram was last reset.
{{% /admonition %}}

## Create Grafana dashboards

When creating a Grafana dashboard for your native histogram, the most relevant panel types are [Histogram](/docs/grafana/latest/panels-visualizations/visualizations/histogram/) and [Heatmap](/docs/grafana/latest/panels-visualizations/visualizations/heatmap/).

In [Explore](https://grafana.com/docs/grafana/latest/explore/), the functions `histogram_count`, `histogram_sum`, and `histogram_quantile` will result in normal floating point series that you can plot as usual.

{{% admonition type="note" %}}
Visualizing native histogram series directly in the **Explore** view is a work in progress.
{{% /admonition %}}
