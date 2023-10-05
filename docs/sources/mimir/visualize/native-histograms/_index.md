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

Prometheus native histograms is a data type in the Prometheus ecosystem that enables producing, storing and querying high resolution [histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) of observations. To learn more about the native histograms data type and how to start sending them to Grafana Mimir, go to [Send native histograms to Mimir]({{< relref "../../send/native-histograms" >}}).

## Prometheus Query Language

In the examples below you'll learn how to query native histogram metrics and how it's different from the previous classic histograms. We'll use the example histogram metric called `request_duration_seconds`. We assume the metric has some labels as well, thus will apply an aggregation (`sum`) to the time series of the metric to produce a single output.

### Direct access to count and sum

To query the overall count of observations made to a histogram, use the following query:

```PromQL
# Native histograms:
histogram_count(sum(request_duration_seconds))

# Previous classic histograms:
sum(request_duration_seconds_count)
```

To query the overall sum of observed values, use the following query:

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
Never use the `histogram_fraction` function without including `rate` or `increase` inside it with a suitable range selector. Not having a range like 5 minutes selected will use the current value of the histogram, which will be an accumulated value since the the histogram existed (or was last reset), which probably not what you want.
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
Never use the `histogram_quantile` function without including `rate` or `increase` inside it with a suitable range selector. Not having a range like 5 minutes selected will use the current value of the histogram, which will be an accumulated value since the the histogram existed (or was last reset), which probably not what you want.
{{% /admonition %}}
