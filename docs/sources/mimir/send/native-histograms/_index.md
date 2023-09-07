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

### Instrument application

TODO: should note that one can keep the old histograms
TODO: add GoLang examples. And maybe Python? Or link to examples.

### Scrape native histograms with Prometheus

TODO: should note that one can scrape both optionally

### Scrape native histograms with Grafana Agent

TODO: should note that one can scrape both optionally

### Send native histograms to Grafana Cloud with Prometheus

### Send native histograms to Grafana Cloud with Grafana Agent

## Migrate from classic histograms

### Instrument application

### Scrape metrics with Prometheus

TODO: keep scraping both

### Scape metrics with Grafana Agent

TODO: keep scraping both

## Bucket boundary calculation

Native histogram samples have three different kind of buckets, for any observed value the value is counted towards one bucket.

1. Zero bucket, which contains the count of observations whose absolute value is smaller or equal to the zero threshold.

   [//]: # "LaTeX equation source: -threshold \leq v \leq threshold"

   ![Zero threshold definition](zero-threshold-def.svg)

1. Positive buckets, which contain the count of observations with a positive value that is greater or equal to the lower bound and smaller than the upper bound of a bucket.

   [//]: # "LaTeX equation source: 2^{\left( 2^{-schema}\right) index} \leq v < 2^{\left( 2^{-schema}\right) (index+1)}"

   ![Positive bucket definition](pos-bucket-def.svg)

1. Negative buckets, which contain the coint of observations with a negative value that is smaller or equal to the upper bound and larger than the lower bound of a bucket.

   [//]: # "Latex equation source: -2^{\left( 2^{-schema}\right) (index+1)} < v \leq -2^{\left( 2^{-schema}\right) index}"

   ![Negative bucket definition](neg-bucket-def.svg)

## Limiting the number of buckets
