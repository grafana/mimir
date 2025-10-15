---
description: Learn how to store classic histograms more efficiently in Grafana Mimir.
keywords:
  - send metrics
  - native histogram
  - custom buckets
  - NHCB
  - prometheus
  - grafana alloy
  - instrumentation
  - TCO
menuTitle: Native histograms with custom buckets
title: Send native histograms with custom buckets to Grafana Mimir
weight: 2
---

# Send native histograms with custom buckets to Grafana Mimir

Prometheus [native histograms](https://prometheus.io/docs/specs/native_histograms/) with custom buckets, also known as NHCBs, are a sample type in the Prometheus ecosystem that makes it possible to store classic Prometheus [histograms](https://prometheus.io/docs/concepts/metric_types/#histogram) as NHCBs.

NHCBs are different from classic Prometheus histograms in a number of ways:

- An instance of a native histogram metric only requires a single time series. This is because the buckets, sum of observations, and the count of observations are stored in a single sample type called `native histogram` rather than in separate time series using the `float` sample type. Thus, there are no `<metric>_bucket`, `<metric>_sum`, or `<metric>_count` series. There is only a `<metric>` time series.
- Querying native histograms via the Prometheus query language (PromQL) uses a different syntax. For details, refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) and [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).

For an introduction to native histograms in general, watch the [Native Histograms in Prometheus](https://www.youtube.com/watch?v=AcmABV6NCYk) presentation. For a short introduction to NHCBs, watch [Native Histograms With Custom Buckets](https://www.youtube.com/watch?v=2v9DOGq2Mos).

This document provides a practical implementation guide for using NHCBs in Grafana Mimir. For comprehensive guidance, refer to the official [specification](https://prometheus.io/docs/specs/native_histograms/) in the Prometheus documentation.

## Advantages and disadvantages

There are advantages and disadvantages of using NHCBs compared to classic Prometheus histograms.

### Advantages

- Lower storage costs. NHCBs use a single series and applies a sparse representation that avoids storing empty buckets.
- Storage and query of NHCBs are atomic, meaning that either all or none of a NHCB sample is stored or retrieved. This is different from classic histograms, which are split into multiple independent time series that might be individually lost or delayed, resulting in corrupted or inconsistent data.
- It's possible to migrate to NHCBs without modifying the instrumentation.
- A migration to [native histograms with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets) takes fewer steps once NHCBs are in use, as queries and visualizations are already migrated.

### Disadvantages

- You need to adopt certain queries and visualizations to enable the advantages.
- NHCBs depends on experimental Prometheus Remote-Write 2.0.
- There's some overhead to the scrape process. This depends on the scrape protocol and the ratio of classic histograms to other kind of metrics.

## Instrumentation

No change is required in instrumentation.

Currently scrape processes like Prometheus or Grafana Alloy convert classic histograms during scrape to NHCBs, as there is no support for NHCBs in exposition formats.

## Scrape and send native histograms with Prometheus

Use the latest Prometheus version.

1. To enable scraping native histograms from the application, you need to enable the native histograms feature via a feature flag on the command line:

   ```bash
   prometheus --enable-feature=native-histograms
   ```

   This flag makes Prometheus detect and scrape [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets) over the `PrometheusProto` scrape protocol and ignore the classic histogram version of metrics that have native histograms defined as well.

   {{< admonition type="note" >}}
   <!-- Issue: https://github.com/prometheus/prometheus/issues/11265 -->

   Native histograms don't have a textual presentation on the application's `/metrics` endpoint. Therefore, Prometheus negotiates a Protobuf protocol transfer in these cases.
   {{< /admonition >}}

1. Optionally, to disable native histograms with exponential buckets and only use NHCBs, you need to set scrape protocols that don't carry native histograms with exponential buckets. This is not a best practice, as it does not save on migration time or effort, yet loses the advantages of exponential buckets and has higher overhead than using `PrometheusProto`.

   For example, use this scrape protocol setting to avoid scraping native histograms with the exponential schema:

   ```yaml
   scrape_configs:
     - job_name: myapp
       scrape_protocols:
         [OpenMetricsText1.0.0, OpenMetricsText0.0.1, PrometheusText0.0.4]
   ```

1. To enable converting classic histograms into NHCBs, you need to set `convert_classic_histograms_to_nhcb` to `true` in your scrape jobs. This setting has no effect for histograms that already have a native histogram defined, such as with [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets).

   For example, to convert classic histograms to NHCBs, use the following configuration:

   ```yaml
   scrape_configs:
     - job_name: myapp
       convert_classic_histograms_to_nhcb: true
   ```

1. To keep scraping the classic histogram version of native histogram metrics, you need to set `always_scrape_classic_histograms` to `true` in your scrape jobs.

   {{< admonition type="note" >}}
   In Prometheus versions 3.0 and earlier, the `always_scrape_classic_histograms` setting is called `scrape_classic_histograms`. Use the setting name that corresponds to the version of Prometheus you're using.
   {{< /admonition >}}

   For example, to get both classic and native histograms, use the following configuration:

   ```yaml
   scrape_configs:
     - job_name: myapp
       convert_classic_histograms_to_nhcb: true
       always_scrape_classic_histograms: true
   ```

1. To be able to send native histograms to a Prometheus remote-write 2.0 compatible receiver, for example, Grafana Cloud Metrics or Mimir, set `send_native_histograms` to `true` and `protobuf_message` to `io.prometheus.write.v2.Request` in the remote-write configuration. For example:

   ```yaml
   remote_write:
     - url: http://.../api/prom/push
       send_native_histograms: true
       protobuf_message: "io.prometheus.write.v2.Request"
   ```

   {{< admonition type="note" >}}
   Prometheus remote-write 2.0 is experimental. For more information, refer to the [Prometheus Remote-Write 2.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/).
   {{< /admonition >}}

   {{< admonition type="note" >}}
   Prometheus remote-write 2.0 doesn't fully support [metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata).
   {{< /admonition >}}

## Migrate from classic histograms

To ease the migration process, you can keep scraping classic histograms and NHCBs at the same time.

1. Let Prometheus scrape both classic histograms and NHCBs.
1. Send native histograms to remote write, along with the existing classic histograms.
1. Modify dashboards to use the native histograms metrics. Refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) for more information.

   Use one of the following strategies to update dashboards.

   - (Recommended) Add new dashboards with the new native histograms queries. This solution requires looking at different dashboards for data before and after the migration until data before the migration is removed due to passing its retention time. You can publish the new dashboard when sufficient time has passed to serve users with the new data.
   - Add a dashboard variable to your dashboard to enable switching between classic histograms and native histograms. There isn't support for selectively enabling and disabling queries in Grafana ([issue 79848](https://github.com/grafana/grafana/issues/79848)). As a workaround, add the dashboard variable `latency_metrics`, for example, and assign it a value of either `-1` or `1`. Then, add the following two queries to the panel:

     ```
     (<classic_query>) and on() (vector($latency_metrics) == 1)
     ```

     ```
     (<native_query>) and on() (vector($latency_metrics) == -1)
     ```

     Where `classic_query` is the original query and `native_query` is the same query using native histograms query syntax placed inside parentheses. Mimir dashboards use this technique. For an example, refer to the [Overview dashboard](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled/dashboards/mimir-overview.json) in the Mimir repository.

     This solution allows users to switch between classic histograms and native histograms without going to a different dashboard.

   - Replace the existing classic histograms queries with modified queries. For example, replace:

     ```
     <classic_query>
     ```

     with

     ```
     <native_query> or <classic_query>
     ```

     Where `classic_query` is the original query and `native_query` is the same query using native histograms query syntax.

     {{< admonition type="warning" >}}
     Using the PromQL operator `or` can lead to unexpected results. For example, if a query uses a range of seven days, such as `sum(rate(http_request_duration_seconds[7d]))`, then this query returns a value as soon as there are two native histograms samples present before the end time specified in the query. In this case, the seven day rate is calculated from a couple of minutes, rather than seven days, worth of data. This results in an inaccuracy in the graph around the time you started scraping native histograms.
     {{< /admonition >}}

1. Start adding new recording rules and alerts to use native histograms. Do not remove the existing recording rules and alerts at this time.
1. It is important to keep scraping both classic and native histograms for at least the period of the longest range in your recording rules and alerts plus one day. This is the minimum amount of time, but it's recommended to keep scraping both sample types until the new rules and alerts can be verified.

   For example, if you have an alert that calculates the rate of requests, such as `sum(rate(http_request_duration_seconds[7d]))`, this query looks at the data from the last seven days plus the Prometheus [lookback period](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness). When you start sending native histograms, the data isn't there for the entire seven days, and therefore, the results might be unreliable for alerting.

1. After configuring the collection of native histograms, choose one of the following ways to stop collecting classic histograms.

   - Drop the classic histogram series with [Prometheus relabeling](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) or [Grafana Alloy prometheus.relabel](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.relabel) at the time of scraping.
   - Stop scraping the classic histogram version of metrics. This option applies to all metrics of a scrape target.

1. Clean up recording rules and alerts by deleting the classic histogram version of the rule or alert.

## Limit the number of buckets

There is no way to automatically reduce the resolution or merge custom buckets like there is with [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets#limit-the-number-buckets). Set bucket limits greater than the largest expected bucket count to avoid having NHCBs rejected.
