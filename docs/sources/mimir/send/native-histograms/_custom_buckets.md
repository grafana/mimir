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
menuTitle: Native histograms custom buckets
title: Send native histograms with custom buckets to Grafana Mimir
weight: 2
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Send native histograms with custom buckets to Grafana Mimir

Prometheus [native histograms](https://prometheus.io/docs/specs/native_histograms/) with custom buckets, also known as NHCB, is a sample type in the Prometheus ecosystem that makes it possible to store classic Prometheus [histograms](https://prometheus.io/docs/concepts/metric_types/#histogram) as NHCB.

NHCB are different from classic Prometheus histograms in a number of ways:

- An instance of a native histogram metric only requires a single time series, because the buckets, sum of observations, and the count of observations are stored in a single sample type called `native histogram` rather than in separate time series using the `float` sample type. Thus, there are no `<metric>_bucket`, `<metric>_sum`, and `<metric>_count` series. There is only a `<metric>` time series.
- Querying native histograms via the Prometheus query language (PromQL) uses a different syntax. For details, refer to [Visualize native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/visualize/native-histograms/) and [functions](https://prometheus.io/docs/prometheus/latest/querying/functions/).

For an introduction to native histograms in general, watch the [Native Histograms in Prometheus](https://www.youtube.com/watch?v=AcmABV6NCYk) presentation. For a short introduction to NHCB, watch [Native Histograms With Custom Buckets](https://www.youtube.com/watch?v=2v9DOGq2Mos).

This document provides a practical implementation guide for using NHCB in Grafana Mimir, but it's not as comprehensive as the official [specification](https://prometheus.io/docs/specs/native_histograms/) in the Prometheus documentation.

## Advantages and disadvantages

There are advantages and disadvantages of using NHCB compared to the classic Prometheus histograms.

### Advantages

- Lower storage costs. NHCB use a single series instead of multiple and apply a sparse representation that aims to avoid storing buckets that are empty.
- Storage and query of NHCB are atomic, meaning that either all or none of a NHCB sample is stored or retrieved. This is different from classic histograms, which are split into multiple independent time series that might be individually lost or delayed, resulting in corrupted or inconsistent data.
- It is possible to migrate to NHCB without modifying the instrumentation.
- Easy to migrate to [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets) later as queries and visualizations would be already migrated.

### Disadvantages

- Queries and visualizations need to be adopted to enable the advantages.
- Depends on experimental Prometheus Remote-Write 2.0.
- Some overhead to the scrape process. This depends on the scrape protocol (Protobuf is the best) and the ratio of classic histograms to other kind of metrics.

## Instrumentation

No change is required in instrumentation.

Currently scrape processes like Prometheus or Grafana Alloy convert classic histograms during scrape to NHCB as there is no support for NHCB in exposition formats.

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

1. Optionally, to disable native histograms with exponential buckets and only use NHCB, you need to set scrape protocols that don't carry native histograms with exponential buckets. This is not recommended as it does not save on migration time or effort, but looses the advantages of exponential buckets and has higher overhead than using `PrometheusProto`.

   For example, use this scrape protocol setting to avoid scraping native histograms with exponential schema:

   ```yaml
   scrape_configs:
     - job_name: myapp
       scrape_protocols:
         [OpenMetricsText1.0.0, OpenMetricsText0.0.1, PrometheusText0.0.4]
   ```

1. To enable converting classic histograms into NHCB, you need to set `convert_histograms_to_nhcb` to `true` in your scrape jobs. This setting will have no effect for histograms that already have a native histogram defined, such as with [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets).

   For example, to convert classic histograms to NHCB, use the following configuration:

   ```yaml
   scrape_configs:
     - job_name: myapp
       convert_histograms_to_nhcb: true
   ```

1. To keep scraping the classic histogram version of native histogram metrics, you need to set `always_scrape_classic_histograms` to `true` in your scrape jobs.

   {{< admonition type="note" >}}
   In Prometheus versions earlier than 3.0, the `always_scrape_classic_histograms` setting is called `scrape_classic_histograms`. Use the setting name that corresponds to the version of Prometheus you're using.
   {{< /admonition >}}

   For example, to get both classic and native histograms, use the following configuration:

   ```yaml
   scrape_configs:
     - job_name: myapp
       convert_histograms_to_nhcb: true
       always_scrape_classic_histograms: true
   ```

1. To be able to send native histograms to a Prometheus remote-write 2.0 compatible receiver, for example Grafana Cloud Metrics, Mimir, etc, set `send_native_histograms` to `true` and `protobuf_message` to `io.prometheus.write.v2.Request` in the remote-write configuration, for example:

   ```yaml
   remote_write:
     - url: http://.../api/prom/push
       send_native_histograms: true
       protobuf_message: "io.prometheus.write.v2.Request"
   ```

   {{< admonition type="note" >}}
   Prometheus remote-write 2.0 is experimental, for more information see the [Prometheus Remote-Write 2.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/).
   {{< /admonition >}}

   {{< admonition type="note" >}}
   Prometheus remote-write 2.0 doesn't fully support [metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) currently.
   {{< /admonition >}}

## Migrate from classic histograms

To ease the migration process, you can keep the custom bucket definition of a classic histogram and add native histogram buckets at the same time.

1. Let Prometheus scrape both classic and NHCB.
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
1. It is important to keep scraping both classic and native histograms for at least the period of the longest range in your recording rules and alerts, plus one day. This is the minimum amount of time, but it's recommended to keep scraping both sample types until the new rules and alerts can be verified.

   For example, if you have an alert that calculates the rate of requests, such as `sum(rate(http_request_duration_seconds[7d]))`, this query looks at the data from the last seven days plus the Prometheus [lookback period](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness). When you start sending native histograms, the data isn't there for the entire seven days, and therefore, the results might be unreliable for alerting.

1. After configuring native histogram collection, choose one of the following ways to stop collecting classic histograms.

   - Drop the classic histogram series with [Prometheus relabeling](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) or [Grafana Alloy prometheus.relabel](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.relabel) at the time of scraping.
   - Stop scraping the classic histogram version of metrics. This option applies to all metrics of a scrape target.

1. Clean up recording rules and alerts by deleting the classic histogram version of the rule or alert.

## Limit the number of buckets

There is no way to automatically reduce the resolution or merge custom buckets like with [native histogram with exponential buckets](https://grafana.com/docs/mimir/<MIMIR_VERSION>/send/native-histograms/_exponential_buckets#limit-the-number-buckets). Set bucket limits greater than the largest expected bucket count to avoid having NHCB rejected.
