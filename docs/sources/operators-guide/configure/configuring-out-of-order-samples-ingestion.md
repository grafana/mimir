---
aliases:
  - /docs/mimir/latest/operators-guide/configuring/configuring-out-of-order-samples-ingestion/
description: Learn how to configure Grafana Mimir to handle out-of-order samples ingestion.
menuTitle: Configuring out-of-order samples ingestion
title: Configuring out-of-order samples ingestion
weight: 120
---

# Configuring out-of-order samples ingestion

Grafana Mimir and the Prometheus TSDB understand out-of-order as follows.

The moment that a new series sample arrives, Mimir need to determine if the series already exists, and whether or not the sample is too old:

- If the series exists, the incoming sample must have a newer timestamp than the latest sample that is stored for the series.
  Otherwise, it is considered out-of-order and will be dropped by the ingesters.
- If the series does not exist, then the sample has to be within bounds, which go back 1 hour from TSDB's head-block max time (when using 2 hour block range). If it fails to be within bounds, then it is also considered out-of-bounds and will be dropped by the ingesters.

> **Note:** If you're writing metrics using Prometheus remote write or the Grafana Agent, then out-of-order samples are unexpected.
> Prometheus and Grafana Agent guarantee that samples are written in-order for the same series.

If you have out-of-order samples due to the nature of your architecture or the system that is being observed, then you can configure Grafana Mimir to set an out-of-order time-window threshold for how old samples can be ingested.
As a result, none of the preceding samples will be dropped if they are within the configured time window.

## Configuring out-of-order samples ingestion instance wide

To configure Grafana Mimir to accept out-of-order samples, see the following configuration snippet:

```yaml
limits:
  # Allow ingestion of out-of-order samples up to 5 minutes since the latest received sample for the series.
  out_of_order_time_window: 5m
```

## Configure out-of-order samples per tenant

If your Grafana Mimir has multitenancy enabled, you can still use the preceding method to set a default out-of-order time window threshold for all tenants.
If a particular tenant needs a custom threshold, you can use the runtime configuration to set a per-tenant override.

1. Enable [runtime configuration]({{< relref "about-runtime-configuration.md" >}}).
1. Add an override for the tenant that needs a custom out-of-order time window:

```yaml
overrides:
  tenant1:
    out_of_order_time_window: 2h
  tenant2:
    out_of_order_time_window: 30m
```

Setting `out_of_order_time_window` to `0s` disables the out-of-order ingestion while you can still continue to query the out-of-order samples ingested till now.

## Query caching with out-of-order ingestion enabled

Once a query has been cached, out-of-order samples that get ingested later can potentially change those query results.

To avoid caching queries that can get outdated, you can set `-query-frontend.max-cache-freshness` to match the `out_of_order_time_window` so that you don't cache queries
for the time window where you still expect samples to arrive. Doing so can increase the load on your Mimir cluster depending on query characteristics.

## Recording rules when out-of-order ingestion is enabled

Similar to the problem above with query caching, the samples recorded via the recording rules can get outdated with new out-of-order samples being ingested.
So you should expect some difference in results if you happen to run the raw query of the recording rule. The difference highly depends on your out-of-order ingestion pattern.

If you happen to have a shorter `out_of_order_time_window`, say less than 10 minutes, then you can use `-ruler.evaluation-delay-duration` to delay your rule evaluation up to that time.
