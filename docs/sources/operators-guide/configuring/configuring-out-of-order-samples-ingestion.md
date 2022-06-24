---
title: "Configuring out-of-order samples ingestion"
menuTitle: "Configuring out-of-order samples ingestion"
description: "Learn how to configure Grafana Mimir to handle out-of-order samples ingestion."
weight: 120
---

# Configuring out-of-order samples ingestion

Grafana Mimir and the Prometheus TSDB understand out-of-order as follows.

The moment that a new series sample arrives, Mimir need to determine if the series already exists, and whether or not the sample is too old:

- If the series exists, incoming sample must have newer timestamp than the latest sample that is stored for the series.
  Otherwise, it is considered out-of-order and will be dropped by the ingesters.
- If the series does not exist, then the sample has to be within bounds, which go back 1 hour from TSDB's head-block max time (when using 2 hour block range). If it fails to be within bounds, then it is also considered out-of-order and will be dropped by the ingesters.

If you have out-of-order samples due to the nature of your architecture or the system that is being observed, then you can configure Grafana Mimir to set an out-of-order time-window threshold for how old samples can be ingested.
As a result, none of the preceding samples will be dropped if they are within the time window that
you configured.

## Configuring out-of-order samples ingestion instance wide

To configure Grafana Mimir to accept out-of-order samples, see the following configuration snippet:

<!-- prettier-ignore-start -->
```yaml
limits:
  # Allow ingestion of out-of-order samples up to 5 minutes since the latest received sample for the series.
  out_of_order_time_window: 5m
```
<!-- prettier-ignore-end -->

## Configure out-of-order samples per tenant

If your Grafana Mimir has multitenancy enabled, you can still use the preceding method to set a default out-of-order time window threshold for all tenants. If a particular tenant needs a higher threshold, Grafana Mimir can also do that.

1. Enable runtime configuration.
   For more information, see [About runtime configuration]({{< relref "about-runtime-configuration.md" >}}).
1. Write down an override for the tenant that needs a bigger out-of-order time window:

<!-- prettier-ignore-start -->
```yaml
overrides:
  tenant1:
    out_of_order_time_window: 2h
  tenant2:
    out_of_order_time_window: 30m
```
<!-- prettier-ignore-end -->
