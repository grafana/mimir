---
title: "Configuring out-of-order samples ingestion"
menuTitle: "Configuring out-of-order samples ingestion"
description: "Learn how to configure Grafana Mimir to handle out-of-order samples ingestion."
weight: 120
---

# Configuring out-of-order samples ingestion

Before we step into out-of-order samples ingestion details, let's clarify what Grafana Mimir and the Prometheus TSDB understand as out-of-order.

The moment a new series sample arrives we need to determine if the series already existed or not and whether or not the sample is too old.

- If the series existed it has to be newer than the latest sample stored otherwise it is considered out-of-order and will be dropped by the ingesters.
- If the series did not exist then it has to be newer than TSDB's head block max time. If its not then it is also considered out-of-order and will be dropped by the ingesters.

If you happen to have out-of-order samples due to the nature of your architecture or the system that is being observed, then you can configure Grafana Mimir to set time window threshold for how old samples can be ingested and none of the samples above will be dropped if they are within the time window you configured.

## Configuring out-of-order samples ingestion instance wide

To configure Grafana Mimir to accept out-of-order samples, see the following configuration snippet:

<!-- prettier-ignore-start -->
```yml
limits:
  # The time window is expressed as a duration so this is how you could set it to 5 minutes
  out_of_order_time_window: 5m
```
<!-- prettier-ignore-end -->

## Configuring out-of-order samples per tenant

If your Grafana Mimir has multitenancy enabled you could still use the method above to set a default out-of-order time window threshold for all tenants. Then if a particular tenant had the needs to increase it, Grafana Mimir can also do that.

1. Enable runtime configuration, for more information check [About runtime configuration]({{< relref "about-runtime-configuration.md" >}})
1. Write down an override for the tenant that needs a bigger out-of-order time window.
<!-- prettier-ignore-start -->

```yml
overrides:
  tenant1:
    out_of_order_time_window: 2h
  tenant2:
    out_of_order_time_window: 30m
```

<!-- prettier-ignore-end -->
