---
title: "Configuring custom trackers"
menuTitle: "Configuring custom trackers"
description: ""
weight: 55
---

# Configuring custom trackers

The custom tracker feature can be used to count the number of active series on an ingester that match a particular label pattern. 

The label pattern to match against is specified using the `-ingester.active-series-custom-trackers` CLI flag (or its respective YAML configuration option). Each custom tracker is defined as a key-value pair, where the key is the name of the tracker and the value is the label matcher. Both the key and the value are type `<string>`. 

To illustrate via example, the following configuration will configure a custom tracker to count the active series coming from `dev` and `prod` namespaces for each tenant. 

```yaml
active_series_custom_trackers:
       dev: '{namespace=~"dev-.*"}'
       prod: '{namespace=~"prod-.*"}'
```

If custom trackers are configured for an ingester, the ingester will expose a a gauge metric called on `cortex_ingester_active_series_custom_tracker` on its [/metrics endpoint](({{< relref "../reference-http-api#metrics" >}})). Each custom tracker is its own time series. Each series has `name` label applied to it that has the name given to the custom tracker.

If you had configured the two custom trackers given in the example above, you would expect to see 
```yaml
cortex_ingester_active_series_custom_tracker{name=dev}
cortex_ingester_active_series_custom_tracker{name=prod}
```
when you scraped the `/metrics` endpoint for the ingester component. 