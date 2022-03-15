---
title: "Configuring custom trackers"
menuTitle: "Configuring custom trackers"
description: "Use the custom tracker to count the number of active series on an ingester."
weight: 55
---

# Configuring custom trackers

You can use the custom tracker feature to count the number of active series on an ingester that match a particular label pattern.

The label pattern to match against is specified using the `-ingester.active-series-custom-trackers` CLI flag (or its respective YAML configuration option). Each custom tracker is defined as a key-value pair, where the key is the name of the tracker and the value is the label matcher. Both the key and the value are type `<string>`.

The following example configures a custom tracker to count the active series coming from `dev` and `prod` namespaces for each tenant.

```yaml
active_series_custom_trackers:
  dev: '{namespace=~"dev-.*"}'
  prod: '{namespace=~"prod-.*"}'
```

If you configure a custom tracker for an ingester, the ingester exposes a `cortex_ingester_active_series_custom_tracker` gauge metric on its [/metrics endpoint](({{< relref "../reference-http-api#metrics" >}})). Each custom tracker is a time series and each series has a `name` label applied to it. The value of the `name` label is the name of the custom tracker.

When two custom trackers are configured in the example above, the following output is generated after the `/metrics` endpoint for the ingester component is scraped:

```yaml
cortex_ingester_active_series_custom_tracker{name=dev}
cortex_ingester_active_series_custom_tracker{name=prod}
```
