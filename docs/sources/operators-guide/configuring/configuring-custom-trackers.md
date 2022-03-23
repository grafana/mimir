---
title: "Configuring custom active series trackers"
menuTitle: "Configuring custom active series trackers"
description: "Use the custom tracker to count the number of active series on an ingester."
weight: 55
---

# Configuring custom active series trackers

You can use the custom tracker feature to count the number of active series on an ingester that match a particular label pattern.

The label pattern to match against is specified using the `-ingester.active-series-custom-trackers` CLI flag (or its respective YAML configuration option). Each custom tracker is defined as a key-value pair, where the key is the name of the tracker and the value is the label matcher. Both the key and the value are type `<string>`.

The following example configures a custom tracker to count the active series coming from `dev` and `prod` namespaces for each tenant.

```yaml
active_series_custom_trackers:
  dev: '{namespace=~"dev-.*"}'
  prod: '{namespace=~"prod-.*"}'
```

If you configure a custom tracker for an ingester, the ingester exposes a `cortex_ingester_active_series_custom_tracker` gauge metric on its [/metrics endpoint]({{< relref "../reference-http-api/index.md#metrics" >}}).

Each custom tracker counts the active series matching its label pattern on a per-tenant basis, which means that each custom tracker generates as many as `# of tenants` series with metric name `cortex_ingester_active_series_custom_tracker`. To reduce the cardinality of this metric, only custom trackers that have matched at least one series are exposed on the metric, and they are removed if they become `0`.

Series with metric name `cortex_ingester_active_series_custom_tracker` have two labels applied: `name` and `user`. The value of the `name` label is the name of the custom tracker specified in the configuration. The value of the `user` label is the tenant-id for which the series count applies.

Assume two custom trackers are configured as in the example above, and that your Grafana Mimir cluster has three tenants: `tenant_1`, `tenant_2`, and `tenant_with_only_prod_metrics`. Assume all series within `tenant_with_only_prod_metrics` have labels that match the pattern `{namespace=~"prod-.*"}` and none that match `{namespace=~"dev-.*"}`.

In this example, the following output appears when the `/metrics` endpoint for the ingester component is scraped:

```
cortex_ingester_active_series_custom_tracker{name="dev", user="tenant_1"}
cortex_ingester_active_series_custom_tracker{name="prod", user="tenant_2"}
cortex_ingester_active_series_custom_tracker{name="prod", user="tenant_with_only_prod_metrics"}
```

> **Note:** The custom active series trackers are exposed on each ingester. To understand the count of active series matching a particular label pattern in your Grafana Mimir cluster at a global level, you must collect and sum this metric across all ingesters. If you're running Grafana Mimir with a `replication_factor` > 1, you must also adjust for the fact that the same series will be replicated `RF` times across your ingesters.
