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

To illustrate this, assume that two custom trackers are configured as in the preceding YAML snippet, and that your Grafana Mimir cluster has two tenants: `tenant_1` and `tenant_with_only_prod_metrics`. Assume that `tenant_with_only_prod_metrics` has three series with labels that match the pattern `{namespace=~"prod-.*"}` and none that match the patten `{namespace=~"dev-.*"}`. Also assume that `tenant_1` has five series that match the pattern `{namespace=~"dev-.*"}` and 10 series that match the pattern `{namespace=~"prod-.*"}`.

In this example, the following output appears when the `/metrics` endpoint for the ingester component is scraped:

```
cortex_ingester_active_series_custom_tracker{name="dev", user="tenant_1"}                         5
cortex_ingester_active_series_custom_tracker{name="prod", user="tenant_1"}                       10
cortex_ingester_active_series_custom_tracker{name="prod", user="tenant_with_only_prod_metrics"}   3
```

Starting with Mimir version 2.1 default configuration as described above can be overridden for specific tenants in the [runtime configuration]({{< relref "./about-runtime-configuration.md" >}}).

The example below shows how you could override the active series custom trackers configuration for tenant `tenant_with_only_prod_metrics` to track two interesting services instead of the default matchers.

```
overrides:
  tenant_with_only_prod_metrics:
    active_series_custom_trackers:
      interesting-service: '{service=~"interesting-.*"}'
      also-interesting-service: '{service=~"also-interesting-.*"}'
```

After adding this override, and assuming there are 1 and 2 matching series for `interesting-service` and `also-interesting-service`, respectively, the output at `/metrics` would change to:

```
cortex_ingester_active_series_custom_tracker{name="dev", user="tenant_1"}                                           5
cortex_ingester_active_series_custom_tracker{name="prod", user="tenant_1"}                                         10
cortex_ingester_active_series_custom_tracker{name="interesting-service", user="tenant_with_only_prod_metrics"}      1
cortex_ingester_active_series_custom_tracker{name="also-interesting-service", user="tenant_with_only_prod_metrics"} 2
```

For detailed information how to set up runtime overrides, refer to [runtime configuration]({{< relref "./about-runtime-configuration.md" >}}).

> **Note:** The custom active series trackers are exposed on each ingester. To understand the count of active series matching a particular label pattern in your Grafana Mimir cluster at a global level, you must collect and sum this metric across all ingesters. If you're running Grafana Mimir with a `replication_factor` > 1, you must also adjust for the fact that the same series will be replicated `RF` times across your ingesters.
