---
title: "About runtime configuration"
description: ""
weight: 10
---

# About runtime configuration

A runtime configuration file is a file containing configuration, which is periodically reloaded while Mimir is running. It allows you to change a subset of Grafana Mimir’s configuration without having to restart the Grafana Mimir component or instance.

Runtime configuration is available for a subset of the configuration that was set at startup. A Grafana Mimir operator can observe the configuration and use runtime configuration to make immediate adjustments to Grafana Mimir.

Runtime configuration values take precedence over command-line options.

## Enable runtime configuration

To enable runtime configuration, specify a path to the file upon startup by using the `-runtime-config.file=<filepath>` CLI flag or from within your YAML configuration file in the `runtime_config` block.

By default, Grafana Mimir reloads the contents of this file every 10 seconds. You can configure this interval by using the `-runtime-config.reload-period=<duration>` CLI flag or by specifying the `period` value in your YAML configuration file.

When running Grafana Mimir on Kubernetes, store the runtime configuration file in a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) and mount it in each container.

## Viewing the runtime configuration

Use Grafana Mimir’s `/runtime_config` endpoint to see the current value of the runtime configuration, including the overrides. To see only the non-default values of the configuration, specify the endpoint with `/runtime_config?mode=diff`.

## Runtime configuration of per-tenant limits

The primary use case for the runtime configuration file is that it allows you to set and adjust limits for each tenant in Grafana Mimir. Doing so lets you set limits that are appropriate for each tenant based on their ingest and query needs.

The values that are defined in the limits section of your YAML configuration define the default set of limits that are applied to tenants. For example, if you set the `ingestion_rate` to 25,000 in your YAML configuration file, any tenant in your cluster that is sending more than 25,000 samples per second (SPS) will be rate limited.

You can use the runtime configuration file to override this behavior. For example, if you have a tenant (`tenant1`) that needs to send twice as many data points as the current limit, and you have another tenant (`tenant2`) that needs to send three times as many data points, you can modify the contents of your runtime configuration file:

```yaml
overrides:
  tenant1:
    ingestion_rate: 50000
  tenant2:
    ingestion_rate: 75000
```

As a result, Grafana Mimir allows `tenant1` to send 50,000 SPS, and `tenant2` to send 75,000 SPS, while maintaining a 25,000 SPS rate limit on all other tenants.

- On a per-tenant basis, you can override all of the limits listed in the [`limits`]({{< relref "reference-configuration-parameters/#limits" >}}) block within the runtime configuration file.
- For each tenant, you can override different limits.
- For any tenant or limit that is not overridden in the runtime configuration file, you can inherit the limit values that are specified in the `limits` block.

## Ingester instance limits

Grafana Mimir ingesters support limits that are applied per instance, meaning that they apply to each ingester process. These limits can be used to ensure individual ingesters are not overwhelmed regardless of any per-tenant limits. These limits can be set under the `ingester.instance_limits` block in the global configuration file, with CLI flags, or under the `ingester_limits` field in the runtime configuration file.

The runtime configuration file can be used to dynamically adjust ingester instance limits. While per-tenant limits are limits applied to each tenant, per-ingester-instance limits are limits applied to each ingester process.

The runtime configuration allows you to override initial values, which is useful for advanced operators who need to dynamically change them in response to changes in ingest or query load.

Everything under the `instance_limits` section within the [`ingester`]({{< relref "reference-configuration-parameters/#ingester" >}}) block can be overridden via runtime configuration. Here is an example portion of runtime configuration that changes the ingester limits:

```yaml
ingester_limits:
  max_ingestion_rate: 20000
  max_series: 1500000
  max_tenants: 1000
  max_inflight_push_requests: 30000
```

## Runtime configuration of ingester streaming

An advanced runtime configuration
controls whether ingesters transfer encoded chunks (the default) or transfer decoded series to queriers at query time.

The parameter `ingester_stream_chunks_when_using_blocks` may only be used in runtime configuration.
A value of true transfers encoded chunks,
and a value of false transfers decoded series.

We strongly recommend against changing the default setting. It already defaults to true, and should remain true except for rare corner cases where users have observed slowdowns in Grafana Mimir rules evaluation.
