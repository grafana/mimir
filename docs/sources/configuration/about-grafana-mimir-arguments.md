---
title: "About command-line arguments"
description: ""
weight: 10
---

# About Grafana Mimir command-line arguments

Configuration may be specified within a YAML file or as command-line options.

Command-line options specify the component that the configuration targets
and the configuration parameter.
The syntax follows the parameter with an equals character (`=`) and the value of the parameter.

Specify duration values with a unit, such as `5s` or `3h`. The valid time units are `ms`, `s`, `m`, or `h`.

## Querier

- `-querier.max-concurrent`

  The maximum number of top-level PromQL queries that will execute at the same time, per querier process.
  If using the query-frontend or query-scheduler, this also sets the number of query execution workers (running inside the a querier process) which will connect to the query-frontend or query-scheduler.

- `-querier.timeout`

  The timeout for a top-level PromQL query.

- `-querier.max-samples`

  The maximum number of samples a single query can load into memory, to avoid issues with enormous queries.

These three options only apply when the querier is used together with the query-frontend or query scheduler:

- `-querier.frontend-address`

  Address of the query-frontend service. Used by workers to find the frontend that will give them queries to execute.

- `-querier.scheduler-address`

  Address of query scheduler service. Used by workers to find the scheduler that will give them queries to execute. If set, `-querier.frontend-address` is ignored, and querier will use query scheduler.

- `-querier.dns-lookup-period`

  How often workers will query DNS to check the address of the query-frontend or query scheduler.

## Querier and ruler

The ingester query API was improved over time, but defaults to the old behaviour for backward compatibility. For best results both of these first two flags should be set to true:

- `-querier.batch-iterators`

  This uses iterators to execute query, as opposed to fully materialising the series in memory, and fetches multiple results per loop.

- `-querier.iterators`

  This is similar to `-querier.batch-iterators`, but less efficient.
  If both `iterators` and `batch-iterators` are true, `batch-iterators` will take precedence.

- `-promql.lookback-delta`

  Time since the last sample after which a time series is considered stale and ignored by expression evaluations.

## Query frontend

- `-query-frontend.parallelize-shardable-queries`

  If set to true, will cause the query frontend to mutate incoming queries when possible by turning `sum` operations into sharded `sum` operations. An abridged example:

  ```
  sum by (foo) (rate(bar{baz=”blip”}[1m]))
  ```

  becomes

  ```
  sum by (foo) (
   sum by (foo) (rate(bar{baz=”blip”,__query_shard__=”0of16”}[1m])) or
   sum by (foo) (rate(bar{baz=”blip”,__query_shard__=”1of16”}[1m])) or
   ...
   sum by (foo) (rate(bar{baz=”blip”,__query_shard__=”15of16”}[1m]))
  )
  ```

  We advise increasing downstream concurrency controls as well, to account for more queries of smaller sizes:

  - `querier.max-outstanding-requests-per-tenant`
  - `querier.max-query-parallelism`
  - `querier.max-concurrent`
  - `server.grpc-max-concurrent-streams` (for both query-frontends and queriers)

  Instrumentation (traces) also scale with the number of sharded queries and we advise you to account for increased throughput there as well, for instance with `JAEGER_REPORTER_MAX_QUEUE_SIZE`.

- `-query-frontend.align-querier-with-step`

  If set to true, causes the query-frontend to mutate incoming queries and align their start and end parameters to the step parameter of the query. This improves the cacheability of the query results.

- `-querier.split-queries-by-day`

  If set to true, causes the query-frontend to split multi-day queries into multiple single-day queries and execute them in parallel.

- `-query-frontend.cache-results`

  If set to true, causes the querier to cache query results. The cache will be used to answer future, overlapping queries. The query-frontend calculates extra queries required to fill gaps in the cache.

- `-query-frontend.max-cache-freshness`

  When caching query results, it is desirable to prevent the caching of very recent results that might still be in flux. Use this parameter to configure the age of results that should be excluded.

- `-query-frontend.results-cache.backend`

  Configures the caching backend used when query results caching is enabled (`-query-frontend.cache-results=true`).

- `-query-frontend.results-cache.memcached.addresses`

  Comma-separated list of Memcached addresses. Each address can be specified using the [DNS service discovery](#dns-service-discovery) syntax.

## Distributor

- `distributor.drop-label`
  Specifies label names to drop during sample ingestion within the distributor. Can be repeated in order to drop multiple labels.

## Runtime configuration

A runtime configuration file is a file containing configuration, which is periodically reloaded while Mimir is running. It allows you to change a subset of Grafana Mimir’s configuration without having to restart Grafana Mimir.

The runtime configuration file allows you to dynamically modify only a subset of the configuration that was set at startup. As an operator Grafana Mimir, you can see the subset of configuration that is of most value to you, and you can use that configuration information to make immediate adjustments to Grafana Mimir.

Runtime configuration values take precedence over command-line options.

## Enable runtime configuration

To enable runtime configuration, specify a path to the file upon startup by using the `-runtime-config.file=<filepath>` CLI flag or from within your YAML configuration file in the `runtime_config` block.

By default, Grafana Mimir reloads the contents of this file every 10 seconds. You can configure this interval by using the `-runtime-config.reload-period=<duration>` CLI flag or by specifying the `period` value in your YAML configuration file.

When running Grafana Mimir on Kubernetes, store the runtime configuration file in a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) and mount it in each container.

## Viewing the runtime configuration

Use Grafana Mimir’s `/runtime_config` endpoint to see the current value of the runtime configuration, including the overrides. To see only the non-default values of the configuration, specify the endpoint with `/runtime_config?mode=diff`.

## Ingester limits

Mimir implements various limits on the requests it can process, in order to prevent a single tenant from overwhelming the cluster. There are various default global limits that apply to all tenants which can be set on the command line. These limits can also be overridden on a per-tenant basis by using the `overrides` field of a runtime configuration file.

The `overrides` field is a map of tenant ID (same values as passed in the `X-Scope-OrgID` header) to the various limits. An example could look like:

```yaml
overrides:
  tenant1:
    ingestion_rate: 10000
```

Valid per-tenant limits are (with their corresponding CLI flags for default values):

- `max_global_series_per_user` / `-ingester.max-global-series-per-user`
- `max_global_series_per_metric` / `-ingester.max-global-series-per-metric`

  Enforced by the ingesters; limits the number of in-memory series a tenant (or a given metric) can have. A series is kept in memory if a sample has been written since the last TSDB head compaction (occurring every 2h) or in the last 1h (regardless when the last TSDB head compaction occurred). The limit is enforced across the cluster. Each ingester is configured with a local limit based on the replication factor and the current number of healthy ingesters. The local limit is updated whenever the number of ingesters change.

  Requires that `-ingester.ring.replication-factor` and `-ingester.ring.zone-awareness-enabled` are set for the ingesters.

- `max_global_metadata_per_user` / `-ingester.max-global-metadata-per-user`
- `max_global_metadata_per_metric` / `-ingester.max-global-metadata-per-metric`

  Enforced by the ingesters; limits the number of active metadata a tenant (or a given metric) can have. The limit is enforced across the cluster. Each ingester is configured with a local limit based on the replication factor and the current number of healthy ingesters. The local limit is updated whenever the number of ingesters change.

  Requires that `-ingester.ring.replication-factor` and `-ingester.ring.zone-awareness-enabled` are set for the ingesters.

- `max_fetched_series_per_query` / `querier.max-fetched-series-per-query`
  This limit is enforced in the queriers on unique series fetched from ingesters and store-gateways (long-term storage).

### Runtime configuration of per-tenant limits

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

- On a per-tenant basis, you can override all of the limits in `limits_config` within the runtime configuration file.
- For each tenant, you can override different limits.
- For any tenant or limit that is not overridden in the runtime configuration file, you can inherit the limit values that are specified in `limits_config`.

## Ingester instance limits

Grafana Mimir ingesters support limits that are applied per instance, meaning that they apply to each ingester process. These limits can be used to ensure individual ingesters are not overwhelmed regardless of any per-tenant limits. These limits can be set under the `ingester.instance_limits` block in the global configuration file, with CLI flags, or under the `ingester_limits` field in the runtime configuration file.

Valid ingester instance limits are (with their corresponding flags):

- `max_ingestion_rate` \ `--ingester.instance-limits.max-ingestion-rate`

  Limit the ingestion rate in samples per second for an ingester. When this limit is reached, new requests will fail with an HTTP 500 error.

- `max_series` \ `-ingester.instance-limits.max-series`

  Limit the total number of series that an ingester keeps in memory, across all users. When this limit is reached, requests that create new series will fail with an HTTP 500 error.

- `max_tenants` \ `-ingester.instance-limits.max-tenants`

  Limit the maximum number of users an ingester will accept metrics for. When this limit is reached, requests from new users will fail with an HTTP 500 error.

- `max_inflight_push_requests` \ `-ingester.instance-limits.max-inflight-push-requests`

  Limit the maximum number of concurrent requests being handled by an ingester. This setting is critical for preventing ingesters from using an excessive amount of memory during high load or temporary slow downs. When this limit is reached, new requests will fail with an HTTP 500 error.

### Runtime configuration of ingester limits

The runtime configuration file can be used to dynamically adjust ingester instance limits. While per-tenant limits are limits applied to each tenant, per-ingester-instance limits are limits applied to each ingester process.

These limits are set at startup in the `ingester` block in the YAML configuration file or by the corresponding CLI flag.

The runtime configuration allows you to override these initial values, which is useful for advanced operators who need to dynamically change them in response to changes in ingest or query load.

Everything under the `instance_limits` section within the `ingester` block can be overridden via runtime configuration. Here is an example portion of runtime configuration that changes the ingester limits:

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

The command-line option `-ingester.stream_chunks_when_using_blocks` may only be used in runtime configuration.
A value of true transfers encoded chunks,
and a value of false transfers decoded series.

We strongly recommend against using the runtime configuration to set this value. It already defaults to true, and should remain true except for rare corner cases where users have observed slowdowns in Grafana Mimir rules evaluation.

## DNS service discovery

Some clients in Grafana Mimir support service discovery via DNS to find the addresses of backend (caching) servers to connect to. These clients support service discovery via DNS:

- [Blocks storage's memcached cache](../reference-configuration-parameters/#blocks_storage)
- [All caching memcached servers](../reference-configuration-parameters/#memcached)
- [Memberlist KV store](..//reference-configuration-parameters#memberlist)

### Supported discovery modes

The DNS service discovery, inspired by Thanos DNS SD, supports different discovery modes. A discovery mode is selected adding a specific prefix to the address. Supported prefixes are:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`.
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+_memcached._tcp.memcached.namespace.svc.cluster.local`.
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+_memcached._tcp.memcached.namespace.svc.cluster.local`.

If no prefix is provided, the provided IP or hostname will be used without pre-resolving it.

## IP address logging of a reverse proxy

If a reverse proxy is used in front of Mimir, it may be difficult to troubleshoot errors. The following settings can be used to log the IP address passed along by the reverse proxy in headers such as `X-Forwarded-For`.

- `-server.log_source_ips_enabled`

  Set this to true to add IP address logging when a `Forwarded`, `X-Real-IP` or `X-Forwarded-For` header is used. A field called `sourceIPs` will be added to error logs when data is pushed into Grafana Mimir.

- `-server.log-source-ips-header`

  Header field storing the source IP addresses. It is only used if `-server.log-source-ips-enabled` is true, and if `-server.log-source-ips-regex` is set. If not set, the default `Forwarded`, `X-Real-IP` or `X-Forwarded-For` headers are searched.

- `-server.log-source-ips-regex`

  Regular expression for matching the source IPs. It should contain at least one capturing group the first of which will be returned. Only used if `-server.log-source-ips-enabled` is true and if `-server.log-source-ips-header` is set. If not set the default Forwarded, X-Real-IP or X-Forwarded-For headers are searched.
