---
title: "Configuring shuffle sharding"
description: "How to configure shuffle sharding."
weight: 30
---

# Configuring shuffle sharding

Grafana Mimir leverages sharding techniques to horizontally scale both single- and multi-tenant clusters beyond the capacity of a single node.

## Background

Grafana Mimir uses a sharding strategy that distributes the workload across a subset of the instances that run a given component. For example, on the write path, each tenant's series are sharded across a subset of the ingesters. The size of this subset, which is the number of instances, is configured using the "shard size" parameter, whose default value is `0`. This default value means that each tenant uses all available instances, in order to fairly balance resources, such as CPU and memory usage, and to maximize the usage of these resources across the cluster.

Note: In a multi-tenant cluster this default (`0`) value introduces some downsides:

- An outage affects all tenants.
- A misbehaving tenant, which for example causes an out-of-memory error, could negatively affect all other tenants.

Configuring a shard size value higher than zero enables shuffle sharding. The goal of shuffle sharding is to reduce the blast radius of an outage and better isolate tenants.

## What is shuffle sharding

Shuffle sharding is a technique that isolates different tenant's workloads and gives each tenant a single-tenant experience even if they're running in a shared cluster. For details, see how AWS answers the question [What is shuffle sharding?](https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/).

The idea is to assign each tenant a shard that is composed of a subset of the Grafana Mimir instances, which minimizes the number of overlapping instances between two different tenants. Shuffle sharding has the following benefits:

- An outage on some Grafana Mimir cluster instances or nodes will only affect a subset of tenants.
- A misbehaving tenant only affects its shard instances. Assuming each tenant shard is relatively small compared to the total number of instances in the cluster, it’s statistically likely that any other tenant will run on different instances or that only a subset of instances will match the affected ones.

Using shuffle sharding doesn’t require more resources, but instances will not be evenly balanced.

### Low overlapping instances probability

For example, given that a Grafana Mimir cluster that is running 50 ingesters and assigning each tenant 4 out of 50 ingesters, by shuffling instances between each tenant, there are 230K possible combinations.

Randomly picking two different tenants we have the:

- 71% chance that they will not share any instance
- 26% chance that they will share only 1 instance
- 2.7% chance that they will share 2 instances
- 0.08% chance that they will share 3 instances
- Only a 0.0004% chance that their instances will fully overlap

![Shuffle sharding probability](/images/guides/shuffle-sharding-probability.png)

<!-- Chart source at https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit -->

## Grafana Mimir shuffle sharding

Grafana Mimir supports shuffle sharding in the following services:

- [Ingesters](#ingesters-shuffle-sharding)
- [Query-frontend / Query-scheduler](#query-frontend-and-query-scheduler-shuffle-sharding)
- [Store-gateway](#store-gateway-shuffle-sharding)
- [Ruler](#ruler-shuffle-sharding)
- [Compactor](#compactor-shuffle-sharding)

When running Grafana Mimir with the default configuration, shuffle sharding is disabled and you need to explicitly enable it by increasing the shard size either globally or for a given tenant.

> **Note:** If the shard size value is equal to or higher than the number of available instances, for example where `-distributor.ingestion-tenant-shard-size` is higher than the number of ingesters, then shuffle sharding is disabled and all instances are used again.

### Guaranteed properties

The Grafana Mimir shuffle sharding implementation provides the following benefits:

- **Stability**<br />
  Given a consistent state of the hash ring, the shuffle sharding algorithm always selects the same instances for a given tenant, even across different machines.
- **Consistency**<br />
  Adding or removing one instance from the hash ring leads to only one instance changed at most, in each tenant's shard.
- **Shuffling**<br />
  Probabilistically and for a large enough cluster, it ensures that every tenant gets a different set of instances, with a reduced number of overlapping instances between two tenants to improve failure isolation.
- **Zone-awareness**<br />
  When [zone-aware replication](../guides/zone-replication.md) is enabled, the subset of instances selected for each tenant contains a balanced number of instances for each availability zone.

### Ingesters shuffle sharding

By default, the Grafana Mimir distributor spreads the received series across all running ingesters.

When shuffle sharding is enabled for the ingesters, the distributor and ruler on the write path spread each tenant series across `-distributor.ingestion-tenant-shard-size` number of ingesters, while on the read path the querier and ruler queries only the subset of ingesters holding the series for a given tenant.

_The shard size can be overridden on a per-tenant basis by setting `ingestion_tenant_shard_size` in the overrides section of the runtime configuration._

#### Ingesters write path

To enable shuffle sharding for ingesters on the write path you need to configure the following CLI flags (or their respective YAML config options) on the distributor, ingester and ruler:

- `-distributor.ingestion-tenant-shard-size=<size>`<br />
  `<size>` set to the number of ingesters each tenant series should be sharded to. If `<size>` is zero or greater than the number of available ingesters in the Grafana Mimir cluster, the tenant series are sharded across all ingesters.

#### Ingesters read path

Assuming shuffle sharding has been enabled for the write path, to enable shuffle sharding for ingesters on the read path too you need to configure the following CLI flags (or their respective YAML config options) on the querier and ruler:

- `-distributor.ingestion-tenant-shard-size=<size>`
- `-querier.shuffle-sharding-ingesters-lookback-period=<period>`<br />
  Queriers and rulers fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since 'now - lookback period'. The configured lookback `<period>` should be greater or equal than `-querier.query-store-after` and `-querier.query-ingesters-within` if set, and greater than the estimated minimum time it takes for the oldest samples stored in a block uploaded by ingester to be discovered and available for querying. When running Grafana Mimir with the default configuration, the estimated minimum time it takes for the oldest sample in a uploaded block to be available for querying is 3h.

In case ingesters shuffle sharding is enabled only for the write path, queriers and rulers on the read path will always query all ingesters instead of querying the subset of ingesters belonging to the tenant's shard. Keeping ingesters shuffle sharding enabled only on write path does not lead to incorrect query results, but may increase query latency.

#### Rollout strategy

If you’re running a Grafana Mimir cluster with shuffle sharding disabled, and you want to enable it for the ingesters, use the following rollout strategy to avoid missing querying any time series in the ingesters:

1. Enable ingesters shuffle sharding on the **write path**
2. **Wait** at least `-querier.shuffle-sharding-ingesters-lookback-period` time
3. Enable ingesters shuffle-sharding on the **read path**

#### Limitation: decreasing the tenant shard size

The current shuffle sharding implementation in Grafana Mimir has a limitation that prevents you from safely decreasing the tenant shard size if the ingesters’ shuffle sharding is enabled on the read path.

The problem is that if a tenant’s shard decreases in size, there is currently no way for the queriers and rulers to know how big the tenant shard was previously, and hence they will potentially miss an ingester with data for that tenant. In other words, the lookback mechanism, used to select the ingesters which may have received series since 'now - lookback period', doesn't work correctly if the tenant shard size is decreased.

> **Note:** Decreasing the tenant shard size is not supported because it is something that you would rarely need to do, but a workaround still exists:

1. Disable shuffle sharding on the read path.
2. Decrease the configured tenant shard size.
3. Wait at least `-querier.shuffle-sharding-ingesters-lookback-period` time.
4. Re-enable shuffle sharding on the read path.

### Query-frontend and query-scheduler shuffle sharding

By default, all Grafana Mimir queriers can execute queries for any tenant.

When shuffle sharding is enabled by setting `-query-frontend.max-queriers-per-tenant` (or its respective YAML config option) to a value higher than `0` and lower than the number of available queriers, only the specified number of queriers will be eligible to execute queries for a given tenant.

Note that this distribution happens in query-frontend, or query-scheduler if used. When using query-scheduler, `-query-frontend.max-queriers-per-tenant` option must be set for query-scheduler component. When not using query-frontend (with or without query-scheduler), this option is not available.

_The maximum number of queriers can be overridden on a per-tenant basis setting `max_queriers_per_tenant` in the overrides section of the runtime configuration._

#### The impact of "query of death"

In the event a tenant is sending a "query of death" which causes a querier to crash, the crashed querier will get disconnected from the query-frontend or query-scheduler and another already running querier will be immediately assigned to the tenant's shard.

If the tenant repeatedly sends this query, the new querier assigned to the tenant's shard will crash as well, and yet another querier will be assigned to the shard. This cascading failure could potentially lead to crash all running queriers one by one, practically invalidating the assumption that shuffle sharding can be used to contain the blast radius of queries of death.

To mitigate this, there are experimental configuration options that allow you to configure a delay between when a querier disconnects because of a crash and when the crashed querier is actually replaced by another healthy querier. When this delay is configured, a tenant repeatedly sending a "query of death" will run with reduced querier capacity after a querier has crashed and could end up having no available queriers at all, but it will reduce the likelihood the crash will impact other tenants too.

A delay of 1 minute might be a reasonable trade-off:

- Query-frontend: `-query-frontend.querier-forget-delay=1m`
- Query-scheduler: `-query-scheduler.querier-forget-delay=1m`

### Store-gateway shuffle sharding

By default, a tenant's blocks are spread across all Grafana Mimir store-gateways.

When store-gateway shuffle sharding is enabled by setting `-store-gateway.tenant-shard-size` (or its respective YAML config option) to a value higher than `0` and lower than the number of available store-gateways, only the specified number of store-gateways will be eligible to load and query blocks for a given tenant. This flag needs to be set on the store-gateway, querier and ruler.

_The store-gateway shard size can be overridden on a per-tenant basis by setting `store_gateway_tenant_shard_size` in the overrides section of the runtime configuration._

_Please check out the [store-gateway documentation](../architecture/components/store-gateway.md) for more information about how it works._

### Ruler shuffle sharding

By default, tenant rule groups are sharded across all Grafana Mimir rulers.

When ruler shuffle sharding is enabled by setting `-ruler.tenant-shard-size` (or its respective YAML config option) to a value higher than `0` and lower than the number of available rulers, only the specified number of rulers will be eligible to evaluate rule groups for a given tenant.

_The ruler shard size can be overridden on a per-tenant basis by setting `ruler_tenant_shard_size` in the overrides section of the runtime configuration._

### Compactor shuffle sharding

By default, tenant blocks can be compacted by any Grafana Mimir compactor.

When compactor shuffle sharding is enabled by setting `-compactor.compactor-tenant-shard-size` (or its respective YAML config option) to a value higher than `0` and lower than the number of available compactors, only the specified number of compactors will be eligible to compact blocks for a given tenant.

_The compactor shard size can be overridden on a per-tenant basis setting by `compactor_tenant_shard_size` in the overrides section of the runtime configuration._

## FAQ

### Does shuffle sharding add additional overhead to the KV store?

No, shards are computed client-side and are not stored in the ring. KV store sizing still depends primarily on the number of replicas (of any component that uses the ring, e.g. ingesters) and tokens per replica.

However, each tenant's shard is cached in memory on the client-side in some components which may slightly increase their memory footprint (mostly the distributor).
