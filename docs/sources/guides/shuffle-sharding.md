---
title: "Shuffle Sharding"
linkTitle: "Shuffle Sharding"
weight: 10
slug: shuffle-sharding
---

Mimir leverages on sharding techniques to horizontally scale both single and multi-tenant clusters beyond the capacity of a single node.

## Background

Sharding strategy employed by Mimir distributes the workload across the subset of instances running a given service (eg. ingesters). For example, on the write path each tenant's series are sharded across subset of ingesters. The size of this subset (i.e. the number of instances) is configured using "shard size" parameter. Default value for shard size is 0, which means to use all available instances for each tenant. This allows to have a fair balance on the resources consumed by each instance (ie. CPU and memory) and to maximise these resources across the cluster.

However, in a **multi-tenant** cluster this zero default value introduces some **downsides**:

1. An outage affects all tenants
2. A misbehaving tenant (eg. causing out of memory) could affect all other tenants

Configuring shard size value higher than zero enables **shuffle sharding**. The goal of **shuffle sharding** is to reduce the blast radius of an outage and better isolate tenants.

## What is shuffle sharding

Shuffle sharding is a technique used to isolate different tenant's workloads and to give each tenant a single-tenant experience even if they're running in a shared cluster. This technique has been publicly shared and clearly explained by AWS in their [builders' library](https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/) and a reference implementation has been shown in the [Route53 Infima library](https://github.com/awslabs/route53-infima/blob/master/src/main/java/com/amazonaws/services/route53/infima/SimpleSignatureShuffleSharder.java).

The idea is to assign each tenant a shard composed by a subset of the Mimir service instances, aiming to minimize the overlapping instances between two different tenants. Shuffle sharding brings the following **benefits**:

- An outage on some Mimir cluster instances/nodes will only affect a subset of tenants.
- A misbehaving tenant will affect only its shard instances. Due to the low overlap of instances between different tenants, it's statistically quite likely that any other tenant will run on different instances or only a subset of instances will match the affected ones.

Using shuffle sharding doesn't require more resources but instances may be less evenly balanced from time to time.

### Low overlapping instances probability

For example, given a Mimir cluster running **50 ingesters** and assigning **each tenant 4** out of 50 ingesters, shuffling instances between each tenant, we get **230K possible combinations**.

Randomly picking two different tenants we have the:

- 71% chance that they will not share any instance
- 26% chance that they will share only 1 instance
- 2.7% chance that they will share 2 instances
- 0.08% chance that they will share 3 instances
- Only a 0.0004% chance that their instances will fully overlap

![Shuffle sharding probability](/images/guides/shuffle-sharding-probability.png)

<!-- Chart source at https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit -->

## Mimir shuffle sharding

Mimir currently supports shuffle sharding in the following services:

- [Ingesters](#ingesters-shuffle-sharding)
- [Query-frontend / Query-scheduler](#query-frontend-and-query-scheduler-shuffle-sharding)
- [Store-gateway](#store-gateway-shuffle-sharding)
- [Ruler](#ruler-shuffle-sharding)

Since default value of shard size is 0, shuffle sharding is **disabled by default** and needs to be explicitly enabled by increasing the shard size, either globally or only for given tenant. Note that if this value is higher than number of available instances (e.g. `-distributor.ingestion-tenant-shard-size` is higher than number of ingesters), then shuffle-sharding is disabled and all instances are used again.

### Guaranteed properties

The Mimir shuffle sharding implementation guarantees the following properties:

- **Stability**<br />
  Given a consistent state of the hash ring, the shuffle sharding algorithm always selects the same instances for a given tenant, even across different machines.
- **Consistency**<br />
  Adding or removing 1 instance from the hash ring leads to only 1 instance changed at most, in each tenant's shard.
- **Shuffling**<br />
  Probabilistically and for a large enough cluster, it ensures that every tenant gets a different set of instances, with a reduced number of overlapping instances between two tenants to improve failure isolation.
- **Zone-awareness**<br />
  When [zone-aware replication](./zone-replication.md) is enabled, the subset of instances selected for each tenant contains a balanced number of instances for each availability zone.

### Ingesters shuffle sharding

By default the Mimir distributor spreads the received series across all running ingesters.

When shuffle sharding is **enabled** for the ingesters, the distributor and ruler on the **write path** spread each tenant series across `-distributor.ingestion-tenant-shard-size` number of ingesters, while on the **read path** the querier and ruler queries only the subset of ingesters holding the series for a given tenant.

_The shard size can be overridden on a per-tenant basis in the limits overrides configuration._

#### Ingesters write path

To enable shuffle-sharding for ingesters on the write path you need to configure the following CLI flags (or their respective YAML config options) to **distributor**, **ingester** and **ruler**:

- `-distributor.ingestion-tenant-shard-size=<size>`<br />
  `<size>` set to the number of ingesters each tenant series should be sharded to. If `<size>` is zero or greater than the number of available ingesters in the Mimir cluster, the tenant series are sharded across all ingesters.

#### Ingesters read path

Assuming shuffle-sharding has been enabled for the write path, to enable shuffle-sharding for ingesters on the read path too you need to configure the following CLI flags (or their respective YAML config options) to **querier** and **ruler**:

- `-distributor.ingestion-tenant-shard-size=<size>`
- `-querier.shuffle-sharding-ingesters-lookback-period=<period>`<br />
  Queriers and rulers fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since 'now - lookback period'. The configured lookback `<period>` should be greater or equal than `-querier.query-store-after` and `-querier.query-ingesters-within` if set, and greater than the estimated minimum time it takes for the oldest samples stored in a block uploaded by ingester to be discovered and available for querying (3h with the default configuration).

#### Rollout strategy

If you're running a Mimir cluster with shuffle-sharding disabled and you want to enable it for ingesters, the following rollout strategy should be used to avoid missing querying any time-series in the ingesters memory:

1. Enable ingesters shuffle-sharding on the **write path**
2. **Wait** at least `-querier.shuffle-sharding-ingesters-lookback-period` time
3. Enable ingesters shuffle-sharding on the **read path**

#### Limitation: decreasing the tenant shard size

The current shuffle-sharding implementation in Mimir has a limitation which prevents to safely decrease the tenant shard size if the ingesters shuffle-sharding is enabled on the read path.

The problem is that if a tenant’s subring decreases in size, there is currently no way for the queriers and rulers to know how big the tenant subring was previously, and hence they will potentially miss an ingester with data for that tenant. In other words, the lookback mechanism to select the ingesters which may have received series since 'now - lookback period' doesn't work correctly if the tenant shard size is decreased.

This is deemed an infrequent operation that we considered banning, but a workaround still exists:

1. **Disable** shuffle-sharding on the read path
2. **Decrease** the configured tenant shard size
3. **Wait** at least `-querier.shuffle-sharding-ingesters-lookback-period` time
4. **Re-enable** shuffle-sharding on the read path

### Query-frontend and Query-scheduler shuffle sharding

By default all Mimir queriers can execute received queries for given tenant.

When shuffle sharding is **enabled** by setting `-frontend.max-queriers-per-tenant` (or its respective YAML config option) to a value higher than 0 and lower than the number of available queriers, only specified number of queriers will execute queries for single tenant.

Note that this distribution happens in query-frontend, or query-scheduler if used. When using query-scheduler, `-frontend.max-queriers-per-tenant` option must be set for query-scheduler component. When not using query-frontend (with or without scheduler), this option is not available.

_The maximum number of queriers can be overridden on a per-tenant basis in the limits overrides configuration._

#### The impact of "query of death"

In the event a tenant is repeatedly sending a "query of death" which leads the querier to crash or getting killed because of out-of-memory, the crashed querier will get disconnected from the query-frontend or query-scheduler and a new querier will be immediately assigned to the tenant's shard. This practically invalidates the assumption that shuffle-sharding can be used to contain the blast radius in case of a query of death.

To mitigate it, Mimir allows to configure a delay between when a querier disconnects because of a crash and when the crashed querier is actually removed from the tenant's shard (and another healthy querier is added as replacement). A delay of 1 minute may be a reasonable trade-off:

- Query-frontend: `-query-frontend.querier-forget-delay=1m`
- Query-scheduler: `-query-scheduler.querier-forget-delay=1m`

### Store-gateway shuffle sharding

The Mimir store-gateway -- used by the [blocks storage](../blocks-storage/_index.md) -- spreads each tenant's blocks across `-store-gateway.tenant-shard-size` running store-gateway instances. If this flag is 0, it means all store-gateway instances. This flag needs to be set to **store-gateway**, **querier** and **ruler**.

_The shard size can be overridden on a per-tenant basis setting `store_gateway_tenant_shard_size` in the limits overrides configuration._

_Please check out the [store-gateway documentation](../blocks-storage/store-gateway.md) for more information about how it works._

### Ruler shuffle sharding

Mimir ruler can run in two modes:

1. **No sharding at all.** This is the most basic mode of the ruler. It is activated by using `-ruler.enable-sharding=false` (default) and works correctly only if single ruler is running. In this mode the Ruler loads all rules for all tenants.
2. **Sharding**, activated by using `-ruler.enable-sharding=true`. In this mode rulers register themselves into the ring. Each ruler will then select and evaluate only those rules that it "owns". Rule groups for each tenant can only be evaluated on limited number of rulers (`-ruler.tenant-shard-size`, can also be set per tenant as `ruler_tenant_shard_size` in overrides). If shard size for tenant is set to 0, then tenant's rules are sharded across all ruler replicas.

Note that when using sharding strategy, each rule group is evaluated by single ruler only, there is no replication.

## FAQ

### Does shuffle sharding add additional overhead to the KV store?

No, shuffle sharding subrings are computed client-side and are not stored in the ring. KV store sizing still depends primarily on the number of replicas (of any component that uses the ring, e.g. ingesters) and tokens per replica.

However, each tenant's subring is cached in memory on the client-side which may slightly increase the memory footprint of certain components (mostly the distributor).
