---
title: "Store-gateway"
description: "Overview of the store-gateway microservice."
weight: 10
---

# Store-gateway

The store-gateway is a stateful component that queries blocks from [long-term storage]({{< relref "./_index.md#long-term-storage" >}}).
The [querier]({{< relref "./querier.md" >}}) and the [ruler]({{< relref "./ruler.md">}}) use the store-gateway on the read path at query time.

To find the right blocks to look up at query time, the store-gateway requires an almost up-to-date view of the bucket in long-term storage.
The store-gateway keeps the bucket view updated in the following ways:

1. Periodically downloading the [bucket index]({{< relref "../blocks-storage/bucket-index.md" >}}) (default)
2. Periodically scanning the bucket

### Bucket index enabled (default)

To discover each tenant's blocks and block deletion marks, at startup, store-gateways fetch the [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}) from long-term storage for each tenant that belongs to their [shard](#blocks-sharding-and-replication).

For each discovered block, the store-gateway downloads the [index header](#blocks-index-header).
During this initial bucket synchronization phase, the store-gateway `/ready` readiness probe endpoint fails.

For more information about the bucket index, refer to [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}).

Store-gateways periodically re-download the bucket index to obtain an updated view of the long-term storage and discover new or deleted blocks.
[Ingesters]({{< relref "./ingester.md" >}}) or the [compactor]({{< relref "./compactor.md" >}}) can upload new blocks.

It is possible that the compactor might have deleted blocks or marked others for deletion since the store-gateway last checked the block.
For new blocks, the store-gateway downloads the index header, and for deleted blocks the store-gateway offloads the index header.
You can configure the `-blocks-storage.bucket-store.sync-interval` flag to control the frequency with which the store-gateway checks for changes in the block.

The store-gateway doesn't fully download the blocks chunks and the index.
To avoid the store-gateway having to re-download the index during subsequent restarts, the store-gateway stores the index-header to the local disk.
Because the store-gateway doesn't download the blocks chunks and index, we recommend that you run the store-gateway with a persistent disk.
For example, if you're running the Grafana Mimir cluster in Kubernetes, you can use a StatefulSet with a PersistentVolumeClaim for the store-gateways.

For more information about the index-header, refer to [Binary index-header documentation]({{< relref "../operating-grafana-mimir/blocks-storage/binary-index-header.md" >}}).

### Bucket index disabled

When bucket index is disabled, at startup, the store-gateway iterates over the entire long-term storage to download `meta.json` metadata files, and then filters out blocks that don't belong to the tenants in their shard.

## Blocks sharding and replication

The store-gateway uses blocks sharding to horizontally scale blocks in a large cluster without reaching a vertical scalability limit.

Blocks are replicated across multiple store-gateway instances based on a replication factor configured via `-store-gateway.sharding-ring.replication-factor`.
Blocks replication protects from query failures that can be caused by the store-gateway failing to load blocks at a given time, for example, when a store-gateway fails or when the store-gateway instances restart, as is the case during a rolling update.

Store-gateway instances build a [hash ring]({{< relref "../architecture/about-the-hash-ring" >}}) and shard and replicate blocks across the pool of store-gateway instances registered in the ring.

Store-gateways continuously monitor the ring state.
When the ring topology changes, for example, when a new instance is added or removed, or the instance becomes healthy or unhealthy, each store-gateway instance resynchronizes the blocks assigned to its shard.
The store-gateway resynchronization process uses the block ID hash that matches the token ranges assigned to the instance within the ring.

The store-gateway loads the index-header of each block that belongs to a store-gateway shard.
After the store-gateway loads a block, the block is ready to be queried by queriers.
When the querier queries blocks through a store-gateway, the response contains the list of queried block IDs.
If a querier attempts to query a block that the store-gateway has not loaded, the querier retries the query on a different store-gateway up to the `-store-gateway.sharding-ring.replication-factor` value, which by default is 3. You can specify a lower replication factor, if necessary.
The query fails if the block can't be successfully queried from any replica.

> **Note**: You must configure the [hash ring]({{< relref "../architecture/about-the-hash-ring" >}}) via the `-store-gateway.sharding-ring.*` flags or their respective YAML configuration parameters.

### Sharding strategy

The store-gateway uses shuffle-sharding to divide the blocks of each tenant across a subset of store-gateway instances.
The number of store-gateway instances that load the blocks of a tenant is limited, which means that the blast radius of issues introduced by the tenant's workload is confined to its shard instances.

The `-store-gateway.tenant-shard-size` flag (or their respective YAML configuration parameters) determines the default number of store-gateway instances per tenant.
The `store_gateway_tenant_shard_size` in the limits overrides can override the shard size on a per-tenant basis.

The default `-store-gateway.tenant-shard-size` value is 0, which means that tenant's blocks are sharded across all store-gateway instances.

For more information about shuffle sharding, refer to [configure shuffle sharding]({{< relref "../operating-grafana-mimir/configure-shuffle-sharding.md" >}}).

### Auto-forget

The store-gateway includes a feature called _auto-forget_ that unregisters an instance from a ring when it does not properly shut down.
Under normal conditions, when a store-gateway instance shuts down, it automatically unregisters from the ring. However, in the event of a crash or node failure, the instance might not properly unregister, which can leave a spurious entry in the ring.

The auto-forget feature works as follows: when an healthy store-gateway instance identifies an instance in the ring that is unhealthy for longer than 10 times the configured `-store-gateway.sharding-ring.heartbeat-timeout` value, the healthy instance removes the unhealthy instance from the ring.

### Zone-awareness

Store-gateway replication optionally supports [zone-awareness]({{< relref "../operating-grafana-mimir/configure-zone-aware-replication.md" >}}). When you enable zone-aware replication and the blocks replication factor is greater than 1, each block is replicated across store-gateway instances located in different availability zones.

**To enable zone-aware replication for the store-gateways**:

1. Configure the availability zone for each store-gateway via the `-store-gateway.sharding-ring.instance-availability-zone` CLI flag or its respective YAML configuration parameter.
1. Enable blocks zone-aware replication via the `-store-gateway.sharding-ring.zone-awareness-enabled` CLI flag or its respective YAML configuration parameter.
1. Set this zone-aware replication flag on store-gateways, queriers, and rulers.
1. To apply the new configuration, roll out store-gateways, queriers, and rulers.

### Waiting for stable ring at startup

If a cluster cold starts or scales up to two or more store-gateway instances simultaneously, the store-gateways could start at different times. As a result, the store-gateway runs the initial blocks synchronization based on a different state of the hash ring.

For example, in the event of a cold start, the first store-gateway that joins the ring might load all blocks because the sharding logic runs based on the current state of the ring, which is a single store-gateway.

To reduce the likelihood of store-gateways starting at different times, you can configure the store-gateway to wait for a stable ring at startup. A ring is considered stable when no instance is added or removed from the ring for the minimum duration specified in the `-store-gateway.sharding-ring.wait-stability-min-duration` flag. If the ring continues to change after reaching the maximum duration specified in the `-store-gateway.sharding-ring.wait-stability-max-duration` flag, the store-gateway stops waiting for a stable ring and proceeds starting up.

To enable waiting for stable ring at startup, start the store-gateway with `-store-gateway.sharding-ring.wait-stability-min-duration=1m`, which is the recommended value for production systems.

## Blocks index-header

The [index-header]({{< relref "../operating-grafana-mimir/blocks-storage/binary-index-header.md" >}}) is a subset of the block index that the store-gateway downloads from long-term storage and keeps on the local disk.
Keeping the index-header on the local disk quickens query execution.

At startup, the store-gateway downloads the index-header of each block that belongs to its shard. A store-gateway is not ready until it completes downloading the initial index-header.
As it runs, the store-gateway periodically looks in the long-term storage for newly uploaded blocks and downloads the index-header for the blocks that belong to its shard.

### Index-header lazy loading

By default, a store-gateway downloads the index-headers to disk instead of committing the index-header to memory.
When required by a query, index-headers are memory-mapped and automatically released by the store-gateway after the amount of inactivity time you specify in `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` has passed

Mimir provides a configuration flag `-blocks-storage.bucket-store.index-header-lazy-loading-enabled=false` to disable index-header lazy loading.
When disabled, the store-gateway memory-maps all index-headers, which provides faster access to the data in the index-header.
However, in a cluster with a large number of blocks, each store-gateway might have a large amount of memory-mapped index-headers, regardless of how frequently they are used at query time.

## Caching

The store-gateway supports the following type of caches:

- [Index cache](#index-cache)
- [Chunks cache](#chunks-cache)
- [Metadata cache](#metadata-cache)

We recommend that you use caching in a production environment.
For more information about configuring the cache, refer to [production tips]({{< relref "../operating-grafana-mimir/blocks-storage/production-tips.md#caching" >}}).

### Index cache

The store-gateway can use a cache to accelerate series and label lookups from TSDB blocks indexes. The store-gateway supports the following backends:

- `inmemory`
- `memcached`

#### In-memory index cache

By default, the `inmemory` index cache is enabled.

Consider the following trade-offs of using the in-memory index cache:

- Pros: There is no latency.
- Cons: Store-gateway memory usage increases and, when sharding is disabled or the replication factor is > 1, memory usage is not shared across multiple store-gateway replicas.

You can configure the index cache max size using the `-blocks-storage.bucket-store.index-cache.inmemory.max-size-bytes` flag or its respective YAML configuration parameter.

#### Memcached index cache

The `memcached` index cache uses [Memcached](https://memcached.org/) as the cache backend.

Consider the following trade-offs of using the Memcached index cache:

- Pros: You can scale beyond a single node memory, or Memcached cluster, that is shared across multiple store-gateway instances.
- Cons: The system experiences higher latency in the cache round trip compared to the latency experienced when using in-memory operations.

The Memcached client uses a jump hash algorithm to shard cached entries across a cluster of Memcached servers.
Because the memcached client uses a jump hash algorithm, ensure that memcached servers are not located behind a load balancer, and configure the address of the memcached servers so that servers are added to or removed from the end of the list whenever a scale up or scale down occurs.

For example, if you're running Memcached in Kubernetes, you might:

1. Deploy your Memcached cluster using a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
1. Create a [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) for Memcached StatefulSet.
1. Configure the Mimir's Memcached client address using the `dnssrvnoa+` [service discovery]({{< relref "../configuration/about-grafana-mimir-arguments.md#dns-service-discovery" >}}).

**To configure the Memcached backend**:

1. Set the following flag to 'memcached': `-blocks-storage.bucket-store.index-cache.backend=memcached`
1. Use the `-blocks-storage.bucket-store.index-cache.memcached.addresses` flag to set the addresses of the Memcached servers.

[DNS service discovery]({{< relref "../configuration/about-grafana-mimir-arguments.md#dns-service-discovery" >}}) resolves the addresses of the Memcached servers.

### Chunks cache

The store-gateway can also use a cache to store [chunks]({{< relref "../reference-glossary.md#chunk" >}}) that are fetched from long-term storage.
Chunks contain samples that the store-gateway can reuse if the query includes the same series for the same time range.
Chunks can only be stored in Memcached cache.

To enable chunks cache, set `-blocks-storage.bucket-store.chunks-cache.backend`.
You can configure the Memcached client via flags that include the prefix `-blocks-storage.bucket-store.chunks-cache.memcached.*`.

> **Note**: There are additional low-level flags that begin with the prefix `-blocks-storage.bucket-store.chunks-cache.*` that you can use to configure chunks cache.

### Metadata cache

Store-gateways and [queriers]({{< relref "./querier.md" >}}) use memcached to cache the following bucket metadata:

- List of tenants
- List of blocks per tenant
- Block `meta.json` existence and content
- Block `deletion-mark.json` existence and content
- Tenant `bucket-index.json.gz` content

Using the metadata cache reduces the number of API calls to long-term storage and eliminates API calls that scale linearly as the number of querier and store-gateway replicas increases.

To enable metadata cache, set `-blocks-storage.bucket-store.metadata-cache.backend`.

> **Note**: Currently, only `memcached` backend is supported. The Memcached client includes additional configuration available via flags that begin with the prefix `-blocks-storage.bucket-store.metadata-cache.memcached.*`.

Additional flags for configuring metadata cache begin with the prefix `-blocks-storage.bucket-store.metadata-cache.*`. By configuring TTL to zero or a negative value, caching of given item type is disabled.

_The same memcached backend cluster should be shared between store-gateways and queriers._

## Store-gateway HTTP endpoints

- `GET /store-gateway/ring`<br />
  Displays the status of the store-gateways ring, including the tokens owned by each store-gateway and an option to remove (or forget) instances from the ring.

## Store-gateway configuration

For more information about store-gateway configuration, refer to [store_gateway]({{< relref "../configuration/reference-configuration-parameters.md#store_gateway" >}}).
