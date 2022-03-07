---
title: "Store-gateway"
description: "Overview of the store-gateway microservice."
weight: 10
---

# Store-gateway

The store-gateway is responsible for querying blocks from [long-term storage]({{< relref "../_index.md#long-term-storage" >}}) and is used by the [querier]({{< relref "querier.md" >}}) and by the [ruler]({{< relref "ruler.md" >}}) on the read path at query time.

The store-gateway is **stateful**.

## How it works

The store-gateway needs to have an almost up-to-date view over the bucket in long-term storage, in order to find the right blocks to lookup at query time. The store-gateway can keep the bucket view updated in two different ways:

1. Periodically downloading the [bucket index]({{< relref "../blocks-storage/bucket-index.md" >}}) (default)
2. Periodically scanning the bucket

### Bucket index enabled (default)

At startup **store-gateways** fetch the [bucket index]({{< relref "../blocks-storage/bucket-index.md" >}}) from long-term storage for each tenant belonging to their [shard](#blocks-sharding-and-replication) in order to discover each tenant's blocks and block deletion marks. For each discovered block the [index header](#blocks-index-header) is downloaded. During this initial bucket synchronization phase, the store-gateway `/ready` readiness probe endpoint will fail.

For more information about the bucket index, please refer to [bucket index documentation]({{< relref "../blocks-storage/bucket-index.md" >}}).

Store-gateways periodically re-download the bucket index to get an updated view over the long-term storage and discover new or deleted blocks.
New blocks can be uploaded by [ingesters]({{< relref "ingester.md" >}}) or by the [compactor]({{< relref "compactor.md" >}}).
The compactor additionally may have deleted blocks or marked others for deletion since the last check.
For new blocks the store-gateway downloads their index header, while for deleted blocks the index header is offloaded.
The frequency at which this occurs is configured with the `-blocks-storage.bucket-store.sync-interval` flag.

The blocks chunks and the entire index are never fully downloaded by the store-gateway. The index-header is stored to the local disk, in order to avoid having to re-download it on subsequent restarts of a store-gateway. For this reason, it's recommended - but not required - to run the store-gateway with a persistent disk. For example, if you're running the Grafana Mimir cluster in Kubernetes, you may use a StatefulSet with a PersistentVolumeClaim for the store-gateways.

For more information about the index-header, please refer to [Binary index-header documentation]({{< relref "../blocks-storage/binary-index-header.md" >}}).

### Bucket index disabled

When bucket index is disabled, the overall workflow is the same, except that the discovery of blocks at startup and during the periodic checks mean iterating over the entire long-term storage to download `meta.json` metadata files while filtering out blocks that don't belong to tenants in their shard.

## Blocks sharding and replication

The store-gateway employs blocks sharding. Sharding is used to horizontally scale blocks in a large cluster without hitting any vertical scalability limit.

Blocks are replicated across multiple store-gateway instances based on a replication factor configured via `-store-gateway.sharding-ring.replication-factor`. The blocks replication is used to protect from query failures caused by some blocks not loaded by any store-gateway instance at a given time like, for example, in the event of a store-gateway failure or while restarting a store-gateway instance (e.g. during a rolling update).

Store-gateway instances build a [hash ring]({{< relref "../hash-ring.md" >}}) and blocks gets sharded and replicated across the pool of store-gateway instances registered within the ring.

Store-gateways continuously monitor the ring state and whenever the ring topology changes (e.g. a new instance has been added/removed or gets healthy/unhealthy) each store-gateway instance resync the blocks assigned to its shard, based on the block ID hash matching the token ranges assigned to the instance itself within the ring.

For each block belonging to a store-gateway shard, the store-gateway loads its index-header. Once a block is loaded on the store-gateway, it's ready to be queried by queriers. When the querier queries blocks through a store-gateway, the response will contain the list of actually queried block IDs. If a querier tries to query a block which has not been loaded by a store-gateway, the querier will retry on a different store-gateway up to `-store-gateway.sharding-ring.replication-factor` (defaults to 3) times or maximum 3 times, whichever is lower. The query will fail if the block can't be successfully queried from any replica.

To configure the store-gateways' hash ring, refer to [configuring hash rings]({{< relref "../../operating-grafana-mimir/configure-hash-ring.md" >}}).

### Sharding strategy

The store-gateway uses shuffle-sharding to spread the blocks of each tenant across a subset of store-gateway instances. The number of store-gateway instances loading blocks of a single tenant is limited and the blast radius of any issue that could be introduced by the tenant's workload is limited to its shard instances.

The default number of store-gateway instances per tenant is configured using the `-store-gateway.tenant-shard-size` flag (or their respective YAML configuration parameters). The shard size can then be overridden on a per-tenant basis setting the `store_gateway_tenant_shard_size` in the limits overrides.

Default value for `-store-gateway.tenant-shard-size` is 0, which means that tenant's blocks are sharded across all store-gateway instances.

For more information on shuffle sharding, refer to [configure shuffle sharding]({{< relref "../../operating-grafana-mimir/configure-shuffle-sharding.md" >}}).

### Auto-forget

When a store-gateway instance cleanly shuts down, it automatically unregisters itself from the ring. However, in the event of a crash or node failure, the instance will not be unregistered from the ring, potentially leaving a spurious entry in the ring forever.

To protect from this, when an healthy store-gateway instance finds another instance in the ring which is unhealthy for more than 10 times the configured `-store-gateway.sharding-ring.heartbeat-timeout`, the healthy instance forcibly removes the unhealthy one from the ring.

This feature is called **auto-forget** and is built into the store-gateway.

### Zone-awareness

The store-gateway replication optionally supports [zone-awareness]({{< relref "../../operating-grafana-mimir/configure-zone-aware-replication.md" >}}). When zone-aware replication is enabled and the blocks replication factor is > 1, each block is guaranteed to be replicated across store-gateway instances running in different availability zones.

**To enable** the zone-aware replication for the store-gateways you should:

1. Configure the availability zone for each store-gateway via the `-store-gateway.sharding-ring.instance-availability-zone` CLI flag (or its respective YAML configuration parameter)
2. Enable blocks zone-aware replication via the `-store-gateway.sharding-ring.zone-awareness-enabled` CLI flag (or its respective YAML configuration parameter). Please be aware this configuration flag should be set on store-gateways, queriers and rulers.
3. Rollout store-gateways, queriers and rulers to apply the new configuration

### Waiting for stable ring at startup

In the event of a cluster cold start or scale up of 2+ store-gateway instances at the same time we may end up in a situation where each new store-gateway instance starts at a slightly different time and thus each one runs the initial blocks sync based on a different state of the ring. For example, in case of a cold start, the first store-gateway joining the ring may load all blocks since the sharding logic runs based on the current state of the ring, which is 1 single store-gateway.

To reduce the likelihood this could happen, the store-gateway can optionally wait for a stable ring at startup. A ring is considered stable if no instance is added/removed to the ring for at least `-store-gateway.sharding-ring.wait-stability-min-duration`. If the ring keeps getting changed after `-store-gateway.sharding-ring.wait-stability-max-duration`, the store-gateway will stop waiting for a stable ring and will proceed starting up normally.

To enable this waiting logic, you can start the store-gateway with `-store-gateway.sharding-ring.wait-stability-min-duration=1m`, which is the recommended value for production systems.

## Blocks index-header

The [index-header]({{< relref "../blocks-storage/binary-index-header.md" >}}) is a subset of the block index which the store-gateway downloads from long-term storage and keeps on the local disk in order to speed up queries.

At startup, the store-gateway downloads the index-header of each block belonging to its shard. A store-gateway is not ready until this initial index-header download is completed. Moreover, while running, the store-gateway periodically looks for newly uploaded blocks in the long-term storage and downloads the index-header for the blocks belonging to its shard.

### Index-header lazy loading

By default, index-headers are downloaded to disk, but not kept in memory by the store-gateway. Index-headers will be memory mapped only once required by a query and will be automatically released after `-blocks-storage.bucket-store.index-header-lazy-loading-idle-timeout` time of inactivity.

Mimir supports a configuration flag `-blocks-storage.bucket-store.index-header-lazy-loading-enabled=false` to disable index-header lazy loading.
When disabled, index-headers are all memory mapped, which provides faster access, however in a cluster with a large number of blocks, each store-gateway may have a large amount of memory mapped index-headers, regardless how frequently they are used at query time.

## Caching

The store-gateway supports the following caches:

- [Index cache](#index-cache)
- [Chunks cache](#chunks-cache)
- [Metadata cache](#metadata-cache)

Caching is optional, but **highly recommended** in a production environment. Please also check out the [production tips]({{< relref "../blocks-storage/production-tips.md#caching" >}}) for more information about configuring the cache.

### Index cache

The store-gateway can use a cache to speed up lookups of series and labels from TSDB blocks indexes. Two backends are supported:

- `inmemory`
- `memcached`

#### In-memory index cache

The `inmemory` index cache is **enabled by default** and its max size can be configured through the flag `-blocks-storage.bucket-store.index-cache.inmemory.max-size-bytes` (or its respective YAML configuration parameter). The trade-off of using the in-memory index cache is:

- Pros: zero latency
- Cons: increased store-gateway memory usage, not shared across multiple store-gateway replicas (when sharding is disabled or replication factor > 1)

#### Memcached index cache

The `memcached` index cache uses [Memcached](https://memcached.org/) as cache backend. This cache backend is configured using `-blocks-storage.bucket-store.index-cache.backend=memcached` and requires setting the addresses of the Memcached servers with the `-blocks-storage.bucket-store.index-cache.memcached.addresses` flag . The addresses are resolved using [DNS service discovery]({{< relref "../../configuring/about-dns-service-discovery.md" >}}).

The trade-off of using the Memcached index cache is:

- Pros: can scale beyond a single node memory (Memcached cluster), shared across multiple store-gateway instances
- Cons: higher latency in the cache round trip compared to the in-memory one

The Memcached client uses a jump hash algorithm to shard cached entries across a cluster of Memcached servers. For this reason, you should make sure memcached servers are **not** behind any kind of load balancer and their address is configured so that servers are added/removed to the end of the list whenever a scale up/down occurs.

For example, if you're running Memcached in Kubernetes, you may:

1. Deploy your Memcached cluster using a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
2. Create an [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) for Memcached StatefulSet
3. Configure the Mimir's Memcached client address using the `dnssrvnoa+` [service discovery]({{< relref "../../configuring/about-dns-service-discovery.md" >}})

### Chunks cache

The store-gateway can also use a cache for storing [chunks]({{< relref "../../reference-glossary.md#chunk" >}}) fetched from the long-term storage. Chunks contain actual samples, and can be reused if a query hits the same series for the same time range.

To enable chunks cache, please set `-blocks-storage.bucket-store.chunks-cache.backend`. Chunks can currently only be stored into Memcached cache. Memcached client can be configured via flags with `-blocks-storage.bucket-store.chunks-cache.memcached.*` prefix.

There are additional low-level flags for configuring chunks cache. Please refer to other flags with `-blocks-storage.bucket-store.chunks-cache.*` prefix.

### Metadata cache

Store-gateway and [querier]({{< relref "querier.md" >}}) can use memcached for caching bucket metadata:

- List of tenants
- List of blocks per tenant
- List of chunks in blocks per tenant
- Block's `meta.json` existence and content
- Block's `deletion-mark.json` existence and content
- Tenant's `bucket-index.json.gz` content

Using the metadata cache can significantly reduce the number of API calls to long-term storage and stops the number of these API calls scaling linearly with the number of querier and store-gateway replicas which periodically scan and sync this metadata.

To enable metadata cache, please set `-blocks-storage.bucket-store.metadata-cache.backend`. Only `memcached` backend is supported currently. Memcached client has additional configuration available via flags with `-blocks-storage.bucket-store.metadata-cache.memcached.*` prefix.

Additional flags for configuring metadata cache have `-blocks-storage.bucket-store.metadata-cache.*` prefix. By configuring TTL to zero or negative value, caching of given item type is disabled.

_The same memcached backend cluster should be shared between store-gateways and queriers._

## Store-gateway HTTP endpoints

- `GET /store-gateway/ring`<br />
  Displays the status of the store-gateways ring, including the tokens owned by each store-gateway and an option to remove (forget) instances from the ring.

## Store-gateway configuration

Refer to the [store-gateway]({{< relref "../../configuring/reference-configuration-parameters.md#store_gateway" >}})
block section for details of store-gateway-related configuration.
