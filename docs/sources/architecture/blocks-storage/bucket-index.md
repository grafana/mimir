---
title: "Bucket Index"
linkTitle: "Bucket Index"
weight: 10
slug: bucket-index
---

The bucket index is a **per-tenant file containing the list of blocks and block deletion marks** in the storage. The bucket index itself is stored in the backend object storage, is periodically updated by the compactor, and used by queriers, store-gateways and rulers to discover blocks in the storage.

The bucket index is enabled by default, but it is **optional**. It can be disabled via `-blocks-storage.bucket-store.bucket-index.enabled=false` (or its respective YAML config option). Disabling the bucket index is not recommended.

## Benefits

The [querier](./querier.md), [store-gateway](./store-gateway.md) and ruler need to have an almost up-to-date view over the entire storage bucket, in order to find the right blocks to lookup at query time (querier) and load block's [index-header](./../../architecture/blocks-storage/binary-index-header.md) (store-gateway). Because of this, they need to periodically scan the bucket to look for new blocks uploaded by ingester or compactor, and blocks deleted (or marked for deletion) by compactor.

When the bucket index is enabled, the querier, store-gateway and ruler periodically look up the per-tenant bucket index instead of scanning the bucket via "list objects" operations. This brings few benefits:

1. Reduced number of API calls to the object storage by querier and store-gateway
2. No "list objects" storage API calls done by querier and store-gateway
3. The [querier](./querier.md) is up and running immediately after the startup (no need to run an initial bucket scan)

## Structure of the index

The `bucket-index.json.gz` contains:

- **`blocks`**<br />
  List of complete blocks of a tenant, including blocks marked for deletion (partial blocks are excluded from the index).
- **`block_deletion_marks`**<br />
  List of block deletion marks.
- **`updated_at`**<br />
  Unix timestamp (seconds precision) of when the index has been updated (written in the storage) the last time.

## How it gets updated

The [compactor](./compactor.md) periodically scans the bucket and uploads an updated bucket index to the storage. The frequency at which the bucket index is updated can be configured via `-compactor.cleanup-interval`.

The use of the bucket index is optional, but the index is built and updated by the compactor even if `-blocks-storage.bucket-store.bucket-index.enabled=false`. This behavior ensures the bucket index for any tenant already exists and that query result consistency is guaranteed if a Grafana Mimir cluster operator decides to enable the bucket index in a live cluster. The overhead introduced by keeping the bucket index updated is not signifcant.

## How it's used by the querier

At query time the [querier](./querier.md) and ruler check whether the bucket index for the tenant has already been loaded in memory. If not, the querier and ruler download it from the storage and cache it in memory.

_Given it's a small file, lazy downloading it doesn't significantly impact on first query performances, but allows to get a querier up and running without pre-downloading every tenant's bucket index. Moreover, if the [metadata cache](./querier.md#metadata-cache) is enabled, the bucket index will be cached for a short time in a shared cache, reducing the actual latency and number of API calls to the object storage in case multiple queriers and rulers will fetch the same tenant's bucket index in a short time._

![Querier - Bucket index](/images/blocks-storage/bucket-index-querier-workflow.png)

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

While in-memory, a background process will keep it **updated at periodic intervals**, so that subsequent queries from the same tenant to the same querier instance will use the cached (and periodically updated) bucket index. There are two config options involved:

- `-blocks-storage.bucket-store.sync-interval`<br />
  This option configures how frequently a cached bucket index should be refreshed.
- `-blocks-storage.bucket-store.bucket-index.update-on-error-interval`<br />
  If downloading a bucket index fails, the failure is cached for a short time in order to avoid hammering the backend storage. This option configures how frequently a bucket index, which previously failed to load, should be tried to load again.

If a bucket index is unused for a long time (configurable via `-blocks-storage.bucket-store.bucket-index.idle-timeout`), e.g. because that querier instance is not receiving any query from the tenant, the querier will offload it, stopping to keep it updated at regular intervals. This is particularly for tenants which are resharded to different queriers when [shuffle sharding](../guides/shuffle-sharding.md) is enabled.

Finally, at query time the querier and ruler check how old a bucket index is (based on its `updated_at`) and fail a query if its age is older than `-blocks-storage.bucket-store.bucket-index.max-stale-period`. This circuit breaker is used to ensure queriers and rulers will not return any partial query results due to a stale view over the long-term storage.

## How it's used by the store-gateway

The [store-gateway](./store-gateway.md), at startup and periodically, fetches the bucket index for each tenant belonging to their shard and uses it as the source of truth for the blocks (and deletion marks) in the storage. This removes the need to periodically scan the bucket to discover blocks belonging to their shard.
