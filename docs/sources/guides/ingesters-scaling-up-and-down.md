---
title: "Ingesters scaling up and down"
linkTitle: "Ingesters scaling up and down"
weight: 10
slug: ingesters-scaling-up-and-down
---

This guide explains how to scale up and down ingesters.

_If you're looking how to run ingesters rolling updates, please refer to the [dedicated guide](./ingesters-rolling-updates.md)._

## Scaling up

Adding more ingesters to a Cortex cluster is considered a safe operation. When a new ingester starts, it will register to the [hash ring](../architecture.md#the-hash-ring) and the distributors will reshard received series accordingly. Ingesters that were previously receiving those series will see data stop arriving and will consider those series "idle".

## Scaling down

A running ingester holds several hours of time series data in memory, before they're flushed to the long-term storage. When an ingester shuts down, because of a scale down operation, the in-memory data must not be discarded in order to avoid any data loss.

The procedure to adopt when scaling down ingesters depends on your Cortex setup:

- [Blocks storage](#blocks-storage)

### Blocks storage

When Cortex is running the [blocks storage](../blocks-storage/_index.md), ingesters don't flush series to blocks at shutdown by default. However, Cortex ingesters expose an API endpoint [`/shutdown`](../api/_index.md#shutdown) that can be called to flush series to blocks and upload blocks to the long-term storage before the ingester terminates.

Even if ingester blocks are compacted and shipped to the storage at shutdown, it takes some time for queriers and store-gateways to discover the newly uploaded blocks. This is due to the fact that the blocks storage runs a periodic scanning of the storage bucket to discover blocks. If two or more ingesters are scaled down in a short period of time, queriers may miss some data at query time due to series that were stored in the terminated ingesters but their blocks haven't been discovered yet.

The ingesters scale down is deemed an infrequent operation and no automation is currently provided, but you can do it manually:

- Configure queriers and rulers to always query the storage
  - `-querier.query-store-after=0s`
- Frequently scan the storage bucket
  - `-blocks-storage.bucket-store.sync-interval=5m`
  - `-compactor.cleanup-interval=5m`
- Lower bucket scanning cache TTLs
  - `-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl=1m`
  - `-blocks-storage.bucket-store.metadata-cache.tenant-blocks-list-ttl=1m`
  - `-blocks-storage.bucket-store.metadata-cache.metafile-doesnt-exist-ttl=1m`
- Scale down the ingesters one by one:
  1. Call `/shutdown` endpoint on the ingester to shutdown.
  2. Wait until the HTTP call returns successfully or "finished flushing and shipping TSDB blocks" is logged.
  3. Terminate the ingester process (the `/shutdown` will not do it).
  4. Before proceeding to the next ingester, wait 2x the maximum between `-blocks-storage.bucket-store.sync-interval` and `-compactor.cleanup-interval`.
