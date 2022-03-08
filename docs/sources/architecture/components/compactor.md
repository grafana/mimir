---
title: "Compactor"
description: "Overview of the compactor component."
weight: 10
---

# Compactor

The compactor increases the efficiency of memory usage on disk and in long term storage by combining blocks.

The compactor is the component responsible for:

- Compacting multiple blocks of a given tenant into a single, optimized larger block. This deduplicates blocks and reduces the size of the index, resulting in reduced storage costs. Querying fewer blocks is faster, so it also increases query speed.
- Keeping the per-tenant bucket index updated. The [bucket index]({{< relref "../blocks-storage/bucket-index.md" >}}) is used by [queriers]({{< relref "querier.md" >}}), [store-gateways]({{< relref "store-gateway.md" >}}), and [rulers]({{< relref "ruler.md" >}}) to discover new blocks in the storage.
- Deleting blocks that are no longer within a configurable retention period.

The compactor is stateless.

## How compaction works

Compaction is applied on a per-tenant basis.

At regular, configurable intervals, the compactor does a vertical compaction followed by a horizontal compaction.

**Vertical compaction** merges all the blocks of a tenant uploaded by ingesters for the same time range (2 hours ranges by default) into a single block. It also deduplicates samples that were originally written to N blocks as a result of replication. Vertical compaction reduces the number of blocks for a single time range from the quantity of ingesters down to one block per tenant.

**Horizontal compaction** triggers after a vertical compaction. It compacts several blocks with adjacent range periods into a single larger block. The total size of the associated block chunks does not change after horizontal compaction. The horizontal compaction may significantly reduce the size of the index and the index-header kept in memory by store-gateways.

![Compactor - horizontal and vertical compaction](../../../images/compactor-horizontal-and-vertical-compaction.png)

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

## Scaling with concurrency

A sophisticated split-and-merge compaction algorithm can be tuned for clusters with large tenants. Configuration specifies both vertical and horizontal scaling of how the compactor runs as it compacts on a per-tenant basis.

- **Vertical scaling**<br />
  The setting `-compactor.compaction-concurrency` configures the max number of concurrent compactions running in a single compactor instance. Each compaction uses one CPU core.
- **Horizontal scaling**<br />
  When you run multiple compactor instances, compaction jobs will be sharded across those instances. CLI flag `-compactor.compactor-tenant-shard-size` or its respective YAML configuration option controls the spread of compaction jobs across the instances. If set to 0, compaction jobs will be spread across all available instances.

By design, the split-and-merge algorithm overcomes time series database (TSDB) index limitations, and it avoids situations in which compacted blocks grow indefinitely for a very large tenant at any compaction stage.

This compaction strategy is a two stage process: split and merge.
The default configuration disables the split stage.

For the first stage of compaction, for example `2h`, the compactor divides all source blocks into _N_ (`-compactor.split-groups`) groups. For each group, the compactor compacts the blocks, but instead of producing a single result block, it outputs _M_ (`-compactor.split-and-merge-shards`) blocks, known as _split blocks_. Each split block contains only a subset of the series belonging to a given shard out of _M_ shards. At the end of the split stage, the compactor produces _N \* M_ blocks with a reference to their respective shard in the block’s `meta.json` file.

For the second stage of compaction, the compactor merges the split blocks for each shard. This compacts all _N_ split blocks of a given shard. The merge reduces the number of blocks from _N \* M_ to _M_. For a given compaction time range, there will be a compacted block for each of the _M_ shards.

![Compactor - split-and-merge compaction strategy](../../../images/compactor-split-and-merge.png)

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

The merge stage then runs on other configured compaction time ranges, for example 12h and 24h. It compacts blocks belonging to the same shard.

This strategy is suitable for clusters with large tenants. The number of shards _M_ is configurable on a per-tenant basis using `-compactor.split-and-merge-shards`, and it can be adjusted based on the number of series of each tenant. The more a tenant grows in terms of series, the more you can grow the configured number of shards. Doing so improves compaction parallelization and keeps each per-shard compacted block size under control. We currently recommend 1 shard per every 25 to 30 million active series in a tenant. For example, for a tenant with 100 million active series, use approximately 4 shards.

The number of split groups, _N_, can also be adjusted per tenant using the `-compactor.split-groups` option. Increasing this value produces more compaction jobs with fewer blocks during the split stage. This allows multiple compactors to work on these jobs, and finish the splitting stage faster. However, increasing this value also generates more intermediate blocks during the split stage, which will only be reduced later in the merge stage.

If the configuration of `-compactor.split-and-merge-shards` changes during compaction, the change will affect only the compaction of blocks which have not yet been split. Split blocks will be merged using the old configuration. The old configuration is stored in the `meta.json` of each split block.

## Compactor sharding

The compactor shards compaction jobs, either from a single tenant or multiple tenants. The compaction of a single tenant can be split and processed by multiple compactor instances.

Whenever the pool of compactors grows or shrinks, tenants and jobs are resharded across the available compactor instances without any manual intervention.

Compactor sharding uses a [hash ring]({{< relref "../hash-ring.md" >}}). At startup, a compactor generates random tokens and registers itself to the compactor hash ring. While running, it periodically scans the storage bucket at every interval defined by `-compactor.compaction-interval`, to discover the list of tenants in storage and compact blocks for each tenant which hash matches the token ranges assigned to the instance itself within the hash ring.

To configure the compactors' hash ring, refer to [configuring hash rings]({{< relref "../../operating/configuring-hash-rings.md" >}}).

### Waiting for a stable hash ring at startup

A cluster cold start or an increase of two or more compactor instances at the same time may result each new compactor instance starting at a slightly different time. Then, each compactor runs its first compaction based on a different state of the hash ring. This is not an error condition, but it may be inefficient, because multiple compactor instances may start compacting the same tenant at nearly the same time.

To mitigate the issue, compactors can be configured to wait for a stable hash ring at startup. A ring is considered stable if no instance is added to or removedfrom the hash ring for at least `-compactor.ring.wait-stability-min-duration`. The maximum time the compactor will wait is controlled by the flag `-compactor.ring.wait-stability-max-duration` (or the respective YAML configuration option). Once the compactor has finished waiting, either because the ring stabilized or because the maximum wait time was reached, it will start up normally.

The default value of zero for `-compactor.ring.wait-stability-min-duration` disables waiting for ring stability.

## Compaction jobs order

The compactor allows configuring of the compaction jobs order via the `-compactor.compaction-jobs-order` flag (or its respective YAML config option). The configured ordering defines which compaction jobs should be executed first. The following values are supported:

- `compaction_jobs_order` = `smallest-range-oldest-blocks-first` (default)

  This ordering gives priority to smallest range, oldest blocks first.

  For example, with compaction ranges `2h, 12h, 24h`, the compactor will compact the 2h ranges first, and among them give priority to the oldest blocks. Once all blocks in the 2h range have been compacted, it moves to the 12h range, and finally to 24h one.

  All split jobs are moved to the front of the work queue, because finishing all split jobs in a given time range unblocks the merge jobs.

- `compaction_jobs_order` = `newest-blocks-first`

  This ordering gives priority to the most recent time ranges first, regardless of their compaction level.

  For example, with compaction ranges `2h, 12h, 24h`, the compactor compacts the most recent blocks first (up to the 24h range), and then moves to older blocks. This policy favours the most recent blocks, assuming they are queried the most frequently.

## Block deletion

Following a successful compaction, the original blocks are deleted from the storage. Block deletion is not immediate; it follows a two step process:

1. An original block is marked for deletion; this is a soft delete
1. Once a block has been marked for deletion for longer than the configurable `-compactor.deletion-delay`, the block is deleted from storage; this is a hard delete

The compactor is responsible for both marking blocks and for hard deletion them.
Soft deletion is based on a small `deletion-mark.json` file stored within the block location in the bucket, used both by querier and store-gateway components.

The soft delete mechanism gives time to queriers and store-gateways to discover the new compacted blocks before the original blocks are deleted. If those original blocks were immediately hard deleted, some queries involving the compacted blocks could fail. The queries would again succeed when the queriers and store-gateways rescanned the bucket to find both the deleted original blocks and the new compacted ones.

## Compactor disk utilization

The compactor needs to download blocks from the bucket to the local disk, and the compactor needs to store compacted blocks to the local disk before uploading them to the bucket. The largest tenants may need a lot of disk space.

Assuming `max_compaction_range_blocks_size` is the total block size for the largest tenant, knowing the longest `-compactor.block-ranges` period, the expression that estimates the minimum disk space required is:

```
compactor.compaction-concurrency * max_compaction_range_blocks_size * 2
```

Alternatively, assuming the largest `-compactor.block-ranges` is `24h` (the default), you could estimate needing 150GB of disk space for every 10M active series owned by the largest tenant. For example, if your largest tenant has 30M active series and `-compactor.compaction-concurrency=1`, we would recommend having a disk with at least 450GB available.

## Compactor configuration

Refer to the [compactor](../../../configuring/reference-configuration-parameters/#compactor)
block section and the [limits](../../../configuring/reference-configuration-parameters/#limits) block section for details of compaction-related configuration.

The [alertmanager]({{< relref "alertmanager.md" >}}) and [ruler]({{< relref "ruler.md" >}}) components can also use object storage to store their configurations and rules uploaded by users. In that case a separate bucket should be created to store alertmanager configurations and rules: using the same bucket between ruler/alertmanager and blocks will cause issues with the compactor.
