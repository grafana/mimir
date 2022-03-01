---
title: "Compactor"
description: "Overview of the compactor component."
weight: 10
---

# Compactor

The compactor is responsible for merging and deduplicating smaller blocks into larger ones, in order to reduce the number of blocks stored in long-term (object) storage for a given tenant and allowing for them to be queried more efficiently. It also keeps the [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}) updated and, for this reason, it's a required component.

The [alertmanager]({{< relref "./alertmanager.md" >}}) and [ruler]({{< relref "./ruler.md" >}}) components can also use object storage to store their configurations and rules uploaded by users. In that case a separate bucket should be created to store alertmanager configurations and rules: using the same bucket between ruler/alertmanager and blocks will cause issues with the compactor.

The **compactor** is a component responsible for:

- Compacting multiple blocks of a given tenant into a single optimized larger block. This helps reducing storage costs (deduplication, index size reduction), and increasing query speed (querying fewer blocks is faster).
- Keeping the per-tenant bucket index updated. The [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}) is used by [queriers]({{< relref "./querier.md" >}}), [store-gateways]({{< relref "./store-gateway.md" >}}), and rulers to discover new blocks in the storage.
- Deleting blocks which are no longer within retention period.

The compactor is **stateless**.

## How compaction works

Block compaction has two main benefits:

1. Vertically compact blocks uploaded by all ingesters for the same time range
2. Horizontally compact blocks with small time ranges into a single larger block

The **vertical compaction** merges all the blocks of a tenant uploaded by ingesters for the same time range (2 hours ranges by default) into a single block, while **de-duplicating samples** that are originally written to N blocks as a result of replication. This step reduces the number of blocks for a single 2 hours time range from #(number of ingesters) to 1 per tenant.

The **horizontal compaction** triggers after the vertical compaction and compacts several blocks with adjacent 2-hour range periods into a single larger block. Even though the total size of block chunks doesn't change after this compaction, it may still significantly reduce the size of the index and the index-header kept in memory by store-gateways.

![Compactor - horizontal and vertical compaction](../../images/compactor-horizontal-and-vertical-compaction.png)

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

## Compaction strategy

Mimir's sophisticated `split-and-merge` compaction strategy allows you to both vertically and horizontally scale the compaction of a single tenant:

- **Vertical scaling**<br />
  The setting `-compactor.compaction-concurrency` allows you to configure the max number of concurrent compactions running in a single compactor replica (each compaction uses 1 CPU core).
- **Horizontal scaling**<br />
  When you run multiple compactor replicas, compaction jobs will be sharded across them. Use the CLI flag `-compactor.compactor-tenant-shard-size` (or its respective YAML config option) to control how many of the available replicas to spread compaction jobs across. If set to 0, compaction jobs will be spread across all available replicas.

By design, the `split-and-merge` compaction strategy overcomes TSDB index limitations and avoids situations where compacted blocks grow indefinitely for a very large tenant (at any compaction stage).

This compaction strategy is a two stage process: split and merge.

For the configured first level of compaction, for example `2h`, the compactor divides all source blocks into _N_ (`-compactor.split-groups`) groups. For each group, the compactor compacts together the blocks, but instead of producing a single result block, it outputs _M_ (`-compactor.split-and-merge-shards`) blocks, which are called _split_ blocks. Each split block contains only a subset of the series belonging to a given shard out of _M_ shards. At the end of the split stage, the compactor produces _N \* M_ blocks with a reference to their respective shard in the block’s `meta.json` file.

Given the split blocks, the compactor then runs the **merge** stage for each shard, which compacts together all _N_ split blocks of a given shard. Once this stage is completed, the number of blocks will be reduced from _N \* M_ to _M_. Given a compaction time range, we'll have a compacted block for each of the _M_ shards.

The merge stage is then run for subsequent compaction time ranges (eg. 12h, 24h), compacting together blocks belonging to the same shard (_not shown in the picture below_).

![Compactor - split-and-merge compaction strategy](../images/compactor-split-and-merge.png)

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

This strategy is suitable for clusters with large tenants. The number of shards _M_ is configurable on a per-tenant basis using `-compactor.split-and-merge-shards`, and it can be adjusted based on the number of series of each tenant. The more a tenant grows in terms of series, the more you can grow the configured number of shards. Doing so improves compaction parallelization and keeps each per-shard compacted block size under control. We currently recommend 1 shard per every 25 to 30 million active series in a tenant. For example, for a tenant with 100 million active series, use approximately 4 shards.

The number of split groups, _N_, can also be adjusted per tenant using the `-compactor.split-groups` option. Increasing this value produces more compaction jobs with fewer blocks during the split stage. This allows multiple compactors to work on these jobs, and finish the splitting stage faster. However, increasing this value also generates more intermediate blocks during the split stage, which will only be reduced later in the merge stage.

Each compaction stage (both split and merge) planned by the compactor can be horizontally scaled. Non conflicting / overlapping jobs will be executed in parallel.

### How does compaction behave if `-compactor.split-and-merge-shards` changes?

In case you change the `-compactor.split-and-merge-shards` setting, the change will affect only the compaction of blocks which haven't been split yet. Blocks which have already run through the split stage will not be split again to produce a number of shards equal to the new setting, but will be merged keeping the old configuration (this information is stored in the `meta.json` of each split block).

## Compactor sharding

The compactor shards compaction jobs, either from a single tenant or multiple tenants. The compaction of a single tenant can be split and processed by multiple compactor instances.

Whenever the pool of compactors grows or shrinks (ie. following up a scale up/down), tenants / jobs are resharded across the available compactor instances without any manual intervention.

The compactor sharding is based on the Mimir [hash ring]({{< relref "./about-the-hash-ring.md" >}}). At startup, a compactor generates random tokens and registers itself to the ring. While running, it periodically scans the storage bucket at every interval defined by `-compactor.compaction-interval`, to discover the list of tenants in storage and compact blocks for each tenant which hash matches the token ranges assigned to the instance itself within the ring.

This feature requires the backend [hash ring]({{< relref "./about-the-hash-ring.md" >}}) to be configured via `-compactor.ring.*` flags (or their respective YAML config options).

### Waiting for stable ring at startup

In the event of a cluster cold start or a scale up of 2+ compactor instances at the same time, we may end up in a situation where each new compactor instance starts at a slightly different time and thus each one runs the first compaction based on a different state of the ring. This is not a critical condition, but may be inefficient, because multiple compactor replicas may start compacting the same tenant nearly at the same time.

To reduce the likelihood of this happening, the compactor can wait for a stable ring at startup. A ring is considered stable if no instance is added/removed to the ring for at least `-compactor.ring.wait-stability-min-duration`. The maximum time the compactor will wait is controlled by the flag `-compactor.ring.wait-stability-max-duration` (or respective YAML option). Once the compactor has finished waiting (either because the ring stabilized or the maximum wait time was reached), it will proceed to start up normally.

The default value for `-compactor.ring.wait-stability-min-duration` is zero, meaning that waiting for ring stability is disabled unless you provide a higher value (f.ex. `-compactor.ring.wait-stability-min-duration=1m`).

## Compaction jobs order

The compactor allows configuring of the compaction jobs order via the `-compactor.compaction-jobs-order` flag (or its respective YAML config option). The configured ordering defines which compaction jobs should be executed first. The following values are supported:

- `smallest-range-oldest-blocks-first` (default)
- `newest-blocks-first`

### `smallest-range-oldest-blocks-first`

This ordering gives priority to smallest range, oldest blocks first.

For example, let's assume that you run the compactor with the compaction ranges `2h, 12h, 24h`. The compactor will compact the 2h ranges first, and among them give priority to the oldest blocks. Once all blocks in the 2h range have been compacted, it moves to the 12h range and finally to 24h one.

All split jobs are moved to the front of the work queue, because finishing all split jobs in a given time range unblocks the merge jobs.

### `newest-blocks-first`

This ordering gives priority to most recent time ranges first, regardless of their compaction level.

Let's assume you run the compactor with the compaction ranges `2h, 12h, 24h`. With this sorting the compactor compacts the most recent blocks first (up to the 24h range) and then moves to older blocks. This policy favours the most recent blocks, assuming they are queried the most frequently.

## Soft and hard blocks deletion

When the compactor successfully compacts some source blocks into a larger block, source blocks are deleted from the storage. Block deletion is not immediate, but follows a two step process:

1. First, a block is **marked for deletion** (soft delete)
2. Then, once a block is marked for deletion for longer than `-compactor.deletion-delay`, it is **deleted** from the storage (hard delete)

The compactor is both responsible for marking blocks for deletion and then hard deleting them once the deletion delay expires.
The soft deletion is based on a tiny `deletion-mark.json` file stored within the block location in the bucket, which gets looked up both by queriers and store-gateways.

This soft deletion mechanism is used to give enough time to queriers and store-gateways to discover the new compacted blocks before the old source blocks are deleted. If source blocks would be immediately hard deleted by the compactor, some queries involving the compacted blocks may fail until the queriers and store-gateways have rescanned the bucket and found both the deleted source blocks and the new compacted ones.

## Compactor disk utilization

The compactor needs to download source blocks from the bucket to the local disk, and to store the compacted block to the local disk before uploading it to the bucket. Depending on the largest tenants in your cluster and the configured `-compactor.block-ranges`, the compactor may need a lot of disk space.

Assuming `max_compaction_range_blocks_size` is the total size of blocks for the largest tenant (you can measure it inspecting the bucket) and the longest `-compactor.block-ranges` period, the formula to estimate the minimum disk space required is:

```
min_disk_space_required = compactor.compaction-concurrency * max_compaction_range_blocks_size * 2
```

Alternatively, assuming the largest `-compactor.block-ranges` is `24h` (default), you could consider 150GB of disk space every 10M active series owned by the largest tenant. For example, if your largest tenant has 30M active series and `-compactor.compaction-concurrency=1` we would recommend having a disk with at least 450GB available.

## Compactor configuration

Refer to the [compactor](../../configuration/reference-configuration-parameters/#compactor)
block section for details of compactor-related configuration.
