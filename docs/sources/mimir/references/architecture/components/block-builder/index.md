---
description: The block-builder builds TSDB blocks in ingest storage architecture.
menuTitle: (Optional) Block-builder
title: (Optional) Grafana Mimir block-builder
weight: 40
---

# (Optional) Grafana Mimir block-builder

The block-builder and block-builder-scheduler are optional components in the [ingest storage architecture](../../../../get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).

{{< admonition type="note" >}}
The block-builder and block-builder-scheduler are experimental components.
For more information, refer to [experimental features](../../../../configure/about-versioning/#experimental-features).
{{< /admonition >}}

The block-builder moves responsibility for producing blocks in [long-term storage](../../../../get-started/about-grafana-mimir-architecture/#long-term-storage)
from [ingesters](../ingester/) to a separate worker pool.

In the ingest storage architecture, to provide high availability for queries multiple ingesters consume ingested series data from Kafka.
When each ingester replica ships blocks to long-term storage, this produces blocks with duplicate series data. The [compactor](../compactor/) then
works through the shipped uncompacted blocks and deduplicate them.

Because Kafka already stores ingested samples durably, the block production doesn't benefit from following the ingester replication model.
The block-builder gives a separate block-production path that doesn't multiply the number of uncompacted blocks by a replication factor.
Ingesters become a query-serving cache for recently ingested series data. This decouples block-production from query-serving requirements.

## How block-builder works

The block-builder-scheduler coordinates work across block-builder workers. It oversees the backlog in the Kafka topic,
creates jobs, assigns them to block-builders, and tracks their progress. It keeps its active job queue in memory and stores progress as Kafka consumer group offsets.

The following flow describes interaction between block-builder-scheduler, block-builder and Kafka:

1. The scheduler monitors the Kafka topic that holds series data. From offset ranges in each topic partition, it creates jobs that it schedules for block-builder workers to pick up.
1. A block-builder leases a job from the scheduler.
1. The block-builder consumes the range of records from a Kafka topic partition that the job covers.
1. The block-builder creates TSDB blocks per tenant locally and uploads them to long-term object storage.
1. After the upload succeeds, the block-builder reports completion to the block-builder-scheduler.
1. The scheduler advances the committed Kafka offsets after jobs complete in partition order.

The block-builder-scheduler is a singleton process: there must be only one active scheduler replica. It can't coordinate its state between its own replicas.

On a restart, the scheduler first reconstructs its state from Kafka consumer group offsets and pings from active block-builder workers.

The block-builders are stateless workers. They use local disk only as a disposable working area to build TSDB blocks.
If a worker fails to process its job, the scheduler assigns the unfinished job to be reprocessed by another worker.

The block-builders can be scaled horizontally in response to demand from the scheduler.

### Job model

The block-builder-scheduler divides time into job buckets of size `-block-builder-scheduler.job-size` (default 1h). When the current wall-clock time crosses
a bucket boundary, the scheduler emits a job covering the Kafka offsets that were produced during the previous bucket.

A job is defined by `(topic, partition, startOffset, endOffset)` tuple. The scheduler tracks committed and planned
offsets per partition and advances them as jobs complete.

## Migrate to block-builder architecture

Migration of a running Grafana Mimir installation to the block-builder architecture is a multistep process.

{{< admonition type="warning" >}}
The block-builder requires Grafana Mimir to run in the [ingest storage architecture](../../../../get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).
{{< /admonition >}}

{{< admonition type="note" >}}
During the outlined migration process both ingesters and block-builders upload TSDB blocks to long-term object storage.
Because the two systems are performing the same work during this period of overlap, the uploaded blocks will contain duplicate versions of the same samples. The compactor will deduplicate these blocks,
but the additional uncompacted blocks from the block-builder temporarily increase compactor and store-gateway load.
{{< /admonition >}}

### 1. Deploy the scheduler and block-builders

{{< admonition type="note" >}}
The following examples show only basic component-specific flags. Refer to block-builder Jsonnet files in the [Mimir repository](https://github.com/grafana/mimir/tree/main/operations/mimir)
for the source of truth for all configuration details.
{{< /admonition >}}

Deploy the block-builder-scheduler:

```sh
mimir \
  -target=block-builder-scheduler \
  -ingest-storage.enabled=true \
  -ingest-storage.kafka.address=kafka:9092 \
  -ingest-storage.kafka.topic=ingest \
  -block-builder-scheduler.lookback-on-no-commit=1h
```

The `-block-builder-scheduler.lookback-on-no-commit` flag controls where the scheduler starts when its Kafka consumer group has no committed offset.
Choose a duration that covers the data you want the block-builder to rebuild without creating an unnecessary initial backlog.

{{< admonition type="note" >}}
The `-block-builder-scheduler.lookback-on-no-commit` flag defaults to `6h`.
Set it explicitly on the initial deployment to control how much retained data the block-builder processes before it catches up with the tip of the Kafka topic.
{{< /admonition >}}

Deploy a pool of block-builder workers:

```sh
mimir \
  -target=block-builder \
  -ingest-storage.enabled=true \
  -ingest-storage.kafka.address=kafka:9092 \
  -ingest-storage.kafka.topic=ingest \
  -block-builder.data-dir=/data/tsdb \
  -block-builder.scheduler.address=block-builder-scheduler:9095
```

Both components must connect to the same Kafka backend and consume from the same topic that the ingesters use.

For the list of available options, refer to [Configure the Grafana Mimir Kafka backend](../../../../configure/configure-kafka-backend/)
and [Grafana Mimir configuration parameters](../../../../configure/configuration-parameters/).

The block-builder data directory is temporary and doesn't need to persist across restarts.
Block-builders must also use the same object storage and runtime limit configurations as the rest of the Mimir components.

### 2. Verify block-builder operation

Wait for the block-builders to catch up with the backlog, while ingesters continue to upload blocks.
Refer the [Mimir / Block-builder dashboard](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin/dashboards/block-builder.libsonnet) to observe the process (while the block-builder is experiment, the dashboard is [disabled by default in "mimir-mixin"](https://github.com/grafana/mimir/blob/release-3.2/operations/mimir-mixin/config.libsonnet#L268-L269)).
Verify that jobs complete, committed offsets advance, blocks reach object storage, and the compactor and store-gateway remain healthy.

### 3. Disable ingester block shipping

After the block-builder path is stable, update ingesters to disable block shipping:

```sh
-blocks-storage.tsdb.ship-interval=0s
-blocks-storage.tsdb.close-idle-tsdb-when-shipping-disabled=true
```

After the ingesters no longer ship blocks, the block-builders become the only producers of uncompacted blocks.
The ingesters continue to serve queries for recently ingested series data.
