---
aliases:
  - ../../../operators-guide/architecture/components/ingester/
description: The ingester writes incoming series to long-term storage.
menuTitle: Ingester
title: Grafana Mimir ingester
weight: 30
---

# Grafana Mimir ingester

The ingester is a stateful component that writes incoming series to [long-term storage]({{< relref "../../../get-started/about-grafana-mimir-architecture#long-term-storage" >}}) on the write path and returns series samples for queries on the read path.

Incoming time series data from [distributors]({{< relref "./distributor" >}}) are temporarily stored in the ingester's memory or offloaded to disk before being written to long-term storage.
Eventually, all series are written to disk and periodically uploaded (by default every two hours) to the long-term storage.
For this reason, the [queriers]({{< relref "./querier" >}}) might need to fetch samples from both ingesters and long-term storage while executing a query on the read path.

Any Grafana Mimir component that calls the ingesters starts by first looking up ingesters registered in the [hash ring]({{< relref "../hash-ring" >}}) to determine which ingesters are available.
Each ingester could be in one of the following states:

- `PENDING`<br />
  The ingester has just started. While in this state, the ingester does not receive write or read requests.
- `JOINING`<br />
  The ingester starts up and joins the ring. While in this state, the ingester does not receive write or read requests.
  The ingester loads tokens from disk (if `-ingester.ring.tokens-file-path` is configured) or generates a set of new random tokens.
  Finally, the ingester optionally observes the ring for token conflicts, and once resolved, moves to the `ACTIVE` state.
- `ACTIVE`<br />
  The ingester is up and running. While in this state, the ingester can receive both write and read requests.
- `LEAVING`<br />
  The ingester is shutting down and leaving the ring. While in this state, the ingester doesn't receive write requests, but can still receive read requests.
- `UNHEALTHY`<br />
  The ingester has failed to heartbeat to the hash ring. While in this state, distributors bypass the ingester, which means that the ingester does not receive write or read requests.

To configure the ingesters' hash ring, refer to [configuring hash rings]({{< relref "../../../configure/configure-hash-rings" >}}).

## Ingesters write de-amplification

Ingesters store recently received samples in-memory in order to perform write de-amplification.
If the ingesters immediately write received samples to the long-term storage, the system would have difficulty scaling due to the high pressure on the long-term storage.
For this reason, the ingesters batch and compress samples in-memory and periodically upload them to the long-term storage.

Write de-amplification is a key factor in reducing Mimir's total cost of ownership (TCO).

## Ingesters failure and data loss

If an ingester process crashes or exits abruptly, any in-memory time series data that have not yet been uploaded to long-term storage might be lost.
There are the following ways to mitigate this failure mode:

- Replication
- Write-ahead log (WAL)
- Write-behind log (WBL), only used if out-of-order ingestion is enabled.

### Replication and availability

Writes to the Mimir cluster are successful if a majority of ingesters received the data. With the default replication factor of 3, this means 2 out of 3 writes to ingesters must succeed.
If the Mimir cluster loses a minority of ingesters, the in-memory series samples held by the lost ingesters are available in at least one other ingester, meaning no time series samples are lost.
If a majority of ingesters fail, time series might be lost if the failure affects all the ingesters holding the replicas of a specific time series.

{{< admonition type="note" >}}
Replication only happens at write time. If an ingester is unavailable during a period when writes are actively being written to other ingesters, that particular ingester will never recover those missed samples.
{{< /admonition >}}

### Write-ahead log

The write-ahead log (WAL) writes all incoming series to a persistent disk until the series are uploaded to the long-term storage.
If an ingester fails, a subsequent process restart replays the WAL and recovers the in-memory series samples.

Unlike sole replication, the WAL ensures that in-memory time series data are not lost in the case of multiple ingester failures. Each ingester can recover the data from the WAL after a subsequent restart.

Replication is still recommended in order to gracefully handle a single ingester failure.

### Write-behind log

The write-behind log (WBL) is similar to the WAL, but it only writes incoming out-of-order samples to a persistent disk until the series are uploaded to long-term storage.

There is a different log for this because it is not possible to know if a sample is out-of-order until Mimir tries to append it.
First Mimir needs to attempt to append it, the TSDB will detect that it is out-of-order, append it anyway if out-of-order is enabled and then write it to the log.

If the ingesters fail, the same characteristics as in the WAL apply.

## Zone aware replication

Zone aware replication ensures that the ingester replicas for a given time series are divided across different zones.
Zones can represent logical or physical failure domains, for example, different data centers.
Dividing replicas across multiple zones prevents data loss and service interruptions when there is a zone-wide outage.

To set up multi-zone replication, refer to [Configuring zone-aware replication]({{< relref "../../../configure/configure-zone-aware-replication" >}}).

## Shuffle sharding

Shuffle sharding can be used to reduce the effect that multiple tenants can have on each other.

For more information on shuffle sharding, refer to [Configuring shuffle sharding]({{< relref "../../../configure/configure-shuffle-sharding" >}}).

## Out-of-order samples ingestion

Out-of-order samples are discarded by default. If the system writing samples to Mimir produces out-of-order samples, you can enable ingestion of such samples.

For more information about out-of-order samples ingestion, refer to [Configuring out of order samples ingestion]({{< relref "../../../configure/configure-out-of-order-samples-ingestion" >}}).

## Read-only mode

You can put ingesters in "read-only" mode through calling the [Prepare Instance Ring Downscale]({{< relref "../../http-api/index.md#prepare-instance-ring-downscale" >}}) API endpoint.
Ingesters in read-only mode don't receive write requests, but can still receive read requests.
Ingesters in read-only mode are not part of the shuffle shard for the write operation.

Read-only mode is useful in downscaling scenarios and is a preparation for later shutdown of the ingester.

Ingester ring states like JOINING, ACTIVE, or LEAVING are separate from read-only mode.
An ingester that is both ACTIVE and read-only does not receive write requests.
Read-only mode is stored in the hash ring as well.
