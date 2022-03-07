---
title: "Ingester"
description: "Overview of the ingester microservice."
weight: 10
---

# Ingester

The **ingester** service is responsible for writing incoming series to [long-term storage]({{< relref "../_index.md#long-term-storage" >}}) on the write path and returning series samples for queries on the read path.

Incoming series from [distributors]({{< relref "distributor.md" >}}) are not immediately written to the long-term storage but kept in-memory or offloaded to disk. Eventually all series are written to disk and are periodically uploaded to the long-term storage (every 2 hours by default). For this reason, the [queriers]({{< relref "querier.md" >}}) may need to fetch samples both from ingesters and long-term storage while executing a query on the read path.

Services calling the **ingesters** first read the **ingester states** from the [hash ring]({{< relref "../hash-ring.md" >}}) to determine which ingester(s) are available. Each ingester could be in one of the following states:

- **`PENDING`**<br />
  The ingester has just started. While in this state, the ingester receives neither write nor read requests.
- **`JOINING`**<br />
  The ingester is starting up and joining the ring. While in this state the ingester receives neither write nor read requests. The ingester will load tokens from disk (if `-ingester.ring.tokens-file-path` is configured) or generate a set of new random tokens. Finally, the ingester optionally observes the ring for token conflicts, and once resolved, will move to the `ACTIVE` state.
- **`ACTIVE`**<br />
  The ingester is up and running. While in this state the ingester can receive both write and read requests.
- **`LEAVING`**<br />
  The ingester is shutting down and leaving the ring. While in this state the ingester doesn't receive write requests, but could still receive read requests.
- **`UNHEALTHY`**<br />
  The ingester has failed to heartbeat to the ring's KV Store. While in this state, distributors skip the ingester while building the replication set for incoming series and the ingester does not receive write or read requests.

To configure the ingesters' hash ring, refer to [configuring hash rings]({{< relref "../../operating-grafana-mimir/configure-hash-ring.md" >}}).

## Ingesters write de-amplification

Ingesters store recently received samples in memory in order to perform write de-amplification. If the ingesters immediately write received samples to the long-term storage, it is difficult for the system to scale due to the high pressure on the long-term storage. For this reason, the ingesters batch and compress samples in memory and periodically upload them to the long-term storage.

Write de-amplification is the main source of Mimir's low total cost of ownership (TCO).

## Ingesters failure and data loss

Ingesters are **stateful**. If an ingester process crashes or exits abruptly, all the in-memory series that have not yet been uploaded to the long-term storage could be lost. There are two main ways to mitigate this failure mode:

1. Replication
2. Write-ahead log (WAL)

**Replication** is used to hold multiple (3 by default) replicas of each time series in the ingesters. Writes to the Mimir cluster are successful if a quorum of ingesters received the data (minimum 2 in case of replication factor 3). If the Mimir cluster loses an ingester, the in-memory series held by the lost ingester are replicated to at least one other ingester. In the event of a single ingester failure, no time series samples will be lost, while in the event that multiple ingesters fail, time series may potentially be lost if the failure affects all the ingesters holding the replicas of a specific time series.

The **write-ahead log** (WAL) is used to write to a persistent disk all incoming series samples until they're uploaded to the long-term storage. In the event of an ingester failure, a subsequent process restart will replay the WAL and recover the in-memory series samples.

Contrary to the sole replication and given the persistent disk data is not lost, in the event of the failure of multiple ingesters, each ingester will recover the in-memory series samples from WAL upon subsequent restart. Replication is still recommended in order to gracefully handle a single ingester failure.

## Zone aware replication

**Zone aware replication** ensures that the ingester replicas for a given time series are spread across different zones.
Zones may represent logical or physical failure domains, for example different data centers.
Spreading replicas across multiple zones prevents data loss and service interruptions when there is a zone wide outage.

To set up multi-zone replication, refer to [Configuring zone-aware replication]({{< relref "../../operating-grafana-mimir/configure-zone-aware-replication.md" >}}).

## Shuffle sharding

**Shuffle sharding** (off by default) can be used to reduce the effect that multiple tenants can have on each other.

For more information on shuffle sharding, refer to [configure shuffle sharding]({{< relref "../../operating-grafana-mimir/configure-shuffle-sharding.md" >}}).
