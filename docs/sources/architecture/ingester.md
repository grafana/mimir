---
title: "Ingester"
description: "Overview of the ingester microservice."
weight: 10
---

# Ingester

The **ingester** service is responsible for writing incoming series to a [long-term storage backend](#storage) on the write path and returning in-memory series samples for queries on the read path.

Incoming series are not immediately written to the storage but kept in memory and periodically flushed to the storage (2 hours by default). For this reason, the [queriers](#querier) may need to fetch samples both from ingesters and long-term storage while executing a query on the read path.

Ingesters contain a **lifecycler** which manages the lifecycle of an ingester and stores the **ingester state** in the [hash ring](#the-hash-ring). Each ingester could be in one of the following states:

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

Ingesters are **semi-stateful**.

## Ingesters failure and data loss

If an ingester process crashes or exits abruptly, all the in-memory series that have not yet been flushed to the long-term storage will be lost. There are two main ways to mitigate this failure mode:

1. Replication
2. Write-ahead log (WAL)

**Replication** is used to hold multiple (typically 3) replicas of each time series in the ingesters. If the Mimir cluster loses an ingester, the in-memory series held by the lost ingester are replicated to at least one other ingester. In the event of a single ingester failure, no time series samples will be lost, while in the event that multiple ingesters fail, time series may potentially be lost if the failure affects all the ingesters holding the replicas of a specific time series.

The **write-ahead log** (WAL) is used to write to a persistent disk all incoming series samples until they're flushed to the long-term storage. In the event of an ingester failure, a subsequent process restart will replay the WAL and recover the in-memory series samples.

Contrary to the sole replication and given the persistent disk data is not lost, in the event of the failure of multiple ingesters, each ingester will recover the in-memory series samples from WAL upon subsequent restart. Replication is still recommended in order to ensure no temporary failures on the read path in the event of a single ingester failure.

## Ingesters write de-amplification

Ingesters store recently received samples in-memory in order to perform write de-amplification. If the ingesters would immediately write received samples to the long-term storage, the system would be difficult to scale due to the high pressure on the storage. For this reason, the ingesters batch and compress samples in-memory and periodically flush them out to the storage.

Write de-amplification is the main source of Mimir's low total cost of ownership (TCO).
