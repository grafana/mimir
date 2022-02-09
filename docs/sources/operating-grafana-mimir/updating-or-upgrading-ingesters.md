---
title: "Updating or upgrading ingesters"
description: ""
weight: 10
---

Mimir [ingesters](architecture.md#ingester) are semi-stateful.
A running ingester holds several hours of time series data in memory, before they're flushed to the long-term storage.
When an ingester shutdowns, because of a rolling update or maintenance, the in-memory data must not be discarded in order to avoid any data loss.

In this document we describe the techniques employed to safely handle rolling updates, based on different setups:

- [Blocks storage](#blocks-storage)

_If you're looking how to scale up / down ingesters, please refer to the [dedicated guide](./ingesters-scaling-up-and-down.md)._

## Blocks storage

The Mimir [blocks storage](../blocks-storage/_index.md) requires ingesters to run with a persistent disk where the TSDB WAL and blocks are stored (eg. a StatefulSet when deployed on Kubernetes).

During a rolling update, the leaving ingester closes the open TSDBs, synchronize the data to disk (`fsync`) and releases the disk resources.
The new ingester, which is expected to reuse the same disk of the leaving one, will replay the TSDB WAL on startup in order to load back in memory the time series that have not been compacted into a block yet.

### Observability

The following metrics can be used to observe this process:

- **`cortex_member_ring_tokens_owned`**<br />
  How many tokens each ingester thinks it owns.
- **`cortex_ring_tokens_owned`**<br />
  How many tokens each ingester is seen to own by other components.
- **`cortex_ring_member_ownership_percent`**<br />
  Same as `cortex_ring_tokens_owned` but expressed as a percentage.
- **`cortex_ring_members`**<br />
  How many ingesters can be seen in each state, by other components.

You can see the current state of the ring via http browser request to
`/ring` on a distributor.
