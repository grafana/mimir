---
title: "Configure owned series tracking"
menuTitle: "Owned series tracking"
description: "Learn how to configure owned series tracking in Grafana Mimir to improve series limit enforcement during ring topology changes."
weight: 100
---

# Configure owned series tracking

{{% admonition type="note" %}}
Owned series tracking is an experimental feature in Grafana Mimir 2.12 and later.
{{% /admonition %}}

## Problem

In a distributed Grafana Mimir deployment, ingesters use consistent hashing to determine which series they should store. During ring topology changes, such as scaling ingesters up or down, there can be a mismatch between the series an ingester stores locally and the series it should own according to the current ring state. This can lead to incorrect series limit enforcement.

Example scenario:

- Two series (S1 and S2) are sharded to ingester `a-0` and stored as in-memory series.
- You scale out and add ingester `a-1`.
- S2 is now pushed to the new replicas, while S1 stays on `a-0`.
- `a-0` still has two in-memory series (S1 and S2) until TSDB compaction garbage-collects S2, but it owns only one series (S1) according to the current ring state.

If you sum the in-memory series count after scaling up, the result is three (two from `a-0`, one from `a-1`), but the tenant is still pushing only two series.

## How owned series tracking works

Owned series tracking maintains a count of the series that an ingester should own based on the current ring state and token distribution. The process:

- Each series is hashed from all its labels and the tenant ID.
- The hash token is compared to the ingester's current token ranges derived from the ring.
- Only series whose tokens fall within the ingester's ranges are counted as owned series.
- The count is recomputed on ring changes and other triggers.

Triggers for recomputation include:

- Ring changed
- Shard size changed
- Local series limit changed
- New TSDB user
- Compaction events

## Before you begin

Owned series tracking requires:

- Zone-aware replication is enabled
- The replication factor is equal to the number of zones

For details, refer to the release notes for Grafana Mimir 2.12.

## Configure owned series tracking

To enable owned series tracking on ingesters:

```sh
# Enable tracking of owned series based on ring state
-ingester.track-ingester-owned-series=true

# How often to check for ring changes and recompute owned series
-ingester.owned-series-update-interval=15s
```

To use owned series for enforcing per-tenant series limits instead of in-memory series:

```sh
-ingester.use-ingester-owned-series-for-limits=true
```

## Monitor owned series

Ingesters expose the following metrics related to owned series:

- `cortex_ingester_owned_series`: Number of currently owned series per tenant
- `cortex_ingester_owned_target_info_series`: Number of currently owned `target_info` series per tenant
- `cortex_ingester_owned_series_check_duration_seconds`: How long the owned series check takes across tenants

## Related resources

- Configure [zone-aware replication]({{< relref "../configure-zone-aware-replication" >}})
- Refer to the [configuration parameters reference]({{< relref "../configuration-parameters/" >}})
- Read the [Grafana Mimir 2.12 release notes]({{< relref "../release-notes/v2.12" >}})


