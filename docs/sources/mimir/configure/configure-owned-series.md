---
title: "Configure owned series tracking"
menuTitle: "Owned series tracking"
description: "Learn how to configure owned series tracking in Grafana Mimir to improve series limit enforcement during ring topology changes."
weight: 100
---

# Configure owned series tracking

{{% admonition type="note" %}}
Owned series tracking is an experimental feature in Grafana Mimir versions 2.12 and later.
{{% /admonition %}}

In a distributed Grafana Mimir deployment, ingesters use consistent hashing to determine which series they should store. During ring topology changes, such as scaling ingesters up or down, there can be a mismatch between the series an ingester stores locally and the series it should own according to the current ring state. This mismatch can cause inaccurate series limit enforcement.

Owned series tracking addresses this problem by counting, per tenant, only the series an ingester should own based on current ring token ranges and shard configuration. When you enable owned series tracking and use owned series for limits, Mimir enforces limits against series ownership rather than transient in-memory state during ring changes.

For example:

- Two series, S1 and S2, are sharded to ingester `a-0` and stored as in-memory series.
- You scale out and add ingester `a-1`.
- Mimir routes S2 to the new replicas, while S1 stays on `a-0`.
- `a-0` still has two in-memory series, S1 and S2, until TSDB compaction garbage-collects S2, but it owns only one series, S1, according to the current ring state.

After scaling up, the in-memory count is three (two on `a-0`, one on `a-1`), even though the tenant still pushes only two series.

## How owned series tracking works

Owned series tracking maintains a count of the series that an ingester should own based on the current ring state and token ranges.

The following process describes owned series tracking:

1. Each series is hashed from all its labels and the tenant ID.
1. The hash token is compared to the ingester's current token ranges derived from the ring.
1. Only series whose tokens fall within the ingester's ranges are counted as owned series.
1. The count is recomputed on ring changes and other triggers.

Triggers for recomputation include the following:

- Ring changes
- Shard size changes
- Local series limit changes
- New TSDB user
- Compaction events

## Before you begin

Complete the following tasks before configuring owned series tracking:

- Enable [zone-aware replication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-zone-aware-replication/).
- Set the replication factor equal to the number of zones.

## Configure owned series tracking

1.  Enable owned series tracking on ingesters. For example:

    ```sh
    # Enable tracking of owned series based on ring state
    -ingester.track-ingester-owned-series=true

    # How often to check for ring changes and recompute owned series
    -ingester.owned-series-update-interval=15s
    ```

1.  Use owned series to enforce per-tenant series limits. For example:

        ```sh
        -ingester.use-ingester-owned-series-for-limits=true
        ```

    For more information, refer to [ingester](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/#ingester) in Grafana Mimir configuration parameters.

## Monitor owned series

Ingesters expose the following metrics related to owned series:

| Metric name                                           | Description                                               |
| ----------------------------------------------------- | --------------------------------------------------------- |
| `cortex_ingester_owned_series`                        | Number of currently owned series per tenant               |
| `cortex_ingester_owned_target_info_series`            | Number of currently owned `target_info` series per tenant |
| `cortex_ingester_owned_series_check_duration_seconds` | How long the owned series check takes across tenants      |
