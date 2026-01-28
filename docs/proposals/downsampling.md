---
title: "XXXX: Downsampling"
description: "Downsampling"
draft: true
---

# XXXX: Downsampling

**Author:** @wilfriedroset

**Date:** 05/2023

**Sponsor(s):** @username of maintainer(s) willing to shepherd this MID

**Type:** Feature

**Status:** Draft

**Related issues/PRs:**

---

## Background

The `compactor` is a component that improves query performance and storage efficiency by consolidating multiple smaller and/or sparse blocks into larger, optimized ones.
The `query-frontend` is a component that is responsible for query acceleration through splitting, sharding, caching, and other techniques.

## Problem statement

Tenants may either scrape samples at a high frequency (e.g: ~1Hz) or query data over a wide time range (e.g: 1 year query for KPIs or QoS dashboards).
In both cases, the full resolution is not needed unless the tenant needs to zoom-in the time range.

Querying such data can be slow or expensive due to the number of samples to pull from the storage and the related processing.

Downsampling serves two distinct motivations:

1. **Query acceleration** – a separate set of metric segments containing only downsampled data allows wide‑range queries to retrieve and sort far fewer samples, improving latency and reducing load on the query‑frontend.
2. **Storage optimization** – by discarding the bulk of original‑frequency samples after a defined age (e.g., keeping only one‑hour samples for segments older than six months), the system can retain data for much longer periods on the same storage capacity.

## Goals

- faster wide range queries.

## Non-Goals

- storage cost optimization.
- different retention between raw and downsampled samples.
- allow tenants to define their own downsampling rules or steps.

## Proposals

### Proposal 0: Do nothing

Current query acceleration techniques are sufficient when high frequency sampling is not used by the tenants or when wide time range queries are rare.

**Pros:**

- Nothing to be done
- Mimir's `querier` and `store-gateway` can be scaled horizontally to improve response time
- Mimir's `ruler` can be used by tenants to define their own recording rules, as such it is possible to downsample data.

**Cons:**

- Hard to know beforehand when `querier` and `store-gateway` need scaling. Even with auto scaling the first queries will be slow which impacts the user experience.
- Chunks pulled from the `store-gateway` could take a significant disk space.
- The `results-cache` could grow in size or be flushed to be able to store the results of a wide range query.

### Proposal 1: Downsampling via compactor with flexible configuration

Taking inspiration from [Thanos' compactor implementation](https://github.com/thanos-io/thanos/tree/main/pkg/compact), the `compactor` would be responsible to downsample 24h-long blocks.

Downsampled data is stored in separated blocks as such full resolution blocks stays immutable.

For each raw data blocks the downsampling process produces _N_ blocks, _N_ being the length of the list age/resolution couple.

Example global configuration applied to all tenants:

```yaml
compactor:
  downsampling:
    - 1d:1m # After 1d apply downsampling and keep 1 sample per minute
    - 2d:5m
    - 2w:1h
```

Example of an override per tenant basis:

```yaml
overrides:
  tenant1:
    downsampling:
      - 1d:1m # After 1d apply downsampling and keep 1 sample per minute
      - 5d:5m
      - 4w:1h
  tenantX:
    downsampling: [] # downsampling disabled
```

Downsampled blocks contain several aggregation:

- count: number of samples in current window.
- sum: sum of current window.
- min: min of current window.
- max: max of current window.
- counter: total counter state since beginning.

**Pros:**

- Operationally simple as `compactor` already exists.
- Takes full advantage of Mimir's compactor shuffle sharding and horizontal scaling.
- Flexible configuration on a per tenant basis with general configuration.

**Cons:**

- Increased load on compactors, increased I/O and objectstorage call.
- Reusing `compactor` can slow down usual compaction.
- Initial downsampling can take a long time and impact the usual compaction.
- Given the flexibility of the configuration a resolution might exists only for a short period of time.

### Proposal 2: Downsampling via compactor with fixed configuration

This proposal is similar to _Proposal #1_ with one modification: the configuration is only a flag to enable/disable the downsampling.
Mimir will apply the same downsampling to all tenants for which it is enable.
Apart from removing flexibility in the configuration the rest is identical to _Proposal #1_.

Example global configuration applied to all tenants:

```yaml
compactor: [downsampling_enabled: <boolean> | default = false]
```

Example of an override per tenant basis:

```yaml
overrides:
  tenant1::
    downsampling_enabled: true
```

**Pros:**

- Operationally simple as `compactor` already exists.
- Takes full advantage of Mimir's compactor shuffle sharding and horizontal scaling.
- Simple configuration which produces a deterministic result even enabling/disabling downsampling back and forth.

**Cons:**

- Downsampling steps are not flexible.
- Increased load on compactors, increased I/O and objectstorage call.
- Reusing `compactor` can slow down usual compaction.
- Initial downsampling can take a long time and impact the usual compaction.

### Proposal 3: Downsampling in a dedicated component with flexible configuration

Based on the `compactor` we would create a new component `downsampler`. This component would be only responsible for downsampling 24h blocks created by the `compactor`.
Apart from being a dedicated component the rest is identical to _Proposal #1_.

**Pros:**

- Downsampling doesn't impact the usual compaction
- Flexible configuration on a per tenant basis with general configuration.

**Cons:**

- Add one more component, one more ring
- Improvements made to the `compactor` will need to be backported to the `downsampler`.
- Initial downsampling can take a long time and impact the usual compaction.
- Given the flexibility of the configuration a resolution might exists only for a short period of time.

### Proposal 4: Downsampling in a dedicated component with fixed configuration

Based on the `compactor` we would create a new component `downsampler`. This component would be only responsible for downsampling 24h blocks created by the `compactor`.
Apart from being a dedicated component the rest is identical to _Proposal #2_.

**Pros:**

- Downsampling doesn't impact the usual compaction
- Simple configuration which produces a deterministic result even enabling/disabling downsampling back and forth.

**Cons:**

- Add one more component, one more ring
- Improvements made to the `compactor` will need to be backported to the `downsampler`.
- Downsampling steps are not flexible.

## Other Notes

If this feature were to be used in conjunction with a selective retention between raw and downsampling data, this would prevent the increase of the storage footprint.
Note that the selective retention is not addressed by this proposal.

Example of selective retention:

```yaml
limits:
  # -compactor.blocks-retention-period Delete raw blocks containing samples older than the specified retention period. Also used by query-frontend to avoid querying beyond the retention period. 0 to disable.
  compactor_blocks_retention_period: 90d # keep raw data for 90d
  # -compactor.blocks-retention-period-5m Delete 5m downsampled blocks containing samples older than the specified retention period. Also used by query-frontend to avoid querying beyond the retention period. 0 to disable.
  compactor_blocks_downsampling_5m_retention_period: 180d # keep 5m downsampling for 180d
  # -compactor.blocks-retention-period-1h Delete 1h downsampled blocks containing samples older than the specified retention period. Also used by query-frontend to avoid querying beyond the retention period. 0 to disable.
  compactor_blocks_downsampling_1h_retention_period: 0 # keep 1h downsampling indefinitively
```

The cost of using Mimir's `ruler` to downsample all series for a given tenant can be complexe or require a lot of maintenance from a tenant point of view. Moreover, the cost of using `ruler` for downsampling is significative.
Given a tenant with 10M actives series and downsampled at 1 point/min with 5 aggregations (e.g: count, sum, min, max, counter) we would compute 50M series.
Aiming for downsampling all series in the previous time range (e.g: 1min) and based on Mimir's `capacity planning` we would need:

- 50M queries / (60 sec \* 10 queries/sec) = 83k CPU core

With that many query to process by the `ruler` we could be limited by `max_fetched_series_per_query`.

While the compactor is able to handle 20M actives series with 1 CPU core.

### References

- https://github.com/grafana/mimir/discussions/1834
- https://thanos.io/tip/components/compact.md/#downsampling
