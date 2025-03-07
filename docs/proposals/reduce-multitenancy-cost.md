---
title: "0003: Tenancy multiplexing"
description: "Tenancy multiplexing"
draft: true
---

# 0003: Tenancy multiplexing

**Authors:** @wilfriedroset @vaxvms @bubu11e

**Date:** 09/2024

**Sponsor(s):** @dimitarvdimitrov

**Type:** Feature

**Status:** Accepted

**Related issues/PRs:**

- https://github.com/grafana/mimir/pull/9978

---

## Background

The `compactor` is a component that improves query performance by compacting multiple blocks for a given tenant in larger optimized ones.
The `ingester` is a component that is responsible for the persistency of the data of each tenant in a TSDB.
The `store-gateway` is a component that is responsible for providing access to the object storage where the blocks are stored.

## Problem statement

For each new tenant hosted on a given ingester, a new TSDB is created/opened. With this new TSDB to manage, Mimir consumes more resources (e.g: CPU, RAM, files on disk, objects in the bucket). With the load balancing, replication factor and shuffle sharding the overhead resources consumed by Mimir can be multiplied by the number of replicas of each component. Example: - For each new tenant, the `ingester` consumes ~6MB of ram - Mimir is currently running with 3 ingesters and a replication factor of 3 - The per-tenant RAM overhead is 18MB (3 ingesters _ 1 tenant _ 6MB/tenant)

Given an increasing number of tenants and the horizontal scaling of Mimir, this overhead becomes bigger.
The problem affects all components that are sensible to the tenant count and their associated TSDB.

## Goals

- From an end-user's point of view, the feature set offered by Mimir works the same as today and there is no breaking changes.
- Reduce the impact on performance of a high number of Mimir tenants, and therefore enable Mimir to support a large number of tenants.

## Non-Goals

- Provide a migration path from the regular multitenancy to the tenancy multiplexing.

## Proposals

### Proposal 0: Do nothing

In this proposal, it is the operator's responsibility to implement a Mimir gateway to expose tenancy multiplexing using a unique Mimir tenant as the backend.
This gateway would modify and proxy requests to the Mimir cluster.

The challenge doing so is to re-implement all the tenant limits and isolation built into Mimir.
Moreover, given the label convention used to enable tenancy multiplexing, the proxy should be implemented in such a way that this private label is not returned in the response to a given Prometheus query.

Note: the gateway of this proposal must not be confused with the GEM gateway.

#### Write path

All the limits enforced by the distributor should be straightforward to implement in another frontend, as the distributor is a stateless process. If the gateway is coded in Go, the code could even be partially reused.

The limits enforced [by the ingester](https://github.com/grafana/mimir/blob/main/pkg/ingester/ingester.go#L102-L109) could be more complex.

#### per_user_series_limit

Gateway could keep in memory a representation of the series already owned by the tenant. This representation should: - be fast to access (as we need to access it for every serie of every write request) - have a small footprint (those information will be exchanged between members of the gateway cluster) - be mergeable (to aggregate information from various members of the gateway cluster) - serializable (for exchange purposes)

A naive approach would be to store in memory all the metadata associated with all the series of every tenants, but this would waste hundreds of gigabytes of memory and would be hard to exchange between frontend cluster members.

A better approach would be to use [HyperLogLog++ estimators](https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/). Such an estimator is able to keep a representation of the cardinality of a data set using a limited footprint (a few kilobytes per estimator), and is serializable and mergeable with a limited error margin (depends of the precision of the estimator).

Sharing those estimators between members of the gateway would ensure us all members share an up-to-date and sufficiently accurate representation of each tenant's cardinality. Communication between the horizontally scaled gateway could be done via memberlist and gRPC or a pub/sub service like Kafka.
However, the amount of data to be exchanged between members could be rather large, as we need one estimator per meta-tenant. A way to mitigate this issue could be to propagate only recently updated estimators.
For example exchange occurs every 15 seconds, for all estimators updated within the last minute.

#### per_metric_series_limit

We could implement this limit in the same way as for `per_user_series_limit`, but this would lead to significant memory consumption as the cardinality of the series is proportional to the cardinality of the tenants.

#### sample-out-of-order

This is not tenant-related, it could be forwarded from the backend.

#### sample-too-old

This is not tenant-related, it could be forwarded from the backend.

#### sample-too-far-in-future

This is not tenant-related, it could be forwarded from the backend.

#### new-value-for-timestamp

This is not tenant-related, it could be forwarded from the backend.

#### sample-timestamp-too-old

This is not tenant-related, it could be forwarded from the backend.

#### invalid-native-histogram

This is not tenant-related, it could be forwarded from the backend.

After checking the limits, the gateway adds a dedicated label to each metric containing the name of the meta tenant.

### Read path

The gateway should modify queries on the fly and add the correct label/value to all nodes of the query.

**Pros:**

- Nothing to be done on Mimir side and all the logic is kept apart
- Operators are free to build any custom made solution

**Cons:**

- The gateway adds an extra hop and therefore latency
- The limits validation are computed several times
- All distinct operators faced with the same situation have to solve it themselves

### Proposal 1: Implement tenancy multiplexing through label

In this proposal, we add the possibility for Mimir to host multiples tenants in a single TSDB instead of having one TSDB per tenant. The distinction is done via a label.
The tenancy multiplexing can be switched on via a configuration file that goes along with the regular multitenancy one.

Example:

```yaml
# When set to true, incoming HTTP requests must specify tenant ID in HTTP
# X-Scope-OrgId header. When set to false, tenant ID from -auth.no-auth-tenant
# is used instead.
# CLI flag: -auth.multitenancy-enabled
[multitenancy_enabled: <boolean> | default = true]

# The mode of multitenancy to use. Either "standard" or "multiplexing".
# When using the "multiplexing" multitenancy several tenants share the same TSDB.
# CLI flag: -auth.multitenancy-mode
[multitenancy_mode: <string> | default = "standard"]
```

All components responsible for manipulating the blocks and/or the TSDB must by adjusted to take into account the tenancy multiplexing.

#### Write path

On the write path we should create a middleware that is responsible for taking the tenant ID and adding the value as a label on each series.
This middleware is only used when "multiplexing" multitenancy mode is selected.

Option: in order to avoid having a single large TSDB we could distribute the tenants over a fixed numbers of TSDBs. Doing so would reduce the number of tenants per TSDB.

Neither the distributor nor the compactor needs to be modified as their current behavior is not impacted by the multiplexing tenancy.

#### Read path

Ingester and store-gateway use the tenant ID as a matcher when querying storage instead of opening the tenant's TSDB

Ruler still work as-is (see <https://grafana.com/docs/mimir/latest/references/architecture/components/ruler/>): - In internal mode it runs its own distributor. Given the following proposal, we must ensure that we apply the same relabeling as in the rest of the write path - In remote mode, only the read path is changed. Indeed, filtering is performed by the ingester and store-gateway using the tenant ID.

Alertmanager multitenancy still working as-is

    - Ruler call the alertmanager with the tenant ID

The tenants federation still works as-is.

#### Limits

Depending on the component in charge of enforcing the limits, we might have to adapt them:

- distributor: no change expected
- ingester: max_series_per_user and max_series_per_metrics have to be computed differently
- querier/query-frontend/query-scheduler: no change expected
- compactor: retention is per TSDB. It can be enforced in the read path by clamping the "start" of the query. Depending on the compute cost vs the storage cost, this might be more cost-efficient to keep the block for longer instead of processing them to discard the data that have exceeded their retention.
- ruler: no change expected
- alertmanager: no change expected

#### Metrics

Depending on the component in charge of metrics exposition, we might have to adapt them:

- ingester: TSDB metrics aren't per user anymore.
- compactor: metrics are per TSDB.
- store-gateway: no change expected
- distributor: no change expected
- querier/query-frontend/query-scheduler: no change expected
- ruler: no change expected
- alertmanager: no change expected

**Pros:**

- The per-tenant overhead is greatly limited and can be considered negligible.

**Cons:**

- Unless modifications, all tenants within the same TSDB share some of the limits. This might be a blocker especially for the retention.
- There is no easy migration back and forth between regular tenancy and tenancy multiplexing.
- It is all or nothing as there is no way to define the mapping between the tenant ID and the TSDB.
- Compactor blocks upload (<https://grafana.com/docs/mimir/latest/configure/configure-tsdb-block-upload/>) will not work properly as block are uploaded to LTS storage as-is. There is no way to add the metatenancy label without rewriting the block.
- Compactor tenant deletion will not work as it relies on mapping the tenant to a dedicated TSDB.
- Migrating a metatenant from one TSDB/cluster to another is not straightforward nor easy.

### Proposal 2: Reduce the overhead by at least an order of magnitude

Disclaimer: We list this proposal for the sake of completeness but our knowledge is limited and we don't know how realistic it is.

As Mimir reuses Prometheus TSDB and code, one could investigate how to improve the resources efficiency in both how Mimir uses the TSDB code and the TSDB code itself.

**Pros:**

- No need to modify Mimir's multitenancy
- The whole Prometheus ecosystem benefits from theses improvements

**Cons:**

- Could be hard to achieve
- Could be out of Mimir's scope

## Other Notes

- Some backend managed to address this concern, for example [VictoriaMetrics' Multi-tenancy](https://docs.victoriametrics.com/cluster-victoriametrics/#multitenancy) isn't impacted.

### References

- <https://grafana.com/docs/mimir/latest/manage/secure/authentication-and-authorization/>
