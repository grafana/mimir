---
title: "0002: Ingester unregister flexibility"
description: "Ingester unregister flexibility"
draft: true
---

# 0002: Ingester unregister flexibility

**Author:** @LasseHels

**Date:** January 26th 2024

**Sponsor(s):** @dimitarvdimitrov

**Type:** Feature

**Status:** Draft

**Related issues/PRs:**

- https://github.com/grafana/mimir/issues/5901

---

## Problem statement

Several cloud platforms offer spot instances (also known as spot nodes or VMs) that offer significant cost savings
in exchange for a weaker guarantee of availability. In some cases, spot instances can provide a discount of up to 90%
compared with traditional on-demand instances.

The cost-conscious Mimir operator will want to run as many Mimir components as possible on spot instances to reduce
costs.

Spot instances provide little to no guarantee of availability, and operators may lose access to the compute resources
of a spot instance at short notice (colloquially referred to as an "eviction").
Typically, processes on the instance will get a brief amount of time to gracefully terminate.
If a process fails to terminate gracefully in the allotted time, the process is terminated forcefully.

Most of Mimir's components are stateless, and have no problem running on spot instances. This document examines the
challenges of running Mimir ingesters on spot instances, specifically the impact that ingester terminations have on the
ingester ring.

### Ingester ring membership

By default, an ingester will leave the ingester ring when it is shut down
(controlled by the [unregister_on_shutdown](https://grafana.com/docs/mimir/v2.11.x/references/configuration-parameters/#ingester) configuration option).

Ingesters leaving the ring on shutdown cause churn during rollouts as series get re-sharded across all ingesters
(see [grafana/helm-charts#1313](https://github.com/grafana/helm-charts/issues/1313)). This proliferation of series leads
to excessive ingester CPU usage, and ultimately results in unstable rollouts.

Setting `unregister_on_shutdown` to `false` mitigates the rollout issue, but introduces a new one: multi-zone
(assuming Mimir runs [zone-aware](https://grafana.com/docs/mimir/v2.11.x/configure/configure-zone-aware-replication/))
evictions now jeopardise the availability of Mimir.

Mimir needs quorum in order to serve reads and writes. With the current implementation, a single non-`ACTIVE` ingester
is enough for that ingester's entire zone to be considered unhealthy. If two (assuming a replication factor of three)
zones are unhealthy, Mimir's ability to serve reads and writes is compromised.

The cost-conscious Mimir operator is now stuck between a rock and a hard place:

|                                               | Single-zone eviction | Multi-zone eviction                                                                          | Rollout                                                                                                                                                                            |
| --------------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-ingester.ring.unregister-on-shutdown=false` | No problem.          | Guaranteed full loss of read path for all series. Loss of write path for overlapping series. | No problem. If an eviction happens in another zone than the one currently being rolled out, then we will see the same issues as a multi-zone eviction.                             |
| `-ingester.ring.unregister-on-shutdown=true`  | No problem.          | Temporary loss of data for overlapping series. No loss of write path.                        | Series churn and ingester CPU problems. If an eviction happens in another zone than the one currently being rolled out, then we will see the same issues as a multi-zone eviction. |

## Goals

- Provide a way to dynamically (i.e., _without_ re-starting the ingester) set whether an ingester should unregister or not on next shutdown.
- Give Mimir operators more flexibility to choose between availability and consistency.

## Proposal

A new `/ingester/unregister-on-shutdown` HTTP endpoint is added.
This endpoint can be used to control whether the ingester should unregister from the ring the next time it is stopped.

The endpoint supports three HTTP methods: `GET`, `PUT` and `DELETE`.

When called with the `GET` method, the endpoint returns the current unregister state in a JSON response body with a `200` status code:

```json
{ "unregister": true }
```

When called with the `PUT` method, the endpoint accepts a request body:

```json
{ "unregister": false }
```

The endpoint sets `i.lifecycler.SetUnregisterOnShutdown()` to the value passed in the request body. After doing so, it returns the current lifecycler unregister value with a `200` status code:

```json
{ "unregister": false }
```

Using a request body supports both of these use cases:

1. Disabling unregister for an ingester that has it enabled by default.
2. Enabling unregister for an ingester that has it disabled by default.

When called with the `DELETE` method, the endpoint sets the ingester's unregister state to what was passed via the `unregister_on_shutdown` configuration option. After doing so, it returns the current unregister value with a `200` status code:

```json
{ "unregister": false }
```

The `DELETE` method is considered to "delete" any override, and can be used to ensure that the ingester's unregister state is set to the value that was set on ingester startup.

All three behaviours of the endpoint are idempotent.

### Example usage

The `/ingester/unregister-on-shutdown` endpoint allows operators to run ingesters in a default mode of _not_ leaving the
ring on shutdown.

Most cloud platforms offer an "eviction notice" prior to node eviction. A helper service can be set up to react to
the eviction notice by updating evicted ingesters to unregister from the ring.

Concrete example:

1. Ingesters `a-1` and `a-7` run on node `zone1-snkq`. Ingesters `b-4` and `b-9` run on node `zone2-iqmd`.
2. An eviction notice is sent for nodes `zone1-snkq` and `zone2-iqmd` at the same time.
3. Helper service picks up the eviction notice, figures out which ingesters run on the two nodes, and invokes the
   `/ingester/unregister-on-shutdown` endpoint to make each ingester leave the ring when evicted.
4. The ingesters are evicted and leave the ring.
5. Because the ingesters leave the ring, each ingester zone is left with exclusively `ACTIVE` ingesters, and availability
   is maintained.

**Pros**:

- Availability is maintained even during multi-zone evictions.

**Cons**:

- Queries may return incomplete results as samples held by the evicted ingesters are temporarily inaccessible.
  The samples become accessible again when the ingesters join the ring after having replayed their WAL.
- Ownership of evicted series (i.e., series whose owner was evicted) is split between two ingesters. This is likely
  negligible unless the volume of evictions is extreme.
