---
title: "(Optional) Query-frontend"
description: "Overview of the query-frontend component."
weight: 10
---

# (Optional) Query-frontend

The **query-frontend** is an **optional component** that provides the same API as the [querier]({{< relref "./querier.md" >}}) and can be used to accelerate the read path. When the query-frontend is in place, incoming query requests should be directed to the query-frontend instead of the queriers. The queriers are still required within the cluster, in order to execute the actual queries.

The query-frontend internally performs some query adjustments and holds queries in an internal queue. In this setup, queriers act as workers which pull jobs from the queue, execute them, and return their results to the query-frontend for aggregation. Queriers need to be configured with the query-frontend address (via the `-querier.frontend-address` CLI flag) to allow them to connect to the query-frontends.

Query-frontends are **stateless**. However, due to how the internal queue works, it's recommended to run multiple query-frontend replicas to reap the benefit of fair scheduling. Two replicas should suffice as a start.

![Query-frontend architecture](../../images/query-frontend-architecture.png)

[//]: # "Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit"

Flow of the query in the system when using query-frontend (_without_ query-scheduler):

1. Query is received by query-frontend. The query-frontend can optionally split it into multiple queries or serve it from the cache.
2. Query frontend stores the query into an in-memory queue, where it waits for some querier to pick it up.
3. Querier picks up the query and executes it.
4. Querier sends result back to query-frontend, which then forwards it to the client.

## Functions

### Queueing

The query-frontend queuing mechanism is used to:

- Ensure that large queries, that could cause an out-of-memory (OOM) error in the querier, will be retried on failure. This allows administrators to under-provision memory for queries, or optimistically run more small queries in parallel, which helps to reduce the TCO.
- Prevent multiple large requests from being convoyed on a single querier by distributing them across all queriers using a first-in/first-out queue (FIFO).
- Prevent a single tenant from denial-of-service-ing (DOSing) other tenants by fairly scheduling queries between tenants.

### Splitting

The query-frontend splits multi-day queries into multiple single-day queries, executing these queries in parallel on downstream queriers and stitching the results back together again. This prevents large (multi-day) queries from causing out-of-memory errors in a single querier and helps to execute them faster.

### Caching

The query-frontend supports caching query results and reuses them on subsequent queries. If the cached results are incomplete, the query-frontend calculates the required subqueries and executes them in parallel on downstream queriers. The query-frontend can optionally align queries with their step parameter to improve the cacheability of the query results[^1]. The result cache is backed by memcached.

[^1]: While this increases the performance of Grafana Mimir, it violates the [PromQL conformance](https://prometheus.io/blog/2021/05/03/introducing-prometheus-conformance-program/) of Grafana Mimir. If this is important to you, make sure the `-query-frontend.align-querier-with-step` configuration option is set to `false`.

## Scaling

Historically scaling the Cortex query-frontend has posed some challenges. These challenges are outlined below.
The [query-scheduler]({{< relref "query-scheduler.md" >}}) can be employed to overcome them.

### Scaling out

Each querier adds a `-querier.max-concurrent` number of concurrent workers. Each worker is capable of executing a query and is connected to a single query-frontend instance. The total query concurrency of the Grafana Mimir cluster is equal to `number_of_queriers * querier.max-concurrent`. There is an exception when the number of query-frontends you are running (referred to as `number_of_frontends` later) is larger than `querier.max-concurrent`. In this case a querier creates `number_of_frontends` workers so that each query-frontend is guaranteed have at least one querier worker attached it.

Inside the querier the concurrency of the PromQL engine evaluating queries is also limited by `querier.max-concurrent`. This means that in case `number_of_frontends` is larger than `querier.max-concurrent`, there will be queuing before the PromQL engine. Therefore, scaling out the query-frontend beyond `querier.max-concurrent` impacts the amount of work each individual querier is attempting to do at any given time. This may cause a querier to attempt more work than they are capable of due to restrictions such as memory and CPU limits.

Additionally, maintaining a high number of query-frontends increases the number of queues. Adding more query-frontends favors high volume tenants by giving them more slots to be picked up by the next available querier worker. Fewer query-frontends allows for an even playing field regardless of the number of active queries per tenant.

### Scaling in

For similar reasons scaling in the query-frontend may cause a querier to underutilize its allocated memory and CPU effectively. This will lower effective resource utilization leading to poor query performances when running Grafana Mimir at scale. Also, because individual queriers will be doing less work, there may be increased queueing in the query-frontends if the request rate remains constant.

## DNS Configuration / Readiness

When a query-frontend is first started it does not immediately have queriers attached to it. The [`/ready` endpoint]({{< relref "../reference-http-api/#readiness-probe" >}}) returns HTTP 200 status code only when the query-frontend has at least one querier attached and is ready to serve queries. Make sure to configure this endpoint as a healthcheck in your load balancer; otherwise, a query-frontend scale out event might result in failed queries or high latency until queriers attach.

When using query-frontend with query-scheduler, `/ready` will report HTTP 200 status code only after the query-frontend discovers some query-schedulers via DNS resolution.
