---
title: "(Optional) Query-scheduler"
description: "Overview of the query-scheduler microservice."
weight: 10
---

# (Optional) Query-scheduler

The **query-scheduler** is an **optional component** that internally keeps a queue of queries to execute and distributes the workload to available [queriers]({{<relref "./querier.md">}}).

When query-scheduler is employed, the [query-frontends]({{<relref "./query-frontend.md">}}) enqueue queries to execute into the query-scheduler and queriers pull queries from the query-scheduler. Once a query is executed, the querier sends the result to the query-frontend that enqueued the query.

![Query-scheduler architecture](../images/query-scheduler-architecture.png)

[//]: # "Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit"

The flow of a query in a Grafana Mimir cluster with the query-scheduler is:

1. The query is received by the query-frontend, which can optionally split and shard it, or serve from the cache.
2. The query-frontend enqueues the query to a random query-scheduler.
3. The query-scheduler stores the query into an in-memory queue, where it waits for some querier to pick it up.
4. A querier picks up the query, and executes it.
5. The querier sends results back to query-frontend, which then forwards it to the client.

The query-scheduler **stateless**.

## Benefits

The query-scheduler enables scaling of query-frontends.

### Why query-frontend scalability is limited

When the query-scheduler is not used, the query-frontend scalability is limited by the configured number of workers per querier.

The query-frontend keeps a queue of queries to execute. A querier internally runs `-querier.max-concurrent` workers and each worker connects to one of the query-frontend replicas to pull queries to execute. Each querier worker can execute one query at a time.

The connection from a querier worker to a query-frontend is persistent: once established, multiple queries are delivered through the connection, one at a time. The querier workers select the query-frontend replicas to connect to in a round robin fashion, in order to keep a balanced number of total workers connected to each query-frontend.

In the case you're running more query-frontend replicas than the number of workers per querier, the querier increases the number of internal workers to match the query-frontend replicas, to ensure all query-frontends have some workers connected.

However, the PromQL engine running in the querier is also configured with a max concurrency equal to `-querier.max-concurrent`.
In the event the number of querier workers is higher than the PromQL engine max concurrency, a worker may pull a query from the query-frontend but could not be able to execute it immediately because the max concurrency has already been reached.
The queries exceeding the configured max concurrency will queue up in the querier itself until other queries will be completed.

This may cause a suboptimal utilization of querier resources, leading to poor query performances when running Grafana Mimir at scale.

### How query-scheduler solves query-frontend scalability limits

When the query-scheduler is used, the queue is moved from the query-frontend to the query-scheduler, and the query-frontend can be scaled to any number of replicas.

The query-scheduler is affected by the same scalability limits of the query-frontend, but since a query-scheduler replica can handle a very high queries throughput, scaling the query-scheduler to a number of replicas higher than `-querier.max-concurrent` is typically not required even for the largest Grafana Mimir clusters.

## Configuration

To use query-scheduler, both query-frontends and queriers must be configured to connect to the query-scheduler:

- Query-frontend: `-query-frontend.scheduler-address`
- Querier: `-querier.scheduler-address`

> Note: the querier pulls queries only from query-frontend or query-scheduler, but not both. `-querier.frontend-address` and `-querier.scheduler-address` options are mutually exclusive, and only one of the two can be set.

## Operational considerations

We recommend to run two query-scheduler replicas for high-availability.
In case you're running a Grafana Mimir cluster with a very high queries throughput we recommend to not scale the query-scheduler to a number of replicas higher than the configured `-querier.max-concurrent`.
