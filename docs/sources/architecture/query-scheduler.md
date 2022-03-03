---
title: "(Optional) Query-scheduler"
description: "Overview of the query-scheduler microservice."
weight: 10
---

# (Optional) Query-scheduler

The **query-scheduler** is an **optional component** that internally keeps a queue of queries to execute and distributes the workload to available [queriers]({{<relref "./querier.md">}}).

When query-scheduler is employed, the [query-frontends]({{<relref "./query-frontend.md">}}) enqueue queries to execute into the query-scheduler and queriers pull queries from the query-scheduler. Once a query is executed, the querier sends the result to the query-frontend that enqueued the query.

![Query-scheduler architecture](../../images/query-scheduler-architecture.png)

[//]: # "Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit"

The flow of a query in a Grafana Mimir cluster with the query-scheduler is:

1. The query is received by the query-frontend, which can optionally split and shard it, or serve from the cache.
2. The query-frontend enqueues the query to a random query-scheduler.
3. The query-scheduler stores the query into an in-memory queue, where it waits for some querier to pick it up.
4. A querier picks up the query, and executes it.
5. The querier sends results back to query-frontend, which then forwards it to the client.

The query-scheduler **stateless**.

## Benefits

The query-scheduler enables scaling of query-frontends. The query-frontend comes with some challenges when it comes to scaling it. You can read more about them in [Query-frontend]({{<relref "./query-frontend.md#scaling">}}).

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
