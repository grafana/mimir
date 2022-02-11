---
title: "Query-scheduler"
description: "Overview of the query-scheduler microservice."
weight: 10
---

# Query-scheduler

Query-scheduler is an **optional** service that moves the internal queue from the query frontend into a separate component.
This enables independent scaling of query frontends and number of queues (query-scheduler).

In order to use query-scheduler, both query frontend and queriers must be configured with the query-scheduler address
(using `-query-frontend.scheduler-address` and `-querier.scheduler-address` options respectively).

Flow of the query in the system changes when using query-scheduler:

1. Query is received by query frontend, which can optionally split it or serve from the cache.
2. Query frontend forwards the query to random query-scheduler process.
3. query-scheduler stores the query into an in-memory queue, where it waits for some querier to pick it up.
4. Querier picks up the query, and executes it.
5. Querier sends result back to query-frontend, which then forwards it to the client.

Query-schedulers are **stateless**. It is recommended to run two replicas to make sure queries can still be serviced while one replica is restarting.
