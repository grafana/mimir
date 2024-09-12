# Query Scheduler

## What is the Query Scheduler?
The Query Scheduler is a dedicated request queue broker component between the Query Frontend instances
and the pool of Queriers available to service the queries.

* The Query Frontend instances split and shard query requests, then enqueue the requests to the Scheduler.
* The Scheduler pushes the queries into its internal in-memory `RequestQueue` process.
* Queriers connect to the Scheduler and enter a loop to continually request their next query to process.
* Queriers also maintain connections to the Query Frontend to send query results directly back to the Frontend.
* The Scheduler does not receive query results, but will report an error for the query
to the Frontend if the connection to the Querier is broken during query processing,
so the Frontend can handle the failure without waiting for the full context timeout.

## Why Do We Need the Query Scheduler?
A dedicated queue component solves scalability issues which can arise from the usage of the "v1 Frontend",
in which the `RequestQueue` process was embedded in each Query Frontend instance,
and Queriers connected directly to the Frontends to request their next query to process.

### Query Frontend Resource Requirements
The Scheduler has limited responsibilities and low resource usage compared to the Query Frontend.
The Query Frontend is a complex component which much parse requests into Prometheus requests,
split, shard, and rewrite requests, and then merge large result sets from Queriers back together.

The Query Frontend therefore has higher baseline needs for its horizontal scale,
and often needs to further autoscale due to increase query count or complexity.
This need for higher pod counts introduces connection scaling issues with the Queriers.

### Querier Connection Scaling
Each of N active Querier maintains at least one connection to each of M components dispatching queries.

In the v1 Frontend, the Queriers connect directly to the Frontends.
The horizontal scale of both Frontends and Queriers can quickly create a very large number of connections to maintain.
A Mimir deployment may have 2 Frontends and 20 Queriers at idle and scale quickly up to 10 Frontends and 100 Queriers.
In order to maintain connection to each Frontend each Querier must create a higher amount of Querier-Worker connections.

Each of those Querier-Worker goroutines does full end-to-end data fetching and query processing.
As the Querier does not have a robust internal pooling, queuing, or rate limiting mechanism,
excessive concurrency can quickly create issues as resource usage spikes in the Querier
without any ability for the system to react other than waiting for completion, cancellations, or crashes.

In contrast, a deployment generally only uses 2 Scheduler pods, and the Scheduler does not autoscale horizontally.
Each Querier can maintain fewer connections to receive queries and rarely needs to create new ones.

