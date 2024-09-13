# Query Scheduler

## What is the Query Scheduler?

The Query Scheduler is a dedicated request queue broker component between the Query Frontend instances
and the pool of Queriers available to service the queries.

- The Query Frontend instances split and shard query requests, then enqueues the requests to the Scheduler.
- The Scheduler pushes the queries into its internal in-memory `RequestQueue` process.
- Queriers connect to the Scheduler and enter a loop to continually request their next query to process.
- Queriers also maintain connections to the Query Frontend to send query results directly back to the Frontend.
- The Scheduler does not receive query results, but will report an error for the query
  to the Frontend if the connection to the Querier is broken during query processing,
  so the Frontend can handle the failure without waiting for the full context timeout.

## Why Do We Need the Query Scheduler?

A dedicated queue component solves scalability issues which can arise from the usage of the "v1 Frontend",
in which the `RequestQueue` process was embedded in each Query Frontend instance,
and Queriers connected directly to the Frontends to request their next query to process.

### Query Frontend Resource Requirements

The Scheduler has limited responsibilities and low resource usage compared to the Query Frontend.
The Query Frontend is a complex component which must parse requests into Prometheus queries,
split, shard, and rewrite requests, and then merge large result sets from Queriers back together.

The Query Frontend therefore has higher baseline needs for its horizontal scale,
and often needs to further autoscale due to increase query count or complexity.
This need for higher pod counts introduces connection scaling issues with the Queriers.

### Querier Connection Scaling

Each of N active Querier pods maintains at least one connection to each of M component pods dispatching queries,
whether that dispatching component is the Query Frontend or the Query Scheduler.

When Mimir is deployed using the v1 Frontend, the Queriers connect directly to the Frontends.
The horizontal scale of both Frontends and Queriers can quickly create a very large number of connections to maintain.
A Mimir deployment may have 2 Frontends and 20 Queriers at idle and scale quickly up to 10 Frontends and 100 Queriers.
In response, each Querier must increase its number of concurrent Querier-Worker goroutines
in order to maintain the minimum connections to each available Frontend.

Each of those Querier-Worker goroutines does full end-to-end data fetching and query processing.
Because the Querier does not have a robust internal queuing or rate-limiting mechanism,
excessive concurrency can quickly cause issues as resource usage spikes and runs up against limits.
The system has no ability to react to those queries stuck or slowed down in the Querier
other than waiting for completion, cancellation, or timeout.

In contrast, a Mimir deployment of any size generally only uses 2 Scheduler pods,
and the Scheduler does not utilize horizontal autoscaling.
Each Querier can therefore maintain fewer connections to receive queries
and is far less likely to experience issues with excessive concurrency.

## Querier-Scheduler Connection Lifecycle

The Querier uses service discovery to track when a Scheduler instance is available.
It creates a Querier-Worker connections to the Scheduler, each of which which begins the `QuerierLoop`,
which is the core method that triggers changes to the state of Querier-Worker connections to the Scheduler.

The Scheduler tracks Querier-Worker connections by Querier ID.
This connection tracking serves two purposes:

1. Adding and removing Queriers from the Tenant-Querier [shuffle sharding](https://grafana.com/docs/mimir/latest/configure/configure-shuffle-sharding/#query-frontend-and-query-scheduler-shuffle-sharding)
   when the first worker connection from a Querier is added or when the last connection is removed.
1. Enabling graceful shutdowns for both Scheduler and Querier (see below).

### Querier Shutdown

There are two ways for the persistent Querier-Worker connections to disconnect from the Scheduler.
Both utilize the control flow in the `QuerierLoop` rpc between Querier-Workers and the Scheduler,
triggering state updates when the loop begins and using defers to perform cleanup tasks when the loop disconnects.

#### Querier-Worker Disconnection Process 1: Errors in QuerierLoop

If any part of the Scheduler's `QuerierLoop` errors out for any reason,
it triggers defers which deregister the Querier-Worker connection from the queue.
The errors captured by this process are generally unexpected, such as broken or timed-out connections.

1. The defer calls in the Scheduler's `QuerierLoop` tell the `RequestQueue`
   to decrement the connection count for that Querier ID.
1. If the Querier no longer has any active worker connections _and_ `querier-forget-delay` _is not enabled_,
   the Querier will be immediately deregistered from the `RequestQueue`, triggering a shuffle-shard update.
1. Otherwise, if the Querier no longer has any worker connections and `querier-forget-delay` _is enabled_,
   then the `RequestQueue` notes the time the Querier was disconnected at but does not deregister it yet.
1. The `RequestQueue` periodically calls a `forgetDisconnectedQueriers` which will deregister all Queriers
   with no remaining connections whose `disconnectedAt` exceeds the `querier-forget-delay` grace period,
   then trigger shuffle-shard updates.
1. If the Querier reconnects during the grace period, the `disconnectedAt` state for the Querier is cleared
   and it will not be affected by the calls to `forgetDisconnectedQueriers`.

#### Querier-Worker Disconnection Process 2: Graceful Shutdown Notification from Querier

The top-level Querier process calls a `NotifyShutdown` rpc on the Scheduler to tell the Scheduler that the Querier is shutting down.
This is the expected behavior we see when Queriers are shut down as part of normal rollout or downscaling procedures.
With minor differences, this process utilizes the same mechanisms as the error-case process described above.

1. The Scheduler tells the `RequestQueue` to mark the connection state as `shuttingDown` for the given Querier ID.
1. All worker connections from that Querier ID in the `QuerierLoop` will receive an `ErrQuerierShuttingDown` from the `RequestQueue` when they ask to dequeue their next query request.
1. The rest of the lifecycle is handled by the logic in the "Errors in QuerierLoop" process described above,
   with the exception that `querier-forget-delay` is ignored when the Querier is in a `shuttingDown` state.
   The Querier is removed as soon as the last Querier-Worker connection is deregistered,
   and shuffle-shard updates are triggered.
