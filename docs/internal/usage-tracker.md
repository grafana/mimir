# Usage tracker service

## Mission, design & decisions

### Mission

In Mimir's ingest architecture, the max series limit enforcement creates three critical challenges when performed by the ingesters (like in classic architecture):

1. **Ingestion cost** - Series over-the-limit are ingested into Kafka/Warpstream before being rejected, generating cost for data that cannot be used.
2. **Block-builder inconsistency** - Block-builder must independently enforce limits, potentially making them each storing an inconsistent view of the data.
3. **Client error feedback** - Errors are not reported back to clients in HTTP responses.

Usage tracker service solves these by enforcing per-tenant max series limits before Kafka ingestion.

Usage tracker is designed to be a highly available, horizontally scalable service that tracks series hashes and enforces series limits across multiple availability zones.

The synchronization between multiple availability-zones is eventually consistent, and it leverages a Kafka-compatible storage to both synchronize replicas in runtime and to persist the state of the service upon restarts.

### Consistency over precision

This service is not designed to be meant as a cost-control mechanism but a system protection.

The consistency of decisions is considered more important than the precision of the limiting algorithm,
so in some scenarios the system will accept more series than the configured limit, but it will never reject series that were already accepted and are still active.

### Timestamps and space optimization

Given that in most cases it's used to consider series as "active" whenever they had samples in a very short recent time, 20m by default, or 10m in previous Mimir versions,
it does not make sense to spend 8 bytes (64bit timestamp) to track the last update of each series.
This service uses a single byte to represent the minutes since the last 2 hours multiple.

### Series hashes and collisions

The collision of series hashes are ignored as they have negligible impact on the system precision.

## Distributor interaction

Distributors interact with usage tracker service through the `UsageTrackerClient` in one of the middlewares of the push chain.

At startup, and then periodically, distributors fetch the list of users that are close to their series limit from a random partition of the usage tracker service.
This works under the assumption that every usage-tracker partition has a similar number of series for each user.

Distributors decide whether to perform a synchronous or asynchronous tracking of series based on whether the user is close to their limit or not, and on the minimum series limit configured.

A quick overview of the synchronous interaction from the Distributor's perspective:

- Series ready to be ingested are hashed into uint64 values, these hashes are what Usage tracker service tracks.
- The hashes are sharded across a fixed number of partitions.
- The usage tracker partition ring is checked to find a partition owner for each partition.
- A simultaneous gRPC request is sent to each partition owner with the series hashes and the userID of the requesting tenant.
- Usage tracker tracks the series hashes and returns a list of rejected hashes.
- Distributor filters the `WriteRequest` to remove rejected series hashes.
- If series are rejected, the distributor returns an error to the client with a `429 Too Many Requests` status code.

If the user has a series limit high enough and it's far enough from their limits, the distributor will track the series asynchronously:

- A goroutine is spawned to track the series in the background while the series are being ingested.
- When the series are ingested, the distributor waits a configurable extra time for usage-tracker calls to succeed.
- After that time, usage-tracker calls are canceled.

The middleware is positioned last in the chain to enforce limits after all filtering, relabeling, and transformations.

## Partitions and Shards

### Partitions

Usage tracker service uses a fixed number of **partitions** (default: 64). This number is configurable but cannot be changed when the system is running without undesirable side effects.

- Each partition is usually owned by at least one replica in each zone.
- During scaling, partitions are reassigned to new replicas and multiple replicas can own the same partition in the same zone.
- It's distributor's responsibility to choose the correct partition where series hashes should be sent to, usage-tracker does not verify this.

### Shards

Within each partition, data is organized into **shards** (default: 16) for memory efficiency:

- Each shard holds the series hashes modulo that shard's number.
- Shards allow concurrent processing of requests within a partition reducing mutex contention.
- Shards allow snapshots to be smaller, reducing the memory spikes when processing snapshots from different replicas.

## A single partition service

In order to understand how the usage tracker service works, let's take a look at a single partition.

A partition is implemented as a `services.Service` and it's mirrored by a Kafka partition of the underlying events and snapshots metadata topics.

When a partition is started it will:

- Read the latest snapshot metadata from the corresponding Kafka partition.
- Download the snapshot files from object storage.
- Load the snapshot files into memory.
- Start a Kafka consumer to read events from the corresponding Kafka partition at the last offset produced when the snapshot was created.

The partition service will subscribe to the events topic, which may receive two kinds of events:

- **SeriesCreatedEvent**: contains tenant ID, series hashes, and timestamps. Used to track new series.
  - This event is produced by the replicas handling this same partition. Own events are ignored (by checking a record header)
  - This event is used to achieve eventual consistency across replicas.
- **SnapshotEvent**: contains the information about a single snapshot file produced by some replica (own events are ignored).
  - This file might contain one or multiple shards.
  - Continuous snapshot loading is done to ensure that a replica that has not received updates for a given series hash won't garbage-collect it if a different replica updated it in the meantime.

The partition service will also start to periodically produce snapshots of the current state:

- These snapshots are written into files, one shard at a time, producing a new file each time a file size limit is reached (100MB by default).
- Once a file is produced, it's written to object storage and a `SnapshotEvent` is produced to the Kafka topic.
- Once all shards are written, a `SnapshotRecord` record is produced to the snapshots metadata topic.

The partition will also periodically run a garbage-collection process to remove the series hashes that have not received updates for a configured amount of time (default: 20 minutes).

## Horizontal Scaling

### Partition distribution across usage tracker replicas

Partitions are distributed using a consistent division algorithm, ensuring that each replica owns a partition range no larger than 2x the smallest partition range owned by any other replica.
This is done by consecutively dividing the partition ranges among the replicas, starting from the lowest index.

For example, if there are only 16 partitions, this represents the range each replica index takes (numbers on the ranges are replica indexes, each row represents a scenario with one more replica added):

```
0   1   2   3   4   5   6   7   8   9   0   1   2   3   4   5
                                0
[==============================================================)
                0               |               1
[==============================)[==============================)
        0       |       2       |               1
[==============)[==============)[==============================)
        0       |       2       |       1       |	    3
[==============)[==============)[==============)[==============)
    0   |   4   |       2       |       1       |	    3
[======)[======)[==============)[==============================)
    0   |   4   |   2   |   5   |       1       |	    3
[======)[======)[======)[======)[==============================)
    0   |   4   |   2   |   5   |   1   |   6   |	    3
[======)[======)[======)[======)[======)[======================)
    0   |   4   |   2   |   5   |   1   |   6   |   3   |   7
[======)[======)[======)[======)[======)[======)[======)[======)
etc.
```

### Meaning of "Read only" in Usage Tracker

Usage tracker instances registered in the instance ring can become read only when they are being scaled down.
These replicas can still serve the write requests correctly, this mechanism is used to signal the replicas with lower indexes that they should take ownership of the partitions owned by the read-only replica.

### Choosing the replica for a partition

Given a list of replicas serving a partition in one zone, the `UsageTrackerClient` in the distributor will choose the `ACTIVE` replica with the highest index that is not currently marked read-only,
and it will choose the read-only replica if there are no _writable_ replicas available.

The situation when only a read-only replica is serving a partition happens when the downscale process has just started and the future owners of the partition have not yet loaded it.

### Scaling Operations

#### Adding replicas

- New replicas join the ring in `JOINING` state.
- Given their own replica index, they determine which partitions they should own.
- They create the services for those partitions and start loading snapshots and replaying events.
- Once they have loaded all the partitions they are expected to own, they transition to `ACTIVE` state.
- When an old partition-owner observes in the partition ring that there is a higher-indexed replica owning the partition, it marks that partition for deletion.
  - This is to ensure that all distributors observe the same state and start sending requests to the new owner.
- After a grace period, the partition is removed from the old owner replica.

#### Removing replicas

- The system has to be prepared for downscale, calling the `/prepare-for-downscale` endpoint on the replica to mark it as `READ_ONLY`.
  - This is automated through the rollout-operator, using a `ReplicaTemplate` for the replica count in each usage-tracker zone.
- Once a lower-indexed replica observes that a higher-indexed replica is in `READ_ONLY` state, it will start the services for those partitions that correspond to it.
- The read-only replica can be stopped after a grace period enough for the new replica to start serving the partitions.
- The read-only replica is then shut down. When shut down in the read-only state it will also remove itself from the instance ring.

### Gradual Partition Adoption

- Replicas create max 1 partition per reconcile interval (10s default).
- Prevents load avalanches during scaling.
- `max-partitions-to-create-per-reconcile` controls adoption rate.

## Events Communication

### Series Creation Events

When new series are tracked, replicas publish `SeriesCreatedEvent` to Kafka:

- **Batching**: Events are batched by tenant and flushed every 1s or when batch reaches size limit.
  - Events can be lost if replica crashes before flushing.
- **Replication**: Other replicas handling same partition consume and apply events.
- **Headers**: Include instance ID used to ignore own events.

### Event Processing

- Events contain tenant ID, timestamp, and series hashes.
- Replicas apply events from other instances to maintain consistency.
  - Series from `SeriesCreatedEvent` are always added, regardless of current limit status.
- Events older than idle timeout (20m default) are ignored.
- Failed event publishing is retried with backoff.

## Snapshots Persistence

### Snapshot Creation

- Periodic snapshots capture complete partition state to object storage.
- Snapshots include all active series with timestamps.
- Files are compressed in a custom format and named with instance/partition/timestamp pattern.
- Metadata published to `usage-tracker-snapshots` Kafka topic.

### Snapshot Cleanup

Instance 0 performs periodic cleanup of old snapshots.

## Multi-Zone

### Zone-Aware Deployment

- Usage tracker service supports multi-zone deployment with 1 replica per zone per partition.
- Distributors prefer same-zone replicas for lower latency.
- Cross-zone failover with hedging when local replica unavailable.

### Hedging Strategy

- If same-zone replica doesn't respond within 100ms, distributor queries other zones.
- First response wins, reducing tail latency.
- Metrics track cross-zone failover for cost monitoring.

## Implementation Details

### clock.Minutes

`clock.Minutes` provides a memory-efficient way to store timestamps in minutes, reducing memory usage compared to standard time.Time.
Usage tracker service only cares about information from the last 20 minutes (configurable with max 1h), and it doesn't require high precision timestamps,
so all timestamps are stored as the amount of minutes since the last 2 hours boundary.

### tenantshard.Map

Tenant's shard series are stored in a custom hash map implementation optimized for usage tracker service's use case.
It's based on a swiss-map design, specialized for our use case.

**Why not standard Go map?**

- Better cache locality with grouped storage
- Reduced memory overhead per entry
- In-place updates without retrieving the value from the map
- Specialized cleanup without key/value pair iteration
