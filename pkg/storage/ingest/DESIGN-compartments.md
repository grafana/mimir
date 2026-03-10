# Compartments Design

## What is a compartment

A compartment is a group of Mimir pods that handles a portion of the load of the entire cell. All compartment pods run in the same namespace, but they're managed by different Deployments and StatefulSets.

There are two, distinct, sets of compartments:

- Write compartments
- Read compartments

The number of write compartments is not required to be the same as the number of read compartments.

## Write path

The flow of write requests is:

- The cortex-gw receives a write request
- The cortex-gw sends the request to a random distributor running in a random compartment
- The distributor shards the series by **read** compartment first, and then by the number of partitions in each read compartment

Each write compartment runs a dedicated pool of Warpstream write agents, running on a dedicated Warpstream virtual cluster (VC). This allows scaling the Warpstream RSM by sharding the processing of RSM mutations across multiple CPU cores (the RSM processing of each single tenant is single threaded, but by using different VCs each VC RSM is processed in parallel).

Each Warpstream VC runs on a dedicated object storage bucket to overcome rate limiting issues. In the write path, to successfully ingest a write request, Warpstream needs to write to 1 single bucket (the bucket of the write compartment where the request is handled).

Each write compartment runs the same number of distributor pods. Each write compartment load is balanced (no hotspot compartments).

## Sharding

The sharding technique used in write and read compartments is different:

- **Write compartments**: Write requests are randomly distributed between compartments
- **Read compartments**: Series are sharded to read compartments based on the metric name, and then – within a compartment – they're sharded between partitions using the series labels hash sharding already used by Mimir

## Read path

Each read compartment runs ingesters, block-builders, store-gateways and compactors, responsible to manage the series within that compartment. Ingesters are the most demanding component given they need real time consumption from Warpstream.

An ingester owns a partition of a read compartment, and it consumes its own partition from all VCs used by write compartments. Each read compartment uses a dedicated topic, to simplify partition management (we have an ingester-0 in each read compartment, and each ingester-0 owns partition 0 but of a different topic).

For example, the ingester-0 in the read-comp-1 consumes partition 0 from the "read-comp-1" topic of all VCs used by write compartments.

## Implementation Notes

### Topic model

Each read compartment has a dedicated Kafka/Warpstream topic. The distributor writes to the correct topic based on which read compartment a series is assigned to. This means the Writer must support writing to multiple topics (not just one default topic).

### Compartment routing

The `CompartmentsConfig` controls compartment behaviour:

- `enabled`: Whether compartments are active (default: `false`).
- `num_compartments`: The number of read compartments. Must be > 0 when enabled.
- `topic_format`: A topic name template containing a `<compartment-id>` placeholder (e.g. `mimir-read-comp-<compartment-id>`). The placeholder is replaced with `0`, `1`, ..., `num_compartments-1` to generate the actual topic names.

The `CompartmentRouter` assigns series to compartments:

1. It hashes `userID + metricName` using `mimirpb.ShardByMetricName()` (FNV-32a based).
2. It takes `hash % num_compartments` to determine the compartment index.
3. It returns the pre-computed topic name for that compartment index.

Compartment assignment is per-tenant: the same metric name from different tenants may be assigned to different compartments because the user ID is part of the hash input.

### Distributor integration

The distributor wires compartment routing into the write path using two-level sharding:

1. **Compartment level**: Each series and metadata item is assigned to a compartment based on `hash(userID + metricName) % numCompartments`. This groups items into `compartmentTokens` structs, each carrying a topic name, item indexes, and partition-ring tokens.

2. **Partition level**: For each compartment, `ring.DoBatchWithOptions` distributes items across partitions using the standard labels-hash sharding. Each compartment's DoBatch runs concurrently via `errgroup`.

The data flow in the distributor:

```
push()
  → sendWriteRequestToBackends()
    → getCompartmentTokensForWriteRequest(router, defaultTopic, userID, req)
      // returns []compartmentTokens, one per non-empty compartment
    → sendWriteRequestToPartitions(ctx, tenantID, ring, req, compartmentTokens, ...)
      // for each compartmentTokens entry (concurrently):
      → ring.DoBatchWithOptions(ring, ct.tokens, callback)
        → callback(partition, tokenIndexes):
            // remap tokenIndexes → WriteRequest indexes via ct.indexes
            req.ForIndexes(writeReqIndexes, initialMetadataIndex)
            WriteSync(ctx, ct.topic, partitionID, tenantID, subReq)
```

When compartments are disabled (`router == nil`), `getCompartmentTokensForWriteRequest` returns a single `compartmentTokens` entry with the default Kafka topic, preserving the pre-compartments behaviour.

The cleanup callback uses an atomic counter to ensure it fires only once after all compartment DoBatch calls have completed.
