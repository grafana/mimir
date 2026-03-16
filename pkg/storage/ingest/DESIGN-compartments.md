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

Each write compartment runs the same number of distributor pods. Each write compartment load is balanced (no hotspot compartments).

## Sharding

The sharding technique used in write and read compartments is different:

- **Write compartments**: Write requests are randomly distributed between compartments
- **Read compartments**: Series are sharded to read compartments based on the metric name, and then – within a compartment – they're sharded between partitions using the series labels hash sharding already used by Mimir

## Read path

Each read compartment runs ingesters, block-builders, store-gateways and compactors, responsible to manage the series within that compartment. Ingesters are the most demanding component given they need real time consumption from Warpstream.

Each read compartment runs a dedicated pool of Warpstream write and read agents, running on a dedicated Warpstream virtual cluster (VC). This allows scaling the Warpstream RSM by sharding the processing of RSM mutations across multiple CPU cores (the RSM processing of each single tenant is single threaded, but by using different VCs each VC RSM is processed in parallel). Each Warpstream VC runs on a dedicated object storage bucket to overcome rate limiting issues.

An ingester owns a partition of a read compartment, and it consumes its own partition from the read compartment VC.

## Implementation Notes

### Compartment = different Kafka backend

Each compartment is fully parametrized via the `<compartment-id>` placeholder in the standard Kafka config fields. The following fields support the placeholder:

- `address` (Kafka seed broker addresses)
- `topic` (Kafka topic name)
- `sasl-username` (SASL authentication username)
- `sasl-password` (SASL authentication password)

The placeholder `<compartment-id>` is replaced with the compartment index (`0`, `1`, ..., `num_compartments-1`). This allows each compartment to connect to a completely different Kafka backend (different Warpstream VC), potentially with a different topic, credentials, and address.

When the placeholder is not used (e.g. in local development), all compartments connect to the same Kafka backend and use the same topic.

### Compartment routing

The `CompartmentsConfig` controls compartment behaviour:

- `enabled`: Whether compartments are active (default: `false`).
- `num_compartments`: The number of read compartments. Must be > 0 when enabled.

The `CompartmentRouter` assigns series to compartments:

1. It hashes `userID + metricName` using `mimirpb.ShardByMetricName()` (FNV-32 based).
2. It takes `hash % num_compartments` to determine the compartment index.

Compartment assignment is per-tenant: the same metric name from different tenants may be assigned to different compartments because the user ID is part of the hash input.

### Writer per-compartment Kafka clients

The `Writer` maintains a 2D slice of Kafka clients: `writers[compartmentID][clientID]`. Each compartment has its own set of lazily-initialized Kafka clients, configured using a per-compartment `KafkaConfig` with all placeholders resolved.

When compartments are disabled, the Writer uses a single compartment (index 0) with the base `KafkaConfig`, preserving pre-compartments behaviour.

The `KafkaConfigForCompartment()` helper creates a copy of the base config with placeholders resolved for a given compartment ID, ensuring no aliasing of slice fields.

### Distributor integration

The distributor wires compartment routing into the write path using two-level sharding:

1. **Compartment level**: Each series and metadata item is assigned to a compartment based on `mimirpb.ShardByMetricName(userID, metricName) % numCompartments`. This groups items into `compartmentTokens` structs, each carrying a compartment ID, item indexes, and partition-ring tokens.

2. **Partition level**: For each compartment, `ring.DoBatchWithOptions` distributes items across partitions using the standard labels-hash sharding. Each compartment's DoBatch runs concurrently via `errgroup`.

The data flow in the distributor:

```
push()
  → sendWriteRequestToBackends()
    → getCompartmentTokensForWriteRequest(router, userID, req)
      // returns []compartmentTokens, one per non-empty compartment
    → sendWriteRequestToPartitions(ctx, tenantID, ring, req, compartmentTokens, ...)
      // for each compartmentTokens entry (concurrently):
      → ring.DoBatchWithOptions(ring, ct.tokens, callback)
        → callback(partition, tokenIndexes):
            // remap tokenIndexes → WriteRequest indexes via ct.indexes
            req.ForIndexes(writeReqIndexes, initialMetadataIndex)
            WriteSync(ctx, ct.compartmentID, partitionID, tenantID, subReq)
```

When compartments are disabled (`router == nil`), `getCompartmentTokensForWriteRequest` returns a single `compartmentTokens` entry with compartment ID 0, preserving the pre-compartments behaviour.

The cleanup callback uses an atomic counter to ensure it fires only once after all compartment DoBatch calls have completed.
