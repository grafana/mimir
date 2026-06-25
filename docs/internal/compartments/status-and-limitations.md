# Status and limitations

Compartments are **experimental and disabled by default**. Only part of the architecture described in
the other files is implemented today; this page tracks the gap.

## Implemented today

- The compartments configuration and per-tenant routing by metric name.
- The **write path** shards series across read compartments and writes each to its compartment's topic.
  Each write compartment produces to its own Kafka cluster.
- Each read compartment has its own partition ring, and ingesters own their partition in the right
  compartment's ring.
- **Multi-cluster consumption**: an ingester consumes its read compartment's topic from **every** write
  compartment's Kafka cluster and unions the records into its TSDB. Each per-cluster consumption tracks
  its own offsets independently.
- **Compartment-aware querying of ingesters**: the query layer narrows a query to the compartments that
  can hold the selected metric names (a single compartment for an equality matcher, the union for an
  enumerable metric-name set) and fans out to all compartments otherwise (see
  [Read compartments](./read-compartments.md)).
- **Per-compartment store-gateway and compactor rings**: a store-gateway or compactor configured for a
  read compartment registers into that compartment's own dedicated ring, separate from the
  non-compartment ring and from the other compartments' rings.
- A local development environment (`development/mimir-compartments`).

## Strong read consistency is not compartment-aware yet

- Strong read consistency does not propagate per-compartment offsets from the query-frontend and querier
  to the ingester, so an ingester enforcing read consistency waits for the last produced offset of every
  Kafka cluster rather than for the specific requested offsets.

## Blocks-storage querying is not compartment-aware yet

- The query path still uses the single, non-compartment store-gateway ring, so it does not yet route
  block queries to the store-gateways of the compartment that holds the blocks. Compartment-aware
  querying of store-gateways (mirroring the compartment-aware querying of ingesters) is not wired yet.
- Running store-gateways and compactors as separate per-compartment deployments, each with a dedicated
  object-storage bucket, is wired in the local development environment but not yet in the production
  deployment tooling.
- The block-builder is not compartment-aware yet, so blocks for every read compartment are not produced
  independently.

## Not yet addressed

- **Cross-cluster ordering.** Consumption from each write compartment's Kafka cluster is independent, so
  there is no guarantee of in-order consumption across clusters. Merging the per-cluster streams to
  reduce out-of-order samples (see [Read compartments](./read-compartments.md)) is not implemented yet.
- Even on the write path, some things are not done yet — for example shuffle sharding and integration
  tests. There is no changelog or experimental-feature documentation entry until compartments work
  end-to-end.

## Local development environment

`development/mimir-compartments` runs 2 write compartments and 2 read compartments. Each write
compartment has its own single-broker Kafka cluster, and each read compartment has its own topic and
ingester set. The ingesters of each read compartment consume that compartment's topic from both Kafka
clusters.

Metrics in either compartment are queryable: a query that pins a metric name is served from the owning
compartment, and a query without a metric-name equality matcher fans out to both.
