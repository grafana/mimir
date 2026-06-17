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
- A local development environment (`development/mimir-compartments`).

## The query path is not compartment-aware yet

- Queries only consult **read compartment 0**. Series that hash to any other compartment are ingested
  correctly but are **not queryable** yet.
- Strong read consistency does not propagate per-compartment offsets from the query-frontend and querier
  to the ingester, so an ingester enforcing read consistency waits for the last produced offset of every
  Kafka cluster rather than for the specific requested offsets.

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

Because the query path only reads compartment 0, only metrics that hash to compartment 0 are queryable
in this environment. In particular, the metric used to populate the bundled dashboards' template
variables happens to hash to a different compartment, so those dashboards render blank. Querying metrics
that hash to compartment 0 returns data as expected.
