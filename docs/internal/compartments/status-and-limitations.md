# Status and limitations

Compartments are **experimental and disabled by default**. Only part of the architecture described in
the other files is implemented today; this page tracks the gap.

## Implemented today

- The compartments configuration and per-tenant routing by metric name.
- The **write path** shards series across read compartments and writes each to its compartment's topic.
- Each read compartment has its own partition ring, and ingesters own their partition in the right
  compartment's ring.
- A local development environment (`development/mimir-compartments`).

## The read path is not compartment-aware yet

- Reads only consult **read compartment 0**. Series that hash to any other compartment are ingested
  correctly but are **not queryable** yet.
- Ingester consumption is unchanged for now: an ingester consumes the default topic as if compartments
  were disabled.

## Not yet addressed on the write path

Even on the write path, some things are not done yet — for example shuffle sharding and integration
tests. There is no changelog or experimental-feature documentation entry until compartments work
end-to-end.

## Local development environment

`development/mimir-compartments` runs 2 read compartments with separate topics and ingester sets,
sharing a single Kafka cluster for now.

Because the read path only queries compartment 0, only metrics that hash to compartment 0 are queryable
in this environment. In particular, the metric used to populate the bundled dashboards' template
variables happens to hash to a different compartment, so those dashboards render blank. Querying metrics
that hash to compartment 0 returns data as expected.
