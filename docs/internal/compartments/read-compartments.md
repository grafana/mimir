# Read compartments

> This describes the target architecture. For what is implemented today, see
> [Status and limitations](./status-and-limitations.md).

A read compartment is the metric-storage side of a compartment. It runs its own ingesters,
store-gateways, block-builders and compactors, and is responsible for the series sharded to it.

## How it works

- Each read compartment uses a **dedicated topic**. Partition 0 of read compartment 0 and partition 0
  of read compartment 1 are different topic-partitions, so partition ownership is naturally scoped per
  compartment.
- Each read compartment has its **own partition ring**, and an ingester owns a partition of its read
  compartment. Each read compartment scales horizontally and independently of the others, so a read
  compartment can have a different number of partitions than another.
- An ingester consumes its partition from **every** write compartment's Kafka cluster, because every
  write compartment writes to every read compartment Kafka topic (in a different Kafka cluster).

```mermaid
flowchart LR
    subgraph WCk["Write compartments (Kafka clusters)"]
        direction TB
        K0["Kafka cluster #0<br/>topic read-comp-1, partition 0"]
        K1["Kafka cluster #1<br/>topic read-comp-1, partition 0"]
    end

    ING["ingester-0 of read compartment #1<br/>owns partition 0"]

    K0 --> ING
    K1 --> ING
```

## Blocks storage

Each read compartment owns its blocks storage end to end:

- A read compartment has a **dedicated object-storage bucket**. Its ingesters and block-builders
  upload blocks there, its compactors compact the blocks within it, and its store-gateways serve
  queries from it. A compartment's blocks never mix with another compartment's.
- Store-gateways and compactors **run per read compartment**, alongside that compartment's ingesters.
- Each read compartment has its **own store-gateway ring** and its **own compactor ring**. Block
  sharding and replication across store-gateways, and compaction-job sharding across compactors, are
  therefore scoped within a compartment: a store-gateway or compactor only owns work for its own
  compartment's blocks.

This is the storage-side expression of the blast-radius and scaling goals (see
[Mimir compartments](./README.md)): a bucket-level problem — for example object-storage rate limiting
or a corrupt block — is contained to one compartment, and each compartment's storage components and
bucket scale independently of the others.

The global query layer queries each read compartment's store-gateways the same way it queries that
compartment's ingesters: it narrows a query to the compartments that can hold the selected metric
names and fans out otherwise (see [Querying read compartments](#querying-read-compartments)).

## Querying read compartments

The global query layer queries the ingesters of read compartments through the same path used without
compartments, extended to be compartment-aware. Because series are sharded to read compartments by
metric name (see [Sharding](./sharding.md)), the layer narrows a query to only the compartments that
can hold the selected series:

- **When the query pins a single metric name** — that is, it filters the metric name with an equality
  matcher — all of its series live in exactly one compartment, so only that compartment's ingesters are
  queried.
- **When the query restricts the metric name to a finite set** — for example a regex matcher equivalent
  to an alternation like `a|b|c` — the query is sent to the union of the compartments owning those
  names, which can still be a subset of all compartments.
- **Otherwise** — whenever the metric name can't be reduced to a finite set of names (no metric-name
  equality matcher and no enumerable regex, for example an open-ended regex like `.+`) — the query fans
  out to **every** compartment and the results are merged. Metadata and whole-tenant statistics queries,
  which carry no metric-name filter, always fan out.

This targeting is what delivers the query blast-radius reduction: a problem in one compartment only
affects queries whose metric name routes to it, while queries for other metric names are unaffected.

Targeting relies on the read-compartment count being **static**: the metric-name-to-compartment mapping
must match the one used on the write path, so a query reads from the same compartment the series were
written to. Changing the number of compartments would shift that mapping and is out of scope.

## Why a dedicated topic per read compartment

A dedicated topic per read compartment simplifies partition management: each compartment's partitions
are an independent topic, so there is a clear, per-compartment mapping between a partition and the
ingester that owns it.

For example, with two read compartments there are two topics, `read-comp-0` and `read-comp-1`. The
ingester that owns partition 0 of read compartment 0 consumes `read-comp-0` partition 0, while the
ingester that owns partition 0 of read compartment 1 consumes `read-comp-1` partition 0 — two distinct
topic-partitions owned by two distinct ingesters, even though both are "partition 0".

## Warpstream specifics

At Grafana Labs the per-write-compartment Kafka clusters are Warpstream clusters. The following points
are specific to Warpstream; the design above is Kafka-cluster-generic.

- **Read agents run (logically) in write compartments, not read compartments.** There is one read-agent
  pool per Warpstream virtual cluster (VC), and VCs are driven by write compartments. An ingester only
  consumes a single partition from each VC, and consuming a single partition requires connecting to only
  one read agent, so an ingester connects to exactly one read agent per write compartment. This bounds the
  number of direct ingester-to-read-agent connections to the number of write compartments.
- **Read-agent distributed caching.** Warpstream read agents build a distributed in-memory cache and can
  fetch portions of files from one another. As a result, consuming a partition across all VCs may, under
  the hood, require data from any read agent in any write compartment. This is a potential scalability
  and availability limitation that needs further investigation.
- **Cross-VC consumption ordering.** An ingester consumes from multiple VCs, and consumption from each VC
  is independent, so there is no guarantee of in-order consumption across VCs. The plan is to offset this
  by merging the per-VC streams with a heap that orders Kafka records by their creation timestamp.
