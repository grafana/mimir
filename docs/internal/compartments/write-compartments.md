# Write compartments

> This describes the target architecture. For what is implemented today, see
> [Status and limitations](./status-and-limitations.md).

A write compartment is the ingestion side of a compartment: a dedicated pool of distributors plus a
**dedicated Kafka cluster** (Kafka is segregated per write compartment).

## How it works

- An ingress auth gateway or a load balancer forwards each write request to a random distributor in a
  random write compartment. Write requests are spread **randomly** across write compartments.
- Write compartments target running a comparable number of distributor pods, so load is balanced across
  write compartments and there is no hotspot compartment. This is a target, not a strict requirement.
- The distributor shards each series to a read compartment and then to a partition (see
  [Sharding](./sharding.md)), and produces it to the corresponding topic in its write compartment's
  Kafka cluster.

Because series are sharded by read compartment but write requests are spread randomly across write
compartments, **every write compartment writes to every read compartment's topic**: each
read-compartment topic exists in every write compartment's Kafka cluster.

## Distributors share a single global ring

Distributors do not have a per-write-compartment ring; every distributor in every write compartment
joins one global distributor ring. This ring is used only to count how many distributors exist,
which drives global per-tenant rate limiting: each distributor enforces a local limit equal to the
tenant's global limit divided by the number of distributors in the ring.

A global ring is required because write requests are spread randomly across all write compartments,
so a tenant's traffic is served by all distributors in all write compartments. The rate-limit
divisor must therefore count every distributor: scoping the ring per write compartment would divide
the global limit by only a fraction of the distributors and over-admit each tenant by a factor equal
to the number of write compartments.

This requires all distributors across all write compartments to share a single ring backing store.
Giving each write compartment an isolated store would re-introduce the per-compartment
over-admission.

## The HA tracker is global

The HA tracker, which deduplicates samples from high-availability Prometheus pairs, is global across
all write compartments. It does not use the distributor ring; it propagates its elected-replica
state through a shared key-value store. It must be global because the two replicas of an HA pair are
spread randomly across write compartments and can land on distributors in different ones — dedup
only works if all distributors share one elected-replica state. This requires all distributors
across all write compartments to share a single HA tracker key-value store.

## Why a dedicated Kafka cluster per write compartment

Segregating Kafka per write compartment bounds how much ingestion load any single cluster must absorb,
and lets each write compartment be scaled and operated independently of the others. This is the
ingestion-path expression of the blast-radius and scaling arguments described in
[Mimir compartments](./README.md).

## Warpstream specifics

At Grafana Labs the dedicated per-write-compartment Kafka cluster is a [Warpstream](https://www.warpstream.com/)
cluster. The following points are specific to Warpstream; the design above is Kafka-cluster-generic.

- **A dedicated Warpstream virtual cluster (VC) per write compartment**, to overcome Warpstream
  control-plane scaling limits at a very large scale. Running one VC per write compartment spreads the
  control-plane work across VCs.
- **A dedicated object-storage bucket per VC**, to overcome object-storage rate limiting. To ingest a
  write request, Warpstream only needs to write to the single bucket of the write compartment that
  handles the request.

### The N² Produce requests scaling issue

Each distributor sends Produce requests to nearly every write agent, so the number of
distributor-to-write-agent connections grows with the product of the two: distributors × write agents.
Splitting the write path into compartments reduces this, because a distributor in one compartment only
talks to that compartment's write agents, and each compartment holds a fraction of both.

For example, with 1,000 distributors and 100 write agents:

- Without compartments: 1,000 × 100 = 100,000 connections.
- With 10 compartments: 10 compartments × 100 distributors per compartment × 10 write agents per
  compartment = 10,000 connections — an order of magnitude fewer.

This was confirmed in a load test at roughly 1 billion active series running 10 write compartments,
where the Produce request rate dropped from approximately 170K/s without compartments to approximately
25K/s with compartments.
