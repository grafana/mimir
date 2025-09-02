---
description: Learn about Grafana Mimir's ingest storage architecture and how it handles data ingestion.
menuTitle: Ingest storage architecture
title: About ingest storage architecture
weight: 22
---

# About ingest storage architecture

Ingest storage architecture uses Kafka as a central pipeline to decouple read and write operations, making Grafana Mimir more reliable and scalable.

Starting with Grafana Mimir 3.0, ingest storage architecture is stable and the default architecture for running Mimir.

Ingest storage architecture enhances reliability, supports growth, and enables larger-scale use cases. It helps you run more robust and cost-effective Mimir deployments.

### How ingest storage architecture works

Compared with classic architecture, ingest storage architecture fundamentally changes how Mimir handles data. In classic architecture, ingester nodes are highly stateful. They combine in-memory data with local write-ahead logs (WALs) and participate in both writes and reads. This can lead to heavy queries disrupting live writes.

Ingest storage architecture mitigates this issue by decoupling the read and write paths and reducing the statefulness of ingesters. It uses Kafka as a central data transfer pipeline between the write and read paths.

The following diagram shows how ingest storage architecture works:

![Ingest storage architecture diagram](/media/docs/mimir/kafka_architecture.png)

Here's an overview of how ingest storage architecture works:

- Distributors receive write requests and validate them, apply transformations, and forward samples to the storage layer.
- Distributors connect to a Kafka topic and shard each write request across multiple partitions, briefly buffering requests before sending them to the Kafka broker.
- Kafka brokers persist the write requests durably and optionally replicate them to other brokers before acknowledging the write.
- Distributors acknowledge the write requests back to the client after Kafka confirms persistence.
- Ingesters consume write requests from Kafka partitions continuously, with each ingester consuming from a single partition.
- Multiple ingesters across different zones consume from the same partition to provide high availability on the read path.
- Ingesters add the fetched write requests to in-memory state and persist them to disk, making the samples available for queries.

### Benefits of ingest storage architecture

Ingest storage architecture offers the following advantages over classic architecture:

- Increased reliability and resilience in the event of sudden spikes in query traffic or ingest volume
- Decoupled read and write paths to prevent performance interference
- Less stateful, easier to manage ingesters
- Reduced cross-availability zone data transfer costs through the use of object storage
