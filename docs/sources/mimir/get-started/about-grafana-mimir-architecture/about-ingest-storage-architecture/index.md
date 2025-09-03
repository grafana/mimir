---
description: Learn about Grafana Mimir's ingest storage architecture and how it handles data ingestion.
menuTitle: Ingest storage architecture
title: About ingest storage architecture
weight: 10
---

# About ingest storage architecture

Ingest storage architecture uses Kafka as a central pipeline to decouple read and write operations, making Grafana Mimir more reliable and scalable.

Starting with Grafana Mimir 3.0, ingest storage architecture is stable and the default architecture for running Mimir.

Ingest storage architecture enhances reliability, supports growth, and enables larger-scale use cases. It helps you run more robust and cost-effective Mimir deployments.

## How ingest storage architecture works

Compared with classic architecture, ingest storage architecture fundamentally changes how Mimir handles data. In classic architecture, ingester nodes are highly stateful. They combine in-memory data with local write-ahead logs (WALs) and participate in both writes and reads. This can lead to heavy queries disrupting live writes.

Ingest storage architecture mitigates this issue by decoupling the read and write paths and reducing the statefulness of ingesters. It uses Kafka as a central data transfer pipeline between the write and read paths.

The following diagram shows how ingest storage architecture works:

<div align="center">

![Ingest storage architecture diagram](/media/docs/mimir/ingest_storage_arch.png)

</div>

Here's an overview of how ingest storage architecture works:

- Distributors receive write requests and validate them, apply transformations, and forward samples to the storage layer.
- Distributors connect to a Kafka topic and shard each write request across multiple partitions, briefly buffering requests before sending them to the Kafka broker.
- Kafka brokers persist the write requests durably and optionally replicate them to other brokers before acknowledging the write.
- Distributors acknowledge the write requests back to the client after Kafka confirms persistence.
- Ingesters consume write requests from Kafka partitions continuously, with each ingester consuming from a single partition.
- Multiple ingesters across different zones consume from the same partition to provide high availability on the read path.
- Ingesters add the fetched write requests to in-memory state and persist them to disk, making the samples available for queries.

## Benefits of ingest storage architecture

Ingest storage architecture offers the following advantages over classic architecture:

- Increased reliability and resilience in the event of sudden spikes in query traffic or ingest volume
- Decoupled read and write paths to prevent performance interference
- Less stateful, easier to manage ingesters
- Reduced cross-availability zone data transfer costs through the use of object storage

## Grafana Mimir components in ingest storage architecture

Most components are stateless and do not require any data persisted between process restarts. Some components are stateful and rely on non-volatile storage to prevent data loss between process restarts. For details about each component, see its page in [Components](../../../references/architecture/components/).

### The write path

[//]: # "Diagram source of write path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_0_899"

![Architecture of Grafana Mimir's write path](../write-path.svg)

Distributors receive incoming samples in the form of push requests.
Each push request belongs to a tenant, and distributors shard incoming push requests across multiple Kafka partitions within the same Kafka topic.
Push requests receive a successful response after the distributor receives an acknowledgement from the Kafka brokers
that all Kafka records have been successfully replicated and durably persisted.

As a result, the ingest storage architecture write path ends at Kafka.
This differs from classic architecture, where the write path includes ingesters and requires a quorum of ingesters to be available for successful push requests.

#### Series sharding and replication

By default, each time series from a push request is independently sharded to a single Kafka partition. As a result, the time series from the push request can fall into multiple partitions.
Replication on the write path is delegated to the Kafka cluster.
Ingesters consume from partitions, and each ingester writes its own block to long-term storage.
The [Compactor](../../../references/architecture/components/compactor/) merges blocks from multiple ingesters into a single block and removes duplicate samples.
Block compaction significantly reduces storage utilization.
For more information, refer to [Compactor](../../../references/architecture/components/compactor/) and [Production tips](../../../manage/run-production-environment/production-tips/).

### The read path

<!-- we need a new diagram. Can we reuse Marco's powerpoints? -->

[//]: # "Diagram source of read path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_2_6"

![Architecture of Grafana Mimir's read path](../read-path.svg)

Distributors write samples to Kafka on the write path, and ingesters consume them on the read path. Each partition is assigned to a single ingester from each zone, and each ingester consumes from exactly one partition. Multiple ingester zones provide high availability on the read path.

Ingesters choose which partition to consume from based on the suffix of their hostname.
For example, if the hostname of the ingester is `ingester-zone-a-13`, then this ingester consumes from partition 13.
Each ingester persists the partition offset up to which it has consumed records in a Kafka consumer group that it doesn't share with other ingesters.

Ingesters continuously consume samples from Kafka and append them to the tenant-specific time series database (TSDB) stored on the local disk.
The tenant-specific TSDB in the ingester is lazily created when the first samples are received for that tenant.
The samples that are received are both kept in-memory and written to a write-ahead log (WAL).
If the ingester abruptly terminates, the WAL can help to recover the in-memory series.
After the ingester recovers from the downtime and has replayed the local WAL,
it resumes consuming from Kafka from the offset stored in its consumer group.
Using this technique, the ingester never misses a sample written to Kafka and observes samples in the same order as other ingesters consuming the same partition.

The in-memory samples are periodically flushed to disk, and the WAL is truncated, when a new TSDB block is created.
By default, this occurs every two hours.
Each newly created block is uploaded to long-term storage and kept in the ingester until the configured `-blocks-storage.tsdb.retention-period` expires.
This gives [queriers](../../../references/architecture/components/querier/) and [store-gateways](../../../references/architecture/components/store-gateway/) enough time to discover the new block on the storage and download its index-header.

To effectively use the WAL, and to be able to recover the in-memory series if an ingester abruptly terminates, store the WAL to a persistent disk that can survive an ingester failure.
For example, when running in the cloud, include an AWS EBS volume or a GCP Persistent Disk.
If you are running the Grafana Mimir cluster in Kubernetes, you can use a StatefulSet with a persistent volume claim for the ingesters.
The location on the filesystem where the WAL is stored is the same location where local TSDB blocks (compacted from head) are stored. The locations of the WAL and the local TSDB blocks cannot be decoupled.

For more information, refer to [timeline of block uploads](../../../manage/run-production-environment/production-tips/#how-to-estimate--querierquery-store-after) and [Ingester](../../../references/architecture/components/ingester/).

#### The lifecycle of a query

Queries coming into Grafana Mimir arrive at the [query-frontend](../../../references/architecture/components/query-frontend/). The query-frontend then splits queries over longer time ranges into multiple, smaller queries.

The query-frontend next checks the results cache. If the result of a query has been cached, the query-frontend returns the cached results. Queries that cannot be answered from the results cache are put into an in-memory queue within the query-frontend.

For queries requiring strong read consistency, with the `X-Read-Consistency: strong` HTTP header, the query-frontend retrieves the offsets of each partition from the Kafka topic and propagates it through the read path to ingesters. For more information, refer to [Data freshness on the read path](#data-freshness-on-the-read-path).

{{< admonition type="note" >}}
If you run the optional [query-scheduler](../../../references/architecture/components/query-scheduler/) component, the query-schedule maintains the queue instead of the query-frontend.
{{< /admonition >}}

The queriers act as workers, pulling queries from the queue.

The queriers connect to the store-gateways and the ingesters to fetch all the data needed to execute a query. For more information about how the query is executed, refer to [querier](../../../references/architecture/components/querier/).

After the querier executes the query, it returns the results to the query-frontend for aggregation. The query-frontend then returns the aggregated results to the client.

#### Data freshness on the read path

Ingesters consume from Kafka asynchronously from serving queries. By default, ingesters don't take any explicit action to make sure that samples written to Kafka have been appended to the in-memory TSDB and on-disk WAL before a query is received. This allows ingesters to whitstand Kafka outages without rejecting queries. The impact of Kafka outages is that
queries that ingesters serve may return stale data and don't provide a read-after-write guarantee.

During normal operations, the delay in ingesting data in ingesters should be below 1 second. This is also known as the end-to-end latency of ingestion.

While the end-to-end latency is usually insignificant for interactive queries, it may pose a challenge for applications that need a guarantee that queries observe all previous writes.
One such application is the Mimir [ruler](../../../references/architecture/components/ruler) when it evaluates a rule group. The ruler needs samples of earlier rules in the same rule group to be available
when it evaluates later rules in the rule group.

To preserve the read-after-write guarantee, clients can add the `X-Read-Consistency: strong` HTTP header to queries.
When a query includes this header, the query-frontend fetches the latest offsets from Kafka for all in-use partitions and propagates these offsets to ingesters. Each ingester then waits until it has consumed up to the specified offset for its partition before running the query. This ensures that the query observes all samples that were written to Kafka before the query was received by the query-frontend, providing strong read consistency at the cost of increased query latency.

By default the ingester waits up to 20 seconds for the record to be consumed before rejecting the strong read consistency query.

## Kafka

Kafka is a central part of ingest storage architecture. It's responsible for replicating and durably persisting incoming data
and for making this data available to ingesters.

The use of Kafka protocol in Mimir is limited, which allows you to use many Kafka-compatible solutions.

### How Mimir uses Kafka

<!-- TODO dimitarvdimitrov: here' i'm not sure about the best way to structure this content. Would a table be better? -->

On the write path the distributor requires the Kafka cluster to be available for push requests to succeed.
The distributor issues direct **Produce API** calls to the Kafka brokers and does not produce in **transactions** and does not need support for **idempotent writes**.

Both the ingester and the distributor use the **Metadata API** to discover the leaders for a topic-partition.

The Mimir ingester uses Kafka **consumer groups** to persist how far an ingester has consumed from its assigned partition. Each ingester maintains its own consumer group. Each Kafka record is consumed exactly once by each ingester zone and records are not replayed after being consumed. There are two exceptions to this:

1. When an ingester is abruptly terminated and has not had the chance to persist its offset in the consumer group. In that case the ingester will pick back up where the consumer group offset points to.
2. Ingesters can optionally be configured to start consuming from the earliest offset, the latest offset, or a specific timestamp of the Kafka topic-partition. In those cases the ingester will use the **ListOffsets API** to discover the target offset.
