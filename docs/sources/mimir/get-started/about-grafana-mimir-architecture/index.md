---
aliases:
  - ../operators-guide/architecture/about-grafana-mimir-architecture/
description: Learn about the Grafana Mimir architecture.
menuTitle: Grafana Mimir architecture
title: Grafana Mimir architecture
weight: 5
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Grafana Mimir architecture

Grafana Mimir has a microservices-based architecture. The system has multiple horizontally scalable microservices that can run separately and in parallel. Grafana Mimir microservices are called components.

Grafana Mimir's design compiles the code for all components into a single binary. The `-target` parameter controls which components that single binary behaves as.

To get started, you can run Grafana Mimir in [monolithic mode](../../references/architecture/deployment-modes/#monolithic-mode) with all components running simultaneously in one process. For more information, refer to [Deployment modes](../../references/architecture/deployment-modes/).

Starting with version 3.0, you can deploy Grafana Mimir using the following architectures:

- Ingest storage (default): This architecture uses Kafka as a central pipeline to decouple read and write operations. Refer to [About ingest storage architecture](about-ingest-storage-architecture/).
- Classic: This architecture uses stateful ingesters with local write-ahead logs to manage both the ingestion of new data and serving recent data for queries. Refer to [About classic architecture](about-classic-architecture/).

{{< admonition type="note" >}}
Classic architecture is still supported in Grafana Mimir version 3.0. However, this architecture is set to be deprecated in a future release. As a best practice, use ingest storage architecture when setting up a new Grafana Mimir deployment.{{< /admonition >}}

## Grafana Mimir components

Most components are stateless and do not require any data persisted between process restarts. Some components are stateful and rely on non-volatile storage to prevent data loss between process restarts. For details about each component, see its page in [Components](../../references/architecture/components/).

### The write path

[//]: # "Diagram source of write path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_0_899"

![Architecture of Grafana Mimir's write path](write-path.svg)

Distributors receive incoming samples in the form of push requests.
Each push request belongs to a tenant. Distributors shard incoming push requests to multiple Kafka partitions of the same Kafka topic.
Push requests receive a successful response after the distributor has received an acknowledgement from the Kafka brokers
that all Kafka records have been successfully replicated and durably persisted.

Because of this we consider that ingest storage architecture's write path ends in Kafka.
This is unlike the classic architecture whose write path includes ingesters and requires a quorum of ingesters for push requests to succeed.

#### Series sharding and replication

By default, each time series from a push request is indepndantly sharded to a single Kafka partition.
Replication on the write path is delegated to the Kafka cluster.
Ingesters consume from partitions and each ingester writes its own block to the long-term storage.
Achieveing high availability on the read path is achieved through multiple ingester zones.
The [Compactor](../../references/architecture/components/compactor/) merges blocks from multiple ingesters into a single block, and removes duplicate samples.
Blocks compaction significantly reduces storage utilization.
For more information, refer to [Compactor](../../references/architecture/components/compactor/) and [Production tips](../../manage/run-production-environment/production-tips/).

### The read path

[//]: # "Diagram source of read path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_2_6"

![Architecture of Grafana Mimir's read path](read-path.svg)

#### Communicating between the read and the write path

Samples written by distributors to Kafka on the write path are consumed by ingesters on the read path.
Each partition is assigned to a single ingester and each ingester consumes from exactly one partition.

Ingesters choose what partition to consume from based on the suffix of their hostname.
For example if the hostname of the ingester is `ingester-zone-a-13`, the this ingester will consume from partition 13.
Ingesters continuously consume samples from Kafka and append them to the specific per-tenant TSDB that is stored on the local disk.
Each ingester persist the partition offset up to which it has consumed records in a Kafka consumer group dedicated to itself.

The per-tenant TSDB in the ingester is lazily created in each ingester as soon as the first samples are received for that tenant.
The samples that are received are both kept in-memory and written to a write-ahead log (WAL).
If the ingester abruptly terminates, the WAL can help to recover the in-memory series.
After the ingester recoveres from the downtime and has replayed the local WAL,
it resumes consuming from Kafka from the offset stored in its consumer group.
Using this technique the ingester never misses a sample written to Kafka and observes samples in the same order as other ingesters consuming the same partition.

The in-memory samples are periodically flushed to disk, and the WAL is truncated, when a new TSDB block is created.
By default, this occurs every two hours.
Each newly created block is uploaded to long-term storage and kept in the ingester until the configured `-blocks-storage.tsdb.retention-period` expires.
This gives [queriers](../../references/architecture/components/querier/) and [store-gateways](../../references/architecture/components/store-gateway/) enough time to discover the new block on the storage and download its index-header.

To effectively use the WAL, and to be able to recover the in-memory series if an ingester abruptly terminates, store the WAL to a persistent disk that can survive an ingester failure.
For example, when running in the cloud, include an AWS EBS volume or a GCP Persistent Disk.
If you are running the Grafana Mimir cluster in Kubernetes, you can use a StatefulSet with a persistent volume claim for the ingesters.
The location on the filesystem where the WAL is stored is the same location where local TSDB blocks (compacted from head) are stored. The locations of the WAL and the local TSDB blocks cannot be decoupled.

Optionally, in the ingest storage architecture the block-builder can take the responsibility of building TSDB blocks from
records in Kafka and uploads them to long-term storage.
<!-- dimitarvdimitrov: I'm not sure we should mention the block-builder since it's still a component in development and not stable and ready for production use -->

For more information, refer to [timeline of block uploads](../../manage/run-production-environment/production-tips/#how-to-estimate--querierquery-store-after) and [Ingester](../../references/architecture/components/ingester/).

#### The lifecycle of a query

Queries coming into Grafana Mimir arrive at the [query-frontend](../../references/architecture/components/query-frontend/). The query-frontend then splits queries over longer time ranges into multiple, smaller queries.

The query-frontend next checks the results cache. If the result of a query has been cached, the query-frontend returns the cached results. Queries that cannot be answered from the results cache are put into an in-memory queue within the query-frontend.

<!-- we should probably mention how kafka plugs in here for queries with strong consistency -->

{{< admonition type="note" >}}
If you run the optional [query-scheduler](../../references/architecture/components/query-scheduler/) component, the query-schedule maintains the queue instead of the query-frontend.
{{< /admonition >}}

The queriers act as workers, pulling queries from the queue.

The queriers connect to the store-gateways and the ingesters to fetch all the data needed to execute a query. For more information about how the query is executed, refer to [querier](../../references/architecture/components/querier/).

After the querier executes the query, it returns the results to the query-frontend for aggregation. The query-frontend then returns the aggregated results to the client.

#### Data freshness on the read path

Ingesters consume from Kafka asynchronously of serving queries. By default ingesters do not take any explicit action to make sure that any samples written to Kafka before a query is received. This allows ingesters to whitstand Kafka outages without rejeceting queries. The impact of that is that
queries that ingesters serve may return stale data and not provide a read-after-write guarantee. 

During normal operations the delay in ingesting data in ingesters should be below 1 second. This is also know as the end-to-end latency of ingestion. 

While this is usually insignificant for interactive queries, it may pose a challenge for applications that need a guarantee.
One such application is the Mimir ruler (TODO link?) when it evaluates a rule group. The ruler needs samples of earlier rules in the group to be available
when evaluating later rules in the rule group.

In order to preserve the read-after-write guanratee, clients can add the `X-Read-Consistency: strong` HTTP header to queries.
<!-- we have decent content about this in pkg/querier/api/DESIGN.md; can we reprupose it? -->

## The role of Prometheus

Prometheus instances scrape samples from various targets and push them to Grafana Mimir by using Prometheus’ [remote write API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).
The remote write API emits batched [Snappy](https://google.github.io/snappy/)-compressed [Protocol Buffer](https://protobuf.dev/) messages inside the body of an HTTP `PUT` request.

Grafana Mimir requires that each HTTP request has a header that specifies a tenant ID for the request. Request [authentication and authorization](../../manage/secure/authentication-and-authorization/) are handled by an external reverse proxy.

Incoming samples (writes from Prometheus) are handled by the [distributor](../../references/architecture/components/distributor/), and incoming reads (PromQL queries) are handled by the [query frontend](../../references/architecture/components/query-frontend/).

## Long-term storage

The Grafana Mimir storage format is based on [Prometheus TSDB storage](https://prometheus.io/docs/prometheus/latest/storage/).
The Grafana Mimir storage format stores each tenant's time series into their own TSDB, which persists series to an on-disk block.
By default, each block has a two-hour range.
Each on-disk block directory contains an index file, a file containing metadata, and the time series chunks.

The TSDB block files contain samples for multiple series.
The series inside the blocks are indexed by a per-block index, which indexes both metric names and labels to time series in the block files.
Each series has its samples organized in chunks, which represent a specific time range of stored samples.
Chunks may vary in length depending on specific configuration options and ingestion rate, usually storing around 120 samples per chunk.

Grafana Mimir requires any of the following object stores for the block files:

- [Amazon S3](https://aws.amazon.com/s3)
- [Google Cloud Storage](https://cloud.google.com/storage/)
- [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
- [OpenStack Swift](https://wiki.openstack.org/wiki/Swift)
- Local Filesystem (single node only)

For more information, refer to [configure object storage](../../configure/configure-object-storage-backend/) and [configure metrics storage retention](../../configure/configure-metrics-storage-retention/).

## Kafka 

<!-- We probably need to add something here too - what the role of kafka is and what mimir requires of kafka; some thoughts:

we don't use transactions and use consumer groups only to persist how far an ingester has consumed from; each ingester maintains its own consumer group

ingesters do not need kafka to be available to serve queries. During a Kafka outage ingesters end up serving queries with the data they have already consumed from kafka and are now stored in memory or on disk.

each kafka record is consumed exactly once by each ingester zone and records are not replayed after consumed once. An exception to this is when an ingester is abruptly terminated and has not had the chance to persist its offset in the consumer group

kafka retention is only informed by how long of an ingester outage the operator wants to be able to whitstand without data loss. If ingesters are unavailable for longer than the kafka retention, then they will not be able to resume from the offset int heir consumer group. This will lead to a gap in ingested samples. 
--> 
