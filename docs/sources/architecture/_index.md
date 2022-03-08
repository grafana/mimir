---
title: "Grafana Mimir architecture"
description: ""
weight: 20
---

# Grafana Mimir architecture

Grafana Mimir has a microservices-based architecture.
The system has multiple horizontally scalable microservices that can run separately and in parallel.
Grafana Mimir microservices are called components.

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

![Architecture of Grafana Mimir](../images/architecture.png)

## Grafana Mimir components

Most components are stateless and do not require any data persisted between process restarts. Some components are stateful and rely on non-volatile storage to prevent data loss between process restarts. For details about each component, see its page.

{{< section >}}

### The write path

[//]: # "Diagram source of write path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_0_899"

![Architecture of Grafana Mimir's write path](../images/write-path.png)

Ingesters receive incoming samples from the distributors.
Each push request belongs to a tenant, and the ingester appends the received samples to the specific per-tenant TSDB that is stored on the local disk.
The samples that are received are both kept in-memory and written to a write-ahead log (WAL).
If the ingester abruptly terminates, the WAL can help to recover the in-memory series.
The per-tenant TSDB is lazily created in each ingester as soon as the first samples are received for that tenant.

The in-memory samples are periodically flushed to disk, and the WAL is truncated, when a new TSDB block is created.
By default, this occurs every two hours.
Each newly created block is uploaded to long-term storage and kept in the ingester until the configured `-blocks-storage.tsdb.retention-period` expires.
This gives [queriers]({{< relref "components/querier.md" >}}) and [store-gateways]({{< relref "components/store-gateway.md" >}}) enough time to discover the new block on the storage and download its index-header.

To effectively use the WAL, and to be able to recover the in-memory series if an ingester abruptly terminates, store the WAL to a persistent disk that can survive an ingester failure.
For example, when running in the cloud, include an AWS EBS volume or a GCP persistent disk.
If you are running the Grafana Mimir cluster in Kubernetes, you can use a StatefulSet with a persistent volume claim for the ingesters.
The location on the filesystem where the WAL is stored is the same location where local TSDB blocks (compacted from head) are stored. The location of the filesystem and the location of the local TSDB blocks cannot be decoupled.

For more information, refer to [timeline of block uploads]({{< relref "blocks-storage/production-tips/#how-to-estimate--querierquery-store-after" >}}) and [Ingester]({{< relref "components/ingester.md" >}}).

#### Series sharding and replication

By default, each time series is replicated to three ingesters, and each ingester writes its own block to the long-term storage.
The [Compactor]({{< relref "components/compactor.md" >}}) merges blocks from multiple ingesters into a single block, and removes duplicate samples.
Blocks compaction significantly reduces storage utilization.
For more information, refer to [Compactor]({{< relref "components/compactor.md" >}}) and [Production tips]({{< relref "blocks-storage/production-tips.md" >}}).

### The read path

[//]: # "Diagram source of read path at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_2_6"

![Architecture of Grafana Mimir's read path](../images/read-path.png)

[Queriers]({{< relref "components/querier.md" >}}) and [store-gateways]({{< relref "components/store-gateway.md" >}}) periodically download the bucket index to discover blocks that are recently uploaded by ingesters and compactors.
The bucket index is kept updated by the compactors.

For each discovered block, store-gateways download the `meta.json` and the index-header, which is a small subset of the block’s index that the store-gateway uses to look up series at query time.

Queriers use the block’s metadata to compute the list of blocks that need to be queried at query time. Queriers also fetch matching series from the store-gateway instances that are holding the required blocks.

## The role of Prometheus

Prometheus instances scrape samples from various targets and push them to Grafana Mimir by using Prometheus’ [remote write API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).
The remote write API emits batched [Snappy](https://google.github.io/snappy/)-compressed [Protocol Buffer](https://developers.google.com/protocol-buffers/) messages inside the body of an HTTP `PUT` request.

Mimir requires that each HTTP request has a header that specifies a tenant ID for the request. Request [authentication and authorization]({{< relref "../securing/authentication-and-authorization.md" >}}) are handled by an external reverse proxy.

Incoming samples (writes from Prometheus) are handled by the [distributor]({{< relref "#distributor" >}}), and incoming reads (PromQL queries) are handled by the [query frontend]({{< relref "#query-frontend" >}}).

## Long-term storage

The Grafana Mimir storage format is based on [Prometheus TSDB storage](https://prometheus.io/docs/prometheus/latest/storage/).
The Grafana Mimir storage format stores each tenant's time series into their own TSDB, which persists series to an on-disk block.
By default, each block has a two-hour range.
Each on-disk block directory contains an index file, a file containing metadata, and the time series chunks.

The TSDB block files contain samples for multiple series.
The series inside the blocks are indexed by a per-block index, which indexes both metric names and labels to time series in the block files.

Grafana Mimir requires any of the following object stores for the block files:

- [Amazon S3](https://aws.amazon.com/s3)
- [Google Cloud Storage](https://cloud.google.com/storage/)
- [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
- [OpenStack Swift](https://wiki.openstack.org/wiki/Swift)
- [Local Filesystem](https://thanos.io/storage.md/#filesystem) (single node only)
