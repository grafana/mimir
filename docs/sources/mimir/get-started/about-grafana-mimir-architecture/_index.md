---
aliases:
  - ../operators-guide/architecture/about-grafana-mimir-architecture/
description: Learn about the Grafana Mimir architecture.
menuTitle: Grafana Mimir architecture
title: Grafana Mimir architecture
weight: 5
---

# Grafana Mimir architecture

Grafana Mimir has a microservices-based architecture. The system has multiple horizontally scalable microservices that can run separately and in parallel. Grafana Mimir microservices are called components.

Grafana Mimir's design compiles the code for all components into a single binary. The `-target` parameter controls which components that single binary behaves as.

To get started, you can run Grafana Mimir in [monolithic mode](../../references/architecture/deployment-modes/#monolithic-mode) with all components running simultaneously in one process. For more information, refer to [Deployment modes](../../references/architecture/deployment-modes/).

## About supported architectures

Starting with version 3.0, you can deploy Grafana Mimir using the following architectures:

| Architecture               | Description                                                                                                                      | Related Documentation                                                   |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Ingest storage (preferred) | Uses Kafka as a central pipeline to decouple read and write operations                                                           | [About ingest storage architecture](about-ingest-storage-architecture/) |
| Classic                    | Uses stateful ingesters with local write-ahead logs to manage both the ingestion of new data and serving recent data for queries | [About classic architecture](about-classic-architecture/)               |

## The role of Prometheus

Prometheus instances scrape samples from various targets and push them to Grafana Mimir by using Prometheus' [remote write API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).
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
