---
description: Learn about the hardware and infrastructure requirements for running Grafana Mimir.
menuTitle: Hardware requirements
title: Grafana Mimir hardware requirements
weight: 5
---

# Grafana Mimir hardware requirements

This page describes the hardware, storage, and infrastructure requirements for running Grafana Mimir in production.
For component-level CPU, memory, and disk sizing, refer to [Planning Grafana Mimir capacity]({{< relref "../manage/run-production-environment/planning-capacity" >}}).

## Compute

The actual CPU and memory requirements depend on the number of active series, the rate of samples ingested per second, and query load.
Refer to [Planning Grafana Mimir capacity]({{< relref "../manage/run-production-environment/planning-capacity" >}}) for per-component sizing guidance.

As a general guideline, provision nodes with a CPU-to-memory ratio of at least 1:4, meaning that for every CPU core, there should be 4GB of memory.
For example, a node with 8 CPU cores should have at least 32 GB of memory.
Ingesters and store-gateways are the most memory-intensive components.

## Disk storage

Components such as ingesters, store-gateways, and compactors require fast and persistent, local disk resources to be available to the host machine storage for the write-ahead log (WAL), TSDB blocks, and temporary data during compaction.

### Disk requirements

Grafana Mimir requires block storage devices that provide:

- **Low latency**: SSD-backed storage is strongly recommended. Ingester write-ahead log (WAL) writes and store-gateway index lookups are latency-sensitive.
- **Adequate IOPS**: A minimum of 50 IOPS per GB is recommended for ingester disks. Compactors also benefit from high IOPS during block compaction.
- **POSIX filesystem semantics**: The filesystem must support standard POSIX operations including `mmap`, `fsync`, and file locking. Grafana Mimir's TSDB engine relies on memory-mapped files for reading on-disk data.

**Cloud provider examples:**

| Cloud provider | Recommended disk types                                          |
| -------------- | --------------------------------------------------------------- |
| AWS            | EBS `io1` or `gp3` SSD volumes                                  |
| GCP            | `pd-ssd` persistent disks                                       |
| Azure          | Premium SSD managed disks (`managed-csi-premium` storage class) |

### Unsupported filesystems

{{< admonition type="warning" >}}
**NFS and network filesystems are not supported** for any Grafana Mimir component that uses local disk storage.
{{< /admonition >}}

The following types of filesystems are **not supported**:

- NFS (including Azure NFS file shares)
- Amazon EFS
- FUSE-based network filesystems
- Any filesystem that does not provide full POSIX `mmap` consistency guarantees

Network filesystems do not provide the consistency guarantees that Grafana Mimir requires for memory-mapped I/O. Using them can result in:

- Component crashes.
- Silent data corruption.

For more background, refer to the [Prometheus storage documentation](https://prometheus.io/docs/prometheus/latest/storage/#operational-aspects).

## Network

Grafana Mimir components communicate over gRPC and HTTP.
For production deployments with high ingest or query throughput, a network bandwidth of 10 gigabit per second or faster between nodes is recommended.

For smaller or development deployments, lower bandwidth is acceptable, but network latency between components should remain low (ideally sub-millisecond within the same datacenter or availability zone).

## Object storage

Grafana Mimir requires object storage for long-term block storage, ruler configuration, and alertmanager state.
The following object storage services are supported:

| Provider    | Supported service          |
| ----------- | -------------------------- |
| AWS         | Amazon S3                  |
| GCP         | Google Cloud Storage (GCS) |
| Azure       | Azure Blob Storage         |
| Self-hosted | Any S3-compatible API      |
