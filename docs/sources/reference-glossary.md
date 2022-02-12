---
title: "Reference: Glossary"
description: ""
weight: 10000
---

# Reference: Glossary

## Blocks storage

Blocks storage is the Mimir storage engine based on the Prometheus TSDB.
Grafana Mimir stores blocks in object stores such as AWS S3, Google Cloud Storage (GCS), or Azure blob storage.
For the full list of supported backends and more information, refer to [Blocks storage]({{<relref "./blocks-storage/_index.md" >}})

## Chunk

A chunk is an object containing encoded timestamp-value pairs for one series.

## Churn

Churn is the frequency at which series become idle.

A series become idle once it's no longer exported by the monitored targets.
Typically, series become idle when a monitored target process or node gets terminated.

## Flushing

Series flushing is the operation run by ingesters to offload time series from memory and store them in the long-term storage.

## HA Tracker

The HA Tracker is a feature of Mimir distributor which is used to deduplicate received series coming from two (or more) Prometheus servers configured in HA pairs.

For more information, please refer to the guide "[Config for sending HA Pairs data to Mimir](../guides/ha-pair-handling.md)".

## Hash ring

The hash ring is a distributed data structure used by Mimir for sharding, replication and service discovery. The hash ring data structure gets shared across Mimir replicas via gossip or a key-value store.

For more information, please refer to the [Architecture](../architecture.md#the-hash-ring) documentation.

## Org

_See [Tenant](#tenant)._

## Ring

_See [Hash ring](#hash-ring)._

## Sample

A sample is a single timestamped value in a time series.

For example, given the series `node_cpu_seconds_total{instance="10.0.0.1",mode="system"}` its stream of values (samples) could be:

```
# Display format: <value> @<timestamp>
11775 @1603812134
11790 @1603812149
11805 @1603812164
11819 @1603812179
11834 @1603812194
```

## Series

In the Prometheus ecosystem, a series (or time series) is a single stream of timestamped values belonging to the same metric, with the same set of label key-value pairs.

For example, given a single metric `node_cpu_seconds_total` you may have multiple series, each one uniquely identified by the combination of metric name and unique label key-value pairs:

```
node_cpu_seconds_total{instance="10.0.0.1",mode="system"}
node_cpu_seconds_total{instance="10.0.0.1",mode="user"}
node_cpu_seconds_total{instance="10.0.0.2",mode="system"}
node_cpu_seconds_total{instance="10.0.0.2",mode="user"}
```

## Tenant

A tenant (also called "user" or "org") is the owner of a set of series written to and queried from Mimir. Mimir multi-tenancy support allows you to isolate series belonging to different tenants. For example, if you have two tenants `team-A` and `team-B`, `team-A` series will be isolated from `team-B`, and each team will be able to query only their own series.

For more information, please refer to:

- [HTTP API authentication](../api/_index.md#authentication)
- [About tenant IDs]({{<relref "./about-tenant-ids.md" >}})

## Time series

_See [Series](#series)._

## User

_See [Tenant](#tenant)._

## WAL

The Write-Ahead Log (WAL) is an append only log stored on disk used by ingesters to recover their in-memory state after the process gets restarted, either after a clear shutdown or an abruptly termination. The WAL is supported by blocks storage engines.

For more information, see [Ingesters with WAL](../blocks-storage/_index.md#the-write-path).
