---
description: Tune Grafana Mimir according to your use cases.
menuTitle: Tuning
title: Tune Grafana Mimir according to your use cases
weight: 110
---

# Tune Grafana Mimir according to your use cases

For most use cases, you can use the default settings that come with Mimir.
However, sometimes you need to tune Mimir to reach optimal performance. Use the following guidance when tuning settings in Mimir.

## Heavy multi-tenancy

For each tenant, Mimir opens and maintains a TSDB in memory. If you have a significant number of tenants, the memory overhead might become prohibitive.
To reduce the associated overhead, consider the following:

- Reduce `-blocks-storage.tsdb.head-chunks-write-buffer-size-bytes`, default `4MB`. For example, try `1MB` or `128KB`.
- Reduce `-blocks-storage.tsdb.stripe-size`, default `16384`. For example, try `256`, or even `64`.
- Configure [shuffle sharding](https://grafana.com/docs/mimir/latest/configure/configure-shuffle-sharding/)

## Compression

Depending on the CPU model used in the underlying infrastructure, the compression for both WALs and GRPC communication might consume a significant portion of the available CPU resources.
To identify this case, you can use profiling with tools like [Grafana Pyroscope](https://grafana.com/docs/pyroscope/latest/).

To reduce resource consumption, consider the following:

- Make sure `wal_compression_enabled` is not enabled.
- Make sure `grpc_compression` is either off, which is the default, or configured to `snappy`. `gzip` consumes more CPU than `snappy`. However, disabling `grpc_compression` implies more network traffic, and in turn, might increase the total cost of ownership (TCO) of running Mimir.

If you must use compression, for example, to fit in the network bandwidth, consider using nodes with more powerful CPU. This implies an increase in TCO.

## Cache size

Grafana Mimir relies on Memcached for its caches. Memcached relies, by default, only on memory.
Memcached [extstore](https://docs.memcached.org/features/flashstorage/) feature allows to extend Memcached’s memory space onto flash (or similar) storage.

Refer to [how we scaled Grafana Cloud Logs' Memcached cluster to 50TB and improved reliability](https://grafana.com/blog/2023/08/23/how-we-scaled-grafana-cloud-logs-memcached-cluster-to-50tb-and-improved-reliability/).

## Periodic latency spikes when cutting blocks

Depending on the workload, you might witness latency spikes when Mimir cuts blocks.
To reduce the impact of this behavior, consider the following:

- Upgrade to `2.15+`. Refer to <https://github.com/grafana/mimir/commit/03f2f06e1247e997a0246d72f5c2c1fd9bd386df>.
- Reduce `-blocks-storage.tsdb.block-ranges-period`, default `2h`. For example. try `1h`.
