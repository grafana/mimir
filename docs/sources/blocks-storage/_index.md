---
title: "Blocks storage"
linkTitle: "Blocks storage"
weight: 3
menu:
---

The blocks storage is a Mimir storage engine based on [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/): it stores each tenant's time series into their own TSDB which write out their series to a on-disk block (defaults to 2h block range periods). Each block is composed by chunk files - containing the timestamp-value pairs for multiple series - and an index, which indexes metric names and labels to time series in the chunk files.

The supported backends for the blocks storage are:

- [Amazon S3](https://aws.amazon.com/s3)
- [Google Cloud Storage](https://cloud.google.com/storage/)
- [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
- [OpenStack Swift](https://wiki.openstack.org/wiki/Swift)
- [Local Filesystem](https://thanos.io/storage.md/#filesystem) (single node only)

_Internally, some components are based on [Thanos](https://thanos.io), but no Thanos knowledge is required in order to run it._

## Architecture

- [Querier](./querier.md)
- [Store-gateway](./store-gateway.md)
- [Production tips](./production-tips.md)
