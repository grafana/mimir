---
title: "Blocks storage"
weight: 20
---

The blocks storage is a Grafana Mimir storage engine based on [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/). Blocks storage stores each tenant's time series into their own TSDB which write out their series to a on-disk block, which by default are `2h` block range periods. Each block is composed by chunk files, which contain the timestamp-value pairs for multiple series, an index, which indexes metric names and labels to time series in the chunk files, and a metadata file.

Blocks storage supports the following backends:

- [Amazon S3](https://aws.amazon.com/s3)
- [Google Cloud Storage](https://cloud.google.com/storage/)
- [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
- [OpenStack Swift](https://wiki.openstack.org/wiki/Swift)
- Local Filesystem (single node only)
