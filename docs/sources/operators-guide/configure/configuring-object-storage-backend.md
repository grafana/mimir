---
title: "Configuring Grafana Mimir object storage backend"
menuTitle: "Configuring object storage"
description: "Learn how to configure Grafana Mimir to use different object storage backend implementations."
weight: 70
---

# Configuring Grafana Mimir object storage backend

Grafana Mimir can use different object storage services to persist blocks containing the metrics data, as well as recording rules and alertmanager state.
The supported alternatives are:

- [Amazon S3](https://aws.amazon.com/s3/) (and compatible implementations like [MinIO](https://min.io/))
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Azure Blob Storage](https://azure.microsoft.com/es-es/services/storage/blobs/)
- [Swift (OpenStack Object Storage)](https://wiki.openstack.org/wiki/Swift)

Additionally, a file system emulated [`filesystem`]({{< relref "../configuring/reference-configuration-parameters/index.md#filesystem_storage_backend" >}}) object storage implementation can be also used for testing purposes (it's not recommended for production workloads).

Ruler and alertmanager support a `local` implementation, which is similar to `filesystem` in the way that it uses the local file system, but it is a read-only data source and can be used to provision state into those components.

## Common configuration

The [common configuration]({{< relref "about-configurations.md#common-configurations" >}}) can be used to avoid repetition by filling the [`common`]({{< relref "../configuring/reference-configuration-parameters/index.md#common" >}}) configuration block or by providing the `-common.storage.*` CLI flags.

Note that blocks storage can't be located in the same path of the same bucket as the ruler and alertmanager stores, so when using the common configuration, [`blocks_storage`]({{< relref "../configuring/reference-configuration-parameters/index.md#blocks_storage" >}}) should either:

- use a different bucket, overriding the common bucket name
- use a storage prefix

Grafana Mimir will fail to start if blocks storage is configured to use the same bucket and storage prefix as alertmanager or ruler store.

A valid configuration for the object storage (taken from the ["Play with Grafana Mimir" tutorial](https://grafana.com/tutorials/play-with-grafana-mimir/)) would be:

```yaml
common:
  storage:
    backend: s3
    s3:
      endpoint: minio:9000
      access_key_id: mimir
      secret_access_key: supersecret
      insecure: true
      bucket_name: mimir

blocks_storage:
  storage_prefix: blocks
```
