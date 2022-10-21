---
description: Learn how to configure the Grafana Mimir object storage backend when using Jsonnet.
menuTitle: Configure object storage
title: Configure the Grafana Mimir object storage backend with Jsonnet
weight: 20
---

# Configure the Grafana Mimir object storage backend with Jsonnet

You can configure the object storage backend for all Mimir components from a single place.
The minimum Jsonnet code required for this is:

```jsonnet
{
  _config+:: {
    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
  }
}
```

The `storage_backend` option must be one of either `azure`, `gcs`, or `s3`.
For each one of those providers, additional configuration options are required:

- Amazon S3 (`s3`)

  - `aws_region`

- Azure (`azure`)

  - `storage_azure_account_name`
  - `storage_azure_account_key`

> **Note:** You need to manually provide the storage credentials for `s3` and `gcs` by using additional command line arguments as necessary.
> For more information about different common storage configurations, see [Grafana Mimir configuration parameters: `common`]({{< relref "../../configure/reference-configuration-parameters/index.md#common" >}}).
>
> For more information about how to provide GCS and S3 storage credentials see the following resources:
>
> - [Grafana Mimir configuration parameters: `gcs_storage_backend`]({{< relref "../../configure/reference-configuration-parameters/#gcs_storage_backend" >}})
> - [Grafana Mimir configuration parameters: `s3_storage_backend`]({{< relref "../../configure/reference-configuration-parameters/#s3_storage_backend" >}})
