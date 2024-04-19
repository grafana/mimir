---
aliases:
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/configure-object-storage-backend/
description:
  Learn how to configure the Grafana Mimir object storage backend when
  using Jsonnet.
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
Additional configuration options are available for each one of these providers.

## Amazon S3 (`s3`) storage configuration options

Amazon S3 storage can be accessed without credentials when [using Amazon VPC](https://aws.amazon.com/premiumsupport/knowledge-center/s3-private-connection-no-authentication/).
In this case, `storage_s3_secret_access_key` and `storage_s3_access_key_id` are optional and can be left null, as in the following example:

```jsonnet
{
  _config+:: {
    storage_backend: 's3',
    blocks_storage_bucket_name: 'blocks-bucket',
    aws_region: 'af-south-1',
  }
}
```

If credentials are required, it is a good practice to keep them in secrets. In that case environment variable interpolation can be used:

```jsonnet
{
  _config+:: {
    storage_backend: 's3',
    storage_s3_access_key_id: '$(BLOCKS_STORAGE_S3_ACCESS_KEY_ID)',
    storage_s3_secret_access_key: '$(BLOCKS_STORAGE_S3_SECRET_ACCESS_KEY)',
    aws_region: 'af-south-1',
    blocks_storage_bucket_name: 'blocks-bucket',
  }
}
```

## Azure (`azure`) storage configuration options

[Hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace) must be disabled in Azure Blob Storage.
Otherwise, Grafana Mimir will leave empty directories behind after deleting blocks.

The Azure storage client requires the `storage_azure_account_name` and `storage_azure_account_key` to be configured.
It is a good practice to keep them in secrets. In that case environment variable interpolation can be used:

```jsonnet
{
  _config+:: {
    storage_backend: 'azure',
    storage_azure_account_name: '$(STORAGE_AZURE_ACCOUNT_NAME)',
    storage_azure_account_key: '$(STORAGE_AZURE_ACCOUNT_KEY)',
    blocks_storage_bucket_name: 'blocks-bucket',
  }
}
```

## Google Cloud Storage (`gcs`) storage configuration options

There are multiple [ways to configure Google Cloud Storage client]({{< relref "../../configure/configuration-parameters#gcs_storage_backend" >}}).
If you run Mimir on Google Cloud Platform it is possible that [the environment already has the credentials configured](https://cloud.google.com/storage/docs/authentication#libauth),
in that case the minimum jsonnet configuration is valid:

```jsonnet
{
  _config+:: {
    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',
  }
}
```

You can use the `storage_gcs_service_account` configuration key to provide the service account when authentication is needed.
It is a good practice to keep credentials in secrets, so environment variable interpolation can be used:

```jsonnet
{
  _config+:: {
    storage_backend: 'gcs',
    storage_gcs_service_account: '$(STORAGE_GCS_SERVICE_ACCOUNT)',
    blocks_storage_bucket_name: 'blocks-bucket',
  }
}
```

Alternatively, you can set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the service account file mounted from a secret.
