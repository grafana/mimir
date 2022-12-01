---
title: "Configure Grafana Mimir object storage backend"
menuTitle: "Configure object storage"
description: "Learn how to configure Grafana Mimir to use different object storage backend implementations."
weight: 65
---

# Configure Grafana Mimir object storage backend

Grafana Mimir can use different object storage services to persist blocks containing the metrics data, as well as recording rules and alertmanager state.
The supported backends are:

- [Amazon S3](https://aws.amazon.com/s3/) (and compatible implementations like [MinIO](https://min.io/))
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Azure Blob Storage](https://azure.microsoft.com/es-es/services/storage/blobs/)
- [Swift (OpenStack Object Storage)](https://wiki.openstack.org/wiki/Swift)

Additionally and for non-production testing purposes, you can use a file-system emulated [`filesystem`]({{< relref "../configure/reference-configuration-parameters/index.md#filesystem_storage_backend" >}}) object storage implementation.

[Ruler and alertmanager support a `local` implementation]({{< relref "../architecture/components/ruler/index.md#local-storage" >}}),
which is similar to `filesystem` in the way that it uses the local file system,
but it is a read-only data source and can be used to provision state into those components.

## Common configuration

To avoid repetition, you can use the [common configuration]({{< relref "about-configurations.md#common-configurations" >}}) and fill the [`common`]({{< relref "../configure/reference-configuration-parameters/index.md#common" >}}) configuration block or by providing the `-common.storage.*` CLI flags.

> **Note:** Blocks storage cannot be located in the same path of the same bucket as the ruler and alertmanager stores. When using the common configuration, make [`blocks_storage`]({{< relref "../configure/reference-configuration-parameters/index.md#blocks_storage" >}}) use either a:

- different bucket, overriding the common bucket name
- storage prefix

Grafana Mimir will fail to start if you configure blocks storage to use the same bucket and storage prefix that the alertmanager or ruler store uses.

Find examples of setting up the different object stores below:

> **Note**: If you're using a mixture of YAML files and CLI flags, pay attention to their [precedence logic]({{< relref "about-configurations.md#common-configurations" >}}).

### S3

```yaml
common:
  storage:
    backend: s3
    s3:
      endpoint: s3.us-east-2.amazonaws.com
      region: us-east
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}" # This is a secret injected via an environment variable
      access_key_id: "${AWS_ACCESS_KEY_ID}" # This is a secret injected via an environment variable

blocks_storage:
  s3:
    bucket_name: mimir-blocks

alertmanager_storage:
  s3:
    bucket_name: mimir-alertmanager

ruler_storage:
  s3:
    bucket_name: mimir-ruler
```

### GCS

```yaml
common:
  storage:
    backend: gcs
    gcs:
      # This is an example to illustrate what the service account content should look like.
      # We recommend injecting the service_account via an environment variable instead.
      service_account: |
        {
          "type": "service_account",
          "project_id": "my-project",
          "private_key_id": "1234abc",
          "private_key": "-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n",
          "client_email": "test@my-project.iam.gserviceaccount.com",
          "client_id": "5678",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40my-project.iam.gserviceaccount.com"
        }

blocks_storage:
  gcs:
    bucket_name: mimir-blocks

alertmanager_storage:
  gcs:
    bucket_name: mimir-alertmanager

ruler_storage:
  gcs:
    bucket_name: mimir-ruler
```

### Azure Blob Storage

```yaml
common:
  storage:
    backend: azure
  azure:
    account_key: "${SWIFT_ACCOUNT_KEY}" # This is a secret injected via an environment variable
    account_name: mimir-prod
    endpoint_suffix: "blob.core.windows.net"

blocks_storage:
  azure:
    container_name: mimir-blocks

alertmanager_storage:
  azure:
    container_name: mimir-alertmanager

ruler_storage:
  azure:
    container_name: mimir-ruler
```

### OpenStack SWIFT

```yaml
common:
  storage:
    backend: swift
  swift:
    auth_url: http://10.121.xx.xx:5000/v3
    username: mimir
    user_domain_name: Default
    password: "${OPENSTACK_API_KEY}" # This is a secret injected via an environment variable
    project_name: mimir-prod
    domain_name: Default

blocks_storage:
  swift:
    container_name: mimir-blocks

alertmanager_storage:
  swift:
    container_name: mimir-alertmanager

ruler_storage:
  swift:
    container_name: mimir-ruler
```
