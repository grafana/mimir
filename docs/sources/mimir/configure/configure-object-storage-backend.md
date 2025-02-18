---
aliases:
  - ../operators-guide/configure/configure-object-storage-backend/
description:
  Learn how to configure Grafana Mimir to use different object storage
  backend implementations.
menuTitle: Object storage
title: Configure Grafana Mimir object storage backend
weight: 65
---

# Configure Grafana Mimir object storage backend

Grafana Mimir can use different object storage services to persist blocks containing the metrics data, as well as recording rules and Alertmanager state.

Mimir doesn't create the configured storage bucket, you must create it yourself.
The supported backends are:

- [Amazon S3](https://aws.amazon.com/s3/) (and compatible implementations like [MinIO](https://min.io/))
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Azure Blob Storage](https://azure.microsoft.com/es-es/services/storage/blobs/)
- [Swift (OpenStack Object Storage)](https://wiki.openstack.org/wiki/Swift)

{{% admonition type="note" %}}
Like Amazon S3, the chosen object storage implementation must not create directories.
Grafana Mimir doesn't have any notion of object storage directories, and so will leave
empty directories behind when removing blocks. For example, if you use Azure Blob Storage, you must disable
[hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace).
{{% /admonition %}}

Additionally and for non-production testing purposes, you can use a file-system emulated [`filesystem`]({{< relref "./configuration-parameters#filesystem_storage_backend" >}}) object storage implementation.

[Ruler and alertmanager support a `local` implementation]({{< relref "../references/architecture/components/ruler#local-storage" >}}),
which is similar to `filesystem` in the way that it uses the local file system,
but it is a read-only data source and can be used to provision state into those components.

## Common configuration

To avoid repetition, you can use the [common configuration]({{< relref "./about-configurations#common-configurations" >}}) and fill the [`common`]({{< relref "./configuration-parameters#common" >}}) configuration block or by providing the `-common.storage.*` CLI flags.

To use environment variables in the configuration file, ensure that you [enable expansion]({{< relref "./configuration-parameters#use-environment-variables-in-the-configuration" >}}) for the variables.

{{< admonition type="note" >}}
Blocks storage can't be located in the same path of the same bucket as the ruler and Alertmanager stores.

When using the common configuration, make [`blocks_storage`]({{< relref "./configuration-parameters#blocks_storage" >}}) use either a:

- different bucket, overriding the common bucket name
- storage prefix

{{< /admonition >}}

Grafana Mimir will fail to start if you configure blocks storage to use the same bucket and storage prefix that the Alertmanager or ruler store uses.

Find examples of setting up the different object stores below:

{{< admonition type="note" >}}
If you're using a mixture of YAML files and CLI flags, pay attention to their [precedence logic]({{< relref "./about-configurations#common-configurations" >}}).
{{< /admonition >}}

### S3

```yaml
common:
  storage:
    backend: s3
    s3:
      endpoint: s3.us-east-2.amazonaws.com
      region: us-east-2
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

You must disable [hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace), otherwise Grafana Mimir will leave empty directories behind when deleting blocks.

```yaml
common:
  storage:
    backend: azure
    azure:
      account_key: "${AZURE_ACCOUNT_KEY}" # This is a secret injected via an environment variable
      account_name: mimirprod
      endpoint_suffix: "blob.core.windows.net"

blocks_storage:
  backend: azure
  azure:
    container_name: mimir-blocks

alertmanager_storage:
  backend: azure
  azure:
    container_name: mimir-alertmanager

ruler_storage:
  backend: azure
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
