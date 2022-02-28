---
title: "Encrypting data at rest"
description: "How to configure object storage encryption."
weight: 10
---

# Encrypting data at rest

Grafana Mimir supports encrypting data at rest in object storage using server-side encryption (SSE).
Configuration of SSE depends on your storage backend.

## Google Cloud Storage (GCS)

Google Cloud Storage (GCS) always encrypts the data before writing it to disk.
For more details of GCS encryption at rest, refer to [Data encryption options](https://cloud.google.com/storage/docs/encryption/).
Grafana Mimir requires no additional configuration to use GCS with SSE.

## AWS S3

Configuring SSE with AWS S3 requires configuration in the Grafana Mimir S3 client.
The S3 client is only used when the storage backend is `s3`.
Grafana Mimir supports the following AWS S3 SSE modes:

- [Server-Side Encryption with Amazon S3-Managed Keys (SSE-S3)](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html)
- [Server-Side Encryption with KMS keys Stored in AWS Key Management Service (SSE-KMS)](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html)

You can configure AWS S3 SSE globally or for specific tenants.

### Configuring AWS S3 SSE globally

Configuring AWS S3 SSE globally requires setting SSE for each of the following storage backends:

- [alertmanager_storage]({{<relref "../configuration/reference-configuration-parameters.md#alertmanager_storage" >}})
- [blocks_storage]({{<relref "../configuration/reference-configuration-parameters.md#blocks_storage" >}})
- [ruler_storage]({{<relref "../configuration/reference-configuration-parameters.md#ruler_storage" >}})

To see all AWS S3 SSE configuration parameters, refer to [sse]({{<relref "../configuration/reference-configuration-parameters.md#sse" >}}).

A snippet of a Grafana Mimir configuration file that has AWS S3 SSE with an Amazon S3-managed key configured looks as follows:

```yaml
alertmanager_storage:
  backend: "s3"
  s3:
    sse:
      type: "SSE-S3"
blocks_storage:
  backend: "s3"
  s3:
    sse:
      type: "SSE-S3"
ruler_storage:
  backend: "s3"
  s3:
    sse:
      type: "SSE-S3"
```

### Configuring AWS S3 SSE for a specific tenant

The following settings can be overridden for each tenant:

- **`s3_sse_type`**<br />
  S3 server-side encryption type.
  It must be set to enable the SSE config override for a given tenant.
- **`s3_sse_kms_key_id`**<br />
  S3 server-side encryption KMS Key ID.
  Ignored if the SSE type override is not set or the type is not `SSE-KMS`.
- **`s3_sse_kms_encryption_context`**<br />
  S3 server-side encryption KMS encryption context.
  If unset and the key ID override is set, the encryption context will not be provided to S3.
  Ignored if the SSE type override is not set or the type is not `SSE-KMS`.

To configure AWS S3 SSE for a specific tenant:

1. Ensure Grafana Mimir uses a runtime configuration file by verifying that the flag `-runtime-config.file` is set to a non-null value.
   For more information about supported runtime configuration, refer to [Runtime configuration file]({{<relref "../configuration/about-grafana-mimir-arguments.md#runtime-configuration-file" >}})
1. In the runtime configuration file, set the `overrides.<TENANT>` SSE settings.

   A partial runtime configuration file that has AWS S3 SSE with Amazon S3-managed keys set for a tenant called "tenant-a" looks as follows:

   ```yaml
   overrides:
     "tenant-a":
       s3_sse_type: "SSE-S3"
   ```

1. Save and deploy the runtime configuration file.
1. After the `-runtime-config.reload-period` has elapsed, components reload the runtime configuration file and use the updated configuration.

## Other storages

Other storage backends may support encryption at rest configured at the storage level.
