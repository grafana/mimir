# Copyblocks

This program can copy Mimir blocks between two buckets.

By default, the copy will be attempted server-side if both the source and destination specify the same object storage service. If the buckets are on different object storage services, or if `--client-side-copy` is passed, then the copy will be performed client side.

The currently supported services are Amazon Simple Storage Service (S3 and S3-compatible), Azure Blob Storage (ABS), and Google Cloud Storage (GCS).

## Features

- Prevents copying blocks multiple times to the same destination bucket by uploading block marker files to the source bucket
- Runs continuously with periodic checks when supplied a time duration with `--copy-period`, otherwise runs one check then exits
- Include or exclude users from having blocks copied (`--enabled-users` and `--disabled-users`)
- Configurable minimum block duration (`--min-block-duration`) to avoid copying blocks that will be compacted
- Configurable time range (`--min-time` and `--max-time`) to only copy blocks inclusively within a provided range
- Log what would be copied without actually copying anything with `--dry-run`

### Example for Google Cloud Storage

```bash
./copyblocks \
  --source-service gcs \
  --destination-service gcs \
  --copy-period 24h \
  --min-block-duration 23h \
  --source-bucket <source bucket name> \
  --destination-bucket <destination bucket name>
```

### Example for Azure Blob Storage

```bash
./copyblocks \
  --source-service abs \
  --destination-service abs \
  --copy-period 24h \
  --min-block-duration 23h \
  --source-bucket https://<source account name>.blob.core.windows.net/<source bucket name> \
  --azure-source-account-name <source account name> \
  --azure-source-account-key <source account key> \
  --destination-bucket https://<destination account name>.blob.core.windows.net/<destination bucket name> \
  --azure-destination-account-name <destination account name> \
  --azure-destination-account-key <destination account key>
```

### Example for Amazon Simple Storage Service

The destination is called to intiate the server-side copy which may require setting up additional permissions for the copy to have access the source bucket.
Consider passing `--client-side-copy` to avoid having to deal with that.

```bash
./copyblocks \
  --source-service s3 \
  --destination-service s3 \
  --copy-period 24h \
  --min-block-duration 23h \
  --source-bucket <source bucket name> \
  --s3-source-access-key <source access key> \
  --s3-source-secret-key <source secret key> \
  --s3-source-endpoint <source endpoint> \
  --destination-bucket <destination bucket name> \
  --s3-destination-access-key <destination access key> \
  --s3-destination-secret-key <destination secret key> \
  --s3-destination-endpoint <destination endpoint>
```

### Example for copying between different providers

Combine the relavant source and destination configuration options using the above examples as a guide.
For instance, to copy from S3 to ABS:

```bash
./copyblocks \
  --source-service s3 \
  --destination-service abs \
  --copy-period 24h \
  --min-block-duration 23h \
  --source-bucket <source bucket name> \
  --s3-source-access-key <source access key> \
  --s3-source-secret-key <source secret key> \
  --s3-source-endpoint <source endpoint> \
  --destination-bucket https://<destination account name>.blob.core.windows.net/<destination bucket name> \
  --azure-destination-account-name <destination account name> \
  --azure-destination-account-key <destination account key>
```
