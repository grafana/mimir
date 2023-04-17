# Copyblocks

This program can copy Mimir blocks between two Google Cloud Storage (GCS) buckets or two Azure Blob Storage (ABS) buckets.
It is specific to these two services and performs server-side copies between buckets.

## Features

- Prevents copying blocks multiple times to the same destination bucket by uploading block marker files to the source bucket
- Run as a one-time copy job, or run continuously with periodic checks (`--copy-period`)
- Include or exclude users from having blocks copied (`--enabled-users` and `--disabled-users`)
- Configurable minimum block duration (`--min-block-duration`) to avoid copying blocks that are too small
- Configurable time range (`--min-time` and `--max-time`) to only copy blocks inclusively within a provided range

### Example for Google Cloud Storage

```bash
./copyblocks \
  --service gcs \
  --copy-period 24h \
  --source-bucket <source bucket name> \
  --destination-bucket <destination bucket name> \
  --min-block-duration 24h
```

### Example for Azure Blob Storage

```bash
./copyblocks \
  --service abs \
  --copy-period 24h \
  --source-bucket https://<source account name>.blob.core.windows.net/<source bucket name> \
  --azure-source-account-name <source account name> \
  --azure-source-account-key <source account key> \
  --azure-destination-account-name <destination account name> \
  --azure-destination-account-key <destination account key> \
  --destination-bucket https://<destination account name>.blob.core.windows.net/<destination bucket name> \
  --min-block-duration 24h
```
