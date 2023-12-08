# Copyprefix

This program copies objects by prefix between two buckets. If your infrastructure is largely on a single object storage service their respective CLI tool may be more convenient to use.

By default, copying will be attempted server-side if both the source and destination specify the same object storage service. If the buckets are on different object storage services, or if `--client-side-copy` is passed, then the copy will be performed client side.

The currently supported services are Amazon Simple Storage Service (S3 and S3-compatible), Azure Blob Storage (ABS), and Google Cloud Storage (GCS).

## Features

- Specify which prefix to copy from the source bucket with `--source-prefix`. The default source prefix is the empty string, which copies everything
- The source prefix on objects copied to the destination can be replaced with a different prefix by providing `--destination-prefix`
- Log what would be copied without actually copying anything with `--dry-run`
- Skip overwrites (as determined by a listing on the destination, so it is only best effort) by specifying `--skip-overwrites`

## Running

Running `go build` in this directory builds the program. Then use an example below as a guide.

### Example for Google Cloud Storage

```bash
./copyprefix \
  --source.backend gcs \
  --destination.backend gcs \
  --gcs.source.bucket-name <source bucket name> \
  --gcs.destination.bucket-name <destination bucket name> \
  --source-prefix tenant1 \
  --destination-prefix tenant2 \
  --dry-run
```

### Example for Azure Blob Storage

```bash
./copyprefix \
  --source.backend azure \
  --destination.backend azure \
  --azure.source.container-name <source container name> \
  --azure.source.account-name <source account name> \
  --azure.source.account-key <source account key> \
  --azure.destination.container-name <destination container name> \
  --azure.destination.account-name <destination account name> \
  --azure.destination.account-key <destination account key> \
  --source-prefix rules \
  --skip-overwrites \
  --dry-run
```

### Example for Amazon Simple Storage Service

The destination is called to intiate the server-side copy which may require setting up additional permissions for the copy to have access the source bucket.
Consider passing `--client-side-copy` to avoid having to deal with that.

```bash
./copyprefix \
  --source.backend s3 \
  --destination.backend s3 \
  --s3.source.bucket-name <source bucket name> \
  --s3.source.access-key-id <source access key id> \
  --s3.source.secret-access-key <source secret access key> \
  --s3.source.endpoint <source endpoint> \
  --s3.destination.bucket-name <destination bucket name> \
  --s3.destination.access-key-id <destination access key id> \
  --s3.destination.secret-access-key <destination secret access key> \
  --s3.destination.endpoint <destination endpoint> \
  --source-prefix tenant2 \
  --dry-run
```

### Example for copying between different providers

Combine the relevant source and destination configuration options using the above examples as a guide.
For instance, to copy from S3 to ABS:

```bash
./copyprefix \
  --source.backend s3 \
  --destination.backend azure \
  --s3.source.bucket-name <source bucket name> \
  --s3.source.access-key-id <source access key id> \
  --s3.source.secret-access-key <source secret access key> \
  --s3.source.endpoint <source endpoint> \
  --azure.destination.container-name <destination container name> \
  --azure.destination.account-name <destination account name> \
  --azure.destination.account-key <destination account key> \
  --source-prefix tenant3 \
  --dry-run
```
