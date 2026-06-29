# copyblocks

This program can copy Mimir blocks between two buckets or from a bucket to a backfill destination. It tracks what has been copied by uploading copy markers to the source bucket, preventing blocks from being copied multiple times to the same destination.

By default, a copy between two buckets will be attempted server-side if both the source and destination specify the same object storage service. If the buckets are on different object storage services, or if `--client-side-copy` is passed, then the copy will be performed client side.

The currently supported object storage services are Amazon Simple Storage Service (S3 and S3-compatible), Azure Blob Storage (ABS), and Google Cloud Storage (GCS).

## Features

- Delete copy markers (e.g. `tenant1/markers/01EZED0X3YZMNJ3NHGMJJKMHCR-copied-backfill`) instead of copying with `--clear-copy-markers`
- Run continuously with periodic checks when supplied a time duration with `--copy-period`, otherwise run one check then exit
- Include or exclude users from having blocks copied (`--enabled-users` and `--disabled-users`)
- Set a minimum block duration (`--min-block-duration`) and optionally skip that check for blocks marked as no-compact (`--skip-no-compact-block-duration-check`) to target blocks that are not awaiting compaction
- Set a time range (`--min-time` and `--max-time`) to only copy blocks within a provided range (e.g. `--min-time 2026-01-15T00:00:00Z`)
- Copy blocks between users with `--user-mapping`. For instance, `--user-mapping="user1:user2,user3:user4"` maps source blocks from `user1` to `user2` and source blocks from `user3` to `user4`. If you don't provide a mapping for a user, it is assumed to be identical to the source user.
- Log what would be copied without actually copying anything with `--dry-run`

## Running

Run `go build` in this directory to build the program. Then, use an example below as a guide.

### Example for Google Cloud Storage

```bash
./copyblocks \
  --source.backend gcs \
  --destination.backend gcs \
  --gcs.source.bucket-name <source bucket name> \
  --gcs.destination.bucket-name <destination bucket name> \
  --copy-period 24h \
  --min-block-duration 13h \
  --dry-run
```

### Example for Azure Blob Storage

```bash
./copyblocks \
  --source.backend azure \
  --destination.backend azure \
  --azure.source.container-name <source container name> \
  --azure.source.account-name <source account name> \
  --azure.source.account-key <source account key> \
  --azure.destination.container-name <destination container name> \
  --azure.destination.account-name <destination account name> \
  --azure.destination.account-key <destination account key> \
  --copy-period 24h \
  --min-block-duration 13h \
  --dry-run
```

### Example for Amazon Simple Storage Service

The destination bucket is called to initiate the server-side copy which may require setting up additional permissions for the copy to have access the source bucket.
Consider passing `--client-side-copy` to avoid having to deal with that.

```bash
./copyblocks \
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
  --copy-period 24h \
  --min-block-duration 13h \
  --dry-run
```

If both `--s3.<source|destination>.access-key-id` and `--s3.<source|destination>.secret-access-key` are omitted, credentials are resolved from the environment. This supports IAM roles for service accounts (IRSA) on EKS, ECS task roles, and EC2 instance metadata, and lets a single role that has access to both buckets authenticate the source and destination clients without passing static credentials.

### Example for copying between different object storage providers

Combine the relevant source and destination configuration options using the above examples as a guide.
For instance, to copy from S3 to ABS:

```bash
./copyblocks \
  --source.backend s3 \
  --destination.backend azure \
  --s3.source.bucket-name <source bucket name> \
  --s3.source.access-key-id <source access key> \
  --s3.source.secret-access-key <source secret access key> \
  --s3.source.endpoint <source endpoint> \
  --azure.destination.container-name <destination container name> \
  --azure.destination.account-name <destination account name> \
  --azure.destination.account-key <destination account key> \
  --copy-period 24h \
  --min-block-duration 13h \
  --dry-run
```

### Example for backfilling to Mimir

Copies blocks from object storage to a Mimir instance using the block upload API.

```bash
./copyblocks \
  --source.backend s3 \
  --destination.backend backfill \
  --s3.source.bucket-name <source bucket name> \
  --s3.source.access-key-id <source access key id> \
  --s3.source.secret-access-key <source secret access key> \
  --s3.source.endpoint <source endpoint> \
  --backfill.address <Mimir address> \
  --backfill.id <tenant id> \
  --backfill.key <token used in basic auth> \
  --min-time 2026-01-15T00:00:00Z \
  --max-time 2026-02-15T00:00:00Z \
  --dry-run
```

`--backfill.id` sets the user ID (X-Scope-OrgID header). Use `--backfill.auth-token` for bearer token auth or `--backfill.key` for basic auth. TLS is configurable with `--backfill.tls-*`.

Note that `--user-mapping` cannot be used with the backfill destination.
