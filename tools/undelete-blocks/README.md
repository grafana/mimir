# Undelete Blocks

This program is a disaster recovery tool that can restore deleted Mimir blocks in object storage.

The currently supported services are Amazon Simple Storage Service (S3 and S3-compatible), Azure Blob Storage (ABS), and Google Cloud Storage (GCS).

## Prerequisites

- A bucket with versioning enabled is assumed.
- A deleted object can not be recovered if a noncurrent version of the object does not exist.
- This program should ideally not be run while deletes are still occurring on the affected blocks since the undelete may race the delete. To prevent deletes from Mimir itself either temporarily stop the compactor component entirely or set the appropriate configuration.

## Flags

- `--blocks-from` (required) Accepted values are `json`, `lines`, or `listing`. When `listing` is provided `--input-file` is ignored and object storage listings are used to discover tenants and blocks.
- `--input-file` (optional) The file path to read when `--blocks-from` is `json` or `lines`, otherwise ignored. The default (`"-"`) assumes reading from standard input.
- `--include-tenants` (optional) A comma separated list of what tenants to target.
- `--exclude-tenants` (optional) A comma separated list of what tenants to ignore. Has precedence over `--include-tenants`.
- `--dry-run` (optional) When set the changes that would be made to object storage are only logged rather than performed.

Each supported object storage service also has an additional set of flags (see examples in [Running](##Running)).

## Input formats (`--blocks-from`)

The `json` format is a map of tenants to a list of blocks:

```json
{
  "tenant1": ["01GDY90HMVFPSJHXZRQH8KRAME", "01GE0SV77NX8ASC7JN0ZQMN0WM"],
  "tenant2": ["01GZDNKM6SQ9S7W5YQBDF0DK49"]
}
```

The `lines` format is a tenant and a block separated by a single `/` on each line:

```
tenant1/01GDY90HMVFPSJHXZRQH8KRAME
tenant1/01GE0SV77NX8ASC7JN0ZQMN0WM
tenant2/01GZDNKM6SQ9S7W5YQBDF0DK49
```

No input is supplied with `listing` since the block information is derived from the object storage itself.

Tenants specified in any format can be refined further by the `--include-tenants` and `--exclude-tenants` options if they are provided.

## What does undeleting a block mean?

Let's start with some background.

The order of writes within a Mimir block is:

1. Files except `meta.json` (chunks/index)
2. `meta.json` (block now complete)

When a block needs to be deleted, the following actions are performed:

1. A local delete marker is added in the block prefix. This is a "soft delete".
2. A global delete marker for the block is added in the tenant's marker prefix for easier indexing.
3. Some time passes to provide time for the delete markers to be picked up and added to the bucket index.
4. Files besides delete markers are deleted within the block's prefix (meta first, then chunks/index).
5. The delete markers are deleted, local then global.

An undelete tries to get a block back to being a complete block by restoring noncurrent object versions (as needed) in the order of a block write, then deleting the delete markers (local then global), then restoring no-compact markers (local then global) as needed. If required data for a complete block (according to `meta.json`) is missing then that block will remain untouched.

## Known limitations

Files not listed within the `meta.json` of a block are not restored if they were deleted. No-compact markers are an exception to this and will be restored if possible.

## Running

Running `go build .` in this directory builds the program. Then use an example below as a guide.

### Example for Google Cloud Storage

```bash
./undelete-blocks \
  --backend gcs \
  --gcs.bucket-name <bucket name> \
  --blocks-from listing \
  --include-tenants tenant1,tenant2 \
  --dry-run
```

### Example for Azure Blob Storage

```bash
./undelete-blocks \
  --backend azure \
  --azure.container-name <container name> \
  --azure.account-name <account name> \
  --azure.account-key <account key> \
  --blocks-from json \
  --input-file undelete.json \
  --exclude-tenants tenant1 \
  --dry-run
```

### Example for Amazon Simple Storage Service

```bash
./undelete-blocks\
  --backend s3 \
  --s3.bucket-name <bucket name> \
  --s3.access-key-id <access key id> \
  --s3.secret-access-key <secret access key> \
  --s3.endpoint <endpoint> \
  --blocks-from lines \
  --input-file undelete.lines \
  --dry-run
```
