# mark-blocks

This program creates or removes markers for blocks.

## Flags

- `--mark-type` (required) Mark type to create or remove, valid options: `deletion`, `no-compact`
- `--tenant` (optional) Tenant ID of the owner of the block(s). If empty (the default) then each block is assumed to be of the form tenantID/blockID, otherwise blockID
- `--blocks` (optional) Comma separated list of blocks. If non-empty, `--blocks-file` is ignored
- `--blocks-file` (optional) File containing a block per-line. Defaults to standard input (`-`). Ignored if `--blocks` is non-empty
- `--meta-presence-policy` (optional) Policy on presence of block `meta.json` files: `none`, `skip-block`, or `require`. Defaults to `skip-block`
- `--remove` (optional) If marks should be removed rather than uploaded. Defaults to `false`
- `--resume-index` (optional) The index of the block to resume from. This index is logged to assist in recovering from partial failures
- `--concurrency` (optional) How many markers to upload or remove concurrently. Defaults to `16`
- `--details` (optional) Details to include in an added mark
- `--dry-run` (optional) Log changes that would be made instead of actually making them

Each supported object storage service also has an additional set of flags (see examples in [Running](##Running)).

## Input formats

For convenience, this tool supports two input formats controlled by the `--tenant` flag.

If `--tenant` is empty (the default), then a tenant must be specified for each block provided in either `--blocks` or `--blocks-file`. For example, an input file could be of the form:

```
tenant1/01GDY90HMVFPSJHXZRQH8KRAME
tenant1/01GE0SV77NX8ASC7JN0ZQMN0WM
tenant2/01GZDNKM6SQ9S7W5YQBDF0DK49
```

If `--tenant` is provided, then that tenant is assumed to be the owner of each block and only blockIDs are expected. For example, an input file could be of the form:

```
01GDY90HMVFPSJHXZRQH8KRAME
01GE0SV77NX8ASC7JN0ZQMN0WM
```

## Running

Run `go build` in this directory to build the program. Then, use an example below as a guide.

### Example for Google Cloud Storage

```bash
./mark-blocks \
  --tenant <tenant> \
  --blocks <blocks> \
  --mark-type <mark-type> \
  --backend gcs \
  --gcs.bucket-name <bucket name> \
  --dry-run
```

### Example for Azure Blob Storage

```bash
./mark-blocks \
  --tenant <tenant> \
  --blocks <blocks> \
  --mark-type <mark-type> \
  --backend azure \
  --azure.container-name <container name> \
  --azure.account-name <account name> \
  --azure.account-key <account key> \
  --dry-run
```

### Example for Amazon Simple Storage Service

```bash
./mark-blocks\
  --tenant <tenant> \
  --blocks <blocks> \
  --mark-type <mark-type> \
  --backend s3 \
  --s3.bucket-name <bucket name> \
  --s3.access-key-id <access key id> \
  --s3.secret-access-key <secret access key> \
  --s3.endpoint <endpoint> \
  --dry-run
```
