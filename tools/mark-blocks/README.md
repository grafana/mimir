# Mark Blocks

This program creates or removes markers for blocks.

## Flags

- `--tenant` (required) The tenant that owns the blocks
- `--mark-type` (required) Mark type to create or remove, valid options: `deletion`, `no-compact`
- `--blocks` (optional) Comma separated list of blocks IDs. If non-empty, `--blocks-file` is ignored
- `--blocks-file` (optional) File containing a block ID per-line. Defaults to standard input (`-`). Ignored if `--blocks` is non-empty
- `--meta-presence-policy` (optional) Policy on presence of block `meta.json` files: `none`, `skip-block`, or `require`. Defaults to `skip-block`
- `--remove` (optional) If marks should be removed rather than uploaded. Defaults to `false`
- `--resume-index` (optional) The index of the block to resume from. This index is logged to assist in recovering from partial failures
- `--concurrency` (optional) How many markers to upload or remove concurrently. Defaults to `16`
- `--details` (optional) Details to include in an added mark
- `--dry-run` (optional) Log changes that would be made instead of actually making them

Each supported object storage service also has an additional set of flags (see examples in [Running](##Running)).

## Running

Running `go build .` in this directory builds the program. Then use an example below as a guide.

### Example for Google Cloud Storage

```bash
./mark-blocks \
  --tenant <tenant>
  --blocks <blocks>
  --mark-type <mark-type>
  --backend gcs \
  --gcs.bucket-name <bucket name> \
  --dry-run
```

### Example for Azure Blob Storage

```bash
./mark-blocks \
  --tenant <tenant>
  --blocks <blocks>
  --mark-type <mark-type>
  --backend azure \
  --azure.container-name <container name> \
  --azure.account-name <account name> \
  --azure.account-key <account key> \
  --dry-run
```

### Example for Amazon Simple Storage Service

```bash
./mark-blocks\
  --tenant <tenant>
  --blocks <blocks>
  --mark-type <mark-type>
  --backend s3 \
  --s3.bucket-name <bucket name> \
  --s3.access-key-id <access key id> \
  --s3.secret-access-key <secret access key> \
  --s3.endpoint <endpoint> \
  --dry-run
```
