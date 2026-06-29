# Split Blocks

This program splits source blocks into new blocks where each spans at most a duration of time. For instance, it can create three 24 hour blocks from a single 72 hour block.

Time boundaries are also considered when determining what to split. For instance, a block spanning `00:00`-`23:59` could not be split while a block spanning `12:00`-`11:59` (the next day) would be.

Source blocks can be read from either object storage or a local filesystem. Blocks that are created are only written to a local filesystem.

## Flags

- `--output.dir` (required) The output directory where split blocks will be written on the local filesystem
- `--blocks` (optional) A comma separated list of blocks to target. If not provided, or empty, all blocks are considered
- `--block-concurrency` (optional, defaults to `5`) How many blocks can be split concurrently
- `--bucket-prefix` (optional) A prefix applied to the bucket path
- `--max-block-duration` (optional, defaults to `24h`) Max block duration, blocks larger than this or crossing a duration boundary are split
- `--full` (optional) If set, blocks that do not need to be split are included in the output directory
- `--verify-blocks` (optional) Verifies blocks after splitting them
- `--dry-run` (optional) If set, blocks are not downloaded (except metadata) and splits are not performed; only what would happen is logged

## Running

Running `go build .` in this directory builds the program. Then use an example below as a guide.

### Splitting blocks from a local filesystem

```bash
./splitblocks \
  --backend filesystem \
  --filesystem.dir <directory> \
  --output.dir <directory> \
  --dry-run
```

### Splitting blocks from Google Cloud Storage

```bash
./splitblocks \
  --backend gcs \
  --gcs.bucket-name <bucket name> \
  --output.dir <directory> \
  --dry-run
```

### Splitting blocks from Azure Blob Storage

```bash
./splitblocks \
  --backend azure \
  --azure.container-name <container name> \
  --azure.account-name <account name> \
  --azure.account-key <account key> \
  --output.dir <directory> \
  --dry-run
```

### Splitting blocks from Amazon Simple Storage Service

```bash
./splitblocks \
  --backend s3 \
  --s3.bucket-name <bucket name> \
  --s3.access-key-id <access key id> \
  --s3.secret-access-key <secret access key> \
  --s3.endpoint <endpoint> \
  --output.dir <directory> \
  --dry-run
```
