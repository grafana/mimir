# listblocks

This program displays information about blocks in object storage.

## Flags

- `--user` (required) The user (tenant) that owns the blocks to be listed
- `--format` The format of the output. Must be one of `tabbed`, `json`, or `yaml` (default `tabbed`)
- `--show-deleted` Show blocks marked for deletion (default `false`)
- `--show-labels` Show block labels (default `false`)
- `--show-ulid-time` Show time from ULID (default `false`)
- `--show-sources` Show compaction sources (default `false`)
- `--show-parents` Show parent blocks (default `false`)
- `--show-compaction-level` Show compaction level (default `false`)
- `--show-block-size` Show size of block based on details in meta.json, if available (default `false`)
- `--show-stats` Show block stats (number of series, chunks, samples) (default `false`)
- `--split-count` If not 0, shows split number that would be used for grouping blocks during split compaction (default `0`)
- `--min-time` If set, only blocks with minTime >= this value are printed
- `--max-time` If set, only blocks with maxTime <= this value are printed
- `--use-ulid-time-for-min-time-check` If true, meta.json files for blocks with ULID time before `--min-time` are not loaded. This may incorrectly skip blocks (default `false`)

Each supported object storage service also has an additional set of flags (see examples in [Running](##Running)).

## Output formats

The default `tabbed` output format is suitable for human consumption without the use of other tools. The `json` and `yaml` formats are more easily parsed and can be used in combination with other tools like `jq` and `yq` respectively.

`tabbed`

```
Block ID                     Min Time               Max Time               Duration        No Compact                                    Size
01HRB9NDFKKYM8CKGPBEY0E8QX   2024-03-06T00:00:00Z   2024-03-07T00:00:00Z   24h0m0s         [Time: 2025-04-10T19:48:56Z Reason: manual]   687 MiB
01HRDWWNZQCH1MWCWKK4VMW08R   2024-03-07T00:00:00Z   2024-03-08T00:00:00Z   24h0m0s                                                       688 MiB
```

`json` (Note: pretty-printed here for readability)

```json
[
  {
    "blockID": "01HRB9NDFKKYM8CKGPBEY0E8QX",
    "duration": "24h0m0s",
    "durationSeconds": 86400,
    "maxTime": "2024-03-07T00:00:00Z",
    "minTime": "2024-03-06T00:00:00Z",
    "noCompact": {
      "time": "2025-04-10T19:48:56Z",
      "reason": "manual"
    },
    "size": "687 MiB",
    "sizeBytes": 720756845
  },
  {
    "blockID": "01HRDWWNZQCH1MWCWKK4VMW08R",
    "duration": "24h0m0s",
    "durationSeconds": 86400,
    "maxTime": "2024-03-08T00:00:00Z",
    "minTime": "2024-03-07T00:00:00Z",
    "size": "688 MiB",
    "sizeBytes": 721221809
  }
]
```

`yaml`

```yaml
- blockID: 01HRB9NDFKKYM8CKGPBEY0E8QX
  duration: 24h0m0s
  durationSeconds: 86400
  maxTime: "2024-03-07T00:00:00Z"
  minTime: "2024-03-06T00:00:00Z"
  noCompact:
    time: "2025-04-10T19:48:56Z"
    reason: manual
  size: 687 MiB
  sizeBytes: 720756845
- blockID: 01HRDWWNZQCH1MWCWKK4VMW08R
  duration: 24h0m0s
  durationSeconds: 86400
  maxTime: "2024-03-08T00:00:00Z"
  minTime: "2024-03-07T00:00:00Z"
  size: 688 MiB
  sizeBytes: 721221809
```

## Running

Run `go build` in this directory to build the program. Then, use an example below as a guide.

### Example for Google Cloud Storage

```bash
./listblocks \
  --user <user> \
  --backend gcs \
  --gcs.bucket-name <bucket name> \
```

### Example for Azure Blob Storage

```bash
./listblocks \
  --user <user> \
  --backend azure \
  --azure.container-name <container name> \
  --azure.account-name <account name> \
  --azure.account-key <account key> \
```

### Example for Amazon Simple Storage Service

```bash
./listblocks \
  --user <user> \
  --backend s3 \
  --s3.bucket-name <bucket name> \
  --s3.access-key-id <access key id> \
  --s3.secret-access-key <secret access key> \
  --s3.endpoint <endpoint> \
```
