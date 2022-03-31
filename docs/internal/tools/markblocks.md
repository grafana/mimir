# Mark blocks tool

`markblocks` is a tool that creates and uploads block markers to a specified backend.
Marks are uploaded to both block folder and the global marks folder for the provided tenant.

See `markblocks -help` for flags usage, and `markblocks -help-all` for full backend configuration flags list.

This tool can create two types of marks, depending on the `-mark` flag provided:

## `deletion` mark

When `-mark deletion` is provided, this tool uploads a `DeletionMark` which tells the compactor that the block provided should be deleted.
This is a **destructive operation** (although it won't happen immediately, the default delation delay is 12h, see `-compactor.deletion-delay` value), proceed with caution.

### Example

```
$ mkdir -p tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U

$ touch tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
└── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U

2 directories, 1 file

$ go run ./tools/markblocks -mark deletion -tenant tenant-1 -details "Corrupted blocks" 01FSCTA0A4M1YQHZQ4B2VTGS2R 01FSCTA0A4M1YQHZQ4B2VTGS2U
level=info time=2022-03-30T08:50:41.277334365Z msg="Successfully uploaded mark." block=01FSCTA0A4M1YQHZQ4B2VTGS2R
level=info time=2022-03-30T08:50:41.277359767Z msg="Block does not exist, skipping." block=01FSCTA0A4M1YQHZQ4B2VTGS7Z

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   ├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/deletion-mark.json
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U
└── tenant-1/markers
    └── tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-deletion-mark.json

3 directories, 3 files
```

## `no-compact` mark

When `-mark no-compact` is provided, this tool uploads a `NoCompactMark` which tells the compactor that the marked blocks should not be compacted.
`-details` flag can be used to provide an explanation of why the block is marked as such (who marked it? issue link?).

### Example

```
$ mkdir -p tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U

$ touch tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
└── ten ant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U

2 directories, 1 file

$ go run ./tools/markblocks -mark no-compact -tenant tenant-1 -details "Blocks with out of order chunks" 01FSCTA0A4M1YQHZQ4B2VTGS2R 01FSCTA0A4M1YQHZQ4B2VTGS2U
level=info time=2022-03-30T08:53:13.012462019Z msg="Successfully uploaded mark." block=01FSCTA0A4M1YQHZQ4B2VTGS2R
level=info time=2022-03-30T08:53:13.012492902Z msg="Block does not exist, skipping." block=01FSCTA0A4M1YQHZQ4B2VTGS7Z

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   ├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/no-compact-mark.json
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2U
└── tenant-1/markers
    └── tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-no-compact-mark.json

3 directories, 3 files
```
