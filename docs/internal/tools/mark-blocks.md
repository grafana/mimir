# Mark blocks tool

`mark-blocks` is a tool that assists in uploading or removing block markers to a specified backend.
Marks are uploaded to both the block folder and the global marks folder for the provided tenant.

Two types of marks can be created, depending on the `-mark-type` flag provided:

## `deletion` mark

When `-mark-type deletion` is provided, `mark-blocks` uploads a `DeletionMark` which tells the compactor that the block provided should be deleted.
This is a **destructive operation** (although it won't happen immediately, the default delation delay is 12h, see `-compactor.deletion-delay` value), proceed with caution.

### Example

```
$ mkdir -p tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z

$ touch tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
└── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z

3 directories, 1 file

$ go run ./tools/mark-blocks -mark-type deletion -tenant tenant-1 -details "Corrupted blocks" -blocks "01FSCTA0A4M1YQHZQ4B2VTGS2R,01FSCTA0A4M1YQHZQ4B2VTGS7Z"
level=info time=2025-02-10T14:23:17.594517Z msg="skipping block because its meta.json was not found: 01FSCTA0A4M1YQHZQ4B2VTGS7Z"
level=info time=2025-02-10T14:23:17.594747Z msg="uploaded mark to tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/deletion-mark.json"
level=info time=2025-02-10T14:23:17.594945Z msg="uploaded mark to tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-deletion-mark.json"

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   ├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/deletion-mark.json
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z
└── tenant-1/markers
    └── tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-deletion-mark.json

4 directories, 3 files
```

## `no-compact` mark

When `-mark-type no-compact` is provided, this tool uploads a `NoCompactMark` which tells the compactor that the marked blocks should not be compacted.
`-details` flag can be used to provide an explanation of why the block is marked as such (who marked it? issue link?).

### Example

```
$ mkdir -p tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z

$ touch tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
└── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z

3 directories, 1 file

$ go run ./tools/mark-blocks -mark-type no-compact -tenant tenant-1 -details "Blocks with out of order chunks" -blocks "01FSCTA0A4M1YQHZQ4B2VTGS2R,01FSCTA0A4M1YQHZQ4B2VTGS7Z"
level=info time=2025-02-10T14:27:46.232169Z msg="skipping block because its meta.json was not found: 01FSCTA0A4M1YQHZQ4B2VTGS7Z"
level=info time=2025-02-10T14:27:46.232419Z msg="uploaded mark to tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/no-compact-mark.json"
level=info time=2025-02-10T14:27:46.232621Z msg="uploaded mark to tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-no-compact-mark.json"

$ tree -f tenant-1
tenant-1
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R
│   ├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/meta.json
│   └── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS2R/no-compact-mark.json
├── tenant-1/01FSCTA0A4M1YQHZQ4B2VTGS7Z
└── tenant-1/markers
    └── tenant-1/markers/01FSCTA0A4M1YQHZQ4B2VTGS2R-no-compact-mark.json

4 directories, 3 files
```
