---
description: Use Markblocks to manage the state of time series data blocks in object storage.
menuTitle: Markblocks
title: Grafana Markblocks
weight: 50
---

# Grafana Markblocks

You can use the markblocks tool in Grafana Mimir to manage the state of time series data blocks in object storage.

Use markblocks for the following purposes:

- Mark blocks for non-compaction. This is useful for corrupted blocks, preventing them from being processed by the compactor and potentially causing issues. Newer Mimir versions might automatically mark corrupted blocks.
- Mark blocks for deletion. Use the tool to explicitly mark specific blocks for deletion from the object storage.

The markblocks tool helps to maintain data integrity and to manage storage effectively, especially in scenarios involving data corruption or manual cleanup.

## Download markblocks

Download markblocks as part of the Mimir binary. To download the binary, refer to [Grafana Mimir releases](https://github.com/grafana/mimir/releases) on GitHub.

## Use markblocks

To use markblocks, you need to specify the tenant ID, bucket configuration, and block IDs you'd like to mark. For example:

```bash
# Mark a block for non-compaction (example for a corrupted block)
mimir markblocks --tenant-id=<your-tenant-id> --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=<your-bucket> --blocks-storage.s3.endpoint=<your-s3-endpoint> --mark-no-compact=<block-id>

# Mark a block for deletion
mimir markblocks --tenant-id=<your-tenant-id> --blocks-storage.backend=s3 --blocks-storage.s3.bucket-name=<your-bucket> --blocks-storage.s3.endpoint=<your-s3-endpoint> --mark-for-deletion=<block-id>
```

{{< admonition type="note" >}}
Use caution when using markblocks, as it affects data management within your Mimir storage.{{< /admonition >}}
