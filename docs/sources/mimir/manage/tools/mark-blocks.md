---
description: Use Mark-blocks to manage the state of time series data blocks in object storage.
menuTitle: Mark-blocks
title: Mark-blocks
weight: 50
aliases:
  - ./markblocks
---

# Mark-blocks

You can use the mark-blocks tool in Grafana Mimir to manage the state of time series data blocks in object storage.

Use mark-blocks for the following purposes:

- Mark blocks for non-compaction. This is useful for corrupted blocks, preventing them from being processed by the compactor and potentially causing issues. Newer Mimir versions might automatically mark corrupted blocks.
- Mark blocks for deletion. Use the tool to explicitly mark specific blocks for deletion from the object storage.

## Download mark-blocks

Download mark-blocks as part of the Mimir release. To download the binary, refer to [Grafana Mimir releases](https://github.com/grafana/mimir/releases) on GitHub.

## Use mark-blocks

To use mark-blocks, you need to specify the tenant ID, bucket configuration, and block IDs you'd like to mark. For example:

```bash
# Mark a block for non-compaction (example for a corrupted block)
mark-blocks --tenant=<your-tenant-id> --backend=s3 --s3.bucket-name=<your-bucket> --s3.endpoint=<your-s3-endpoint> --mark-type=no-compact --blocks=<block-id>

# Mark a block for deletion
mark-blocks --tenant=<your-tenant-id> --backend=s3 --s3.bucket-name=<your-bucket> --s3.endpoint=<your-s3-endpoint> --mark-type=deletion --blocks=<block-id>
```

{{< admonition type="note" >}}
Use caution when using markblocks, as it affects data management within your Mimir storage.{{< /admonition >}}
