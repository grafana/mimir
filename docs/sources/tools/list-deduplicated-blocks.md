---
title: "List deduplicated blocks"
description: ""
weight: 100
---

# List deduplicated blocks

`list-deduplicated-blocks` downloads all `meta.json` files from tenants' debug/metas directory, removes duplicates
(using `ShardAwareDeduplicateFilter`), and prints information about remaining blocks.

This should output blocks that are supposed to be in the bucket (although old blocks may have been deleted due to
retention -- Mimir doesn't cleanup meta.json file from debug/metas in such case).

To make this process faster, it is recommended to sync debug/metas to local disk before running this program:

Eg.

```
cd /tmp/test-bucket
mkdir -p tenant
cd tenant
gsutil -m cp -r -n gs://bucket/tenant/debug .
```

And then run:

```
list-deduplicated-blocks -backend=filesystem -filesystem.dir=/tmp/test-bucket -user=tenant
```
