---
title: "Migrating from Thanos or Prometheus to Grafana Mimir"
menuTitle: "Migrating from Thanos or Prometheus"
description: "Learn how to migrate from Thanos or Prometheus to Grafana Mimir."
weight: 10
---

# Migrating from Thanos or Prometheus to Grafana Mimir

This document guides an operator through the process of migrating a deployment of Thanos or Prometheus to Grafana Mimir.

## Overview

Grafana Mimir stores series in TSDB blocks uploaded in an object storage bucket.
These blocks are the same as those used by Prometheus and Thanos.
Each project stores blocks in different places and uses slightly different block metadata files.

## Configuring remote write to Grafana Mimir

For configuration of remote write to Grafana Mimir, refer to [Configuring Prometheus remote write]({{< relref "../operators-guide/secure/authentication-and-authorization.md#configuring-prometheus-remote-write" >}}).

## Uploading historic TSDB blocks to Grafana Mimir

Grafana Mimir supports uploading of historic TSDB blocks, notably from Prometheus.
In order to enable this functionality, either for all tenants or a specific one, refer to
[Configuring TSDB block upload]({{< relref "../operators-guide/configure/configure-tsdb-block-upload.md" >}}).

Prometheus stores TSDB blocks in the path specified in the `--storage.tsdb.path` flag.

To find all block directories in the TSDB `<STORAGE TSDB PATH>`, run the following command:

```bash
find <STORAGE TSDB PATH> -name chunks -exec dirname {} \;
```

Grafana Mimir supports multiple tenants and stores blocks per tenant. With multi-tenancy disabled, there
is a single tenant called `anonymous`.

Use Grafana mimirtool to upload each block, such as those identified by the previous command, to Grafana Mimir:

```bash
mimirtool backfill --address=http://<mimir-hostname> --id=<tenant> <block1> <block2>...
```

> **Note**: If you need to authenticate against Grafana Mimir, you can provide an API key via the `--key` flag,
> for example `--key=$(cat token.txt)`.

Grafana Mimir performs some sanitization and validation of each block's metadata.
As a result, it rejects Thanos blocks due to unsupported labels.
As a workaround, if you need to upload Thanos blocks, upload the blocks directly to the
Grafana Mimir blocks bucket, prefixed by `<tenant>/<block ID>/`.

## Block metadata

Each block has a `meta.json` metadata file that is used by Grafana Mimir, Prometheus, and Thanos to identify the block contents.
Each project has its own metadata conventions.

In the Grafana Mimir 2.1 (or earlier) release, the ingesters added an external label to the `meta.json` file to identify the tenant that owns the block.

In the Grafana Mimir 2.2 (or later) release, blocks no longer have a label that identifies the tenant.

> **Note**: Blocks from Prometheus do not have any external labels stored in them; only blocks from Thanos use labels.

## Considerations on Thanos specific features

Thanos requires that Prometheus is configured with external labels.
When the Thanos sidecar uploads blocks, it includes the external labels from Prometheus in the `meta.json` file inside the block.
When you query the block, Thanos injects Prometheus’ external labels in the series returned in the query result.
Thanos also uses labels for the deduplication of replicated data.

If you want to use existing blocks from Thanos by Grafana Mimir, there are some considerations:

**Grafana Mimir doesn't inject external labels into query results.**
This means that blocks that were originally created by Thanos will not include their external labels in the results when queried by Grafana Mimir.
If you need to have external labels in your query results, this is currently not possible to achieve in Grafana Mimir.

**Grafana Mimir will not respect deduplication labels configured in Thanos when querying the blocks.**
For best query performance, only upload Thanos blocks from a single Prometheus replica from each HA pair.
If you upload blocks from both replicas, the query results returned by Mimir will include samples from both replicas.

> **Note**: Thanos provides the `thanos tools bucket rewrite` tool for manipulating blocks in the bucket.
> It may be possible to use this tool to embed external labels into blocks.
> Please refer to [`thanos tools bucket rewrite` documentation](https://thanos.io/tip/components/tools.md/#bucket-rewrite) for more details.

**Grafana Mimir does not support Thanos’ _downsampling_ feature.**
To guarantee query results correctness please only upload original (raw) Thanos blocks into Mimir's storage.
If you also upload blocks with downsampled data (ie. blocks with non-zero `Resolution` field in `meta.json` file), Grafana Mimir will merge raw samples and downsampled samples together at the query time.
This may cause that incorrect results are returned for the query.

> **Note**: It is possible to run compaction and deduplicate blocks by Thanos first by using `thanos compact` command
> with `--compact.enable-vertical-compaction --deduplication.func=penalty --deduplication.replica-label=<LABEL>` flags.
> Please refer to [Vertical Compaction Use Cases](https://thanos.io/tip/components/compact.md/#vertical-compaction-use-cases) for more details on offline deduplication.
