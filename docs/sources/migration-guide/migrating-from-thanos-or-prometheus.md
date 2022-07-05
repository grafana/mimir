---
title: "Migrating from Thanos or Prometheus to Grafana Mimir"
menuTitle: "Migrating from Thanos or Prometheus to Grafana Mimir"
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

For configuration of remote write to Grafana Mimir, refer to [Configuring Prometheus remote write]({{< relref "../operators-guide/securing/authentication-and-authorization.md#configuring-prometheus-remote-write" >}}).

## Uploading historic blocks to the Grafana Mimir storage bucket

Prometheus stores TSDB blocks in the path specified in the `--storage.tsdb.path` flag.

To find all blocks directories in the TSDB `<STORAGE TSDB PATH>`, run the following command:

```bash
find <STORAGE TSDB PATH> -name chunks -exec dirname {} \;
```

Grafana Mimir supports multiple tenants and stores blocks with a tenant prefix.
With multi-tenancy disabled, there is a single tenant called `anonymous`.

Copy each directory output from the previous command to the Grafana Mimir object storage bucket with
your tenant prefix.

- For AWS S3, use the `aws` tool in the following command:

```bash
aws s3 cp <DIRECTORY> s3://<TENANT>/<DIRECTORY>
```

- For Google Cloud Storage (GCS), use the `gsutil` tool in the following command:

```bash
gsutil -m cp -r <DIRECTORY> gs://<TENANT>/<DIRECTORY>
```

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
For the best query performance please only upload Thanos blocks from single Prometheus replica from each HA pair.
If you upload blocks from both replicas, the query results returned by Mimir will include samples from both replicas.

> **Note**: Thanos provides `thanos tools bucket rewrite` tool for manipulating blocks in the bucket.
> It may be possible to use this tool to embed external labels into blocks.
> Please refer to [`thanos tools bucket rewrite` documentation](https://thanos.io/tip/components/tools.md/#bucket-rewrite) for more details.

**Grafana Mimir does not support Thanos’ _downsampling_ feature.**
To guarantee query results correctness please only upload original (raw) Thanos blocks into Mimir's storage.
If you also upload blocks with downsampled data (ie. blocks with non-zero `Resolution` field in `meta.json` file), Grafana Mimir will merge raw samples and downsampled samples together at the query time.
This may cause that incorrect results are returned for the query.

> **Note**: It is possible to run compaction and deduplicate blocks by Thanos first by using `thanos compact` command
> with `--compact.enable-vertical-compaction --deduplication.func=penalty --deduplication.replica-label=<LABEL>` flags.
> Please refer to [Vertical Compaction Use Cases](https://thanos.io/tip/components/compact.md/#vertical-compaction-use-cases) for more details on offline deduplication.
