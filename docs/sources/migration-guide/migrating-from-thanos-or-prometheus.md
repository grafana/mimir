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
If you enable the sharding feature of Mimir's split-and-merge compactor, compactor adds a special label to identify the compactor-shard.

In the Grafana Mimir 2.2 (or later) release, blocks no longer have a label that identifies the tenant. The compactor-shard label is still used, if enabled.

When querying blocks, Mimir ignores all external labels and doesn't add them to the results.
During compaction, Mimir respects the external labels in the block and only merges blocks with the same labels.

> **Note**: Blocks from Prometheus do not have any external labels stored in them; only blocks from Thanos use labels.

> **Note**: Thanos requires that Prometheus is configured with external labels.
When the Thanos sidecar uploads blocks, it includes the external labels from Prometheus in the `meta.json` file inside the block.
When you query the block, Thanos returns Prometheus’ external labels. Thanos also uses labels for the deduplication of replicated data.

Blocks that were originally created by Thanos do not include their external labels in the query results, because Mimir doesn't include external labels in the query results.

This means that blocks that were originally created by Thanos will not include their external labels in the results when queried.

You can configure Thanos to use labels for deduplication. After uploading such blocks into Mimir, the query results will contain duplicate samples (assuming their timestamps are not identical).
As long as the blocks have different set of external labels, Mimir's compactor does not compact such blocks together.

> **Note:** Mimir does not support Thanos’ _downsampling_ feature. As a result, Mimir ignores the `Resolution` field from the `meta.json` file that Thanos creates.
A notable exception is that the Mimir compactor does not compact together blocks of different resolutions.
The blocks that Mimir generates have unset resolution fields.
