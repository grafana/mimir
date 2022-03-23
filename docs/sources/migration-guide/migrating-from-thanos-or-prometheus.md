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
Each project stores blocks in different places and uses different block metadata files.

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

## Migrating the block `meta.json` metadata using `metaconvert`

Every block has a `meta.json` metadata file used by Grafana Mimir, Prometheus, and Thanos to identify the block contents.
Each project has its own metadata conventions.
The `metaconvert` tool migrates the `meta.json` metadata from one project to another.

### Downloading `metaconvert`

- If you are using Docker, run the following command:

```bash
docker pull grafana/metaconvert:latest
```

- If you are using a release binary, download the appropriate release asset for your operating system and architecture and make it executable. For Linux with the AMD64 architecture, run the following command:

```bash
curl -LO https://github.com/grafana/mimir/releases/latest/download/metaconvert-linux-amd64
chmod +x metaconvert-linux-amd64
```

- If you are using Go, run the following command:

```bash
go install github.com/grafana/mimir/cmd/metaconvert@latest
```

### Running `metaconvert`

> **Warning:** The `metaconvert` tool modifies objects in place.
> Ensure you enable bucket versioning or have backups before running the tool.

To run `metaconvert`, provide it with the bucket configuration. Use `metaconvert -h` to list available parameters.

1. Complete one of the following steps to run `metaconvert` in a dry-run mode, which lists blocks for migration.

   - If you are using Docker, run the following command:

   ```bash
   docker run grafana/metaconvert -backend=gcs -gcs.bucket-name=bucket -tenant=anonymous -dry-run
   ```

   - If you are using a local binary, run the following command:

   ```bash
   ./metaconvert -backend=filesystem -filesystem.dir=/bucket -tenant=anonymous -dry-run
   ```

1. Remove the `-dry-run` flag to apply the migration.
1. Verify the migration by re-running the tool with `-dry-run` and confirming that no changes are required.
