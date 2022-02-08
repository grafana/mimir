---
title: "Migrating to Grafana Mimir from Thanos or Prometheus"
description: "Configuring remote-write and migrating historic TSDB blocks from Prometheus or Thanos."
weight: 10
---

# Migrating to Grafana Mimir from Thanos or Prometheus

## Overview

Grafana Mimir stores series in TSDB blocks uploaded in an object storage bucket.
These blocks are the same as those used by Prometheus and Thanos.
Each project stores blocks in different places use different block metadata files.

## Configure remote-write to Grafana Mimir

TODO

## Upload historic blocks to the Grafana Mimir storage bucket

Prometheus stores TSDB blocks in the path specified in the `--storage.tsdb.path` flag.

Find all blocks directories in the TSDB `<STORAGE TSDB PATH>`:

```bash
find <STORAGE TSDB PATH> -name chunks -exec dirname {} \;
```

Grafana Mimir supports multiple tenants and stores blocks with a tenant prefix.
With multi-tenancy disabled, there is a single tenant called `anonymous`.

Copy each directory output by the previous command to the Mimir object storage bucket with
your tenant prefix.

- For AWS S3 using the `aws` tool:

```bash
aws s3 cp <DIRECTORY> s3://<TENANT>/<DIRECTORY>
```

- For Google Cloud Storage (GCS), using the the `gsutil` tool:

```bash
gsutil -m cp -r <DIRECTORY> gs://<TENANT>/<DIRECTORY>
```

## Migrate the block metadata using `metaconvert`

Every block has a `meta.json` metadata file used by Grafana Mimir, Prometheus, and Thanos to understand the block contents.
Each project has its own metadata conventions.
The `metaconvert` tool migrates the metadata from project to another.

### Download `metaconvert`

- Using Docker:

```bash
docker pull grafana/metaconvert:latest
```

- Using a release binary:

Download the appropriate release asset for your operating system and architecture and make it executable. For Linux with the AMD64 architecture:

```bash
curl -LO https://github.com/grafana/mimir/releases/latest/download/metaconvert-linux-amd64
chmod +x metaconvert-linux-amd64
```

- Using Go:

```bash
go install github.com/grafana/mimir/cmd/metaconvert@latest
```

### Run `metaconvert`

> **Warning:** The `metaconvert` tool modifies objects in place.
> Ensure you enable bucket versioning or have backups before running running the tool.

To run `metaconvert`, you need to provide it with the bucket configuration. Use `metaconvert -h` to get list of available parameters.

1. Use one of the following steps to run `metaconvert` in a dry-run mode that lists blocks for migration.

- Using Docker:

```bash
docker run grafana/metaconvert -backend=gcs -gcs.bucket-name=bucket -tenant=anonymous -dry-run
```

- Using a local binary:

```bash
./metaconvert -backend=filesystem -filesystem.dir=/bucket -tenant=anonymous -dry-run
```

3. Remove the `--dry-run` flag to apply the migration.
4. Verify the migration by re-running the tool with `--dry-run` and confirming that there is no output.
