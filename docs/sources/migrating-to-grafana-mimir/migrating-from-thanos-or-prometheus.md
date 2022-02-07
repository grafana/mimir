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
With multi-tenancy disabled, there is a single tenant called `fake`.

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

## Migrate the block metadata using `thanosconvert`

Every block has a `meta.json` metadata file used by Grafana Mimir, Prometheus, and Thanos to understand the block contents.
Each project has its own metadata conventions.
The `thanosconvert` tool migrates the metadata from project to another.

### Download `thanosconvert`

- Using Docker:

```bash
docker pull grafana/thanosconvert:latest
```

- Using a release binary:

Download the appropriate release asset for your operating system and architecture and make it executable. For Linux with the AMD64 architecture:

```bash
curl -LO https://github.com/grafana/mimir/releases/latest/download/thanosconvert-linux-amd64
chmod +x mimir-linux-amd64
```

- Using Go:

```bash
go install github.com/grafana/mimir/cmd/thanosconvert@latest
```

### Run `thanosconvert`

> **Warning:** The `thanosconvert` tool modifies objects in place.
> Ensure you enable bucket versioning or have backups before running running the tool.

To run `thanosconvert`, you need to provide it with the bucket configuration.
The configuration format is the same as the [blocks storage bucket configuration]({{<relref "../configuration/config-file-reference.md#blocks_storage_config" >}}).

1. Write the bucket configuration to a file `bucket.yaml`.
1. Use one of the following steps to run `thanosconvert` in a dry-run mode that lists blocks for migration.

- Using Docker:

```bash
docker run grafana/thanosconvert -v "$(pwd)"/bucket.yaml:/bucket.yaml --config /bucket.yaml --dry-run
```

- Using a local binary:

```bash
./thanosconvert --config ./bucket.yaml --dry-run
```

- Using Go:

```bash
"$(go env GOPATH)"/bin/thanosconvert --config ./bucket.yaml --dry-run
```

3. Remove the `--dry-run` flag to apply the migration.
3. Verify the migration by re-running the tool with `--dry-run` and confirming that there is no output.
