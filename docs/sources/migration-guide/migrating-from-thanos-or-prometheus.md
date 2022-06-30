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

For configuration of remote write to Grafana Mimir, refer to [Configuring Prometheus remote write]({{< relref "../operators-guide/securing/authentication-and-authorization.md#configuring-prometheus-remote-write" >}}).

## Uploading historic TSDB blocks to Grafana Mimir

Grafana Mimir supports uploading of historic TSDB blocks, also from other systems such as Prometheus and Thanos.
In order to enable this functionality, either for all tenants or a specific one, refer to
[Configuring TSDB block upload]({{< relref "../operators-guide/configuring/configuring-tsdb-block-upload.md" >}}).

Prometheus stores TSDB blocks in the path specified in the `--storage.tsdb.path` flag.

To find all block directories in the TSDB `<STORAGE TSDB PATH>`, run the following command:

```bash
find <STORAGE TSDB PATH> -name chunks -exec dirname {} \;
```

Grafana Mimir supports multiple tenants and stores blocks per tenant. With multi-tenancy disabled, there
is a single tenant called `anonymous`.

Use Grafana mimirtool to upload each block, e.g. identified by the previous command, to Grafana Mimir:

```bash
mimirtool backfill --user=<tenant> --key=$(cat token.txt) --address=http://<mimir-hostname> --id=<tenant> <block1> <block2>...
```

Grafana Mimir will perform the necessary conversions of each block, if they are from another system
(e.g. Prometheus or Thanos). Additionally, blocks are validated so only healthy blocks are imported.
