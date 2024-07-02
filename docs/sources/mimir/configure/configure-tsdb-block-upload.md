---
aliases:
  - ../operators-guide/configure/configure-tsdb-block-upload/
description: Learn how to configure Grafana Mimir to enable TSDB block upload
menuTitle: TSDB block upload
title: Configure TSDB block upload
weight: 120
---

# Configure TSDB block upload

Grafana Mimir supports uploading of historic TSDB blocks, sourced from Prometheus, Cortex, or even other
Grafana Mimir installations. Upload from Thanos is currently not supported; for more information, see [Known limitations of TSDB block upload]({{< relref "#known-limitations-of-tsdb-block-upload" >}}).

To make performing block upload simple, we've built support for it into Mimir's CLI tool, [mimirtool]({{< relref "../manage/tools/mimirtool" >}}). For more information, see [mimirtool backfill]({{< relref "../manage/tools/mimirtool#backfill" >}}).

Block upload is still considered experimental and is therefore disabled by default. You can enable it via the `-compactor.block-upload-enabled`
CLI flag, or via the corresponding `limits.compactor_block_upload_enabled` configuration parameter:

```yaml
limits:
  # Enable TSDB block upload
  compactor_block_upload_enabled: true
```

## Enable TSDB block upload per tenant

If your Grafana Mimir has multi-tenancy enabled, you can still use the preceding method to enable
TSDB block upload for all tenants. If instead you wish to enable it per tenant, you can use the
runtime configuration to set a per-tenant override:

1. Enable [runtime configuration]({{< relref "./about-runtime-configuration" >}}).
1. Add an override for the tenant that should have TSDB block upload enabled:

```yaml
overrides:
  tenant1:
    compactor_block_upload_enabled: true
```

## Known limitations of TSDB block upload

### Thanos blocks cannot be uploaded

Because Thanos blocks contain unsupported labels among their metadata, they cannot be uploaded.

For information about limitations that relate to importing blocks from Thanos as well as existing workarounds, see
[Migrating from Thanos or Prometheus to Grafana Mimir]({{< relref "../set-up/migrate/migrate-from-thanos-or-prometheus" >}}).

### No validation on imported blocks

Grafana Mimir does not validate that the uploaded blocks are well-formed. This means that users could upload malformed blocks to Grafana Mimir. These malformed blocks could potentially cause problems on the Mimir query path or for the operation of Mimir's compactor component.

We intend to add validation of uploaded blocks in a future release, which would allow us to identify and reject malformed blocks at upload time and prevent any downstream impact to Grafana Mimir.

### The results-cache needs flushing

Grafana Mimir caches query results for queries that don't require the most recent 10 minutes of data.
After uploading blocks however, queries may return different results – because new data was uploaded.
This means that cached results may be wrong.
To fix the cache results, Mimir operator can manually flush the results cache.
Possible alternative is to decrease time-to-live period for cache results from default 7 days to shorter period, for example 6 hours, by using `-query-frontend.results-cache-ttl` command line option (or per tenant).
This will guarantee that query results will use backfilled data at most after this period.

### Blocks that are too new will not be queryable until later

When queriers receive a query for a given [start, end] period, they consult this period to decide whether to read
data from storage, ingesters, or both. Say `-querier.query-store-after` is set to `12h`. It means that a query
`[now-13h, now]` will read data from storage. But a query `[now-5h, now]` will _not_. If a user uploads blocks that are
“too new”, the querier may not query them, because it is configured to only query ingesters for “fresh” time ranges.
