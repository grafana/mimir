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

### Validation of blocks

Before uploading block data starts, Grafana Mimir performs the following checks on the `meta.json` file:

- Only TSDB "v1" blocks are supported. This is the format used by Prometheus v2, Grafana Mimir, and Thanos.
- Blocks with invalid MinTime or MaxTime are rejected (negative values or MaxTime < MinTime).
- Blocks where MinTime or MaxTime is in the future are rejected.
- Blocks that are outside of the retention period are rejected.
- Blocks covering a time range larger than the maximum compaction range (`-compactor.block-ranges` option, maximum defaults to 24h) are rejected.
- Blocks that cross the boundary of the maximum compaction range are rejected. For example, if the largest compaction range is 24 hours, blocks that start before midnight and finish after midnight are rejected.
- Blocks with Thanos downsampling configuration are rejected.
- Blocks that are larger than the `compactor_block_upload_max_block_size_bytes` (per-tenant override) setting are rejected.
- Blocks with "external labels" (Thanos feature) are rejected. Some Mimir-specific labels are allowed.

After you upload the block index and chunks, Grafana Mimir performs additional block validation to verify that blocks are well-formed and that they won't cause problems for Mimir's operation.
You can disable these "full block" validations through the `compactor_block_upload_validation_enabled` per-tenant override.
To disable chunks validation while keeping index-validation, use the `compactor_block_upload_verify_chunks` per-tenant override instead.

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

### The results-cache needs flushing

Grafana Mimir caches samples older than 10 minute (configurable via -query-frontend.max-cache-freshness) in the range query results.
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
