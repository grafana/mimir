---
title: "Configure TSDB block upload"
menuTitle: "Configure TSDB block upload"
description: "Learn how to configure Grafana Mimir to enable TSDB block upload"
weight: 120
---

# Configure TSDB block upload

Grafana Mimir supports uploading of historic TSDB blocks, sourced from f.ex. Prometheus or
Grafana Mimir itself.

For information about limitations that relate to importing blocks from Thanos, see
[Migrating from Thanos or Prometheus to Grafana Mimir]({{< relref "../../migration-guide/migrating-from-thanos-or-prometheus.md" >}}).

The functionality is disabled by default, but you can enable it via the `-compactor.block-upload-enabled`
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

1. Enable [runtime configuration]({{< relref "about-runtime-configuration.md" >}}).
1. Add an override for the tenant that should have TSDB block upload enabled:

```yaml
overrides:
  tenant1:
    compactor_block_upload_enabled: true
```

## Known limitations of TSDB block upload

### The results-cache needs flushing

After uploading one or more blocks, the results-cache needs flushing. The reason is that Grafana Mimir caches query results
for queries that don’t touch the most recent 10 minutes of data. After uploading blocks however, queries may return different
results (because new data was uploaded). Therefore cached results may be wrong, meaning the cache should manually be flushed
after uploading blocks.

### Blocks that are too new will not be queryable until later

When queriers receive a query for a given [start, end] period, they consult this period to decide whether to read
data from storage, ingesters, or both. Say `-querier.query-store-after` is set to `12h`. It means that a query
`[now-13h, now]` will read data from storage. But a query `[now-5h, now]` will _not_. If a user uploads blocks that are
“too new”, the querier may not query them, because it is configured to only query ingesters for “fresh” time ranges.

### Thanos blocks cannot be uploaded

Because Thanos blocks contain unsupported labels among their metadata, they cannot be uploaded.
