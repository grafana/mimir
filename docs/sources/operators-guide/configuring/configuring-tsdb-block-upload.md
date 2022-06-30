---
title: "Configuring TSDB block upload"
menuTitle: "Configuring TSDB block upload"
description: "Learn how to configure Grafana Mimir to enable TSDB block upload"
weight: 120
---

# Configuring TSDB block upload

Grafana Mimir supports uploading of historic TSDB blocks, sourced from f.ex. Prometheus, Thanos, or
Grafana Mimir itself.

The functionality is disabled by default, but can be enabled via the `-compactor.block-upload-enabled`
CLI flag, or via the corresponding `limits.compactor_block_upload_enabled` configuration parameter:

```yaml
limits:
  # Enable TSDB block upload
  compactor_block_upload_enabled: true
```

## Enabling TSDB block upload per tenant

If your Grafana Mimir has multitenancy enabled, you can still use the preceding method to enable
TSDB block upload for all tenants. If instead you wish to enable it per tenant, you can use the
runtime configuration to set a per-tenant override:

1. Enable [runtime configuration]({{< relref "about-runtime-configuration.md" >}}).
1. Add an override for the tenant that should have TSDB block upload enabled:

```yaml
overrides:
  tenant1:
    compactor_block_upload_enabled: true
```
