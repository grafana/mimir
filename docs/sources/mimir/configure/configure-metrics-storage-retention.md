---
aliases:
  - ../operators-guide/configure/configure-metrics-storage-retention/
description: Learn how to configure Grafana Mimir metrics storage retention.
menuTitle: Metrics storage retention
title: Configure Grafana Mimir metrics storage retention
weight: 80
---

# Configure Grafana Mimir metrics storage retention

Grafana Mimir stores metrics in object storage.

By default, metrics that are stored in the object storage are never deleted, and the storage utilization will increase over time.
You can configure the object storage retention to automatically delete all metrics data older than the configured period.

## Configure the storage retention

The [compactor](/docs/mimir/<MIMIR_VERSION>/references/architecture/components/compactor/) is the Mimir component that is responsible for enforcing the storage retention.
To configure the storage retention, set the CLI flag `-compactor.blocks-retention-period` or change the following YAML configuration:

```yaml
limits:
  # Delete from storage metrics data older than 1 year.
  compactor_blocks_retention_period: 1y
```

To configure the storage retention on a per-tenant basis, set overrides in the [runtime configuration](../about-runtime-configuration/):

```yaml
overrides:
  tenant1:
    # Delete from storage tenant1's metrics data older than 1 year.
    compactor_blocks_retention_period: 1y
  tenant2:
    # Delete from storage tenant2's metrics data older than 2 years.
    compactor_blocks_retention_period: 2y
  tenant3:
    # Disable retention for tenant3's metrics (never delete its data).
    compactor_blocks_retention_period: 0
```

## Per-series retention

Grafana Mimir doesn’t support per-series deletion and retention, nor does it support Prometheus' [Delete series API](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series).
