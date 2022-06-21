---
title: "Migrating from Consul to Memberlist KV store for hash rings"
menuTitle: "Migrating from Consul to Memberlist"
description: "Learn how to migrate from using Consul as KV store for hash rings to using memberlist without any downtime."
weight: 30
---

# Migrating from Consul to Memberlist as KV store for hash rings without downtime

Mimir Jsonnet uses `memberlist` as KV store for hash rings, unless disabled by using

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: false
  }
}
```

If you are still using Consul (ie. you have `memberlist_ring_enabled: false` in your Jsonnet, this was default before Grafana Mimir 2.2),
and would like to migrate to `memberlist` without any downtime, you can follow instructions in this document.

## Step 1: Enable memberlist and multi KV store.

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
    multikv_migration_enabled: true,
  }
}
```

Step 1 configures components to use `multi` KV store, with `consul` as primary and `memberlist` as secondary stores.
This step requires rollout of all components.
After applying this step all Mimir components will expose `/memberlist` page on HTTP admin interface, which can be used to check health of Memberlist cluster.

## Step 2: Enable mirroring

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
    multikv_migration_enabled: true,
    multikv_mirror_enabled: true,  // Changed in this step.
  }
}
```

In this step we enable writes to primary KV store (Consul) to be mirrored into secondary store (Memberlist).
Applying this change will not cause restart of components.

You can monitor following metrics to check if mirroring was enabled on all components and if it works correctly:

- `cortex_multikv_mirror_enabled`
- `rate(cortex_multikv_mirror_writes_total[1m])`
- `rate(cortex_multikv_mirror_write_errors_total[1m])`

After mirroring is enabled Memberlist admin page should display following keys in the KV store for all components using hash ring:

- `compactor` (compactors ring)
- `distributor` (distributors ring)
- `multi-zone/store-gateway` (or just `store-gateway` if multi-zone store gateways are not enabled).
- `ring` (ingesters ring)
- `rulers/ring` (if ruler is running)
- `alertmanager` (if alertmanager is running)

## Step 3: Switch Primary and Secondary store

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
    multikv_migration_enabled: true,
    multikv_mirror_enabled: true,
    multikv_switch_primary_secondary: true,  // Changed in this step.
  }
}
```

This change will switch primary and secondary stores as used by `multi` KV.
From this point on components will use memberlist as primary KV store, and they will mirror updates to Consul.
This step does NOT require restart of components.

To see if all components started to use `memberlist` as primary store, please watch `cortex_multikv_primary_store` metric.

## Step 4: Disable mirroring to Consul

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
    multikv_migration_enabled: true,
    multikv_mirror_enabled: false,  // Changed in this step.
    multikv_switch_primary_secondary: true,
  }
}
```

This step doesn't require restart. After applying the change components will stop writing ring updates to Consul, and will only use `memberlist`.
You can watch `cortex_multikv_mirror_enabled` metric to see if all components have picked up updated configuration.

## Step 5: Disable `multi` KV Store

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
    multikv_migration_enabled: false,  // Changed in this step.
    multikv_mirror_enabled: false,
    multikv_switch_primary_secondary: true,
    multikv_migration_teardown: true,  // Added in this step.
  }
}
```

This configuration change will cause a new rollout of all components.
After restart components will no longer use `multi` KV store and will be configured to use `memberlist` only.
We use `multikv_migration_teardown` to preserve runtime configuration for `multi` KV store for components that haven't restarted yet.

All `cortex_multikv_*` metrics are only exposed by components that use `multi` KV store. As components restart, these metrics will disappear.

**Note: setting `multikv_migration_enabled: false` while keeping `memberlist_ring_enabled: true` will also remove Consul! That's expected, since Consul is not used anymore – mirroring to it was disabled in step 4.**

If you need to keep consul running, you can explicitly set `consul_enabled: true` in `_config`.

## Step 6: Cleanup

We have successfully migrated Mimir cluster from using Consul to Memberlist without any downtime!
As a final step, we can remove all migration-related config options:

- `multikv_migration_enabled`
- `multikv_mirror_enabled`
- `multikv_switch_primary_secondary`
- `multikv_migration_teardown`

Our final memberlist configuration will be:

```jsonnet
{
  _config+:: {
    memberlist_ring_enabled: true,
  }
}
```

This will not trigger new restart of components. After applying this change, you are finished.
