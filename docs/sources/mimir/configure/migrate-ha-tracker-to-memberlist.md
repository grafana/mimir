---
description: Learn how to migrate the HA tracker from Consul or etcd to memberlist without downtime.
menuTitle: Migrate HA tracker to memberlist
title: Migrate HA tracker from Consul or etcd to memberlist without downtime
weight: 55
---

# Migrate HA tracker from Consul or etcd to memberlist without downtime

Since Grafana Mimir 2.17, the HA tracker supports memberlist as a key-value (KV) store backend. Memberlist is the recommended KV store for the HA tracker.

Follow this guidance to migrate your HA tracker configuration from Consul or etcd to memberlist without any downtime.

## Why migrate to memberlist

Memberlist eliminates the need to deploy and maintain a separate consistent KV store (Consul or etcd) for the HA tracker. Benefits include:

- Simplified operations: No separate service to deploy, monitor, and maintain
- Reduced infrastructure: One less component in your deployment
- Built-in: Memberlist is already used by other Mimir components for hash rings and can be used for the HA tracker as well

## Before you begin

Before starting this migration:

- Ensure you're running Grafana Mimir 2.17 or later
- Have a running Mimir cluster with HA tracker currently using Consul or etcd

## Migration steps

The migration uses Mimir's multi KV store feature to transition from Consul or etcd to memberlist without downtime. The process involves:

1. [Configuring a multi KV store](#configure-multi-kv-store) with your current backend as primary and memberlist as secondary
1. [Enabling mirroring](#enable-kv-store-mirroring) to write to both stores
1. [Switching memberlist to be the primary store](#switch-to-memberlist-as-the-primary-store)
1. [Removing the old backend](#remove-multi-kv-store-configuration)

### Configure multi KV store

Configure the HA tracker to use the `multi` KV store with your current backend, Consul or etcd, as the primary store and memberlist as the secondary store.

For Consul:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: consul
        secondary: memberlist
        # Mirroring is disabled by default, we'll enable it in Step 2
        mirror_enabled: false
      consul:
        # Your existing Consul configuration
        host: "consul.default.svc.cluster.local:8500"
      memberlist:
        # Memberlist configuration - typically shared with other components
        join_members:
          - mimir-gossip-ring.default.svc.cluster.local:7946
```

For etcd:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: etcd
        secondary: memberlist
        # Mirroring is disabled by default, we'll enable it in Step 2
        mirror_enabled: false
      etcd:
        # Your existing etcd configuration
        endpoints:
          - etcd.default.svc.cluster.local:2379
      memberlist:
        # Memberlist configuration - typically shared with other components
        join_members:
          - mimir-gossip-ring.default.svc.cluster.local:7946
```

Apply this configuration change. This step requires a rollout of all distributor instances.

After applying this step, all distributors expose the `/memberlist` admin page on their HTTP port, which you can use to check the health of the memberlist cluster.

### Enable KV store mirroring

Enable mirroring to write the HA tracker state to both the primary (Consul or etcd) and secondary (memberlist) stores.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: consul # or etcd
        secondary: memberlist
        mirror_enabled: true # Changed in this step
```

Verification:

Monitor the following metrics to verify mirroring is working:

- `cortex_multikv_mirror_enabled`: Shows which distributors have mirroring enabled. This should be set to 1 for all distributors.
- `rate(cortex_multikv_mirror_writes_total[$__rate_interval])`: Rate of writes to secondary store, memberlist
- `rate(cortex_multikv_mirror_write_errors_total[$__rate_interval])`: Rate of write errors. This should be 0.

After enabling mirroring, you should see HA tracker keys in the memberlist cluster information on the `/memberlist` admin page.

### Switch to memberlist as the primary store

Switch the primary and secondary stores so that memberlist is the primary store.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: memberlist # Changed in this step
        secondary: consul # or etcd - changed in this step
        mirror_enabled: true
```

From this point on, distributors read from memberlist and mirror updates to Consul or etcd.

Verification:

Monitor `cortex_multikv_primary_store` to verify all distributors are using memberlist as the primary store. The `store` label should have the value `memberlist`.

### Disable mirroring

Stop mirroring writes to Consul or etcd.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: memberlist
        secondary: consul # or etcd
        mirror_enabled: false # Changed in this step
```

Verification:

Monitor `cortex_multikv_mirror_enabled` to verify that all distributors have disabled mirroring. This value should be 0.

### Remove multi KV store configuration

Configure the HA tracker to use memberlist directly, removing the multi KV store wrapper.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: memberlist # Changed in this step
      memberlist:
        join_members:
          - mimir-gossip-ring.default.svc.cluster.local:7946
```

Apply this configuration change. This step requires a rollout of all distributor instances.

After the rollout completes, distributors use memberlist directly for the HA tracker. The `cortex_multikv_*` metrics are no longer exposed.

### Cleanup

You have successfully migrated the HA tracker from Consul or etcd to memberlist without downtime.

If you are no longer using Consul or etcd for any other purpose in your Mimir deployment, you can:

- Remove the Consul or etcd deployment
- Remove any Consul or etcd monitoring dashboards and alerts
- Remove Consul or etcd client configuration from your Mimir configuration

## Helm-specific guidance

If you're using the `mimir-distributed` Helm chart, the migration steps are the same but use Helm values instead of direct YAML configuration. For example:

```yaml
mimir:
  structuredConfig:
    distributor:
      ha_tracker:
        enable_ha_tracker: true
        kvstore:
          store: multi
          multi:
            primary: consul # or etcd
            secondary: memberlist
            mirror_enabled: false
          consul:
            host: "{{ .Release.Name }}-consul-server.{{ .Release.Namespace }}.svc.cluster.local:8500"
          # or for etcd:
          # etcd:
          #   endpoints:
          #     - "{{ .Release.Name }}-etcd.{{ .Release.Namespace }}.svc.cluster.local:2379"
```
