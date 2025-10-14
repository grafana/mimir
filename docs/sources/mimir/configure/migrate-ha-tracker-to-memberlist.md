---
description: Learn how to migrate the HA tracker from Consul or etcd to memberlist without downtime.
menuTitle: Migrate HA tracker to memberlist
title: Migrate HA tracker from Consul or etcd to memberlist without downtime
weight: 55
---

# Migrate HA tracker from Consul or etcd to memberlist without downtime

Since Grafana Mimir 2.17, the HA tracker supports memberlist as a key-value (KV) store backend. Memberlist is now the recommended KV store for the HA tracker, and the `consul` and `etcd` backends are deprecated.

This guide describes how to migrate your HA tracker configuration from Consul or etcd to memberlist without any downtime.

## Why migrate to memberlist

Memberlist eliminates the need to deploy and maintain a separate consistent KV store (Consul or etcd) for the HA tracker. Benefits include:

- Simplified operations: No separate service to deploy, monitor, and maintain
- Reduced infrastructure: One less component in your deployment
- Built-in: Memberlist is already used by other Mimir components for hash rings and can be used for the HA tracker as well

## Prerequisites

Before starting this migration:

- Ensure you're running Grafana Mimir 2.17 or later
- Have a running Mimir cluster with HA tracker currently using Consul or etcd
- Understand your current HA tracker configuration
- Have the ability to update Mimir configuration (either through config files, helm values, or runtime config)

## Migration steps

The migration uses Mimir's multi KV store feature to transition from Consul/etcd to memberlist without downtime. The process involves:

1. Configuring a multi KV store with your current backend as primary and memberlist as secondary
2. Enabling mirroring to write to both stores
3. Switching memberlist to be the primary store
4. Removing the old backend

### Step 1: Configure multi KV store

Configure the HA tracker to use the `multi` KV store with your current backend (Consul or etcd) as the primary store and memberlist as the secondary store.

**For Consul:**

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

**For etcd:**

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: etcd
        secondary: memberlist
        mirror_enabled: false
      etcd:
        # Your existing etcd configuration
        endpoints:
          - etcd.default.svc.cluster.local:2379
      memberlist:
        join_members:
          - mimir-gossip-ring.default.svc.cluster.local:7946
```

Apply this configuration change. This step requires a rollout of all distributor instances.

After applying this step, all distributors will expose the `/memberlist` admin page on their HTTP port, which you can use to check the health of the memberlist cluster.

### Step 2: Enable KV store mirroring

Enable mirroring to write HA tracker state to both the primary (Consul/etcd) and secondary (memberlist) stores.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: consul  # or etcd
        secondary: memberlist
        mirror_enabled: true  # Changed in this step
```

This change can be applied via runtime configuration and does not require restarting distributors.

**Verification:**

Monitor the following metrics to verify mirroring is working:

- `cortex_multikv_mirror_enabled` – Shows which distributors have mirroring enabled (should be 1 for all)
- `rate(cortex_multikv_mirror_writes_total[1m])` – Rate of writes to secondary store (memberlist)
- `rate(cortex_multikv_mirror_write_errors_total[1m])` – Rate of write errors (should be 0 or very low)

After mirroring is enabled, you should see HA tracker keys in the memberlist cluster information on the `/memberlist` admin page.

### Step 3: Switch to memberlist as primary

Switch the primary and secondary stores so memberlist becomes the primary store.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: memberlist  # Changed in this step
        secondary: consul    # or etcd - changed in this step
        mirror_enabled: true
```

This change can be applied via runtime configuration and does not require restarting distributors.

From this point on, distributors will read from memberlist and mirror updates to Consul/etcd.

**Verification:**

Monitor `cortex_multikv_primary_store` to verify all distributors are using memberlist as the primary store. The metric should show `memberlist` as the value.

### Step 4: Disable mirroring

Stop mirroring writes to Consul/etcd.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: multi
      multi:
        primary: memberlist
        secondary: consul  # or etcd
        mirror_enabled: false  # Changed in this step
```

This change can be applied via runtime configuration and does not require restarting distributors.

**Verification:**

Monitor `cortex_multikv_mirror_enabled` to verify all distributors have disabled mirroring (value should be 0).

### Step 5: Remove multi KV store configuration

Configure the HA tracker to use memberlist directly, removing the multi KV store wrapper.

Update the configuration:

```yaml
distributor:
  ha_tracker:
    enable_ha_tracker: true
    kvstore:
      store: memberlist  # Changed in this step
      memberlist:
        join_members:
          - mimir-gossip-ring.default.svc.cluster.local:7946
```

Apply this configuration change. This step requires a rollout of all distributor instances.

After the rollout completes, distributors will use memberlist directly for the HA tracker. The `cortex_multikv_*` metrics will no longer be exposed.

### Step 6: Cleanup

You have successfully migrated the HA tracker from Consul/etcd to memberlist without downtime.

If you are no longer using Consul or etcd for any other purpose in your Mimir deployment, you can now:

- Remove the Consul or etcd deployment
- Remove any Consul/etcd monitoring dashboards and alerts
- Remove Consul/etcd client configuration from your Mimir configuration

## Helm-specific guidance

If you're using the `mimir-distributed` Helm chart, the migration steps are similar but use Helm values instead of direct YAML configuration.

### Step 1: Configure multi KV store

```yaml
mimir:
  structuredConfig:
    distributor:
      ha_tracker:
        enable_ha_tracker: true
        kvstore:
          store: multi
          multi:
            primary: consul  # or etcd
            secondary: memberlist
            mirror_enabled: false
          consul:
            host: "{{ .Release.Name }}-consul-server.{{ .Release.Namespace }}.svc.cluster.local:8500"
          # or for etcd:
          # etcd:
          #   endpoints:
          #     - "{{ .Release.Name }}-etcd.{{ .Release.Namespace }}.svc.cluster.local:2379"
```

### Step 2-4: Runtime configuration updates

For steps 2-4, you can update the runtime configuration without redeploying. Use the runtime config ConfigMap or API to update:

- Step 2: Set `mirror_enabled: true`
- Step 3: Swap primary/secondary values
- Step 4: Set `mirror_enabled: false`

### Step 5: Remove multi KV store

Update your Helm values to use memberlist directly:

```yaml
mimir:
  structuredConfig:
    distributor:
      ha_tracker:
        enable_ha_tracker: true
        kvstore:
          store: memberlist
```

Upgrade the Helm release to roll out the change.

## Verification and testing

After completing the migration:

1. Check the `/memberlist` admin page on distributors to verify memberlist cluster health
2. Monitor HA tracker metrics:
   - `cortex_ha_tracker_elected_replica_total` – Number of elected replicas per cluster
   - `cortex_ha_tracker_replicas_count` – Number of replicas tracked per cluster
3. Verify HA deduplication is working by sending samples from HA Prometheus replicas and confirming only one replica's samples are accepted

## Rollback procedure

If you encounter issues during migration, you can rollback to the previous step:

- **From Step 2**: Disable mirroring (set `mirror_enabled: false`) and return to Step 1 configuration
- **From Step 3**: Switch primary/secondary back to original (Consul/etcd as primary)
- **From Step 4**: Re-enable mirroring (set `mirror_enabled: true`)
- **From Step 5**: Redeploy with multi KV store configuration from Step 4

At any point before Step 5, you can revert changes via runtime configuration without restarting distributors.

## Troubleshooting

### Memberlist cluster not forming

If the memberlist cluster is not forming properly:

- Check that all distributor instances can reach each other on the gossip port (default 7946)
- Verify the `join_members` addresses are correct and DNS is resolving properly
- Check firewall rules allow gossip traffic between distributors
- Review distributor logs for memberlist connection errors

### High mirror write errors

If you see high rates of `cortex_multikv_mirror_write_errors_total`:

- Check memberlist cluster health on the `/memberlist` admin page
- Verify network connectivity between distributors
- Review distributor logs for specific error messages
- Ensure sufficient resources (CPU, memory) for distributors

### HA deduplication not working after migration

If HA deduplication stops working after migration:

- Verify `cortex_multikv_primary_store` shows the expected primary store
- Check that elected replica information is present in memberlist (visible on `/memberlist` page)
- Review `cortex_ha_tracker_*` metrics to confirm tracking is active
- Verify Prometheus external labels (cluster and replica) are still configured correctly
