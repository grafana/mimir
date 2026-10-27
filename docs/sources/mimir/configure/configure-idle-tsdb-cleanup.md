---
description: Preserve empty ingester TSDB idle age across restarts.
menuTitle: Idle TSDB cleanup
title: Configure restart-resilient idle TSDB cleanup
weight: 90
---

# Configure restart-resilient idle TSDB cleanup

An ingester normally measures TSDB inactivity using in-memory state. Restarting an ingester resets the idle age of TSDBs with empty heads. Frequent restarts can therefore prevent those TSDBs from reaching the idle timeout.

The experimental `-blocks-storage.tsdb.persist-idle-state-enabled` flag preserves empty TSDB idle age on local disk. It is disabled by default.

When enabled, the ingester starts the idle period when it observes an empty head. This is conservative: the period starts after head compaction, rather than at the last write. The ingester checks for expired TSDBs during startup, before reading new ingestion data, and during its existing periodic cleanup sweep.

## Enable persistent idle state

Enable the feature on ingesters using the following configuration:

```yaml
blocks_storage:
  tsdb:
    persist_idle_state_enabled: true
```

The feature only takes effect when `-blocks-storage.tsdb.close-idle-tsdb-timeout` is positive. Block shipping must be enabled, or `-blocks-storage.tsdb.close-idle-tsdb-when-shipping-disabled` must explicitly allow cleanup without shipping.

The existing cleanup safeguards still apply: the head must be empty and all local blocks must have been shipped, unless cleanup without shipping is explicitly allowed. Tenant deletion markers retain their existing behavior.

## Persistence and failure behavior

Each tenant TSDB stores a versioned `idle-state.json` file containing either active state or an empty timestamp and local block identities. The ingester validates saved empty state against the replayed head and current blocks before startup compaction. Blocks removed by retention do not reset the idle age. New block identities or a nonempty replayed head invalidate the saved empty period, including after writes made while the feature was disabled.

Before committing the first accepted samples, histograms, or exemplars after an empty period, the ingester durably records active state. That first write includes a file update and filesystem synchronization. Subsequent writes while active do not update the idle-state file. A failure to persist active state rolls back the append and returns a retryable storage error.

Missing, invalid, unsupported, or future-dated metadata starts a new conservative empty period. Metadata write failures prevent idle cleanup. Losing the local disk also loses the saved age.

The following counters report persistence behavior without tenant labels:

- `cortex_ingester_idle_tsdb_state_restorations_total`: saved empty ages successfully restored.
- `cortex_ingester_idle_tsdb_state_failures_total{operation="read"}`: metadata read or validation failures, excluding missing files.
- `cortex_ingester_idle_tsdb_state_failures_total{operation="write"}`: metadata persistence failures.

## Disable the feature

Set `persist_idle_state_enabled` to `false` to return to the existing in-memory idle timer. Disabled ingesters ignore the metadata file and perform no idle-state metadata I/O. On re-enablement, the ingester validates saved state against the recovered TSDB before using it.
