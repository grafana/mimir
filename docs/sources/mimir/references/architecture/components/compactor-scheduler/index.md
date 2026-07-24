---
description: The compactor-scheduler coordinates compaction work across compactors.
menuTitle: Compactor-scheduler
title: Grafana Mimir compactor-scheduler
weight: 15
---

# Grafana Mimir compactor-scheduler

The compactor-scheduler is an optional component that coordinates compaction work across [compactors](../compactor/). It maintains a queue of compaction jobs for each tenant and distributes the jobs to compactors over gRPC.

{{< admonition type="note" >}}
The compactor-scheduler is experimental. For the related configuration parameters, refer to [experimental features](../../../../configure/about-versioning/#experimental-features).
{{< /admonition >}}

When a compactor-scheduler is deployed, compactors run in _scheduler mode_: instead of using a [hash ring](../../hash-ring/) to determine which tenants and blocks to compact, they request jobs from the compactor-scheduler and execute them. Any compactor can execute jobs for any tenant. For a description of standalone mode, which is the default, refer to [compactor sharding](../compactor/#compactor-sharding).

## How it works

The following flow describes how compaction work moves through a Grafana Mimir cluster running in scheduler mode:

1. The compactor-scheduler discovers the tenants by listing the object storage bucket, at every interval defined by `-compactor-scheduler.tenant-discovery-interval`.
1. For each tenant, the compactor-scheduler enqueues a planning job at every interval defined by `-compactor-scheduler.planning-interval`.
1. A compactor leases the planning job, computes the compaction plan for the tenant, and returns the resulting compaction jobs to the compactor-scheduler, which enqueues them. The compactor-scheduler never reads blocks or the bucket index itself.
1. Compactors lease the compaction jobs, execute them, and periodically report progress back to the compactor-scheduler.
1. When a compactor reports that a job completed, the compactor-scheduler removes the job from the queue.

By default, planning jobs and compaction jobs are kept in separate queues ("lanes") that are consumed from different worker goroutines, so that planning is not starved by long-running compactions. The `-compactor.scheduler-client.lanes` parameter configures how many worker goroutines each compactor runs and the lanes each worker leases jobs from.

The compactor-scheduler ensures tenant fairness using a round-robin between all tenants that have pending jobs.

### Job leases

Jobs are leased to compactors, not permanently assigned. If a compactor doesn't report progress on a job for longer than `-compactor-scheduler.lease-duration`, the compactor-scheduler makes the job available for other compactors to lease. A job that has been leased `-compactor-scheduler.max-leases` times without completing is removed from the queue and reported as a repeated failure. A discarded job will still be re-planned and re-enqueued on the next planning interval, unless it no longer exists (e.g., the tenant has been deleted or the blocks were marked for deletion).

## State

The compactor-scheduler persists its job queues to local disk, in bbolt databases stored under `-compactor-scheduler.bbolt.dir`. After a restart, the compactor-scheduler recovers the queues from disk and delays planning for a few maintenance intervals to avoid enqueueing duplicate jobs.

Run exactly one compactor-scheduler replica. Two active compactor-schedulers would both schedule work for all tenants, resulting in duplicate compactions. A short compactor-scheduler outage doesn't stop in-progress compactions: compactors keep executing the jobs they hold, and retry job requests and progress reports with backoff until the compactor-scheduler is available again.

## Enable scheduler mode

To run compaction in scheduler mode:

1. Deploy the compactor-scheduler, using `-target=compactor-scheduler`.
1. Configure the compactors with:
   - `-compactor.scheduler-client.enabled=true`
   - `-compactor.scheduler-client.scheduler-endpoint=<host:port>`, pointing to the compactor-scheduler gRPC endpoint.

In scheduler mode, compactors still register in the compactor hash ring to coordinate blocks cleanup, which includes keeping the bucket index updated, deleting blocks, and enforcing retention.


### Migrate from standalone mode

The mode is a per-compactor setting, applied at startup. There is no coordination between the two modes. Running both modes simultaneously could result in a tenant being planned and compacted by both. This doesn't corrupt data, but it duplicates work for compactors and potentially increases store-gateways load.
A mixed fleet is expected while the configuration change rolls out, but don't run it as a steady state.

On a fresh deployment, the compactor-scheduler's cold start (`-compactor-scheduler.maintenance-intervals-before-cold-start-planning`) mitigates the duplicate work issue, giving time to compactors that are still running in standalone mode to finish their work and for the rollout to complete before planning starts. In practice, this makes it safe to migrate solely by rolling out the compactor-scheduler and switching compactors to scheduler mode in a single step, as described above.

## Compactor-scheduler configuration

Refer to the `compactor_scheduler` block in the [Grafana Mimir configuration parameters](../../../../configure/configuration-parameters/) for details of compactor-scheduler configuration, and to the `scheduler_client` block within the [compactor](../../../../configure/configuration-parameters/#compactor) block section for the compactor side of the configuration.
