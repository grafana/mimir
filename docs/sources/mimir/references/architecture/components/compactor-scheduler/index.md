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

## Benefits of using the compactor-scheduler

Compared to standalone mode, scheduler mode:

- Spreads work evenly across the whole compactor fleet. Because jobs aren't tied to the compactors that own a tenant in the hash ring, no compactor sits idle while others are backlogged, which improves utilization and, with properly sized compactors, reduces the overall time to compact.
- Ensures tenant fairness: pending jobs are distributed with a round-robin across tenants, so a tenant with many jobs can't starve the others.
- Retries failed jobs more efficiently. In standalone mode, when a job fails, the compactor restarts the compaction of the whole tenant, and reaches the failed job again only after re-planning and re-walking the preceding jobs. In scheduler mode, a failed job returns to the queue, and the next available compactor leases just that job.
- Allows scaling compactors based on the amount of pending work, because the compactor-scheduler knows the queue of outstanding jobs.

## How it works

The following flow describes how compaction work moves through a Grafana Mimir cluster running in scheduler mode:

1. The compactor-scheduler discovers the tenants by listing the object storage bucket, at every interval defined by `-compactor-scheduler.tenant-discovery-interval`.
1. For each tenant, the compactor-scheduler enqueues a planning job at every interval defined by `-compactor-scheduler.planning-interval`.
1. A compactor leases the planning job, computes the compaction plan for the tenant, and returns the resulting compaction jobs to the compactor-scheduler, which enqueues them. The compactor-scheduler never reads blocks or the bucket index itself.
1. Compactors lease the compaction jobs, execute them, and periodically report progress back to the compactor-scheduler.
1. When a compactor reports that a job completed, the compactor-scheduler removes the job from the queue.

Planning jobs and compaction jobs are kept in separate queues, called lanes. Within each lane, the compactor-scheduler ensures tenant fairness using a round-robin across all tenants that have pending jobs in that lane.

The `-compactor.scheduler-client.lanes` parameter configures the worker goroutines each compactor runs and the lanes each worker leases jobs from. The default value, `compact+plan,plan`, runs two workers: one that leases compaction jobs and falls back to planning jobs, and one dedicated to planning jobs, so that planning is not starved by long-running compactions.

### Job leases

Jobs are leased to compactors, not permanently assigned. If a compactor doesn't report progress on a job for longer than `-compactor-scheduler.lease-duration`, the compactor-scheduler makes the job available for other compactors to lease. A job that has been leased more than `-compactor-scheduler.repeated-failure-report-threshold` times without completing is reported as a repeated failure, and once it has been leased `-compactor-scheduler.max-leases` times it is removed from the queue. A discarded job is still re-planned and re-enqueued on the next planning interval, unless it no longer exists (for example, if the tenant has been deleted or the blocks were marked for deletion).

## State

The compactor-scheduler persists its job queues to local disk, in bbolt databases stored under `-compactor-scheduler.bbolt.dir`. After a restart, the compactor-scheduler recovers the queues from disk. When it starts with no recovered state, it delays planning for a few maintenance intervals, defined by `-compactor-scheduler.maintenance-intervals-before-cold-start-planning`, to avoid enqueueing jobs that duplicate work still in progress. If the persisted state becomes corrupted, it can be wiped and rebuilt: refer to the [recovery steps](../../../../manage/mimir-runbooks/#MimirCompactorSchedulerUnreachable) in the runbooks.

Run exactly one compactor-scheduler replica. Two active compactor-schedulers would both schedule work for all tenants, resulting in duplicate compactions. Because it runs as a single replica, zone-aware replication doesn't apply to the compactor-scheduler.

A short compactor-scheduler outage doesn't stop in-progress compactions: compactors keep executing the jobs they hold, retry job requests with backoff, and resume progress reports at the next report interval, until the compactor-scheduler is available again.

## Enable scheduler mode

To run compaction in scheduler mode:

1. Deploy the compactor-scheduler, using `-target=compactor-scheduler` and other configuration parameters as described in [compactor-scheduler configuration](../../../../configure/configuration-parameters/#compactor_scheduler).
1. Configure the compactors with:
   - `-compactor.scheduler-client.enabled=true`
   - `-compactor.scheduler-client.scheduler-endpoint=<host:port>`, pointing to the compactor-scheduler gRPC endpoint.

In scheduler mode, compactors still register in the compactor hash ring to coordinate blocks cleanup, which includes keeping the bucket index updated, deleting blocks, and enforcing retention.

### Migrate from standalone mode

The mode is a per-compactor setting, applied at startup. There is no coordination between the two modes. Running both modes simultaneously could result in a tenant being planned and compacted by both. This doesn't corrupt data, but it duplicates work for compactors and potentially increases store-gateway load. A mixed fleet is expected while the configuration change rolls out, but don't run it as a steady state.

On a fresh deployment, the compactor-scheduler delays planning for a few maintenance intervals (`-compactor-scheduler.maintenance-intervals-before-cold-start-planning`), giving the compactor rollout time to complete before the first jobs are planned. The rollout itself stops any in-flight standalone compactions, so in practice little or no work is duplicated. This makes it safe to migrate in a single step, rolling out the compactor-scheduler and switching the compactors to scheduler mode together, as described above.

If limiting which tenants are compacted using `-compactor.enabled-tenants` or `-compactor.disabled-tenants`, the same values should be configured on the compactor-scheduler. Compactors should still be configured with the same values as they apply to blocks cleanup as well.

## Compactor-scheduler configuration

Refer to the `compactor_scheduler` block in the [Grafana Mimir configuration parameters](../../../../configure/configuration-parameters/) for details of compactor-scheduler configuration, and to the `scheduler_client` block within the [compactor](../../../../configure/configuration-parameters/#compactor) block section for the compactor side of the configuration.
