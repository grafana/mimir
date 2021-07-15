# Playbooks

This document contains playbooks, or at least a checklist of what to look for, for alerts in the cortex-mixin and logs from Cortex. This document assumes that you are running a Cortex cluster:

1. Using this mixin config
2. Using GCS as object store (but similar procedures apply to other backends)

## Alerts

### CortexIngesterRestarts
First, check if the alert is for a single ingester or multiple. Even if the alert is only for one ingester, it's best to follow up by checking `kubectl get pods --namespace=<prod/staging/etc.>` every few minutes, or looking at the query `rate(kube_pod_container_status_restarts_total{container="ingester"}[30m]) > 0` just until you're sure there isn't a larger issue causing multiple restarts.

Next, check `kubectl get events`, with and without the addition of the `--namespace` flag, to look for node restarts or other related issues. Grep or something similar to filter the output can be useful here. The most common cause of this alert is a single cloud providers node restarting and causing the ingester on that node to be rescheduled somewhere else.

In events you're looking for things like:
```
57m Normal NodeControllerEviction Pod Marking for deletion Pod ingester-01 from Node cloud-provider-node-01
37m Normal SuccessfulDelete ReplicaSet (combined from similar events): Deleted pod: ingester-01
32m         Normal    NodeNotReady              Node   Node cloud-provider-node-01 status is now: NodeNotReady
28m         Normal    DeletingAllPods           Node   Node cloud-provider-node-01 event: Deleting all Pods from Node cloud-provider-node-01.
```

If nothing obvious from the above, check for increased load:
- If there is an increase in the number of active series and the memory provisioned is not enough, scale up the ingesters horizontally to have the same number of series as before per ingester.
- If we had an outage and once Cortex is back up, the incoming traffic increases. (or) The clients have their Prometheus remote-write lagging and starts to send samples at a higher rate (again, an increase in traffic but in terms of number of samples). Scale up the ingester horizontally in this case too.

### CortexIngesterReachingSeriesLimit

This alert fires when the `max_series` per ingester instance limit is enabled and the actual number of in-memory series in a ingester is reaching the limit. Once the limit is reached, writes to the ingester will fail (5xx) for new series, while appending samples to existing ones will continue to succeed.

In case of **emergency**:
- If the actual number of series is very close or already hit the limit, then you can increase the limit via runtime config to gain some time
- Increasing the limit will increase the ingesters memory utilization. Please monitor the ingesters memory utilization via the `Cortex / Writes Resources` dashboard

How the limit is **configured**:
- The limit can be configured either on CLI (`-ingester.instance-limits.max-series`) or in the runtime config:
  ```
  ingester_limits:
    max_series: <int>
  ```
- The mixin configures the limit in the runtime config and can be fine-tuned via:
  ```
  _config+:: {
    ingester_instance_limits+:: {
      max_series: <int>
    }
  }
  ```
- When configured in the runtime config, changes are applied live without requiring an ingester restart
- The configured limit can be queried via `cortex_ingester_instance_limits{limit="max_series"}`

How to **fix**:
1. **Temporarily increase the limit**<br />
   If the actual number of series is very close or already hit the limit, or if you foresee the ingester will hit the limit before dropping the stale series as effect of the scale up, you should also temporarily increase the limit.
1. **Check if shuffle-sharding shard size is correct**<br />
   When shuffle-sharding is enabled, we target to 100K series / tenant / ingester. You can run `avg by (user) (cortex_ingester_memory_series_created_total{namespace="<namespace>"} - cortex_ingester_memory_series_removed_total{namespace="<namespace>"}) > 100000` to find out tenants with > 100K series / ingester. You may want to increase the shard size for these tenants.
1. **Scale up ingesters**<br />
   Scaling up ingesters will lower the number of series per ingester. However, the effect of this change will take up to 4h, because after the scale up we need to wait until all stale series are dropped from memory as the effect of TSDB head compaction, which could take up to 4h (with the default config, TSDB keeps in-memory series up to 3h old and it gets compacted every 2h).

### CortexIngesterReachingTenantsLimit

This alert fires when the `max_tenants` per ingester instance limit is enabled and the actual number of tenants in a ingester is reaching the limit. Once the limit is reached, writes to the ingester will fail (5xx) for new tenants, while they will continue to succeed for previously existing ones.

In case of **emergency**:
- If the actual number of tenants is very close or already hit the limit, then you can increase the limit via runtime config to gain some time
- Increasing the limit will increase the ingesters memory utilization. Please monitor the ingesters memory utilization via the `Cortex / Writes Resources` dashboard

How the limit is **configured**:
- The limit can be configured either on CLI (`-ingester.instance-limits.max-tenants`) or in the runtime config:
  ```
  ingester_limits:
    max_tenants: <int>
  ```
- The mixin configures the limit in the runtime config and can be fine-tuned via:
  ```
  _config+:: {
    ingester_instance_limits+:: {
      max_tenants: <int>
    }
  }
  ```
- When configured in the runtime config, changes are applied live without requiring an ingester restart
- The configured limit can be queried via `cortex_ingester_instance_limits{limit="max_tenants"}`

How to **fix**:
1. Ensure shuffle-sharding is enabled in the Cortex cluster
1. Assuming shuffle-sharding is enabled, scaling up ingesters will lower the number of tenants per ingester. However, the effect of this change will be visible only after `-blocks-storage.tsdb.close-idle-tsdb-timeout` period so you may have to temporarily increase the limit

### CortexRequestLatency

This alert fires when a specific Cortex route is experiencing an high latency.

The alert message includes both the Cortex service and route experiencing the high latency. Establish if the alert is about the read or write path based on that (see [Cortex routes by path](#cortex-routes-by-path)).

#### Write Latency

How to **investigate**:
- Check the `Cortex / Writes` dashboard
  - Looking at the dashboard you should see in which Cortex service the high latency originates
  - The panels in the dashboard are vertically sorted by the network path (eg. cortex-gw -> distributor -> ingester)
- Deduce where in the stack the latency is being introduced
  - **`cortex-gw`**
    - The cortex-gw may need to be scaled up. Use the `Cortex / Scaling` dashboard to check for CPU usage vs requests.
    - There could be a problem with authentication (eg. slow to run auth layer)
  - **`distributor`**
    - Typically, distributor p99 latency is in the range 50-100ms. If the distributor latency is higher than this, you may need to scale up the distributors.
  - **`ingester`**
    - Typically, ingester p99 latency is in the range 5-50ms. If the ingester latency is higher than this, you should investigate the root cause before scaling up ingesters.
    - Check out the following alerts and fix them if firing:
      - `CortexProvisioningTooManyActiveSeries`
      - `CortexProvisioningTooManyWrites`

#### Read Latency

Query performance is a known issue. A query may be slow because of high cardinality, large time range and/or because not leveraging on cache (eg. querying series data not cached yet). When investigating this alert, you should check if it's caused by few slow queries or there's an operational / config issue to be fixed.

How to **investigate**:
- Check the `Cortex / Reads` dashboard
  - Looking at the dashboard you should see in which Cortex service the high latency originates
  - The panels in the dashboard are vertically sorted by the network path (eg. cortex-gw -> query-frontend -> query->scheduler -> querier -> store-gateway)
- Check the `Cortex / Slow Queries` dashboard to find out if it's caused by few slow queries
- Deduce where in the stack the latency is being introduced
  - **`cortex-gw`**
    - The cortex-gw may need to be scaled up. Use the `Cortex / Scaling` dashboard to check for CPU usage vs requests.
    - There could be a problem with authentication (eg. slow to run auth layer)
  - **`query-frontend`**
    - The query-frontend may beed to be scaled up. If the Cortex cluster is running with the query-scheduler, the query-frontend can be scaled up with no side effects, otherwise the maximum number of query-frontend replicas should be the configured `-querier.worker-parallelism`.
  - **`querier`**
    - Look at slow queries traces to find out where it's slow.
    - Typically, slowness either comes from running PromQL engine (`innerEval`) or fetching chunks from ingesters and/or store-gateways.
    - If slowness comes from running PromQL engine, typically there's not much we can do. Scaling up queriers may help only if querier nodes are overloaded.
    - If slowness comes from fetching chunks from ingesters and/or store-gateways you should investigate deeper on the root cause. Common causes:
      - High CPU utilization in ingesters
        - Scale up ingesters
      - Low cache hit ratio in the store-gateways
        - Check `Memcached Overview` dashboard
        - If memcached eviction rate is high, then you should scale up memcached replicas. Check the recommendations by `Cortex / Scaling` dashboard and make reasonable adjustments as necessary.
        - If memcached eviction rate is zero or very low, then it may be caused by "first time" queries

### CortexRequestErrors

This alert fires when the rate of 5xx errors of a specific route is > 1% for some time.

This alert typically acts as a last resort to detect issues / outages. SLO alerts are expected to trigger earlier: if an **SLO alert** has triggered as well for the same read/write path, then you can ignore this alert and focus on the SLO one (but the investigation procedure is typically the same).

How to **investigate**:
- Check for which route the alert fired (see [Cortex routes by path](#cortex-routes-by-path))
  - Write path: open the `Cortex / Writes` dashboard
  - Read path: open the `Cortex / Reads` dashboard
- Looking at the dashboard you should see in which Cortex service the error originates
  - The panels in the dashboard are vertically sorted by the network path (eg. on the write path: cortex-gw -> distributor -> ingester)
- If the failing service is going OOM (`OOMKilled`): scale up or increase the memory
- If the failing service is crashing / panicking: look for the stack trace in the logs and investigate from there

### CortexTransferFailed
This alert goes off when an ingester fails to find another node to transfer its data to when it was shutting down. If there is both a pod stuck terminating and one stuck joining, look at the kubernetes events. This may be due to scheduling problems caused by some combination of anti affinity rules/resource utilization. Adding a new node can help in these circumstances. You can see recent events associated with a resource via kubectl describe, ex: `kubectl -n <namespace> describe pod <pod>`

### CortexIngesterUnhealthy
This alert goes off when an ingester is marked as unhealthy. Check the ring web page to see which is marked as unhealthy. You could then check the logs to see if there are any related to that ingester ex: `kubectl logs -f ingester-01 --namespace=prod`. A simple way to resolve this may be to click the "Forgot" button on the ring page, especially if the pod doesn't exist anymore. It might not exist anymore because it was on a node that got shut down, so you could check to see if there are any logs related to the node that pod is/was on, ex: `kubectl get events --namespace=prod | grep cloud-provider-node`.

### CortexMemoryMapAreasTooHigh

This alert fires when a Cortex process has a number of memory map areas close to the limit. The limit is a per-process limit imposed by the kernel and this issue is typically caused by a large number of mmap-ed failes.

How to **fix**:
- Increase the limit on your system: `sysctl -w vm.max_map_count=<NEW LIMIT>`
- If it's caused by a store-gateway, consider enabling `-blocks-storage.bucket-store.index-header-lazy-loading-enabled=true` to lazy mmap index-headers at query time

More information:
- [Kernel doc](https://www.kernel.org/doc/Documentation/sysctl/vm.txt)
- [Side effects when increasing `vm.max_map_count`](https://www.suse.com/support/kb/doc/?id=000016692)

### CortexRulerFailedRingCheck

This alert occurs when a ruler is unable to validate whether or not it should claim ownership over the evaluation of a rule group. The most likely cause is that one of the rule ring entries is unhealthy. If this is the case proceed to the ring admin http page and forget the unhealth ruler. The other possible cause would be an error returned the ring client. If this is the case look into debugging the ring based on the in-use backend implementation.

### CortexRulerTooManyFailedPushes

This alert fires when rulers cannot push new samples (result of rule evaluation) to ingesters.

In general, pushing samples can fail due to problems with Cortex operations (eg. too many ingesters have crashed, and ruler cannot write samples to them), or due to problems with resulting data (eg. user hitting limit for number of series, out of order samples, etc.).
This alert fires only for first kind of problems, and not for problems caused by limits or invalid rules.

How to **fix**:
- Investigate the ruler logs to find out the reason why ruler cannot write samples. Note that ruler logs all push errors, including "user errors", but those are not causing the alert to fire. Focus on problems with ingesters.

### CortexRulerTooManyFailedQueries

This alert fires when rulers fail to evaluate rule queries.

Each rule evaluation may fail due to many reasons, eg. due to invalid PromQL expression, or query hits limits on number of chunks. These are "user errors", and this alert ignores them.

There is a category of errors that is more important: errors due to failure to read data from store-gateways or ingesters. These errors would result in 500 when run from querier. This alert fires if there is too many of such failures.

How to **fix**:
- Investigate the ruler logs to find out the reason why ruler cannot evaluate queries. Note that ruler logs rule evaluation errors even for "user errors", but those are not causing the alert to fire. Focus on problems with ingesters or store-gateways.

### CortexRulerMissedEvaluations

_TODO: this playbook has not been written yet._

### CortexIngesterHasNotShippedBlocks

This alert fires when a Cortex ingester is not uploading any block to the long-term storage. An ingester is expected to upload a block to the storage every block range period (defaults to 2h) and if a longer time elapse since the last successful upload it means something is not working correctly.

How to **investigate**:
- Ensure the ingester is receiving write-path traffic (samples to ingest)
- Look for any upload error in the ingester logs (ie. networking or authentication issues)

_If the alert `CortexIngesterTSDBHeadCompactionFailed` fired as well, then give priority to it because that could be the cause._

#### Ingester hit the disk capacity

If the ingester hit the disk capacity, any attempt to append samples will fail. You should:

1. Increase the disk size and restart the ingester. If the ingester is running in Kubernetes with a Persistent Volume, please refers to [Resizing Persistent Volumes using Kubernetes](#resizing-persistent-volumes-using-kubernetes).
2. Investigate why the disk capacity has been hit
  - Was the disk just too small?
  - Was there an issue compacting TSDB head and the WAL is increasing indefinitely?

### CortexIngesterHasNotShippedBlocksSinceStart

Same as [`CortexIngesterHasNotShippedBlocks`](#CortexIngesterHasNotShippedBlocks).

### CortexIngesterHasUnshippedBlocks

This alert fires when a Cortex ingester has compacted some blocks but such blocks haven't been successfully uploaded to the storage yet.

How to **investigate**:
- Look for details in the ingester logs

### CortexIngesterTSDBHeadCompactionFailed

This alert fires when a Cortex ingester is failing to compact the TSDB head into a block.

A TSDB instance is opened for each tenant writing at least 1 series to the ingester and its head contains the in-memory series not flushed to a block yet. Once the TSDB head is compactable, the ingester will try to compact it every 1 minute. If the TSDB head compaction repeatedly fails, it means it's failing to compact a block from the in-memory series for at least 1 tenant, and it's a critical condition that should be immediately investigated.

The cause triggering this alert could **lead to**:
- Ingesters run out of memory
- Ingesters run out of disk space
- Queries return partial results after `-querier.query-ingesters-within` time since the beginning of the incident

How to **investigate**:
- Look for details in the ingester logs

### CortexIngesterTSDBHeadTruncationFailed

This alert fires when a Cortex ingester fails to truncate the TSDB head.

The TSDB head is the in-memory store used to keep series and samples not compacted into a block yet. If head truncation fails for a long time, the ingester disk might get full as it won't continue to the WAL truncation stage and the subsequent ingester restart may take a long time or even go into an OOMKilled crash loop because of the huge WAL to replay. For this reason, it's important to investigate and address the issue as soon as it happen.

How to **investigate**:
- Look for details in the ingester logs

### CortexIngesterTSDBCheckpointCreationFailed

This alert fires when a Cortex ingester fails to create a TSDB checkpoint.

How to **investigate**:
- Look for details in the ingester logs
- If the checkpoint fails because of a `corruption in segment`, you can restart the ingester because at next startup TSDB will try to "repair" it. After restart, if the issue is repaired and the ingester is running, you should also get paged by `CortexIngesterTSDBWALCorrupted` to signal you the WAL was corrupted and manual investigation is required.

### CortexIngesterTSDBCheckpointDeletionFailed

This alert fires when a Cortex ingester fails to delete a TSDB checkpoint.

Generally, this is not an urgent issue, but manual investigation is required to find the root cause of the issue and fix it.

How to **investigate**:
- Look for details in the ingester logs

### CortexIngesterTSDBWALTruncationFailed

This alert fires when a Cortex ingester fails to truncate the TSDB WAL.

How to **investigate**:
- Look for details in the ingester logs

### CortexIngesterTSDBWALCorrupted

This alert fires when a Cortex ingester finds a corrupted TSDB WAL (stored on disk) while replaying it at ingester startup or when creation of a checkpoint comes across a WAL corruption.

If this alert fires during an **ingester startup**, the WAL should have been auto-repaired, but manual investigation is required. The WAL repair mechanism cause data loss because all WAL records after the corrupted segment are discarded and so their samples lost while replaying the WAL. If this issue happen only on 1 ingester then Cortex doesn't suffer any data loss because of the replication factor, while if it happens on multiple ingesters then some data loss is possible.

If this alert fires during a **checkpoint creation**, you should have also been paged with `CortexIngesterTSDBCheckpointCreationFailed`, and you can follow the steps under that alert.

### CortexIngesterTSDBWALWritesFailed

This alert fires when a Cortex ingester is failing to log records to the TSDB WAL on disk.

How to **investigate**:
- Look for details in the ingester logs

### CortexQuerierHasNotScanTheBucket

This alert fires when a Cortex querier is not successfully scanning blocks in the storage (bucket). A querier is expected to periodically iterate the bucket to find new and deleted blocks (defaults to every 5m) and if it's not successfully synching the bucket since a long time, it may end up querying only a subset of blocks, thus leading to potentially partial results.

How to **investigate**:
- Look for any scan error in the querier logs (ie. networking or rate limiting issues)

### CortexQuerierHighRefetchRate

This alert fires when there's an high number of queries for which series have been refetched from a different store-gateway because of missing blocks. This could happen for a short time whenever a store-gateway ring resharding occurs (e.g. during/after an outage or while rolling out store-gateway) but store-gateways should reconcile in a short time. This alert fires if the issue persist for an unexpected long time and thus it should be investigated.

How to **investigate**:
- Ensure there are no errors related to blocks scan or sync in the queriers and store-gateways
- Check store-gateway logs to see if all store-gateway have successfully completed a blocks sync

### CortexStoreGatewayHasNotSyncTheBucket

This alert fires when a Cortex store-gateway is not successfully scanning blocks in the storage (bucket). A store-gateway is expected to periodically iterate the bucket to find new and deleted blocks (defaults to every 5m) and if it's not successfully synching the bucket for a long time, it may end up querying only a subset of blocks, thus leading to potentially partial results.

How to **investigate**:
- Look for any scan error in the store-gateway logs (ie. networking or rate limiting issues)

### CortexCompactorHasNotSuccessfullyCleanedUpBlocks

This alert fires when a Cortex compactor is not successfully deleting blocks marked for deletion for a long time.

How to **investigate**:
- Ensure the compactor is not crashing during compaction (ie. `OOMKilled`)
- Look for any error in the compactor logs (ie. bucket Delete API errors)

### CortexCompactorHasNotSuccessfullyCleanedUpBlocksSinceStart

Same as [`CortexCompactorHasNotSuccessfullyCleanedUpBlocks`](#CortexCompactorHasNotSuccessfullyCleanedUpBlocks).

### CortexCompactorHasNotUploadedBlocks

This alert fires when a Cortex compactor is not uploading any compacted blocks to the storage since a long time.

How to **investigate**:
- If the alert `CortexCompactorHasNotSuccessfullyRunCompaction` has fired as well, then investigate that issue first
- If the alert `CortexIngesterHasNotShippedBlocks` or `CortexIngesterHasNotShippedBlocksSinceStart` have fired as well, then investigate that issue first
- Ensure ingesters are successfully shipping blocks to the storage
- Look for any error in the compactor logs

### CortexCompactorHasNotSuccessfullyRunCompaction

This alert fires if the compactor is not able to successfully compact all discovered compactable blocks (across all tenants).

When this alert fires, the compactor may still have successfully compacted some blocks but, for some reason, other blocks compaction is consistently failing. A common case is when the compactor is trying to compact a corrupted block for a single tenant: in this case the compaction of blocks for other tenants is still working, but compaction for the affected tenant is blocked by the corrupted block.

How to **investigate**:
- Look for any error in the compactor logs
  - Corruption: [`not healthy index found`](#compactor-is-failing-because-of-not-healthy-index-found)

#### Compactor is failing because of `not healthy index found`

The compactor may fail to compact blocks due a corrupted block index found in one of the source blocks:

```
level=error ts=2020-07-12T17:35:05.516823471Z caller=compactor.go:339 component=compactor msg="failed to compact user blocks" user=REDACTED err="compaction: group 0@6672437747845546250: block with not healthy index found /data/compact/0@6672437747845546250/REDACTED; Compaction level 1; Labels: map[__org_id__:REDACTED]: 1/1183085 series have an average of 1.000 out-of-order chunks: 0.000 of these are exact duplicates (in terms of data and time range)"
```

When this happen you should:
1. Rename the block prefixing it with `corrupted-` so that it will be skipped by the compactor and queriers. Keep in mind that doing so the block will become invisible to the queriers too, so its series/samples will not be queried. If the corruption affects only 1 block whose compaction `level` is 1 (the information is stored inside its `meta.json`) then Cortex guarantees no data loss because all the data is replicated across other blocks. In all other cases, there may be some data loss once you rename the block and stop querying it.
2. Ensure the compactor has recovered
3. Investigate offline the root cause (eg. download the corrupted block and debug it locally)

To rename a block stored on GCS you can use the `gsutil` CLI:

```
# Replace the placeholders:
# - BUCKET: bucket name
# - TENANT: tenant ID
# - BLOCK:  block ID

gsutil mv gs://BUCKET/TENANT/BLOCK gs://BUCKET/TENANT/corrupted-BLOCK
```

### CortexBucketIndexNotUpdated

This alert fires when the bucket index, for a given tenant, is not updated since a long time. The bucket index is expected to be periodically updated by the compactor and is used by queriers and store-gateways to get an almost-updated view over the bucket store.

How to **investigate**:
- Ensure the compactor is successfully running
- Look for any error in the compactor logs

### CortexTenantHasPartialBlocks

This alert fires when Cortex finds partial blocks for a given tenant. A partial block is a block missing the `meta.json` and this may usually happen in two circumstances:

1. A block upload has been interrupted and not cleaned up or retried
2. A block deletion has been interrupted and `deletion-mark.json` has been deleted before `meta.json`

How to **investigate**:
- Look for the block ID in the logs. Example Loki query:
  ```
  {cluster="<cluster>",namespace="<namespace>",container="compactor"} |= "skipped partial block"
  ```
- Find out which Cortex component operated on the block at last (eg. uploaded by ingester/compactor, or deleted by compactor)
- Investigate if was a partial upload or partial delete
- Safely manually delete the block from the bucket if was a partial delete or an upload failed by a compactor
- Further investigate if was an upload failed by an ingester but not later retried (ingesters are expected to retry uploads until succeed)

### CortexWALCorruption

This alert is only related to the chunks storage. This can happen because of 2 reasons: (1) Non graceful shutdown of ingesters. (2) Faulty storage or NFS.

WAL corruptions are only detected at startups, so at this point the WAL/Checkpoint would have been repaired automatically. So we can only check what happened and if there was any data loss and take actions to avoid this happening in future.

1. Check if there was any node restarts that force killed pods. If there is, then the corruption is from the non graceful shutdown of ingesters, which is generally fine. You can:
  * Describe the pod to see the last state.
  * Use `kube_pod_info` to check the node for the pod. `node_boot_time_seconds` to see if node just booted (which also indicates restart).
  * You can use `eventrouter` logs to double check.
  * Check ingester logs to check if the shutdown logs are missing at that time.
2. To confirm this, in the logs, check the WAL segment on which the corruption happened (let's say `X`) and the last checkpoint attempt number (let's say `Y`, this is the last WAL segment that was present when checkpointing started).
3. If `X > Y`, then it's most likely an abrupt restart of ingester and the corruption would be on the last few records of the last segment. To verify this, check the file timestamps of WAL segment `X` and `X - 1` if they were recent.
4. If `X < Y`, then the corruption was in some WAL segment which was not the last one. This indicates faulty disk and some data loss on that ingester.
5. In case of faulty disk corruption, if the number or ingesters that had corruption within the chunk flush age:
  1. Less than the quorum number for your replication factor: No data loss, because there is a guarantee that the data is replicated. For example, if replication factor is 3, then it's fine if corruption was on 1 ingester.
  2. Equal or more than the quorum number but less than replication factor: There is a good chance that there is no data loss if it was replicated to desired number of ingesters. But it's good to check once for data loss.
  3. Equal or more than the replication factor: Then there is definitely some data loss.

### CortexTableSyncFailure

_This alert applies to Cortex chunks storage only._

### CortexQueriesIncorrect

_TODO: this playbook has not been written yet._

### CortexInconsistentRuntimeConfig

This alert fires if multiple replicas of the same Cortex service are using a different runtime config for a longer period of time.

The Cortex runtime config is a config file which gets live reloaded by Cortex at runtime. In order for Cortex to work properly, the loaded config is expected to be the exact same across multiple replicas of the same Cortex service (eg. distributors, ingesters, ...). When the config changes, there may be short periods of time during which some replicas have loaded the new config and others are still running on the previous one, but it shouldn't last for more than few minutes.

How to **investigate**:
- Check how many different config file versions (hashes) are reported
  ```
  count by (sha256) (cortex_runtime_config_hash{namespace="<namespace>"})
  ```
- Check which replicas are running a different version
  ```
  cortex_runtime_config_hash{namespace="<namespace>",sha256="<unexpected>"}
  ```
- Check if the runtime config has been updated on the affected replicas' filesystem. Check `-runtime-config.file` command line argument to find the location of the file.
- Check the affected replicas logs and look for any error loading the runtime config

### CortexBadRuntimeConfig

This alert fires if Cortex is unable to reload the runtime config.

This typically means an invalid runtime config was deployed. Cortex keeps running with the previous (valid) version of the runtime config; running Cortex replicas and the system availability shouldn't be affected, but new replicas won't be able to startup until the runtime config is fixed.

How to **investigate**:
- Check the latest runtime config update (it's likely to be broken)
- Check Cortex logs to get more details about what's wrong with the config

### CortexFrontendQueriesStuck

This alert fires if Cortex is running without query-scheduler and queries are piling up in the query-frontend queue.

The procedure to investigate it is the same as the one for [`CortexSchedulerQueriesStuck`](#CortexSchedulerQueriesStuck): please see the other playbook for more details.

### CortexSchedulerQueriesStuck

This alert fires if queries are piling up in the query-scheduler.

How it **works**:
- A query-frontend API endpoint is called to execute a query
- The query-frontend enqueues the request to the query-scheduler
- The query-scheduler is responsible for dispatching enqueued queries to idle querier workers
- The querier runs the query, sends the response back directly to the query-frontend and notifies the query-scheduler that it can process another query

How to **investigate**:
- Are queriers in a crash loop (eg. OOMKilled)?
  - `OOMKilled`: temporarily increase queriers memory request/limit
  - `panic`: look for the stack trace in the logs and investigate from there
- Is QPS increased?
  - Scale up queriers to satisfy the increased workload
- Is query latency increased?
  - An increased latency reduces the number of queries we can run / sec: once all workers are busy, new queries will pile up in the queue
  - Temporarily scale up queriers to try to stop the bleed
  - Check if a specific tenant is running heavy queries
    - Run `sum by (user) (cortex_query_scheduler_queue_length{namespace="<namespace>"}) > 0` to find tenants with enqueued queries
    - Check the `Cortex / Slow Queries` dashboard to find slow queries
  - On multi-tenant Cortex cluster with **shuffle-sharing for queriers disabled**, you may consider to enable it for that specific tenant to reduce its blast radius. To enable queriers shuffle-sharding for a single tenant you need to set the `max_queriers_per_tenant` limit override for the specific tenant (the value should be set to the number of queriers assigned to the tenant).
  - On multi-tenant Cortex cluster with **shuffle-sharding for queriers enabled**, you may consider to temporarily increase the shard size for affected tenants: be aware that this could affect other tenants too, reducing resources available to run other tenant queries. Alternatively, you may choose to do nothing and let Cortex return errors for that given user once the per-tenant queue is full.

### CortexMemcachedRequestErrors

This alert fires if Cortex memcached client is experiencing an high error rate for a specific cache and operation.

How to **investigate**:
- The alert reports which cache is experiencing issue
  - `metadata-cache`: object store metadata cache
  - `index-cache`: TSDB index cache
  - `chunks-cache`: TSDB chunks cache
- Check which specific error is occurring
  - Run the following query to find out the reason (replace `<namespace>` with the actual Cortex cluster namespace)
    ```
    sum by(name, operation, reason) (rate(thanos_memcached_operation_failures_total{namespace="<namespace>"}[1m])) > 0
    ```
- Based on the **`reason`**:
  - `timeout`
    - Scale up the memcached replicas
  - `server-error`
    - Check both Cortex and memcached logs to find more details
  - `network-error`
    - Check Cortex logs to find more details
  - `malformed-key`
    - The key is too long or contains invalid characters
    - Check Cortex logs to find the offending key
    - Fixing this will require changes to the application code
  - `other`
    - Check both Cortex and memcached logs to find more details

### CortexOldChunkInMemory

_This alert applies to Cortex chunks storage only._

### CortexCheckpointCreationFailed

_This alert applies to Cortex chunks storage only._

### CortexCheckpointDeletionFailed

_This alert applies to Cortex chunks storage only._

### CortexProvisioningMemcachedTooSmall

_This alert applies to Cortex chunks storage only._

### CortexProvisioningTooManyActiveSeries

This alert fires if the average number of in-memory series per ingester is above our target (1.5M).

How to **fix**:
- Scale up ingesters
  - To find out the Cortex clusters where ingesters should be scaled up and how many minimum replicas are expected:
    ```
    ceil(sum by(cluster, namespace) (cortex_ingester_memory_series) / 1.5e6) >
    count by(cluster, namespace) (cortex_ingester_memory_series)
    ```
- After the scale up, the in-memory series are expected to be reduced at the next TSDB head compaction (occurring every 2h)

### CortexProvisioningTooManyWrites

This alert fires if the average number of samples ingested / sec in ingesters is above our target.

How to **fix**:
- Scale up ingesters
  - To compute the desired number of ingesters to satisfy the average samples rate you can run the following query, replacing `<namespace>` with the namespace to analyse and `<target>` with the target number of samples/sec per ingester (check out the alert threshold to see the current target):
    ```
    sum(rate(cortex_ingester_ingested_samples_total{namespace="<namespace>"}[$__rate_interval])) / (<target> * 0.9)
    ```

### CortexAllocatingTooMuchMemory

This alert fires when an ingester memory utilization is getting closer to the limit.

How it **works**:
- Cortex ingesters are a stateful service
- Having 2+ ingesters `OOMKilled` may cause a cluster outage
- Ingester memory baseline usage is primarily influenced by memory allocated by the process (mostly go heap) and mmap-ed files (used by TSDB)
- Ingester memory short spikes are primarily influenced by queries and TSDB head compaction into new blocks (occurring every 2h)
- A pod gets `OOMKilled` once its working set memory reaches the configured limit, so it's important to prevent ingesters memory utilization (working set memory) from getting close to the limit (we need to keep at least 30% room for spikes due to queries)

How to **fix**:
- Check if the issue occurs only for few ingesters. If so:
  - Restart affected ingesters 1 by 1 (proceed with the next one once the previous pod has restarted and it's Ready)
    ```
    kubectl -n <namespace> delete pod ingester-XXX
    ```
  - Restarting an ingester typically reduces the memory allocated by mmap-ed files. After the restart, ingester may allocate this memory again over time, but it may give more time while working on a longer term solution
- Check the `Cortex / Writes Resources` dashboard to see if the number of series per ingester is above the target (1.5M). If so:
  - Scale up ingesters
  - Memory is expected to be reclaimed at the next TSDB head compaction (occurring every 2h)

### CortexGossipMembersMismatch

This alert fires when any instance does not register all other instances as members of the memberlist cluster.

How it **works**:
- This alert applies when memberlist is used for the ring backing store.
- All Cortex instances using the ring, regardless of type, join a single memberlist cluster.
- Each instance (=memberlist cluster member) should be able to see all others.
- Therefore the following should be equal for every instance:
  - The reported number of cluster members (`memberlist_client_cluster_members_count`)
  - The total number of currently responsive instances.

How to **investigate**:
- The instance which has the incomplete view of the cluster (too few members) is specified in the alert.
- If the count is zero:
  - It is possible that the joining the cluster has yet to succeed.
  - The following log message indicates that the _initial_ initial join did not succeed: `failed to join memberlist cluster`
  - The following log message indicates that subsequent re-join attempts are failing: `re-joining memberlist cluster failed`
  - If it is the case that the initial join failed, take action according to the reason given.
- Verify communication with other members by checking memberlist traffic is being sent and received by the instance using the following metrics:
  - `memberlist_tcp_transport_packets_received_total`
  - `memberlist_tcp_transport_packets_sent_total`
- If traffic is present, then verify there are no errors sending or receiving packets using the following metrics:
  - `memberlist_tcp_transport_packets_sent_errors_total`
  - `memberlist_tcp_transport_packets_received_errors_total`
  - These errors (and others) can be found by searching for messages prefixed with `TCPTransport:`.
- Logs coming directly from memberlist are also logged by Cortex; they may indicate where to investigate further. These can be identified as such due to being tagged with `caller=memberlist_logger.go:xyz`.

### EtcdAllocatingTooMuchMemory

This can be triggered if there are too many HA dedupe keys in etcd. We saw this when one of our clusters hit 20K tenants that were using HA dedupe config. Raise the etcd limits via:

```
  etcd+: {
    spec+: {
      pod+: {
        resources+: {
          limits: {
            memory: '2Gi',
          },
        },
      },
    },
  },
```

## Cortex routes by path

**Write path**:
- `/distributor.Distributor/Push`
- `/cortex.Ingester/Push`
- `api_v1_push`
- `api_prom_push`
- `api_v1_push_influx_write`

**Read path**:
- `/schedulerpb.SchedulerForFrontend/FrontendLoop`
- `/cortex.Ingester/QueryStream`
- `/cortex.Ingester/QueryExemplars`
- `/gatewaypb.StoreGateway/Series`
- `api_prom_label`
- `api_prom_api_v1_query_exemplars`

**Ruler / rules path**:
- `api_v1_rules`
- `api_v1_rules_namespace`
- `api_prom_rules_namespace`

## Cortex blocks storage - What to do when things to wrong

## Recovering from a potential data loss incident

The ingested series data that could be lost during an incident can be stored in two places:

1. Ingesters (before blocks are shipped to the bucket)
2. Bucket

There could be several root causes leading to a potential data loss. In this document we're going to share generic procedures that could be used as a guideline during an incident.

### Halt the compactor

The Cortex cluster continues to successfully operate even if the compactor is not running, except that over a long period (12+ hours) this will lead to query performance degradation. The compactor could potentially be the cause of data loss because:

- It marks blocks for deletion (soft deletion). _This doesn't lead to any immediate deletion, but blocks marked for deletion will be hard deleted once a delay expires._
- It permanently deletes blocks marked for deletion after `-compactor.deletion-delay` (hard deletion)
- It could generate corrupted compacted blocks (eg. due to a bug or if a source block is corrupted and the automatic checks can't detect it)

**If you suspect the compactor could be the cause of data loss, halt it** (delete the statefulset or scale down the replicas to 0). It can be restarted anytime later.

When the compactor is **halted**:

- No new blocks will be compacted
- No blocks will be deleted (soft and hard deletion)

### Recover source blocks from ingesters

Ingesters keep, on their persistent disk, the blocks compacted from TSDB head until the `-experimental.tsdb.retention-period` retention expires. The **default retention is 4 days**, in order to give cluster operators enough time to react in case of a data loss incident.

The blocks retained in the ingesters can be used in case the compactor generates corrupted blocks and the source blocks, shipped from ingesters, have already been hard deleted from the bucket.

How to manually blocks from ingesters to the bucket:

1. Ensure [`gsutil`](https://cloud.google.com/storage/docs/gsutil) is installed in the Cortex pod. If not, [install it](#install-gsutil-in-the-cortex-pod)
2. Run `cd /data/tsdb && /path/to/gsutil -m rsync -n -r -x 'thanos.shipper.json|chunks_head|wal' . gs://<bucket>/recovered/`
   - `-n` enabled the **dry run** (remove it once you've verified the output matches your expectations)
   - `-m` enables parallel mode
   - `-r` enables recursive rsync
   - `-x <pattern>` excludes specific patterns from sync (no WAL or shipper metadata file should be uploaded to the bucket)
   - Don't use `-d` (dangerous) because it will delete from the bucket any block which is not in the local filesystem

### Freeze ingesters persistent disk

The blocks and WAL stored in the ingester persistent disk are the last fence of defence in case of an incident involving blocks not shipped to the bucket or corrupted blocks in the bucket. If the data integrity in the ingester's disk is at risk (eg. close to hit the TSDB retention period or close to reach max disk utilisation), you should freeze it taking a **disk snapshot**.

To take a **GCP persistent disk snapshot**:

1. Identify the Kubernetes PVC volume name (`kubectl get pvc -n <namespace>`) of the volumes to snapshot
2. For each volume, [create a snapshot](https://console.cloud.google.com/compute/snapshotsAdd) from the GCP console ([documentation](https://cloud.google.com/compute/docs/disks/create-snapshots))

### Halt the ingesters

Halting the ingesters should be the **very last resort** because of the side effects. To halt the ingesters, while preserving their disk and without disrupting the cluster write path, you need to:

1. Create a second pool of ingesters
  - Uses the functions `newIngesterStatefulSet()`, `newIngesterPdb()`
2. Wait until the second pool is up and running
3. Halt existing ingesters (scale down to 0 or delete their statefulset)

However the **queries will return partial data**, due to all the ingested samples which have not been compacted to blocks yet.

## Manual procedures

### Resizing Persistent Volumes using Kubernetes

This is the short version of an extensive documentation on [how to resize Kubernetes Persistent Volumes](https://kubernetes.io/blog/2018/07/12/resizing-persistent-volumes-using-kubernetes/).

**Pre-requisites**:

- Running Kubernetes v1.11 or above
- The PV storage class has `allowVolumeExpansion: true`
- The PV is backed by a supported block storage volume (eg. GCP-PD, AWS-EBS, ...)

**How to increase the volume**:

1. Edit the PVC (persistent volume claim) `spec` for the volume to resize and **increase** `resources` > `requests` > `storage`
2. Restart the pod attached to the PVC for which the storage request has been increased

### How to create clone volume (Google Cloud specific)

In some scenarios, it may be useful to preserve current volume status for inspection, but keep using the volume.
[Google Persistent Disk supports "Clone"](https://cloud.google.com/compute/docs/disks/add-persistent-disk#source-disk) operation that can be used to do that.
Newly cloned disk is independant from its original, and can be used for further investigation by attaching it to a new Machine / Pod.

When using Kubernetes, here is YAML file that creates PV (`clone-ingester-7-pv`) pointing to the new disk clone (`clone-pvc-80cc0efa-4996-11ea-ba79-42010a96008c` in this example),
PVC (`clone-ingester-7-pvc`) pointing to PV, and finally Pod (`clone-ingester-7-dataaccess`) using the PVC to access the disk.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: clone-ingester-7-pv
spec:
  accessModes:
  - ReadWriteOnce
  capacity:
    storage: 150Gi
  gcePersistentDisk:
    fsType: ext4
    pdName: clone-pvc-80cc0efa-4996-11ea-ba79-42010a96008c
  persistentVolumeReclaimPolicy: Retain
  storageClassName: fast
  volumeMode: Filesystem
---
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: clone-ingester-7-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 150Gi
  storageClassName: fast
  volumeName: clone-ingester-7-pv
  volumeMode: Filesystem
---
apiVersion: v1
kind: Pod
metadata:
    name: clone-ingester-7-dataaccess
spec:
    containers:
    - name: alpine
      image: alpine:latest
      command: ['sleep', 'infinity']
      volumeMounts:
      - name: mypvc
        mountPath: /data
      resources:
        requests:
          cpu: 500m
          memory: 1024Mi
    volumes:
    - name: mypvc
      persistentVolumeClaim:
        claimName: clone-ingester-7-pvc
```

After this preparation, one can use `kubectl exec -t -i clone-ingester-7-dataaccess /bin/sh` to inspect the disk mounted under `/data`.

### Install `gsutil` in the Cortex pod

1. Install python
   ```
   apk add python3 py3-pip
   ln -s /usr/bin/python3 /usr/bin/python
   pip install google-compute-engine
   ```
2. Download `gsutil`
   ```
   wget https://storage.googleapis.com/pub/gsutil.tar.gz
   tar -zxvf gsutil.tar.gz
   ./gsutil/gsutil --help
   ```
3. Create `/etc/boto.cfg` with the following content:
   ```
   [GoogleCompute]
   service_account = default

   [Plugin]
   plugin_directory = /usr/lib/python3.8/site-packages/google_compute_engine/boto
   ```

### Deleting a StatefulSet with persistent volumes

When you delete a Kubernetes StatefulSet whose pods have persistent volume claims (PVC), the PVCs are not automatically deleted. This means that if the StatefulSet is recreated, the pods for which there was already a PVC will get the volume mounted previously.

A PVC can be manually deleted by an operator. When a PVC claim is deleted, what happens to the volume depends on its [Reclaim Policy](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#reclaiming):

- `Retain`: the volume will not be deleted until the PV resource will be manually deleted from Kubernetes
- `Delete`: the volume will be automatically deleted


## Log lines

### Log line containing 'sample with repeated timestamp but different value'

This means a sample with the same timestamp as the latest one was received with a different value. The number of occurrences is recorded in the `cortex_discarded_samples_total` metric with the label `reason="new-value-for-timestamp"`.

Possible reasons for this are:
- Incorrect relabelling rules can cause a label to be dropped from a series so that multiple series have the same labels. If these series were collected from the same target they will have the same timestamp.
- The exporter being scraped sets the same timestamp on every scrape. Note that exporters should generally not set timestamps.
