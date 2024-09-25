---
aliases:
  # Do not remove this alias before the new location is released to "latest" documentation and mixins are updated.
  - ../operators-guide/mimir-runbooks/
description: Grafana Mimir runbooks.
keywords:
  - Mimir runbooks
menuTitle: Runbooks
title: Grafana Mimir runbooks
weight: 110
---

# Grafana Mimir runbooks

This document contains runbooks, or at least a checklist of what to look for, for alerts in the mimir-mixin and logs from Mimir. This document assumes that you are running a Mimir cluster:

1. Using this mixin config
2. Using GCS as object store (but similar procedures apply to other backends)

## Alerts

### MimirIngesterRestarts

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
- If we had an outage and once Mimir is back up, the incoming traffic increases. (or) The clients have their Prometheus remote-write lagging and starts to send samples at a higher rate (again, an increase in traffic but in terms of number of samples). Scale up the ingester horizontally in this case too.

### MimirIngesterReachingSeriesLimit

This alert fires when the `max_series` per ingester instance limit is enabled and the actual number of in-memory series in an ingester is reaching the limit.
The threshold is set at 80%, to give some chance to react before the limit is reached.
Once the limit is reached, writes to the ingester will fail for new series. Appending samples to existing ones will continue to succeed.

Note that the error responses sent back to the sender are classed as "server error" (5xx), which should result in a retry by the sender.
While this situation continues, these retries will stall the flow of data, and newer data will queue up on the sender.
If the condition is cleared in a short time, service can be restored with no data loss.

This is different to what happens when the `max_global_series_per_user` is exceeded, which is considered a "client error" (4xx) where excess data is discarded.

In case of **emergency**:

- If the actual number of series is very close to or already hit the limit, then you can increase the limit via runtime config to gain some time
- Increasing the limit will increase the ingesters' memory utilization. Please monitor the ingesters' memory utilization via the `Mimir / Writes Resources` dashboard

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

How to **fix** it:

1. **Temporarily increase the limit**<br />
   If the actual number of series is very close to or already hit the limit, or if you foresee the ingester will hit the limit before dropping the stale series as an effect of the scale up, you should also temporarily increase the limit.
2. **Check if shuffle-sharding shard size is correct**<br />

- When shuffle-sharding is enabled, we target up to 100K series / tenant / ingester assuming tenants on average use 50% of their max series limit.
- Run the following **instant query** to find tenants that might cause higher pressure on ingesters. The query excludes tenants which are already sharded across all ingesters:

  ```
  topk by (pod) (5, # top 5 tenants per ingester
      sum by (user, pod) ( # get in-memory series for each tenant on each pod
          cortex_ingester_memory_series_created_total{namespace="<namespace>"} - cortex_ingester_memory_series_removed_total{namespace="<namespace>"}
      )
      and on(user) # intersection with tenants that are exceeding 50% of their series limit (added across ingesters & accounting for replication)
      (
          sum by (user) ( # total in-memory series for the tenant across ingesters
              cortex_ingester_memory_series_created_total{namespace="<namespace>"} - cortex_ingester_memory_series_removed_total{namespace="<namespace>"}
          )
          /
          scalar( # Account for replication
              ( # Classic storage
                  max(cortex_distributor_replication_factor{namespace="<namespace>"})
              )
              or
              ( # Ingest storage
                  # count the number of zones processing writes
                  count(group by (job) (cortex_ingester_memory_series{namespace="<namespace>"}))
              )
          )
          > 70000 # show only big tenants - with more than 70K series before replication
          > 0.5 * max by(user) (cortex_limits_overrides{namespace="<namespace>", limit_name="max_global_series_per_user"}) # global limit
      )
      and on (pod) ( # intersection with top 3 ingesters by in-memory series
          topk(3,
              sum by (pod) (cortex_ingester_memory_series{namespace="<namespace>"})
          )
      )
      and on(user) ( # intersection with the tenants which don't have series on all ingesters
          count by (user) (cortex_ingester_memory_series_created_total{namespace="<namespace>"}) # count ingesters where each tenant has series
          !=
          scalar(count(count by (pod) (cortex_ingester_memory_series{namespace="<namespace>"}))) # count total ingesters: first `count` counts series by ingester (we ignore the counted number), second `count` counts rows in series per ingester, second count gives the number of ingesters
      )
  )
  ```

- Check the current shard size of each tenant in the output and, if they're not already sharded across all ingesters, you may consider to double their shard size
- Be warned that the when increasing the shard size for a tenant, the number of in-memory series will temporarily increase. Make sure to monitor:
  - The per-ingester number of series, to make sure that any are not close to reaching the limit. You might need to temporarily raise the ingester `max_series`.
  - The per-tenant number of series. Due to reshuffling, series will be counted multiple times (in the new and old ingesters), and therefore a tenant may risk having samples rejected because they hit the `per_user` series limit. You might need to temporarily raise the limit.
- The in-memory series in the ingesters will be effectively reduced at the TSDB head compaction happening at least 1h after you increased the shard size for the affected tenants

3. **Scale up ingesters**<br />
   Scaling up ingesters will lower the number of series per ingester. However, the effect of this change will take up to 4h, because after the scale up we need to wait until all stale series are dropped from memory as the effect of TSDB head compaction, which could take up to 4h (with the default config, TSDB keeps in-memory series up to 3h old and it gets compacted every 2h).

### MimirIngesterReachingTenantsLimit

This alert fires when the `max_tenants` per ingester instance limit is enabled and the actual number of tenants in an ingester is reaching the limit. Once the limit is reached, writes to the ingester will fail (5xx) for new tenants, while they will continue to succeed for previously existing ones.

The per-tenant memory utilisation in ingesters includes the overhead of allocations for TSDB stripes and chunk writer buffers. If the tenant number is high, this may contribute significantly to the total ingester memory utilization. The size of these allocations is controlled by `-blocks-storage.tsdb.stripe-size` (default 16KiB) and `-blocks-storage.tsdb.head-chunks-write-buffer-size-bytes` (default 4MiB), respectively.

In case of **emergency**:

- If the actual number of tenants is very close to or already hit the limit, then you can increase the limit via runtime config to gain some time
- Increasing the limit will increase the ingesters' memory utilization. Please monitor the ingesters' memory utilization via the `Mimir / Writes Resources` dashboard

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

How to **fix** it:

1. Ensure shuffle-sharding is enabled in the Mimir cluster
1. Assuming shuffle-sharding is enabled, scaling up ingesters will lower the number of tenants per ingester. However, the effect of this change will be visible only after `-blocks-storage.tsdb.close-idle-tsdb-timeout` period so you may have to temporarily increase the limit

### MimirDistributorReachingInflightPushRequestLimit

This alert fires when the `cortex_distributor_inflight_push_requests` per distributor instance limit is enabled and the actual number of in-flight push requests is approaching the set limit. Once the limit is reached, push requests to the distributor will fail (5xx) for new requests, while existing in-flight push requests will continue to succeed.

In case of **emergency**:

- If the actual number of in-flight push requests is very close to or already at the set limit, then you can increase the limit via CLI flag or config to gain some time
- Increasing the limit will increase the number of in-flight push requests which will increase distributors' memory utilization. Please monitor the distributors' memory utilization via the `Mimir / Writes Resources` dashboard

How the limit is **configured**:

- The limit can be configured either by the CLI flag (`-distributor.instance-limits.max-inflight-push-requests`) or in the config:
  ```
  distributor:
    instance_limits:
      max_inflight_push_requests: <int>
  ```
- These changes are applied with a distributor restart.
- The configured limit can be queried via `cortex_distributor_instance_limits{limit="max_inflight_push_requests"})`

How to **fix** it:

1. **Temporarily increase the limit**<br />
   If the actual number of in-flight push requests is very close to or already hit the limit.
2. **Scale up distributors**<br />
   Scaling up distributors will lower the number of in-flight push requests per distributor.

### MimirRequestLatency

This alert fires when a specific Mimir route is experiencing an high latency.

The alert message includes both the Mimir service and route experiencing the high latency. Establish if the alert is about the read or write path based on that (see [Mimir routes by path](#mimir-routes-by-path)).

#### Write Latency

How to **investigate**:

- Check the `Mimir / Writes` dashboard
  - Looking at the dashboard you should see in which Mimir service the high latency originates
  - The panels in the dashboard are vertically sorted by the network path (eg. gateway -> distributor -> ingester). When using [ingest-storage](#mimir-ingest-storage-experimental), network path changes to gateway -> distributor -> Kafka instead.
- Deduce where in the stack the latency is being introduced
  - **`gateway`**
    - Latency may be caused by the time taken for the gateway to receive the entire request from the client. There are a multitude of reasons this can occur, so communication with the user may be necessary. For example:
      - Network issues such as packet loss between the client and gateway.
      - Poor performance of intermediate network hops such as load balancers or HTTP proxies.
      - Client process having insufficient CPU resources.
    - The gateway may need to be scaled up. Use the `Mimir / Scaling` dashboard to check for CPU usage vs requests.
    - There could be a problem with authentication (eg. slow to run auth layer)
  - **`distributor`**
    - Typically, distributor p99 latency is in the range 50-100ms. If the distributor latency is higher than this, you may need to scale up the distributors.
    - When using Mimir [ingest-storage](#mimir-ingest-storage-experimental), distributors are writing requests to Kafka-compatible backend. Increased latency in distributor may also come from this backend.
  - **`ingester`**
    - Typically, ingester p99 latency is in the range 5-50ms. If the ingester latency is higher than this, you should investigate the root cause before scaling up ingesters.
    - Check out the following alerts and fix them if firing:
      - `MimirIngesterReachingSeriesLimit`
      - `MimirProvisioningTooManyWrites`

#### Read Latency

Query performance is a known issue. A query may be slow because of high cardinality, large time range and/or because not leveraging on cache (eg. querying series data not cached yet). When investigating this alert, you should check if it's caused by few slow queries or there's an operational / config issue to be fixed.

How to **investigate**:

- Check the `Mimir / Reads` dashboard
  - Looking at the dashboard you should see in which Mimir service the high latency originates
  - The panels in the dashboard are vertically sorted by the network path (eg. gateway -> query-frontend -> query->scheduler -> querier -> store-gateway)
- Check the `Mimir / Slow Queries` dashboard to find out if it's caused by few slow queries
- Deduce where in the stack the latency is being introduced
  - **`gateway`**
    - The gateway may need to be scaled up. Use the `Mimir / Scaling` dashboard to check for CPU usage vs requests.
    - There could be a problem with authentication (eg. slow to run auth layer)
  - **`query-frontend`**
    - The query-frontend may need to be scaled up. If the Mimir cluster is running with the query-scheduler, the query-frontend can be scaled up with no side effects, otherwise the maximum number of query-frontend replicas should be the configured `-querier.max-concurrent`.
  - **`querier`**
    - Look at slow queries traces to find out where it's slow.
    - Typically, slowness either comes from running PromQL engine (`innerEval`) or fetching chunks from ingesters and/or store-gateways.
    - If slowness comes from running PromQL engine, typically there's not much we can do. Scaling up queriers may help only if querier nodes are overloaded.
    - If slowness comes from fetching chunks from ingesters and/or store-gateways you should investigate deeper on the root cause. Common causes:
      - High CPU utilization in ingesters
        - Scale up ingesters
      - Low cache hit ratio in the store-gateways
        - Check `Memcached Overview` dashboard
        - If memcached eviction rate is high, then you should scale up memcached replicas. Check the recommendations by `Mimir / Scaling` dashboard and make reasonable adjustments as necessary.
        - If memcached eviction rate is zero or very low, then it may be caused by "first time" queries
      - Cache query timeouts
        - Check store-gateway logs and look for warnings about timed out Memcached queries (example query: `{namespace="example-mimir-cluster", name=~"store-gateway.*"} |= "level=warn" |= "memcached" |= "timeout"`)
        - If there are indeed a lot of timed out Memcached queries, consider whether the store-gateway Memcached timeout setting (`-blocks-storage.bucket-store.chunks-cache.memcached.timeout`) is sufficient
    - By consulting the "Queue length" panel of the `Mimir / Queries` dashboard, determine if queries are waiting in queue due to busy queriers (an indication of this would be queue length > 0 for some time)
      - If queries are waiting in queue
        - Consider scaling up number of queriers if they're not auto-scaled; if auto-scaled, check auto-scaling parameters
      - If queries are not waiting in queue
        - Consider [enabling query sharding]({{< relref "../../references/architecture/query-sharding#how-to-enable-query-sharding" >}}) if not already enabled, to increase query parallelism
        - If query sharding already enabled, consider increasing total number of query shards (`query_sharding_total_shards`) for tenants submitting slow queries, so their queries can be further parallelized
  - **`ingester`**
    - Check if ingesters are not overloaded. If they are and you can scale up ingesters vertically, that may be the best action. If that's not possible, scaling horizontally can help as well, but it can take several hours for ingesters to fully redistribute their series.
    - When using [ingest-storage](#mimir-ingest-storage-experimental), check ratio of queries using strong-consistency, and latency of queries using strong-consistency.

#### Alertmanager

How to **investigate**:

- Check the `Mimir / Alertmanager` dashboard
  - Looking at the dashboard you should see which part of the stack is affected
- Deduce where in the stack the latency is being introduced
  - **Configuration API (gateway) + Alertmanager UI**
    - Latency may be caused by the time taken for the gateway to receive the entire request from the client. There are a multitude of reasons this can occur, so communication with the user may be necessary. For example:
      - Network issues such as packet loss between the client and gateway.
      - Poor performance of intermediate network hops such as load balancers or HTTP proxies.
      - Client process having insufficient CPU resources.
    - The gateway may need to be scaled up. Use the `Mimir / Scaling` dashboard to check for CPU usage vs requests.
    - There could be a problem with authentication (eg. slow to run auth layer)
  - **Alertmanager distributor**
    - Typically, Alertmanager distributor p99 latency is in the range 50-100ms. If the distributor latency is higher than this, you may need to scale up the number of alertmanager replicas.

### MimirRequestErrors

This alert fires when the rate of 5xx errors of a specific route is > 1% for some time.

This alert typically acts as a last resort to detect issues / outages. SLO alerts are expected to trigger earlier: if an **SLO alert** has triggered as well for the same read/write path, then you can ignore this alert and focus on the SLO one (but the investigation procedure is typically the same).

How to **investigate**:

- Check for which route the alert fired (see [Mimir routes by path](#mimir-routes-by-path))
  - Write path: open the `Mimir / Writes` dashboard
  - Read path: open the `Mimir / Reads` dashboard
- Looking at the dashboard you should see in which Mimir service the error originates
  - The panels in the dashboard are vertically sorted by the network path (eg. on the write path: gateway -> distributor -> ingester)
- If the failing service is going OOM (`OOMKilled`): scale up or increase the memory
- If the failing service is crashing / panicking: look for the stack trace in the logs and investigate from there
  - If crashing service is query-frontend, querier or store-gateway, and you have "activity tracker" feature enabled, look for `found unfinished activities from previous run` message and subsequent `activity` messages in the log file to see which queries caused the crash.
- When using Memberlist as KV store for hash rings, ensure that Memberlist is working correctly. See instructions for the [`MimirGossipMembersTooHigh`](#MimirGossipMembersTooHigh) and [`MimirGossipMembersTooLow`](#MimirGossipMembersTooLow) alerts.
- When using [ingest-storage](#mimir-ingest-storage-experimental) and distributors are failing to write requests to Kafka, make sure that Kafka is up and running correctly.

#### Alertmanager

How to **investigate**:

- Looking at `Mimir / Alertmanager` dashboard you should see in which part of the stack the error originates
- If some replicas are going OOM (`OOMKilled`): scale up or increase the memory
- If the failing service is crashing / panicking: look for the stack trace in the logs and investigate from there
- If the `route` label is `alertmanager`, check the logs for distributor errors containing `component=AlertmanagerDistributor`
  - Check if instances are starved for resources using the `Mimir / Alertmanager resources` dashboard
  - If the distributor errors are `context deadline exceeded` and the instances are not starved for resources, increase the distributor
    timeout with `-alertmanager.alertmanager-client.remote-timeout=<timeout>`. The default is 2s if not specified.

### MimirIngesterUnhealthy

This alert goes off when one or more ingesters are marked as unhealthy. Check the ring web page to see which ones are marked as unhealthy. You could then check the logs to see if there are any related to involved ingesters, such as `kubectl logs --follow ingester-01 --namespace=prod`. A simple way to resolve this might be to select **Forget** on the ring page, especially if the Pod doesn't exist anymore. It might not no longer exist because it was on a node that was shut down. Check to see if there are any logs related to the node that pod is or was on, such as `kubectl get events --namespace=prod | grep cloud-provider-node`.

### MimirMemoryMapAreasTooHigh

This alert fires when a Mimir process has a number of memory map areas close to the limit. The limit is a per-process limit imposed by the kernel and this issue is typically caused by a large number of mmap-ed failures.

How to **fix** it:

- Increase the limit on your system: `sysctl --write vm.max_map_count=<NEW LIMIT>`
- If it's caused by a store-gateway, consider enabling `-blocks-storage.bucket-store.index-header.lazy-loading-enabled=true` to lazy mmap index-headers at query time

More information:

- [Kernel doc](https://www.kernel.org/doc/Documentation/sysctl/vm.txt)
- [Side effects when increasing `vm.max_map_count`](https://www.suse.com/support/kb/doc/?id=000016692)

### MimirRulerFailedRingCheck

This alert occurs when a ruler is unable to validate whether or not it should claim ownership over the evaluation of a rule group. The most likely cause is that one of the rule ring entries is unhealthy. If this is the case proceed to the ring admin http page and forget the unhealth ruler. The other possible cause would be an error returned the ring client. If this is the case look into debugging the ring based on the in-use backend implementation.

When using Memberlist as KV store for hash rings, ensure that Memberlist is working correctly. See instructions for the [`MimirGossipMembersTooHigh`](#MimirGossipMembersTooHigh) and [`MimirGossipMembersTooLow`](#MimirGossipMembersTooLow) alerts.

### MimirRulerTooManyFailedPushes

This alert fires when rulers cannot push new samples (result of rule evaluation) to ingesters.

In general, pushing samples can fail due to problems with Mimir operations (eg. too many ingesters have crashed, and ruler cannot write samples to them), or due to problems with resulting data (eg. user hitting limit for number of series, out of order samples, etc.).
This alert fires only for first kind of problems, and not for problems caused by limits or invalid rules.

How to **fix** it:

- Investigate the ruler logs to find out the reason why ruler cannot write samples. Note that ruler logs all push errors, including "user errors", but those are not causing the alert to fire. Focus on problems with ingesters.
- When using Memberlist as KV store for hash rings, ensure that Memberlist is working correctly. See instructions for the [`MimirGossipMembersTooHigh`](#MimirGossipMembersTooHigh) and [`MimirGossipMembersTooLow`](#MimirGossipMembersTooLow) alerts.

### MimirRulerTooManyFailedQueries

This alert fires when rulers fail to evaluate rule queries.

Each rule evaluation may fail due to many reasons, eg. due to invalid PromQL expression, or query hits limits on number of chunks. These are "user errors", and this alert ignores them.

There is a category of errors that is more important: errors due to failure to read data from store-gateways or ingesters. These errors would result in 500 when run from querier. This alert fires if there is too many of such failures.

How to **fix** it:

- Investigate the ruler logs to find out the reason why ruler cannot evaluate queries. Note that ruler logs rule evaluation errors even for "user errors", but those are not causing the alert to fire. Focus on problems with ingesters or store-gateways.
- In case remote operational mode is enabled the problem could be at any of the ruler query path components (ruler-query-frontend, ruler-query-scheduler and ruler-querier). Check the `Mimir / Remote ruler reads` and `Mimir / Remote ruler reads resources` dashboards to find out in which Mimir service the error is being originated.
  - If the ruler is logging the gRPC error "received message larger than max", consider increasing `-ruler.query-frontend.grpc-client-config.grpc-max-recv-msg-size` in the ruler. This configuration option sets the maximum size of a message received by the ruler from the query-frontend (or ruler-query-frontend if you're running a dedicated read path for rule evaluations). If you're using jsonnet, you should just tune `_config.ruler_remote_evaluation_max_query_response_size_bytes`.
  - If the ruler is logging the gRPC error "trying to send message larger than max", consider increasing `-server.grpc-max-send-msg-size-bytes` in the query-frontend (or ruler-query-frontend if you're running a dedicated read path for rule evaluations). If you're using jsonnet, you should just tune `_config.ruler_remote_evaluation_max_query_response_size_bytes`.
- When using Memberlist as KV store for hash rings, ensure that Memberlist is working correctly. See instructions for the [`MimirGossipMembersTooHigh`](#MimirGossipMembersTooHigh) and [`MimirGossipMembersTooLow`](#MimirGossipMembersTooLow) alerts.

### MimirRulerMissedEvaluations

This alert fires when there is a rule group that is taking longer to evaluate than its evaluation interval.

How it **works**:

- The Mimir ruler will evaluate a rule group according to the evaluation interval on the rule group.
- If an evaluation is not finished by the time the next evaluation should happen, the next evaluation is missed.

How to **fix** it:

- Increase the evaluation interval of the rule group. You can use the rate of missed evaluation to estimate how long the rule group evaluation actually takes.
- Try splitting up the rule group into multiple rule groups. Rule groups are evaluated in parallel, so the same rules may still fit in the same resolution.

### MimirRulerRemoteEvaluationFailing

This alert fires when communication between `ruler` and `ruler-query-frontend` is failing to be established.

The `ruler-query-frontend` component is exclusively used by the `ruler` to evaluate rule expressions when running in remote operational mode. If communication between these two components breaks, gaps are expected to appear in the case of recording rules or alerting rules will not fire when they should.

How to **investigate**:

- Check the `Mimir / Remote ruler reads` dashboard to see if the issue is caused by failures or high latency
  - **Failures**
    - Check the `ruler-query-frontend` logs to find out more details about the error
  - **High latency**
    - Check the `Mimir / Remote ruler reads resources` dashboard to see if CPU or Memory usage increased unexpectedly

### MimirIngesterHasNotShippedBlocks

This alert fires when a Mimir ingester is not uploading any block to the long-term storage. An ingester is expected to upload a block to the storage every block range period (defaults to 2h) and if a longer time elapse since the last successful upload it means something is not working correctly.

How to **investigate**:

- Ensure the ingester is receiving write-path traffic (samples to ingest)
- Look for any upload error in the ingester logs (ie. networking or authentication issues)

_If the alert `MimirIngesterTSDBHeadCompactionFailed` fired as well, then give priority to it because that could be the cause._

#### Ingester hit the disk capacity

If the ingester hit the disk capacity, any attempt to append samples will fail. You should:

1. Increase the disk size and restart the ingester. If the ingester is running in Kubernetes with a Persistent Volume, please refer to [Resizing Persistent Volumes using Kubernetes](#resizing-persistent-volumes-using-kubernetes).
2. Investigate why the disk capacity has been hit

- Was the disk just too small?
- Was there an issue compacting TSDB head and the WAL is increasing indefinitely?

### MimirIngesterHasNotShippedBlocksSinceStart

Same as [`MimirIngesterHasNotShippedBlocks`](#MimirIngesterHasNotShippedBlocks).

### MimirIngesterHasUnshippedBlocks

This alert fires when a Mimir ingester has compacted some blocks but such blocks haven't been successfully uploaded to the storage yet.

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterTSDBHeadCompactionFailed

This alert fires when a Mimir ingester is failing to compact the TSDB head into a block.

A TSDB instance is opened for each tenant writing at least 1 series to the ingester and its head contains the in-memory series not flushed to a block yet. Once the TSDB head is compactable, the ingester will try to compact it every 1 minute. If the TSDB head compaction repeatedly fails, it means it's failing to compact a block from the in-memory series for at least 1 tenant, and it's a critical condition that should be immediately investigated.

The cause triggering this alert could **lead to**:

- Ingesters run out of memory
- Ingesters run out of disk space
- Queries return partial results after `-querier.query-ingesters-within` time since the beginning of the incident

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterTSDBHeadTruncationFailed

This alert fires when a Mimir ingester fails to truncate the TSDB head.

The TSDB head is the in-memory store used to keep series and samples not compacted into a block yet. If head truncation fails for a long time, the ingester disk might get full as it won't continue to the WAL truncation stage and the subsequent ingester restart may take a long time or even go into an OOMKilled crash loop because of the huge WAL to replay. For this reason, it's important to investigate and address the issue as soon as it happen.

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterTSDBCheckpointCreationFailed

This alert fires when a Mimir ingester fails to create a TSDB checkpoint.

How to **investigate**:

- Look for details in the ingester logs
- If the checkpoint fails because of a `corruption in segment`, you can restart the ingester because at next startup TSDB will try to "repair" it. After restart, if the issue is repaired and the ingester is running, you should also get paged by `MimirIngesterTSDBWALCorrupted` to signal you the WAL was corrupted and manual investigation is required.

### MimirIngesterTSDBCheckpointDeletionFailed

This alert fires when a Mimir ingester fails to delete a TSDB checkpoint.

Generally, this is not an urgent issue, but manual investigation is required to find the root cause of the issue and fix it.

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterTSDBWALTruncationFailed

This alert fires when a Mimir ingester fails to truncate the TSDB WAL.

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterTSDBWALCorrupted

This alert fires when more than one Mimir ingester finds a corrupted TSDB WAL (stored on disk) while replaying it at ingester startup or when creation of a checkpoint comes across a WAL corruption.

If this alert fires during an **ingester startup**, the WAL should have been auto-repaired, but manual investigation is required. The WAL repair mechanism causes data loss because all WAL records after the corrupted segment are discarded, and so their samples are lost while replaying the WAL. If this happens only on 1 ingester or only on one zone in a multi-zone cluster, then Mimir doesn't suffer any data loss because of the replication factor. But if it happens on multiple ingesters, multiple zones, or both, some data loss is possible.

To investigate how the ingester dealt with the WAL corruption, it's recommended you search the logs, e.g. with the following Grafana Loki query:

```
{cluster="<cluster>",namespace="<namespace>", pod="<pod>"} |= "corrupt"
```

The aforementioned query should typically produce entries starting with the ingester discovering the WAL corruption ("Encountered WAL read error, attempting repair"), and should hopefully show that the ingester repaired the WAL.

WAL corruption can occur after pods are rescheduled following a fault with the underlying node, causing the node to be marked `NotReady` (e.g. an unplanned power outage, storage and/or network fault). Check for recent events related to the ingester pod in question:

```
kubectl get events --field-selector involvedObject.name=ingester-X
```

If this alert fires during a **checkpoint creation**, you should have also been paged with `MimirIngesterTSDBCheckpointCreationFailed`, and you can follow the steps under that alert.

### MimirIngesterTSDBWALWritesFailed

This alert fires when a Mimir ingester is failing to log records to the TSDB WAL on disk.

How to **investigate**:

- Look for details in the ingester logs

### MimirIngesterInstanceHasNoTenants

This alert fires when an ingester instance doesn't own any tenants and is therefore idling.

How it **works**:

- Ingesters join a hash ring that facilitates per-tenant request sharding across ingester replicas.
- Distributors shard requests that belong to an individual tenant across a subset of ingester replicas. The number of replicas used per tenant is determined by the `-distributor.ingestion-tenant-shard-size` or the `ingestion_tenant_shard_size` limit.
- When the tenant shard size is lower than the number of ingester replicas, some ingesters might not receive requests for any tenants.
- This is more likely to happen in Mimir clusters with a lower number of tenants.

How to **fix** it:

Choose one of three options:

- Increase the shard size of one or more tenants to match the number of ingester replicas.
- Set the shard size of one or more tenants to `0`; this will shard the given tenant’s requests across all ingesters.
- [Decrease the number of ingester replicas]({{< relref "../run-production-environment/scaling-out#scaling-down-ingesters" >}}) to match the highest number of shards per tenant.

### MimirRulerInstanceHasNoRuleGroups

This alert fires when a ruler instance doesn't own any rule groups and is therefore idling.

How it **works**:

- When [ruler shuffle sharding]({{< relref "../../configure/configure-shuffle-sharding#ruler-shuffle-sharding" >}}) is enabled, a single tenant's rule groups are sharded across a subset of ruler instances, with a given rule group always being evaluated on a single ruler.
- The parameters `-ruler.tenant-shard-size` or `ruler_tenant_shard_size` control how many ruler instances a tenant's rule groups are sharded across.
- When the overall number of rule groups or the tenant's shard size is lower than the number of ruler replicas, some replicas might not be assigned any rule group to evaluate and remain idle.

How to **fix** it:

- Increase the shard size of one or more tenants to match the number of ruler replicas.
- Set the shard size of one or more tenants to `0`; this will shard the given tenant's rule groups across all ingesters.
- Decrease the total number of ruler replicas by the number of idle replicas.

### MimirStoreGatewayHasNotSyncTheBucket

This alert fires when a Mimir store-gateway is not successfully scanning blocks in the storage (bucket). A store-gateway is expected to periodically iterate the bucket to find new and deleted blocks (defaults to every 5m) and if it's not successfully synching the bucket for a long time, it may end up querying only a subset of blocks, thus leading to potentially partial results.

How to **investigate**:

- Look for any scan error in the store-gateway logs (ie. networking or rate limiting issues)

### MimirStoreGatewayNoSyncedTenants

This alert fires when a store-gateway doesn't own any tenant. Effectively it is sitting idle because no blocks are sharded to it.

How it **works**:

- Store-gateways join a hash ring to shard tenants and blocks across all store-gateway replicas.
- A tenant can be sharded across multiple store-gateways. How many exactly is determined by `-store-gateway.tenant-shard-size` or the `store_gateway_tenant_shard_size` limit.
- When the tenant shard size is less than the replicas of store-gateways, some store-gateways may not get any tenants' blocks sharded to them.
- This is more likely to happen in Mimir clusters with fewer number of tenants.

How to **fix** it:

There are three options:

- Reduce the replicas of store-gateways so that they match the highest number of shards per tenant or
- Increase the shard size of one or more tenants to match the number of replicas or
- Set the shard size of one or more tenant to `0`; this will shard this tenant's blocks across all store-gateways.

### MimirCompactorHasNotSuccessfullyCleanedUpBlocks

This alert fires when a Mimir compactor is not successfully deleting blocks marked for deletion for a long time.

How to **investigate**:

- Ensure the compactor is not crashing during compaction (ie. `OOMKilled`)
- Look for any error in the compactor logs (ie. bucket Delete API errors)

### MimirCompactorHasNotSuccessfullyCleanedUpBlocksSinceStart

Same as [`MimirCompactorHasNotSuccessfullyCleanedUpBlocks`](#MimirCompactorHasNotSuccessfullyCleanedUpBlocks).

### MimirCompactorHasNotUploadedBlocks

This alert fires when a Mimir compactor is not uploading any compacted blocks to the storage since a long time.

How to **investigate**:

- If the alert `MimirCompactorHasNotSuccessfullyRunCompaction` has fired as well, then investigate that issue first
- If the alert `MimirIngesterHasNotShippedBlocks` or `MimirIngesterHasNotShippedBlocksSinceStart` have fired as well, then investigate that issue first
- Ensure ingesters are successfully shipping blocks to the storage
- Look for any error in the compactor logs

### MimirCompactorHasNotSuccessfullyRunCompaction

This alert fires if the compactor is not able to successfully compact all discovered compactable blocks (across all tenants).

When this alert fires, the compactor may still have successfully compacted some blocks but, for some reason, other blocks compaction is consistently failing. A common case is when the compactor is trying to compact a corrupted block for a single tenant: in this case the compaction of blocks for other tenants is still working, but compaction for the affected tenant is blocked by the corrupted block.

How to **investigate**:

- Look for any error in the compactor logs

  - Corruption: [`not healthy index found`](#compactor-is-failing-because-of-not-healthy-index-found)
  - Invalid result block:
    - **How to detect**: Search compactor logs for `invalid result block`.
    - **What it means**: The compactor successfully validated the source blocks. But the validation of the result block after the compaction did not succeed. The result block was not uploaded and the compaction job will be retried.
  - Out-of-order chunks:
    - **How to detect**: Search compactor logs for `invalid result block` and `out-of-order chunks`.
    - This is caused by a bug in the ingester - see [mimir#1537](https://github.com/grafana/mimir/issues/1537). Ingesters upload blocks where the MinT and MaxT of some chunks don't match the first and last samples in the chunk. When the faulty chunks' MinT and MaxT overlap with other chunks, the compactor merges the chunks. Because one chunk's MinT and MaxT are incorrect the merge may be performed incorrectly, leading to OoO samples.
    - **How to mitigate**: Mark the faulty blocks to avoid compacting them in the future:
      - Find all affected compaction groups in the compactor logs. You will find them as `invalid result block /data/compact/<compaction_group>/<result_block>`.
      - For each failed compaction job
        - Pick one result block (doesn't matter which)
        - Find source blocks for the compaction job: search for `msg="compact blocks"` and a mention of the result block ID.
        - Mark the source blocks for no compaction (in this example the object storage backend is GCS):
          ```
          ./tools/markblocks/markblocks -backend gcs -gcs.bucket-name <bucket> -mark no-compact -tenant <tenant-id> -details "Leading to out-of-order chunks when compacting with other blocks" <block-1> <block-2>...
          ```
  - Result block exceeds symbol table maximum size:
    - **How to detect**: Search compactor logs for `symbol table size exceeds`.
    - **What it means**: The compactor successfully validated the source blocks. But the resulting block is impossible to write due to the error above.
    - This is caused by too many series being stored in the blocks, which indicates that `-compactor.split-and-merge-shards` is too low for the tenant. Could be also an indication of very high churn in labels causing label cardinality explosion.
    - **How to mitigate**: These blocks are not possible to compact, mark the source blocks indicated in the error message with `no-compact`.
      - Find all affected source blocks in the compactor logs by searching for `symbol table size exceeds`.
      - The log lines contain the block IDs in a list of paths, such as:
        ```
        [/data/compact/0@17241709254077376921-merge-3_of_4-1683244800000-1683331200000/01GZS91PMTAWAWAKRYQVNV1FPP /data/compact/0@17241709254077376921-merge-3_of_4-1683244800000-1683331200000/01GZSC5803FN1V1ZFY6Q8PWV1E]
        ```
        Where the filenames are the block IDs: `01GZS91PMTAWAWAKRYQVNV1FPP` and `01GZSC5803FN1V1ZFY6Q8PWV1E`
      - Mark the source blocks for no compaction (in this example the object storage backend is GCS):
        ```
        ./tools/markblocks/markblocks -backend gcs -gcs.bucket-name <bucket> -mark no-compact -tenant <tenant-id> -details "Result block exceeds symbol table maximum size" <block-1> <block-2>...
        ```
    - Further reading: [Compaction algorithm]({{< relref "../../references/architecture/components/compactor#compaction-algorithm" >}}).
  - Compactor network disk unresponsive:
    - **How to detect**: A telltale sign is having many cores of sustained kernel-mode CPU usage by the compactor process. Check the metric `rate(container_cpu_system_seconds_total{pod="<pod>"}[$__rate_interval])` for the affected pod.
    - **What it means**: The compactor process has frozen because it's blocked on kernel-mode flushes to an unresponsive network block storage device.
    - **How to mitigate**: Unknown. This typically self-resolves after ten to twenty minutes.

- Check the [Compactor Dashboard]({{< relref "../monitor-grafana-mimir/dashboards/compactor" >}}) and set it to view the last 7 days.

  - Compactor has fallen behind:
    - **How to detect**:
      - Check the `Last successful run per-compactor replica` panel - are there recent runs in the last 6-12 hours?
      - Also check the `Average blocks / tenant` panel - what is the trend? A tenant should not have a steadily increasing number of blocks. A pattern of growth followed by compaction is normal. Total block counts can also be examined but these depend on the age of the tenants in the cluster and sharding settings. Values from <1200 blocks upward could be normal. 50K blocks would generally not be normal.
    - **What it means**: Compaction likely was failing for some reason in the past and now there is too much work to catch up at the current configuration and scaling level. This can also result in long-term queries failing as the store-gateways fail to handle the much larger number of smaller blocks than expected.
    - **How to mitigate**: Reconfigure and modify the compactor settings and resources for more scalability:
      - Ensure your compactors are at least sized according to the [Planning capacity]({{< relref "../run-production-environment/planning-capacity#compactor" >}}) page and you have the recommended number of replicas.
      - Set `-compactor.split-groups` and `-compactor.split-and-merge-shards` to a value that is 1 for every 8M active series you have - rounded to the closest even number. So, if you have 100M series - `100/8 = 12.5` = value of `12`.
      - Allow the compactor to run for some hours and see if the runs begin to succeed and the `Average blocks / tenant` starts to decrease.
      - If you encounter any Compactor resource issues, add CPU/Memory as needed temporarily, then scale back later.
      - You can also optionally scale replicas and shards further to split the work up into even smaller pieces until the situation has recovered.

### MimirCompactorHasRunOutOfDiskSpace

This alert fires when the compactor has run out of disk space at least once.
When this happens the compaction will fail and after some time the compactor will retry the failed compaction.
It's very likely that on each retry of the job, the compactor will just hit the same disk space limit again and it won't be able to recover on its own.
Alternatively, if compactor concurrency is higher than 1, it could have been just an unlucky combination of jobs that caused compactor to run out of disk space.

How to **investigate**:

- Look at the disk space usage in the compactor's data volumes.
- Look for an error with the string `no space left on device` to confirm that the compactor ran out of disk space.

How to **fix** it:

- The only long-term solution is to give the compactor more disk space, as it requires more space to fit the largest single job into its disk.
- If the number of blocks that the compactor is failing to compact is not very significant and you want to skip compacting them and focus on more recent blocks instead, consider marking the affected blocks for no compaction:
  ```
  ./tools/markblocks/markblocks -backend gcs -gcs.bucket-name <bucket> -mark no-compact -tenant <tenant-id> -details "focus on newer blocks"
  ```

### MimirCompactorSkippedUnhealthyBlocks

This alert fires when compactor tries to compact a block, but finds that given block is unhealthy. This indicates a bug in Prometheus TSDB library and should be investigated.

#### Compactor is failing because of `not healthy index found`

The compactor may fail to compact blocks due to a corrupted block index found in one of the source blocks:

```
level=error ts=2020-07-12T17:35:05.516823471Z caller=compactor.go:339 component=compactor msg="failed to compact user blocks" user=REDACTED-TENANT err="compaction: group 0@6672437747845546250: block with not healthy index found /data/compact/0@6672437747845546250/REDACTED-BLOCK; Compaction level 1; Labels: map[__org_id__:REDACTED]: 1/1183085 series have an average of 1.000 out-of-order chunks: 0.000 of these are exact duplicates (in terms of data and time range)"
```

When this happens, the affected block(s) will be marked as non-compact by the compactor in order to prevent the next execution from being blocked, which could potentially have a negative impact on the performance of the read path.

If the corruption affects only 1 block whose compaction `level` is 1 (the information is stored inside its `meta.json`) then Mimir guarantees no data loss because all the data is replicated across other blocks. In all other cases, there may be some data loss.

Once this alert has been triggered, it is recommended to follow the following steps:

1. Ensure the compactor has recovered.
2. Investigate offline the root cause by downloading the corrupted block and debugging it locally

To download a block stored on GCS you can use the `gsutil` CLI command:

```
gsutil cp gs://[BUCKET_NAME]/[OBJECT_NAME] [LOCAL_DESTINATION]
```

Where:

- `BUCKET` is the gcs bucket name the compactor is using. The cluster's bucket name is specified as the `blocks_storage_bucket_name` in the cluster configuration
- `TENANT` is the tenant id reported in the example error message above as `REDACTED-TENANT`
- `BLOCK` is the last part of the file path reported as `REDACTED-BLOCK` in the example error message above

### MimirBucketIndexNotUpdated

This alert fires when the bucket index, for a given tenant, is not updated since a long time. The bucket index is expected to be periodically updated by the compactor and is used by queriers and store-gateways to get an almost-updated view over the bucket store.

How to **investigate**:

- Ensure the compactor is successfully running
- Look for any error in the compactor logs
- Check how long compactor cleanup tasks have been failing for
  ```
  sum(rate(cortex_compactor_block_cleanup_failed_total{namespace="<namespace>"}[$__rate_interval]))
  ```
- Check for object storage failures for the compactor
  ```
  sum(rate(thanos_objstore_bucket_operation_failures_total{namespace="<namespace>", component="compactor"}[$__rate_interval]))
  ```

How to **fix** it:

- Temporarily increase the tolerance for stale bucket indexes on queriers:
  ```
  -blocks-storage.bucket-store.bucket-index.max-stale-period=2h
  ```
- Temporarily increase the frequency at which compactors perform cleanup tasks like updating bucket indexes:
  ```
  -compactor.cleanup-interval=5m
  ```

### MimirInconsistentRuntimeConfig

This alert fires if multiple replicas of the same Mimir service are using a different runtime config for a longer period of time.

The Mimir runtime config is a config file which gets live reloaded by Mimir at runtime. In order for Mimir to work properly, the loaded config is expected to be the exact same across multiple replicas of the same Mimir service (eg. distributors, ingesters, ...). When the config changes, there may be short periods of time during which some replicas have loaded the new config and others are still running on the previous one, but it shouldn't last for more than few minutes.

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

### MimirBadRuntimeConfig

This alert fires if Mimir is unable to reload the runtime config.

This typically means an invalid runtime config was deployed. Mimir keeps running with the previous (valid) version of the runtime config; running Mimir replicas and the system availability shouldn't be affected, but new replicas won't be able to startup until the runtime config is fixed.

How to **investigate**:

- Check the latest runtime config update (it's likely to be broken)
- Check Mimir logs to get more details about what's wrong with the config

### MimirFrontendQueriesStuck

This alert fires if Mimir is running without query-scheduler and queries are piling up in the query-frontend queue.

The procedure to investigate it is the same as the one for [`MimirSchedulerQueriesStuck`](#MimirSchedulerQueriesStuck): please see the other runbook for more details.

### MimirSchedulerQueriesStuck

This alert fires if queries are piling up in the query-scheduler.

The size of the queue is shown on the `Queue length` dashboard panel on the `Mimir / Reads` (for the standard query path) or `Mimir / Remote Ruler Reads`
(for the dedicated rule evaluation query path) dashboards.

How it **works**:

- A query-frontend API endpoint is called to execute a query
- The query-frontend enqueues the request to the query-scheduler
- The query-scheduler is responsible for dispatching enqueued queries to idle querier workers
- The querier runs the query, sends the response back directly to the query-frontend and notifies the query-scheduler that it can process another query

How to **investigate**:

- Are queriers in a crash loop (eg. OOMKilled)?
  - `OOMKilled`: temporarily increase queriers memory request/limit
  - `panic`: look for the stack trace in the logs and investigate from there
  - if queriers run with activity tracker enabled, they may log `unfinished activities` message on startup with queries that possibly caused the crash.
- Is QPS increased?
  - Scale up queriers to satisfy the increased workload
- Is query latency increased?
  - An increased latency reduces the number of queries we can run / sec: once all workers are busy, new queries will pile up in the queue
  - Temporarily scale up queriers to try to stop the bleed
  - Check if a specific tenant is running heavy queries
    - Run `sum by (user) (cortex_query_scheduler_queue_length{namespace="<namespace>"}) > 0` to find tenants with enqueued queries
    - If remote ruler evaluation is enabled, make sure you understand which one of the read paths (user or ruler queries?) is being affected - check the alert message.
    - Check the `Mimir / Slow Queries` dashboard to find slow queries
  - On multi-tenant Mimir cluster with **shuffle-sharing for queriers disabled**, you may consider to enable it for that specific tenant to reduce its blast radius. To enable queriers shuffle-sharding for a single tenant you need to set the `max_queriers_per_tenant` limit override for the specific tenant (the value should be set to the number of queriers assigned to the tenant).
  - On multi-tenant Mimir cluster with **shuffle-sharding for queriers enabled**, you may consider to temporarily increase the shard size for affected tenants: be aware that this could affect other tenants too, reducing resources available to run other tenant queries. Alternatively, you may choose to do nothing and let Mimir return errors for that given user once the per-tenant queue is full.
  - On multi-tenant Mimir clusters with **query-sharding enabled** and **more than a few tenants** being affected: The workload exceeds the available downstream capacity. Scaling of queriers and potentially store-gateways should be considered.
  - On multi-tenant Mimir clusters with **query-sharding enabled** and **only a single tenant** being affected:
    - Verify if the particular queries are hitting edge cases, where query-sharding is not benefical, by getting traces from the `Mimir / Slow Queries` dashboard and then look where time is spent. If time is spent in the query-frontend running PromQL engine, then it means query-sharding is not beneficial for this tenant. Consider disabling query-sharding or reduce the shard count using the `query_sharding_total_shards` override.
    - Otherwise and only if the queries by the tenant are within reason representing normal usage, consider scaling of queriers and potentially store-gateways.
  - On a Mimir cluster with **querier auto-scaling enabled** after checking the health of the existing querier replicas, check to see if the auto-scaler has added additional querier replicas or if the maximum number of querier replicas has been reached and is not sufficient and should be increased.

### MimirCacheRequestErrors

This alert fires if the Mimir cache client is experiencing a high error rate for a specific cache and operation.

How to **investigate**:

- The alert reports which cache is experiencing issue
  - `metadata-cache`: object store metadata cache
  - `index-cache`: TSDB index cache
  - `chunks-cache`: TSDB chunks cache
- Check which specific error is occurring
  - Run the following query to find out the reason (replace `<namespace>` with the actual Mimir cluster namespace)
    ```
    sum by(name, operation, reason) (rate(thanos_cache_operation_failures_total{namespace="<namespace>"}[1m])) > 0
    ```
- Based on the **`reason`**:
  - `timeout`
    - Scale up the cache replicas
  - `server-error`
    - Check both Mimir and cache logs to find more details
  - `network-error`
    - Check Mimir logs to find more details
  - `malformed-key`
    - The key is too long or contains invalid characters
    - Check Mimir logs to find the offending key
    - Fixing this will require changes to the application code
  - `other`
    - Check both Mimir and cache logs to find more details

### MimirProvisioningTooManyWrites

This alert fires if the average number of samples ingested / sec in ingesters is above our target.

How to **fix** it:

- Scale up ingesters
  - To compute the desired number of ingesters to satisfy the average samples rate you can run the following query, replacing `<namespace>` with the namespace to analyse and `<target>` with the target number of samples/sec per ingester (check out the alert threshold to see the current target):
    ```
    sum(rate(cortex_ingester_ingested_samples_total{namespace="<namespace>"}[$__rate_interval])) / (<target> * 0.9)
    ```

### MimirAllocatingTooMuchMemory

This alert fires when ingester memory utilization is getting too close to the limit.

How it **works**:

- Mimir ingesters are stateful services
- Having 2+ ingesters `OOMKilled` might cause a cluster outage
- Ingester memory baseline usage is primarily influenced by memory allocated by the process (mostly Go heap) and mmap-ed files (used by TSDB)
- Ingester memory short spikes are primarily influenced by queries and TSDB head compaction into new blocks (occurring every 2h)
- A pod gets `OOMKilled` once its working set memory reaches the configured limit, so it's important to prevent ingesters' memory utilization (working set memory) from getting close to the limit (we need to keep at least 30% room for spikes due to queries)

How to **fix** it:

- Check if the issue occurs only for few ingesters. If so:
  - Restart affected ingesters 1 by 1 (proceed with the next one once the previous pod has restarted and it's Ready)
    ```
    kubectl --namespace <namespace> delete pod ingester-XXX
    ```
  - Restarting an ingester typically reduces the memory allocated by mmap-ed files. After the restart, ingester may allocate this memory again over time, but it may give more time while working on a longer term solution
- Check the `Mimir / Writes Resources` dashboard to see if the number of series per ingester is above the target (1.5M). If so:
  - Scale up ingesters; you can use e.g. the `Mimir / Scaling` dashboard for reference, in order to determine the needed amount of ingesters (also keep in mind that each ingester should handle ~1.5 million series, and the series will be duplicated across three instances)
  - Memory is expected to be reclaimed at the next TSDB head compaction (occurring every 2h)

### MimirGossipMembersTooHigh

This alert fires when any instance registers too many instances as members of the memberlist cluster.

How it **works**:

- This alert applies when memberlist is used as KV store for hash rings.
- All Mimir instances using the ring, regardless of type, join a single memberlist cluster.
- Each instance (ie. memberlist cluster member) should see all memberlist cluster members, but not see any other instances (eg. from Loki or Tempo, or other Mimir clusters).
- Therefore the following should be equal for every instance:
  - The reported number of cluster members (`memberlist_client_cluster_members_count`)
  - The total number of currently responsive instances that use memberlist KV store for hash ring.
- During rollouts, the number of members reported by some instances may be higher than expected as it takes some time for notifications of instances that have shut down
  to propagate throughout the cluster.

How to **investigate**:

- Check which instances are reporting a higher than expected number of cluster members (the `memberlist_client_cluster_members_count` metric)
- If most or all instances are reporting a higher than expected number of cluster members, then this cluster may have merged with another cluster
  - Check the instances listed on each instance's view of the memberlist cluster using the `/memberlist` admin page on that instance, and confirm that all instances listed there are expected
- If only a small number of instances are reporting a higher than expected number of cluster members, these instances may be experiencing memberlist communication issues:
  - Verify communication with other members by checking memberlist traffic is being sent and received by the instance using the following metrics:
    - `memberlist_tcp_transport_packets_received_total`
    - `memberlist_tcp_transport_packets_sent_total`
  - If traffic is present, then verify there are no errors sending or receiving packets using the following metrics:
    - `memberlist_tcp_transport_packets_sent_errors_total`
    - `memberlist_tcp_transport_packets_received_errors_total`
    - These errors (and others) can be found by searching for messages prefixed with `TCPTransport:`.
- Logs coming directly from memberlist are also logged by Mimir; they may indicate where to investigate further. These can be identified as such due to being tagged with `caller=memberlist_logger.go:<line>`.

### MimirGossipMembersTooLow

This alert fires when any instance registers too few instances as members of the memberlist cluster.

How it **works**:

- This alert applies when memberlist is used as KV store for hash rings.
- All Mimir instances using the ring, regardless of type, join a single memberlist cluster.
- Each instance (ie. memberlist cluster member) should see all memberlist cluster members.
- Therefore the following should be equal for every instance:
  - The reported number of cluster members (`memberlist_client_cluster_members_count`)
  - The total number of currently responsive instances that use memberlist KV store for hash ring.

How to **investigate**:

- Check which instances are reporting a lower than expected number of cluster members (the `memberlist_client_cluster_members_count` metric)
- If most or all instances are reporting a lower than expected number of cluster members, then there may be a configuration issue preventing cluster members from finding each other
  - Check the instances listed on each instance's view of the memberlist cluster using the `/memberlist` admin page on that instance, and confirm that all expected instances are listed there
- If only a small number of instances are reporting a lower than expected number of cluster members, these instances may be experiencing memberlist communication issues:
  - Verify communication with other members by checking memberlist traffic is being sent and received by the instance using the following metrics:
    - `memberlist_tcp_transport_packets_received_total`
    - `memberlist_tcp_transport_packets_sent_total`
  - If traffic is present, then verify there are no errors sending or receiving packets using the following metrics:
    - `memberlist_tcp_transport_packets_sent_errors_total`
    - `memberlist_tcp_transport_packets_received_errors_total`
    - These errors (and others) can be found by searching for messages prefixed with `TCPTransport:`.
- Logs coming directly from memberlist are also logged by Mimir; they may indicate where to investigate further. These can be identified as such due to being tagged with `caller=memberlist_logger.go:<line>`.

### MimirGossipMembersEndpointsOutOfSync

This alert fires when the list of endpoints returned by the `gossip-ring` service is out-of-sync.

How it **works**:

- The Kubernetes service `gossip-ring` is used by Mimir to find memberlist seed nodes to join at startup. The service
  DNS returns all Mimir pods by default, which means any Mimir pod can be used as a seed node (this is the safest option).
- Due to Kubernetes bugs (for example, [this one](https://github.com/kubernetes/kubernetes/issues/127370)) the pod IPs
  returned by the service DNS address may go out-of-sync, up to a point where none of the returned IPs belongs to any
  live pod. If that happens, then new Mimir pods can't join memberlist at startup.

How to **investigate**:

- Check the number of endpoints matching the `gossip-ring` service:
  ```
  kubectl --namespace <namespace> get endpoints gossip-ring
  ```
- If the number of endpoints is 1000 then it means you reached the Kubernetes limit, the endpoints get truncated and
  you could be hit by [this bug](https://github.com/kubernetes/kubernetes/issues/127370). Having more than 1000 pods
  matched by the `gossip-ring` service and then getting endpoints truncated to 1000 is not an issue per-se, but it's
  an issue if you're running a version of Kubernetes affected by the mentioned bug.
- If you've been affected by the Kubernetes bug:

  1. Stop the bleed re-creating the service endpoints list

     ```sh
     CONTEXT="TODO"
     NAMESPACE="TODO"
     SERVICE="gossip-ring"

     # Re-apply the list of bad endpoints as is.
     kubectl --context "$CONTEXT" --namespace "$NAMESPACE" get endpoints "$SERVICE" -o yaml > /tmp/service-endpoints.yaml
     kubectl --context "$CONTEXT" --namespace "$NAMESPACE" apply -f /tmp/service-endpoints.yaml

     # Delete a random querier pod to trigger K8S service endpoints reconciliation.
     POD=$(kubectl --context "$CONTEXT" --namespace "$NAMESPACE" get pods -l name=querier --output="jsonpath={.items[0].metadata.name}")
     kubectl --context "$CONTEXT" --namespace "$NAMESPACE" delete pod "$POD"
     ```

  2. Consider removing some deployments from `gossip-ring` selector label, to reduce the number of matching pods below 1000.
     This is a temporarily workaround, and you should revert it once you upgrade Kubernetes to a version with the bug fixed.

     An example of how you can do it with jsonnet:

     ```
     querier_deployment+:
       $.apps.v1.statefulSet.spec.template.metadata.withLabelsMixin({ [$._config.gossip_member_label]: 'false' }),
     ```

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

Note that you may need to recreate each etcd pod in order for this change to take effect, as etcd-operator does not automatically recreate pods in response to changes like these.
First, check that all etcd pods are running and healthy. Then delete one pod at a time and wait for it to be recreated and become healthy before repeating for the next pod until all pods have been recreated.

### MimirAlertmanagerSyncConfigsFailing

How it **works**:

This alert is fired when the multi-tenant alertmanager cannot load alertmanager configs from the remote object store for at least 30 minutes.

Loading the alertmanager configs can happen in the following situations:

1. When the multi tenant alertmanager is started
2. Each time it polls for config changes in the alertmanager
3. When there is a ring change

The metric for this alert is cortex_alertmanager_sync_configs_failed_total and is incremented each time one of the above fails.

When there is a ring change or the interval has elapsed, a failure to load configs from the store is logged as a warning.

How to **investigate**:

Look at the error message that is logged and attempt to understand what is causing the failure. I.e. it could be a networking issue, incorrect configuration for the store, etc.

### MimirAlertmanagerRingCheckFailing

How it **works**:

This alert is fired when the multi-tenant alertmanager has been unable to check if one or more tenants should be owned on this shard for at least 10 minutes.

When the alertmanager loads its configuration on start up, when it polls for config changes or when there is a ring change it must check the ring to see if the tenant is still owned on this shard. To prevent one error from causing the loading of all configurations to fail we assume that on error the tenant is NOT owned for this shard. If checking the ring continues to fail then some tenants might not be assigned an alertmanager and might not be able to receive notifications for their alerts.

The metric for this alert is `cortex_alertmanager_ring_check_errors_total`.

How to **investigate**:

- Look at the error message that is logged and attempt to understand what is causing the failure. In most cases the error will be encountered when attempting to read from the ring, which can fail if there is an issue with in-use backend implementation.
- When using Memberlist as KV store for hash rings, ensure that Memberlist is working correctly. See instructions for the [`MimirGossipMembersTooHigh`](#MimirGossipMembersTooHigh) and [`MimirGossipMembersTooLow`](#MimirGossipMembersTooLow) alerts.

### MimirAlertmanagerPartialStateMergeFailing

How it **works**:

This alert is fired when the multi-tenant alertmanager attempts to merge a partial state for something that it either does not know about or the partial state cannot be merged with the existing local state. State merges are gRPC messages that are gossiped between a shard and the corresponding alertmanager instance in other shards.

The metric for this alert is cortex_alertmanager_partial_state_merges_failed_total.

How to **investigate**:

The error is not currently logged on the receiver side. If this alert is firing, it is likely that `MimirAlertmanagerReplicationFailing` is firing also, so instead follow the investigation steps for that alert, with the assumption that the issue is not RPC/communication related.

### MimirAlertmanagerReplicationFailing

How it **works**:

This alert is fired when the multi-tenant alertmanager attempts to replicate a state update for a tenant (i.e. a silence or a notification) to another alertmanager instance but failed. This could be due to an RPC/communication error or the other alertmanager being unable to merge the state with its own local state.

The metric for this alert is cortex_alertmanager_state_replication_failed_total.

How to **investigate**:

When state replication fails it gets logged as an error in the alertmanager that attempted the state replication. Check the error message in the log to understand the cause of the error (i.e. was it due to an RPC/communication error or was there an error in the receiving alertmanager).

### MimirAlertmanagerPersistStateFailing

How it **works**:

This alert is fired when the multi-tenant alertmanager cannot persist its state to the remote object store. This operation is attempted periodically (every 15m by default).

Each alertmanager writes its state (silences, notification log) to the remote object storage and the cortex_alertmanager_state_persist_failed_total metric is incremented each time this fails. The alert fires if this fails for an hour or more.

How to **investigate**:

Each failure to persist state to the remote object storage is logged. Find the reason in the Alertmanager container logs with the text “failed to persist state”. Possibles reasons:

- The most probable cause is that remote write failed. Try to investigate why based on the message (network issue, storage issue). If the error indicates the issue might be transient, then you can wait until the next periodic attempt and see if it succeeds.
- It is also possible that encoding the state failed. This does not depend on external factors as it is just pulling state from the Alertmanager internal state. It may indicate a bug in the encoding method.

### MimirAlertmanagerInitialSyncFailed

How it **works**:

When a tenant replica becomes owned it is assigned to an alertmanager instance. The alertmanager instance attempts to read the state from other alertmanager instances. If no other alertmanager instances could replicate the full state then it attempts to read the full state from the remote object store. This alert fires when both of these operations fail.

Note that the case where there is no state for this user in remote object storage, is not treated as a failure. This is expected when a new tenant becomes active for the first time.

How to **investigate**:

When an alertmanager cannot read the state for a tenant from storage it gets logged as the following error: "failed to read state from storage; continuing anyway". The possible causes of this error could be:

- The state could not be merged because it might be invalid and could not be decoded. This could indicate data corruption and therefore a bug in the reading or writing of the state, and would need further investigation.
- The state could not be read from storage. This could be due to a networking issue such as a timeout or an authentication and authorization issue with the remote object store.

### MimirAlertmanagerAllocatingTooMuchMemory

This alert fires when alertmanager memory utilization is getting too close to the limit.

How it **works**:

- Mimir alertmanager is an stateful service
- Having 2+ alertmanagers `OOMKilled` might cause service interruption as it needs quorum for API responses. Notification (from alertmanager to third-party) can succeed without quorum.
- Alertmanager memory baseline usage is primarily influenced by memory allocated by the process (mostly Go heap) for alerts and silences.
- A pod gets `OOMKilled` once its working set memory reaches the configured limit, so it's important to prevent alertmanager's memory utilization (working set memory) from going over to the limit. The memory usage is typically sustained and does not suffer from spikes, hence thresholds are set very close to the limit.

How to **fix** it:

- Scale up alertmanager replicas; you can use e.g. the `Mimir / Scaling` dashboard for reference, in order to determine the needed amount of alertmanagers.

### MimirAlertmanagerInstanceHasNoTenants

This alert fires when an alertmanager instance doesn't own any tenants and is therefore idling.

How it **works**:

- Alerts handled by alertmanagers are sharded by tenant.
- When the tenant shard size is lower than the number of alertmanager replicas, some replicas will not own any tenant and therefore idle.
- This is more likely to happen in Mimir clusters with a lower number of tenants.

How to **fix** it:

- Decrease the number of alertmanager replicas

### MimirRolloutStuck

This alert fires when a Mimir service rollout is stuck, which means the number of updated replicas doesn't match the expected one and looks there's no progress in the rollout. The alert monitors services deployed as Kubernetes `StatefulSet` and `Deployment`.

How to **investigate**:

- Run `kubectl --namespace <namespace> get pods --selector='name=<statefulset|deployment>'` to get a list of running pods
- Ensure there's no pod in a failing state (eg. `Error`, `OOMKilled`, `CrashLoopBackOff`)
- Ensure there's no pod `NotReady` (the number of ready containers should match the total number of containers, eg. `1/1` or `2/2`)
- Run `kubectl --namespace <namespace> describe statefulset <name>` or `kubectl --namespace <namespace> describe deployment <name>` and look at "Pod Status" and "Events" to get more information

### MimirKVStoreFailure

This alert fires if a Mimir instance is failing to run any operation on a KV store (eg. consul or etcd).
When using Memberlist as KV store for hash rings, all read and update operations work on a local copy of the hash ring, and will never fail and raise this alert.

How it **works**:

- Consul is typically used to store the hash ring state.
- Etcd is typically used to store by the HA tracker (distributor) to deduplicate samples.
- If an instance is failing operations on the **hash ring**, either the instance can't update the heartbeat in the ring or is failing to receive ring updates.
- If an instance is failing operations on the **HA tracker** backend, either the instance can't update the authoritative replica or is failing to receive updates.

How to **investigate**:

- Ensure Consul/Etcd is up and running.
- Investigate the logs of the affected instance to find the specific error occurring when talking to Consul/Etcd.

### MimirReachingTCPConnectionsLimit

This alert fires if a Mimir instance is configured with `-server.http-conn-limit` or `-server.grpc-conn-limit` and is reaching the limit.

How it **works**:

- A Mimir service could be configured with a limit of the max number of TCP connections accepted simultaneously on the HTTP and/or gRPC port.
- If the limit is reached:
  - New connections acceptance will put on hold or rejected. Exact behaviour depends on backlog parameter to `listen()` call and kernel settings.
  - The **health check endpoint may fail** (eg. timeout).
- The limit is typically set way higher than expected usage, so if limit is reached (or close to be) then it means there's a critical issue.

How to **investigate**:

- Limit reached in `gateway`:
  - Check if it's caused by **high latency on write path**:
    - Check the distributors and ingesters latency in the `Mimir / Writes` dashboard
    - High latency on write path could lead our customers Prometheus / Agent to increase the number of shards nearly at the same time, leading to a significantly higher number of concurrent requests to the load balancer and thus gateway
  - Check if it's caused by a **single tenant**:
    - We don't have a metric tracking the active TCP connections or QPS per tenant
    - As a proxy metric, you can check if the ingestion rate has significantly increased for any tenant (it's not a very accurate proxy metric for number of TCP connections so take it with a grain of salt):
    ```
    topk(10, sum by(user) (rate(cortex_distributor_samples_in_total{namespace="<namespace>"}[$__rate_interval])))
    ```
    - In case you need to quickly reject write path traffic from a single tenant, you can override its `ingestion_rate` and `ingestion_rate_burst` setting lower values (so that some/most of their traffic will be rejected)

### MimirAutoscalerNotActive

This alert fires when any of Mimir's Kubernetes Horizontal Pod Autoscaler's (HPA) `ScalingActive` condition is `false` and the related scaling metrics are not 0.
When this happens, it's not able to calculate desired scale and generally indicates problems with fetching metrics.

How it **works**:

- HPA's can be configured to autoscale Mimir components based on custom metrics fetched from Prometheus via the KEDA custom metrics API server
- HPA periodically queries updated metrics and updates the number of desired replicas based on that
- Refer to [Mimir's Autoscaling documentation]({{< relref "../../set-up/jsonnet/configure-autoscaling" >}}) and the upstream [HPA documentation](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) for more information.

How to **investigate**:

- Check HPA conditions and events to get more details about the failure
  ```
  kubectl describe hpa --namespace <namespace> keda-hpa-$component
  ```
- Ensure KEDA pods are up and running
  ```
  # Assuming KEDA is running in a dedicated namespace "keda":
  kubectl get pods --namespace keda
  ```
- Check KEDA custom metrics API server logs
  ```
  # Assuming KEDA is running in a dedicated namespace "keda":
  kubectl logs --namespace keda deployment/keda-operator-metrics-apiserver
  ```
- Check KEDA operator logs
  ```
  # Assuming KEDA is running in a dedicated namespace "keda":
  kubectl logs --namespace keda deployment/keda-operator
  ```
- Check that Prometheus is running (since we configure KEDA to scrape custom metrics from it by default)
  ```
  # Assuming Prometheus is running in namespace "default":
  kubectl --namespace default get pod --selector='name=prometheus'
  ```

For scaled objects with 0 `minReplicas` it is expected for HPA to be inactive when the scaling metric exposed in `keda_scaler_metrics_value` is 0.
When `keda_scaler_metrics_value` value is 0 or missing, the alert should not be firing.

### MimirAutoscalerKedaFailing

This alert fires when KEDA is reporting errors for any ScaledObject defined in the same Kubernetes namespace where Mimir is deployed.

How it **works**:

- _See [`MimirAutoscalerNotActive`](#MimirAutoscalerNotActive)_

How to **investigate**:

- Check KEDA custom metrics API server logs
  ```
  # Assuming KEDA is running in a dedicated namespace "keda":
  kubectl logs --namespace keda deployment/keda-operator-metrics-apiserver
  ```
- Check KEDA operator logs
  ```
  # Assuming KEDA is running in a dedicated namespace "keda":
  kubectl logs --namespace keda deployment/keda-operator
  ```
- Check that Prometheus is running (since we configure KEDA to scrape custom metrics from it by default)
  ```
  # Assuming Prometheus is running in namespace "default":
  kubectl --namespace default get pod --selector='name=prometheus'
  ```

### MimirContinuousTestNotRunningOnWrites

This alert fires when `mimir-continuous-test` is deployed in the Mimir cluster, and continuous testing is not effectively running because writes are failing.

How it **works**:

- `mimir-continuous-test` is an optional testing tool that can be deployed in the Mimir cluster
- The tool runs some tests against the Mimir cluster itself at regular intervals
- This alert fires if the tool is unable to properly run the tests, and not if the tool assertions don't match the expected results

How to **investigate**:

- Check continuous test logs to find out more details about the failure:
  ```
  kubectl logs --namespace <namespace> deployment/continuous-test
  ```

### MimirContinuousTestNotRunningOnReads

This alert is like [`MimirContinuousTestNotRunningOnWrites`](#MimirContinuousTestNotRunningOnWrites) but it fires when queries are failing.

### MimirContinuousTestFailed

This alert fires when `mimir-continuous-test` is deployed in the Mimir cluster, and continuous testing tool's assertions don't match the expected results.
When this alert fires there could be a bug in Mimir that should be investigated as soon as possible.

How it **works**:

- `mimir-continuous-test` is an optional testing tool that can be deployed in the Mimir cluster
- The tool runs some tests against the Mimir cluster itself at regular intervals
- This alert fires if the tool assertions don't match the expected results

How to **investigate**:

- Check continuous test logs to find out more details about the failed assertions:
  ```
  kubectl logs --namespace <namespace> deployment/continuous-test
  ```
- Check if query result comparison is failing
  - Is query failing both when results cache is enabled and when it's disabled?
- This alert should always be actionable. There are two possible outcomes:
  1. The alert fired because of a bug in Mimir: fix it.
  1. The alert fired because of a bug or edge case in the continuous test tool, causing a false positive: fix it.

### MimirRingMembersMismatch

This alert fires when the number of ring members does not match the number of running replicas.

How it **works**:

- The alert compares each component (currently just `ingester`) against the number of `up` instances for the component in that cluster.

How to **investigate**:

- Check the [hash ring web page]({{< relref "../../references/http-api#ingesters-ring-status" >}}) for the component for which the alert has fired, and look for unexpected instances in the list.
- Consider manually forgetting unexpected instances in an `Unhealthy` state.
- Ensure all the registered instances in the ring belong to the Mimir cluster for which the alert fired.

### RolloutOperatorNotReconciling

This alert fires if the [`rollout-operator`](https://github.com/grafana/rollout-operator) is not successfully reconciling in a namespace.

How it **works**:

- The rollout-operator coordinates the rollout of pods between different StatefulSets within a specific namespace and is used to manage multi-zone deployments
- The rollout-operator is deployed in namespaces where some Mimir components (e.g. ingesters) are deployed in multi-zone
- The rollout-operator reconciles as soon as there's any change in observed Kubernetes resources or every 5m at most

How to **investigate**:

- Check rollout-operator logs to find more details about the error, e.g. with the following Grafana Loki query:
  ```
  {name="rollout-operator",namespace="<namespace>"}
  ```

### MimirIngestedDataTooFarInTheFuture

This alert fires when Mimir ingester accepts a sample with timestamp that is too far in the future.
This is typically a result of processing of corrupted message, and it can cause rejection of other samples with timestamp close to "now" (real-world time).

How it **works**:

- The metric exported by ingester computes maximum timestamp from all TSDBs open in ingester.
- Alert checks this exported metric and fires if maximum timestamp is more than 1h in the future.

How to **investigate**

- Find the tenant with bad sample on ingester's tenants list, where a warning "TSDB Head max timestamp too far in the future" is displayed.
- Flush tenant's data to blocks storage.
- Remove tenant's directory on disk and restart ingester.

### MimirStoreGatewayTooManyFailedOperations

How it **works**:

- This alert fires when the `store-gateways` report errors when interacting with the object storage for an extended period of time.
- This is usually because Mimir cannot read an object due to an issue with the object itself or the object storage.

How to **investigate**

- Check the `store-gateways` logs which should contain details about the error such as tenant or object id, e.g. with the following Grafana Loki query:

```
{cluster="<cluster>",namespace="<namespace>", name=~"store-gateway.*"} |= "level=warn"
```

You might find logs similar to the following:

```
create index header reader: write index header: new index reader: get TOC from object storage of 01H9QMTQRE2MT8XVDWP6RWAMC6/index: Multipart upload has broken segment data.
```

- Use the `Mimir / Object Store` dashboard to check for error rate and the failed object storage's operation impacted, e.g: `get_range`.

### KubePersistentVolumeFillingUp

This alert is not defined in the Mimir mixin, but it's part of [`kube-prometheus`](https://github.com/prometheus-operator/kube-prometheus) alerts.
This alert fires when a `PersistentVolume` is nearing capacity.

#### Compactor

How it **works**:

- The compactor uses the volume to temporarily store blocks to compact. The compactor doesn't require persistence, so it's safe to stop the compactor, delete the volume content and restart it with an empty disk.
- The compactor disk utilization is typically a function of the size of source blocks to compact as part of a compaction job and the configured number of maximum concurrent compactions (`-compactor.compaction-concurrency`).

How to **fix** it:

- Increase the compactor volume size to stop the bleed. You can either:
  - Resize the volume
  - Delete the compactor StatefulSet and its PersistentVolumeClaims, then re-create the compactor StatefulSet with a bigger volume size request
- Check if the compactor is configured with `-compactor.compaction-concurrency` greater than 1 and there are multiple concurrent compactions running in the affected compactor. If so, you can consider lowering the concurrency.

#### Store-gateway

How it **works**:

- Blocks in the long-term storage are sharded and replicated between store-gateway replicas using the store-gateway hash ring. This means that each store-gateway owns a subset of the blocks.
  - The sharding algorithm is designed to try to evenly balance the number of blocks per store-gateway replica, but not their size. This means that in case of a tenant with uneven blocks sizes, some store-gateways may use more disk than others even if the number of blocks assigned to each replicas are perfectly balanced.
  - The sharding algorithm can achieve a fair balance of the number of blocks between store-gateway replicas only on a large number of blocks. This means that in case of a Mimir cluster with a small number of blocks, these may not be evenly balanced between replicas. Currently, a perfect (or even very good) balance between store-gateway replicas is nearly impossible to achieve.
  - When store-gateway shuffle sharding is in use for a given tenant and the tenant's shard size is smaller than the number of store-gateway replicas, the tenant's blocks are sharded only across a subset of replicas. Shuffle sharding can cause an imbalance in store-gateway disk utilization.
- The store-gateway uses the volume to store the [index-header]({{< relref "../../references/architecture/binary-index-header.md" >}}) of each owned block.

How to **investigate** and **fix** it:

- Check the `Mimir / Compactor` dashboard

  - Ensure the compactor is healthy and running successfully.
    - The "Last successful run per-compactor replica" panel should show all compactors are running Ok and none of them having Delayed, Late or Very Late status.
    - "Tenants with largest number of blocks" must not be trending upwards
  - An issue in the compactor (e.g. compactor is crashing, OOMKilled or can't catch up with compaction jobs) would cause the number of non-compacted blocks to increase, causing an increased disk utilization in the store-gateway. In case of an issue with the compactor you should fix it first:
    - If the compactor is OOMKilled, increase compactor memory request.
    - If the compactor is lagging behind or there are many blocks to compactor, temporarily increase increase the compactor replicas to let the compactor catching up quickly.

- Check the `Mimir / Reads resources` dashboard

  - Check if disk utilization is nearly balanced between store-gateway replicas (e.g. a 20-30% variance between replicas is expected)
    - If disk utilization is nearly balanced you can scale out store-gateway replicas to lower disk utilization on average
    - If disk utilization is unbalanced you may consider the other options before scaling out store-gateways

- Check if disk utilization unbalance is caused by shuffle sharding

  - Investigate which tenants use most of the store-gateway disk in the replicas with highest disk utilization. To investigate it you can run the following command for a given store-gateway replica. The command returns the top 10 tenants by disk utilization (in megabytes):
    ```
    kubectl --context $CLUSTER --namespace $CELL exec -ti $POD -- sh -c 'du -sm /data/tsdb/* | sort -n -r | head -10'
    ```
  - Check the configured `-store-gateway.tenant-shard-size` (`store_gateway_tenant_shard_size`) of each tenant that mostly contributes to disk utilization. Consider increase the tenant's the shard size if it's smaller than the number of available store-gateway replicas (a value of `0` disables shuffle sharding for the tenant, effectively sharding their blocks across all replicas).

- Check if disk utilization unbalance is caused by a tenant with uneven block sizes
  - Even if a tenant has no shuffle sharding and their blocks are sharded across all replicas, it may still cause unbalance in store-gateway disk utilization if the size of their blocks dramatically changed over time (e.g. because the number of series per block significantly changed over time). As a proxy metric, the number of series per block is roughly the total number of series across all blocks for the largest `-compactor.block-ranges` (default is 24h) divided by the number of `-compactor.split-and-merge-shards` (`compactor_split_and_merge_shards`).
  - If you suspect this may be an issue:
    - Check the number of series in each block in the store-gateway blocks list for the affected tenant, through the web page exposed by the store-gateway at `/store-gateway/tenant/<tenant ID>/blocks`
    - Check the number of in-memory series shown on the `Mimir / Tenants` dashboard for an approximation of the number of series that will be compacted once these blocks are shipped from ingesters.
    - Check the configured `compactor_split_and_merge_shards` for the tenant. A reasonable rule of thumb is 8-10 million series per compactor shard - if the number of series per shard is above this range, increase `compactor_split_and_merge_shards` for the affected tenant(s) accordingly.

## Mimir ingest storage (experimental)

This section contains runbooks for alerts related to experimental Mimir ingest storage.
In this context, any reference to Kafka means a Kafka protocol-compatible backend.

### MimirIngesterLastConsumedOffsetCommitFailed

This alert fires when an ingester is failing to commit the last consumed offset to the Kafka backend.

How it **works**:

- The ingester ingests data (metrics, exemplars, ...) from Kafka and periodically commits the last consumed offset back to Kafka.
- At startup, an ingester reads the last consumed offset committed to Kafka and resumes the consumption from there.
- If the ingester fails to commit the last consumed offset to Kafka, the ingester keeps working correctly from the consumption perspective (assuming there's no other on-going issue in the cluster) but in case of a restart the ingester will resume the consumption from the last successfully committed offset. If the last offset was successfully committed several minutes ago, the ingester will re-ingest data which has already been ingested, potentially causing OOO errors, wasting resources and taking longer to startup.

How to **investigate**:

- Check ingester logs to find details about the error.
- Check Kafka logs and health.

### MimirIngesterFailedToReadRecordsFromKafka

This alert fires when an ingester is failing to read records from Kafka backend.

How it **works**:

- Ingester connects to Kafka brokers and reads records from it. Records contain write requests committed by distributors.
- When ingester fails to read more records from Kafka due to error, ingester logs such error.
- This can be normal if Kafka brokers are restarting, however if read errors continue for some time, alert is raised.

How to **investigate**:

- Check ingester logs to find details about the error.
- Check Kafka logs and health.

### MimirIngesterKafkaFetchErrorsRateTooHigh

This alert fires when an ingester is receiving errors instead of "fetches" from Kafka.

How it **works**:

- Ingester uses Kafka client to read records (containing write requests) from Kafka.
- Kafka client can return errors instead of more records.
- If rate of returned errors compared to returned records is too high, alert is raised.
- Kafka client can return errors [documented in the source code](https://github.com/grafana/mimir/blob/24591ae56cd7d6ef24a7cc1541a41405676773f4/vendor/github.com/twmb/franz-go/pkg/kgo/record_and_fetch.go#L332-L366).

How to **investigate**:

- Check ingester logs to find details about the error.
- Check Kafka logs and health.

### MimirStartingIngesterKafkaReceiveDelayIncreasing

This alert fires when "receive delay" reported by ingester during "starting" phase is not decreasing.

How it **works**:

- When an ingester starts, it needs to fetch and process records from Kafka until a preconfigured consumption lag is honored. There are two configuration options that control the lag before an ingester is considered to have caught up reading from a partition at startup:
  - `-ingest-storage.kafka.max-consumer-lag-at-startup`: this is the guaranteed maximum lag before an ingester is considered to have caught up. The ingester doesn't become ACTIVE in the hash ring and doesn't pass the readiness check until the measured lag is below this setting.
  - `-ingest-storage.kafka.target-consumer-lag-at-startup`: this is the desired maximum lag that an ingester sets to achieve at startup. This setting is a best-effort. The ingester is granted a "grace period" to have the measured lag below this setting. However, the ingester still starts if the target lag hasn't been reached within this "grace period", as long as the max lag is honored. The "grace period" is equal to the configured `-ingest-storage.kafka.max-consumer-lag-at-startup`.
- Each record has a timestamp when it was sent to Kafka by the distributor. When ingester reads the record, it computes "receive delay" as a difference between current time (when record was read) and time when record was sent to Kafka. This receive delay is reported in the metric `cortex_ingest_storage_reader_receive_delay_seconds`. You can see receive delay on `Mimir / Writes` dashboard, in section "Ingester (ingest storage – end-to-end latency)".
- Under normal conditions when ingester is processing records faster than records are appearing, receive delay should be decreasing, until `-ingest-storage.kafka.max-consumer-lag-at-startup` is honored.
- When ingester is starting, and observed "receive delay" is increasing, alert is raised.

How to **investigate**:

- Check if ingester is fast enough to process all data in Kafka.

See also "[Ingester is overloaded when consuming from Kafka](#ingester-is-overloaded-when-consuming-from-kafka)".

### MimirRunningIngesterReceiveDelayTooHigh

This alert fires when "receive delay" reported by ingester while it's running reaches alert threshold.

How it **works**:

- After ingester start and catches up with records in Kafka, ingester switches to "running" mode.
- In running mode, ingester continues to process incoming records from Kafka and continues to report "receive delay". See [`MimirStartingIngesterKafkaReceiveDelayIncreasing`](#MimirStartingIngesterKafkaReceiveDelayIncreasing) runbook for details about this metric.
- Under normal conditions when ingester is running and it is processing records faster than records are appearing, receive delay should be stable and low.
- If observed "receive delay" increases and reaches certain threshold, alert is raised.

How to **investigate**:

- Check if ingester is fast enough to process all data in Kafka.
- If ingesters are too slow, consider scaling ingesters horizontally to spread incoming series between more ingesters.

See also "[Ingester is overloaded when consuming from Kafka](#ingester-is-overloaded-when-consuming-from-kafka)".

### MimirIngesterFailsToProcessRecordsFromKafka

This alert fires when ingester is unable to process incoming records from Kafka due to internal errors. If ingest-storage wasn't used, such push requests would end up with 5xx errors.

How it **works**:

- Ingester reads records from Kafka, and processes them locally. Processing means unmarshalling the data and handling write requests stored in records.
- Write requests can fail due to "client" or "server" errors. An example of client error is too low limit for number of series. Server error can be for example ingester hitting an instance limit.
- If requests keep failing due to server errors, this alert is raised.

How to **investigate**:

- Check ingester logs to see why requests are failing, and troubleshoot based on that.

### MimirIngesterStuckProcessingRecordsFromKafka

This alert fires when an ingester has successfully fetched records from Kafka but it's not processing them at all.

How it **works**:

- Ingester reads records from Kafka, and processes them locally. Processing means unmarshalling the data and handling write requests stored in records.
- Fetched records, containing write requests, are expected to be processed by ingesting the write requests data into the ingester.
- This alert fires if no processing is occurring at all, like if the processing is stuck (e.g. a deadlock in ingester).

How to **investigate**:

- Take goroutine profile of the ingester and check if there's any routine calling `pushToStorage`:
  - If the call exists and it's waiting on a lock then there may be a deadlock.
  - If the call doesn't exist then it could either mean processing is not stuck (false positive) or the `pushToStorage` wasn't called at all, and so you should investigate the callers in the code.

### MimirStrongConsistencyEnforcementFailed

This alert fires when too many read requests with strong consistency are failing.

How it **works**:

- When read request asks for strong-consistency guarantee, query-frontend reads the last produced offsets from Kafka and propagates this information down to ingesters. Then, ingesters wait until record with the requested offset is consumed.
- If fetching the last produced offsets fail or the read request times out when fetching offsets or waiting for the offset to be consumed, that is considered to be a failure of request with strong-consistency. Ingesters waiting fails if the requested offset doesn't get consumed within the configured `-ingest-storage.kafka.wait-strong-read-consistency-timeout`.
- If requests keep failing due to failure to enforce strong-consistency, this alert is raised.

How to **investigate**:

- Check failures and latency to the "last produced offset" on `Mimir / Queries` dashboard.
- Check wait latency of requests with strong-consistency on `Mimir / Queries` dashboard.
- Check if ingesters are processing too many records, and they need to be scaled up (vertically or horizontally).
- Check actual error in the query-frontend and/or ingester logs to see whether the `-ingest-storage.kafka.wait-strong-read-consistency-timeout` or the request timeout has been hit first.

### MimirStrongConsistencyOffsetNotPropagatedToIngesters

This alert fires when ingesters receive an unexpected high number of strongly consistent requests without an offset specified.

How it **works**:

- See [`MimirStrongConsistencyEnforcementFailed`](#MimirStrongConsistencyEnforcementFailed).

How to **investigate**:

- We expect query-frontend to fetch the last produced offsets and then propagate it down to ingesters. If it's not happening, then it's likely we introduced a bug in Mimir that's breaking the propagation of offsets from query-frontend to ingester. You should investigate the Mimir code changes and fix it.

### MimirKafkaClientBufferedProduceBytesTooHigh

This alert fires when the Kafka client buffer, used to write incoming write requests to Kafka, is getting full.

How it **works**:

- Distributor and ruler encapsulate write requests into Kafka records and send them to Kafka.
- The Kafka client has a limit on the total byte size of buffered records either sent to Kafka or sent to Kafka but not acknowledged yet.
- When the limit is reached, the Kafka client stops producing more records and fast fails.
- The limit is configured via `-ingest-storage.kafka.producer-max-buffered-bytes`.
- The default limit is configured intentionally high, so that when the buffer utilization gets close to the limit, this indicates that there's probably an issue.

How to **investigate**:

- Query `cortex_ingest_storage_writer_buffered_produce_bytes{quantile="1.0"}` metrics to see the actual buffer utilization peaks.
  - If the high buffer utilization is isolated to a small set of pods, then there might be an issue in the client pods.
  - If the high buffer utilization is spread across all or most pods, then there might be an issue in Kafka.

### Ingester is overloaded when consuming from Kafka

This runbook covers the case an ingester is overloaded when ingesting metrics data (consuming) from Kafka.

For example, if the amount of active series written to a partition exceeds the ingester capacity, the write-path will keep writing to the partition, but then the ingesters owning that partition will fail ingesting the data. Possible symptoms of this situation:

- The ingester is lagging behind replaying metrics data from Kafka, and [`MimirStartingIngesterKafkaReceiveDelayIncreasing`](#MimirStartingIngesterKafkaReceiveDelayIncreasing) or [`MimirRunningIngesterReceiveDelayTooHigh`](#MimirRunningIngesterReceiveDelayTooHigh) alerts are firing.
- The ingester logs [`err-mimir-ingester-max-series`](#err-mimir-ingester-max-series) when ingesting metrics data from Kafka.
- The ingester is OOMKilled.

How it **works**:

- An ingester owns 1 and only 1 partition. A partition can be owned by multiple ingesters, but each ingester always own a single partition.
- Metrics data is written to a partition by distributors, and the amount of written data is driven by the incoming traffic in the write-path. Distributors don't know whether the per-partition load is "too much" for the ingesters that will consume from that partition.
- Ingesters are expected to autoscale. When the number of active series in ingesters grow above the scaling threshold, more ingesters will be added to the cluster. When ingesters are scaled out, new partitions are added and incoming metrics data re-balanced between partitions. However, the old data (already written to partitions) will not be moved, and the load will be re-balanced only for metrics data ingested after the scaling.

How to **fix**:

- **Vertical scale ingesters** (no data loss)
  - Add more CPU/memory/disk to ingesters, depending on the saturated resources.
  - Increase the ingester max series instance limit (see [`MimirIngesterReachingSeriesLimit`](#MimirIngesterReachingSeriesLimit) runbook).
- **Skip replaying overloading backlog from partition** (data loss)

  1. Ensure ingesters have been scaled out, and the new partitions are ACTIVE in the partitions ring. If autoscaler didn't scaled out ingesters yet, manually add more ingester replicas (e.g. increasing HPA min replicas or manually setting the desired number of ingester replicas if ingester autoscaling is disabled).
  1. Find out the timestamp at which new partitions were created and became ACTIVE in the ring (e.g. looking at new ingesters logs).
  1. Temporarily restart ingesters with the following configuration:

     ```
     # Set <value> to the timestamp retrieved from previous step. The timestamp should be Unix epoch with milliseconds precision.
     -ingest-storage.kafka.consume-from-position-at-startup=timestamp
     -ingest-storage.kafka.consume-from-timestamp-at-startup=<value>
     ```

     Alternatively, if you can quickly find the timestamp at which new partitions became ACTIVE in the ring, you can temporarily configure ingesters to replay a partition from the end:

     ```
     -ingest-storage.kafka.consume-from-position-at-startup=end
     ```

  1. Once ingesters are stable, revert the temporarily config applied in the previous step.

## Errors catalog

Mimir has some codified error IDs that you might see in HTTP responses or logs.
These error IDs allow you to read related details in the documentation that follows.

### err-mimir-missing-metric-name

This non-critical error occurs when Mimir receives a write request that contains a series without a metric name.
Each series must have a metric name. Rarely it does not, in which case there might be a bug in the sender client.

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-metric-name-invalid

This non-critical error occurs when Mimir receives a write request that contains a series with an invalid metric name.
A metric name can only contain characters as defined by Prometheus’ [Metric names and labels](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-max-label-names-per-series

This non-critical error occurs when Mimir receives a write request that contains a series with a number of labels that exceed the configured limit.
The limit protects the system’s stability from potential abuse or mistakes. To configure the limit on a per-tenant basis, use the `-validation.max-label-names-per-series` option.

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-max-native-histogram-buckets

This non-critical error occurs when Mimir receives a write request that contains a sample that is a native histogram that has too many observation buckets.
The limit protects the system from using too much memory. To configure the limit on a per-tenant basis, use the `-validation.max-native-histogram-buckets` option.

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-not-reducible-native-histogram

This non-critical error occurs when Mimir receives a write request that contains a sample that is a native histogram that has too many observation buckets and it is not possible to reduce the buckets further. Since native buckets at the lowest resolution of -4 can cover all 64 bit float observations with a handful of buckets, this indicates that the
`-validation.max-native-histogram-buckets` option is set too low (<20).

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-invalid-native-histogram-schema

This non-critical error occurs when Mimir receives a write request that contains a sample that is a native histogram with an invalid schema number. Currently, valid schema numbers are from the range [-4, 8].

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-native-histogram-count-mismatch

This non-critical error occures when Mimir receives a write request that contains a sample that is a native histogram
where the buckets counts don't add up to the overall count recorded in the native histogram, provided that the overall
sum is a regular float number.

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, invalid native histogram errors are logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-native-histogram-count-not-big-enough

This non-critical error occures when Mimir receives a write request that contains a sample that is a native histogram
where the buckets counts add up to a higher number than the overall count recorded in the native histogram, provided
that the overall sum is not a float number (NaN).

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, invalid native histogram errors are logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-native-histogram-negative-bucket-count

This non-critical error occures when Mimir receives a write request that contains a sample that is a native histogram
where some bucket count is negative.

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, invalid native histogram errors are logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-native-histogram-span-negative-offset

This non-critical error occures when Mimir receives a write request that contains a sample that is a native histogram
where a bucket span has a negative offset.

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, invalid native histogram errors are logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-native-histogram-spans-buckets-mismatch

This non-critical error occures when Mimir receives a write request that contains a sample that is a native histogram
where the number of bucket counts does not agree with the number of buckets encoded in the bucket spans.

{{< admonition type="note" >}}
The series containing such samples are skipped during ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, invalid native histogram errors are logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-label-invalid

This non-critical error occurs when Mimir receives a write request that contains a series with an invalid label name.
A label name name can only contain characters as defined by Prometheus’ [Metric names and labels](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-label-name-too-long

This non-critical error occurs when Mimir receives a write request that contains a series with a label name whose length exceeds the configured limit.
The limit protects the system’s stability from potential abuse or mistakes. To configure the limit on a per-tenant basis, use the `-validation.max-length-label-name` option.

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-label-value-too-long

This non-critical error occurs when Mimir receives a write request that contains a series with a label value whose length exceeds the configured limit.
The limit protects the system’s stability from potential abuse or mistakes. To configure the limit on a per-tenant basis, use the `-validation.max-length-label-value` option.

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-duplicate-label-names

This non-critical error occurs when Mimir receives a write request that contains a series with the same label name two or more times.
A series that contains a duplicated label name is invalid and gets skipped during the ingestion.

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-labels-not-sorted

This error occurs when Mimir receives a write request that contains a series whose label names are not sorted alphabetically.
However, Mimir internally sorts labels for series that it receives, so this error should not occur in practice.
If you experience this error, [open an issue in the Mimir repository](https://github.com/grafana/mimir/issues).

{{< admonition type="note" >}}
Invalid series are skipped during the ingestion, and valid series within the same request are ingested.
{{< /admonition >}}

### err-mimir-too-far-in-future

This non-critical error occurs when Mimir receives a write request that contains a sample whose timestamp is in the future compared to the current "real world" time.
Mimir accepts timestamps that are slightly in the future, due to skewed clocks for example. It rejects timestamps that are too far in the future, based on the definition that you can set via the `-validation.create-grace-period` option.
On a per-tenant basis, you can fine tune the tolerance by configuring the `creation_grace_period` option.

{{< admonition type="note" >}}
Only series with invalid samples are skipped during the ingestion. Valid samples within the same request are still ingested.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-exemplar-too-far-in-future

This non-critical error occurs when Mimir receives a write request that contains an exemplar whose timestamp is in the future compared to the current "real world" time.
Mimir accepts timestamps that are slightly in the future, due to skewed clocks for example. It rejects timestamps that are too far in the future, based on the definition that you can set via the `-validation.create-grace-period` option.
On a per-tenant basis, you can fine tune the tolerance by configuring the `creation_grace_period` option.

{{< admonition type="note" >}}
Only series with invalid samples are skipped during the ingestion. Valid samples within the same request are still ingested.
{{< /admonition >}}

### err-mimir-too-far-in-past

This non-critical error occurs when Mimir rejects a sample because its timestamp is too far in the past compared to the wall clock.

How it **works**:

- The distributor or the ingester implements an lower limit on the timestamp of incoming samples, it is used to protect the system from potential abuse or mistakes.
- The lower limit is defined by the current wall clock minus the `out_of_order_time_window` and minus the `past_grace_period` settings.
- The samples that are too far in the past aren't ingested.

How to **fix** it:

- Make sure that it is intended that the timestamps of the incoming samples are that old.
- If the timestamps are correct, increase the `past_grace_period` setting, or set it to 0 to disable the limit.

{{< admonition type="note" >}}
Only the invalid samples are skipped during the ingestion. Valid samples within the same request are still ingested.
{{< /admonition >}}

### err-mimir-exemplar-too-far-in-past

This non-critical error occurs when Mimir rejects an exemplar because its timestamp is too far in the past compared to the wall clock.

Refer to [`err-mimir-too-far-in-past`](#err-mimir-too-far-in-past) for more details and how to fix it.

### err-mimir-exemplar-labels-missing

This non-critical error occurs when Mimir receives a write request that contains an exemplar without a label that identifies the related metric.
An exemplar must have at least one valid label pair, otherwise it cannot be associated with any metric.

{{< admonition type="note" >}}
Invalid exemplars are skipped during the ingestion, and valid exemplars within the same request are ingested.
{{< /admonition >}}

### err-mimir-exemplar-labels-too-long

This non-critical error occurs when Mimir receives a write request that contains an exemplar where the combined set size of its labels exceeds the limit.
The limit is used to protect the system’s stability from potential abuse or mistakes, and it cannot be configured.

{{< admonition type="note" >}}
Invalid exemplars are skipped during the ingestion, and valid exemplars within the same request are ingested.
{{< /admonition >}}

### err-mimir-exemplar-timestamp-invalid

This non-critical error occurs when Mimir receives a write request that contains an exemplar without a timestamp.
An exemplar must have a valid timestamp, otherwise it cannot be correlated to any point in time.

{{< admonition type="note" >}}
Invalid exemplars are skipped during the ingestion, and valid exemplars within the same request are ingested.
{{< /admonition >}}

### err-mimir-metadata-missing-metric-name

This non-critical error occurs when Mimir receives a write request that contains a metric metadata without a metric name.
Each metric metadata must have a metric name. Rarely it does not, in which case there might be a bug in the sender client.

{{< admonition type="note" >}}
Invalid metrics metadata are skipped during the ingestion, and valid metadata within the same request are ingested.
{{< /admonition >}}

### err-mimir-metric-name-too-long

This non-critical error occurs when Mimir receives a write request that contains a metric metadata with a metric name whose length exceeds the configured limit.
The limit protects the system’s stability from potential abuse or mistakes. To configure the limit on a per-tenant basis, use the `-validation.max-metadata-length` option.

{{< admonition type="note" >}}
Invalid metrics metadata are skipped during the ingestion, and valid metadata within the same request are ingested.
{{< /admonition >}}

### err-mimir-unit-too-long

This non-critical error occurs when Mimir receives a write request that contains a metric metadata with unit name whose length exceeds the configured limit.
The limit protects the system’s stability from potential abuse or mistakes. To configure the limit on a per-tenant basis, use the `-validation.max-metadata-length` option.

{{< admonition type="note" >}}
Invalid metrics metadata are skipped during the ingestion, and valid metadata within the same request are ingested.
{{< /admonition >}}

### err-mimir-distributor-max-ingestion-rate

This critical error occurs when the rate of received samples, exemplars and metadata per second is exceeded in a distributor.

The distributor implements a rate limit on the samples per second that can be ingested, and it's used to protect a distributor from overloading in case of high traffic.
This per-instance limit is applied to all samples, exemplars, and all of the metadata that it receives.
Also, the limit spans all of the tenants within each distributor.

How to **fix** it:

- Scale up the distributors.
- Increase the limit by using the `-distributor.instance-limits.max-ingestion-rate` option.

### err-mimir-distributor-max-inflight-push-requests

This error occurs when a distributor rejects a write request because the maximum in-flight requests limit has been reached.

How it **works**:

- The distributor has a per-instance limit on the number of in-flight write (push) requests.
- The limit applies to all in-flight write requests, across all tenants, and it protects the distributor from becoming overloaded in case of high traffic.
- To configure the limit, set the `-distributor.instance-limits.max-inflight-push-requests` option.

How to **fix** it:

- Increase the limit by setting the `-distributor.instance-limits.max-inflight-push-requests` option.
- Check the write requests latency through the `Mimir / Writes` dashboard and come back to investigate the root cause of high latency (the higher the latency, the higher the number of in-flight write requests).
- Consider scaling out the distributors.

### err-mimir-distributor-max-inflight-push-requests-bytes

This error occurs when a distributor rejects a write request because the total size in bytes of all in-flight requests limit has been reached.

How it **works**:

- The distributor has a per-instance limit on the total size in bytes of all in-flight write (push) requests.
- The limit applies to all in-flight write requests, across all tenants, and it protects the distributor from going out of memory in case of high traffic or high latency on the write path.
- To configure the limit, set the `-distributor.instance-limits.max-inflight-push-requests-bytes` option.

How to **fix** it:

- Increase the limit by setting the `-distributor.instance-limits.max-inflight-push-requests-bytes` option.
- Check the write requests latency through the `Mimir / Writes` dashboard and come back to investigate the root cause of the increased size of requests or the increased latency (the higher the latency, the higher the number of in-flight write requests, the higher their combined size).
- Consider scaling out the distributors.

### err-mimir-ingester-max-ingestion-rate

This critical error occurs when the rate of received samples per second is exceeded in an ingester.

The ingester implements a rate limit on the samples per second that can be ingested, and it's used to protect an ingester from overloading in case of high traffic.
This per-instance limit is applied to all samples that it receives.
Also, the limit spans all of the tenants within each ingester.

How to **fix** it:

- Scale up the ingesters.
- Increase the limit by using the `-ingester.instance-limits.max-ingestion-rate` option (or `max_ingestion_rate` in the runtime config).

### err-mimir-ingester-max-tenants

This critical error occurs when the ingester receives a write request for a new tenant (a tenant for which no series have been stored yet) but the ingester cannot accept it because the maximum number of allowed tenants per ingester has been reached.

How to **fix** it:

- Increase the limit by using the `-ingester.instance-limits.max-tenants` option (or `max_tenants` in the runtime config).
- Consider configuring ingesters shuffle sharding to reduce the number of tenants per ingester.

### err-mimir-ingester-max-series

This critical error occurs when an ingester rejects a write request because it reached the maximum number of in-memory series.

How it **works**:

- The ingester keeps most recent series data in-memory.
- The ingester has a per-instance limit on the number of in-memory series, used to protect the ingester from overloading in case of high traffic.
- When the limit on the number of in-memory series is reached, new series are rejected, while samples can still be appended to existing ones.
- To configure the limit, set the `-ingester.instance-limits.max-series` option (or `max_series` in the runtime config).

How to **fix** it:

- See [`MimirIngesterReachingSeriesLimit`](#MimirIngesterReachingSeriesLimit) runbook.

### err-mimir-ingester-max-inflight-push-requests

This error occurs when an ingester rejects a write request because the maximum in-flight requests limit has been reached.

How it **works**:

- The ingester has a per-instance limit on the number of in-flight write (push) requests.
- The limit applies to all in-flight write requests, across all tenants, and it protects the ingester from becoming overloaded in case of high traffic.
- To configure the limit, set the `-ingester.instance-limits.max-inflight-push-requests` option (or `max_inflight_push_requests` in the runtime config).

How to **fix** it:

- Increase the limit by setting the `-ingester.instance-limits.max-inflight-push-requests` option (or `max_inflight_push_requests` in the runtime config).
- Check the write requests latency through the `Mimir / Writes` dashboard and come back to investigate the root cause of high latency (the higher the latency, the higher the number of in-flight write requests).
- Consider scaling out the ingesters.

### err-mimir-ingester-max-inflight-push-requests-bytes

This error occurs when an ingester rejects a write request because of the maximum size of all in-flight push requests has been reached.

How it **works**:

- The ingester has a per-instance limit on the total size of the in-flight write (push) requests.
- The limit applies to all in-flight write requests, across all tenants, and it protects the ingester from using too much memory for incoming requests in case of high traffic.
- To configure the limit, set the `-ingester.instance-limits.max-inflight-push-requests-bytes` option (or `max_inflight_push_requests_bytes` in the runtime config).

How to **fix** it:

- Increase the limit by setting the `-ingester.instance-limits.max-inflight-push-requests-bytes` option (or `max_inflight_push_requests_bytes` in the runtime config), if possible.
- Check the write requests latency through the `Mimir / Writes` dashboard and come back to investigate the root cause of high latency (the higher the latency, the higher the number of in-flight write requests).
- Consider scaling out the ingesters.

### err-mimir-max-series-per-user

This error occurs when the number of in-memory series for a given tenant exceeds the configured limit.

The limit is used to protect ingesters from overloading in case a tenant writes a high number of series, as well as to protect the whole system’s stability from potential abuse or mistakes.
To configure the limit on a per-tenant basis, use the `-ingester.max-global-series-per-user` option (or `max_global_series_per_user` in the runtime configuration).

How to **fix** it:

- Ensure the actual number of series written by the affected tenant is legit.
- Consider increasing the per-tenant limit by using the `-ingester.max-global-series-per-user` option (or `max_global_series_per_user` in the runtime configuration).

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-max-series-per-metric

This error occurs when the number of in-memory series for a given tenant and metric name exceeds the configured limit.

The limit is primarily used to protect a tenant from potential mistakes on their metrics instrumentation.
For example, if an instrumented application exposes a metric with a label value including very dynamic data (e.g. a timestamp) the ingestion of that metric would quickly lead to hit the per-tenant series limit, causing other metrics to be rejected too.
This limit introduces a cap on the maximum number of series each metric name can have, rejecting exceeding series only for that metric name, before the per-tenant series limit is reached.
To configure the limit on a per-tenant basis, use the `-ingester.max-global-series-per-metric` option (or `max_global_series_per_metric` in the runtime configuration).

How to **fix** it:

- Check the details in the error message to find out which is the affected metric name.
- Investigate if the high number of series exposed for the affected metric name is legit.
- Consider reducing the cardinality of the affected metric, by tuning or removing some of its labels.
- Consider increasing the per-tenant limit by using the `-ingester.max-global-series-per-metric` option.
- Consider excluding specific metric names from this limit's check by using the `-ingester.ignore-series-limit-for-metric-names` option (or `max_global_series_per_metric` in the runtime configuration).

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-max-metadata-per-user

This non-critical error occurs when the number of in-memory metrics with metadata for a given tenant exceeds the configured limit.

Metric metadata is a set of information attached to a metric name, like its unit (e.g. counter) and description.
Metric metadata can be included by the sender in the write request, and it's returned when querying the `/api/v1/metadata` API endpoint.
Metric metadata is stored in the ingesters memory, so the higher the number of metrics metadata stored, the higher the memory utilization.

Mimir has a per-tenant limit of the number of metric names that have metadata attached.
This limit is used to protect the whole system’s stability from potential abuse or mistakes.
To configure the limit on a per-tenant basis, use the `-ingester.max-global-series-per-user` option (or `max_global_metadata_per_user` in the runtime configuration).

How to **fix** it:

- Check the current number of metric names for the affected tenant, running the instant query `count(count by(__name__) ({__name__=~".+"}))`. Alternatively, you can get the cardinality of `__name__` label calling the API endpoint `/api/v1/cardinality/label_names`.
- Consider increasing the per-tenant limit setting to a value greater than the number of unique metric names returned by the previous query.

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-max-metadata-per-metric

This non-critical error occurs when the number of different metadata for a given metric name exceeds the configured limit.

Metric metadata is a set of information attached to a metric name, like its unit (e.g. counter) and description.
Typically, for a given metric name there's only one set of metadata (e.g. the same metric name exposed by different application has the same counter and description).
However, there could be some edge cases where the same metric name has a different meaning between applications or the same meaning but a slightly different description.
In these edge cases, different applications would expose different metadata for the same metric name.

This limit is used to protect the whole system’s stability from potential abuse or mistakes, in case the number of metadata variants for a given metric name grows indefinitely.
To configure the limit on a per-tenant basis, use the `-ingester.max-global-series-per-metric` option (or `max_global_metadata_per_metric` in the runtime configuration).

How to **fix** it:

- Check the metadata for the affected metric name, querying the `/api/v1/metadata?metric=<name>` API endpoint (replace `<name>` with the metric name).
- If the different metadata is unexpected, consider fixing the discrepancy in the instrumented applications.
- If the different metadata is expected, consider increasing the per-tenant limit by using the `-ingester.max-global-series-per-metric` option (or `max_global_metadata_per_metric` in the runtime configuration).

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-max-chunks-per-query

This error occurs when execution of a query exceeds the limit on the number of series chunks fetched.

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a query fetching a huge amount of data.
To configure the limit on a global basis, use the `-querier.max-fetched-chunks-per-query` option.
To configure the limit on a per-tenant basis, set the `max_fetched_chunks_per_query` per-tenant override in the runtime configuration.

How to **fix** it:

- Consider reducing the time range and/or cardinality of the query. To reduce the cardinality of the query, you can add more label matchers to the query, restricting the set of matching series.
- Consider increasing the global limit by using the `-querier.max-fetched-chunks-per-query` option.
- Consider increasing the limit on a per-tenant basis by using the `max_fetched_chunks_per_query` per-tenant override in the runtime configuration.

### err-mimir-max-estimated-chunks-per-query

This error occurs when execution of a query exceeds the limit on the estimated number of series chunks expected to be fetched.

The estimate is based on the actual number of chunks that will be sent from ingesters to queriers, and an estimate of the number of chunks that will be sent from store-gateways to queriers.

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a query fetching a huge amount of data.
To configure the limit on a global basis, use the `-querier.max-estimated-fetched-chunks-per-query-multiplier` option.
To configure the limit on a per-tenant basis, set the `max_estimated_fetched_chunks_per_query_multiplier` per-tenant override in the runtime configuration.

How to **fix** it:

- Consider reducing the time range and/or cardinality of the query. To reduce the cardinality of the query, you can add more label matchers to the query, restricting the set of matching series.
- Consider increasing the global limit by using the `-querier.max-estimated-fetched-chunks-per-query-multiplier` option.
- Consider increasing the limit on a per-tenant basis by using the `max_estimated_fetched_chunks_per_query_multiplier` per-tenant override in the runtime configuration.

### err-mimir-max-series-per-query

This error occurs when execution of a query exceeds the limit on the maximum number of series.

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a query fetching a huge amount of data.
To configure the limit on a global basis, use the `-querier.max-fetched-series-per-query` option.
To configure the limit on a per-tenant basis, set the `max_fetched_series_per_query` per-tenant override in the runtime configuration.

How to **fix** it:

- Consider reducing the time range and/or cardinality of the query. To reduce the cardinality of the query, you can add more label matchers to the query, restricting the set of matching series.
- Consider increasing the global limit by using the `-querier.max-fetched-series-per-query` option.
- Consider increasing the limit on a per-tenant basis by using the `max_fetched_series_per_query` per-tenant override in the runtime configuration.

### err-mimir-max-chunks-bytes-per-query

This error occurs when execution of a query exceeds the limit on aggregated size (in bytes) of fetched chunks.

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a query fetching a huge amount of data.
To configure the limit on a global basis, use the `-querier.max-fetched-chunk-bytes-per-query` option.
To configure the limit on a per-tenant basis, set the `max_fetched_chunk_bytes_per_query` per-tenant override in the runtime configuration.

How to **fix** it:

- Consider reducing the time range and/or cardinality of the query. To reduce the cardinality of the query, you can add more label matchers to the query, restricting the set of matching series.
- Consider increasing the global limit by using the `-querier.max-fetched-chunk-bytes-per-query` option.
- Consider increasing the limit on a per-tenant basis by using the `max_fetched_chunk_bytes_per_query` per-tenant override in the runtime configuration.

### err-mimir-max-estimated-memory-consumption-per-query

This error occurs when execution of a query exceeds the limit on the maximum estimated memory consumed by a single query.

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a query fetching a huge amount of data.
This limit only applies when Mimir's query engine is used (ie. `-querier.query-engine=mimir`).
To configure the limit on a global basis, use the `-querier.max-estimated-memory-consumption-per-query` option.
To configure the limit on a per-tenant basis, set the `max_estimated_memory_consumption_per_query` per-tenant override in the runtime configuration.

How to **fix** it:

- Consider reducing the time range of the query.
- Consider reducing the cardinality of the query. To reduce the cardinality of the query, you can add more label matchers to the query, restricting the set of matching series.
- Consider applying aggregations such as `sum` or `avg` to the query.
- Consider increasing the global limit by using the `-querier.max-estimated-memory-consumption-per-query` option.
- Consider increasing the limit on a per-tenant basis by using the `max_estimated_memory_consumption_per_query` per tenant-override in the runtime configuration.

### err-mimir-max-query-length

This error occurs when the time range of a partial (after possible splitting, sharding by the query-frontend) query exceeds the configured maximum length. For a limit on the total query length, see [err-mimir-max-total-query-length](#err-mimir-max-total-query-length).

Both PromQL instant and range queries can fetch metrics data over a period of time.
A [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) requires a `start` and `end` timestamp, so the difference of `end` minus `start` is the time range length of the query.
An [instant query](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries) requires a `time` parameter and the query is executed fetching samples at that point in time.
However, even an instant query can fetch metrics data over a period of time by using the [range vector selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors).
For example, the instant query `sum(rate(http_requests_total{job="prometheus"}[1h]))` fetches metrics over a 1 hour period.
This time period is what Grafana Mimir calls the _query time range length_ (or _query length_).

Mimir has a limit on the query length.
This limit is applied to partial queries, after they've split (according to time) by the query-frontend. This limit protects the system’s stability from potential abuse or mistakes.
To configure the limit on a per-tenant basis, use the `-querier.max-partial-query-length` option (or `max_partial_query_length` in the runtime configuration).

### err-mimir-max-total-query-length

This error occurs when the time range of a query exceeds the configured maximum length. For a limit on the partial query length (after query splitting by interval and/or sharding), see [err-mimir-max-query-length](#err-mimir-max-query-length).

PromQL range queries can fetch metrics data over a period of time.
A [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) requires a `start` and `end` timestamp, so the difference of `end` minus `start` is the time range length of the query.

Mimir has a limit on the query length.
This limit is applied to range queries before they are split (according to time) or sharded by the query-frontend. This limit protects the system’s stability from potential abuse or mistakes.
To configure the limit on a per-tenant basis, use the `-query-frontend.max-total-query-length` option (or `max_total_query_length` in the runtime configuration).

### err-mimir-max-query-expression-size-bytes

This error occurs when the size of a raw query exceeds the configured maximum size (in bytes).

This limit is used to protect the system’s stability from potential abuse or mistakes, when running a large potentially expensive query.
To configure the limit on a per-tenant basis, use the `-query-frontend.max-query-expression-size-bytes` option (or `max_query_expression_size_bytes` in the runtime configuration).

How to **fix** it:

- Consider reducing the size of the query. It's possible there's a simpler way to select the desired data or a better way to export data from Mimir.
- Consider increasing the per-tenant limit by using the `-query-frontend.max-query-expression-size-bytes` option (or `max_query_expression_size_bytes` in the runtime configuration).

### err-mimir-tenant-max-request-rate

This error occurs when the rate of write requests per second is exceeded for this tenant.

How it **works**:

- There is a per-tenant rate limit on the write requests per second, and it's applied across all distributors for this tenant.
- The limit is implemented using [token buckets](https://en.wikipedia.org/wiki/Token_bucket).

How to **fix** it:

- Increase the per-tenant limit by using the `-distributor.request-rate-limit` (requests per second) and `-distributor.request-burst-size` (number of requests) options (or `request_rate` and `request_burst_size` in the runtime configuration). The configurable burst represents how many requests can temporarily exceed the limit, in case of short traffic peaks. The configured burst size must be greater or equal than the configured limit.

### err-mimir-tenant-max-ingestion-rate

This error occurs when the rate of received samples, exemplars and metadata per second is exceeded for this tenant.

How it **works**:

- There is a per-tenant rate limit on the samples, exemplars and metadata that can be ingested per second, and it's applied across all distributors for this tenant.
- The limit is implemented using [token buckets](https://en.wikipedia.org/wiki/Token_bucket).

How to **fix** it:

- Increase the per-tenant limit by using the `-distributor.ingestion-rate-limit` (samples per second) and `-distributor.ingestion-burst-size` (number of samples) options (or `ingestion_rate` and `ingestion_burst_size` in the runtime configuration). The configurable burst represents how many samples, exemplars and metadata can temporarily exceed the limit, in case of short traffic peaks. The configured burst size must be greater or equal than the configured limit.

### err-mimir-tenant-too-many-ha-clusters

This error occurs when a distributor rejects a write request because the number of [high-availability (HA) clusters]({{< relref "../../configure/configure-high-availability-deduplication" >}}) has hit the configured limit for this tenant.

How it **works**:

- The distributor implements an upper limit on the number of clusters that the HA tracker will keep track of for a single tenant.
- It is triggered when the write request would add a new cluster while the number the tenant currently has is already equal to the limit.
- To configure the limit, set the `-distributor.ha-tracker.max-clusters` option (or `ha_max_clusters` in the runtime configuration).

How to **fix** it:

- Increase the per-tenant limit by using the `-distributor.ha-tracker.max-clusters` option (or `ha_max_clusters` in the runtime configuration).

### err-mimir-sample-timestamp-too-old

This error occurs when the ingester rejects a sample because its timestamp is too old as compared to the most recent timestamp received for the same tenant across all its time series.

How it **works**:

- If the incoming timestamp is more than 1 hour older than the most recent timestamp ingested for the tenant, the sample will be rejected.

{{< admonition type="note" >}}
If the out-of-order sample ingestion is enabled, then this error is similar to `err-mimir-sample-out-of-order` below with a difference that the sample is older than the out-of-order time window as it relates to the latest sample for that particular time series or the TSDB.
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-sample-out-of-order

This error occurs when the ingester rejects a sample because another sample with a more recent timestamp has already been ingested.

How it **works**:

- Currently, samples are not allowed to be ingested out of order for a given series.

Common **causes**:

- Your code has a single target that exposes the same time series multiple times, or multiple targets with identical labels.
- System time of your Prometheus instance has been shifted backwards. If this was a mistake, fix the system time back to normal. Otherwise, wait until the system time catches up to the time it was changed. To measure the clock skew of a target node, you could use timex metrics, like `node_timex_maxerror_seconds` and `node_timex_estimated_error_seconds`
- You are running multiple Prometheus instances pushing the same metrics and [your high-availability tracker is not properly configured for deduplication]({{< relref "../../configure/configure-high-availability-deduplication" >}}).
- Prometheus relabelling has been configured and it causes series to clash after the relabelling. Check the error message for information about which series has received a sample out of order.
- A Prometheus instance was restarted, and it pushed all data from its Write-Ahead Log to remote write upon restart, some of which has already been pushed and ingested. This is normal and can be ignored.
- Prometheus and Mimir have the same recording rule, which generates the exact same series in both places and causes either the remote write or the rule evaluation to fail randomly, depending on timing.

{{< admonition type="note" >}}
You can learn more about out of order samples in Prometheus, in the blog post [Debugging out of order samples](https://www.robustperception.io/debugging-out-of-order-samples/).
{{< /admonition >}}

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-sample-duplicate-timestamp

This error occurs when the ingester rejects a sample because it is a duplicate of a previously received sample with the same timestamp but different value in the same time series.

Common **causes**:

- Multiple endpoints are exporting the same metrics, or multiple Prometheus instances are scraping different metrics with identical labels.
- Prometheus relabelling has been configured and it causes series to clash after the relabelling. Check the error message for information about which series has received a duplicate sample.
- If this error is logged by rulers when writing the `ALERTS_FOR_STATE` metric, this can be caused by multiple alerting rules with the same alert name and labels firing at the same time.
  Check if the alert name mentioned in the error message is defined multiple times, and if this is intentional, ensure each alert rule generates alerts with unique labels.

{{< admonition type="note" >}}
When `-ingester.error-sample-rate` is configured to a value greater than `0`, this error is logged only once every `-ingester.error-sample-rate` times.
{{< /admonition >}}

### err-mimir-exemplar-series-missing

This error occurs when the ingester rejects an exemplar because its related series has not been ingested yet.

How it **works**:

- The series must already exist before exemplars can be appended, as we do not create new series upon ingesting exemplars. The series will be created when a sample from it is ingested.

### err-mimir-store-consistency-check-failed

This error occurs when the querier is unable to fetch some of the expected blocks after multiple retries and connections to different store-gateways. The query fails because some blocks are missing in the queried store-gateways.

How it **works**:

- Mimir has been designed to guarantee query results correctness and never return partial query results. Either a query succeeds returning fully consistent results or it fails.
- Queriers, and rulers running with the "internal" evaluation mode, run a consistency check to ensure all expected blocks have been queried from the long-term storage via the store-gateways.
- If any expected block has not been queried via the store-gateways, then the query fails with this error.
- See [Anatomy of a query request]({{< relref "../../references/architecture/components/querier#anatomy-of-a-query-request" >}}) to learn more.

How to **fix** it:

- Ensure all store-gateways are healthy.
- Ensure all store-gateways are successfully synching owned blocks (see [`MimirStoreGatewayHasNotSyncTheBucket`](#mimirstoregatewayhasnotsyncthebucket)).

### err-mimir-bucket-index-too-old

This error occurs when a query fails because the bucket index is too old.

How it **works**:

- Compactors periodically write a per-tenant file, called the "bucket index", to the object storage. The bucket index contains all known blocks for the given tenant and is updated every `-compactor.cleanup-interval`.
- When a query is executed, queriers and rulers running with the "internal" evaluation mode look up the bucket index to find which blocks should be queried through the store-gateways.
- To ensure all required blocks are queried, queriers and rulers determine how old a bucket index is based on the time that it was last updated by the compactor.
- If the age is older than the maximum stale period that is configured via `-blocks-storage.bucket-store.bucket-index.max-stale-period`, the query fails.
- This circuit breaker ensures that the queriers and rulers do not return any partial query results due to a stale view of the long-term storage.

How to **fix** it:

- Ensure the compactor is running successfully (e.g. not crashing, not going out of memory).
- Ensure each compactor replica has successfully updated bucket index of each owned tenant within the double of `-compactor.cleanup-interval` (query below assumes the cleanup interval is set to 15 minutes):
  `time() - cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds > 2 * (15 * 60)`

### err-mimir-distributor-max-write-message-size

This error occurs when a distributor rejects a write request because its message size is larger than the allowed limit.

How it **works**:

- The distributor implements an upper limit on the message size of incoming write requests.
- To configure the limit, set the `-distributor.max-recv-msg-size` option.

How to **fix** it:

- Increase the allowed limit by using the `-distributor.max-recv-msg-size` option.

### err-mimir-distributor-max-otlp-request-size

This error occurs when a distributor rejects an OTel write request because its message size is larger than the allowed limit before or after decompression.

How it **works**:

- The distributor implements an upper limit on the message size of incoming OTel write requests before and after decompression regardless of the compression type. Refer to [OTLP collector compression details](https://github.com/open-telemetry/opentelemetry-collector/tree/main/config/confighttp) for more information.
- Configure this limit in the `-distributor.max-otlp-request-size` setting.

How to **fix** it:

- If you use the batch processor in the OTLP collector, decrease the maximum batch size in the `send_batch_max_size` setting. Refer to [Batch Collector](https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/batchprocessor/README.md) for details.
- Increase the allowed limit in the `-distributor.max-otlp-request-size` setting.

### err-mimir-distributor-max-write-request-data-item-size

This error can only be returned when the experimental ingest storage is enabled and is caused by a write request containing a timeseries or metadata entry which is larger than the allowed limit.

How it **works**:

- The distributor shards a write request into N partitions, where N is the tenant partitions shard size.
- For each partition, the write request data is encoded into one or more Kafka records.
- The maximum size of a Kafka record is hardcoded, so the per-partition write request data is automatically split into multiple Kafka records in order to ingest large write requests.
- A single timeseries or metadata is the smallest splittable unit, which means that a single timeseries or metadata entry can't be split into multiple Kafka records.
- If the write request contains a single timeseries or metadata entry whose size is bigger than the Kafka record size limit, then the ingestion of the write request will fail and the distributor will return a 4xx HTTP status code. The 4xx status code is used to ensure the client will not retry a request which will consistently fail.

How to **fix** it:

- Configure the client remote writing to Mimir to send smaller write requests.

### err-mimir-query-blocked

This error occurs when a query-frontend blocks a read request because the query matches at least one of the rules defined in the limits.

How it **works**:

- The query-frontend implements a middleware responsible for assessing whether the query is blocked or not.
- To configure the limit, set the block `blocked_queries` in the `limits`.

How to **fix** it:

This error only occurs when an administrator has explicitly define a blocked list for a given tenant. After assessing whether or not the reason for blocking one or multiple queries you can update the tenant's limits and remove the pattern.

## Mimir routes by path

**Write path**:

- `/distributor.Distributor/Push`
- `/cortex.Ingester/Push`
- `api_v1_push`
- `api_v1_push_influx_write`
- `otlp_v1_metrics`

**Read path**:

- `/schedulerpb.SchedulerForFrontend/FrontendLoop`
- `/cortex.Ingester/QueryStream`
- `/cortex.Ingester/QueryExemplars`
- `/gatewaypb.StoreGateway/Series`
- `api_prom_api_v1_label_name_values`
- `api_prom_api_v1_labels`
- `api_prom_api_v1_metadata`
- `api_prom_api_v1_query`
- `api_prom_api_v1_query_exemplars`
- `api_prom_api_v1_query_range`
- `api_prom_api_v1_rules`
- `api_prom_api_v1_series`

**Ruler / rules path**:

- `api_v1_rules`
- `api_v1_rules_namespace`
- `prometheus_api_v1_rules`
- `prometheus_rules_namespace`
- `prometheus_rules`

## Mimir blocks storage - What to do when things go wrong

## Recovering from a potential data loss incident

The ingested series data that could be lost during an incident can be stored in two places:

1. Ingesters (before blocks are shipped to the bucket)
2. Bucket

There could be several root causes leading to a potential data loss. In this document we're going to share generic procedures that could be used as a guideline during an incident.

### Halt the compactor

The Mimir cluster continues to successfully operate even if the compactor is not running, except that over a long period (12+ hours) this will lead to query performance degradation. The compactor could potentially be the cause of data loss because:

- It marks blocks for deletion (soft deletion). _This doesn't lead to any immediate deletion, but blocks marked for deletion will be hard deleted once a delay expires._
- It permanently deletes blocks marked for deletion after `-compactor.deletion-delay` (hard deletion)
- It could generate corrupted compacted blocks (eg. due to a bug or if a source block is corrupted and the automatic checks can't detect it)

**If you suspect the compactor could be the cause of data loss, halt it** (delete the statefulset or scale down the replicas to 0). It can be restarted anytime later.

When the compactor is **halted**:

- No new blocks will be compacted
- No blocks will be deleted (soft and hard deletion)

### Recover source blocks from ingesters

Ingesters keep, on their persistent disk, the blocks compacted from TSDB head until the `-blocks-storage.tsdb.retention-period` retention expires.

The blocks retained in the ingesters can be used in case the compactor generates corrupted blocks and the source blocks, uploaded from ingesters, have already been hard deleted from the bucket.

How to manually upload blocks from ingesters to the bucket:

1. Ensure [`gsutil`](https://cloud.google.com/storage/docs/gsutil) is installed in the Mimir pod. If not, [install it](#install-gsutil-in-the-mimir-pod)
2. Run `cd /data/tsdb && /path/to/gsutil -m rsync -n -r -x 'thanos.shipper.json|chunks_head|wal' . gs://<bucket>/recovered/`
   - `-n` enabled the **dry run** (remove it once you've verified the output matches your expectations)
   - `-m` enables parallel mode
   - `-r` enables recursive rsync
   - `-x <pattern>` excludes specific patterns from sync (no WAL or shipper metadata file should be uploaded to the bucket)
   - Don't use `-d` (dangerous) because it will delete from the bucket any block which is not in the local filesystem

### Freeze ingesters persistent disk

The blocks and WAL stored in the ingester persistent disk are the last fence of defence in case of an incident involving blocks not shipped to the bucket or corrupted blocks in the bucket. If the data integrity in the ingester's disk is at risk (eg. close to hit the TSDB retention period or close to reach max disk utilisation), you should freeze it taking a **disk snapshot**.

To take a **GCP Persistent Disk snapshot**:

1. Identify the Kubernetes PVC volume name (`kubectl get pvc --namespace <namespace>`) of the volumes to snapshot
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
Newly cloned disk is independent from its original, and can be used for further investigation by attaching it to a new Machine / Pod.

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
      command: ["sleep", "infinity"]
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

After this preparation, one can use `kubectl exec --tty=false --stdin=false clone-ingester-7-dataaccess /bin/sh` to inspect the disk mounted under `/data`.

### Install `gsutil` in the Mimir pod

1. Install python
   ```
   apk add python3 py3-pip
   ln --symbolic /usr/bin/python3 /usr/bin/python
   pip install google-compute-engine
   ```
2. Download `gsutil`
   ```
   wget https://storage.googleapis.com/pub/gsutil.tar.gz
   tar -zxvf gsutil.tar.gz
   ./gsutil/gsutil --help
   ```
3. Configure credentials

   ```
   # '-e' prompt for service account credentials.
   gsutil config -e

   # Private key path: /var/secrets/google/credentials.json
   # Project ID: your google project ID
   ```

### Deleting or scaling a StatefulSet with persistent volumes

When you delete or scale down a Kubernetes StatefulSet whose pods have persistent volume claims (PVCs), the unused PVCs are not automatically deleted by default.
This means that if the StatefulSet is recreated or scaled back up, the pods for which there was already a PVC will get the volume mounted previously.

However, this behaviour can be changed [as of Kubernetes 1.27](https://kubernetes.io/blog/2023/05/04/kubernetes-1-27-statefulset-pvc-auto-deletion-beta/).
If `spec.persistentVolumeClaimRetentionPolicy.whenScaled` is set to `Delete`, unused PVCs will be deleted when the StatefulSet is scaled down.
Similarly, if `spec.persistentVolumeClaimRetentionPolicy.whenDeleted` is set to `Delete`, all PVCs will be deleted when the StatefulSet is deleted.
Note that neither of these behaviours apply when a StatefulSet is scaled up, a rolling update is performed or pods are shifted between nodes.

When a PVC is deleted, what happens to the persistent volume (PV) it is bound to depends on its [reclaim policy](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#reclaiming):

- `Retain`: the volume will not be deleted automatically, and will need to be manually deleted
- `Delete`: the volume will be automatically deleted

The initial reclaim policy for a PV is defined by its associated storage class.
However, once the PV has been created, the PV's reclaim policy can be changed at any time, allowing it to be retained for further examination after the PVC has been deleted.
For example, if the StatefulSet has `spec.persistentVolumeClaimRetentionPolicy.whenScaled` set to `Delete` and the PV has its reclaim policy set to `Delete`,
but you wish to retain a PV for a pod that will be removed when scaling down the StatefulSet, you should change the affected PV's reclaim policy to `Retain` before scaling down the StatefulSet.

To set a PV's reclaim policy to `Retain`, use `kubectl patch pv`: `kubectl patch pv <pv-name> -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'`

### Recover accidentally deleted blocks (Google Cloud specific)

_This runbook assumes you've enabled versioning in your GCS bucket and the retention of deleted blocks didn't expire yet._

#### Recover accidentally deleted blocks using `undelete-block-gcs`

Step 1: Compile the `undelete-block-gcs` tool, whose sources are available in the Mimir repository at `tools/undelete-block-gcs/`.

Step 2: Build a list of TSDB blocks to undelete and store it to a file named `deleted-list`. The file should contain the path of 1 block per line, prefixed by `gs://`. For example:

```
gs://bucket/tenant-1/01H6NCQVS3D3H6D8WGBZ9KB41Z
gs://bucket/tenant-1/01H6NCR7HSZ8DHKEG9SSJ0QZKQ
gs://bucket/tenant-1/01H6NCRBJTY8R1F4FQJ3B1QK9W
```

Step 3: Run the `undelete-block-gcs` tool to recover the deleted blocks:

```
cat deleted-list | undelete-block-gcs -concurrency 16
```

{{< admonition type="note" >}}
we recommend to try the `undelete-block-gcs` on a single block first, ensure that it gets recovered correctly and then run it against a bigger set of blocks to recover.
{{< /admonition >}}

#### Recover accidentally deleted blocks using `gsutil`

These are just example actions but should give you a fair idea on how you could go about doing this. Read the [GCS doc](https://cloud.google.com/storage/docs/using-versioned-objects#gsutil_1) before you proceed.

Step 1: Use `gsutil ls -l -a $BUCKET` to list all blocks, including the deleted ones. Now identify the deleted blocks and save the ones to restore in a file named `deleted-block-list` (one block per line).

- `-l` prints long listing
- `-a` includes non-current object versions / generations in the listing. When combined with -l option also prints metageneration for each listed object.

Step 2: Once you have the `deleted-block-list`, you can now list all the objects you need to restore, because only objects can be restored and not prefixes:

```
while read block; do
# '-a' includes non-current object versions / generations in the listing
# '-r' requests a recursive listing.
gsutil ls -a -r $block | grep "#" | grep --invert-match deletion-mark.json
done < deleted-list > full-deleted-file-list
```

The above script will ignore the `deletion-mark.json` and `index.cache.json` which shouldn't be restored.

Step 3: Run the following script to restore the objects:

```
while read file; do
gsutil cp $file ${file%#*}
done < full-deleted-file-list
```

### Debugging distroless container images (in Kubernetes)

Mimir publishes "distroless" container images. A [distroless image](https://github.com/GoogleContainerTools/distroless/blob/main/README.md)
contains very little outside of what is needed to run a single binary.
They don't include any text editors, process managers, package managers, or other debugging tools, unless the application itself requires these.

This can pose a challenge when diagnosing problems. There exists no shell inside the container
to attach to or any tools to inspect configuration files and so on.

However, to debug distroless containers we can take the approach of attaching a more complete
container to the existing container's namespace. This allows us to bring in all of the
tools we may need and to not disturb the existing environment.
That is, we do not need to restart the running container to attach our debug tools.

## Creating a debug container

Kubernetes gives us a command that allows us to start an ephemeral debug container in a pre-existing pod,
attaching it to the same namespace as other containers in that pod. More detail about the command and
how to debug running pods is available in [the Kubernetes docs](https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/#ephemeral-container).

```bash
kubectl --namespace mimir debug -it pod/compactor-0 --image=ubuntu:latest --target=compactor --container=mimir-debug-container
```

- `pod/name` is the pod to attach to.
- `--target=` is the container within that pod with which to share a kernel namespace.
- `--image=` is the image of the debug container you wish to use.
- `--container` is the name to use for the ephemeral container. This is optional, but useful if you want to re-use it.

You can now see all of the processes running in this space. For example:

```
/ # ps aux
PID   USER     TIME  COMMAND
    1 root      5:36 /usr/bin/mimir -flags
   31 root      0:00 /bin/bash
   36 root      0:00 ps aux
```

PID 1 is the process that is executed in the target container. You can now use
tools within your debug image to interact with the running process. However, note
that your root path and important environment variables like $PATH will be different to
that of the target container.

The root filesystem of the target container is available in `/proc/1/root`. For
example, `/data` would be found at `/proc/1/root/data`, and
binaries of the target container would be somewhere like `/proc/1/root/usr/bin/mimir`.

## Copying files from a distroless container

Because distroless images do not have `tar` in them, it is not possible to copy files using `kubectl cp`.

To work around this, you can create a debug container attached to the pod (as per above) and then use `kubectl cp` against that.
The debug container cannot have terminated in order for us to be able to use it. This means if you run a debug container to get a shell,
you need to keep the shell open in order to do the following.

For example, after having created a debug container called `mimir-debug-container` for the `compactor-0` pod, run the following to copy `/etc/hostname` from the compactor pod to `./hostname` on your local machine:

```bash
kubectl --namespace mimir cp compactor-0:/proc/1/root/etc/hostname -c mimir-debug-container ./hostname
```

- `-c` is the debug container to execute in.

Note, however, that there is a limitation with `kubectl cp` wherein it cannot follow symlinks. To get around this, we can similarly use `exec`
to create a tar.

For example, you can create a tar of the path you are interested in, and then extract it locally:

```bash
kubectl --namespace mimir exec compactor-0 -c mimir-debug-container -- tar cf - "/proc/1/root/etc/cortex" | tar xf -
```

## Cleanup and Limitations

One downside of using [ephemeral containers](https://kubernetes.io/docs/concepts/workloads/pods/ephemeral-containers/#understanding-ephemeral-containers)
(which is what `kubectl debug` is a wrapper around), is that they cannot be changed
after they have been added to a pod. This includes not being able to delete them.
If the process in the debug container has finished (for example, the shell has exited), the container
will remain in the `Terminated` state. This is harmless and will remain there until the pod is deleted (eg. due to a rollout).

## Log lines

### Log line containing 'sample with repeated timestamp but different value'

This means a sample with the same timestamp as the latest one was received with a different value. The number of occurrences is recorded in the `cortex_discarded_samples_total` metric with the label `reason="new-value-for-timestamp"`.

Possible reasons for this are:

- Incorrect relabelling rules can cause a label to be dropped from a series so that multiple series have the same labels. If these series were collected from the same target they will have the same timestamp.
- The exporter being scraped sets the same timestamp on every scrape. Note that exporters should generally not set timestamps.
