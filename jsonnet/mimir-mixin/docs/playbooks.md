# Playbooks
This document contains playbooks, or at least a checklist of what to look for, for alerts in the cortex-mixin.

# Alerts

## CortexIngesterRestarts
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

## CortexRequest Latency
First establish if the alert is for read or write latency. The alert should say.

### Write Latency
Using the Cortex write dashboard, find the cluster which reported the high write latency and deduce where in the stack the latency is being introduced:

distributor: It is quite normal for the distributor P99 latency to be 50-100ms, and for the ingesters to be ~5ms. If the distributor latency is higher than this, you may need to scale up the distributors. If there is a high error rate being introduced at the distributors (400s or 500s) this has been know to induce latency.

ingesters: It is very unusual for ingester latency to be high, as they just write to memory. They probably needs scaling up, but it is worth investigating what is going on first.

### Read Latency
Query performance is an known problem. When you get this alert, you need to work out if: (a) this is a operation issue / configuration (b) this is because of algorithms and inherently limited (c) this is a bug

Using the Cortex read dashboard, find the cluster which reported the high read latency and deduce where in the stack the latency is being introduced.

query_frontend: If there is a significant P99 or avg latency difference between the frontend and the querier, you can't scale them up - we rely on their being two frontend. Is this latency coming from the cache? Scale that up. What the CPU usage of the query frontend service? Do we need to increase the CPU requests and have it scheduled to a less busy box? Note QPS on the querier will be higher than on the frontend as it splits queries into multiple smaller ones.

ingesters: Latency should be in the ~100ms - queries are in memory. If its more, check the CPU usage and consider scaling it up. NB scale ingesters slowly, 1-2 new replicas an hour.

If you think its provisioning / scaling is the problem, consult the scaling dashboard. These are just recommendations - make reasonable adjustments.

Right now most of the execution time will be spent in PromQL's innerEval. NB that the prepare (index and chunk fetch) are now interleaved with Eval, so you need to expand both to confirm if its flow execution of slow fetching.

## CortexTransferFailed
This alert goes off when an ingester fails to find another node to transfer its data to when it was shutting down. If there is both a pod stuck terminating and one stuck joining, look at the kubernetes events. This may be due to scheduling problems caused by some combination of anti affinity rules/resource utilization. Adding a new node can help in these circumstances. You can see recent events associated with a resource via kubectl describe, ex: `kubectl -n <namespace> describe pod <pod>`

## CortexIngesterUnhealthy
This alert goes off when an ingester is marked as unhealthy. Check the ring web page to see which is marked as unhealthy. You could then check the logs to see if there are any related to that ingester ex: `kubectl logs -f ingester-01 --namespace=prod`. A simple way to resolve this may be to click the "Forgot" button on the ring page, especially if the pod doesn't exist anymore. It might not exist anymore because it was on a node that got shut down, so you could check to see if there are any logs related to the node that pod is/was on, ex: `kubectl get events --namespace=prod | grep cloud-provider-node`.

## CortexFlushStuck
@todo

## CortexLoadBalancerErrors
@todo

## CortexTableSyncFailure
@todo

## CortexQuerierCapacityFull
@todo

## CortexFrontendQueriesStuck
@todo

## CortexProvisioningTooMuchMemory
@todo

## MemcachedDown
@todo

## CortexRulerFailedRingCheck

This alert occurs when a ruler is unable to validate whether or not it should claim ownership over the evaluation of a rule group. The most likely cause is that one of the rule ring entries is unhealthy. If this is the case proceed to the ring admin http page and forget the unhealth ruler. The other possible cause would be an error returned the ring client. If this is the case look into debugging the ring based on the in-use backend implementation.

## CortexIngesterHasNotShippedBlocks

This alert fires when a Cortex ingester is not uploading any block to the long-term storage. An ingester is expected to upload a block to the storage every block range period (defaults to 2h) and if a longer time elapse since the last successful upload it means something is not working correctly.

How to investigate:
- Ensure the ingester is receiving write-path traffic (samples to ingest)
- Look for any upload error in the ingester logs (ie. networking or authentication issues)

## CortexIngesterHasNotShippedBlocksSinceStart

Same as [`CortexIngesterHasNotShippedBlocks`](#CortexIngesterHasNotShippedBlocks).

## CortexQuerierHasNotScanTheBucket

This alert fires when a Cortex querier is not successfully scanning blocks in the storage (bucket). A querier is expected to periodically iterate the bucket to find new and deleted blocks (defaults to every 5m) and if it's not successfully synching the bucket since a long time, it may end up querying only a subset of blocks, thus leading to potentially partial results.

How to investigate:
- Look for any scan error in the querier logs (ie. networking or rate limiting issues)

## CortexStoreGatewayHasNotSyncTheBucket

This alert fires when a Cortex store-gateway is not successfully scanning blocks in the storage (bucket). A store-gateway is expected to periodically iterate the bucket to find new and deleted blocks (defaults to every 5m) and if it's not successfully synching the bucket for a long time, it may end up querying only a subset of blocks, thus leading to potentially partial results.

How to investigate:
- Look for any scan error in the store-gateway logs (ie. networking or rate limiting issues)

## CortexCompactorHasNotSuccessfullyRun

This alert fires when a Cortex compactor is not successfully completing a compaction run since a long time.

How to investigate:
- Ensure the compactor is not crashing during compaction (ie. `OOMKilled`)
- Look for any error in the compactor logs

## CortexCompactorHasNotSuccessfullyRunSinceStart

Same as [`CortexCompactorHasNotSuccessfullyRun`](#CortexCompactorHasNotSuccessfullyRun).

## CortexCompactorHasNotUploadedBlocks

This alert fires when a Cortex compactor is not uploading any compacted blocks to the storage since a long time.

How to investigate:
- If the alert `CortexCompactorHasNotSuccessfullyRun` or `CortexCompactorHasNotSuccessfullyRunSinceStart` have fired as well, then investigate that issue first
- If the alert `CortexIngesterHasNotShippedBlocks` or `CortexIngesterHasNotShippedBlocksSinceStart` have fired as well, then investigate that issue first
- Ensure ingesters are successfully shipping blocks to the storage
- Look for any error in the compactor logs

## CortexCompactorHasNotUploadedBlocksSinceStart

Same as [`CortexCompactorHasNotUploadedBlocks`](#CortexCompactorHasNotUploadedBlocks).
