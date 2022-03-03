---
title: "Horizontal scaling"
description: ""
weight: 10
---

# Horizontal scaling

Grafana Mimir can horizontally scale every component.
Horizontal scaling means that to respond to increased load you can increase the number of replicas of each Mimir component.

We have designed Grafana Mimir to scale up quickly, safely, and with no manual intervention.
However, be careful when scaling down some of the stateful components.

## Monolithic mode

When running Grafana Mimir in monolithic mode, you can safely scale up to any number of instances.
To scale down the Mimir cluster, see [Scaling down ingesters](#scaling-down-ingesters).

## Microservices mode

When running Grafana Mimir in microservices mode, you can safely scale up any component.
You can also safely scale down any stateless component.

The following stateful components have limitations when scaling down:

- Alertmanagers
- Ingesters
- Store-gateways

### Scaling down alertmanagers

**To guarantee no downtime when scaling down [alertmanagers]({{< relref "../architecture/alertmanager.md">}}):**

- Scale down at most two alertmanagers at the same time.
- Ensure at least `-alertmanager.sharding-ring.replication-factor` alertmanager instances are running (three when running Grafana Mimir with the default configuration).

> **Note:** If you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for alertmanagers, you can, in parallel, scale down any number of alertmanager instances within one zone at a time.

### Scaling down ingesters

[Ingesters]({{< relref "../architecture/ingester.md">}}) store recently received samples in memory.
When an ingester shuts down, because of a scale down operation, the samples stored in the ingester cannot be discarded in order to guarantee no data loss.

There are two challenges to overcome when scaling down ingesters:

1. Ensure the ingester blocks are uploaded to the long-term storage before shutting down.
1. Ensure no query temporarily returns partial results.

**Ensure ingester blocks are uploaded to the long-term storage before shutting down**:

By default, when an ingester shuts down, the samples that are stored in the ingester are not uploaded to the long-term storage, which causes the data to be lost.

Ingesters expose an API endpoint [`/ingester/shutdown`]({{< relref "../reference-http-api/_index.md#shutdown">}}) that flushes in-memory time series data from ingester to the long-term storage and unregisters the ingester from the ring.
After the `/ingester/shutdown` API endpoint successfully returns, the ingester will not receive any write or read request, but the process will not exit.
Terminate the process by sending a `SIGINT` or `SIGTERM` signal after the shutdown endpoint returns.

**Ensure no query temporarily returns partial results**:

The blocks an ingester uploads to the long-term storage are not immediately available for querying.
The [queriers]({{< relref "../architecture/querier.md">}}) and [store-gateways]({{< relref "../architecture/store-gateway.md">}}) take some time before a newly uploaded block is available for querying.
If you scale down two or more ingesters in a short period of time, queries might return partial results.

#### Scaling down ingesters deployed in a single zone (default)

When ingesters are deployed in a single zone, the scale-down procedure requires the following steps:

1. Configure the Grafana Mimir cluster to discover and query new uploaded blocks as quickly as possible:
   1. Configure queriers and rulers to always query the long-term storage and to disable ingesters [shuffle sharding]({{< relref "./configure-shuffle-sharding.md">}}) on the read path:
      ```
      -querier.query-store-after=0s
      -querier.shuffle-sharding-ingesters-lookback-period=87600h
      ```
   1. Configure the compactors to frequently update the bucket index:
      ```
      -compactor.cleanup-interval=5m
      ```
   1. Configure the store-gateways to frequently refresh the bucket index and to immediately load all blocks:
      ```
      -blocks-storage.bucket-store.sync-interval=5m
      -blocks-storage.bucket-store.ignore-blocks-within=0s
      ```
    1. Configure queriers, rulers and store-gateways with reduced TTLs for the metadata cache:
      ```
      -blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl=1m
      -blocks-storage.bucket-store.metadata-cache.tenants-list-ttl=1m
      -blocks-storage.bucket-store.metadata-cache.tenant-blocks-list-ttl=1m
      -blocks-storage.bucket-store.metadata-cache.metafile-doesnt-exist-ttl=1m
      ```
1. Scale down one ingester at a time:
   1. Invoke the `/ingester/shutdown` API endpoint on the ingester to terminate.
   1. Wait until the API endpoint call has successfully returned and the ingester logged "finished flushing and shipping TSDB blocks".
   1. Send a `SIGINT` or `SIGTERM` signal to the process of the ingester to terminate.
   1. Wait 10 minutes before proceeding with the next ingester. The temporarily configuration applied guarantees newly uploaded blocks are available for querying within 10 minutes.
1. Wait until the originally configured `-querier.query-store-after` period of time has elapsed since when all ingesters have been shutdown.
1. Revert the temporarily configuration changes done at the beginning of the scale down procedure.

#### Scaling down ingesters deployed in multiple zones

Grafana Mimir can tolerate a full-zone outage when ingesters are deployed in [multiple zones]({{< relref "./configure-zone-aware-replication.md">}}).
A scale down of ingesters in one zone can be seen as a partial-zone outage.
You can leverage on ingesters deployed in multiple zones to simplify the scale down procedure.

For each zone, go through the following steps:

1. Invoke the `/ingester/shutdown` API endpoint on all ingesters that you want to terminate.
1. Wait until the API endpoint calls have successfully returned and the ingester logged "finished flushing and shipping TSDB blocks".
1. Send a `SIGINT` or `SIGTERM` signal to the processes of ingesters that you want to terminate.
1. Wait until the blocks uploaded by terminated ingesters are available for querying before proceeding with the next zone. The required amount of time to wait depends on your configuration and it's the maximum value among the following settings:
   - The configured `-querier.query-store-after`
   - Two times the configured `-blocks-storage.bucket-store.sync-interval`
   - Two times the configured `-compactor.cleanup-interval`

### Scaling down store-gateways

To guarantee no downtime when scaling down [store-gateways]({{< relref "../architecture/store-gateway.md">}}), you should:

- Scale down at most two store-gateways at the same time.
- Ensure at least `-store-gateway.sharding-ring.replication-factor` store-gateway instances are running (three when running Grafana Mimir with the default configuration).

> **Note:** if you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for store-gateways, you can in parallel scale down any number of store-gateway instances in one zone at a time.
