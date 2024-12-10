---
aliases:
  - ../../operators-guide/running-production-environment/scaling-out/
  - ../../operators-guide/run-production-environment/scaling-out/
description: Learn how to scale out Grafana Mimir.
menuTitle: Scaling out
title: Scaling out Grafana Mimir
weight: 30
---

# Scaling out Grafana Mimir

Grafana Mimir can horizontally scale every component.
Scaling out Grafana Mimir means that to respond to increased load, you can increase the number of replicas of each Grafana Mimir component.

We have designed Grafana Mimir to scale up quickly, safely, and with no manual intervention.
However, be careful when scaling down some of the stateful components as these actions can result in writes and reads failures, or partial query results.

## Monolithic mode

When running Grafana Mimir in monolithic mode, you can safely scale up to any number of instances.
To scale down the Grafana Mimir cluster, see [Scaling down ingesters](#scaling-down-ingesters).

## Read-write mode

When running Grafana Mimir in read-write mode, you can safely scale up any of the three components:

- You can safely scale the Mimir read component up or down because it is stateless. You could also use an autoscaler.

- You can safely scale the Mimir backend component down within one zone at a time. Because it contains the store-gateway, for more information see [Scaling down store-gateways](#scaling-down-store-gateways).

- To scale down the Mimir write component, see [Scaling down ingesters](#scaling-down-ingesters).

## Microservices mode

When running Grafana Mimir in microservices mode, you can safely scale up any component.
You can also safely scale down any stateless component.

The following stateful components have limitations when scaling down:

- Alertmanagers
- Ingesters
- Store-gateways

### Scaling down Alertmanagers

Scaling down [Alertmanagers]({{< relref "../../references/architecture/components/alertmanager" >}}) can result in downtime.

Consider the following guidelines when you scale down Alertmanagers:

- Scale down no more than two Alertmanagers at the same time.
- Ensure at least `-alertmanager.sharding-ring.replication-factor` Alertmanager instances are running (three when running Grafana Mimir with the default configuration).

{{< admonition type="note" >}}
If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}}) for Alertmanagers, you can, in parallel, scale down any number of Alertmanager instances within one zone at a time.
{{< /admonition >}}

### Scaling down ingesters

[Ingesters]({{< relref "../../references/architecture/components/ingester" >}}) store recently received samples in memory.
When an ingester shuts down, because of a scale down operation, the samples stored in the ingester cannot be discarded in order to guarantee no data loss.

You might experience the following challenges when you scale down ingesters:

- By default, when an ingester shuts down, the samples stored in the ingester are not uploaded to the long-term storage, which causes data loss.

  Ingesters expose an API endpoint [`/ingester/shutdown`]({{< relref "../../references/http-api#shutdown" >}}) that flushes in-memory time series data from ingester to the long-term storage and unregisters the ingester from the ring.

  After the `/ingester/shutdown` API endpoint successfully returns, the ingester does not receive write or read requests, but the process will not exit.

  You can terminate the process by sending a `SIGINT` or `SIGTERM` signal after the shutdown endpoint returns.

  **To mitigate this challenge, ensure that the ingester blocks are uploaded to the long-term storage before shutting down.**

- When you scale down ingesters, the querier might temporarily return partial results.

  The blocks an ingester uploads to the long-term storage are not immediately available for querying.
  It takes the [queriers]({{< relref "../../references/architecture/components/querier" >}}) and [store-gateways]({{< relref "../../references/architecture/components/store-gateway" >}}) some time before a newly uploaded block is available for querying.
  If you scale down two or more ingesters in a short period of time, queries might return partial results.

#### Scaling down ingesters

Complete the following steps to scale down ingesters in any zone.

1. Set each ingester to read-only mode:

   a. Send a POST request to the `/ingester/prepare-instance-ring-downscale` API endpoint on the ingester to place it into read-only mode.

1. Wait until the blocks uploaded by read-only ingesters are available for querying before proceeding. The required amount of time to wait depends on your configuration and is the maximum value for the following settings:

- The configured `-querier.query-store-after` setting
- Two times the configured `-blocks-storage.bucket-store.sync-interval` setting
- Two times the configured `-compactor.cleanup-interval` setting

1. Scale down each ingester:

   a. Send a POST request to the `/ingester/shutdown` API endpoint on the ingester to terminate it.

   b. Wait until the API endpoint call has successfully returned and the ingester has logged "finished flushing and shipping TSDB blocks".

   c. Send a `SIGINT` or `SIGTERM` signal to the process of the ingester to terminate.

### Scaling down store-gateways

To guarantee no downtime when scaling down [store-gateways]({{< relref "../../references/architecture/components/store-gateway" >}}), complete the following steps:

1. Ensure at least `-store-gateway.sharding-ring.replication-factor` store-gateway instances are running (three when running Grafana Mimir with the default configuration).
1. Scale down no more than two store-gateways at the same time.
   If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}})
   for store-gateways, you can in parallel scale down any number of store-gateway instances in one zone at a time.
   Zone-aware replication is enabled by default in the `mimir-distributed` Helm chart.
1. Stop the store-gateway instances you want to scale down.
1. If you have set the value of `-store-gateway.sharding-ring.unregister-on-shutdown` to `false`, then remove the stopped instances from the store-gateway ring:
   1. In a browser, go to the `GET /store-gateway/ring` page that store-gateways expose on their HTTP port.
   1. Click **Forget** on the instances that you scaled down.
      Alternatively, wait for the duration of the value of `-store-gateway.sharding-ring.heartbeat-timeout` times 10.
      The default value of `-store-gateway.sharding-ring.heartbeat-timeout` is one minute.
1. Proceed with the next two store-gateway replicas. If you are using zone-aware replication, the proceed with the next zone.
