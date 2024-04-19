---
aliases:
  - ../../operators-guide/running-production-environment/perform-a-rolling-update/
  - ../../operators-guide/running-production-environment/performing-a-rolling-update/
  - ../../operators-guide/run-production-environment/perform-a-rolling-update/
description: Learn how to perform a rolling update to Grafana Mimir.
menuTitle: Perform a rolling update
title: Perform a rolling update to Grafana Mimir
weight: 20
---

# Perform a rolling update to Grafana Mimir

You can use a rolling update strategy to apply configuration changes to
Grafana Mimir, and to upgrade Grafana Mimir to a newer version. A
rolling update results in no downtime to Grafana Mimir.

## Monolithic mode

When you run Grafana Mimir in monolithic mode, roll out changes to one instance at a time.
After you apply changes to an instance, and the instance restarts, its `/ready` endpoint returns HTTP status code `200`, which means that you can proceed with rolling out changes to another instance.

{{< admonition type="note" >}}
When you run Grafana Mimir on Kubernetes, to roll out changes to one instance at a time, configure the `Deployment` or `StatefulSet` update strategy to `RollingUpdate` and `maxUnavailable` to `1`.
{{< /admonition >}}

## Microservices mode

When you run Grafana Mimir in microservices mode, roll out changes to multiple instances of each stateless component at the same time.
You can also roll out multiple stateless components in parallel.
Stateful components have the following restrictions:

- Alertmanagers: Roll out changes to a maximum of two Alertmanagers at a time.
- Ingesters: Roll out changes to one ingester at a time.
- Store-gateways: Roll out changes to a maximum of two store-gateways at a time.

{{< admonition type="note" >}}
If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}}) for a component, you can roll out changes to all component instances in the same zone at the same time.
{{< /admonition >}}

### Alertmanagers

[Alertmanagers]({{< relref "../../references/architecture/components/alertmanager" >}}) store alerts state in memory.
When an Alertmanager is restarted, the alerts stored on the Alertmanager are not available until the Alertmanager runs again.

By default, Alertmanagers replicate each tenant's alerts to three Alertmanagers.
Alerts notification and visualization succeed when each tenant has at least one healthy Alertmanager in their shard.

To ensure no alerts notification, reception, or visualization fail during a rolling update, roll out changes to a maximum of two Alertmanagers at a time.

{{< admonition type="note" >}}
If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}}) for Alertmanager, you can roll out changes to all Alertmanagers in one zone at the same time.
{{< /admonition >}}

### Ingesters

[Ingesters]({{< relref "../../references/architecture/components/ingester" >}}) store recently received samples in memory.
When an ingester restarts, the samples stored in the restarting ingester are not available for querying until the ingester runs again.

By default, ingesters run with a replication factor equal to `3`.
Ingesters running with the replication factor of `3` require a quorum of two instances to successfully query any series samples.
Because series are sharded across all ingesters, Grafana Mimir tolerates up to one unavailable ingester.

To ensure no query fails during a rolling update, roll out changes to one ingester at a time.

{{< admonition type="note" >}}
If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}}) for ingesters, you can roll out changes to all ingesters in one zone at the same time.
{{< /admonition >}}

### Store-gateways

[Store-gateways]({{< relref "../../references/architecture/components/store-gateway" >}}) shard blocks among running instances.
By default, each block is replicated to three store-gateways.
Queries succeed when each required block is loaded by at least one store-gateway.

To ensure no query fails during a rolling update, roll out changes to a maximum of two store-gateways at a time.

{{< admonition type="note" >}}
If you enabled [zone-aware replication]({{< relref "../../configure/configure-zone-aware-replication" >}}) for store-gateways, you can roll out changes to all store-gateways in one zone at the same time.
{{< /admonition >}}
