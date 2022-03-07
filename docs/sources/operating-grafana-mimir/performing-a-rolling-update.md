---
title: "Performing a rolling update"
description: ""
weight: 10
---

# Performing a rolling update

Grafana Mimir can be updated with no downtime using a rolling update strategy.
The rolling update strategy can be used both to apply configuration changes and to upgrade Grafana Mimir to a newer version.

## Monolithic mode

When running Grafana Mimir in monolithic mode, you should roll out changes to one instance at a time.
Once changes have been applied to an instance, the instance has restarted, its `/ready` endpoint returns HTTP status code `200`, then you can proceed with rolling out another instance.

> **Note**: When running Grafana Mimir on Kubernetes, you can configure the `Deployment` or `StatefulSet` update strategy to `RollingUpdate` and `maxUnavailable` to `1`, to roll out one instance at a time.

## Microservices mode

When running Grafana Mimir in microservices mode, you can roll out changes to multiple instances of each stateless component at the same time.
You can also roll out multiple stateless components in parallel.
However, the stateful components have some restrictions:

- Alertmanagers: roll out one at most two at a time.
- Ingesters: roll out one at a time.
- Store-gateways: roll out at most two at a time.

> **Note**: If you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for a component, you can roll out all component instances in one zone at once.

### Alertmanagers

[Alertmanagers]({{< relref "../architecture/components/alertmanager.md">}}) store alerts state in memory.
When an Alertmanager is restarted, the alerts stored on the Alertmanager are not available until the Alertmanager is running again.

By default, Alertmanagers replicate each tenant's alerts to three Alertmanagers.
Alerts notification and visualization succeed as far as each tenant has at least one healthy Alertmanager in their shard.

To ensure no alerts notification, reception or visualization fail during a rolling update, we recommend to roll out at most two Alertmanagers at a time.

> **Note**: If you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for Alertmanager, you can roll out all Alertmanagers in one zone at once.

### Ingesters

[Ingesters]({{< relref "../architecture/components/ingester.md">}}) store recently received samples in memory.
When an ingester is restarted, the samples stored in the restarting ingester are not available for querying until the ingester will be running again.

By default, ingesters run with a replication factor equal to three.
Ingesters running with the replication factor three require a quorum of two instances to successfully query any series samples.
Given series are sharded across all ingesters, Grafana Mimir tolerates up to one unavailable ingester.

To ensure no query fails during a rolling update, we recommend to roll out one ingester at a time.

> **Note**: If you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for ingesters, you can roll out all ingesters in one zone at once.

### Store-gateways

[Store-gateways]({{< relref "../architecture/components/store-gateway.md">}}) shard blocks among running instances.
By default, each block is replicated to three store-gateways.
Queries succeed as long as each required block is loaded by at least one store-gateway.

To ensure no query fails during a rolling update, we recommend to roll out at most two store-gateways at a time.

> **Note**: if you enabled [zone-aware replication]({{< relref "./configure-zone-aware-replication.md">}}) for store-gateways, you can roll out all store-gateways in one zone at once.
