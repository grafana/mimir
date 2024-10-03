---
aliases:
  - ../configuring/configuring-zone-aware-replication/
  - configuring-zone-aware-replication/
  - ../operators-guide/configure/configure-zone-aware-replication/
description: Learn how to replicate data across failure domains.
menuTitle: Zone-aware replication
title: Configure Grafana Mimir zone-aware replication
weight: 110
---

# Configure Grafana Mimir zone-aware replication

Zone-aware replication is the replication of data across failure domains.
Zone-aware replication helps to avoid data loss during a domain outage.
Grafana Mimir defines failure domains as _zones_. Depending on the underlying infrastructure that Grafana Mimir is deployed on, zones could be, for example,

- Availability zones
- Data centers
- Racks
- Anti-affinity groups within a single availability zone

Without zone-aware replication enabled, Grafana Mimir replicates data randomly across all component replicas, regardless of whether the replicas are running within the same zone.
Even with a Grafana Mimir cluster deployed across multiple zones, the replicas for any given data could reside in the same zone.
If an outage affects a zone containing multiple replicas, data loss might occur.

With zone-aware replication enabled, Grafana Mimir ensures data replication to replicas across different zones.

{{< admonition type="warning" >}}
Ensure that you configure deployment tooling so that it's also zone-aware.
The deployment tooling is responsible for executing rolling updates.

Rolling updates should only update replicas in a single zone at any given time.
You can utilize the [Kubernetes rollout Operator](#kubernetes-operator-for-simplifying-rollouts-of-zone-aware-components) to assist with this.
{{< /admonition >}}

Grafana Mimir supports zone-aware replication for the following:

- [Alertmanager alerts](#configuring-alertmanager-alerts-replication)
- [Ingester time series](#configuring-ingester-time-series-replication)
- [Store-gateway blocks](#configuring-store-gateway-blocks-replication)

## Configuring Alertmanager alerts replication

Zone-aware replication in the Alertmanager ensures that Grafana Mimir replicates alerts across `-alertmanager.sharding-ring.replication-factor` Alertmanager replicas, with one replica located in each zone.

**To enable zone-aware replication for alerts**:

1. Configure the zone of each Alertmanager replica via the `-alertmanager.sharding-ring.instance-availability-zone` CLI flag or its respective YAML configuration parameter.
1. Roll out Alertmanagers so that each Alertmanager replica runs with a configured zone.
1. Set the `-alertmanager.sharding-ring.zone-awareness-enabled=true` CLI flag or its respective YAML configuration parameter for Alertmanagers.

## Configuring ingester time series replication

Zone-aware replication in the ingester ensures that Grafana Mimir replicates each time series to `-ingester.ring.replication-factor` ingester replicas, with one replica located in each zone.

**To enable zone-aware replication for time series**:

1. Configure the zone of each ingester replica via the `-ingester.ring.instance-availability-zone` CLI flag or its respective YAML configuration parameter.
2. Roll out ingesters so that each ingester replica runs with a configured zone.
3. Set the `-ingester.ring.zone-awareness-enabled=true` CLI flag or its respective YAML configuration parameter for distributors, ingesters, and queriers.

## Configuring store-gateway blocks replication

To enable zone-aware replication for the store-gateways, refer to [Zone awareness]({{< relref "../references/architecture/components/store-gateway#zone-awareness" >}}).

## Minimum number of zones

To ensure zone-aware replication, deploy Grafana Mimir across a number of zones equal-to or greater-than the configured replication factor.
With a replication factor of 3, which is the default, deploy the Grafana Mimir cluster across at least three zones.
Deploying Grafana Mimir clusters to more zones than the configured replication factor does not have a negative impact.
Deploying Grafana Mimir clusters to fewer zones than the configured replication factor can cause writes to the replica to be missed, or can cause writes to fail completely.

A Grafana Mimir deployment should have at least `floor(replication factor / 2) + 1` healthy zones to operate.

## Unbalanced zones

To ensure that the workload across zones is balanced, run the same number of replicas of each component in each zone.
When replica counts are unbalanced, zones with fewer replicas have higher resource utilization than those with more replicas.

## Costs

Most cloud providers charge for inter-availability zone networking.
Deploying Grafana Mimir with zone-aware replication across multiple cloud provider availability zones likely results in additional networking costs.

{{< admonition type="note" >}}
The requests that the distributors receive are usually compressed, and the requests that the distributors send to the ingesters are uncompressed by default.
This can result in increased cross-zone bandwidth costs because at least two ingesters will be in different availability zones.

If this cost is a concern, you can compress those requests by setting the `-ingester.client.grpc-compression` CLI flag, or its respective YAML configuration parameter, to `snappy` or `gzip` in the distributors.
{{< /admonition >}}

## Kubernetes operator for simplifying rollouts of zone-aware components

The [Kubernetes Rollout Operator](https://github.com/grafana/rollout-operator) is a Kubernetes Operator that makes it easier for you to manage multi-availability-zone rollouts. Consider using the Kubernetes Rollout Operator when you run Grafana Mimir on Kubernetes with zone awareness enabled.

Note that even in a scenario where you have deployed Grafana Mimir to a single physical availability zone, the Rollout Operator is immensely useful.

This is because the Operator automates and accelerates the process of rollouts by:

- Allowing an entire zone to be updated at one time
- Prioritizing rollout of unhealthy zones over healthy zones to ensure stability
- Allowing the administrator to determine how aggressive rollouts should be

The automated functionality provided by an Operator is particularly useful in larger deployments where rollouts would otherwise take a very long time and likely require manual intervention if issues were to occur.

## Enabling zone-awareness via the Grafana Mimir Jsonnet

Instead of configuring Grafana Mimir directly, you can use the [Grafana Mimir Jsonnet](https://github.com/grafana/mimir/tree/main/operations/mimir) to enable ingester and store-gateway zone awareness.
To enable ingester and store-gateway zone awareness, set the top level `multi_zone_store_gateway_enabled` or `multi_zone_ingester_enabled` Jsonnet fields to `true`. These flags set the required Grafana Mimir configuration parameters that support ingester and store-gateway zone awareness.
