---
title: "Configuring zone-aware replication"
description: ""
weight: 40
---

# Configuring zone-aware replication

Zone-aware replication is the replication of data across failure domains to avoid data loss in a domain outage.
Grafana Mimir calls failure domains **zones**.
Types of zones include:

- Availability zones
- Data centers
- Racks

Without zone-aware replication enabled, Grafana Mimir replicates data randomly across all component replicas, regardless of whether these replicas are all running within the same zone.
Even with the Grafana Mimir cluster deployed across multiple zones, the replicas for any given data may reside in the same zone.
If an outage affects a whole zone containing multiple replicas at the same time, data loss may occur.

With zone-aware replication enabled, Grafana Mimir guarantees data replication to replicas across different zones.

> **Warning:**
> Ensure that deployment tooling for rolling updates is also zone-aware.
> Rolling updates should only update replicas in a single zone at any one time.

Grafana Mimir supports zone-aware replication for each of:

- [Alertmanager alerts](#configuring-alertmanager-alerts-replication)
- [Ingester time series](#configuring-ingester-time-series-replication)
- [Store-gateway blocks](#configuring-store-gateway-blocks-replication)

## Configuring Alertmanager alerts replication

Zone-aware replication in the Alertmanager ensures that Grafana Mimir replicates alerts across `-alertmanager.sharding-ring.replication-factor` Alertmanager replicas, one in each zone.

To enable zone-aware replication for alerts:

1. Set the zone for each Alertmanager replica via the `-alertmanager.sharding-ring.instance-availability-zone` CLI flag or its respective YAML configuration parameter.
2. Rollout Alertmanagers so that each Alertmanager replica is running with a configured zone.
3. Set the `-alertmanager.sharding-ring.zone-awareness-enabled=true` CLI flag or its respective YAML configuration parameter for Alertmanagers.

## Configuring ingester time series replication

Zone-aware replication in the ingester ensures that Grafana Mimir replicates each time series to `-ingester.ring.replication-factor` ingester replicas, one in each zone.

To enable zone-aware replication for time series:

1. Set the zone for each ingester replica via the `-ingester.ring.instance-availability-zone` CLI flag or its respective YAML configuration parameter.
2. Rollout ingesters so that each ingester replica is running with a configured zone.
3. Set the `-ingester.ring.zone-awareness-enabled=true` CLI flag or its respective YAML configuration parameter for distributors, ingesters, and queriers.

## Configure store-gateway blocks replication

To enable the zone-aware replication for the store-gateways, refer to [Zone awareness]({{<relref "../architecture/store-gateway.md#zone-awareness" >}}) in the store-gateway component documentation.

## Minimum number of zones

To guarantee zone-aware replication, deploy Grafana Mimir across a number of zones equal-to or greater-than the configured replication factor.
With a replication factor of 3, which is the default for time series replication, deploy the Grafana Mimir cluster across at least 3 zones.
Deploying to more zones than the configured replication factor is safe.
Deploying to fewer zones than the configured replication factor causes missed writes to replicas or for the writes to fail completely.

Reads can withstand zone failures as long as there are no more than `floor(replication factor / 2)` zones with failing replicas.

## Unbalanced zones

To ensure fair balancing of the workload across zones, run the same number of replicas of each component in each zone.
With unbalanced replica counts, zones with fewer replicas have higher resource utilization than those with more replicas.

## Costs

Most cloud providers charge for inter availability zone networking and deploying Grafana Mimir with zone-aware replication across multiple cloud provider availability zones may incur additional networking costs.
