---
aliases:
  - ../../migrate/migrate-ingest-storage/
description: Learn how to migrate a Grafana Mimir cluster from classic architecture to ingest storage architecture with no downtime.
menuTitle: Migrate from classic to ingest storage
title: Migrate from classic to ingest storage architecture
weight: 20
---

# Migrate from classic to ingest storage architecture

Migrate from a Grafana Mimir cluster running with classic architecture to one running with ingest storage architecture. This process allows you to migrate without downtime by temporarily running two clusters in parallel and duplicating writes. For background on the different architectures, refer to [Grafana Mimir architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/).

{{< admonition type="note" >}}
This procedure temporarily doubles your ingestion and storage costs, because both clusters run in parallel and receive duplicated writes.
{{< /admonition >}}

Follow these steps to migrate your Grafana Mimir cluster from classic architecture to ingest storage architecture.

1. [Before you begin](#before-you-begin)
1. [Prepare your existing Grafana Mimir cluster for migration](#prepare-your-existing-grafana-mimir-cluster-for-migration)
1. [Set up a new Grafana Mimir cluster with ingest storage](#set-up-a-new-grafana-mimir-cluster-with-ingest-storage)
1. [Configure write clients to duplicate writes](#configure-write-clients-to-duplicate-writes)
1. [Wait until both clusters have a complete view of the data](#wait-until-both-clusters-have-a-complete-view-of-the-data)
1. [Switch read clients to the new cluster](#switch-read-clients-to-the-new-cluster)
1. [Scale down compactors in the old cluster](#scale-down-compactors-in-the-old-cluster)
1. [Scale out compactors in the new cluster](#scale-out-compactors-in-the-new-cluster)
1. [Update ruler alerting configuration](#update-ruler-alerting-configuration)
1. [Stop writing to the old cluster](#stop-writing-to-the-old-cluster)
1. [Decommission the old cluster](#decommission-the-old-cluster)
1. [Scale down compactors in the new cluster](#scale-down-compactors-in-the-new-cluster)

## Before you begin

Before you begin, ensure you have the following:

- An existing Mimir cluster running with classic architecture.
- Access to deploy and configure a second Mimir cluster.
- Write clients, for example, Prometheus or OpenTelemetry Collector, that you can configure to send data to multiple endpoints.
- Read clients, for example, Grafana, that you can reconfigure to point to a new endpoint.
- The ability to modify Helm or Jsonnet configurations for both clusters.

## Prepare your existing Grafana Mimir cluster for migration

Before creating a new Grafana Mimir cluster, temporarily scale out compactors in your existing cluster. This step helps handle the increased number of blocks created while both clusters are writing to the same object storage bucket.

## Set up a new Grafana Mimir cluster with ingest storage

Deploy a new Grafana Mimir cluster that uses ingest storage. Configure the new cluster to use the same object storage bucket as the old cluster.

To prevent conflicts, disable the compactor, as shown in the following Helm values file sample:

```yaml
compactor:
  replicas: 0
```

This configuration lets the new cluster use existing blocks from the old cluster while avoiding block overwrites.

Optionally, to avoid duplicate alerts, disable alert sending from the ruler. For example, if you're using the Helm chart:

```yaml
mimir:
structuredConfig:
  limits:
    ruler_alerting_rules_evaluation_enabled: false

# or

ruler:
extraArgs:
  ruler.alerting-rules-evaluation-enabled: false
```

If your alert delivery system, for example, Grafana OnCall or PagerDuty, already deduplicates alerts, you can skip this step.

For setup instructions, refer to the existing [Helm chart](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/) or [Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/) configuration documentation for ingest storage.

After completing this step, open the Mimir overview dashboards for both clusters and verify that all components are healthy. Check that ingestion and querying are succeeding and that there are no gaps or errors.

## Configure write clients to duplicate writes

Configure all write clients, such as Prometheus servers and OpenTelemetry Collectors, to send metrics to both clusters simultaneously.

This ensures that the new cluster receives all recent samples while it accesses historical blocks from object storage.

Keep read clients, such as Grafana, configured to use the old cluster during this step.

After completing this step, open the Mimir overview dashboards for both clusters and verify that all components are healthy. Check that ingestion and querying are succeeding and that there are no gaps or errors.

## Wait until both clusters have a complete view of the data

Keep write duplication running long enough for both clusters to have a complete view of recent data.

Ingesters in the old cluster continuously upload new blocks to object storage and create new blocks on disk every 2 hours. Because ingesters typically hold about 12–13 hours of recent data in memory and on local disk, continue double writing for at least that duration. This ensures that the new cluster receives all data written during the overlap period and that queries return a complete dataset.

You don’t need to disable block uploads from ingesters in the new cluster. Both clusters can safely upload blocks to the shared object storage because compactors merge them correctly.

After the migration is complete, scale the compactors in the new cluster back down to their normal count.

## Switch read clients to the new cluster

Update read clients, like Grafana, to query the new cluster.

Verify that the old cluster is no longer receiving queries by checking for `msg="query stats"` entries in the query-frontend logs.

After completing this step, open the Mimir overview dashboards for both clusters and verify that all components are healthy. Check that ingestion and querying are succeeding and that there are no gaps or errors.

## Scale down compactors in the old cluster

Reduce the number of compactor replicas in the old cluster to zero. For example:

```yaml
compactor:
  replicas: 0
```

Run this step immediately before scaling up the compactors in the new cluster, ideally within 15 minutes. If the bucket index isn’t updated by the new cluster’s compactors within about an hour of stopping the old compactors, read queries can fail.

## Scale out compactors in the new cluster

Increase the number of compactor replicas in the new cluster to match the old configuration. This activates compaction in the new cluster and completes the transition for background storage management.

After completing this step, open the Mimir overview dashboards for both clusters and verify that all components are healthy. Check that ingestion and querying are succeeding and that there are no gaps or errors.

## Update ruler alerting configuration

If you disabled alert sending in the new cluster:

1. Disable rule evaluations in the old cluster.
2. Enable them in the new cluster by removing the override you previously set.

{{< admonition type="tip" >}}
Complete these actions in quick succession to prevent duplicate alerts.
{{< /admonition >}}

## Stop writing to the old cluster

Reconfigure write clients to only send data to the new cluster.

## Decommission the old cluster

After the new cluster is fully operational, decommission the old cluster.
For example, if you’re using Helm, run the following command:

```sh
helm uninstall <OLD_RELEASE_NAME>
```

Your system runs entirely with ingest storage architecture.

## Scale down compactors in the new cluster

After the migration is complete and the old cluster is decommissioned, reduce the number of compactor replicas in the new cluster to your normal steady-state configuration. This lowers resource use after the temporary load from the dual-cluster operation is gone.
