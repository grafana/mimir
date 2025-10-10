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

The following workflow describes the migration process:

1. [Before you begin](#before-you-begin)
1. [Set up a new Mimir cluster with ingest storage](#set-up-a-new-mimir-cluster-with-ingest-storage)
1. [Configure write clients to duplicate writes](#configure-write-clients-to-duplicate-writes)
1. [Wait for the old cluster’s ingesters to flush data](#wait-for-the-old-cluster-s-ingesters-to-flush-data)
1. [Switch read clients to the new cluster](#switch-read-clients-to-the-new-cluster)
1. [Scale down compactors in the old cluster](#scale-down-compactors-in-the-old-cluster)
1. [Scale out compactors in the new cluster](#scale-out-compactors-in-the-new-cluster)
1. [Update ruler alerting configuration](#update-ruler-alerting-configuration)
1. [Stop writing to the old cluster](#stop-writing-to-the-old-cluster)
1. [Decommission the old cluster](#decommission-the-old-cluster)

## Before you begin

Before you begin, ensure you have the following:

- An existing Mimir cluster running with classic architecture
- Access to deploy and configure a second Mimir cluster
- Write clients, for example, Prometheus or OpenTelemetry Collector, that you can configure to send data to multiple endpoints
- Read clients, for example, Grafana, that you can reconfigure to point to a new endpoint
- The ability to modify Helm or Jsonnet configurations for both clusters

{{< admonition type="caution" >}}
This procedure temporarily doubles your ingestion and storage costs, because both clusters run in parallel and receive duplicated writes.
{{< /admonition >}}

## Set up a new Grafana Mimir cluster with ingest storage

Deploy a new Grafana Mimir cluster that uses ingest storage. Configure the new cluster to use the same object storage bucket as the old cluster.

To prevent conflicts, disable the compactor, as shown in the following code sample:

```yaml
compactor:
  replicas: 0
```

This configuration lets the new cluster use existing blocks from the old cluster while avoiding block overwrites.

Optionally, to avoid duplicate alerts, disable alert sending from the ruler. For example:

```yaml
limits:
  ruler_alerting_rules_evaluation_enabled: false
# or set the flag:
# -ruler.alerting-rules-evaluation-enabled=false
```

If your alert delivery system, for example, Grafana OnCall or PagerDuty, already deduplicates alerts, you can skip this step.

For setup instructions, refer to existing [Helm chart](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/) or [Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/) configuration documentation for ingest storage.

## Configure write clients to duplicate writes

Configure all write clients, such as Prometheus servers and OpenTelemetry Collectors, to send metrics to both clusters simultaneously.

This ensures that the new cluster receives all recent samples while it accesses historical blocks from object storage.

Keep read clients, such as Grafana, configured to use the old cluster during this step.

## Wait for the old cluster’s ingesters to flush data

Allow enough time for all ingesters in the old cluster to flush their in-memory data to object storage. This usually takes about 13 hours. During this time, both clusters are writing and both are healthy.

## Switch read clients to the new cluster

Update read clients, like Grafana, to query the new cluster.

Verify that the old cluster is no longer receiving queries by checking for `msg="query stats"` entries in the query-frontend logs.

## Scale down compactors in the old cluster

Reduce the number of compactor replicas in the old cluster to zero. For example:

```yaml
compactor:
  replicas: 0
```

Run this step immediately before scaling up the new compactors, ideally within 30 minutes.

## Scale out compactors in the new cluster

Increase the number of compactor replicas in the new cluster to match the old configuration.

This activates compaction in the new cluster and completes the transition for background storage management.

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
For example, if you’re using Helm:

```sh
helm uninstall <OLD_RELEASE_NAME>
```

Your system runs entirely with ingest storage architecture.
