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

## Before you begin

Before you begin, ensure you have the following:

- An existing Mimir cluster running with classic architecture
- Access to deploy and configure a second Mimir cluster
- Write clients, for example, Prometheus or OpenTelemetry Collector, that you can configure to send data to multiple endpoints
- Read clients, for example, Grafana, that you can reconfigure to point to a new endpoint
- The ability to modify Helm or Jsonnet configurations for both clusters

{{< admonition type="caution" >}}
This procedure temporarily doubles your ingestion and storage costs because both clusters run in parallel and receive duplicated writes.
{{< /admonition >}}

## Step 1. Set up a new Mimir cluster with ingest storage

Deploy a new Mimir cluster that uses ingest storage.  
Configure the new cluster to use the same object storage bucket as the old cluster.

Disable the compactor to prevent conflicts:

```yaml
compactor:
  replicas: 0
```

This configuration lets the new cluster use existing blocks from the old cluster while avoiding block overwrites.

Optionally, disable alert sending from the ruler to avoid duplicate alerts:

```yaml
limits:
  ruler_alerting_rules_evaluation_enabled: false
# or set the flag:
# -ruler.alerting-rules-evaluation-enabled=false
```

If your alert delivery system (for example, Grafana OnCall or PagerDuty) already deduplicates alerts, you can skip this step.

For setup instructions, refer to existing [Helm chart](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/) or [Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/) configuration documentation for ingest storage.

## Step 2. Configure write clients to duplicate writes

Configure all write clients—such as Prometheus servers and OpenTelemetry Collectors—to send metrics to both clusters simultaneously.

This ensures that the new cluster receives all recent samples while it accesses historical blocks from object storage.

Keep read clients (such as Grafana) configured to use the old cluster during this step.

{{< admonition type="note" >}}
You can add examples here for Prometheus or OpenTelemetry configuration once they’re available or link to existing setup documentation.
{{< /admonition >}}

## Step 3. Wait for the old cluster’s ingesters to flush data

Allow enough time for all ingesters in the old cluster to flush their in-memory data to object storage.  
This usually takes about 13 hours.

During this time, both clusters are writing and both are healthy.

## Step 4. Switch read clients to the new cluster

Update read clients (such as Grafana) to query the new cluster.

Verify that the old cluster is no longer receiving queries by checking for `msg="query stats"` entries in the query-frontend logs.

## Step 5. Scale down compactors in the old cluster

Reduce the number of compactor replicas in the old cluster to zero:

```yaml
compactor:
  replicas: 0
```

Run this step immediately before scaling up the new compactors (within 30 minutes).

## Step 6. Scale out compactors in the new cluster

Increase the number of compactor replicas in the new cluster to match the old configuration.

This activates compaction in the new cluster and completes the transition for background storage management.

## Step 7. Update ruler alerting configuration

If you disabled alert sending in the new cluster earlier:

1. Disable rule evaluations in the old cluster.
2. Enable them in the new cluster by removing the override set in Step 1.

Do these actions in quick succession to prevent duplicate alerts.

## Step 8. Stop writing to the old cluster

Reconfigure write clients to send data only to the new cluster.

## Step 9. Decommission the old cluster

Once the new cluster is fully operational, decommission the old Mimir cluster.  
For example, if you’re using Helm:

```sh
helm uninstall <OLD_RELEASE_NAME>
```

At this point, your system runs entirely on the ingest storage architecture.

## Next steps

- Review [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/)
- Monitor the new cluster’s ingestion and query performance
- Verify alerting and ruler behavior after migration
