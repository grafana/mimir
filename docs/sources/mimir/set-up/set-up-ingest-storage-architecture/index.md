---
title: Set up ingest storage architecture
menuTitle: Set up ingest storage architecture
description: Learn how to set up ingest storage architecture for Grafana Mimir using a Kafka-compatible backend.
weight: 20
---

# Set up ingest storage architecture

Set up ingest storage architecture for Grafana Mimir. Ingest storage architecture decouples the Mimir read and write paths using a Kafka-compatible backend. This process improves scalability and reliability in production environments.

Follow this workflow to set up ingest storage architecture:

1. [Before you begin](#before-you-begin)
2. [Deploy or connect to Kafka](#deploy-or-connect-to-kafka)
3. [Enable ingest storage in Mimir](#enable-ingest-storage-in-mimir)
4. [Configure Kafka connection parameters](#configure-kafka-connection-parameters)
5. [Verify your deployment](#verify-your-deployment)

For an overview of how ingest storage architecture works, refer to [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).

Before you begin, review the [architecture diagram](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-ingest-storage-architecture/#how-ingest-storage-architecture-works) to learn how components interact across the write and read paths.

## Before you begin

Before you begin, ensure you have the following:

- A Grafana Mimir 3.0 or later deployment. Ingest storage is stable and the default architecture for Grafana Mimir versions 3.0 and later.
- A Kafka-compatible backend, for example, Apache Kafka, Warpstream, Redpanda, or AWS MSK.
- [Helm](https://helm.sh/docs/intro/install/) installed to manage deployments.
- Access to the [Grafana Mimir distributed Helm chart](https://grafana.com/docs/helm-charts/mimir-distributed/next/run-production-environment-with-helm/).
- Proper networking between Mimir components and your Kafka cluster.

## Deploy or connect to Kafka

Deploy a production-grade Kafka cluster or connect to an existing managed service.

By default, Mimir expects Kafka to be available at:

```
kafka.<namespace>.svc.<cluster_domain>:9092
```

You can customize Kafka broker addresses and topic configuration later in your Helm or Jsonnet configuration.

For Kafka setup guidance, refer to [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/).

---

## Enable ingest storage in Mimir

You can enable ingest storage using either Helm or Jsonnet.

### Enable with Helm

If you're deploying Grafana Mimir using the distributed Helm chart, set the following parameter in your values file or command line:

```yaml
mimir:
  configuration:
    ingest_storage_enabled: true
```

Apply your Helm release:

```bash
helm upgrade --install mimir grafana/mimir-distributed -f values.yaml
```

### Enable with Jsonnet

If you manage Mimir with Jsonnet, add the following configuration:

```jsonnet
{
  _config+:: {
    ingest_storage_enabled: true,
  },
}
```

For full details, refer to [Configure ingest storage with Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/).

---

## Configure Kafka connection parameters

You can customize Kafka connection options in Helm or Jsonnet to match your Kafka deployment.

### Common configuration parameters

| Component | Parameter | Description |
|------------|------------|-------------|
| Distributor | `ingest-storage.kafka.produce-endpoint` | Kafka broker addresses used for producing writes |
| Ingester | `ingest-storage.kafka.consume-endpoint` | Kafka broker addresses used for consuming records |
| Global | `ingest-storage.kafka.topic` | Topic name used for ingestion (default: `ingest`) |
| Global | `ingest-storage.kafka.auto-create-topic-default-partitions` | Default number of partitions to create (default: 1000) |

Ensure that the Kafka topic has at least as many partitions as there are ingesters per zone.  
This allows Mimir to evenly distribute load and maintain high availability.

---

## Verify your deployment

After deployment:

1. Confirm that the distributors are producing records to Kafka.  
   Check distributor logs for successful `Produce API` acknowledgements.

2. Verify that ingesters are consuming from Kafka partitions.  
   Each ingester should consume from one partition and persist offsets in its Kafka consumer group.

3. Ensure blocks are uploaded to long-term storage as expected.  
   Check that new blocks appear in your object storage bucket every two hours (default compaction interval).

---

## Next steps

- [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-ingest-storage-architecture/)
- [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)
- [Run Grafana Mimir in production with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/next/run-production-environment-with-helm/)
- [Configure ingest storage architecture with Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/)


