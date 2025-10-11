---
description: Set up Grafana Mimir with ingest storage architecture.
menuTitle: Set up ingest storage
title: Set up Grafana Mimir with ingest storage architecture
weight: 20
---

# Set up Grafana Mimir with ingest storage architecture

Ingest storage architecture provides a scalable and resilient path for high-volume metric ingestion using Kafka and object storage. It's designed for production environments that need to efficiently handle large workloads.

## Before you begin

Before you start, ensure you have the following:

 - Kafka cluster: Used as the backend for ingest storage. You can use a managed Kafka service or host your own. For configuration details, refer to [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/).
- Performant object storage: Mimir requires high-throughput, durable object storage compatible with S3 APIs.
 - Helm: To deploy Grafana Mimir in Kubernetes using the Helm chart. For setup details, refer to [Run a production environment with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/).
 - Access to the ingest storage configuration templates: Refer to [Configure ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/).

## About ingest storage architecture

Ingest storage architecture decouples ingestion from query and storage components using Kafka as a durable buffer between the distributor and ingester services. This design improves reliability, resilience, and scalability for high-ingestion environments.

 You can review the full system flow and component relationships in [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).

## Workflow for setting up ingest storage architecture

The following outlines the high-level setup workflow for a Mimir cluster running in ingest storage mode:

1. Deploy Kafka.  

   Create and configure the Kafka cluster with appropriate replication, partitions, and retention settings for your expected ingestion rate. Size Kafka topics according to your Kafka vendor’s guidance and workload needs. Refer to the following resources:
   - [Creating topics and specifying partitions/replication factor](https://kafka.apache.org/documentation/)
   - [Partition count guidance and operational considerations](https://docs.confluent.io/kafka/operations-tools/partition-determination.html)
   - [Apache Kafka topic configuration reference](https://kafka.apache.org/38/generated/topic_config.html)

1. Deploy Mimir services.  

   Install Grafana Mimir and enable the ingest storage configuration.

1. Configure ingest storage parameters.

   Update your configuration for the following components:
   - [Distributor](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/distributor/): Configure the Kafka producer to publish to the ingestion topics.
   - [Ingester](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/ingester/): Configure the Kafka consumer to read and process ingested metric data, and connect to object storage for writing blocks.
   - [Compactor](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/compactor/): Ensure the compactor shares access to the same object storage bucket for block compaction.

1. Validate ingestion and compaction.
 
   After deployed, confirm that distributors are writing to Kafka and ingesters are consuming messages successfully. Verify that metrics are stored in your object storage backend.

## Configuration overview

You can configure ingest storage either via Helm values or through Jsonnet configuration.

Key configuration parameters include:

| Configuration parameter     | Description                                                   |
| --------------------------- | ------------------------------------------------------------- |
| `distributor.kafka.producer` | Kafka producer configuration for the distributor service.     |
| `ingester.kafka.consumer`     | Kafka consumer configuration for the ingester service.        |
| `ingester.storage`            | Object storage configuration for persistent metric blocks.    |
| `compactor.storage`           | Shared object storage for compacted blocks.                   |

 Refer to [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/) and [Configure ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/) for complete parameter details.

## See also

 - [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/)
 - [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)
 - [Run a production environment with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/)



