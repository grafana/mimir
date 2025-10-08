---
description: Set up Grafana Mimir with ingest storage architecture.
menuTitle: Ingest storage
title: Set up Grafana Mimir with ingest storage architecture
weight: 20
---

# Set up Grafana Mimir with ingest storage architecture

Set up Grafana Mimir with ingest storage architecture. Ingest storage architecture provides a scalable and resilient path for high-volume metric ingestion using Kafka and object storage. It’s designed for production environments that need to efficiently handle large workloads.

## Before you begin

Before you start, ensure you have the following:

 - Kafka cluster: Used as the backend for ingest storage. You can use a managed Kafka service or host your own. For configuration details, refer to [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/).
- Performant object storage: Mimir requires high-throughput, durable object storage compatible with S3 APIs.
 - Helm: To deploy Grafana Mimir in Kubernetes using the Helm chart. For setup details, refer to [Run a production environment with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/).
 - Access to the ingest storage configuration templates: Available through the [Configure ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ingest-storage/).

## About ingest storage architecture

Ingest storage architecture decouples ingestion from query and storage components using Kafka as a durable buffer between the distributor, ingester, and store-gateway services. This design improves reliability, resilience, and scalability for high-ingestion environments.

 You can review the full system flow and component relationships in [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).

## Workflow for setting up ingest storage architecture

The following outlines the high-level setup flow for a Mimir cluster running in ingest storage mode:

1. Deploy Kafka  
   Create and configure the Kafka cluster with appropriate replication, partitions, and retention settings for your expected ingestion rate.

1. Deploy Mimir services  
   Install Grafana Mimir using Helm or Jsonnet and enable the ingest storage configuration.

1. Configure ingest storage parameters  
   Update the service configuration for the following components:
   - Distributor: Configure the Kafka producer to publish to the ingestion topics.
   - Ingester: Configure the Kafka consumer to read and process ingested metric data.
   - Ingester storage: Set up object storage credentials and paths for long-term persistence.

1. Validate ingestion and compaction  
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

 - [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/).
 - [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/).
 - [Run a production environment with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/).



