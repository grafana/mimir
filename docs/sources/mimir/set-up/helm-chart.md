---
description: Learn how to deploy Mimir with Helm.
menuTitle: Deploy with Helm
title: Deploy Mimir with Helm
weight: 10
---

# Deploy Mimir with Helm

The [`mimir-distributed` Helm chart](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/) for Grafana Mimir and Grafana Enterprise Metrics allows you to configure, install, and upgrade Grafana Mimir or Grafana Enterprise Metrics within a Kubernetes cluster.

## Use ingest storage

Grafana Mimir uses the ingest storage architecture by default when deployed with the Helm chart.
You can configure an external Kafka backend or disable ingest storage if needed.

For details, refer to [Run Grafana Mimir in production using the Helm chart](https://grafana.com/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/), which provides complete setup and configuration guidance for production environments.

To learn more about the ingest storage architecture and related configuration options, refer to:

- [About ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/)
- [Configure the Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)

## Example values files

The mimir-distributed Helm chart includes the following example values files:

| File name                                                                                                        | Description                                                                                                                                                                                          |
| ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`values.yaml`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml) | Contains default values for testing GEM in non-production environments using a test MinIO deployment for object storage.                                                                             |
| [`small.yaml`](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed/small.yaml)   | Contains values for a higher scale than defaults, for ingestion up to approximately one million series. Not suitable for high-availability production use, due to single replicas of key components. |
| [`large.yaml`](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed/large.yaml)   | Contains values for production use for ingestion up to approximately ten million series.                                                                                                             |

## See also

To deploy Mimir using the `mimir-distributed` Helm chart, see [Get started with Grafana Mimir using the Helm chart](/docs/helm-charts/mimir-distributed/latest/get-started-helm-charts/).
