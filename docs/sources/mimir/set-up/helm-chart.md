---
description: Learn how to deploy Mimir with Helm.
menuTitle: Deploy with Helm
title: Deploy Mimir with Helm
weight: 10
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Deploy Mimir with Helm

The [`mimir-distributed` Helm chart](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/) for Grafana Mimir and Grafana Enterprise Metrics allows you to configure, install, and upgrade Grafana Mimir or Grafana Enterprise Metrics within a Kubernetes cluster.

## Example values files

The mimir-distributed Helm chart includes the following example values files:

| File name                                                                                                        | Description                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| [`values.yaml`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml) | Contains default values for testing GEM in non-production environments using a test MinIO deployment for object storage. |
| [`small.yaml`](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed/small.yaml)   | Contains values for production use for ingestion up to approximately one million series.                                 |
| [`large.yaml`](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed/large.yaml)   | Contains values for production use for ingestion up to approximately ten million series.                                 |

## See also

To deploy Mimir using the `mimir-distributed` Helm chart, see [Get started with Grafana Mimir using the Helm chart](/docs/helm-charts/mimir-distributed/latest/get-started-helm-charts/).
