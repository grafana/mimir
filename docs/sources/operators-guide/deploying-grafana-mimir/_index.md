---
title: "Deploying Grafana Mimir on Kubernetes"
menuTitle: "Deploying on Kubernetes"
description: "Learn how to deploy Grafana Mimir on Kubernetes."
weight: 12
keywords:
  - Mimir deployment
  - Mimir Kubernetes
---

# Deploying Grafana Mimir on Kubernetes

You can use Helm or Tanka to deploy Grafana Mimir on Kubernetes.

## Helm

A [mimir-distributed](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed) Helm chart that deploys Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes/index.md#microservices-mode" >}}) is available in the [grafana/helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

## Jsonnet and Tanka

A [set of Jsonnet files]({{< relref "./jsonnet/_index.md" >}}) that you can use to deploy Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes/index.md#microservices-mode" >}}) using Jsonnet and Tanka.
