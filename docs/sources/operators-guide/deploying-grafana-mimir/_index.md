---
title: "Deploying Grafana Mimir on Kubernetes"
menuTitle: "Deploying on Kubernetes"
description: "Learn how to deploy Grafana Mimir on Kubernetes."
weight: 15
keywords:
  - Mimir deployment
  - Mimir Kubernetes
---

# Deploying Grafana Mimir on Kubernetes

You can use Helm or Tanka to deploy Grafana Mimir on Kubernetes.

## Helm

A [mimir-distributed](https://github.com/grafana/helm-charts/tree/main/charts/mimir-distributed) Helm chart that deploys Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes/index.md#microservices-mode" >}}) is available in the grafana/helm-charts repo.

## Tanka

Grafana Labs also publishes [Jsonnet](https://jsonnet.org/) files that you can use to deploy Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes/index.md#microservices-mode" >}}). To locate the Jsonnet files and a README file, refer to [Jsonnet for Mimir on Kubernetes](https://github.com/grafana/mimir/tree/main/operations/mimir).

The README explains how to use [Tanka](https://tanka.dev/) and [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler) to generate Kubernetes YAML manifests from the jsonnet files. Alternatively, if you are familiar with Tanka, you can use it directly to deploy Grafana Mimir.
