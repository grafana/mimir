---
title: "Deploying Grafana Mimir"
menuTitle: "Deployment"
description: "Learn how to deploy Grafana Mimir."
weight: 15
keywords:
  - Mimir deployment
---

# Deploying Grafana Mimir

You can use Helm or Tanka to deploy Grafana Mimir on Kubernetes. 

## Helm

A [mimir-distributed](FIXME) Helm chart that deploys Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes.md#microservices-mode" >}}) is available in the grafana/helm-charts repo.

## Tanka

Grafana Labs also publishes [jsonnet](https://jsonnet.org/) files that you can use to deploy Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes.md#microservices-mode" >}}). You can find the files [here](FIXME) along with a README file. 

The README explains how to use [tanka](https://tanka.dev/) and [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler) to generate Kubernetes YAML manifests from the jsonnet files. Alternatively, if you are familiar with tanka, you can use it directly to deploy Grafana Mimir. 
