---
title: "Deploying Grafana Mimir"
menuTitle: "Deployment"
description: "Learn how to deploy Grafana Mimir."
weight: 15
keywords:
  - Mimir deployment
---

# Deploying Grafana Mimir

## On Kubernetes

You can use Helm or Jsonnet+Tanka to deploy Grafana Mimir on Kubernetes. 

### Helm

A Helm chart for deploying Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes.md#microservices-mode" >}}) is available in the grafana/helm-charts repo. The chart is called [mimir-distributed](FIXME).

### Jsonnet+Tanka

Grafana Labs also publishes [jsonnet](https://jsonnet.org/) files that can be used to deploy Grafana Mimir in [microservices mode]({{< relref "../architecture/deployment-modes.md#microservices-mode" >}}). The files can be found [here](FIXME) along with a README. 

The README explains how [tanka](https://tanka.dev/) and [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler) can be used to generate YAML manifests from the jsonnet files. Users familiar with tanka can also choose to use it directly to deploy Grafana Mimir. 