---
aliases:
  - operators-guide/deploy-grafana-mimir/
title: Set up
menuTitle: Set up
description: Learn how to set up a Grafana Mimir server or cluster and visualize data.
keywords:
  - Mimir deployment
  - Mimir Kubernetes
weight: 15
---

<!-- This page is borrowed from Tempo, in case there are aspects that we would like to keep. -->

# Set up Mimir

To set up Mimir, you need to:

1. Plan your deployment
1. Deploy Mimir
<!-- 1. Test your installation -->
1. (Optional) Configure Mimir services

## Plan your deployment

How you choose to deploy Mimir depends upon your tracing needs.
Mimir has two deployment modes: monolithic or microservices.

A [mimir-distributed](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed) Helm chart that deploys Grafana Mimir in [microservices mode]({{< relref "../references/architecture/deployment-modes/index.md#microservices-mode" >}}) is available in the [grafana/helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

Alternatively, you can use a set of Jsonnet files to deploy Grafana Mimir in microservices mode using Jsonnet and Tanka.

## Deploy Mimir

Once you have decided how to deploy Mimir, you can install and set up Mimir.

Grafana Mimir is available as a [pre-compiled binary, OS_specific packaging](https://github.com/grafana/mimir/releases), and [Docker image](https://github.com/grafana/mimir/tree/main/example/docker-compose).

<!-- -->
The following procedures provide example Mimir deployments that you can use as a starting point:

- Deploy with Helm (microservices and monolithic)
- Deploy on Linux (monolithic)
- [Deploy on Kubernetes using Tanka]({{< relref "jsonnet/deploy">}}) (microservices)

-->

You can also use Docker to deploy Mimir using [the Docker examples](https://github.com/grafana/mimir/tree/main/example/docker-compose).

<!--
## Test your installation

Once Mimir is deployed, you can test Mimir by visualizing metrics data:

- Using a test application for a Mimir cluster for the Kubernetes with Tanka setup
- Using a Docker example to test the Linux setup

These visualizations test Kubernetes with Tanka and Linux procedures. They do not check optional configuration you have enabled.

-->

## (Optional) Configure Mimir services

Explore Mimir's features by learning about [available features and configurations]({{< relref "../references/configuration-parameters" >}}).

If you would to see a simplified, annotated example configuration for Mimir, the [Introduction To MLT](https://github.com/grafana/intro-to-mlt) example repository contains a [configuration](https://github.com/grafana/intro-to-mlt/blob/main/mimir/mimir.yaml) for a monolithic instance.
