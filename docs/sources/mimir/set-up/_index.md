---
title: Set up
menuTitle: Set up
description: Learn how to set up a Mimir server or cluster and visualize data.
aliases:
  -
weight: 15
---

<!-- This page is borrowed from Tempo, in case there are aspects that we would like to keep. -->

# Set up Mimir

To set up Mimir, you need to:

1. Plan your deployment
1. Deploy Mimir
1. Test your installation
1. (Optional) Configure Mimir services

## Plan your deployment

How you choose to deploy Mimir depends upon your tracing needs.
Mimir has two deployment modes: monolithic or microservices.

A [mimir-distributed](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed) Helm chart that deploys Grafana Mimir in [microservices mode]({{< relref "../../references/architecture/deployment-modes/index.md#microservices-mode" >}}) is available in the [grafana/helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

Alternatively, you can use a set of Jsonnet files to deploy Grafana Mimir in microservices mode using Jsonnet and Tanka.

Read [Plan your deployment]({{< relref "./deployment" >}}) to determine the best method to deploy Mimir.

## Deploy Mimir

Once you have decided how to deploy Mimir, you can install and set up Mimir. For additional samples, refer to the [Example setups]({{< relref "../getting-started/example-demo-app" >}}) topic.

Grafana Mimir is available as a [pre-compiled binary, OS_specific packaging](https://github.com/grafana/mimir/releases), and [Docker image](https://github.com/grafana/mimir/tree/main/example/docker-compose).

The following procedures provide example Mimir deployments that you can use as a starting point:

- [Deploy with Helm]({{< relref "helm-chart" >}}) (microservices and monolithic)
- [Deploy on Linux]({{< relref "linux">}}) (monolithic)
- [Deploy on Kubernetes using Tanka]({{< relref "tanka">}}) (microservices)

You can also use Docker to deploy Mimir using [the Docker examples](https://github.com/grafana/mimir/tree/main/example/docker-compose).

## Test your installation

Once Mimir is deployed, you can test Mimir by visualizing traces data:

- Using a [test application for a Mimir cluster]({{< relref "set-up-test-app" >}}) for the Kubernetes with Tanka setup
- Using a [Docker example]({{< relref "linux">}}) to test the Linux setup

These visualizations test Kubernetes with Tanka and Linux procedures. They do not check optional configuration you have enabled.

## (Optional) Configure Mimir services

Explore Mimir's features by learning about [available features and configurations]({{< relref "../configuration" >}}).

If you would to see a simplified, annotated example configuration for Mimir, the [Introduction To MLT](https://github.com/grafana/intro-to-mlt) example repository contains a [configuration](https://github.com/grafana/intro-to-mlt/blob/main/mimir/mimir.yaml) for a monolithic instance.
