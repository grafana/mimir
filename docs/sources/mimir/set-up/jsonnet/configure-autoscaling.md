---
aliases:
  - ../../operators-guide/deploying-grafana-mimir/jsonnet/configuring-autoscaling/
  - configuring-autoscaling/
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/configure-autoscaling/
description: Learn how to configure Grafana Mimir autoscaling when using Jsonnet.
menuTitle: Configure autoscaling
title: Configure Grafana Mimir autoscaling with Jsonnet
weight: 30
---

# Configure Grafana Mimir autoscaling with Jsonnet

Mimir Jsonnet supports autoscaling for the following components:

- [Ruler Querier]({{< relref "../../references/architecture/components/ruler" >}})
- [Querier]({{< relref "../../references/architecture/components/querier" >}})
- [Distributor]({{< relref "../../references/architecture/components/distributor" >}})

Autoscaling, which is based on Prometheus metrics and [Kubernetes-based Event Driven Autoscaler (KEDA)](https://keda.sh), uses Kubernetes’ Horizontal Pod Autoscaler (HPA).

HPA is not configured directly in Jsonnet but it's created and updated by KEDA.
KEDA is an operator, running in the Kubernetes cluster, which is responsible to simplify the setup of HPA with custom metrics (Prometheus in our case).

## How KEDA works

KEDA is a Kubernetes operator aiming to simplify the wiring between HPA and Prometheus.

Kubernetes HPA, out of the box, is not capable of autoscaling based on metrics scraped by Prometheus, but it allows to configure a custom metrics API server which proxies metrics from a data source (e.g. Prometheus) to Kubernetes.
Setting up the custom metrics API server for Prometheus in a Kubernetes can be a tedious operation, so KEDA offers an operator to set it up automatically.
KEDA supports proxying metrics for a variety of sources, including Prometheus.

### KEDA in a nutshell

- Runs an operator and an external metrics server.
- The metrics server supports proxying for many metric sources, including Prometheus.
- The operator watches for `ScaledObject` custom resource definition (CRD), defining the minimum and maximum replicas, and scaling trigger metrics of a Deployment or StatefulSet, and then configures the related HPA resource. You don't create the HPA resource in Kubernetes, but the operator creates it for you whenever a `ScaledObject` CRD is created (and keeps it updated for its whole lifecycle).

Refers to [KEDA documentation](https://keda.sh) for more information.

### What happens if KEDA is unhealthy

The autoscaling of deployments is always managed by HPA, which is a native Kubernetes feature.
KEDA, as we use it, never changes the number of replicas of Mimir Deployments or StatefulSets.

However, if KEDA is not running successfully, there are consequences for Mimir autoscaling too:

- `keda-operator` is down (critical): as the operator is the single point of truth, it will not request the metrics when `keda-operator-metrics-apiserver` requests them. Changes to `ScaledObject` CRD will not be reflected to the HPA until the operator is back online. The deployment (e.g. queriers) will keep working but, if there is a surge of traffic, HPA will not be able to detect it due to a lack of metrics and so will not scale up.
- `keda-operator-metrics-apiserver` is down (critical): HPA is not able to fetch updated metrics and it will stop scaling the deployment until metrics will be back. The deployment (e.g. queriers) will keep working but, if there is a surge of traffic, HPA will not be able to detect it due to a lack of metrics and so will not scale up.
- `keda-admission-webhooks` is down (not critical): CRD validation will not be executed. Based on KEDA's configuration, this can block changes on CRDs until the service is restored. HPA functionality is not affected.

{{< admonition type="note" >}}
Use a [high availability](https://keda.sh/docs/latest/operate/cluster/#high-availability) KEDA configuration if autoscaling is critical for your use case.
{{< /admonition >}}

The [alert `MimirAutoscalerNotActive`]({{< relref "../../manage/monitor-grafana-mimir" >}}) fires if HPA is unable to scale the deployment for any reason (e.g. unable to scrape metrics from KEDA metrics API server).

## How Kubernetes HPA works

Refer to Kubernetes [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) documentation to have a full understanding of how HPA works.

## How to enable autoscaling

The following Jsonnet configuration snippet shows an example of how to enable Mimir autoscaling with Jsonnet:

```jsonnet
local mimir = import 'mimir/mimir.libsonnet';

mimir {
    _config+:: {
        autoscaling_prometheus_url: 'http://prometheus.default:9090/prometheus',
        // Enable queriers autoscaling.
        autoscaling_querier_enabled: true,
        autoscaling_querier_min_replicas: 10,
        autoscaling_querier_max_replicas: 40,
        // Enable ruler queriers autoscaling.
        autoscaling_ruler_querier_enabled: true,
        autoscaling_ruler_querier_min_replicas: 10,
        autoscaling_ruler_querier_max_replicas: 40,
        // Enable distributor autoscaling.
        autoscaling_distributor_enabled: true,
        autoscaling_distributor_min_replicas: 10,
        autoscaling_distributor_max_replicas: 40,
    }
}
```

{{< admonition type="note" >}}
The Mimir Jsonet doesn't install KEDA.

To install KEDA, follow the instructions in [Deploying KEDA](https://keda.sh/docs/latest/deploy/).
{{< /admonition >}}

## How to disable autoscaling

There are two options to disable autoscaling in a Mimir cluster:

1. Set minimum replicas = maximum replicas.
2. Decommission HPA.

### Set minimum replicas = maximum replicas

If KEDA and Kubernetes HPA work correctly but the HPA configuration (metric and threshold) are not giving the expected results (e.g. not scaling up when required), a simple solution to bypass the autoscaling algorithm is to set the minimum and maximum replicas to the same value. (e.g. `autoscaling_querier_min_replicas: 40` and `autoscaling_querier_max_replicas: 40`).

### Decommission HPA

To fully decommission HPA in a Mimir cluster you have to:

1. Disable the relevant autoscaler (e.g. `autoscaling_querier_enabled: false`).
2. Manually set the expected number of replicas for the given Mimir component.

The following example shows how to disable querier autoscaler and configure querier Deployment with 10 replicas:

```jsonnet
local k = import 'github.com/grafana/jsonnet-libs/ksonnet-util/kausal.libsonnet';
local deployment = k.apps.v1.deployment;

mimir {
    _config+:: {
        autoscaling_querier_enabled: false,
    },

    querier_deployment+: deployment.mixin.spec.withReplicas(10),
}
```
