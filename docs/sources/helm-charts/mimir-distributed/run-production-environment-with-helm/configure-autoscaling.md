---
aliases:
  - ../../operators-guide/deploying-grafana-mimir/helm/configuring-autoscaling/
  - configuring-autoscaling/
  - ../../operators-guide/deploy-grafana-mimir/helm/configure-autoscaling/
description: Learn how to configure Grafana Mimir autoscaling when using Helm.
menuTitle: Configure autoscaling
title: Configure Grafana Mimir autoscaling with Helm
weight: 30
---

# Configure Grafana Mimir autoscaling with Helm

Mimir Helm chart supports autoscaling for the following components:

- [Querier]({{< relref "../../references/architecture/components/querier" >}})
- [Query-frontend]({{< relref "../../references/architecture/components/query-frontend" >}})
- [Distributor]({{< relref "../../references/architecture/components/distributor" >}})

Autoscaling is based on Prometheus metrics and uses [Kubernetes-based Event Driven Autoscaler (KEDA)](https://keda.sh). KEDA creates and manages Kubernetes' Horizontal Pod Autoscaler (HPA) resources based on custom metrics from Prometheus.

## Prerequisites

To use autoscaling, you need:

1. KEDA installed in your Kubernetes cluster
2. Prometheus metrics available for scaling decisions

## How KEDA works

KEDA is a Kubernetes operator that simplifies the setup of HPA with custom metrics (Prometheus in our case).

Kubernetes HPA, out of the box, cannot autoscale based on metrics scraped by Prometheus, but it allows configuring a custom metrics API server which proxies metrics from a data source (e.g. Prometheus) to Kubernetes. Setting up the custom metrics API server for Prometheus in Kubernetes can be complex, so KEDA offers an operator to set it up automatically.

### KEDA in a nutshell

- Runs an operator and an external metrics server
- The metrics server supports proxying for many metric sources, including Prometheus
- The operator watches for `ScaledObject` custom resources, which define:
  - Minimum and maximum replicas
  - Scaling trigger metrics
  - Target Deployment or StatefulSet
- KEDA creates and manages the related HPA resources automatically

Refer to [KEDA documentation](https://keda.sh) for more information.

## How to enable autoscaling

To enable autoscaling for a component, you need to:

1. Configure the Prometheus metrics source
2. Enable KEDA autoscaling for the desired component
3. Configure the scaling parameters

Here's an example configuration that enables autoscaling for the querier component:

```yaml
kedaAutoscaling:
  # Prometheus metrics endpoint for scaling decisions
  prometheusAddress: "http://prometheus.monitoring:9090"
  # Optional custom headers for Prometheus requests
  customHeaders: {}
  # Polling interval for checking metrics
  pollingInterval: 10

querier:
  kedaAutoscaling:
    enabled: true
    minReplicaCount: 2
    maxReplicaCount: 10
    # Target number of in-flight requests per querier
    querySchedulerInflightRequestsThreshold: 12
    # Optional: Enable predictive scaling based on historical patterns
    predictiveScalingEnabled: false
    predictiveScalingPeriod: 6d23h30m
    predictiveScalingLookback: 30m
    # Configure scaling behavior
    behavior:
      scaleDown:
        policies:
          - periodSeconds: 120
            type: Percent
            value: 10
        stabilizationWindowSeconds: 600
      scaleUp:
        policies:
          - periodSeconds: 120
            type: Percent
            value: 50
          - periodSeconds: 120
            type: Pods
            value: 15
        stabilizationWindowSeconds: 60
```

Similar configuration can be applied to other components that support autoscaling:

```yaml
distributor:
  kedaAutoscaling:
    enabled: true
    minReplicaCount: 2
    maxReplicaCount: 10
    targetCPUUtilizationPercentage: 80
    targetMemoryUtilizationPercentage: 80

query_frontend:
  kedaAutoscaling:
    enabled: true
    minReplicaCount: 2
    maxReplicaCount: 10
    targetCPUUtilizationPercentage: 80
    targetMemoryUtilizationPercentage: 80
```

## Scaling metrics and thresholds

Each component uses different metrics for scaling decisions:

### Querier
- In-flight requests from the query scheduler
- Request duration
- Optional predictive scaling based on historical patterns

### Distributor
- CPU utilization
- Memory utilization

### Query Frontend
- CPU utilization
- Memory utilization

## Migration to autoscaling

When migrating existing deployments to use autoscaling, you can use the `preserveReplicas` option to ensure a smooth transition:

```yaml
querier:
  kedaAutoscaling:
    enabled: true
    preserveReplicas: true  # Keeps existing replica count during migration
```

This option allows you to migrate from non-autoscaled to autoscaled deployments without losing your current replica count. For more details, see [helm: autoscaling migration procedure](https://github.com/grafana/mimir/issues/7367).

## What happens if KEDA is unhealthy

The autoscaling of deployments is always managed by HPA, which is a native Kubernetes feature. If KEDA becomes unhealthy:

- If the KEDA operator is down: Changes to `ScaledObject` resources won't be reflected in HPA until the operator recovers
- If the KEDA metrics API server is down: HPA won't receive updated metrics and will stop scaling until metrics are available
- The deployment continues to run but won't scale in response to load changes

{{< admonition type="note" >}}
Use a [high availability](https://keda.sh/docs/latest/operate/cluster/#high-availability) KEDA configuration if autoscaling is critical for your use case.
{{< /admonition >}}

The [alert `MimirAutoscalerNotActive`]({{< relref "../../manage/monitor-grafana-mimir" >}}) fires if HPA is unable to scale the deployment for any reason (e.g. unable to scrape metrics from KEDA metrics API server). 