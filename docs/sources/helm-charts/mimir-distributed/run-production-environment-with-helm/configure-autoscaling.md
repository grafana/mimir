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

{{< admonition type="warning" >}}
Autoscaling support in the Helm chart is currently experimental. Use with caution in production environments.
{{< /admonition >}}

Mimir Helm chart supports autoscaling for the following components:

- [Querier]({{< relref "../../references/architecture/components/querier" >}})
- [Query-frontend]({{< relref "../../references/architecture/components/query-frontend" >}})
- [Distributor]({{< relref "../../references/architecture/components/distributor" >}})

Autoscaling is based on Prometheus metrics and uses [Kubernetes-based Event Driven Autoscaler (KEDA)](https://keda.sh). KEDA creates and manages Kubernetes' Horizontal Pod Autoscaler (HPA) resources based on custom metrics from Prometheus.

## Prerequisites

To use autoscaling, you need:

1. KEDA installed in your Kubernetes cluster
2. Prometheus metrics available for scaling decisions

{{< admonition type="warning" >}}
Do not use the same Mimir or Grafana Enterprise Metrics cluster for storing and querying autoscaling metrics. Using the same cluster can create a dangerous feedback loop:

1. If the Mimir/GEM cluster becomes unavailable, autoscaling will stop working because it cannot query the metrics
2. This can prevent the cluster from automatically scaling up during high load or recovery
3. The inability to scale can further exacerbate the cluster's unavailability
4. This may prevent the cluster from recovering

Instead, use a separate Prometheus instance or a different metrics backend for autoscaling metrics.
{{< /admonition >}}

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

There are two scenarios for enabling autoscaling:
1. Enabling it on a fresh installation
2. Migrating existing deployments to use autoscaling

### Fresh installation

For a fresh installation, you need to:

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

### Migrating existing deployments

{{< admonition type="warning" >}}
Autoscaling support in the Helm chart is currently experimental and migrating existing deployments carries risks for the availability of the cluster. The main risk is that enabling autoscaling removes the `replicas` field from Deployments. If KEDA/HPA hasn't started autoscaling the Deployment yet, Kubernetes interprets no replicas as meaning 1 replica, which can cause an outage. Consider testing the migration in a non-production environment first.
{{< /admonition >}}

To safely migrate an existing deployment to autoscaling:

1. Configure the Prometheus metrics source and autoscaling parameters as shown above, but add the `preserveReplicas` option to maintain stability during migration:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       preserveReplicas: true  # Keeps existing replica count during migration
       # ... rest of the autoscaling configuration ...
   ```

2. Deploy the changes and wait for KEDA to set up properly. Verify this by checking:
   ```bash
   # Check that HPA is created and active
   kubectl get hpa
   
   # Check that ScaledObject is created
   kubectl get scaledobject
   
   # Verify HPA is receiving metrics (Status column should show metrics)
   kubectl describe hpa
   ```

3. After confirming KEDA is managing scaling (usually after a few minutes, depending on the `pollingInterval`), remove `preserveReplicas`:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       # preserveReplicas setting removed
       # ... rest of the configuration ...
   ```

4. Deploy the changes and monitor your pods to ensure they maintain the expected replica count.

{{< admonition type="note" >}}
If you're using GitOps tools like FluxCD or ArgoCD, additional steps might be required to handle the transition smoothly. These tools might try to reconcile the state and conflict with HPA's scaling decisions. Consult your GitOps tool's documentation for handling HPA transitions.
{{< /admonition >}}

If you observe your pods scaling down to 1 replica after removing `preserveReplicas`:
1. Immediately revert by setting `preserveReplicas: true` again
2. Verify that KEDA and HPA are properly set up and receiving metrics
3. Ensure there are no conflicts with other tools managing the Deployment's replicas
4. Try the migration again after confirming all prerequisites are met

For more details about the migration procedure and its implementation, see [helm: autoscaling migration procedure](https://github.com/grafana/mimir/issues/7367).

## What happens if KEDA is unhealthy

The autoscaling of deployments is always managed by HPA, which is a native Kubernetes feature. If KEDA becomes unhealthy:

- If the KEDA operator is down: Changes to `ScaledObject` resources won't be reflected in HPA until the operator recovers
- If the KEDA metrics API server is down: HPA won't receive updated metrics and will stop scaling until metrics are available
- The deployment continues to run but won't scale in response to load changes

{{< admonition type="note" >}}
Use a [high availability](https://keda.sh/docs/latest/operate/cluster/#high-availability) KEDA configuration if autoscaling is critical for your use case.
{{< /admonition >}}

The [alert `MimirAutoscalerNotActive`]({{< relref "../../manage/monitor-grafana-mimir" >}}) fires if HPA is unable to scale the deployment for any reason (e.g. unable to scrape metrics from KEDA metrics API server). 