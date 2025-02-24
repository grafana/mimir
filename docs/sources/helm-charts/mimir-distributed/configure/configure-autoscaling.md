---
description: Learn how to configure Grafana Mimir autoscaling when using Helm.
menuTitle: Configure autoscaling
title: Configure Grafana Mimir autoscaling with Helm
weight: 30
---

# Configure Grafana Mimir autoscaling with Helm

{{< admonition type="warning" >}}
Autoscaling support in the Helm chart is currently experimental. Use with caution in production environments and thoroughly test in a non-production environment first.
{{< /admonition >}}

This topic explains how to configure autoscaling for Mimir components using KEDA and Kubernetes Horizontal Pod Autoscaler (HPA).

## Before you begin

- Ensure you have a running Mimir cluster deployed with Helm
- Verify you have the required permissions to modify Helm deployments
- Familiarize yourself with Kubernetes HPA concepts

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

## Supported components

Mimir Helm chart supports autoscaling for the following components:

- [Querier]({{< relref "../../references/architecture/components/querier" >}})
- [Query-frontend]({{< relref "../../references/architecture/components/query-frontend" >}})
- [Distributor]({{< relref "../../references/architecture/components/distributor" >}})

## About KEDA

KEDA is a Kubernetes operator that simplifies the setup of HPA with custom metrics from Prometheus. It consists of:

- An operator and external metrics server
- Support for multiple metric sources, including Prometheus
- Custom resources (`ScaledObject`) that define scaling parameters
- Automatic HPA resource management

For more information, refer to [KEDA documentation](https://keda.sh).

## Configure autoscaling for a new installation

This section describes how to enable autoscaling when deploying Mimir for the first time.

### Steps

1. Configure the Prometheus metrics source in your values file:
   ```yaml
   kedaAutoscaling:
     prometheusAddress: "http://prometheus.monitoring:9090"
     pollingInterval: 10
   ```

2. Enable and configure autoscaling for desired components:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       minReplicaCount: 2
       maxReplicaCount: 10
   ```

3. Deploy Mimir using Helm:
   ```bash
   helm upgrade --install mimir grafana/mimir-distributed -f values.yaml
   ```

### Expected outcome

After deployment:
- KEDA creates ScaledObject resources for configured components
- HPA resources are automatically created and begin monitoring metrics
- Components scale based on configured thresholds and behaviors

## Migrate existing deployments to autoscaling

This section describes how to safely enable autoscaling for an existing Mimir deployment.

{{< admonition type="warning" >}}
Migrating to autoscaling carries risks for cluster availability:

1. Autoscaling support in the Helm chart is currently experimental
2. Enabling autoscaling removes the `replicas` field from Deployments
3. If KEDA/HPA hasn't started autoscaling the Deployment yet, Kubernetes interprets no replicas as meaning 1 replica
4. This can cause an outage if the transition is not handled carefully
5. If you're using GitOps tools like FluxCD or ArgoCD, additional steps might be required to handle the transition smoothly

Consider testing the migration in a non-production environment first.
{{< /admonition >}}

### Before you begin

- Back up your current Helm values
- Plan for potential service disruption
- Consider testing in a non-production environment first
- Ensure you have a rollback plan ready
- Consider migrating one component at a time to minimize risk

### Steps

1. Add autoscaling configuration with `preserveReplicas` enabled:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       preserveReplicas: true  # Maintains stability during migration
       # ... autoscaling configuration ...
   ```

2. Apply the changes and verify KEDA setup:
   ```bash
   # Apply changes
   helm upgrade mimir grafana/mimir-distributed -f values.yaml

   # Verify setup
   kubectl get hpa
   kubectl get scaledobject
   kubectl describe hpa
   ```

3. After confirming KEDA is managing scaling (wait at least 2-3 polling intervals), remove `preserveReplicas`:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       # Remove preserveReplicas
   ```

4. Apply the updated configuration:
   ```bash
   helm upgrade mimir grafana/mimir-distributed -f values.yaml
   ```

### Troubleshooting

If pods scale down to 1 replica after removing `preserveReplicas`:

1. Revert changes:
   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       preserveReplicas: true
   ```

2. Verify KEDA setup:
    - Check HPA status
    - Verify metrics are being received
    - Check for conflicts with other tools
    - Ensure enough time was given for KEDA to take control (at least 2-3 polling intervals)

3. Try migration again after resolving issues

{{< admonition type="note" >}}
If you're using GitOps tools like FluxCD or ArgoCD, they might try to reconcile the state and conflict with HPA's scaling decisions. Consult your GitOps tool's documentation for handling HPA transitions.
{{< /admonition >}}

## Monitor autoscaling health

The following conditions indicate unhealthy autoscaling:

- KEDA operator is down: ScaledObject changes don't propagate to HPA
- KEDA metrics server is down: HPA can't receive updated metrics
- HPA is unable to scale: MimirAutoscalerNotActive alert fires

For production deployments, configure [high availability](https://keda.sh/docs/latest/operate/cluster/#high-availability) for KEDA.

For more information about monitoring autoscaling, refer to [Monitor Grafana Mimir]({{< relref "../../manage/monitor-grafana-mimir" >}}). 
