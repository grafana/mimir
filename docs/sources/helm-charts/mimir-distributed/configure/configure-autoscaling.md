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

You can configure autoscaling for Mimir components using (Kubernetes Event-driven Autoscaling) KEDA and Kubernetes Horizontal Pod Autoscaler (HPA).

## Before you begin

- Ensure you have a running Mimir cluster deployed with Helm.
- Verify you have the required permissions to modify Helm deployments.
- Familiarize yourself with Kubernetes HPA concepts.

## Prerequisites

To use autoscaling, you need:

1. KEDA installed in your Kubernetes cluster
2. Prometheus metrics available for scaling decisions

{{< admonition type="warning" >}}
Don't use the same Mimir cluster for storing and querying autoscaling metrics. Using the same cluster can create a dangerous feedback loop.

For instance, if the Mimir cluster becomes unavailable, autoscaling stops working, because it cannot query the metrics. This prevents the cluster from automatically scaling up during high load or recovery. This inability to scale further exacerbates the cluster's unavailability, which might, in turn, prevent the cluster from recovering.

Instead, use a separate Prometheus instance or a different metrics backend for autoscaling metrics.
{{< /admonition >}}

## Supported components

The Mimir Helm chart supports autoscaling for the following components:

- [Querier](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/querier/)
- [Query-frontend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/query-frontend/)
- [Distributor](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/distributor/)

## About KEDA

KEDA is a Kubernetes operator that simplifies the setup of HPA with custom metrics from Prometheus. It consists of:

- An operator and external metrics server
- Support for multiple metric sources, including Prometheus
- Custom resources (`ScaledObject`) that define scaling parameters
- Automatic HPA resource management

For more information, refer to the [KEDA documentation](https://keda.sh).

## Configure autoscaling for a new installation

Follow these steps to enable autoscaling when deploying Mimir for the first time.

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

- KEDA creates `ScaledObject` resources for configured components.
- HPA resources are automatically created and begin monitoring metrics.
- Components scale based on configured thresholds and behaviors.

## Migrate existing deployments to autoscaling

Follow these steps to enable autoscaling for an existing Mimir deployment.

{{< admonition type="warning" >}}
Autoscaling support in the Helm chart is currently experimental. Migrating to autoscaling carries risks for cluster availability.

Enabling autoscaling removes the `replicas` field from deployments. If KEDA/HPA hasn't started autoscaling a deployment yet, Kubernetes interprets no replicas as meaning 1 replica. This can cause an outage if the transition is not handled carefully. If you're using GitOps tools like FluxCD or ArgoCD, you might need to take additional steps to manage the transition.

Consider testing the migration in a non-production environment first.
{{< /admonition >}}

### Before you begin

- Back up your current Helm values.
- Plan for potential service disruption.
- Consider testing in a non-production environment first.
- Ensure you have a rollback plan ready.
- Consider migrating one component at a time to minimize risk.

### Steps

1. Add the autoscaling configuration with `preserveReplicas` enabled:

   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       preserveReplicas: true # Maintains stability during migration
       # ... autoscaling configuration ...
   ```

2. Apply the changes and verify the KEDA setup:

   ```bash
   # Apply changes
   helm upgrade mimir grafana/mimir-distributed -f values.yaml

   # Verify setup
   kubectl get hpa
   kubectl get scaledobject
   kubectl describe hpa
   ```

3. Wait 2-3 polling intervals to confirm that KEDA is managing scaling.
4. Remove `preserveReplicas`.

   ```yaml
   querier:
     kedaAutoscaling:
       enabled: true
       # Remove preserveReplicas
   ```

5. Apply the updated configuration:
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

3. Try migrating again after resolving issues.

{{< admonition type="note" >}}
If you're using GitOps tools like FluxCD or ArgoCD, they might try to reconcile the state and conflict with HPA's scaling decisions. Consult your GitOps tool's documentation for handling HPA transitions.
{{< /admonition >}}

## Monitor autoscaling health

The following conditions indicate unhealthy autoscaling:

- KEDA operator is down: `ScaledObject` changes don't propagate to HPA.
- KEDA metrics server is down: HPA can't receive updated metrics.
- HPA is unable to scale: `MimirAutoscalerNotActive` alert fires.

For production deployments, configure [high availability](https://keda.sh/docs/latest/operate/cluster/#high-availability) for KEDA.

For more information about monitoring autoscaling, refer to [Monitor Grafana Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/).
