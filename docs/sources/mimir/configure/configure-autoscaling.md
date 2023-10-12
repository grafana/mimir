---
aliases:
  - ../../configuring/configuring-autoscaling/
  - configuring/autoscaling
description: Learn how to configure Grafana Mimir autoscaling
title: Configure Grafana Mimir autoscaling
weight: 110
---

# Configure Grafana Mimir autoscaling

The Grafana Mimir Helm chart supports autoscaling for the following components:

- [alertmanager]({{< relref "../architecture/components/alertmanager.md" >}})
- [compactor]({{< relref "../architecture/components/compactor/index.md" >}})
- [distributor]({{< relref "../architecture/components/distributor.md" >}})
- [ingester]({{< relref "../architecture/components/ingester.md" >}})
- [querier]({{< relref "../architecture/components/querier.md" >}})
- [query-frontend]({{< relref "../architecture/components/query-frontend/index.md" >}})
- [query-scheduler]({{< relref "../architecture/components/query-scheduler/index.md" >}})
- [store-gateway]({{< relref "../architecture/components/store-gateway.md" >}})

Autoscaling in the Mimir Helm chart is implemented using the Kubernetes Horizontal Pod Autosacler (HPA).

## How Kubernetes HPA works

Refer to Kubernetes [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) documentation to have a full understanding of how HPA works.

## How to enable autoscaling

The following Helm configuration snippet shows an example of how to enable Mimir autoscaling in the Helm chart for the distributor:

```yaml
distributor:
  hpa:
    enabled: true
    minReplicas: 3
    maxReplicas: 6
    targetMemoryUtilizationPercentage: 60
    targetCPUUtilizationPercentage: 60
    behavior:
      scaleDown:
        stabilizationWindowSeconds: 180
```

> **Note**: Enabling the HPA for a given component will cause it's `replicas` field to be ignored. Instead, the number of pods are controlled using `autoscaling.minReplicas` and `autoscaling.maxReplicas`.

## Using autoscaling with zone-aware replication

When autoscaling is enabled for a component that supports zone-aware replication, such as the alertmanger, ingester, and store-gateway, individual HPAs will be deployed for each zone. The minimum replicas per zone will then be adjusted as `ceil(autoscaling.minReplicas) / number_of_zones)` to ensure an even spread of resources across each zone at the minimum level.

> E.g. if `autoscaling.minReplicas` is set to 4 and there are 3 zones, then 4/3=1.33, and rounding up gives us a new `minReplicas` value of 2 pods per zone.

The value of `maxReplicas` is taken as is.
