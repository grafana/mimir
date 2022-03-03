---
title: "Running Grafana Mimir on Kubernetes"
description: ""
weight: 40
---

# Running Grafana Mimir on Kubernetes

Because Grafana Mimir is designed to run multiple instances of each component
(ingester, querier, etc.), you probably want to automate the placement
and shepherding of these instances. Most users choose Kubernetes to do
this, but it's not mandatory.

## Configuration

### Resource requests

Each container should specify resource requests so that the Kubernetes
scheduler can place them on a node with sufficient capacity.

For example an ingester might request:

```
        resources:
          requests:
            cpu: 4
            memory: 10Gi
```

The specific values here should be adjusted based on your own
experiences running Grafana Mimir - they are very dependent on rate of data
arriving and other factors such as series churn.

### Take extra care with ingesters

Ingesters hold hours of timeseries data in memory; you can configure
Grafana Mimir to replicate the data but you should take steps to avoid losing
all replicas at once:

- Don't run multiple ingesters on the same node.
- Don't run ingesters on preemptible/spot nodes.
- Spread out ingesters across racks / availability zones / whatever
  applies in your datacenters.

The standard Grafana Mimir Kubernetes configuration avoids scheduling multiple ingesters
on the same nodes like this:

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            name: ingester
        topologyKey: kubernetes.io/hostname
```

Give plenty of time for an ingester to flush data to the store when shutting
down; for Kubernetes this looks like:

```
      terminationGracePeriodSeconds: 1200
```

Ask Kubernetes to limit rolling updates to one ingester at a time, and
signal the old one to stop before the new one is ready:

```
  strategy:
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 1
```

Ingesters provide an HTTP hook to signal readiness when all is well;
this is valuable because it stops a rolling update at the first
problem:

```
        readinessProbe:
          httpGet:
            path: /ready
            port: 80
          initialDelaySeconds: 15
          timeoutSeconds: 1
```

We do not recommend configuring a liveness probe on ingesters -
killing them is a last resort and should not be left to a machine.
