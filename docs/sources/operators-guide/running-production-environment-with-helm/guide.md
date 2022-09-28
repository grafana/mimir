---
title: "Run in production using the Helm chart"
menuTitle: "Run in production using the Helm chart"
description: "Learn what's necessary to make the mimir-distributed Helm chart ready for production."
weight: 90
---

# Run in production using the Helm chart

## Overview

[Getting started with Grafana Mimir using the Helm chart]({{< relref "../deploy-grafana-mimir/getting-started-helm-charts" >}}) already covered
how to set up a Grafana Mimir on a local Kubernetes cluster or a small development environment. This article will guide
you through the process of extending the foundation there and deploying Grafana Mimir to production.

"Production" has different meaning to different people. The meaning used in this article is for an environment where
you want to benefit from the highly available and horizontally scalable features of Grafana Mimir. The deployment may be
used to ingest metrics coming from a development environment or from a customer-facing one. Each section help you make
a decision for whether this applies to your case or not.

### Main differences with the getting-started deployment

The main differences between the deployment in [Getting started with Grafana Mimir using the Helm chart]({{< relref "../deploy-grafana-mimir/getting-started-helm-charts" >}})
and the one in this guide are

- the scheduling of Pods onto Kubernetes Nodes and the size of the Mimir cluster, and
- using an object storage setup different from the MinIO deployment that comes with the mimir-distributed chart.

## Before you begin

This guide has some prerequisites. Check if you meet all:

- Familiarity with [Helm](https://helm.sh/docs/intro/quickstart/).
- An external object storage different from the MinIO that mimir-distributed deploys.

  The MinIO deployment in the chart is not intended for production use.
  You can replace it with any S3-compatible service, GCS, Azure Blob Storage, or OpenStack Swift.
  Alternatively you can [deploy MinIO yourself](https://min.io/docs/minio/kubernetes/upstream/index.html).

- Add the grafana Helm repository to your local environment or to your CI/CD tooling:

  ```bash
  helm repo add grafana https://grafana.github.io/helm-charts
  helm repo update
  ```

## Guide

### Capacity planning and Pod scheduling

The mimir-distributed chart comes with two sizing plans:

- for 1M series - [small.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/small.yaml) and
- for 10M series - [large.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/large.yaml).

These sizing plans are estimations based on experience from operating Grafana Mimir at Grafana Labs.
The ideal size for your cluster highly depends on your usage patterns. This means that you should use them as a starting
point to sizing your Grafana Mimir cluster rather than strict instructions. For more details refer to
to [Planning Grafana Mimir capacity]({{< relref "../run-production-environment/planning-capacity.md" >}}) and the
YAML comments at the beginning of small.yaml and large.yaml.

To use these sizing plans, copy them from the [mimir GitHub repository](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed)
and pass them as a values files to the `helm` command:

For example:

```bash
helm install mimir-prod grafana/mimir-distributed -f ./small.yaml
```

#### Pod scheduling

The small.yaml and large.yaml add Pod anti-affinity rules so that Pods in the ingester and store-gateway StatefulSets are
scheduled on distinct Kubernetes Nodes. This increases the fault tolerance of the Mimir cluster. It also means that you
need at least as many Nodes in your cluster as store-gateway replicas or ingester replicas, whichever is bigger.

Refer to [Ingesters failure and data loss]({{< relref "../architecture/components/ingester/#ingesters-failure-and-data-loss">}})
and [Store-gateway: Blocks sharding and replication]({{< relref "../architecture/components/store-gateway/#blocks-sharding-and-replication">}})
for more information about failure modes of those two components.

##### Zone-aware replication

Grafana Mimir supports [replication across availability zones]({{< relref "../configure/configuring-zone-aware-replication/">}}) within your Kubernetes cluster.
This further increases fault tolerance of the Mimir cluster. We recommend that you enable this even if you don't currently have
multiple zones in your Kubernetes cluster.

If you are deploying a **fresh** installation of Mimir, then zone-awareness is enabled by default.
You only need to choose the Node selectors for the different zones. You can use the following YAML to configure that:

TODO: check if this is actually the correct yaml after github.com/grafana/mimir/pull/2778 is merged

```yaml
ingester:
  zoneAwareReplication:
    enabled: true
    topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
    zones:
      - name: zone-a
        nodeSelector:
          topology.kubernetes.io/zone: zone-a
      - name: zone-b
        nodeSelector:
          topology.kubernetes.io/zone: zone-b
      - name: zone-c
        nodeSelector:
          topology.kubernetes.io/zone: zone-c

store_gateway:
  zoneAwareReplication:
    enabled: true
    topologyKey: "kubernetes.io/hostname" # Triggers creating anti-affinity rules
    zones:
      - name: zone-a
        nodeSelector:
          topology.kubernetes.io/zone: zone-a
      - name: zone-b
        nodeSelector:
          topology.kubernetes.io/zone: zone-b
      - name: zone-c
        nodeSelector:
          topology.kubernetes.io/zone: zone-c
```

If you are **upgrading** a Mimir cluster, then refer to the [migration guide]({{< relref "../../migration-guide/migrating-from-single-zone-with-helm" >}}) for enabling up zone aware replication.

### Object storage

The getting-started deployment uses a small MinIO deployment. That deployment is not optimized for larger workloads.
You can replace it with any S3-compatible service, GCS, Azure Blob Storage, or OpenStack Swift.
Alternatively you can [deploy MinIO yourself](https://min.io/docs/minio/kubernetes/upstream/index.html).

1. The sizing plans mentioned in [Capacity planning and Pod scheduling](#capacity-planning-and-pod-scheduling) disable the built-in MinIO deployment.
   If you aren't using the sizing plans, add the following YAML to your values file:

   ```yaml
   minio:
     enabled: false
   ```

2. Prepare credentials and bucket names for the object storage of your choice. The article
   [Configure Grafana Mimir object storage backend]({{< relref "../configure/configure-object-storage-backend" >}})
   has examples for the different types of object storage that Mimir supports.

3. Add the object storage configuration to the Helm chart values. Nest the object storage configuration under
   `mimir.structuredConfig`. This example uses S3:

   ```yaml
   mimir:
     structuredConfig:
       common:
         storage:
           backend: s3
           s3:
             endpoint: s3.us-east-2.amazonaws.com
             region: us-east
             secret_access_key: "${AWS_SECRET_ACCESS_KEY}" # This is a secret injected via an environment variable
             access_key_id: "${AWS_ACCESS_KEY_ID}" # This is a secret injected via an environment variable

       blocks_storage:
         s3:
           bucket_name: mimir-blocks
       alertmanager_storage:
         s3:
           bucket_name: mimir-alertmanager
       ruler_storage:
         s3:
           bucket_name: mimir-ruler

       # The admin_client configuration applies only to GEM deployments
       #admin_client:
       #  storage:
       #    s3:
       #      bucket_name: gem-admin
   ```

### Security

Grafana Mimir does not require any special permissions from the hosts it runs on. Because of this it can be deployed
in environments that enforce the [Restricted security policy](https://kubernetes.io/docs/concepts/security/pod-security-standards/).

In Kubernetes >=1.23 the Restricted policy may be enforced via a namespace label on the namespace where Mimir will be installed:

```
pod-security.kubernetes.io/enforce: restricted
```

In Kubernetes versions prior to 1.23, the mimir-distributed chart provides a
[PodSecurityPolicy resource](https://v1-24.docs.kubernetes.io/docs/concepts/security/pod-security-policy/)
which enforces a lot of the recommendations of the Restricted policy that the namespace label enforces.
To enable the PodSecurityPolicy admission controller for your Kubernetes cluster refer to
[How do I turn on an admission controller?](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#how-do-i-turn-on-an-admission-controller)
in the Kubernetes documentation.

### Metamonitoring

You can use ready Grafana dashboards, and Prometheus alerting and recording rules to monitor Mimir itself.
See [Installing Grafana Mimir dashboards and alerts]({{< relref "../monitor-grafana-mimir/installing-dashboards-and-alerts/">}})
for more details.

The mimir-distributed Helm chart makes it easy to collect metrics and logs from Mimir. It takes care of assigning the
right labels so that the dashboards and alerts work out of the box. The chart ships metrics to a Prometheus-compatible
remote and logs to a Loki cluster.

If you are using the latest mimir-distributed Helm chart:

1. Download the Grafana Agent Operator CRDs from https://github.com/grafana/agent/tree/main/production/operator/crds
2. Install the CRDs in your cluster

   ```bash
   kubectl apply -f production/operator/crds/
   ```

3. Add the following to your values file:

   ```yaml
   metaMonitoring:
     serviceMonitor:
       enabled: true
     grafanaAgent:
       enabled: true
       installOperator: true

       logs:
         remote:
           url: "https://example.com/loki/api/v1/push"
           auth:
             username: 12345

       metrics:
         remote:
           url: "https://mimir-nginx.mimir.svc.cluster.local./api/v1/push"
           headers:
             X-Scope-OrgID: metamonitoring
   ```

The article [Collecting metrics and logs from Grafana Mimir]({{< relref "../monitor-grafana-mimir/collecting-metrics-and-logs/">}})
goes into greater detail of how to set up the credentials for this.

### Configure clients to write metrics to Mimir

Refer to [Configure Prometheus to write to Grafana Mimir]({{< relref "../deploy-grafana-mimir/getting-started-helm-charts/#configure-prometheus-to-write-to-grafana-mimir">}})
and [Configure Grafana Agent to write to Grafana Mimir]({{< relref "../deploy-grafana-mimir/getting-started-helm-charts/#configure-grafana-agent-to-write-to-grafana-mimir">}})
for details on how to configure each client to remote-write metrics to Mimir.

#### High availability setup

It is possible to set up redundant groups of clients to write metrics to Mimir. Refer to
[Configuring mimir-distributed Helm Chart for high-availability deduplication with Consul]({{< relref "../configure/setting-ha-helm-deduplication-consul">}})
for instructions on setting up a Consul instance, configuring Mimir, and configuring clients.
