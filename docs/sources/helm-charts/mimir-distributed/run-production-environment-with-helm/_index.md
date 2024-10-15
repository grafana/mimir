---
title: "Run Grafana Mimir in production using the Helm chart"
aliases:
  - docs/mimir/latest/operators-guide/run-production-environment-with-helm/
  - docs/mimir/latest/operators-guide/running-production-environment-with-helm/
menuTitle: "Run Mimir in production"
description: "Learn how to run Grafana Mimir in production using the mimir-distributed Helm chart."
weight: 40
---

# Run Grafana Mimir in production using the Helm chart

In addition to the guide [Get started with Grafana Mimir using the Helm chart]({{< relref "../get-started-helm-charts" >}}),
which covers setting up Grafana Mimir on a local Kubernetes cluster or
within a low-risk development environment, you can prepare Grafana Mimir
for production.

Although the information that follows assumes that you are using Grafana Mimir in
a production environment that is customer-facing, you might need the
high-availability and horizontal-scalability features of Grafana Mimir even in an
internal, development environment.

## Before you begin

Meet all the follow prerequisites:

- You are familiar with [Helm 3.x](https://helm.sh/docs/intro/quickstart/).

  Add the grafana Helm repository to your local environment or to your CI/CD tooling:

  ```bash
  helm repo add grafana https://grafana.github.io/helm-charts
  helm repo update
  ```

- You have an external object storage that is different from the MinIO
  object storage that `mimir-distributed` deploys, because the MinIO
  deployment in the Helm chart is only intended for getting started and is not intended for production use.

  To use Grafana Mimir in production, you must replace the default object storage
  with an Amazon S3 compatible service, Google Cloud Storage, Microsoft® Azure Blob Storage,
  or OpenStack Swift. Alternatively, to deploy MinIO yourself, see [MinIO High
  Performance Object Storage](https://min.io/docs/minio/kubernetes/upstream/index.html).

- {{% admonition type="note" %}}
  Like Amazon S3, the chosen object storage implementation must not create directories.
  Grafana Mimir doesn't have any notion of object storage directories, and so will leave
  empty directories behind when removing blocks. For example, if you use Azure Blob Storage, you must disable
  [hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace).
  {{% /admonition %}}

## Plan capacity

The `mimir-distributed` Helm chart comes with two sizing plans:

- For 1M series: [`small.yaml`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/small.yaml)
- For 10M series: [`large.yaml`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/large.yaml)

These sizing plans are estimated based on experience from operating Grafana
Mimir at Grafana Labs. The ideal size for your cluster depends on your
usage patterns. Therefore, use the sizing plans as starting
point for sizing your Grafana Mimir cluster, rather than as strict guidelines.
To get a better idea of how to plan capacity, refer to the YAML comments at
the beginning of `small.yaml` and `large.yaml` files, which relate to read and write workloads.
See also [Planning Grafana Mimir capacity](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/run-production-environment/planning-capacity/).

To use a sizing plan, copy it from the [mimir](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed)
GitHub repository, and pass it as a values file to the `helm` command. Note that sizing plans may change with new
versions of the `mimir-distributed` chart. Make sure to use a sizing plan from a version close to the version of the
Helm chart that you are installing.

For example:

```bash
helm install mimir-prod grafana/mimir-distributed -f ./small.yaml
```

### Conform to fault-tolerance requirements

As part of _Pod scheduling_, the `small.yaml` and `large.yaml` files add Pod
anti-affinity rules so that no two ingester Pods, nor two store-gateway
Pods, are scheduled on any given Kubernetes Node. This increases fault
tolerance of the Mimir cluster.

You must create and add Nodes, such that the number of Nodes is equal to or
larger than either the number of ingester Pods or the number of store-gateway Pods,
whichever one is larger. Expressed as a formula, it reads as follows:

```
number_of_nodes >= max(number_of_ingesters_pods, number_of_store_gateway_pods)
```

For more information about the failure modes of either the ingester or store-gateway
component, refer to [Ingesters failure and data loss](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/ingester/#ingesters-failure-and-data-loss)
or [Store-gateway: Blocks sharding and replication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/store-gateway/#blocks-sharding-and-replication).

## Decide whether you need geographical redundancy, fast rolling updates, or both.

You can use a rolling update strategy to apply configuration changes to
Grafana Mimir, and to upgrade Grafana Mimir to a newer version. A
rolling update results in no downtime to Grafana Mimir.

The Helm chart performs a rolling update for you. To make sure that rolling updates are faster,
configure the Helm chart to deploy Grafana Mimir with zone-aware replication.

### New installations

Grafana Mimir supports [replication across availability zones](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-zone-aware-replication/)
within your Kubernetes cluster.
This further increases fault tolerance of the Mimir cluster. Even if you
do not currently have multiple zones across your Kubernetes cluster, you
can avoid having to extraneously migrate your cluster when you start using
multiple zones.

For `mimir-distributed` Helm chart v4.0 or higher, zone-awareness is enabled by
default for new installations.

To benefit from zone-awareness, choose the node selectors for your different
zones. For convenience, you can use the following YAML configuration snippet
as a starting point:

```yaml
ingester:
  zoneAwareReplication:
    enabled: true
    topologyKey: kubernetes.io/hostname
    zones:
      - name: zone-a
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-a
      - name: zone-b
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-b
      - name: zone-c
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-c

store_gateway:
  zoneAwareReplication:
    enabled: true
    topologyKey: kubernetes.io/hostname
    zones:
      - name: zone-a
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-a
      - name: zone-b
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-b
      - name: zone-c
        nodeSelector:
          topology.kubernetes.io/zone: us-central1-c
```

### Existing installations

If you are upgrading from a previous `mimir-distributed` Helm chart version
to v4.0, then refer to the [migration guide]({{< relref "../migration-guides/migrate-from-single-zone-with-helm" >}}) to configure
zone-aware replication.

## Configure Mimir to use object storage

For the different object storage types that Mimir supports, and examples,
see [Configure Grafana Mimir object storage backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-object-storage-backend/).

1. Add the following YAML to your values file, if you are not using the sizing
   plans that are mentioned in [Plan capacity](#plan-capacity):

   ```yaml
   minio:
     enabled: false
   ```

2. Prepare the credentials and bucket names for the object storage.

3. Add the object storage configuration to the Helm chart values. Nest the object storage configuration under
   `mimir.structuredConfig`. This example uses Amazon S3:

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

       # The following admin_client configuration only applies to Grafana Enterprise Metrics deployments:
       #admin_client:
       #  storage:
       #    s3:
       #      bucket_name: gem-admin
   ```

## Meet security compliance regulations

Grafana Mimir does not require any special permissions on the hosts that it
runs on. Because of this, you can deploy it in environments that enforce the
Kubernetes [Restricted security policy](https://kubernetes.io/docs/concepts/security/pod-security-standards/).

In Kubernetes v1.23 and higher, the Restricted policy can be enforced via a
namespace label on the Namespace resource where Mimir is deployed. For example:

```
pod-security.kubernetes.io/enforce: restricted
```

In Kubernetes versions prior to 1.23, the `mimir-distributed` Helm chart
provides a [PodSecurityPolicy resource](https://v1-24.docs.kubernetes.io/docs/concepts/security/pod-security-policy/)
that enforces many of the recommendations from the Restricted policy that the
namespace label enforces.
To enable the PodSecurityPolicy admission controller for your Kubernetes
cluster, refer to
[How do I turn on an admission controller?](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#how-do-i-turn-on-an-admission-controller).

For OpenShift-specific instructions see [Deploy on OpenShift](#deploy-on-openshift).

The `mimir-distributed` Helm chart also deploys most of the containers
with a read-only root filesystem (`readOnlyRootFilesystem: true`).
The exceptions are the optional MinIO and Grafana Agent (deprecated) containers.
The PodSecurityPolicy resource enforces this setting.

## Monitor the health of your Grafana Mimir cluster

To monitor the health of your Grafana Mimir cluster, which is also known as
_metamonitoring_, you can use ready-made Grafana dashboards, and Prometheus
alerting and recording rules.
For more information, see [Installing Grafana Mimir dashboards and alerts](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/installing-dashboards-and-alerts/).

The `mimir-distributed` Helm chart makes it easy for you to collect metrics and
logs from Mimir. It assigns the correct labels for you so that the dashboards
and alerts simply work. The chart uses the Grafana Agent to ship metrics to
a Prometheus-compatible server and logs to a Loki or GEL (Grafana Enterprise
Metrics) server.

{{< docs/shared source="alloy" lookup="agent-deprecation.md" version="next" >}}

1. Download the Grafana Agent Operator Custom Resource Definitions (CRDs) from
   https://github.com/grafana/agent/tree/main/operations/agent-static-operator/crds
2. Install the CRDs on your cluster:

   ```bash
   kubectl apply -f operations/agent-static-operator/crds/
   ```

3. Add the following YAML snippet to your
   values file, to send metamonitoring telemetry from Mimir. Change the URLs and credentials to match your desired
   destination.

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
           url: "https://prometehus.prometheus.svc.cluster.local./api/v1/push"
           headers:
             X-Scope-OrgID: metamonitoring
   ```

   For details about how to set up the credentials, see [Collecting metrics and logs from Grafana Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/collecting-metrics-and-logs/).

Your Grafana Mimir cluster can now ingest metrics in production.

## Configure clients to write metrics to Mimir

To configure each client to remote-write metrics to Mimir, refer to [Configure Prometheus to write to Grafana Mimir]({{< relref "../get-started-helm-charts#configure-prometheus-to-write-to-grafana-mimir" >}})
and [Configure Grafana Alloy to write to Grafana Mimir](https://grafana.com/docs/helm-charts/mimir-distributed/latest/get-started-helm-charts/#configure-grafana-alloy-to-write-to-grafana-mimir).

## Set up redundant Prometheus or Grafana Alloy instances for high availability

If you need redundancy on the write path before it reaches Mimir, then you
can set up redundant instances of Prometheus or Grafana Alloy to
write metrics to Mimir.

For more information, see [Configure high-availability deduplication with Consul]({{< relref "./configure-helm-ha-deduplication-consul" >}}).

## Deploy on OpenShift

To deploy the `mimir-distributed` Helm chart on OpenShift you need to change some of the default values.
Add the following YAML snippet to your values file.
This will create a dedicated SecurityContextConstraints (SCC) resource for the `mimir-distributed` chart.

```yaml
rbac:
  create: true
  type: scc
  podSecurityContext:
    fsGroup: null
    runAsGroup: null
    runAsUser: null
rollout_operator:
  podSecurityContext:
    fsGroup: null
    runAsGroup: null
    runAsUser: null
```

Alternatively, to deploy using the default SCC in your OpenShift cluster, add the following YAML snippet to your values file:

```yaml
rbac:
  create: false
  type: scc
  podSecurityContext:
    fsGroup: null
    runAsGroup: null
    runAsUser: null
rollout_operator:
  podSecurityContext:
    fsGroup: null
    runAsGroup: null
    runAsUser: null
```

> **Note**: When using `mimir-distributed` as a subchart, setting Helm values to `null` requires a workaround due to [a bug in Helm](https://github.com/helm/helm/issues/9027).
> To set the PodSecurityContext fields to `null`, in addition to the YAML, set the values to `null` via the command line
> when using `helm`. For example, to use `helm tempalte`:
>
> ```bash
> helm template grafana/mimir-distributed -f values.yaml \
>   --set 'mimir-distributed.rbac.podSecurityContext.fsGroup=null' \
>   --set 'mimir-distributed.rbac.podSecurityContext.runAsUser=null' \
>   --set 'mimir-distributed.rbac.podSecurityContext.runAsGroup=null' \
>   --set 'mimir-distributed.rollout_operator.podSecurityContext.fsGroup=null' \
>   --set 'mimir-distributed.rollout_operator.podSecurityContext.runAsUser=null' \
>   --set 'mimir-distributed.rollout_operator.podSecurityContext.runAsGroup=null'
> ```
