---
aliases:
  - /docs/mimir/latest/operators-guide/deploying-grafana-mimir/getting-started-helm-charts/
  - /docs/helm-charts/mimir-distributed/latest/get-started/
description: Learn how to get started with Grafana Mimir and GEM using the Helm chart.
menuTitle: Get started
title: Get started with Grafana Mimir and GEM using the Helm chart
weight: 20
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Get started with Grafana Mimir and GEM using the Helm chart

The mimir-distributed Helm chart allows you to install, configure, and upgrade Grafana Mimir and Grafana Enterprise Metrics (GEM) within a Kubernetes cluster.

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. If you don't have experience with Kubernetes, you can install a lightweight flavor of Kubernetes such as [kind](https://kind.sigs.k8s.io/).

Experience with the following is recommended, but not essential:

- General knowledge about using a Kubernetes cluster.
- Understanding of what the `kubectl` command does.
- Understanding of what the `helm` command does.

> **Caution:** This procedure is primarily aimed at local or development setups. To set up in a production environment, see [Run Grafana Mimir in production using the Helm chart]({{< relref "../run-production-environment-with-helm" >}}).

### Hardware requirements

- A single Kubernetes node with a minimum of 4 cores and 16GiB RAM

### Software requirements

- Kubernetes 1.20 or higher
- The [`kubectl`](https://kubernetes.io/docs/reference/kubectl/) command for your version of Kubernetes

  Run the following command to get both the Kubernetes and `kubectl` version: `kubectl version`. The command prints out the server and client versions, where the server is the Kubernetes itself and client means `kubectl`.

- The [`helm`](https://helm.sh) command version 3.8 or higher

  Run the following command to get the Helm version: `helm version`.

### Verify that you have

- Access to the Kubernetes cluster

  For example by running the command `kubectl get ns`, which lists all namespaces.

- Persistent storage is enabled in the Kubernetes cluster, which has a default storage class set up. You can [change the default StorageClass](https://kubernetes.io/docs/tasks/administer-cluster/change-default-storage-class/).

  > **Note:** If you are using `kind` or you are unsure, assume it is enabled and continue.

- DNS service works in the Kubernetes cluster

  > **Note:** If you are using `kind` or you are unsure, assume it works and continue.

### Security setup

If you are using `kind` or similar local Kubernetes setup and haven't set security policies, you can safely skip this section.

This installation will not succeed if you have enabled the
[PodSecurityPolicy](https://v1-23.docs.kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#podsecuritypolicy) admission controller
or if you are enforcing the Restricted policy with [Pod Security](https://v1-24.docs.kubernetes.io/docs/concepts/security/pod-security-admission/#pod-security-admission-labels-for-namespaces) admission controller.
The reason is that the installation includes a deployment of MinIO. The [minio/minio chart](https://github.com/minio/minio/tree/master/helm/minio)
is not compatible with running under a Restricted policy or the PodSecurityPolicy that the mimir-distributed chart provides.

If you are using the PodSecurityPolicy admission controller, then it is not possible to deploy the mimir-distributed chart with MinIO.
Refer to [Run Grafana Mimir in production using the Helm chart]({{< relref "../run-production-environment-with-helm" >}}) for instructions on
setting up an external object storage and disable the built-in MinIO deployment with `minio.enabled: false` in the Helm values file.

If you are using the [Pod Security](https://kubernetes.io/docs/concepts/security/pod-security-admission/) admission controller, then MinIO and the mimir-distributed chart can successfully deploy under the [baseline](https://kubernetes.io/docs/concepts/security/pod-security-admission/#pod-security-levels) pod security level.

## Install the Helm chart in a custom namespace

Using a custom namespace solves problems later on because you do not have to overwrite the default namespace.

1. Create a unique Kubernetes namespace, for example `mimir-test`:

   ```bash
   kubectl create namespace mimir-test
   ```

   For more details, see the Kubernetes documentation about [Creating a new namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/#creating-a-new-namespace).

1. Set up a Helm repository using the following commands:

   ```bash
   helm repo add grafana https://grafana.github.io/helm-charts
   helm repo update
   ```

   > **Note:** The Helm chart at [https://grafana.github.io/helm-charts](https://grafana.github.io/helm-charts) is a publication of the source code at [**grafana/mimir**](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed).

1. Install Grafana Mimir using the Helm chart:

   ```bash
   helm -n mimir-test install mimir grafana/mimir-distributed
   ```

   > **Note:** The output of the command contains the write and read URLs necessary for the following steps.

1. Check the statuses of the Mimir pods:

   ```bash
   kubectl -n mimir-test get pods
   ```

   The results look similar to this:

   ```bash
   NAME                                        READY   STATUS      RESTARTS   AGE
   mimir-minio-7bd89b757d-q5hp6                1/1     Running     0          2m44s
   mimir-rollout-operator-76c67c7d56-v6xtl     1/1     Running     0          2m44s
   mimir-nginx-858455979c-hjvhx                1/1     Running     0          2m44s
   mimir-make-minio-buckets-svgvd              0/1     Completed   1          2m44s
   mimir-ruler-64b9d59b94-tvj7z                1/1     Running     0          2m44s
   mimir-query-frontend-c444b56f9-jrmwl        1/1     Running     0          2m44s
   mimir-overrides-exporter-86c4d54645-zktkm   1/1     Running     0          2m44s
   mimir-querier-5d9c55d6d9-l6fdc              1/1     Running     0          2m44s
   mimir-distributor-7796db494f-rsvdx          1/1     Running     0          2m44s
   mimir-query-scheduler-d5dccfff7-5c5rw       1/1     Running     0          2m44s
   mimir-querier-5d9c55d6d9-xghl6              1/1     Running     0          2m44s
   mimir-query-scheduler-d5dccfff7-vz4vf       1/1     Running     0          2m44s
   mimir-alertmanager-0                        1/1     Running     0          2m44s
   mimir-store-gateway-zone-b-0                1/1     Running     0          2m44s
   mimir-store-gateway-zone-c-0                1/1     Running     0          2m43s
   mimir-ingester-zone-b-0                     1/1     Running     0          2m43s
   mimir-compactor-0                           1/1     Running     0          2m44s
   mimir-ingester-zone-a-0                     1/1     Running     0          2m43s
   mimir-store-gateway-zone-a-0                1/1     Running     0          2m44s
   mimir-ingester-zone-c-0                     1/1     Running     0          2m44s
   ```

1. Wait until all of the pods have a status of `Running` or `Completed`, which might take a few minutes.

## Generate test metrics

{{< docs/shared source="alloy" lookup="agent-deprecation.md" version="next" >}}

The Grafana Mimir Helm chart can collect metrics, logs, or both, about Grafana Mimir itself. This is called _metamonitoring_.
In the example that follows, metamonitoring scrapes metrics about Grafana Mimir itself, and then writes those metrics to the same Grafana Mimir instance.

1. Deploy the Grafana Agent Operator Custom Resource Definitions (CRDs). For more information, refer to [Deploy the Agent Operator Custom Resource Definitions (CRDs)](https://grafana.com/docs/agent/latest/operator/getting-started/#deploy-the-agent-operator-custom-resource-definitions-crds) in the Grafana Agent documentation.

1. Create a YAML file called `custom.yaml` for your Helm values overrides.
   Add the following YAML snippet to `custom.yaml` to enable metamonitoring in Mimir:

   ```yaml
   metaMonitoring:
     serviceMonitor:
       enabled: true
     grafanaAgent:
       enabled: true
       installOperator: true
       metrics:
         additionalRemoteWriteConfigs:
           - url: "http://mimir-nginx.mimir-test.svc:80/api/v1/push"
   ```

   {{< admonition type="note" >}}
   In a production environment the `url` above would point to an external system, independent of your Grafana Mimir instance, such as an instance of Grafana Cloud Metrics.
   {{< /admonition >}}

1. Upgrade Grafana Mimir by using the `helm` command:

   ```bash
   helm -n mimir-test upgrade mimir grafana/mimir-distributed -f custom.yaml
   ```

## Start Grafana in Kubernetes and query metrics

1. Install Grafana in the same Kubernetes cluster.

   For details, see [Deploy Grafana on Kubernetes](/docs/grafana/latest/setup-grafana/installation/kubernetes/).

1. If you haven't done it as part of the previous step, port-forward Grafana to `localhost`, by using the `kubectl` command:

   ```bash
   kubectl port-forward service/grafana 3000:3000
   ```

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir server, by using the following settings:

   | Field | Value                                           |
   | ----- | ----------------------------------------------- |
   | Name  | Mimir                                           |
   | URL   | http://mimir-nginx.mimir-test.svc:80/prometheus |

   To add a data source, see [Add a data source](/docs/grafana/latest/datasources/add-a-data-source/).

1. Verify success:

   You should be able to query metrics in [Grafana Explore](/docs/grafana/latest/explore/),
   as well as create dashboard panels by using your newly configured `Mimir` data source.

## Deploy Grafana Enterprise Metrics

For additional GEM-specific configurations, refer to [Deploy Grafana Enterprise Metrics](https://grafana.com/docs/helm-charts/mimir-distributed/latest/get-started-helm-charts/gs-gem).

## Enable external access to Grafana Mimir

To enable write and query access to Grafana Mimir from outside the Kubernetes cluster, refer to [Enable external access to Grafana Mimir](https://grafana.com/docs/helm-charts/mimir-distributed/latest/get-started-helm-charts/gs-external-access).
