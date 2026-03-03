---
aliases:
  - /docs/mimir/latest/operators-guide/deploying-grafana-mimir/getting-started-helm-charts/
  - /docs/helm-charts/mimir-distributed/latest/get-started/
description: Learn how to get started with Grafana Mimir using the Helm chart.
menuTitle: Get started
title: Get started with Grafana Mimir using the Helm chart
weight: 20
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Get started with Grafana Mimir using the Helm chart

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. If you don't have experience with Kubernetes, you can install a lightweight flavor of Kubernetes such as [kind](https://kind.sigs.k8s.io/).

Experience with the following is recommended, but not essential:

- General knowledge about using a Kubernetes cluster.
- Understanding of what the `kubectl` command does.
- Understanding of what the `helm` command does.

> **Caution:** This procedure is primarily aimed at local or development setups. To set up in a production environment, see [Run Grafana Mimir in production using the Helm chart](../run-production-environment-with-helm/).

### Hardware requirements

- A single Kubernetes node with a minimum of 4 cores and 16GiB RAM

### Software requirements

- Kubernetes 1.32 or higher
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
Refer to [Run Grafana Mimir in production using the Helm chart](../run-production-environment-with-helm/) for instructions on
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
   NAME                                         READY   STATUS      RESTARTS       AGE
   mimir-alertmanager-0                         1/1     Running     0              2m25s
   mimir-compactor-0                            1/1     Running     0              2m25s
   mimir-distributor-5c8f54fcf-t7f9m            1/1     Running     2 (2m1s ago)   2m25s
   mimir-ingester-zone-a-0                      1/1     Running     2 (117s ago)   2m25s
   mimir-ingester-zone-b-0                      1/1     Running     2 (118s ago)   2m25s
   mimir-ingester-zone-c-0                      1/1     Running     2 (112s ago)   2m25s
   mimir-kafka-0                                1/1     Running     0              2m25s
   mimir-make-minio-buckets-5.4.0-d556m         0/1     Completed   0              2m25s
   mimir-minio-5477c4c7b4-dwvsv                 1/1     Running     0              2m25s
   mimir-gateway-5ddc75564-lcqlv                1/1     Running     0              2m25s
   mimir-overrides-exporter-6db7cfd7c-c4ljk     1/1     Running     0              2m25s
   mimir-querier-7fb9d8c875-x9jkd               1/1     Running     2 (119s ago)   2m25s
   mimir-querier-7fb9d8c875-xf9hd               1/1     Running     2 (117s ago)   2m25s
   mimir-query-frontend-6f56dc997f-lgd4j        1/1     Running     0              2m25s
   mimir-query-scheduler-bf95f647-rm9cf         1/1     Running     0              2m25s
   mimir-query-scheduler-bf95f647-swlzs         1/1     Running     0              2m25s
   mimir-rollout-operator-569576c88c-xs2wz      1/1     Running     0              2m25s
   mimir-ruler-67548666f-bmmn2                  1/1     Running     2 (2m ago)     2m25s
   mimir-store-gateway-zone-a-0                 1/1     Running     0              2m25s
   mimir-store-gateway-zone-b-0                 1/1     Running     0              2m25s
   mimir-store-gateway-zone-c-0                 1/1     Running     0              2m25s
   ```

1. Wait until all of the pods have a status of `Running` or `Completed`, which might take a few minutes.

## Generate test metrics

We will install [Grafana Alloy](https://grafana.com/docs/alloy/latest/), preconfigured to scrap metrics from Grafana Mimir pods, and write those metrics to the same Grafana Mimir instance.

1. Create a YAML file called `alloy-values.yaml` for Grafana Alloy Helm chart overrides:

   ```yaml
   alloy:
     clustering:
       enabled: false

     resources:
       requests:
         cpu: 100m
         memory: 128Mi
       limits:
         memory: 512Mi

     serviceAccount:
       create: true
       name: ""
     rbac:
       create: true

     configMap:
       content: |
         // Kubernetes service discovery for pods in test-mimir namespace
         discovery.kubernetes "pods" {
           role = "pod"
           namespaces {
             names = ["mimir-test"]
           }
         }
         // Relabel pods to extract metrics endpoints
         discovery.relabel "pod_relabel" {
           targets = discovery.kubernetes.pods.targets
           rule {
             source_labels = ["__meta_kubernetes_pod_label_name"]
             regex = ""
             action = "drop"
           }
           rule {
               source_labels = ["__meta_kubernetes_pod_container_port_name"]
               regex = ".*-metrics"
               action = "keep"
           }
           rule {
             source_labels = ["__meta_kubernetes_pod_phase"]
             regex = "Succeeded|Failed"
             action = "drop"
           }
           // Add pod metadata as labels
           rule {
             source_labels = ["__meta_kubernetes_namespace", "__meta_kubernetes_pod_label_name"]
             replacement = "$1"
             separator = "/"
             action = "replace"
             target_label = "job"
           }
           rule {
             source_labels = ["__meta_kubernetes_namespace"]
             action        = "replace"
             target_label  = "namespace"
           }
           rule {
             source_labels = ["__meta_kubernetes_pod_name"]
             action        = "replace"
             target_label  = "pod"
           }
           rule {
             source_labels = ["__meta_kubernetes_pod_container_name"]
             action        = "replace"
             target_label  = "container"
           }
         }
         // Scrape metrics from discovered targets
         prometheus.scrape "kubernetes_pods" {
           targets    = discovery.relabel.pod_relabel.output
           forward_to = [prometheus.remote_write.test_mimir.receiver]
           scrape_interval = "15s"
           scrape_timeout  = "10s"
         }
         // Remote write to Mimir
         prometheus.remote_write "test_mimir" {
           endpoint {
             url = "http://mimir-gateway.mimir-test.svc.cluster.local/api/v1/push"
             send_native_histograms = true
           }
         }
     securityContext:
       runAsNonRoot: true
       runAsUser: 472
       fsGroup: 472

     podSecurityContext:
       runAsNonRoot: true
       runAsUser: 472
       fsGroup: 472

   # Disable ServiceMonitor for monitoring Alloy itself
   serviceMonitor:
     enabled: false
   ```

   {{< admonition type="note" >}}
   In a production environment the `url` parameter in the `prometheus.remote_write` stanza would point to an external system, independent of your Grafana Mimir instance. For example, this could be an instance of Grafana Cloud Metrics.
   {{< /admonition >}}

2. Install Grafana Alloy from the Helm chart, passing the overrides file:

   ```
   helm install alloy grafana/alloy --namespace alloy-test --create-namespace -f alloy-values.yaml
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

   | Field | Value                                             |
   | ----- | ------------------------------------------------- |
   | Name  | Mimir                                             |
   | URL   | http://mimir-gateway.mimir-test.svc:80/prometheus |

   To add a data source, see [Add a data source](/docs/grafana/latest/datasources/add-a-data-source/).

1. Verify success:

   You should be able to query metrics in [Grafana Explore](/docs/grafana/latest/explore/),
   as well as create dashboard panels by using your newly configured `Mimir` data source.

## Enable external access to Grafana Mimir

To enable write and query access to Grafana Mimir from outside the Kubernetes cluster, refer to [Enable external access to Grafana Mimir](https://grafana.com/docs/helm-charts/mimir-distributed/<MIMIR_HELM_VERSION>/get-started-helm-charts/gs-external-access).
