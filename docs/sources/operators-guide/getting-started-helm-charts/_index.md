---
title: "Getting started with Grafana Mimir using the Helm chart"
menuTitle: "Getting started using the Helm chart"
description: "Learn how to get started with Grafana Mimir using the Helm chart."
weight: 20
---

# Getting started with Grafana Mimir using the Helm chart

<!-- What is this about? TODO -->
<!-- TODO introduce the notation of <...> to mean that the user needs to set their own value here. -->

## Before you begin

The instructions that follow are common across any flavor of Kubernetes.

The information that follows assumes that you are able to install Kubernetes, and configure and operate it.

Hardware requirements:

- For this tutorial a single Kubernetes node with a minimum of 4 cores and 16GiB RAM should be sufficient. Note that production setups start from 3 nodes for redundancy!

Install the following software:

- Either a [Prometheus server](https://prometheus.io/docs/prometheus/latest/installation/) or [Grafana Agent](https://grafana.com/docs/grafana-cloud/agent/#installing-the-grafana-agent).
- Helm 3 or higher
- Kubernetes 1.10 or higher
- The kubectl command for your version of Kubernetes, and an understanding of what the command does.

Verify that you have:

- Access to the Kubernetes cluster
- Persistent storage is enabled in the Kubernetes cluster and there is a default storage class set up
- DNS service works in the Kubernetes cluster
- An ingress controller is set up in the Kubernetes cluster

  **Note:** Although this is not strictly necessary, if you want to access Mimir from outside of the Kubernetes cluster, you will need an ingress.

## Install the Helm chart in a custom namespace

Using a custom namespace solves problems later on because you do not have to overwrite the default namespace.

1. [Create a unique Kubernetes namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/#creating-a-new-namespace):

   ```console
   kubectl create namespace <namespace>
   ```

1. Set up a Helm repository using the following commands:

   ```console
   helm repo add grafana https://grafana.github.io/helm-charts
   helm repo update
   ```

   > **Note:** The Helm chart at [https://grafana.github.io/helm-charts](https://grafana.github.io/helm-charts) is a publication of the source code at [**grafana/mimir**](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed).

1. Configure an ingress by creating a YAML file, such as `custom.yaml` and adding the following configuration:

   ```yaml
   nginx:
     ingress:
       enabled: true
       ingressClassName: nginx
       hosts:
         - host: <ingress-host>
           paths:
             - path: /
               pathType: Prefix
       tls:
         # empty, disabled.
   ```

   An ingress enables you to externally access a Kubernetes cluster via the hostname defined by the _`<ingress-host>`_ variable.
   Replace _`<ingress-host>`_ with a suitable hostname that DNS can resolve to the external IP address of the Kubernetes cluster. For more information, see [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).

   > **Note:** Without using an ingress, it is still possible to access Grafana Mimir from inside the cluster, see chapter <!-- TODO -->

1. Install Grafana Mimir using the Helm chart:

   ```bash
   helm -n <namespace> install <release-name> grafana/mimir-distributed -f custom.yaml
   ```

1. Check the statuses of the Mimir services:

   ```bash
   kubectl -n <namespace> get pod
   ```

   The results look similar to this:

   ```bash
   kubectl -n dev get pod
   NAME                                                       READY   STATUS      RESTARTS   AGE
   <release-name>-mimir-nginx-69fd969c-g6bkn                  1/1     Running     0          159m
   <release-name>-minio-5757f44456-dz7xx                      1/1     Running     0          159m
   <release-name>-mimir-distributed-make-bucket-job-f5gbj     0/1     Completed   0          159m
   <release-name>-mimir-distributor-7db5f7c8bc-444sd          1/1     Running     0          148m
   <release-name>-mimir-overrides-exporter-79b475c98d-sqzxx   1/1     Running     0          148m
   <release-name>-mimir-querier-5cb58b7b9c-zwnr6              1/1     Running     0          148m
   <release-name>-mimir-query-frontend-74d666bcd5-4dcwh       1/1     Running     0          148m
   <release-name>-mimir-ruler-745fff6c6d-8swsv                1/1     Running     0          148m
   <release-name>-mimir-alertmanager-0                        1/1     Running     0          148m
   <release-name>-mimir-compactor-0                           1/1     Running     0          148m
   <release-name>-mimir-ingester-1                            1/1     Running     0          147m
   <release-name>-mimir-ingester-2                            1/1     Running     0          147m
   <release-name>-mimir-store-gateway-0                       1/1     Running     0          146m
   <release-name>-mimir-ingester-0                            1/1     Running     0          144m
   ```

1. What until all of the pods have a status of `Running` or `Completed`, which might take a few minutes.

## Accessing Grafana Mimir from outside the Kubernetes cluster

This chapter assumes that an ingress is set up. If this is not the case, skip to the next chapter <!-- TODO link -->

### Configure Prometheus to write to Grafana Mimir

Add the following YAML snippet to your existing Prometheus configuration file and restart the Prometheus server:

```yaml
remote_write:
  - url: http://<ingress-host>/api/v1/push
```

The configuration for a Prometheus server that scrapes itself and writes those metrics to Grafana Mimir looks similar to this:

```yaml
remote_write:
  - url: http://<ingress-host>/api/v1/push

scrape_configs:
  - job_name: prometheus
    honor_labels: true
    static_configs:
      - targets: ["localhost:9090"]
```

Assuming this configuration is written to a `prometheus.yml`, the following command can quickly start a Prometheus instance:

```bash
docker run --network=host -p 9090:9090  -v <path-to>/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
```

### Configure Grafana Agent to write to Grafana Mimir

Add the following YAML snippet to one of your existing Agent metrics configurations (`metrics.configs`) in your Agent configuration file and restart Grafana Agent:

```yaml
remote_write:
  - url: http://<ingress-host>/api/v1/push
```

The configuration for an agent that scrapes itself for metrics and writes those metrics to Grafana Mimir looks similar to this:

```yaml
server:
  http_listen_port: 12345
  grpc_listen_port: 54321

metrics:
  wal_directory: /tmp/grafana-agent/wal

  configs:
    - name: agent
      scrape_configs:
        - job_name: agent
          static_configs:
            - targets: ["127.0.0.1:12345"]
      remote_write:
        - url: http://<ingress-host>/api/v1/push
```

Assuming this configuration is written to a `config.yaml` and there is a directory for writing the write ahead log (WAL), the following command can quickly start a Grafana Agent instance:

```bash
docker run --network=host  -v <path-to-wal-directory>:/etc/agent/data -v <path-to>/config.yaml:/etc/agent/agent.yaml grafana/agent:latest
```

### Query data in Grafana

First install Grafana, and then add Mimir as a Prometheus data source.

#### Install Grafana

You can either [deploy Grafana Mimir on Kubernetes](https://grafana.com/docs/grafana/latest/setup-grafana/installation/kubernetes/)
or get a test instance of a local Grafana server up and running
quickly by using Docker:

```bash
docker run --rm --name=grafana --network=host grafana/grafana
```

#### Add Grafana Mimir as a Prometheus data source

1. In a browser, go to the Grafana server at [http://localhost:3000/datasources](http://localhost:3000/datasources).
1. Sign in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Grafana Mimir server using the following settings:

   | Field | Value                              |
   | ----- | ---------------------------------- |
   | Name  | Mimir                              |
   | URL   | http://\<ingress-host\>/prometheus |

To add a data source, refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

### Verify success

When you have completed the tasks in this getting started guide, you can query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/)
as well as create dashboard panels using the newly configured Grafana Mimir data source.

## Accessing Grafana Mimir from inside the Kubernetes cluster

This chapter does not assume that an ingress is set up, but of course it does not prohibit it either. You can mix the two approaches as needed.

### Enable Meta Monitoring in Grafana Mimir

Grafana Mimir meta monitoring collects metrics and or logs about Grafana Mimir itself. It's primary purpose is to send meta monitoring information to some external receiver, for example a [free tier Grafana Metrics account](https://grafana.com/metrics/). In this example we'll use it to collect and send metrics into itself for testing.

Add the following YAML snippet to your Grafana Mimir `custom.yaml` file:

```yaml
serviceMonitor:
  enabled: true
metaMonitoring:
  grafanaAgent:
    enabled: true
    installOperator: true
    metrics:
      additionalRemoteWriteConfigs:
        - url: "http://<release-name>-mimir-nginx.<namespace>.svc:80/api/v1/push"
```

Upgrade Grafana Mimir via the helm chart to start a Grafana Agent and start collecting metrics about Grafana Mimir itself:

```bash
helm -n <namespace> upgrade <release-name> grafana/mimir-distributed -f custom.yaml
```

### Query data in Grafana

First install Grafana in the Kubernetes cluster, and then add Mimir as a Prometheus data source.

#### Install Grafana

Follow the instructions in [Deploy Grafana on Kubernetes](https://grafana.com/docs/grafana/latest/setup-grafana/installation/kubernetes/).

#### Add Grafana Mimir as a Prometheus data source

1. Port forward Grafana to localhost with the command:
   ```bash
   kubectl port-forward service/grafana 3000:3000
   ```
1. In a browser, go to the Grafana server at [http://localhost:3000/datasources](http://localhost:3000/datasources).
1. Sign in using the default username `admin` and password `admin`.
1. Configure a new Prometheus data source to query the local Grafana Mimir server using the following settings:

   | Field | Value                                                               |
   | ----- | ------------------------------------------------------------------- |
   | Name  | Mimir                                                               |
   | URL   | http://\<release-name\>-mimir-nginx.\<namespace\>.svc:80/prometheus |

To add a data source, refer to [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

### Verify success

When you have completed the tasks in this getting started guide, you can query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/)
as well as create dashboard panels using the newly configured Grafana Mimir data source.
