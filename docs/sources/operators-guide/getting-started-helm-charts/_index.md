---
title: "Getting started with Grafana Mimir using the Helm chart"
menuTitle: "Getting started using the Helm chart"
description: "Learn how to get started with Grafana Mimir using the Helm chart."
weight: 20
---

# Getting started with Grafana Mimir using the Helm chart

The Helm chart allows you to configure, install, and upgrade Grafana Mimir within a Kubernetes cluster.

## Before you begin

The instructions that follow are common across any flavor of Kubernetes. They also assume that you know how to install a Kubernetes cluster, and configure and operate it.

It also assumes that you have an understanding of what the `kubectl` command does.

> **Caution:** Do not use this getting-started procedure in a production environment.

Hardware requirements:

- A single Kubernetes node with a minimum of 4 cores and 16GiB RAM

Software requirements:

- Kubernetes 1.10 or higher
- The `kubectl` command for your version of Kubernetes
- Helm 3 or higher

Verify that you have:

- Access to the Kubernetes cluster
- Persistent storage is enabled in the Kubernetes cluster, which has a default storage class set up
- DNS service works in the Kubernetes cluster
- An ingress controller is set up in the Kubernetes cluster

  **Note:** Although this is not strictly necessary, if you want to access Mimir from outside of the Kubernetes cluster, you will need an ingress. This procedure assumes you have an ingress controller set up.

## Install the Helm chart in a custom namespace

Using a custom namespace solves problems later on because you do not have to overwrite the default namespace.

1. Create a unique Kubernetes namespace:

   ```console
   kubectl create namespace <namespace>
   ```

   Replace `<namespace>` with a namespace of your choice. For example, `mimir` or `test-mimir`.

   For more details, see the Kubernetes documentation about [Creating a new namespace](https://kubernetes.io/docs/tasks/administer-cluster/namespaces/#creating-a-new-namespace).

1. Set up a Helm repository using the following commands:

   ```console
   helm repo add grafana https://grafana.github.io/helm-charts
   helm repo update
   ```

   > **Note:** The Helm chart at [https://grafana.github.io/helm-charts](https://grafana.github.io/helm-charts) is a publication of the source code at [**grafana/mimir**](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed).

1. Configure an ingress:

   a. Create a YAML file of Helm values called `custom.yaml`.

   b. Add the following configuration to the file:

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

   An ingress enables you to externally access a Kubernetes cluster.
   Replace _`<ingress-host>`_ with a suitable hostname that DNS can resolve to the external IP address of the Kubernetes cluster. For more information, see [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).

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

## Configure Prometheus to write to Grafana Mimir

Make a choice based on whether or not you already have a Prometheus server set up:

- For an existing Prometheus server:

  1. Add the following YAML snippet to your Prometheus configuration file:

     ```yaml
     remote_write:
       - url: http://<ingress-host>/api/v1/push
     ```

     In this case, your Prometheus server writes metrics to Grafana Mimir, based on what is defined in the existing `scrape_configs` configuration.

  1. Restart the Prometheus server.

- For a Prometheus server that does not exist yet:

  1. Write the following configuration to a `prometheus.yml` file:

     ```yaml
     remote_write:
       - url: http://<ingress-host>/api/v1/push

     scrape_configs:
       - job_name: prometheus
         honor_labels: true
         static_configs:
           - targets: ["localhost:9090"]
     ```

     In this case, your Prometheus server writes metrics to Grafana Mimir that it scrapes from itself.

  1. Start a Prometheus server by using Docker:

     ```bash
     docker run --network=host -p 9090:9090  -v <path-to>/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
     ```

## Configure Grafana Agent to write to Grafana Mimir

Make a choice based on whether or not you already have a Grafana Agent set up:

* For an existing Grafana Agent:

  1. Add the following YAML snippet to your Grafana Agent metrics configurations (`metrics.configs`):

      ```yaml
      remote_write:
        - url: http://<ingress-host>/api/v1/push
      ```

      In this case, your Grafana Agent will write metrics to Grafana Mimir, based on what is defined in the existing `metrics.configs.scrape_configs` configuration.

  1. Restart the Grafana Agent.

* For a Grafana Agent that does not exist yet:

  1. Write the following configuration to an `agent.yaml` file:

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

      In this case, your Grafana Agent writes metrics to Grafana Mimir that it scrapes from itself.

  1. Create an empty directory for the write ahead log (WAL) of the Grafana Agent

  1. Start a Grafana Agent by using Docker:

      ```bash
      docker run --network=host  -v <path-to-wal-directory>:/etc/agent/data -v <path-to>/agent.yaml:/etc/agent/agent.yaml grafana/agent
      ```

## Query data in Grafana

First install Grafana, and then add Mimir as a Prometheus data source.

1. Start Grafana by using Docker:

    ```bash
    docker run --rm --name=grafana --network=host grafana/grafana
    ```

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir server, by using the following settings:

      | Field | Value                              |
      | ----- | ---------------------------------- |
      | Name  | Mimir                              |
      | URL   | http://\<ingress-host\>/prometheus |

    To add a data source, see [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

 1. Verify success:

    You should be able to query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/),
    as well as create dashboard panels by using your newly configured `Mimir` data source.

## Set up metamonitoring

Grafana Mimir metamonitoring collects metrics or logs, or both,
about Grafana Mimir itself.
In the example that follows, metamonitoring scrapes metrics about 
Grafana Mimir itself, and then writes those metrics to the same Grafana Mimir instance.

1. To enable metamonitoring in Grafana Mimir, add the following YAML snippet to your Grafana Mimir `custom.yaml` file:

    ```yaml
    metaMonitoring:
      serviceMonitor:
        enabled: true
      grafanaAgent:
        enabled: true
        installOperator: true
        metrics:
          additionalRemoteWriteConfigs:
            - url: "http://<release-name>-mimir-nginx.<namespace>.svc:80/api/v1/push"
    ```

    Replace _`<release-name>`_ with the release name that you used when you installed the Grafana Mimir Helm chart.

    Replace  _`<namespace>`_ with the namespace in which Grafana Mimir is installed.

1. Upgrade Grafana Mimir by using the `helm` command:

    ```bash
    helm -n <namespace> upgrade <release-name> grafana/mimir-distributed -f custom.yaml
    ```

1. In Grafana, verify that your metrics are being scraped, by quering the metric `cortex_ingester_ingested_samples_total{}`.

## Query data in Grafana that is running within the same Kubernetes cluster

1. Install Grafana in the same Kubernetes cluster.

   For details, see [Deploy Grafana on Kubernetes](https://grafana.com/docs/grafana/latest/setup-grafana/installation/kubernetes/).

1. Stop the Grafana instance that is running in the Docker container, to allow for port-forwarding.

1. Port-forward Grafana to `localhost`, by using the `kubectl` command:

    ```bash
    kubectl port-forward service/grafana 3000:3000
    ```

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir server, by using the following settings:

      | Field | Value                              |
      | ----- | ---------------------------------- |
      | Name  | Mimir                              |
      | URL   | http://\<release-name\>-mimir-nginx.\<namespace\>.svc:80/prometheus |

    To add a data source, see [Add a data source](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/).

 1. Verify success:

    You should be able to query metrics in [Grafana Explore](https://grafana.com/docs/grafana/latest/explore/),
    as well as create dashboard panels by using your newly configured `Mimir` data source.
