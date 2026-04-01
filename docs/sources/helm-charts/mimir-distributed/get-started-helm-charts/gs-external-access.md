---
description: Learn how to enable external access to Grafana Mimir from outside the Kubernetes cluster.
menuTitle: Enable external access
title: Enable external access to Grafana Mimir
weight: 50
keywords:
  - Helm chart
  - Kubernetes
  - Grafana Mimir
---

# Enable external access to Grafana Mimir

Set up external access to Grafana Mimir to allow writing and quering metrics from outside the Kubernetes cluster. You can set up an [ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) that enables you to externally access a Kubernetes cluster.

## Before you begin

Verify that an ingress controller is set up in the Kubernetes cluster, for example [ingress-nginx](https://kubernetes.github.io/ingress-nginx/)

## Set up ingress

1. Configure an ingress:

   b. Add the following to your `custom.yaml` Helm values file:

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

   Replace _`<ingress-host>`_ with a suitable hostname that DNS can resolve
   to the external IP address of the Kubernetes cluster.
   For more information, refer to [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/).

   > **Note:** On Linux systems, and if it is not possible for you set up local DNS resolution, you can use the `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` command-line flag to define the _`<ingress-host>`_ local address for the `docker` commands in the examples that follow.

   > **Note:** To see all of the configurable parameters for a Helm chart installation, use `helm show values grafana/mimir-distributed`.

1. Upgrade Grafana Mimir by using the `helm` command:

   ```bash
   helm -n mimir-test upgrade mimir grafana/mimir-distributed -f custom.yaml
   ```

   The output of the command should contain the URL to use for querying Grafana Mimir from the outside, for example:

   ```bash
   From outside the cluster via ingress:
   http://myhost.mynetwork/prometheus
   ```

## Configure Prometheus to write to Grafana Mimir

You can either configure Prometheus to write to Grafana Mimir or [configure Grafana Alloy to write to Mimir](#configure-grafana-alloy-to-write-to-grafana-mimir). Although you can configure both, you don't need to.

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
     docker run -p 9090:9090  -v <absolute-path-to>/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
     ```

     > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by the Prometheus server, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.

## Configure Grafana Alloy to write to Grafana Mimir

You can either configure Grafana Alloy to write to Grafana Mimir or [configure Prometheus to write to Mimir](#configure-prometheus-to-write-to-grafana-mimir). Although you can configure both, you don't need to.

Make a choice based on whether you already have Alloy set up:

- For an existing Alloy:

  1. Add the following configuration snippet for the `prometheus.remote_write` component to your Alloy configuration file:

     ```
     prometheus.remote_write "LABEL" {
       endpoint {
         url = http://<ingress-host>/api/v1/push
       }
     }
     ```

  1. Add `forward_to = [prometheus.remote_write.LABEL.receiver]` to an existing pipeline.

  1. Restart Alloy.

- For a new Alloy:

  1. Write the following configuration to a `config.alloy` file:

     ```
     prometheus.exporter.self "self_metrics" {
     }

     prometheus.scrape "self_scrape" {
       targets    = prometheus.exporter.self.self_metrics.targets
       forward_to = [prometheus.remote_write.mimir.receiver]
     }

     prometheus.remote_write "mimir" {
       endpoint {
         url = "http://<ingress-host>/api/v1/push"
       }
     }
     ```

  1. Start Alloy by using Docker:

     ```bash
     docker run -v <absolute-path-to>/config.alloy:/etc/alloy/config.alloy -p 12345:12345 grafana/alloy:latest run --server.http.listen-addr=0.0.0.0:12345 --storage.path=/var/lib/alloy/data /etc/alloy/config.alloy
     ```

     > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by Alloy, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.

For more information about the `prometheus.remote_write` component, refer to [prometheus.remote_write](https://grafana.com/docs/alloy/<ALLOY_VERSION>/reference/components/prometheus/prometheus.remote_write) in the Grafana Alloy documentation.

## Query metrics in Grafana

You can use the Grafana installed in Kubernetes in the [Start Grafana in Kubernetes and query metrics](https://grafana.com/docs/helm-charts/mimir-distributed/<MIMIR_HELM_VERSION>/get-started-helm-charts/#start-grafana-in-kubernetes-and-query-metrics) documentation, or follow the instructions below.

> **Note:** If you have the port-forward running for Grafana from an earlier step, stop it.

First install Grafana, and then add Mimir as a Prometheus data source.

1. Start Grafana by using Docker:

   ```bash
   docker run --rm --name=grafana -p 3000:3000 grafana/grafana
   ```

   > **Note:** On Linux systems, if \<ingress-host\> cannot be resolved by Grafana, use the additional command-line flag `--add-host=<ingress-host>:<kubernetes-cluster-external-address>` to set it up.

1. In a browser, go to the Grafana server at [http://localhost:3000](http://localhost:3000).
1. Sign in using the default username `admin` and password `admin`.
1. On the left-hand side, go to **Configuration** > **Data sources**.
1. Configure a new Prometheus data source to query the local Grafana Mimir cluster, by using the following settings:

   | Field | Value                              |
   | ----- | ---------------------------------- |
   | Name  | Mimir                              |
   | URL   | http://\<ingress-host\>/prometheus |

   To add a data source, refer to [Add a data source](/docs/grafana/latest/datasources/add-a-data-source/).

1. Verify success:

   You should be able to query metrics in [Grafana Explore](http://localhost:3000/explore),
   as well as create dashboard panels by using your newly configured `Mimir` data source.
   For more information, refer to [Monitor Grafana Mimir](https://grafana.com/docs/mimir/next/manage/monitor-grafana-mimir/monitor-system-health/).
