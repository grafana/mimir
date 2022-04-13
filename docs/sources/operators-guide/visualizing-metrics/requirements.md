---
title: "About Grafana Mimir dashboards and alerts requirements"
menuTitle: "About dashboards and alerts requirements"
description: "Requirements for installing Grafana Mimir dashboards and alerts."
weight: 10
---

# About Grafana Mimir dashboards and alerts requirements

Grafana Mimir dashboards and alerts require certain labels to exist on metrics scraped from Grafana Mimir.
Your Prometheus or Grafana Agent must be configured to add these labels in order for the dashboards and alerts to function.
The following table shows the required label names and whether they can be customized when [compiling dashboards or alerts from sources]({{< relref "installing-dashboards-and-alerts.md" >}}).

| Label name  | Configurable | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| :---------- | :----------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`   | Yes          | The Kubernetes cluster or datacenter where the Mimir cluster is running. The cluster label can be configured with the `per_cluster_label` field in the mixin config.                                                                                                                                                                                                                                                                                                                             |
| `namespace` | No           | The Kubernetes namespace where the Mimir cluster is running.                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `job`       | Partially    | The Kubernetes namespace and Mimir component in the format `<namespace>/<component>`. When running in monolithic mode, the `<component>` should be `mimir`. When running in microservices mode, the `<component>` should be the name of the specific Mimir component (singular), like `distributor`, `ingester` or `store-gateway`. The label name can't be configured, while the regular expressions used to match components can be configured with the `job_names` field in the mixin config. |
| `pod`       | Yes          | The unique identifier of a Mimir replica (eg. Pod ID when running on Kubernetes). The label name can be configured with the `per_instance_label` field in the mixin config.                                                                                                                                                                                                                                                                                                                      |
| `instance`  | Yes          | The unique identifier of the node or machine where the Mimir replica is running (eg. the node when running on Kubernetes). The label name can be configured with the `per_node_label` field in the mixin config.                                                                                                                                                                                                                                                                                 |

## Job selection

A metric could be exposed by multiple Grafana Mimir components, or even different applications running in the same namespace.
To provide accurate dashboards and alerts, we use the `job` label to select a metric from specific components.
A `job` is a combination of namespace and component, for example `<namespace>/ingester`.

Pre-compiled dashboards and alerts are shipped with a default configuration.
If you compile dashboards and alerts from source, you have the option to customize the regular expression used to select each Mimir component through the `job_names` field in the mixin config.

### Default `job` selection in monolithic mode

When running Grafana Mimir in monolithic mode and using the pre-compiled dashboards and alerts, the `job` label should be set to `<namespace>/mimir`.

### Default `job` selection in microservices mode

When running Grafana Mimir in microservices mode and using the pre-compiled dashboards and alerts, the `job` label should be set according to the following table.

| Mimir service   | Expected `job` label          |
| :-------------- | :---------------------------- |
| Distributor     | `<namespace>/distributor`     |
| Ingester        | `<namespace>/ingester`        |
| Querier         | `<namespace>/querier`         |
| Ruler           | `<namespace>/ruler`           |
| Query-frontend  | `<namespace>/query-frontend`  |
| Query-scheduler | `<namespace>/query-scheduler` |
| Store-gateway   | `<namespace>/store-gateway`   |
| Compactor       | `<namespace>/compactor`       |

## Additional resources metrics

The Grafana Mimir dashboards displaying CPU, memory, disk, and network resources utilization require Prometheus metrics scraped from the following endpoints:

- cAdvisor
- kubelet
- [node_exporter](https://github.com/prometheus/node_exporter)
- [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics) exporter

For more information about the kubelet metrics and cAdvisor metrics exported by the kubelet, refer to [Metrics For Kubernetes System Components](https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/).
