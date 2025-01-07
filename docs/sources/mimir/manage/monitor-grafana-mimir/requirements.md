---
aliases:
  - ../../operators-guide/monitoring-grafana-mimir/requirements/
  - ../../operators-guide/visualizing-metrics/requirements/
  - ../../operators-guide/monitor-grafana-mimir/requirements/
description: Requirements for installing Grafana Mimir dashboards and alerts.
menuTitle: About dashboards and alerts requirements
title: About Grafana Mimir dashboards and alerts requirements
weight: 10
---

# About Grafana Mimir dashboards and alerts requirements

Grafana Mimir dashboards and alerts require certain labels to exist on metrics scraped from Grafana Mimir.

The `mimir-distributed` Helm chart provides metamonitoring support, which takes care of these labels.
For more information about Helm chart metamonitoring, refer to [Collect metrics and logs via the Helm chart](/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/monitor-system-health/).
If you are using Helm chart metamonitoring, go to [Installing Grafana Mimir dashboards and alerts]({{< relref "./installing-dashboards-and-alerts" >}}).

If you're not using the Helm chart, you must configure your Prometheus or Grafana Alloy instance to add these labels for the dashboards and alerts to function.
The following table shows the required label names and whether they can be customized when [compiling dashboards or alerts from sources]({{< relref "./installing-dashboards-and-alerts" >}}).

| Label name  | Configurable? | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| :---------- | :------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| `cluster`   | Yes           | The Kubernetes cluster or datacenter where the Mimir cluster is running. You can configure the cluster label via the `per_cluster_label` field in the mixin configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `namespace` | Yes           | The Kubernetes namespace where the Mimir cluster is running. You can configure the namespace label via the `per_namespace_label` field in the mixin configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |     |
| `job`       | Yes           | A prefix (by default `<namespace>/`) followed by the Mimir component. When running in monolithic mode, make sure that the `<component>` is `mimir`. When running in microservices mode, make sure that the `<component>` is the name of the specific Mimir component (singular), such as `distributor`, `ingester`, or `store-gateway`. Similarly, in read-write mode, make sure the `<component>` is either `mimir-read`, `mimir-write`, or `mimir-backend`. In the mixin configuration, you can configure the prefix via the `job_prefix` field, the label name via the `per_job_label` field, and the regular expressions that are used to match components via the `job_names` field. |
| `pod`       | Yes           | The unique identifier of a Mimir replica, for example the Pod ID when running on Kubernetes. You can configure the instance label via the `per_instance_label` field in the mixin configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `instance`  | Yes           | The unique identifier of the node or machine where the Mimir replica is running, for example the node when running on Kubernetes. You can configure the node label via the `per_node_label` field in the mixin configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |

For rules and alerts to function, you must configure your Prometheus or Grafana Alloy instance to scrape metrics from Grafana Mimir at an interval of `15s` or shorter.

## Deployment type

By default, Grafana Mimir dashboards assume Mimir is deployed in containers orchestrated by Kubernetes.
If you're running Mimir on baremetal, set the configuration field `deployment_type: 'baremetal'` and [re-compile the dashboards]({{< relref "./installing-dashboards-and-alerts" >}}).

## Job selection

A metric could be exposed by multiple Grafana Mimir components, or even different applications running in the same namespace.
To provide accurate dashboards and alerts, the job label (by default `job`) selects a metric from specific components.
A job is a combination of a prefix and component. The default prefix is the Kubernetes namespace followed by a slash, for example `<namespace>/` in `<namespace>/ingester`.

Pre-compiled dashboards and alerts are shipped with a default configuration.
If you compile dashboards and alerts from source, you have the option to customize a) the label used for the job selection via the `per_job_label` field, b) the prefix expected in the label value, which you can omit by setting it to `''` via the `job_prefix` field, and c) the regular expression used to select each Mimir component via the `job_names` field in the mixin configuration.

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

### Default `job` selection in read-write mode

When running Grafana Mimir in read-write mode and using the pre-compiled dashboards and alerts, set the `job` label accordingly:

| Mimir service | Expected `job` label        |
| :------------ | :-------------------------- |
| Mimir Read    | `<namespace>/mimir-read`    |
| Mimir Write   | `<namespace>/mimir-write`   |
| Mimir Backend | `<namespace>/mimir-backend` |

## Additional resources metrics

The Grafana Mimir dashboards displaying CPU, memory, disk, and network resources utilization require Prometheus metrics scraped from the following endpoints:

- [cAdvisor](https://github.com/google/cadvisor)
- [kubelet](https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/)
- [Node Exporter](https://github.com/prometheus/node_exporter)
- [kube-state-metrics](https://github.com/kubernetes/kube-state-metrics) exporter

For more information about the kubelet metrics and cAdvisor metrics exported by the kubelet, refer to [Metrics For Kubernetes System Components](https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/).

Metrics from kubelet, kube-state-metrics, and cAdvisor must all have a `cluster` label with the same value as in the
Mimir metrics.

Metrics from Node Exporter and cAdvisor must all have an `instance` label on them that has the same value as the `instance` label on Mimir metrics.

## Log labels

The **Slow queries** dashboard uses a Loki data source with the logs from Grafana Mimir to visualize slow queries. The query-frontend component logs query statistics when the `-query-frontend.query-stats-enabled` parameter is set to `true`.
These logs need to have specific labels in order for the dashboard to work.

| Label name  | Configurable? | Description                                                                                                                                                                |
| :---------- | :------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`   | Yes           | The Kubernetes cluster or datacenter where the Mimir cluster is running. You can configure the cluster label via the `per_cluster_label` field in the mixin configuration. |
| `namespace` | No            | The Kubernetes namespace where the Mimir cluster is running.                                                                                                               |
| `name`      | Yes           | Name of the component. For example, `query-frontend`. You can configure the cluster label via the `per_component_loki_label` field in the mixin configuration.             |
