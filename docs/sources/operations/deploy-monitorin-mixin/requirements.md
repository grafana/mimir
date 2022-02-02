+++
title = "Requirements"
weight = 100
+++

# Grafana Mimir dashboards and alerts requirements

Grafana Mimir dashboards and alerts require your Prometheus or Grafana Agent to be configured to add some labels to metrics scraped by Grafana Mimir. The following table shows the required labels and whether they can be customized when [compiling dashboards or alerts from sources]({{< relref "./install-dashboards-and-alerts.md" >}}).

| Label name  | Configurable | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| :---------- | :----------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`   | No           | The Kubernetes cluster or datacenter where the Mimir cluster is running.                                                                                                                                                                                                                                                                                                                                                                                                          |
| `namespace` | No           | The Kubernetes namespace where the Mimir cluster is running.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `job`       | Partially    | The Mimir deployment in the format `<namespace>/<deployment>`. When running in single-binary mode the `<deployment>` should be `mimir`, while when running in microservices mode the `<deployment>` should be the name of the specific Mimir service (singular), like `distributor`, `ingester` or `store-gateway`. The label name can't be configured, while the regular expressions used to match deployments can be configured with the `job_names` field in the mixin config. |
| `pod`       | Yes          | The unique identifier of a Mimir replica (eg. Pod ID when running on Kubernetes). The label name can be configured with the `per_instance_label` field in the mixin config.                                                                                                                                                                                                                                                                                                       |
| `instance`  | Yes          | The unique identifier of the node or machine where the Mimir replica is running (eg. the node when running on Kubernetes). The label name can be configured with the `per_node_label` field in the mixin config.                                                                                                                                                                                                                                                                  |

## Job selection

A metric could be exposed by multiple Grafana Mimir services, or even different applications running in the same namespace. To provide you accurate dashboards and alerts, we're used to select a metric from specific jobs. A job is a combination of namespace and deployment, for example `<namespace>/ingester`.

Pre-compiled dashboards and alerts are shipped with a default configuration, while if you compile them from source you have the option to customize the regular expression used to select each Mimir service through the `job_names` field in the mixin config.

### Default `job` selection in single-binary mode

When running Grafana Mimir in single-binary mode and using the pre-compiled dashboards and alerts, the Grafana Mimir deployment should be named `mimir`.

### Default `job` selection in microservices mode

When running Grafana Mimir in microservices mode and using the pre-compiled dashboards and alerts, the Grafana Mimir deployments should be named according to the following table.

| Mimir service   | Expected deployment name |
| :-------------- | :----------------------- |
| Distributor     | `distributor`            |
| Ingester        | `ingester`               |
| Querier         | `querier`                |
| Ruler           | `ruler`                  |
| Query-frontend  | `query-frontend`         |
| Query-scheduler | `query-frontend`         |
| Store-gateway   | `store-gateway`          |
| Compactor       | `compactor`              |
