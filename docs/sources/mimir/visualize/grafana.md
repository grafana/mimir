---
description: Visualize your metric data with Grafana
title: Visualize metric data
menuTitle: Visualize data
weight: 45
keywords:
  - visualize
  - grafana
  - dashboards
---

# Visualize metric data

[Grafana](/grafana/download/) has built-in support for Mimir through the Prometheus data source type.

1. Log in to your Grafana instance.
   If this is your first time running Grafana,
   the username and password are both `admin`.
1. From within Grafana, go to `Connections` > `Add new connection`.
1. Search for Prometheus, and select **Create a Prometheus data source**.
1. In the **HTTP** > **Prometheus server URL** field, point to the proxy in front of Mimir (such as Nginx or the GEM gateway) rather than to one component of Mimir.

   - When running locally or with Docker using port mapping,
     the address is likely `http://localhost:9090`.

   - When running with docker-compose or Kubernetes,
     the address is likely `http://prometheus:9090`.

1. To see metrics, select **Explore**.
1. Select a metric, apply (optional) label filters, and select **Run query**.
1. Learn more about PromQL and querying by reading [Querying Prometheus](https://prometheus.io/docs/prometheus/latest/querying/basics/).

## See also

- Read more about Grafana's [Explore](http://docs.grafana.org/features/explore) feature.
- Read about Grafana’s built-in support for the [Prometheus data source](/docs/grafana/latest/datasources/prometheus/).
