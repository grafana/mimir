---
aliases:
  - /docs/mimir/latest/operators-guide/monitoring-grafana-mimir/collecting-metrics-and-logs/
  - /docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/monitor-system-health/
description: Learn how to collect metrics and logs from Grafana Mimir or GEM itself.
menuTitle: Monitor system health
title: Monitor the health of your system
weight: 5
---

# Monitor the health of your system

To monitor and understand the health of your Grafana Mimir or Grafana Enterprise Metrics (GEM) database, you should collect and store the metrics and logs it exposes. This process is called _meta-monitoring_. You're "monitoring" your "monitoring database."

## Collect metrics and logs from Grafana Mimir

{{< admonition type="note" >}}
The Grafana Mimir Helm chart contains built-in configurations for meta-monitoring that use the Grafana Agent, which is now **deprecated**. Therefore, we no longer recommend using this approach. Instead we recommend an approach that uses the [Kubernetes Monitoring Helm chart](https://github.com/grafana/k8s-monitoring-helm/tree/main/charts/k8s-monitoring).
{{< /admonition >}}

To collect metrics, logs and traces from Grafana Mimir, use the [built-in Mimir integration](https://github.com/grafana/k8s-monitoring-helm/blob/main/charts/k8s-monitoring/charts/feature-integrations/docs/integrations/mimir.md) provided by the [grafana/k8s-monitoring-helm](https://github.com/grafana/k8s-monitoring-helm) chart. This configures [Grafana Alloy](https://grafana.com/docs/alloy/latest/) to handle all scraping and log collection automatically, no `ServiceMonitors` are needed. You should not enable any `metaMonitoring` settings in the Mimir Helm chart (see note above).

Refer to the [meta-monitoring example](https://github.com/grafana/k8s-monitoring-helm/tree/main/charts/k8s-monitoring/docs/examples/meta-monitoring) for guidance. Update the [`destinations`](https://github.com/grafana/k8s-monitoring-helm/tree/main/charts/k8s-monitoring/docs/destinations) section to specify where to send the metrics, logs, and traces collected from Grafana Mimir or GEM. Metrics must be sent to Prometheus or a Prometheus-compatible TSDB (such as Thanos, another Mimir instance, or Grafana Cloud Metrics). You can configure multiple metrics, logs, and/or traces destinations to forward data to several backends simultaneously.

## Visualize metrics and logs from Grafana Mimir

Once you've collected metrics, logs, and traces from Grafana Mimir or GEM and stored them in their respective databases, you can use this data to build dashboards and define alerts.

Start by configuring Grafana with datasources for metrics, logs, and traces. This typically involves adding Prometheus, Loki, and Tempo datasources that point to the respective backends.

From there, refer to [Installing Grafana Mimir dashboards and alerts](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/installing-dashboards-and-alerts/) for instructions on how to set up our best practice dashboards and alerts for observing Grafana Mimir.
