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

To monitor and understand the health of your Grafana Mimir or Grafana Enterprise Metrics (GEM) database, you should collect and store the metrics and logs it exposes. This process is called _metamonitoring_. You're "monitoring" your "monitoring database."

## Collect metrics and logs from Grafana Mimir

{{< admonition type="note" >}}
The Grafana Mimir Helm chart contains built-in configurations for metamonitoirng that use the Grafana Agent, which is now deprecated. Therefore, we no longer recommend using this approach. Instead we recommend an approach that uses the [Kubernetes Monitoring Helm chart](https://github.com/grafana/k8s-monitoring-helm/tree/main/charts/k8s-monitoring).
{{< /admonition >}}

To collect metrics and logs from Grafana Mimir, use the [built-in Mimir integration](https://github.com/grafana/k8s-monitoring-helm/blob/main/charts/k8s-monitoring/charts/feature-integrations/docs/integrations/mimir.md) provided by the [grafana/k8s-monitoring-helm](https://github.com/grafana/k8s-monitoring-helm) chart. This configures [Grafana Alloy](https://grafana.com/docs/alloy/latest/) to handle all scraping and log collection automatically—no `ServiceMonitors` are needed. You should not enable any metaMonitoring settings in the Mimir Helm chart (see note). 

See the [meta-monitoring example](https://github.com/grafana/k8s-monitoring-helm/tree/main/charts/k8s-monitoring/docs/examples/meta-monitoring) as a reference. You will need to update the `destinations` section to the metrics and logs databases that you'd like to use to store the metrics and logs scraped from Grafana Mimir or GEM. Metrics must be stored in Prometheus or a Prometheus-compatible TSDB (e.g., Thanos, a separate Grafana Mimir, or Grafana Cloud Metrics). You can even define multiple metrics and/or logs `destinations` to allow you to forward the metrics and logs to multiple places. 

## Visualize metrics and logs from Grafana Mimir

Once you have collected the metrics and logs collected from your Grafana Mimir or GEM and stored them in separate metrics and logs databases, you can create dashboards and alerts from this data.

Start by configuring Grafana to read from these metrics and logs databases. This requires adding Prometheus and Loki datasources that point to these databases. 

From there, refer to [Installing Grafana Mimir dashboards and alerts](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/installing-dashboards-and-alerts/) for instructions on how to set up our best practice dashboards and alerts for observing Grafana Mimir. 


