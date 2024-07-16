---
aliases:
  - ../../operators-guide/monitoring-grafana-mimir/collecting-metrics-and-logs/
  - ../../operators-guide/monitor-grafana-mimir/collecting-metrics-and-logs/
description: Learn how to collect metrics and logs from Grafana Mimir itself
menuTitle: Collecting metrics and logs
title: Collecting metrics and logs from Grafana Mimir
weight: 60
---

# Collecting metrics and logs from Grafana Mimir

You can collect logs and metrics from a Mimir or GEM cluster. To set up dashboards and alerts,
see [Installing Grafana Mimir dashboards and alerts]({{< relref "./installing-dashboards-and-alerts" >}})
or [Grafana Cloud: Self-hosted Grafana Mimir integration](/docs/grafana-cloud/monitor-infrastructure/integrations/integration-reference/integration-mimir/).

## Install Grafana Mimir using the Helm chart

As a best practice, use the Grafana Mimir Helm chart to install a Mimir or GEM cluster. When you deploy the Helm chart, ensure that you've enabled `ServiceMonitor` objects. For more information, refer to the [documentation for the Grafana Mimir Helm chart](/docs/helm-charts/mimir-distributed/latest/).

After installing the cluster, deploy the Grafana Kubernetes Monitoring Helm chart, which contains instances of Grafana Alloy for collecting metrics and logs. These Alloy instances detect the `ServiceMonitor` objects deployed as part of the Mimir Helm chart. For more information, refer to [Grafana Kubernetes Monitoring Helm chart](https://grafana.com/docs/grafana-cloud/monitor-infrastructure/kubernetes-monitoring/configuration/helm-chart/).

{{< admonition type="note" >}}
The Grafana Mimir Helm chart contains built-in configurations for Grafana Agent, which is now deprecated. Use the Grafana Alloy configurations included in the Kubernetes Monitoring Helm chart instead.{{< /admonition >}}

## Install Grafana Mimir without the Helm chart

You can still use the dashboards and rules in the monitoring-mixin,
even if you're not deploying Mimir or GEM via the Helm chart.
If you're not using the Helm chart, start by using the Grafana Alloy configuration
from [Collect metrics and logs via Grafana Alloy](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/collecting-metrics-and-logs/#collect-metrics-and-logs-via-grafana-alloy).
It's possible that you need to modify this configuration. For
more information, see [dashboards and alerts requirements]({{< relref "./requirements" >}}).

### Service discovery

As a best practice, deploy Grafana Mimir on Kubernetes. The Grafana Alloy configuration relies on Kubernetes service discovery and Pod labels to constrain the collected metrics and
logs to ones that are strictly related to the Grafana Mimir deployment. If you are deploying Grafana Mimir on something other than Kubernetes, then replace the `discovery.kubernetes` component with another [Alloy component](https://grafana.com/docs/alloy/latest/reference/components) that can discover the Mimir processes.

### Collect metrics and logs via Grafana Alloy

Set up Grafana Alloy to collect logs and metrics from Mimir or GEM. To get started with Grafana Alloy,
refer to [Get started with Grafana Alloy](https://grafana.com/docs/<ALLOY_VERSION>/latest/get-started). After deploying Alloy, refer to [Collect and forward Prometheus metrics](https://grafana.com/docs/alloy/<ALLOY_VERSION>/collect/prometheus-metrics/) for instructions on how to configure your Alloy instance to scrape Mimir or GEM.
