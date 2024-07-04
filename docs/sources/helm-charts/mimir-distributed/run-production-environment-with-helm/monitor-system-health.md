---
aliases:
  - /docs/mimir/latest/operators-guide/monitoring-grafana-mimir/collecting-metrics-and-logs/
description: Learn how to collect metrics and logs from Grafana Mimir or GEM itself.
menuTitle: Monitor system health
title: Monitor the health of your system
weight: 60
refs:
  collect-metrics-and-logs-without-the-helm-chart:
    - pattern: /
      destination: /docs/mimir/<MIMIR_DOCS_VERSION>/manage/monitor-grafana-mimir/collecting-metrics-and-logs/#collect-metrics-and-logs-without-the-helm-chart
  installing-grafana-mimir-dashboards-and-alerts:
    - pattern: /
      destination: /docs/mimir/<MIMIR_DOCS_VERSION>/manage/monitor-grafana-mimir/installing-dashboards-and-alerts/
---

# Monitor the health of your system

You can monitor Grafana Mimir or Grafana Enterprise Metrics by collecting metrics and logs from a Mimir or GEM instance that's running on a Kubernetes cluster. This process is called _metamonitoring_.

As part of *metamonitoring*, You can create dashboards and receive alerts about the metrics and logs collected from Mimir. To set up these dashboards and alerts,
refer to [Installing Grafana Mimir dashboards and alerts](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/monitor-grafana-mimir/installing-dashboards-and-alerts/).

You can also use the self-hosted Grafana Cloud integration to monitor your Mimir system. Refer to [Grafana Cloud: Self-hosted Grafana Mimir integration](/docs/grafana-cloud/monitor-infrastructure/integrations/integration-reference/integration-mimir/) for more information.

To monitor the health of your system without using the Helm chart, see [Collect metrics and logs without the Helm chart].