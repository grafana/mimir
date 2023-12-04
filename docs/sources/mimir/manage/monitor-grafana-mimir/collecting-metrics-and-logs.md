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

It is easier and best to monitor a cluster if it was installed via the Grafana Mimir Helm chart.
For more information, see the [documentation for the Grafana Mimir Helm chart](/docs/helm-charts/mimir-distributed/latest/).

It is also possible to use this integration if Mimir was deployed another way.
For more information, see [Collect metrics and logs without the Helm chart]({{< relref "#collect-metrics-and-logs-without-the-helm-chart" >}}).

## Collect metrics and logs without the Helm chart

You can still use the dashboards and rules in the monitoring-mixin,
even if Mimir or GEM is not deployed via the Helm chart.
If you are not using the Helm chart, start by using the Agent configuration
from [Collect metrics and logs via Grafana Agent]({{< relref "#collect-metrics-and-logs-via-grafana-agent" >}}).
You might need to modify it. For
more information, see [dashboards and alerts requirements]({{< relref "./requirements" >}}).

### Service discovery

The Agent configuration relies on Kubernetes service discovery and Pod labels to constrain the collected metrics and
logs to ones that are strictly related to the Grafana Mimir deployment. If you are deploying Grafana Mimir on something other than Kubernetes,
then replace the `kubernetes_sd_configs` block with a block from
the [Agent configuration](/docs/agent/latest/configuration/) that can discover the Mimir processes.

### Collect metrics and logs via Grafana Agent

Set up a Grafana Agent that collects logs and metrics from Mimir or GEM. To set up Grafana Agent,
see [Set up Grafana Agent](/docs/agent/latest/set-up/). After your Agent is deployed, use the [example Agent configuration](#example-agent-configuration) to configure the Agent to scrape Mimir or GEM.

#### Caveats

Managing your own Agent comes with some caveats:

- You will have to keep the Agent configuration up to date manually as you update the Mimir Helm chart. While we will
  try to keep this article up to date, we cannot guarantee that
  the [example Agent configuration](#example-agent-configuration) will always work.
- The static configuration makes some assumptions about the naming of the chart, such as that you have not overridden
  the `fullnameOverride` in the Helm chart.
- The static configuration cannot be selective in the PersistentVolumes metrics it collects from Kubelet, so it will
  scrape metrics for all PersistentVolumes.
- The static configuration hardcodes the value of the `cluster` label on all metrics and logs. This means that the
  configuration cannot account for multiple installations of the Helm chart.

If possible, upgrade the Mimir Helm chart to version 3.0 or higher and use
the [built-in Grafana Agent operator](/docs/helm-charts/mimir-distributed/latest/run-production-environment-with-helm/monitor-system-health/). Using the Agent operator allows the
chart to automatically configure the Agent, eliminating the aforementioned caveats.

#### Example Agent configuration

In the following example Grafana Agent configuration file for collecting logs and metrics, replace `url`, `password`, and `username` in
the `logs` and `metrics` blocks with the details of your Prometheus and Loki clusters.

```yaml
logs:
  configs:
    - clients:
        - basic_auth:
            password: xxx
            username: xxx
          url: https://example.com/loki/api/v1/push
      name: integrations
      positions:
        filename: /tmp/positions.yaml
      scrape_configs:
        - job_name: integrations/grafana-mimir-logs
          kubernetes_sd_configs:
            - role: pod
          pipeline_stages:
            - cri: {}
          relabel_configs:
            - action: keep
              regex: mimir-distributed-.*
              source_labels:
                - __meta_kubernetes_pod_label_helm_sh_chart
            - source_labels:
                - __meta_kubernetes_pod_node_name
              target_label: __host__
            - action: replace
              replacement: $1
              separator: /
              source_labels:
                - __meta_kubernetes_namespace
                - __meta_kubernetes_pod_container_name
              target_label: job
            - action: replace
              regex: ""
              replacement: k8s-cluster
              separator: ""
              source_labels:
                - cluster
              target_label: cluster
            - action: replace
              source_labels:
                - __meta_kubernetes_namespace
              target_label: namespace
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_name
              target_label: pod
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_container_name
              target_label: name
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_container_name
              target_label: container
            - replacement: /var/log/pods/*$1/*.log
              separator: /
              source_labels:
                - __meta_kubernetes_pod_uid
                - __meta_kubernetes_pod_container_name
              target_label: __path__
      target_config:
        sync_period: 10s
metrics:
  configs:
    - name: integrations
      remote_write:
        - basic_auth:
            password: xxx
            username: xxx
          url: https://example.com/api/prom/push
      scrape_configs:
        - job_name: integrations/grafana-mimir/kube-state-metrics
          kubernetes_sd_configs:
            - role: pod
          metric_relabel_configs:
            - action: keep
              regex: (.*-mimir-)?alertmanager.*|(.*-mimir-)?compactor.*|(.*-mimir-)?distributor.*|(.*-mimir-)?(gateway|cortex-gw|cortex-gw).*|(.*-mimir-)?ingester.*|(.*-mimir-)?querier.*|(.*-mimir-)?query-frontend.*|(.*-mimir-)?query-scheduler.*|(.*-mimir-)?ruler.*|(.*-mimir-)?store-gateway.*
              separator: ""
              source_labels:
                - deployment
                - statefulset
                - pod
          relabel_configs:
            - action: keep
              regex: kube-state-metrics
              source_labels:
                - __meta_kubernetes_pod_label_app_kubernetes_io_name
            - action: replace
              regex: ""
              replacement: k8s-cluster
              separator: ""
              source_labels:
                - cluster
              target_label: cluster
        - bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
          job_name: integrations/grafana-mimir/kubelet
          kubernetes_sd_configs:
            - role: node
          metric_relabel_configs:
            - action: keep
              regex: kubelet_volume_stats.*
              source_labels:
                - __name__
          relabel_configs:
            - replacement: kubernetes.default.svc.cluster.local:443
              target_label: __address__
            - regex: (.+)
              replacement: /api/v1/nodes/${1}/proxy/metrics
              source_labels:
                - __meta_kubernetes_node_name
              target_label: __metrics_path__
            - action: replace
              regex: ""
              replacement: k8s-cluster
              separator: ""
              source_labels:
                - cluster
              target_label: cluster
          scheme: https
          tls_config:
            ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
            insecure_skip_verify: false
            server_name: kubernetes
        - bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
          job_name: integrations/grafana-mimir/cadvisor
          kubernetes_sd_configs:
            - role: node
          metric_relabel_configs:
            - action: keep
              regex: (.*-mimir-)?alertmanager.*|(.*-mimir-)?compactor.*|(.*-mimir-)?distributor.*|(.*-mimir-)?(gateway|cortex-gw|cortex-gw).*|(.*-mimir-)?ingester.*|(.*-mimir-)?querier.*|(.*-mimir-)?query-frontend.*|(.*-mimir-)?query-scheduler.*|(.*-mimir-)?ruler.*|(.*-mimir-)?store-gateway.*
              source_labels:
                - pod
          relabel_configs:
            - replacement: kubernetes.default.svc.cluster.local:443
              target_label: __address__
            - regex: (.+)
              replacement: /api/v1/nodes/${1}/proxy/metrics/cadvisor
              source_labels:
                - __meta_kubernetes_node_name
              target_label: __metrics_path__
            - action: replace
              regex: ""
              replacement: k8s-cluster
              separator: ""
              source_labels:
                - cluster
              target_label: cluster
          scheme: https
          tls_config:
            ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
            insecure_skip_verify: false
            server_name: kubernetes
        - job_name: integrations/grafana-mimir/metrics
          kubernetes_sd_configs:
            - role: pod
          relabel_configs:
            - action: keep
              regex: .*metrics
              source_labels:
                - __meta_kubernetes_pod_container_port_name
            - action: keep
              regex: mimir-distributed-.*
              source_labels:
                - __meta_kubernetes_pod_label_helm_sh_chart
            - action: replace
              regex: ""
              replacement: k8s-cluster
              separator: ""
              source_labels:
                - cluster
              target_label: cluster
            - action: replace
              source_labels:
                - __meta_kubernetes_namespace
              target_label: namespace
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_name
              target_label: pod
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_container_name
              target_label: container
            - action: replace
              separator: ""
              source_labels:
                - __meta_kubernetes_pod_label_name
                - __meta_kubernetes_pod_label_app_kubernetes_io_component
              target_label: __tmp_component_name
            - action: replace
              separator: /
              source_labels:
                - __meta_kubernetes_namespace
                - __tmp_component_name
              target_label: job
            - action: replace
              source_labels:
                - __meta_kubernetes_pod_node_name
              target_label: instance
  global:
    scrape_interval: 15s
  wal_directory: /tmp/grafana-agent-wal
```
