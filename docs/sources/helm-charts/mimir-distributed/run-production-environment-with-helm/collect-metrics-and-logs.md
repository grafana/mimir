---
aliases:
  - /docs/mimir/latest/operators-guide/monitoring-grafana-mimir/collecting-metrics-and-logs/
description: Learn how to collect metrics and logs from Grafana Mimir or GEM itself.
menuTitle: Metamonitor Mimir or GEM
title: Collect metrics and logs from Grafana Mimir or GEM itself
weight: 60
---

# Collect metrics and logs from Grafana Mimir or GEM itself

You can collect metrics and logs from a Mimir or GEM cluster.

> **Note:** In Grafana, you can create dashboards and receive alerts about those metrics and logs. To set up dashboards and alerts,
> see [Installing Grafana Mimir dashboards and alerts]({{< relref "installing-dashboards-and-alerts.md" >}}) or [Grafana Cloud: Self-hosted Grafana Mimir integration](https://grafana.com/docs/grafana-cloud/integrations/integrations/integration-mimir/)
.

In the Helm chart, you can configure where to send metrics and logs.
You can send metrics to a Prometheus-compatible server
and logs to a Loki cluster.
The Helm chart can also scrape additional metrics from kube-state-metrics, kubelet, and cAdvisor.

The Helm chart does not collect Prometheus node_exporter metrics;
metrics from node_exporter must all have an instance label on them
that has the same value as the instance label on Mimir metrics.

## Configure the Grafana Agent operator via the Helm chart

You can configure your collection of metrics and logs
by using the [Grafana Agent operator](https://grafana.com/docs/agent/latest/operator/).
The Helm chart can install and use the Grafana Agent operator.

> **Note:** Before the Helm chart can use the operator,
> you need to manually install all of the Kubernetes [Custom Resource Definitions (CRDs)](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/) from the [Grafana Agent operator YAML files](https://github.com/grafana/agent/tree/main/production/operator/crds).

It’s best to use the Grafana Agent operator for metrics and logs collection.
However, if you prefer not to use it or you already have an existing Grafana Agent that you want to use, see [Collect metrics and logs via Grafana Agent](#collect-metrics-and-logs-via-grafana-agent) instead.

TODO: ^ Replace with a relref to https://grafana.com/docs/mimir/latest/operators-guide/monitor-grafana-mimir/collecting-metrics-and-logs/#collect-metrics-and-logs-via-grafana-agent.

### Store credentials in a Secret

If Prometheus and Loki are running without authentication, then you scan skip this section.
Metamonitoring supports multiple ways of authentication for metrics and logs. If you are using a secret such as an API
key to authenticate with Prometheus or Loki, then you need to create a Kubernetes Secret with that secret.

This is an example Kubernetes Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: metamonitoring-credentials
data:
  prometheus-api-key: FAKEACCESSKEY
  loki-api-key: FAKESECRETKEY
```

For information about how to create a Kubernetes Secret, see
[Creating a Secret](https://kubernetes.io/docs/concepts/configuration/secret/#creating-a-secret).

TODO: Start here.

### Configure Helm chart values

Merge the following YAML configuration into your Helm values file, and replace the values for `url`, `username`, `passwordSecretName`
, and `passwordSecretKey` with the details of the Prometheus and Loki clusters, and the Secret that you created. If your
Prometheus and Loki servers are running without authentication, then remove the `auth` blocks from the YAML below.

If you already have the Agent operator installed in your Kubernetes cluster, then set `installOperator: false`.

```yaml
metaMonitoring:
  serviceMonitor:
    enabled: true
  grafanaAgent:
    enabled: true
    installOperator: true

    logs:
      remote:
        url: "https://example.com/loki/api/v1/push"
        auth:
          username: "12345"
          passwordSecretName: "metamonitoring-credentials"
          passwordSecretKey: "prometheus-api-key"

    metrics:
      remote:
        url: "https://example.com/api/v1/push"
        auth:
          username: "54321"
          passwordSecretName: "metamonitoring-credentials"
          passwordSecretKey: "loki-api-key"

      scrapeK8s:
        enabled: true
        kubeStateMetrics:
          namespace: kube-system
          labelSelectors:
            app.kubernetes.io/name: kube-state-metrics
```

### Send metrics back into Mimir or GEM

You can also send the collected metamonitoring metrics to the installation of Mimir or GEM.

When you leave the `metamonitoring.grafanaAgent.metrics.remote.url` field empty,
then the chart automatically fills in the address of the GEM gateway Service
or the Mimir NGINX Service.

If you have deployed Mimir, and `metamonitoring.grafanaAgent.metrics.remote.url` is not set,
then the metamonitoring metrics are be sent to the Mimir cluster.
You can query these metrics using the HTTP header X-Scope-OrgID: metamonitoring

If you have deployed GEM, then there are two alternatives:

- If are using the `trust` authentication type (`mimir.structuredConfig.auth.type=trust`),
  then the same instructions apply as for Mimir.

- If you are using the enterprise authentication type (`mimir.structuredConfig.auth.type=enterprise`, which is
  also the default when `enterprise.enabled=true`), then you also need to provide a Secret with the authentication
  token for the tenant.The token should be to an access policy with `metrics:write` scope.
  To set up the Secret, refer to [Credentials](#credentials).
  Assuming you are using the GEM authentication model, the Helm chart values should look like the following example.

```yaml
metaMonitoring:
  serviceMonitor:
    enabled: true
  grafanaAgent:
    enabled: true
    installOperator: true

    metrics:
      remote:
        auth:
          username: metamonitoring
          passwordSecretName: gem-tokens
          passwordSecretKey: metamonitoring
```
