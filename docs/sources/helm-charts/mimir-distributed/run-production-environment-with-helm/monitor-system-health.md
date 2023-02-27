---
aliases:
  - /docs/mimir/latest/operators-guide/monitoring-grafana-mimir/collecting-metrics-and-logs/
description: Learn how to collect metrics and logs from Grafana Mimir or GEM itself.
menuTitle: Monitor system health
title: Monitor the health of your system
weight: 60
---

# Monitor the health of your system

You can monitor Grafana Mimir or Grafana Enterprise Metrics itself, by collecting metrics and logs from Mimir or GEM that is running on a Kubernetes cluster. This is called _metamonitoring_.

> **Note:** In Grafana, you can create dashboards and receive alerts about those metrics and logs. To set up dashboards and alerts,
> see [Installing Grafana Mimir dashboards and alerts](/docs/mimir/v2.6.x/operators-guide/monitor-grafana-mimir/installing-dashboards-and-alerts/) or [Grafana Cloud: Self-hosted Grafana Mimir integration](/docs/grafana-cloud/integrations/integrations/integration-mimir/).

Alternatively, to monitor the health of your system without using the Helm chart, see [Collect metrics and logs without the Helm chart](/docs/mimir/v2.6.x/operators-guide/monitor-grafana-mimir/collecting-metrics-and-logs/#collect-metrics-and-logs-without-the-helm-chart).

## Configure the Grafana Agent operator via the Helm chart

In the Helm chart, you can configure where to send metrics and logs.
You can send metrics to a Prometheus-compatible server
and logs to a Loki cluster.
The Helm chart can also scrape additional metrics from kube-state-metrics, kubelet, and cAdvisor.

The Helm chart does not collect Prometheus node_exporter metrics;
metrics from node_exporter must all have an instance label on them
that has the same value as the instance label on Mimir metrics.

You can configure your collection of metrics and logs
by using the [Grafana Agent operator](/docs/agent/latest/operator/).
The Helm chart can install and use the Grafana Agent operator.

> **Note:** Before the Helm chart can use the operator,
> you need to manually install all of the Kubernetes [Custom Resource Definitions (CRDs)](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/) from the [Grafana Agent operator YAML files](https://github.com/grafana/agent/tree/main/production/operator/crds).

It’s best to use the Grafana Agent operator for metrics and logs collection.
However, if you prefer not to use it or you already have an existing Grafana Agent that you want to use, see _Collect metrics and logs via Grafana Agent_ documentation in Grafana Mimir version 2.5.0.

1. Store credentials in a Secret:

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

1. Configure Helm chart values:

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

You can also send the metrics that you collected (metamonitoring metrics)
back into Mimir or GEM itself rather than sending them elsewhere.

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
