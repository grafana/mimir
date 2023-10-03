# Grafana Mimir Helm chart

Helm chart for deploying [Grafana Mimir](https://grafana.com/docs/mimir/latest/) or optionally [Grafana Enterprise Metrics](https://grafana.com/docs/enterprise-metrics/latest/) to Kubernetes.

For the full documentation, visit [Grafana mimir-distributed Helm chart documentation](https://grafana.com/docs/helm-charts/mimir-distributed/latest/).

> **Note:** The documentation version is derived from the Helm chart version which is 5.2.0-weekly.258.

When upgrading from Helm chart version 4.X, please see [Migrate the Helm chart from version 4.x to 5.0](https://grafana.com/docs/helm-charts/mimir-distributed/latest/migration-guides/migrate-helm-chart-4.x-to-5.0/).
When upgrading from Helm chart version 3.x, please see [Migrate from single zone to zone-aware replication with Helm](https://grafana.com/docs/helm-charts/mimir-distributed/latest/migration-guides/migrate-from-single-zone-with-helm/).
When upgrading from Helm chart version 2.1, please see [Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0](https://grafana.com/docs/helm-charts/mimir-distributed/latest/migration-guides/migrate-helm-chart-2.1-to-3.0/) as well.

**IMPORTANT**: Always consult the [CHANGELOG.md](./CHANGELOG.md) file and the deprecation list there to learn about breaking changes that require action during upgrade.

# mimir-distributed

![Version: 5.2.0-weekly.258](https://img.shields.io/badge/Version-5.2.0--weekly.258-informational?style=flat-square) ![AppVersion: r258](https://img.shields.io/badge/AppVersion-r258-informational?style=flat-square)

Grafana Mimir

## Requirements

Kubernetes: `^1.20.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://charts.min.io/ | minio(minio) | 5.0.7 |
| https://grafana.github.io/helm-charts | grafana-agent-operator(grafana-agent-operator) | 0.2.8 |
| https://grafana.github.io/helm-charts | rollout_operator(rollout-operator) | 0.9.1 |

# Contributing and releasing

Please see the dedicated "[Contributing to Grafana Mimir helm chart](https://github.com/grafana/mimir/tree/main/docs/internal/contributing/contributing-to-helm-chart.md)" page.
