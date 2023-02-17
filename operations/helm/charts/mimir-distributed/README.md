# Grafana Mimir Helm chart

Helm chart for deploying [Grafana Mimir](https://grafana.com/docs/mimir/v2.5.x/) or optionally [Grafana Enterprise Metrics](https://grafana.com/docs/enterprise-metrics/v2.5.x/) to Kubernetes. Derived from [Grafana Enterprise Metrics Helm chart](https://github.com/grafana/helm-charts/blob/main/charts/enterprise-metrics/README.md)

See the [Grafana Mimir version 2.5 release notes](https://grafana.com/docs/mimir/v2.5.x/release-notes/v2.5/).

When upgrading from Helm chart version 3.x, please see [Migrate from single zone to zone-aware replication with Helm](https://grafana.com/docs/mimir/latest/migration-guide/migrating-from-single-zone-with-helm/).
When upgrading from Helm chart version 2.1, please see [Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0](https://grafana.com/docs/mimir/latest/operators-guide/deploying-grafana-mimir/upgrade-helm-chart-2.1-to-3.0/) as well.

**IMPORTANT**: Always consult the [CHANGELOG.md](./CHANGELOG.md) file and the deprecation list there to learn about breaking changes that require action during upgrade.

# mimir-distributed

![Version: 4.1.0-weekly.225](https://img.shields.io/badge/Version-4.1.0--weekly.225-informational?style=flat-square) ![AppVersion: r225](https://img.shields.io/badge/AppVersion-r225-informational?style=flat-square)

Grafana Mimir

## Requirements

Kubernetes: `^1.20.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://charts.min.io/ | minio(minio) | 5.0.4 |
| https://grafana.github.io/helm-charts | grafana-agent-operator(grafana-agent-operator) | 0.2.8 |
| https://grafana.github.io/helm-charts | rollout_operator(rollout-operator) | 0.2.0 |

## Dependencies

### Storage

Grafana Mimir and Grafana Enterprise Metrics require an object storage backend to store metrics and indexes.

The default chart values will deploy [Minio](https://min.io) for initial set up. Production deployments should use a separately deployed object store.
See [Grafana Mimir documentation](https://grafana.com/docs/mimir/v2.5.x/) for details on storage types and documentation.

### Grafana Enterprise Metrics license

In order to use the enterprise features of this chart, you need to provide the contents of a Grafana Enterprise Metrics license file as the value for the `license.contents` variable.
To obtain a Grafana Enterprise Metrics license, refer to [Get a license](https://grafana.com/docs/enterprise-metrics/v2.5.x/setup/#get-a-gem-license).

### Helm3

The chart requires at least Helm version 3.0.0 to work.

## Installation

This section describes various use cases for installation, upgrade and migration from different systems and versions.

### Preparation

These are the common tasks to perform before any of the use cases.

```bash
# Add the repository
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

### Installation of Grafana Mimir

```bash
helm install <cluster name> grafana/mimir-distributed
```

As part of this chart many different pods and services are installed which all
have varying resource requirements. Please make sure that you have sufficient
resources (CPU/memory) available in your cluster before installing Grafana Mimir Helm Chart.

For details about setting up Grafana Mimir, refer to [Get started with Grafana Mimir using the Helm chart](https://grafana.com/docs/mimir/latest/operators-guide/deploy-grafana-mimir/getting-started-helm-charts/).

### Migrate from Cortex to Grafana Mimir

To update the configuration, see [Migrating from Cortex to Grafana Mimir](https://grafana.com/docs/mimir/latest/migration-guide/migrating-from-cortex/).

## Installation of Grafana Enterprise Metrics

To install the chart with licensed features enabled, using a local Grafana Enterprise Metrics license file called `license.jwt`, provide the license as a value and set the `enterprise.enabled` value to `true`.

```bash
helm install <cluster name> grafana/mimir-distributed --set 'enterprise.enabled=true' --set-file 'license.contents=./license.jwt'
```

### Upgrade from version 1.7 of Grafana Enterprise Metrics

To make the necessary configuration changes, see [Migrating from Grafana Enterprise Metrics 1.7](https://grafana.com/docs/enterprise-metrics/latest/migrating-from-gem-1.7/).
Prepare a custom values file, with the contents:

```yaml
enterprise:
  enabled: true
  legacyLabels: true

mimir:
  config: |
    <text of configuration>
```

The value (`enterprise.legacyLabels`) is needed because this chart installs objects with kubernetes de-facto standard labels by default which are different from older Grafana Enterprise Metrics labels.

```bash
helm upgrade <cluster name> grafana/mimir-distributed -f <custom values file> --set-file 'license.contents=./license.jwt'
```

### Upgrade from Grafana Mimir to Grafana Enterprise Metrics

Use the name override to align labels and selectors and enable licensed features.

```yaml
nameOverride: mimir-distributed

enterprise:
  enabled: true
```

## Sizing values

The default Helm chart values in the `values.yaml` file are configured to allow you to quickly test out Grafana Mimir.
Alternative values files are included to provide a more realistic configuration that should facilitate a certain level of ingest load.

For details about the sizing plans, see [Capacity planning and Pod scheduling](https://grafana.com/docs/mimir/latest/operators-guide/run-production-environment-with-helm/#plan-capacity).

# Contributing and releasing

Please see the dedicated "[Contributing to Grafana Mimir helm chart](https://github.com/grafana/mimir/tree/main/docs/internal/contributing/contributing-to-helm-chart.md)" page.
