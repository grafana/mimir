# Grafana Mimir Helm chart

Helm chart for deploying [Grafana Mimir](https://grafana.com/docs/mimir/v2.4.x/) or optionally [Grafana Enterprise Metrics](https://grafana.com/docs/enterprise-metrics/v2.4.x/) to Kubernetes. Derived from [Grafana Enterprise Metrics Helm chart](https://github.com/grafana/helm-charts/blob/main/charts/enterprise-metrics/README.md)

See the [Grafana Mimir version 2.4 release notes](https://grafana.com/docs/mimir/v2.4.x/release-notes/v2.4/).

When upgrading from Helm chart version 2.1, please see [Upgrade the Grafana Mimir Helm chart from version 2.1 to 3.0](https://grafana.com/docs/mimir/latest/operators-guide/deploying-grafana-mimir/upgrade-helm-chart-2.1-to-3.0/) as well.

**IMPORTANT**: Always consult the [CHANGELOG.md](./CHANGELOG.md) file and the deprecation list there to learn about breaking changes that require action during upgrade.

# mimir-distributed

![Version: 3.2.0-rc.0](https://img.shields.io/badge/Version-3.2.0--rc.0-informational?style=flat-square) ![AppVersion: 2.4.0](https://img.shields.io/badge/AppVersion-2.4.0-informational?style=flat-square)

Grafana Mimir

## Requirements

Kubernetes: `^1.20.0-0`

| Repository | Name | Version |
|------------|------|---------|
| https://charts.min.io/ | minio(minio) | 4.0.12 |
| https://grafana.github.io/helm-charts | grafana-agent-operator(grafana-agent-operator) | 0.2.5 |

## Dependencies

### Storage

Grafana Mimir and Grafana Enterprise Metrics require an object storage backend to store metrics and indexes.

The default chart values will deploy [Minio](https://min.io) for initial set up. Production deployments should use a separately deployed object store.
See [Grafana Mimir documentation](https://grafana.com/docs/mimir/v2.4.x/) for details on storage types and documentation.

### Grafana Enterprise Metrics license

In order to use the enterprise features of this chart, you need to provide the contents of a Grafana Enterprise Metrics license file as the value for the `license.contents` variable.
To obtain a Grafana Enterprise Metrics license, refer to [Get a license](https://grafana.com/docs/enterprise-metrics/v2.4.x/setup/#get-a-gem-license).

### Helm3

The chart requires at least Helm version 3 to work.

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

### Migration from Cortex to Grafana Mimir

Please consult the [Migration from Cortex to Grafana](https://grafana.com/docs/mimir/v2.4.x/migration-guide/migrating-from-cortex/) guide on how to update the configuration.
Prepare a custom values file with the contents:

```yaml
nameOverride: cortex

mimir:
  config: |
    <text of configuration>
```

Perform the upgrade:

```bash
helm upgrade <cluster name> grafana/mimir-distributed -f <custom values file>
```

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

## Scale values

The default Helm chart values in the `values.yaml` file are configured to allow you to quickly test out Grafana Mimir.
Alternative values files are included to provide a more realistic configuration that should facilitate a certain level of ingest load.

### Small

The `small.yaml` values file configures the Grafana Mimir cluster to
handle production ingestion of ~1M series using the blocks storage engine.
Query requirements can vary dramatically depending on query rate and query
ranges. The values here satisfy a "usual" query load as seen from our
production clusters at this scale.
It is important to ensure that you run no more than one ingester replica
per node so that a single node failure does not cause data loss. Zone aware
replication can be configured to ensure data replication spans availability
zones. Refer to [Zone Aware Replication](https://grafana.com/docs/mimir/v2.4.x/operators-guide/configuring/configuring-zone-aware-replication/)
for more information.
Minio is no longer enabled and you are encouraged to use your cloud providers
object storage service for production deployments.

To deploy a cluster using `small.yaml` values file:

```bash
helm install <cluster name> grafana/mimir-distributed -f small.yaml
```

### Large

The `large.yaml` values file configures the Grafana Mimir cluster to
handle production ingestion of ~10M series using the blocks storage engine.
Query requirements can vary dramatically depending on query rate and query
ranges. The values here satisfy a "usual" query load as seen from our
production clusters at this scale.
It is important to ensure that you run no more than one ingester replica
per node so that a single node failure does not cause data loss. Zone aware
replication can be configured to ensure data replication spans availability
zones. Refer to [Zone Aware Replication](https://grafana.com/docs/mimir/v2.4.x/operators-guide/configuring/configuring-zone-aware-replication/)
for more information.
Minio is no longer enabled and you are encouraged to use your cloud providers
object storage service for production deployments.

To deploy a cluster using the `large.yaml` values file:

```bash
helm install <cluster name> grafana/mimir-distributed -f large.yaml
```

# Development

To configure a local default storage class for k3d:

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

To install the chart with the values used in CI tests:

```bash
helm install test ./ --values ./ci/test-values.yaml
```

# Contributing/Releasing

Please see the dedicated "[Contributing to Grafana Mimir helm chart](https://github.com/grafana/mimir/tree/main/docs/internal/contributing/contributing-to-helm-chart.md)" page.
