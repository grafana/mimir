---
title: "Migrate from Cortex to Grafana Mimir"
menuTitle: "Migrate from Cortex"
description: "Learn how to migrate your deployment of Cortex to Grafana Mimir to simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage."
weight: 10
refs:
  mimirtool_rules:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/manage/tools/mimirtool/#rules
  convert:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/manage/tools/mimirtool/#convert
  additional-resources-metrics:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/manage/monitor-grafana-mimir/requirements/#additional-resources-metrics
  mimirtool-config-convert:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/manage/tools/mimirtool/#convert
  migrate-from-cortex:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/set-up/migrate/migrate-from-cortex/
  monitoring-mixin-dashboards:
    - pattern: /
      destination: /docs/mimir/<MIMIR DOCS VERSION>/manage/monitor-grafana-mimir/dashboards/
---

# Migrate from Cortex to Grafana Mimir

As an operator, you can migrate a Helm deployment of [Cortex](https://cortexmetrics.io/) to Grafana Mimir.
The overview includes the steps required for any environment. To migrate deployment environments with Helm, see [Migrate to Grafana Mimir using Helm](#migrate-to-grafana-mimir-using-helm).

> **Note:** This document was tested with Cortex versions 1.10 and 1.11. It might work with more recent versions of Cortex, but that is not guaranteed.

To migrate a Jsonnet deployment of Cortex refer to [Migrate from Cortex].

Grafana Mimir includes significant changes that simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage.

The changes make Grafana Mimir easier to run out of the box:

- Removed configuration parameters that don't require tuning
- Renamed some parameters so that they're more easily understood
- Updated the default values of some existing parameters

The `mimirtool` automates configuration conversion.
It provides a simple migration by generating Mimir configuration from Cortex configuration.

**Before you begin:**

- Ensure that you are running either Cortex 1.10.X or Cortex 1.11.X.

  If you are running an older version of Cortex, upgrade to [Cortex 1.11.1](https://github.com/cortexproject/cortex/releases) before proceeding with the migration.

- Ensure you have installed Cortex alerting and recording rules as well as Cortex dashboards.

  Using the monitoring mixin, you need to install both alerting and recording rules in either Prometheus or Cortex. You also need to install dashboards in Grafana.
  To download a prebuilt ZIP file that contains the alerting and recording rules, refer to [Release Cortex-jsonnet 1.11.0](https://github.com/grafana/cortex-jsonnet/releases/download/1.11.0/cortex-mixin.zip).

  To upload rules to the ruler using mimirtool, refer to [mimirtool rules].
  To import the dashboards into Grafana, refer to [Import dashboard](/docs/grafana/latest/dashboards/export-import/#import-dashboard)
  .

## Notable changes

> **Note:** For the full list of changes, refer to Mimir’s [CHANGELOG](https://github.com/grafana/mimir/blob/main/CHANGELOG.md).

- The Grafana Mimir HTTP server defaults to listening on port 8080; Cortex defaults to listening on port 80.
  To maintain port 80 as the listening port, set `-server.http-listen-port=80`.
- Grafana Mimir uses `anonymous` as the default tenant ID when `-auth.multitenancy=false`.
  Cortex uses `fake` as the default tenant ID when `-auth.enabled=false`.
  Use `-auth.no-auth-tenant=fake` when `-auth.multitenancy=false` to match the Cortex default tenant ID.
- Grafana Mimir removes the legacy HTTP prefixes deprecated in Cortex.

  - Query endpoints

    | Legacy                                                  | Current                                                    |
    | ------------------------------------------------------- | ---------------------------------------------------------- |
    | `/<legacy-http-prefix>/api/v1/query`                    | `<prometheus-http-prefix>/api/v1/query`                    |
    | `/<legacy-http-prefix>/api/v1/query_range`              | `<prometheus-http-prefix>/api/v1/query_range`              |
    | `/<legacy-http-prefix>/api/v1/query_exemplars`          | `<prometheus-http-prefix>/api/v1/query_exemplars`          |
    | `/<legacy-http-prefix>/api/v1/series`                   | `<prometheus-http-prefix>/api/v1/series`                   |
    | `/<legacy-http-prefix>/api/v1/labels`                   | `<prometheus-http-prefix>/api/v1/labels`                   |
    | `/<legacy-http-prefix>/api/v1/label/{name}/values`      | `<prometheus-http-prefix>/api/v1/label/{name}/values`      |
    | `/<legacy-http-prefix>/api/v1/metadata`                 | `<prometheus-http-prefix>/api/v1/metadata`                 |
    | `/<legacy-http-prefix>/api/v1/read`                     | `<prometheus-http-prefix>/api/v1/read`                     |
    | `/<legacy-http-prefix>/api/v1/cardinality/label_names`  | `<prometheus-http-prefix>/api/v1/cardinality/label_names`  |
    | `/<legacy-http-prefix>/api/v1/cardinality/label_values` | `<prometheus-http-prefix>/api/v1/cardinality/label_values` |
    | `/api/prom/user_stats`                                  | `/api/v1/user_stats`                                       |

  - Distributor endpoints

    | Legacy endpoint              | Current                       |
    | ---------------------------- | ----------------------------- |
    | `/<legacy-http-prefix>/push` | `/api/v1/push`                |
    | `/all_user_stats`            | `/distributor/all_user_stats` |
    | `/ha-tracker`                | `/distributor/ha_tracker`     |

  - Ingester endpoints

    | Legacy      | Current              |
    | ----------- | -------------------- |
    | `/ring`     | `/ingester/ring`     |
    | `/shutdown` | `/ingester/shutdown` |
    | `/flush`    | `/ingester/flush`    |
    | `/push`     | `/ingester/push`     |

  - Ruler endpoints

    | Legacy                                                | Current                                                            |
    | ----------------------------------------------------- | ------------------------------------------------------------------ |
    | `/<legacy-http-prefix>/api/v1/rules`                  | `<prometheus-http-prefix>/api/v1/rules`                            |
    | `/<legacy-http-prefix>/api/v1/alerts`                 | `<prometheus-http-prefix>/api/v1/alerts`                           |
    | `/<legacy-http-prefix>/rules`                         | `<prometheus-http-prefix>/config/v1/rules`                         |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `<prometheus-http-prefix>/config/v1/rules/{namespace}`             |
    | `/ruler_ring`                                         | `/ruler/ring`                                                      |

  - Alertmanager endpoints

    | Legacy                  | Current                            |
    | ----------------------- | ---------------------------------- |
    | `/<legacy-http-prefix>` | `/alertmanager`                    |
    | `/status`               | `/multitenant_alertmanager/status` |

## Generate the configuration for Grafana Mimir

The [`mimirtool config convert`] command converts Cortex configuration to Mimir configuration. You can use it to update both flags and configuration files.

### Install mimirtool

To install Mimirtool, download the appropriate binary from the [latest release](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.

Alternatively, use a command line tool such as `curl` to download `mimirtool`. For example, for Linux with the AMD64 architecture, use the following command:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

### Use mimirtool

The `mimirtool config convert` command converts Cortex 1.11 configuration files to Grafana Mimir configuration files.
It removes any configuration parameters that are no longer available in Grafana Mimir, and it renames configuration parameters that have a new name.
If you have explicitly set configuration parameters to a value matching the Cortex default, by default, `mimirtool config convert` doesn't update the value.
To have `mimirtool config convert` update explicitly set values from the Cortex defaults to the new Grafana Mimir defaults, provide the `--update-defaults` flag.
Refer to [convert] for more information on using `mimirtool` for configuration conversion.

## Migrate to Grafana Mimir using Helm

You can migrate to the Grafana Mimir Helm chart (`grafana/mimir-distributed` v3.1.0) from the Cortex Helm chart
(`cortex-helm/cortex` v1.7.0).

### Before you begin

- Ensure that you are running the v1.7.0 release of the Cortex Helm chart.
- Ensure that you are running ingesters using a Kubernetes StatefulSet.
- Install `yq` [v4](https://github.com/mikefarah/yq).

  In the `values.yaml` file:

  ```
  ingester:
    statefulSet:
      enabled: true
  ```

  The ingester needs storage capacity for write-ahead-logging (WAL) and to create blocks for uploading.
  The WAL was optional in Cortex with chunks, but not optional in Mimir.
  A StatefulSet is the most convenient way to make sure that each Pod gets a dedicated storage volume.

**To migrate to the Grafana Mimir Helm chart:**

1. Install the updated monitoring mixin.

   a. Add the dashboards to Grafana. The dashboards replace your Cortex dashboards and continue to work for monitoring Cortex deployments.

   > **Note:** Resource dashboards are now enabled by default and require additional metrics sources.
   > To understand the required metrics sources, refer to [Additional resources metrics].

   b. Install the recording and alerting rules into the ruler or a Prometheus server.

1. Run the following command to add the Grafana Helm chart repository:

   ```bash
   helm repo add grafana https://grafana.github.io/helm-charts
   ```

1. Convert the Cortex configuration in your `values.yaml` file.

   a. Extract the Cortex configuration and write the output to the `cortex.yaml` file.

   ```bash
   yq '.config' <VALUES YAML FILE> > cortex.yaml
   ```

   b. Use `mimirtool` to update the configuration.

   ```bash
   mimirtool config convert --yaml-file cortex.yaml --yaml-out mimir.yaml
   ```

   c. Clean up the generated YAML configuration.

   You have to remove some fields that are generated by `mimirtool config convert` or are coming from old configuration,
   because the mimir-distributed Helm chart has already set the default value for them. Use the following script to
   clean up those fields:

   ```bash
   yq -i 'del(.activity_tracker.filepath,.alertmanager.data_dir,.compactor.data_dir,.frontend_worker.frontend_address,.ingester.ring.tokens_file_path,.ruler.alertmanager_url,.ruler.rule_path,.runtime_config.file)' mimir.yaml
   ```

   d. At the top level of your custom Helm values file, put the updated configuration under the `mimir.structuredConfig` key.

   > **Note:** The `mimir.structuredConfig` field, which is added in v3.0.0, allows you to override a specific
   > configuration without needing to rewrite the whole block string literal, such as in `mimir.config`.

   In your Helm values file:

   ```yaml
   mimir:
     structuredConfig: <CONFIGURATION FILE CONTENTS>
   ```

   Example:

   ```yaml
   mimir:
     structuredConfig:
       ingester:
         ring:
           num_tokens: 512
   ```

   e. Set the ingester `podManagementPolicy` to `"OrderedReady"`.
   The Grafana Mimir chart prefers `"Parallel"` for faster scale up, but this field is immutable on an existing StatefulSet.

   In your `values.yaml` file:

   ```yaml
   ingester:
     podManagementPolicy: "OrderedReady"
   ```

   f. Set the `nameOverride` parameter to `cortex`.
   This configuration parameter ensures that resources have the same names as those created by the Cortex Helm chart and ensures Kubernetes performs a rolling upgrade of existing resources instead of creating new resources.

   In your `values.yaml` file:

   ```yaml
   nameOverride: "cortex"
   ```

   g. Disable MinIO.
   The Grafana Mimir Helm chart enables MinIO by default for convenience during first time install.
   If you are migrating from Cortex and have your existing object storage you must disable MinIO in Grafana Mimir Helm
   chart custom values.yaml.

   In your `values.yaml` file:

   ```yaml
   minio:
     enabled: false
   ```

1. Run the Helm upgrade with the Grafana Mimir chart.

   > **Note:** The name of the release must match your Cortex Helm chart release.

   ```bash
   helm upgrade <RELEASE> grafana/mimir-distributed [-n <NAMESPACE>]
   ```

To verify that the cluster is operating correctly, use the [monitoring mixin dashboards].
