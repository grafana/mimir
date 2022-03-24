---
title: "Migrating from Cortex to Grafana Mimir"
description: "Learn how to migrate your deployment of Cortex to Grafana Mimir to simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage."
weight: 10
---

# Migrating from Cortex to Grafana Mimir

This document guides an operator through the process of migrating a deployment of [Cortex](https://cortexmetrics.io/) to Grafana Mimir.
It includes an overview of the steps required for any environment and specific instructions for [environments deployed with Jsonnet](#updating-to-grafana-mimir-using-jsonnet).

Grafana Mimir 2.0.0 includes significant changes that simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage.

The changes focus on making Grafana Mimir easier to run out of the box, including:

- Removing configuration parameters that don't require tuning
- Renaming some parameters so that they're more easily understood
- Updating the default values of some existing parameters

The `mimirtool` automates configuration conversion.
It provides a simple migration by generating Mimir configuration from Cortex configuration.

## Before you begin

- Ensure that you are running either Cortex 1.10.X or Cortex 1.11.X.

  If you are running an older version of Cortex, upgrade to [1.11.1](https://github.com/cortexproject/cortex/releases) before proceeding with the migration.

- Ensure you have installed Cortex alerting and recording rules as well as Cortex dashboards.

  The monitoring mixin has both alerting and recording rules to install in either Prometheus or Cortex as well as dashboards to install in Grafana.
  To download a prebuilt ZIP file that contains the alerting and recording rules, refer to [Release Cortex-jsonnet 1.11.1](https://github.com/grafana/cortex-jsonnet/releases/download/1.11.1/cortex-mixin.zip).

  To upload rules to the ruler using mimirtool, refer to [mimirtool rules]({{< relref "../operators-guide/tools/mimirtool.md" >}}).
  To import the dashboards into Grafana, refer to [Import dashboard](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard).

## Notable changes

> **Note:** For full list of changes, refer to the project [CHANGELOG](https://github.com/grafana/mimir/blob/main/CHANGELOG.md).

- The Grafana Mimir HTTP server defaults to listening on port 8080; Cortex defaults to listening on port 80.
  To maintain port 80 as the listening port, set `-server.http-listen-port=80`.
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

    | Legacy                                                | Current                                             |
    | ----------------------------------------------------- | --------------------------------------------------- |
    | `/<legacy-http-prefix>/api/v1/rules`                  | `<prometheus-http-prefix>/api/v1/rules`             |
    | `/<legacy-http-prefix>/api/v1/alerts`                 | `<prometheus-http-prefix>/api/v1/alerts`            |
    | `/<legacy-http-prefix>/rules`                         | `/api/v1/rules` (see below)                         |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `/api/v1/rules/{namespace}/{groupName}` (see below) |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |
    | `/<legacy-http-prefix>/rules/{namespace}/{groupName}` | `/api/v1/rules/{namespace}/{groupName}` (see below) |
    | `/<legacy-http-prefix>/rules/{namespace}`             | `/api/v1/rules/{namespace}` (see below)             |
    | `/ruler_ring`                                         | `/ruler/ring`                                       |

    > **Note:** The `/api/v1/rules/**` endpoints are considered deprecated with Mimir 2.0.0 and will be removed
    > in Mimir 2.2.0. After upgrading to 2.0.0 we recommend switching uses to the equivalent
    > `/<prometheus-http-prefix>/config/v1/**` endpoints that Mimir 2.0.0 introduces.

  - Alertmanager endpoints

    | Legacy                  | Current                            |
    | ----------------------- | ---------------------------------- |
    | `/<legacy-http-prefix>` | `/alertmanager`                    |
    | `/status`               | `/multitenant_alertmanager/status` |

## Generating configuration for Grafana Mimir

[`mimirtool`]({{< relref "../operators-guide/tools/mimirtool.md" >}}) provides a command for converting Cortex configuration to Mimir configuration that you can use to update both flags and configuration files.

### Download mimirtool

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.
For Linux with the AMD64 architecture:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

### Use mimirtool

The `mimirtool config convert` command converts Cortex 1.11 configuration files to Grafana Mimir 2.0 configuration files.
It removes any configuration parameters that are no longer available in Grafana Mimir, and it renames configuration parameters that have a new name.
If you have explicitly set configuration parameters to a value matching the Cortex default, by default, `mimirtool config convert` doesn't update the value.
To have `mimirtool config convert` update explicitly set values from the Cortex defaults to the new Grafana Mimir defaults, provide the `--update-defaults` flag.
Refer to [convert]({{< relref "../operators-guide/tools/mimirtool.md#convert" >}}) for more information on using `mimirtool` for configuration conversion.

## Updating to Grafana Mimir using Jsonnet

Grafana Mimir has a Jsonnet library that replaces the existing Cortex Jsonnet library and updated monitoring mixin.

### Migrate to Grafana Mimir video

The following video shows you how to migrate to Grafana Mimir using Jsonnet.

{{< vimeo 691929138 >}}

<br/>

### Migrate to Grafana Mimir instructions

The following instructions describe how to migrate to Grafana Mimir using Jsonnet.

To install the updated libraries using `jsonnet-bundler`, run the following commands:

```bash
jb install github.com/grafana/mimir/operations/mimir@main
jb install github.com/grafana/mimir/operations/mimir-mixin@main
```

**To deploy the updated Jsonnet:**

1. Install the updated monitoring mixin.

   a. Add the dashboards to Grafana. The dashboards replace your Cortex dashboards and continue to work for monitoring Cortex deployments.

   > **Note:** Resource dashboards are now enabled by default and require additional metrics sources.
   > To understand the required metrics sources, refer to [Additional resources metrics]({{< relref "../operators-guide/visualizing-metrics/requirements.md#additional-resources-metrics" >}}).

   b. Install the recording and alerting rules into the ruler or a Prometheus server.

1. Replace the import of the Cortex Jsonnet library with the Mimir Jsonnet library.
   For example:
   ```jsonnet
   import 'github.com/grafana/mimir/operations/mimir/mimir.libsonnet'
   ```
1. Remove the `cortex_` prefix from any member keys of the `<MIMIR>._config` object.
   For example, `cortex_compactor_disk_data_size` becomes `compactor_disk_data_size`.
1. If you are using the Cortex defaults, set the server HTTP port to 80.
   The new Mimir default is 8080.
   For example:
   ```jsonnet
   (import 'github.com/grafana/mimir/operations/mimir/mimir.libsonnet') {
     _config+: {
       server_http.port: 80,
     },
   }
   ```
1. For each component, use `mimirtool` to update the configured arguments.
   To extract the flags for each component, refer to [Extracting flags from Jsonnet]({{< relref "../operators-guide/tools/mimirtool.md#extracting-flags-from-jsonnet" >}}).
1. Apply the updated Jsonnet

To verify that the cluster is operating correctly, use the [monitoring mixin dashboards]({{< relref "../operators-guide/visualizing-metrics/dashboards/_index.md" >}}).
