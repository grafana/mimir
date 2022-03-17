---
title: "Migrating from Cortex to Grafana Mimir"
description: ""
weight: 10
---

# Migrating from Cortex to Grafana Mimir

This document guides an operator through the process of migrating a deployment of [Cortex](https://cortexmetrics.io/) to Grafana Mimir.

## Prerequisites

- Ensure that you are running [Cortex 1.11.0](https://github.com/cortexproject/cortex/releases).

  If you are running an older version of Cortex, upgrade to 1.11.0 before proceeding with the migration.

- Ensure you have the Cortex alerting and recordings rules, and dashboards installed.

  The monitoring mixin has alerting and recording rules that you install in either Prometheus or Cortex and dashboards that you install in Grafana.
  To download a prebuilt ZIP file that contains the alerting and recording rules, refer to [Release Cortex-jsonnet 1.11.0](https://github.com/grafana/cortex-jsonnet/releases/download/1.11.0/cortex-mixin.zip).

  To upload rules to the ruler using mimirtool, refer to [mimirtool rules]({{< relref "../operators-guide/tools/mimirtool.md" >}}).
  To import the dashboards into Grafana, refer to [Import dashboard](https://grafana.com/docs/grafana/latest/dashboards/export-import/#import-dashboard).

- Ensure you have read the [Release notes]({{< relref "../release-notes/v2.0.md" >}}) for any breaking configuration changes.

  > **Note:** Mimirtool can automate configuration conversion, documented below.

## Updating Cortex configuration for Grafana Mimir

Grafana Mimir 2.0.0 includes significant changes that simplifies the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage.
Parameters that don't require tuning have been removed, some parameters have been renamed so that they are more easily understood, and a number of parameters have updated default values so that Grafana Mimir is easier to run out of the box.

[`mimirtool`]({{< relref "../operators-guide/tools/mimirtool.md" >}}) has a command for converting Cortex configuration into Mimir configuration.
You can use to update both flags and configuration files.

### Downloading mimirtool

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.
For Linux with the AMD64 architecture:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

### Converting a Cortex configuration file with mimirtool

The Grafana Mimir team has written a tool for converting Cortex 1.11 configuration files to Grafana Mimir 2.0 configuration files.
The tool removes any configuration parameters that are no longer available in Grafana Mimir and renames configuration parameters that have a new name.
Unless you provide the `--update-defaults` flag, the tool doesn't update default values that you have explicitly set in your configuration file.
For more information on how to use Mimirtool for configuration conversion, refer to [convert]({{< relref "../operators-guide/tools/mimirtool.md#convert" >}}).

To install mimirtool, download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.
For Linux with the AMD64 architecture:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

## Updating to Grafana Mimir using Jsonnet

Grafana Mimir has a Jsonnet library that replaces the existing Cortex Jsonnet library and updated monitoring mixin.

To install the updated libraries using `jsonnet-bundler`, run the following commands:

```bash
jb install github.com/grafana/mimir/operations/mimir@main
jb install github.com/grafana/mimir/operations/mimir-mixin@main
```

**To deploy the updated Jsonnet:**

1. Install the updated monitoring mixin.

   a. Add the dashboards to Grafana. The dashboards replace your Cortex dashboards and continue to work for monitoring Cortex deployments.

   > **Note:** Resource dashboards are now enabled by default and require additional metrics sources.
   > To understand the required metrics sources, refer to [Additional resource metrics]({{< relref "../operators-guide/visualizing-metrics/requirements.md#additional-resource-metrics" >}}).

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

To verify the cluster is operating correctly, use the [monitoring mixin dashboards]({{< relref "../operators-guide/visualizing-metrics/dashboards/_index.md" >}}).
