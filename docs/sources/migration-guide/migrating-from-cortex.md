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

- Ensure you have the Cortex monitoring mixin installed.

  The monitoring mixin has alerting and recording rules to be installed in either Prometheus or Cortex and dashboards to be installed in Grafana.
  To download a prebuilt ZIP file of these rules and dashboards, refer to [Release Cortex-jsonnet 1.11.0](https://github.com/grafana/cortex-jsonnet/releases/download/1.11.0/cortex-mixin.zip).

## Updating Cortex configuration for Grafana Mimir

Grafana Mimir 2.0.0 includes significant changes to simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage.
All configuration parameters have been reviewed with this goal in mind.
Parameters that do not require tuning have been removed, some parameters have been renamed so that they are more easily understood, and a number of parameters have updated default values so that Grafana Mimir is easier to run out of the box.

[`mimirtool`]({{< relref "../tools/mimirtool/_index.md" >}}) has a command for converting Cortex configuration into Mimir configuration.
The tool can be used to update both flags and configuration files.

### Downloading mimirtool

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.
For Linux with the AMD64 architecture:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

### Converting a Cortex configuration file with mimirtool

> **Note:** If you are using the [Cortex Helm chart](https://github.com/cortexproject/cortex-helm-chart), you can extract the configuration file from the `values.yaml` file with the [`yq`](https://github.com/kislyuk/yq) tool.
>
> ```bash
> yq -Y '.config' <VALUES YAML FILE>
> ```

```bash
mimirtool config convert --yaml-file <CORTEX YAML FILE>
```

The tool writes the converted configuration file to the terminal.
The tool removes any configuration parameters that are no longer present in Mimir and rename any configuration parameters that have a new name.

Grafana Mimir has updated some default values.
To include them in the output YAML, use the flag `--include-defaults`.
Unless you provide the `--update-defaults` flag, the tool doesn't update default values that you have explicitly set in your configuration file.

To understand all the configuration changes, use the `--verbose` flag.
The tool outputs a line to `stderr` for each configuration parameter change.
To only output the changes, use shell input redirection.
The following command redirects `stderr` to `stdout` and `stdout` to `/dev/null`:

```bash
mimirtool config convert --yaml-file <CORTEX YAML FILE> --verbose 2>&1 1>/dev/null
```

Each line of the output is one of the following:

- **field is no longer supported: <CONFIGURATION PARAMETER>**:
  Grafana Mimir removed a configuration parameter and the tool removed that parameter from the output configuration.
- **using a new default for <CONFIGURATION PARAMETER>: <NEW VALUE> (used to be <OLD VALUE>)**:
  Grafana Mimir updated the default value for a configuration parameter not explicitly set in your input configuration file.
- **default value for <CONFIGURATION PARAMETER> changed: <NEW VALUE> (used to be <OLD VALUE>); not updating**:
  Grafana Mimir updated the default value for a configuration parameter set in your configuration file.
  By default, the tool doesn't update it in the output configuration.
  To have the tool update this parameter to the new default, use the `--update-defaults` flag.

## Updating to Grafana Mimir using Jsonnet

Grafana Mimir has a Jsonnet library that replaces the existing [Cortex Jsonnet](https://github.com/grafana/cortex-jsonnet) library.
There is also an updated monitoring mixin.

To install the updated libraries using `jsonnet-bundler`:

```bash
jb install github.com/grafana/mimir/operations/mimir@main
jb install github.com/grafana/mimir/operations/mimir-mixin@main
```

To deploy the updated Jsonnet, use the following steps:

1. Install the updated monitoring mixin
   1. Add the dashboards to Grafana. The dashboards replace your Cortex dashboards and continue to work for monitoring Cortex deployments.
   > **Note:** Resource dashboards are now enabled by default an require additional metrics sources.
   > Refer to 
