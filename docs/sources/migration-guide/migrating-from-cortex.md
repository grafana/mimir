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

Grafana Mimir 2.0.0 includes significant changes to simplify the deployment and continued operation of a horizontally scalable, multi-tenant time series database with long-term storage.
All configuration parameters have been reviewed with this goal in mind.
Parameters that do not require tuning have been removed, some parameters have been renamed so that they are more easily understood, and a number of parameters have updated default values so that Grafana Mimir is easier to run out of the box.

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

> **Note:** If you are using the [Cortex Helm chart](https://github.com/cortexproject/cortex-helm-chart), you can extract the configuration file from the `values.yaml` file with the [`yq`](https://github.com/kislyuk/yq) tool.
>
> ```bash
> yq -Y '.config' <VALUES YAML FILE>
> ```

```bash
mimirtool config convert --yaml-file <CORTEX YAML FILE>
```

The tool writes the converted configuration file to the terminal.
The tool removes any configuration parameters that are no longer available in Grafana Mimir and renames configuration parameters that have a new name.

Grafana Mimir has updated some default values.
Unless you provide the `--update-defaults` flag, the tool doesn't update default values that you have explicitly set in your configuration file.
To include all defaults values in the output YAML, use the flag `--include-defaults`.

To understand all the configuration changes, use the `--verbose` flag.
The tool outputs a line to `stderr` for each configuration parameter change.
To only output the changes, use shell input redirection.
The following command redirects `stderr` to `stdout` and `stdout` to `/dev/null`:

```bash
mimirtool config convert --yaml-file <CORTEX YAML FILE> --verbose 2>&1 1>/dev/null
```

The output includes the following lines:

- `field is no longer supported: <CONFIGURATION PARAMETER>`:
  Grafana Mimir removed a configuration parameter and the tool removed that parameter from the output configuration.
- `using a new default for <CONFIGURATION PARAMETER>: <NEW VALUE> (used to be <OLD VALUE>)`:
  Grafana Mimir updated the default value for a configuration parameter not explicitly set in your input configuration file.
- `default value for <CONFIGURATION PARAMETER> changed: <NEW VALUE> (used to be <OLD VALUE>); not updating`:
  Grafana Mimir updated the default value for a configuration parameter set in your configuration file.
  By default, the tool doesn't update the value the output configuration.
  If you want the tool to update this parameter to the new default value, use the `--update-defaults` flag.

## Updating to Grafana Mimir using Jsonnet

Grafana Mimir has a Jsonnet library that replaces the existing Cortex Jsonnet library and updated monitoring mixin.

To install the updated libraries using `jsonnet-bundler`, run the following commands:

```bash
jb install github.com/grafana/mimir/operations/mimir@main
jb install github.com/grafana/mimir/operations/mimir-mixin@main
```

**To deploy the updated Jsonnet:**

1. Install the updated monitoring mixin

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
   To extract the arguments from a specific component, use the following bash script:

   ```bash
    #!/usr/bin/env bash

    set -euf -o pipefail

    function usage {
      cat <<EOF
    Extract the CLI flags from individual components.

    Usage:
      $0 <resources JSON> <component>

    Examples:
      $0 resources.json ingester
      $0 <(tk eval environments/default) distributor
      $0 <(jsonnet environments/default/main.jsonnet) query-frontend
    EOF
    }

    if ! command -v jq &>/dev/null; then
      echo "jq command not found in PATH"
      echo "To download jq, refer to https://stedolan.github.io/jq/download/."
    fi

    if [[ $# -ne 2 ]]; then
      usage
      exit 1
    fi

    jq -rf /dev/stdin -- "$1" <<EOF
    ..
    | if type == "object" and .metadata.name == "$2" then .spec.template.spec.containers[]?.args[] else null end
    | select(. != null)
    EOF
   ```

   The first argument to the script is the file containing JSON from evaluating the Jsonnet.
   The second argument is the name of the specific container.
   To retrieve the arguments from the distributor for a Tanka environment:

   ```bash
   <PATH TO SCRIPT> <(tk eval environments/default) distributor
   ```

   The script outputs something like the following:

   ```console
   -consul.hostname=consul.cortex-to-mimir.svc.cluster.local:8500
   -distributor.extend-writes=true
   -distributor.ha-tracker.enable=false
   -distributor.ha-tracker.enable-for-all-users=true
   -distributor.ha-tracker.etcd.endpoints=etcd-client.cortex-to-mimir.svc.cluster.local.:2379
   -distributor.ha-tracker.prefix=prom_ha/
   -distributor.ha-tracker.store=etcd
   -distributor.health-check-ingesters=true
   -distributor.ingestion-burst-size=200000
   -distributor.ingestion-rate-limit=10000
   -distributor.ingestion-rate-limit-strategy=global
   -distributor.remote-timeout=20s
   -distributor.replication-factor=3
   -distributor.ring.consul.hostname=consul.cortex-to-mimir.svc.cluster.local:8500
   -distributor.ring.prefix=
   -distributor.shard-by-all-labels=true
   -mem-ballast-size-bytes=1073741824
   -ring.heartbeat-timeout=10m
   -ring.prefix=
   -runtime-config.file=/etc/cortex/overrides.yaml
   -server.grpc.keepalive.max-connection-age=2m
   -server.grpc.keepalive.max-connection-age-grace=5m
   -server.grpc.keepalive.max-connection-idle=1m
   -server.grpc.keepalive.min-time-between-pings=10s
   -server.grpc.keepalive.ping-without-stream-allowed=true
   -target=distributor
   -validation.reject-old-samples=true
   -validation.reject-old-samples.max-age=12h
   ```

   The output of the script is the format for `mimirtool` conversion via the `--flags-file` flag.

   ```bash
   mimirtool config convert --flags-file=<FLAGS FILE> --yaml-out=/dev/null
   ```

   You can transform the converted flags back into JSON with the following script:

   ```bash
   #!/usr/bin/env bash

   set -euf -o pipefail

   function usage {
    cat <<EOF
   Transform Go flags into JSON key value pairs

   Usage:
    $0 <flags file>

   Examples:
    $0 flags.flags
   EOF
   }

   if [[ $# -ne 1 ]]; then
    usage
    exit 1
   fi

   key_values=$(sed -E -e 's/^-*(.*)=(.*)$/  "\1": "\2",/' "$1")
   printf "{\n%s\n}" "${key_values::-1}"
   ```

   The only argument to the script is a file containing the newline separated flags.
