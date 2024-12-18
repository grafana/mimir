---
aliases:
  - ../../operators-guide/tools/mimirtool/
description:
  Use Mimirtool to perform common tasks in Grafana Mimir or Grafana Cloud
  Metrics.
menuTitle: Mimirtool
title: Grafana Mimirtool
weight: 40
---

# Grafana Mimirtool

Mimirtool is a command-line tool that operators and tenants can use to execute a number of common tasks that involve Grafana Mimir or Grafana Cloud Metrics.

- The `alertmanager` command enables you to create, update, and delete tenant configurations in Grafana Mimir Alertmanager or Grafana Cloud Metrics.

  For more information about the `alertmanager` command, refer to [Alertmanager]({{< relref "#alertmanager" >}}).

- The `rules` command enables you to validate and lint Prometheus rule files and convert them for use in Grafana Mimir.
  You can also create, update, and delete rulegroups in Grafana Mimir or Grafana Cloud Metrics.

  For more information about the `rules` command, refer to [Rules]({{< relref "#rules" >}}).

- The `remote-read` subcommand enables you to fetch statistics and series from [remote-read](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations) APIs.
  You can write series from a remote-read API to a local TSDB file that you load into Prometheus.

  For more information about the remote-read command, refer to [Remote-read]({{< relref "#remote-read" >}}).

- The `analyze` command extracts statistics about metric usage from Grafana or Hosted Grafana instances.
  You can also extract the same metrics from Grafana dashboard JSON files or Prometheus rule YAML files.

  For more information about the `analyze` command, refer to [Analyze]({{< relref "#analyze" >}}).

- The `bucket-validation` command verifies that an object storage bucket is suitable as a backend storage for Grafana Mimir.

  For more information about the `bucket-validation` command, refer to [Bucket validation]({{< relref "#bucket-validation" >}}).

- The `acl` command generates the label-based access control header used in Grafana Enterprise Metrics and Grafana Cloud Metrics.

  For more information about the `acl` command, refer to [ACL]({{< relref "#acl" >}}).

- The `config` command helps convert configuration files from Cortex to Grafana Mimir.

  For more information about the `config` command, refer to [Config]({{< relref "#config" >}})

- The `backfill` command uploads existing Prometheus TSDB blocks into Grafana Mimir.

  For more information about the `backfill` command, refer to [Backfill]({{< relref "#backfill" >}})

Mimirtool interacts with:

- User-facing APIs provided by Grafana Mimir.
- Backend storage components containing Grafana Mimir data.

## Installation

To install Mimirtool with Homebrew on macOS, run `brew install mimirtool`.

To install Mimirtool in other environments, download the appropriate binary from the [latest release](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.

Alternatively, use a command line tool such as `curl` to download `mimirtool`. For example, for Linux with the AMD64 architecture, use the following command:

```bash
curl -fLo mimirtool https://github.com/grafana/mimir/releases/latest/download/mimirtool-linux-amd64
chmod +x mimirtool
```

## Configuration options

For Mimirtools to interact with Grafana Mimir, Grafana Enterprise Metrics, Prometheus, or Grafana, set the following environment variables or CLI flags.

| Environment variable | Flag        | Description                                                                                                                                                                                      |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `MIMIR_ADDRESS`      | `--address` | Sets the address of the API of the Grafana Mimir cluster.                                                                                                                                        |
| `MIMIR_API_USER`     | `--user`    | Sets the basic auth username. If this variable is empty and `MIMIR_API_KEY` is set, the system uses `MIMIR_TENANT_ID` instead. If you're using Grafana Cloud, this variable is your instance ID. |
| `MIMIR_API_KEY`      | `--key`     | Sets the basic auth password. If you're using Grafana Cloud, this variable is your API key.                                                                                                      |
| `MIMIR_TENANT_ID`    | `--id`      | Sets the tenant ID of the Grafana Mimir instance that Mimirtools interacts with.                                                                                                                 |

It is also possible to set TLS-related options with the following environment variables or CLI flags:

| Environment variable             | Flag                         | Description                                                                                                            |
| -------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `MIMIR_TLS_CA_PATH`              | `--tls-ca-path`              | Sets the path to the CA certificate to use to verify the connection to the Grafana Mimir cluster.                      |
| `MIMIR_TLS_CERT_PATH`            | `--tls-cert-path`            | Sets the path to the client certificate to use to authenticate to the Grafana Mimir cluster.                           |
| `MIMIR_TLS_KEY_PATH`             | `--tls-key-path`             | Sets the path to the private key to use to authenticate to the Grafana Mimir cluster.                                  |
| `MIMIR_TLS_INSECURE_SKIP_VERIFY` | `--tls-insecure-skip-verify` | If `true`, disables verification of the Grafana Mimir cluster's TLS certificate. This is insecure and not recommended. |

## Commands

The following sections outline the commands that you can run against Grafana Mimir and Grafana Cloud Metrics.

### Alertmanager

The following commands interact with Grafana Mimir Alertmanager configuration and alert template files.

{{< admonition type="note" >}}
The Mimir Alertmanager exposes the configuration API based on the path that you set via the `-server.path-prefix` flag; it doesn't use the path of the `-http.alertmanager-http-prefix` flag.

If you run Mimir with the default configuration, `-server.path-prefix`, where the default value is `/`, then only set the hostname for the `--address` flag of the `mimirtool` command; do not set a path-specific address.

For example, `/` is correct, and `/alertmanager` is incorrect.
{{< /admonition >}}

#### Get Alertmanager configuration

The following command shows the current Alertmanager configuration.

```bash
mimirtool alertmanager get
```

Alternatively, you can output the config and template files to a folder which can then be loaded back
into alert manager at a later date. For example, the following command outputs the files to a folder called `am`

```bash
mimirtool alertmanager get --output-dir="am"
```

The config file is named `config.yaml` and the template files ends in `.tpl`, where each template is written
out to its own file. Note that using the `--output-dir` flag only writes the output to files and no longer print
the config to the console.

#### Load Alertmanager configuration

The following command loads an Alertmanager configuration to the Alertmanager instance.

```bash
mimirtool alertmanager load <config_file>
mimirtool alertmanager load <config_file> <template_files>...
```

##### Example

```bash
mimirtool alertmanager load ./example_alertmanager_config.yaml ./example_alertmanager_template.tpl
```

`./example_alertmanager_config.yaml`:

```yaml
route:
  receiver: "example_receiver"
  group_by: ["example_groupby"]
receivers:
  - name: "example_receiver"
templates:
  - example_template.tpl
```

`./example_alertmanager_template.tpl`:

```gotemplate
{{ define "alert_customer_env_message" }}
  [{{ .CommonLabels.alertname }} | {{ .CommonLabels.customer }} | {{ .CommonLabels.environment }}]
{{ end }}
```

The input to `[<template_files>...]` accepts wildcard, i.e. `*.tpl` will include all the template files ending
in `.tpl`. If we have used the previous command of exporting the config and templates to a directory, they
can be loaded

```bash
# assuming we have written the files out to the folder am
# mimirtool alertmanager get --output-dir="am"
mimirtool alertmanager load  am/config.yaml am/*.tpl
```

#### Delete Alertmanager configuration

The following command deletes the Alertmanager configuration in the Grafana Mimir Alertmanager.

```bash
mimirtool alertmanager delete
```

#### Migrate Alertmanager configuration for UTF-8 in Mimir 2.12 and later

This requires mimirtool version 2.12 or later. To check your version of mimirtool, run `mimirtool version`.

In accordance with [prometheus/prometheus#13095](https://github.com/prometheus/prometheus/issues/13095)
and [prometheus/alertmanager#3486](https://github.com/prometheus/alertmanager/issues/3486), Mimir is
adding support for UTF-8. To support UTF-8 in alerts, routes, silences, and inhibition rules,
Alertmanager has added a new parser for matchers that has a number of backwards incompatible changes.
More information about these changes can be found [here](https://prometheus.io/docs/alerting/latest/configuration/#label-matchers).

The migrate-utf8 command migrates an existing Alertmanager configuration in preparation for when
UTF-8 is enabled in a Mimir installation. To do this, it translates matchers that are incompatible
with UTF-8 into equivalent matchers that are compatible. This translation is backwards compatible,
does not change the behavior of existing matchers, and works even in Mimir installations that
do not have UTF-8 enabled.

The need to migrate an Alertmanager configuration depends on whether it contains matchers
incompatible with UTF-8. You can verify if an existing Alertmanager configuration needs to be
migrated for UTF-8 by running the verify command:

```bash
mimirtool alertmanager verify <config_file> [template_files...]
```

If the command succeeds without any warnings, the configuration is compatible with UTF-8 and you
don't need to migrate it.

However, if the command prints the following warning, then you need to migrate the Alertmanager
configuration with the migrate-utf8 command.

```
Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible. Alertmanager has instead parsed the input using the classic matchers parser as a fallback. To make this input compatible with the UTF-8 matchers parser please make sure all regular expressions and values are double-quoted. If you are still seeing this message please open an issue.
```

The command takes as input an existing configuration and template files, and prints as output
the migrated configuration and template files:

```bash
mimirtool alertmanager migrate-utf8 <config_file> [template_files...]
```

You can also output the migrated configuration and template files to a folder which can be
reviewed before being loaded back into the Alertmanager at a later time. We recommended you output
the migrated files to a different folder than the original files so you always have the original files
as a backup. For example, the following command outputs the migrated configuration and template files
to a folder called `migrated`:

```bash
mimirtool alertmanager migrate-utf8 <config_file> [template_files...] --output-dir="migrated"
```

Within the output dir, the configuration file is named `config.yaml` and the template files ends in
`.tpl`, where each template is written out to its own file.

{{< admonition type="note" >}}
When you use the `--output-dir` flag, the command only writes the output to files and doesn't print the configuration to the console.
{{< /admonition >}}

After you migrate an Alertmanager configuration, use the verify command to verify it:

```bash
mimirtool alertmanager verify <config_file> [template_files...]
```

It should succeed without warnings.

You can also run the verify command with the `--utf8-strict-mode` flag to verify if an Alertmanager
configuration is compatible with UTF-8:

```bash
mimirtool alertmanager verify <config_file> [template_files...] --utf8-strict-mode
```

When using this flag, the verify command prints a warning with the text `UTF-8 mode enabled` to let
you know the command is using UTF-8 strict mode to validate the Alertmanager configuration. Unlike
when the flag is omitted, it doesn't print warnings if a configuration is incompatible with UTF-8.
Instead, the command exits with an error. For example:

```bash
level=warn msg="UTF-8 strict mode enabled"
mimirtool: error: end of input: expected label value, try --help
```

If the command exits without error, and you're satisfied with the changes made to the migrated
configuration and template files, reload it using the `load` command.

#### Validate Alertmanager configuration

The following command validates an Alertmanager configuration file. It does not load the configuration to the Alertmanager instance.

```bash
mimirtool alertmanager verify <config_file> [template_files...]
```

#### Alert verification

The following command verifies if alerts in an Alertmanager cluster are deduplicated. This command is useful for verifying the correct configuration when transferring from Prometheus to Grafana Mimir alert evaluation.

```bash
mimirtool alerts verify
```

#### Render template

You can render your Alertmanager template with the `render` command.

The argument to the `--template.glob` option is a glob that expands to the alert template paths.

The argument to the `--template.data` option is a JSON file containing Alertmanager template data.

The argument to the `--template.text` option is a Go template.
The command renders this template with the templates found be expanding `--template-glob`.

The following command render a template and prints it to the terminal.
It assumes you have written the templates to the directory `templates`.

```bash
mimirtool alertmanager render --template.glob 'templates/*' --template.data alert_data1.json --template.text '{{ template "my_message" . }}'
```

### Rules

The rules command features sub-commands for working with Prometheus rule files and with the APIs in the Grafana Mimir ruler.

The commands in this section enable you to perform the following actions:

- Load and show Prometheus rule files
- Interact with individual rule groups in the Mimir ruler
- Manipulate local rule files

Some of the functionality that `mimirtool rules` offers is also available as a GitHub Action.
For more information, refer to the [documentation of Mimirtool Github Action](https://github.com/grafana/mimir/blob/main/operations/mimir-rules-action/README.md).

Configuration options relevant to rules commands:

| Flag              | Description                                                                               |
| ----------------- | ----------------------------------------------------------------------------------------- |
| `--auth-token`    | Authentication token for bearer token or JWT auth.                                        |
| `--extra-headers` | Extra headers to add to the requests in header=value format. (Can specify multiple times) |

#### List rules

The following command retrieves the names of all rule groups in the Grafana Mimir instance and prints them to the terminal.

```bash
mimirtool rules list
```

#### Print rules

The following command retrieves all rule groups in the Grafana Mimir instance and prints them to the terminal.

```bash
mimirtool rules print
```

To save all the rules for editing and re-upload to Mimir, use the `--output-dir` option.
The default output directory is the current directory.
The output file has the format required by `mimirtool rules load` or `mimirtool rules sync`.

For example, to save the file in the `rules` subdirectory:

```bash
mimirtool rules print --output-dir=rules
```

#### Get rule group

The following command retrieves a single rule group and prints it to the terminal.

```bash
mimirtool rules get <namespace> <rule_group_name>
```

To save the rule group for editing and re-upload to Mimir, use the `--output-dir` option.
The default output directory is the current directory.
The output file has the format required by `mimirtool rules load` or `mimirtool rules sync`.

For example, to save the file in the `rules` subdirectory:

```bash
mimirtool rules get <namespace> <rule_group_name> --output-dir=rules
```

#### Delete rule group

The following command deletes a rule group.

```bash
mimirtool rules delete <namespace> <rule_group_name>
```

#### Load rule group

The following command loads each rule group from the files into Grafana Mimir.
This command overwrites all existing rule groups with the same name.

```bash
mimirtool rules load <file_path>...
```

##### Example

```bash
mimirtool rules load ./example_rules_one.yaml
```

`./example_rules_one.yaml`:

```yaml
namespace: my_namespace
groups:
  - name: example
    interval: 5m
    rules:
      - record: job:http_inprogress_requests:sum
        expr: sum by (job) (http_inprogress_requests)
```

This command, like the other `rules` subcommands, can load multiple rule groups at once:

```bash
mimirtool rules load ./example_rules_one.yaml ./example_rules_two.yaml
```

#### Delete a namespace

The following command deletes all of the rule groups in a namespace, including the namespace itself:

```bash
mimirtool rules delete-namespace <namespace>
```

#### Lint

The `lint` command provides YAML and PromQL expression formatting within the rule file.

This command edits the rule file in place.
To perform a trial run that does not make changes, you can use the dry run flag (`-n`).

{{< admonition type="note" >}}
This command doesn't verify if a query is correct and doesn't interact with your Grafana Mimir cluster.
{{< /admonition >}}

```bash
mimirtool rules lint <file_path>...
```

The format of the file is the same format as shown in [rules load](#load-rule-group).

#### Prepare

This `prepare` command prepares a rules file that you upload to Grafana Mimir.
It lints all PromQL expressions and adds a label to your PromQL query aggregations in the file.
The format of the file is the same format as shown in [rules load](#load-rule-group).

{{< admonition type="note" >}}
This command doesn't interact with your Grafana Mimir cluster.
{{< /admonition >}}

```bash
mimirtool rules prepare <file_path>...
```

##### Configuration

| Flag                           | Description                                                                                                                                    |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `-i`, `--in-place`             | Edits the file in place. If not set, the system generates a new file with the extension `.result` that contains the results.                   |
| `-l`, `--label="cluster"`      | Specifies the label for aggregations. By default, the label is set to `cluster`.                                                               |
| `--label-excluded-rule-groups` | Comma separated list of rule group names to exclude when including the configured label to aggregations.                                       |
| `--rule-dirs`                  | Comma separated list of paths to directories containing rules yaml files. Each file in a directory with a .yml or .yaml suffix will be parsed. |

##### Example

```bash
mimirtool rules prepare ./example_rules_one.yaml
```

`./example_rules_one.yaml`:

```yaml
namespace: my_namespace
groups:
  - name: example
    interval: 5m
    rules:
      - record: job:http_inprogress_requests:sum
        expr: sum by (job) (http_inprogress_requests)
```

`./example_rules_one.yaml.result`:

```yaml
namespace: my_namespace
groups:
  - name: example
    interval: 5m
    rules:
      - record: job:http_inprogress_requests:sum
        # note the added cluster label
        expr: sum by(job, cluster) (http_inprogress_requests)
```

After the command runs, an output message indicates if the operation was successful:

```console
INFO[0000] SUCCESS: 1 rules found, 0 modified expressions
```

#### Check

The `check` command checks rules against the recommended [best practices](https://prometheus.io/docs/practices/rules/) for
rules.
This command does not interact with your Grafana Mimir cluster.

```bash
mimirtool rules check <file_path>...
```

##### Example

```bash
mimirtool rules check rules.yaml
```

`rules.yaml`

```yaml
namespace: my_namespace
groups:
  - name: example
    interval: 5m
    rules:
      - record: job_http_inprogress_requests_sum
        expr: sum by (job) (http_inprogress_requests)
```

```console
ERRO[0000] bad recording rule name error="recording rule name does not match level:metric:operation format, must contain at least one colon" file=rules.yaml rule=job_http_inprogress_requests_sum ruleGroup=example
```

The format of the file is the same format as shown in [rules load](#load-rule-group).

#### Diff

The following command compares rules against the rules in your Grafana Mimir cluster.

```bash
mimirtool rules diff <file_path>...
```

The format of the file is the same format as shown in [rules load](#load-rule-group).

To restrict the affected namespaces, use the `--namespaces` and `--ignore-namespaces` parameters as well as their RegEx variants `--namespaces-regex` and `--ignore-namespaces-regex`.

##### Configuration

| Flag                         | Description                                                |
| ---------------------------- | ---------------------------------------------------------- |
| `--namespaces`               | comma-separated list of namespaces to check during a diff  |
| `--ignored-namespaces`       | comma-separated list of namespaces to ignore during a diff |
| `--namespaces-regex`         | regex matching namespaces to check during a diff           |
| `--ignored-namespaces-regex` | regex matching namespaces to ignore during a diff          |

Only one of the namespace selection flags can be specified.

#### Sync

The `sync` command compares rules against the rules in your Grafana Mimir cluster.
The command applies any differences to your Grafana Mimir cluster.

```bash
mimirtool rules sync <file_path>...
```

The format of the file is the same format as shown in [rules load](#load-rule-group).

##### Configuration

| Flag                         | Description                                                |
| ---------------------------- | ---------------------------------------------------------- |
| `--namespaces`               | comma-separated list of namespaces to check during a sync  |
| `--ignored-namespaces`       | comma-separated list of namespaces to ignore during a sync |
| `--namespaces-regex`         | regex matching namespaces to check during a sync           |
| `--ignored-namespaces-regex` | regex matching namespaces to ignore during a sync          |

Only one of the namespace selection flags can be specified.

### Remote-read

Grafana Mimir exposes a [remote read API] which allows the system to access the stored series.
The `remote-read` subcommand `mimirtool` enables you to interact with its API, and to determine which series are stored.

[remote read api]: https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations

#### Stats

The `remote-read stats` command summarizes statistics of the stored series that match the selector.

##### Example

```bash
mimirtool remote-read stats --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
```

Running the command results in the following output:

```console
INFO[0000] Create remote read client using endpoint 'http://demo.robustperception.io:9090/api/v1/read'
INFO[0000] Querying time from=2020-12-30T14:00:00Z to=2020-12-30T15:00:00Z with selector={job="node"}
INFO[0000] MIN TIME                           MAX TIME                           DURATION     NUM SAMPLES  NUM SERIES   NUM STALE NAN VALUES  NUM NAN VALUES
INFO[0000] 2020-12-30 14:00:00.629 +0000 UTC  2020-12-30 14:59:59.629 +0000 UTC  59m59s       159480       425          0                     0
```

#### Dump

The `remote-read dump` command prints all series and samples that match the selector.

##### Example

```bash
mimirtool remote-read dump --selector 'up{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
```

Running the command results in the following output:

```console
{__name__="up", instance="demo.robustperception.io:9100", job="node"} 1 1609336914711
{__name__="up", instance="demo.robustperception.io:9100", job="node"} NaN 1609336924709 # StaleNaN
...
```

#### Export

The `remote-read export` command exports all series and samples that match the selector into a local TSDB.
You can use local tooling such as `prometheus` and [`promtool`](https://github.com/prometheus/prometheus/tree/main/cmd/promtool) to further analyze the TSDB.

```bash
# Use Remote Read API to download all metrics with label job=name into local tsdb
mimirtool remote-read export --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read --tsdb-path ./local-tsdb
```

Running the command results in the following output:

```console
INFO[0000] Create remote read client using endpoint 'http://demo.robustperception.io:9090/api/v1/read'
INFO[0000] Created TSDB in path './local-tsdb'
INFO[0000] Using existing TSDB in path './local-tsdb'
INFO[0000] Querying time from=2020-12-30T13:53:59Z to=2020-12-30T14:53:59Z with selector={job="node"}
INFO[0001] Store TSDB blocks in './local-tsdb'
INFO[0001] BLOCK ULID                  MIN TIME                       MAX TIME                       DURATION     NUM SAMPLES  NUM CHUNKS   NUM SERIES   SIZE
INFO[0001] 01ETT28D6B8948J87NZXY8VYD9  2020-12-30 13:53:59 +0000 UTC  2020-12-30 13:59:59 +0000 UTC  6m0.001s     15950        429          425          105KiB867B
INFO[0001] 01ETT28D91Z9SVRYF3DY0KNV41  2020-12-30 14:00:00 +0000 UTC  2020-12-30 14:53:58 +0000 UTC  53m58.001s   143530       1325         425          509KiB679B
```

##### Local TSDB examples

The following command uses promtool to analyze file contents.

```bash
promtool tsdb analyze ./local-tsdb
```

The following command dumps all values of the TSDB.

```bash
promtool tsdb dump ./local-tsdb
```

The following command runs a local Prometheus with the local TSDB.

```bash
prometheus --storage.tsdb.path ./local-tsdb --config.file=<(echo "")
```

### ACL

The `acl` command generates the label-based access control header used in Grafana Enterprise Metrics and Grafana Cloud Metrics.

#### Generate header

The following command enables you to generate a header that you can use to enforce access control rules in Grafana Enterprise Metrics or Grafana Cloud.

{{< admonition type="note" >}}
Grafana Mimir doesn't support ACLs.
{{< /admonition >}}

```bash
mimirtool acl generate-header --id=<tenant_id> --rule=<promql_selector>
```

##### Example

```bash
mimirtool acl generate-header --id=1234 --rule='{namespace="A"}'
```

Example output:

```console
The header to set:
X-Prom-Label-Policy: 1234:%7Bnamespace=%22A%22%7D
```

### Analyze

You can analyze your Grafana or Hosted Grafana instance to determine which metrics are used and exported. You can also extract metrics from dashboard JSON files and rules YAML files.

#### Grafana

The following command runs against your Grafana instance, downloads its dashboards, and extracts the Prometheus
metrics used in its queries.
The output is a JSON file. You can use this file with `analyze prometheus --grafana-metrics-file`.

```bash
mimirtool analyze grafana --address=<url>
```

##### Configuration

| Environment variable | Flag             | Description                                                                                                                                                           |
| -------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GRAFANA_ADDRESS`    | `--address`      | Sets the address of the Grafana instance.                                                                                                                             |
| `GRAFANA_API_KEY`    | `--key`          | Sets the API Key for the Grafana instance. To create a key, refer to [Authentication API](/docs/grafana/latest/http_api/auth/).                                       |
| -                    | `--output`       | Sets the output file path, which by default is `metrics-in-grafana.json`.                                                                                             |
| -                    | `--folder-title` | Sets the folder filter to limit dashboards analysis for unused metrics based on their exact folder title. When repeated any of the matching folders will be analyzed. |

##### Example output file

```json
{
  "metricsUsed": [
    "apiserver_request:availability30d",
    "workqueue_depth",
    "workqueue_queue_duration_seconds_bucket"
  ],
  "dashboards": [
    {
      "slug": "",
      "uid": "09ec8aa1e996d6ffcd6817bbaff4db1b",
      "title": "Kubernetes / API server",
      "metrics": [
        "apiserver_request:availability30d",
        "apiserver_request_total",
        "cluster_quantile:apiserver_request_duration_seconds:histogram_quantile",
        "workqueue_depth",
        "workqueue_queue_duration_seconds_bucket"
      ],
      "parse_errors": ["unsupported panel type: \"news\""]
    }
  ]
}
```

#### Ruler

The following command runs against your Grafana Mimir, Grafana Enterprise Metrics, or Grafana Cloud Prometheus instance. The command fetches the rule groups and extracts the Prometheus metrics used in the rule queries.
The output is a JSON file. You can use this file with `analyze prometheus --ruler-metrics-file`.

```bash
mimirtool analyze ruler --address=<url> --id=<tenant_id>
```

##### Configuration

| Environment variable | Flag           | Description                                                                                               |
| -------------------- | -------------- | --------------------------------------------------------------------------------------------------------- |
| `MIMIR_ADDRESS`      | `--address`    | Sets the address of the Prometheus instance.                                                              |
| `MIMIR_TENANT_ID`    | `--id`         | Sets the basic authentication username. If you're using Grafana Cloud, this variable is your instance ID. |
| `MIMIR_API_KEY`      | `--key`        | Sets the basic authentication password. If you're using Grafana Cloud, this variable is your API key.     |
| `MIMIR_AUTH_TOKEN`   | `--auth-token` | Sets the bearer or JWT token that is required for Mimir clusters authenticating with this method.         |
| -                    | `--output`     | Sets the output file path, which by default is `metrics-in-ruler.json`.                                   |

##### Example output file

```json
{
  "metricsUsed": [
    "apiserver_request_duration_seconds_bucket",
    "container_cpu_usage_seconds_total",
    "scheduler_scheduling_algorithm_duration_seconds_bucket"
  ],
  "ruleGroups": [
    {
      "namspace": "prometheus_rules",
      "name": "kube-apiserver.rules",
      "metrics": [
        "apiserver_request_duration_seconds_bucket",
        "apiserver_request_duration_seconds_count",
        "apiserver_request_total"
      ],
      "parse_errors": null
    }
  ]
}
```

#### Dashboard

The following command accepts Grafana dashboard JSON files as input and extracts Prometheus metrics used in the queries.
The output is a JSON file.
You can use the output file with `analyze prometheus --grafana-metrics-file`.

```bash
mimirtool analyze dashboard <file>...
```

##### Configuration

| Environment variable | Flag       | Description                                                               |
| -------------------- | ---------- | ------------------------------------------------------------------------- |
| -                    | `--output` | Sets the output file path, which by default is `metrics-in-grafana.json`. |

#### Rule-file

The following command accepts Prometheus rule YAML files as input and extracts Prometheus metrics used in the queries.
The output is a JSON file. You can use the output file with `analyze prometheus --ruler-metrics-file`.

```bash
mimirtool analyze rule-file <file>
```

##### Configuration

| Environment variable | Flag       | Description                                                             |
| -------------------- | ---------- | ----------------------------------------------------------------------- |
| -                    | `--output` | Sets the output file path, which by default is `metrics-in-ruler.json`. |

#### Prometheus

The following command runs against your Grafana Mimir, Grafana Metrics Enterprise, Prometheus, or Cloud Prometheus instance.
The command uses the output from a previous run of `analyze grafana`, `analyze dashboard`, `analyze ruler`
or `analyze rule-file` to show the number of series in the Prometheus instance that are used in dashboards or rules, or both.
This command also shows which metrics exist in Grafana Cloud that are _not_ in dashboards or rules. The output is a JSON file.

{{< admonition type="note" >}}
The command makes a request for every active series in the Prometheus instance.
For Prometheus instances with a large number of active series, this command might take time to complete.
{{< /admonition >}}

```bash
mimirtool analyze prometheus --address=<url> --id=<tenant_id>
```

##### Configuration

| Environment variable | Flag                       | Description                                                                                                                     |
| -------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `MIMIR_ADDRESS`      | `--address`                | Sets the address of the Prometheus instance.                                                                                    |
| `MIMIR_TENANT_ID`    | `--id`                     | Sets the basic authentication username. If you're using Grafana Cloud this variable is your instance ID, also set as tenant ID. |
| `MIMIR_API_KEY`      | `--key`                    | Sets the basic authentication password. If you're using Grafana Cloud, this variable is your API key.                           |
| `MIMIR_AUTH_TOKEN`   | `--auth-token`             | Sets the bearer or JWT token that is required for Mimir clusters authenticating with this method.                               |
| -                    | `--grafana-metrics-file`   | `mimirtool analyze grafana` or `mimirtool analyze dashboard` output file, which by default is `metrics-in-grafana.json`.        |
| -                    | `--ruler-metrics-file`     | `mimirtool analyze ruler` or `mimirtool analyze rule-file` output file, which by default is `metrics-in-ruler.json`.            |
| -                    | `--output`                 | Sets the output file path, which by default is `prometheus-metrics.json`.                                                       |
| -                    | `--prometheus-http-prefix` | Sets the HTTP URL path under which the Prometheus api will be served.                                                           |

##### Example output

```json
{
  "total_active_series": 38184,
  "in_use_active_series": 14047,
  "additional_active_series": 24137,
  "in_use_metric_counts": [
    {
      "metric": "apiserver_request_duration_seconds_bucket",
      "count": 11400,
      "job_counts": [
        {
          "job": "apiserver",
          "count": 11400
        }
      ]
    },
    {
      "metric": "apiserver_request_total",
      "count": 684,
      "job_counts": [
        {
          "job": "apiserver",
          "count": 684
        }
      ]
    }
  ],
  "additional_metric_counts": [
    {
      "metric": "etcd_request_duration_seconds_bucket",
      "count": 2688,
      "job_counts": [
        {
          "job": "apiserver",
          "count": 2688
        }
      ]
    }
  ]
}
```

### Bucket validation

The following command validates that the object store bucket works correctly.

```bash
mimirtool bucket-validation
```

| Flag                   | Description                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------- |
| `--object-count`       | Sets the number of objects to create and delete. By default, the value is 2000.                               |
| `--report-every`       | Sets the number operations afterwhich an operations progress report is printed. By default, the value is 100. |
| `--test-runs`          | Sets the number of times to run the test. By default, the value is 1.                                         |
| `--prefix`             | Sets the path prefix to use for test objects in the object store.                                             |
| `--retries-on-error`   | Sets the number of times to retry if the object store returns an error.                                       |
| `--bucket-config`      | Sets the CLI arguments to configure a storage bucket.                                                         |
| `--bucket-config-help` | Displays help text that explains how to use the -bucket-config parameter.                                     |

### Config

#### Convert

The config convert command converts configuration parameters that work with Cortex v1.10.0 and above to parameters that work with Grafana Mimir v2.0.0.
It supports converting both CLI flags and [YAML configuration files]({{< relref "../../configure/configuration-parameters" >}}).

##### Configuration

| Flag                 | Description                                                                                                                                                                                                                                         |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--yaml-file`        | The YAML configuration file to convert.                                                                                                                                                                                                             |
| `--flags-file`       | A file containing a newline-delimited list of CLI flags to convert.                                                                                                                                                                                 |
| `--yaml-out`         | File to use for the converted YAML configuration. If not set, output to `stdout`.                                                                                                                                                                   |
| `--flags-out`        | File to use for list of converted CLI flags. If not set, output to `stdout`.                                                                                                                                                                        |
| `--update-defaults`  | If you set this flag and you set a configuration parameter to a default value that has changed in Mimir 2.0, the parameter updates to the new default value.                                                                                        |
| `--include-defaults` | If you set this flag, all default values are included in the output YAML, regardless of whether you explicitly set the values in the input files.                                                                                                   |
| `-v`, `--verbose`    | If you set this flag, the CLI flags and YAML paths from the old configuration that do not exist in the new configuration are printed to `stderr`. This flag also prints default values that have changed between the old and the new configuration. |
| `--gem`              | If you set this flag, the tool will convert from Grafana Metrics Enterprise (GEM) v1.7.x to v2.0.0.                                                                                                                                                 |

##### Changes to default values

`mimirtool config convert` helps you migrate from Cortex to Grafana Mimir. There are changes to the default values of
some configuration parameters in Mimir v2.0.0 that you might not want to use as part of this migration:

| Parameter                                     | Cortex/GEM 1.7 default   | Mimir/GEM 2.0 default    |
| --------------------------------------------- | ------------------------ | ------------------------ |
| `blocks_storage.backend`                      | `s3`                     | `filesystem`             |
| `ruler_storage.backend`                       | `s3`                     | `filesystem`             |
| `alertmanager_storage.backend`                | `s3`                     | `filesystem`             |
| `server.http_listen_port`                     | `80`                     | `8080`                   |
| `activity_tracker.filepath`                   | `./active-query-tracker` | `./metrics-activity.log` |
| `alertmanager.data_dir`                       | `data/`                  | `./data-alertmanager/`   |
| `blocks_storage.filesystem.dir`               | `<empty>`                | `blocks`                 |
| `compactor.data_dir`                          | `./data`                 | `./data-compactor/`      |
| `ruler.rule_path`                             | `/rules`                 | `./data-ruler/`          |
| `ruler_storage.filesystem.dir`                | `<empty>`                | `ruler`                  |
| (GEM only) `auth.type`                        | `trust`                  | `enterprise`             |
| (GEM only) `graphite.querier.schemas.backend` | `s3`                     | `filesystem`             |

For these parameters `mimirtool config convert` will output the Cortex default value even when the configuration parameter is not explicitly set in the input
configuration. If in the input configuration you explicitly set the Cortex default value, and you have provided
the `--update-defaults` flag, `mimirtool config convert` will not update the value to the Mimir default.

##### Example

The following example shows a command that converts Cortex [query-frontend]({{< relref "../../references/architecture/components/query-frontend" >}}) YAML configuration file and CLI flag to a Mimir-compatible YAML and CLI flag.

```bash
mimirtool config convert --yaml-file=cortex.yaml --flags-file=cortex.flags --yaml-out=mimir.yaml --flags-out=mimir.flags
```

`cortex.yaml` input file:

```yaml
query_range:
  results_cache:
    cache:
      memcached:
        expiration: 10s # Expiration was removed in Grafana Mimir, so this parameter will be missing from the output YAML
        batch_size: 2048
        parallelism: 10
      memcached_client:
        max_idle_conns: 32
```

`cortex.flags` input file:

```
-frontend.background.write-back-concurrency=45
```

After you run the command, the converted output should be:

`mimir.yaml` converted output file:

```yaml
frontend:
  results_cache:
    memcached:
      max_get_multi_batch_size: 2048
      max_get_multi_concurrency: 10
      max_idle_connections: 32

server:
  http_listen_port: 80
```

{{< admonition type="note" >}}
As a precaution,the output file includes `server.http_listen_port`.
The default value in Grafana Mimir changed from 80 to 8080.

Unless you explicitly set the port in the input configuration, the tool outputs the original default value.
{{< /admonition >}}

`mimir.flags` converted output file:

```
-query-frontend.results-cache.memcached.max-async-concurrency=45
```

##### Verbose output

When you set the `--verbose` flag, the output explains which configuration parameters were removed and which default values were changed.
The verbose output is printed to `stderr`.

The output includes the following entries:

- `field is no longer supported: <yaml_path>`

  This parameter was used in the input Cortex YAML file and removed from the output configuration.

- `flag is no longer supported: <flag_name>`

  This parameter was used in the input Cortex CLI flags file, but the parameter was removed in Grafana Mimir. The tool removed this CLI flag from the output configuration.

- `using a new default for <yaml_path>: <new_value> (used to be <old_value>)`

  The default value for a configuration parameter changed in Grafana Mimir. This parameter was not explicitly set in the input configuration files.
  When you run Grafana Mimir with the output configuration from `mimirtool config convert` Grafana Mimir uses the new default.

- `default value for <yaml_path> changed: <new_value> (used to be <old_value>); not updating`

  The default value for a configuration parameter that was set in the input configuration file has changed in Grafana Mimir.
  The tool has not converted the old default value to the new default value. To automatically update the default value to the new default value, pass the `--update-defaults` flag.

##### Extracting flags from Jsonnet

When using the Grafana Mimir Jsonnet library, all configuration uses flags set as object member key-value pairs.
To perform conversion with mimirtool, you first need to extract the flags from the JSON manifested from the Jsonnet evaluation.

Use the following bash script to extract the arguments from a specific component:

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

The first parameter of the script is a JSON file containing Kubernetes resources.
The second parameter of the script is the name of a container.

To retrieve the arguments from the distributor for a Tanka environment:

```bash
<PATH TO SCRIPT> <(tk eval environments/default) distributor
```

The script outputs results that are similar to the following:

```console
-consul.hostname=consul.cortex-to-mimir.svc.cluster.local:8500
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

Use the output of the script as input to run the `mimirtool` configuration conversion.

After conversion, you can use the following script to transform the converted flags back into JSON:

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

The only parameter of the script is a file containing the flags, with each flag on its own line.

### Backfill

The `backfill` command uploads Prometheus TSDB blocks into Grafana Mimir, by using the [block-upload API that is exposed by the compactor component]({{< relref "../../references/http-api#compactor" >}}).

If the command is interrupted, you can restart it. Mimirtool detects which blocks are already uploaded, and will only upload unfinished or new blocks.

The block-upload feature is disabled by default.
To enable the block-upload feature for a user or an entire system, refer to [Configure TSDB block upload]({{< relref "../../configure/configure-tsdb-block-upload" >}}).
If block upload is not enabled for the user, `mimirtool backfill` will fail.

##### Example

```bash
mimirtool backfill --address=http://mimir-compactor/ --id=anonymous /var/prometheus/{01G803NFXZ0MVKN71GT91HMV3Z,01G8BQ8PRR4TAP7EXZVBNTRBZ4,01G8CB7GTTC5ZXY23WTXHSYQXQ}
```

The results of the `backfill` command are as follows:

```console
INFO[0000] Backfilling                              blocks="/var/prometheus/01G803NFXZ0MVKN71GT91HMV3Z,/var/prometheus/01G8BQ8PRR4TAP7EXZVBNTRBZ4,/var/prometheus/01G8CB7GTTC5ZXY23WTXHSYQXQ" user=anonymous
INFO[0000] making request to start block upload     block=01G803NFXZ0MVKN71GT91HMV3Z file=meta.json path=/var/prometheus/01G803NFXZ0MVKN71GT91HMV3Z
WARN[0000] block already exists on the server       path=/var/prometheus/01G803NFXZ0MVKN71GT91HMV3Z
INFO[0000] making request to start block upload     block=01G8BQ8PRR4TAP7EXZVBNTRBZ4 file=meta.json path=/var/prometheus/01G8BQ8PRR4TAP7EXZVBNTRBZ4
INFO[0000] uploading block file                     block=01G8BQ8PRR4TAP7EXZVBNTRBZ4 file=index path=/var/prometheus/01G8BQ8PRR4TAP7EXZVBNTRBZ4 size=259867
INFO[0000] uploading block file                     block=01G8BQ8PRR4TAP7EXZVBNTRBZ4 file=chunks/000001 path=/var/prometheus/01G8BQ8PRR4TAP7EXZVBNTRBZ4 size=5024391
INFO[0000] block uploaded successfully              block=01G8BQ8PRR4TAP7EXZVBNTRBZ4 path=/var/prometheus/01G8BQ8PRR4TAP7EXZVBNTRBZ4
INFO[0000] making request to start block upload     block=01G8CB7GTTC5ZXY23WTXHSYQXQ file=meta.json path=/var/prometheus/01G8CB7GTTC5ZXY23WTXHSYQXQ
INFO[0000] uploading block file                     block=01G8CB7GTTC5ZXY23WTXHSYQXQ file=index path=/var/prometheus/01G8CB7GTTC5ZXY23WTXHSYQXQ size=151181
INFO[0000] uploading block file                     block=01G8CB7GTTC5ZXY23WTXHSYQXQ file=chunks/000001 path=/var/prometheus/01G8CB7GTTC5ZXY23WTXHSYQXQ size=1986792
INFO[0000] block uploaded successfully              block=01G8CB7GTTC5ZXY23WTXHSYQXQ path=/var/prometheus/01G8CB7GTTC5ZXY23WTXHSYQXQ
INFO[0001] finished uploading blocks                already_exists=1 failed=0 succeeded=2
```

## License

This software is licensed as AGPLv3. For more information, see [LICENSE](https://github.com/grafana/mimir/blob/main/LICENSE).
