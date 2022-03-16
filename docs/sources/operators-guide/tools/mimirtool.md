---
title: "Grafana Mimirtool"
menuTitle: "Mimirtool"
description: "Use Mimirtool to perform common tasks in Grafana Mimir or Grafana Cloud Metrics."
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

- The `bucket-validate` command verifies that an object storage bucket is suitable as a backend storage for Grafana Mimir.

  For more information about the `bucket-validate` command, refer to [Bucket-validate]({{< relref "#bucket-validate" >}}).

- The `acl` command generates the label-based access control header used in Grafana Enterprise Metrics and Grafana Cloud Metrics.

  For more information about the `acl` command, refer to [ACL]({{< relref "#acl" >}}).

- The `config` command helps convert configuration files from Cortex to Grafana Mimir.

  For more information about the `config` command, refer to [Config]({{< relref "#config" >}})

Mimirtool interacts with:

- User-facing APIs provided by Grafana Mimir.
- Backend storage components containing Grafana Mimir data.

## Installation

To install Grafana Mimirtool, refer to the [latest release](https://github.com/grafana/mimir/releases).

## Configuration options

For Mimirtools to interact with Grafana Mimir, Grafana Enterprise Metrics, Prometheus, or Grafana, set the following environment variables or CLI flags.

| Environment variable | Flag        | Description                                                                                                                                                                                      |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `MIMIR_ADDRESS`      | `--address` | Sets the address of the API of the Grafana Mimir cluster.                                                                                                                                        |
| `MIMIR_API_USER`     | `--user`    | Sets the basic auth username. If this variable is empty and `MIMIR_API_KEY` is set, the system uses `MIMIR_TENANT_ID` instead. If you're using Grafana Cloud, this variable is your instance ID. |
| `MIMIR_API_KEY`      | `--key`     | Sets the basic auth password. If you're using Grafana Cloud, this variable is your API key.                                                                                                      |
| `MIMIR_TENANT_ID`    | `--id`      | Sets the tenant ID of the Grafana Mimir instance that Mimirtools interacts with.                                                                                                                 |

## Commands

The following sections outline the commands that you can run against Grafana Mimir and Grafana Cloud Metrics.

### Alertmanager

The following commands interact with Grafana Mimir Alertmanager configuration and alert template files.

#### Get configuration

The following command shows the current Alertmanager configuration.

```bash
mimirtool alertmanager get
```

#### Load configuration

The following command loads an Alertmanager configuration to the Alertmanager instance.

```bash
mimirtool alertmanager load <config_file>
mimirtool alertmanager load <config_file> <template_files>...
```

##### Example

```bash
mimirtool alertmanager load ./example_alertmanager_config.yaml
```

`./example_alertmanager_config.yaml`:

```yaml
route:
  receiver: "example_receiver"
  group_by: ["example_groupby"]
receivers:
  - name: "example_receiver"
```

#### Delete configuration

The following command deletes the Alertmanager configuration in the Grafana Mimir Alertmanager.

```bash
mimirtool alertmanager delete
```

#### Alert verification

The following command verifies if alerts in an Alertmanager cluster are deduplicated. This command is useful for verifying the correct configuration when transferring from Prometheus to Grafana Mimir alert evaluation.

```bash
mimirtool alerts verify
```

### Rules

The rules command features sub-commands for working with Prometheus rule files and with the APIs in the Grafana Mimir ruler.

The commands in this section enable you to perform the following actions:

- Load and show Prometheus rule files
- Interact with individual rule groups in the Mimir ruler
- Manipulate local rule files

#### List

The following command retrieves the names of all rule groups in the Grafana Mimir instance and prints them to the terminal.

```bash
mimirtool rules list
```

#### Print

The following command retrieves all rule groups in the Grafana Mimir instance and prints them to the terminal.

```bash
mimirtool rules print
```

#### Get

The following command retrieves a single rule group and prints it to the terminal.

```bash
mimirtool rules get <namespace> <rule_group_name>
```

#### Delete

The following command deletes a rule group.

```bash
mimirtool rules delete <namespace> <rule_group_name>
```

#### Load

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

#### Lint

The `lint` command provides YAML and PromQL expression formatting within the rule file.

This command edits the rule file in place.
To perform a trial run that does not make changes, you can use the dry run flag (`-n`).

> **Note:** This command does not verify if a query is correct and does not interact with your Grafana Mimir cluster.

```bash
mimirtool rules lint <file_path>...
```

The format of the file is the same format as shown in [rules load](#load).

#### Prepare

This `prepare` command prepares a rules file that you upload to Grafana Mimir.
It lints all PromQL expressions and adds a label to your PromQL query aggregations in the file.
The format of the file is the same format as shown in [rules load](#load).

> **Note:** This command does not interact with your Grafana Mimir cluster.

```bash
mimirtool rules prepare <file_path>...
```

##### Configuration

| Flag                      | Description                                                                                                                  |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `-i`, `--in-place`        | Edits the file in place. If not set, the system generates a new file with the extension `.result` that contains the results. |
| `-l`, `--label="cluster"` | Specifies the label for aggregations. By default, the label is set to `cluster`.                                             |

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

The format of the file is the same format as shown in [rules load](#load).

#### Diff

The following command compares rules against the rules in your Grafana Mimir cluster.

```bash
mimirtool rules diff <file_path>...
```

The format of the file is the same format as shown in [rules load](#load).

#### Sync

The `sync` command compares rules against the rules in your Grafana Mimir cluster.
The command applies any differences to your Grafana Mimir cluster.

```bash
mimirtool rules sync <file_path>...
```

The format of the file is the same format as shown in [rules load](#load).

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

> **Note**: Grafana Mimir does not support ACLs.

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
The output is a JSON file. You can use this file with `analyse prometheus --grafana-metrics-file`.

```bash
mimirtool analyze grafana --address=<url>
```

##### Configuration

| Environment variable | Flag        | Description                                                                                                                                        |
| -------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GRAFANA_ADDRESS`    | `--address` | Sets the address of the Grafana instance.                                                                                                          |
| `GRAFANA_API_KEY`    | `--key`     | Sets the API Key for the Grafana instance. To create a key, refer to [Authentication API](https://grafana.com/docs/grafana/latest/http_api/auth/). |
| -                    | `--output`  | Sets the output file path, which by default is `metrics-in-grafana.json`.                                                                          |

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
The output is a JSON file. You can use this file with `analyse prometheus --ruler-metrics-file`.

```bash
mimirtool analyze ruler --address=<url> --id=<tenant_id>
```

##### Configuration

| Environment variable | Flag        | Description                                                                                     |
| -------------------- | ----------- | ----------------------------------------------------------------------------------------------- |
| `MIMIR_ADDRESS`      | `--address` | Sets the address of the Prometheus instance.                                                    |
| `MIMIR_TENANT_ID`    | `--user`    | Sets the basic auth username. If you're using Grafana Cloud, this variable is your instance ID. |
| `MIMIR_API_KEY`      | `--key`     | Sets the basic auth password. If you're using Grafana Cloud, this variable is your API key.     |
| -                    | `--output`  | Sets the output file path, which by default is `metrics-in-ruler.json`.                         |

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
| -                    | `--output` | Sets the output file path, which by default is `prometheus-metrics.json`. |

#### Rule-file

The following command accepts Prometheus rule YAML files as input and extracts Prometheus metrics used in the queries.
The output is a JSON file. You can use the output file with `analyse prometheus --ruler-metrics-file`.

```bash
mimirtool analyze rule-file <file>
```

##### Configuration

| Environment variable | Flag       | Description                                                               |
| -------------------- | ---------- | ------------------------------------------------------------------------- |
| -                    | `--output` | Sets the output file path, which by default is `prometheus-metrics.json`. |

#### Prometheus

The following command runs against your Grafana Mimir, Grafana Metrics Enterprise, Prometheus, or Cloud Prometheus instance.
The command uses the output from a previous run of `analyse grafana`, `analyse dashboard`, `analyse ruler`
or `analyse rule-file` to show the number of series in the Prometheus instance that are used in dashboards or rules, or both.
This command also shows which metrics exist in Grafana Cloud that are _not_ in dashboards or rules. The output is a JSON file.

> **Note:** The command makes a request for every active series in the Prometheus instance.
> For Prometheus instances with a large number of active series, this command might take time to complete.

```bash
mimirtool analyze prometheus --address=<url> --id=<tenant_id>
```

##### Configuration

| Environment variable | Flag                     | Description                                                                                                              |
| -------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `MIMIR_ADDRESS`      | `--address`              | Sets the address of the Prometheus instance.                                                                             |
| `MIMIR_TENANT_ID`    | `--user`                 | Sets the basic auth username. If you're using Grafana Cloud this variable is your instance ID.                           |
| `MIMIR_API_KEY`      | `--key`                  | Sets the basic auth password. If you're using Grafana Cloud, this variable is your API key.                              |
| -                    | `--grafana-metrics-file` | `mimirtool analyse grafana` or `mimirtool analyse dashboard` output file, which by default is `metrics-in-grafana.json`. |
| -                    | `--ruler-metrics-file`   | `mimirtool analyse ruler` or `mimirtool analyse rule-file` output file, which by default is `metrics-in-ruler.json`.     |
| -                    | `--output`               | Sets the output file path, which by default is `prometheus-metrics.json`.                                                |

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
It supports converting both CLI flags and [YAML configuration files]({{< relref "../configuring/reference-configuration-parameters.md" >}}).

##### Configuration

| Flag                 | Description                                                                                                                                                                                                                                         |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--yaml-file`        | The YAML configuration file to convert.                                                                                                                                                                                                             |
| `--flags-file`       | Newline-delimited list of CLI flags to convert.                                                                                                                                                                                                     |
| `--yaml-out`         | The file to output the converted YAML configuration to. If not set, output to `stdout`.                                                                                                                                                             |
| `--flags-out`        | The file to output the list of converted CLI flags to. If not set, output to `stdout`.                                                                                                                                                              |
| `--update-defaults`  | If you set this flag and you set a configuration parameter to a default value that has changed in Mimir 2.0, the parameter updates to the new default value.                                                                                        |
| `--include-defaults` | If you set this flag, all default values are included in the output YAML, regardless of whether you explicitly set the values in the input files.                                                                                                   |
| `-v`, `--verbose`    | If you set this flag, the CLI flags and YAML paths from the old configuration that do not exist in the new configuration are printed to `stderr`. This flag also prints default values that have changed between the old and the new configuration. |

##### Example

The following example shows a command that converts Cortex [query-frontend]({{< relref "../architecture/components/query-frontend" >}}) YAML configuration file and CLI flag to a Mimir-compatible YAML and CLI flag.

```bash
mimirtool config convert --yaml-file=cortex.yaml --flags-file=cortex.flags --yaml-out=mimir.yaml --flags-out=mimir.flags
```

`cortex.yaml`:

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

`cortex.flags`:

```
-frontend.background.write-back-concurrency=45
```

After you run the command, the contents of `mimir.yaml` and `mimir.flags` should look like:

`mimir.yaml`:

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

> **Note:** As a precaution,`server.http_listen_port` is included. The default value in Grafana Mimir changed from 80 to 8080. Unless you explicitly set the port in the input configuration, the tool outputs the old default value.

`mimir.flags`:

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

## License

Licensed AGPLv3, see [LICENSE](../../LICENSE).
