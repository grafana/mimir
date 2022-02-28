---
title: "Reference: Mimirtool"
description: ""
weight: 100
---

# Mimirtool

Mimirtool is designed to interact with:

- user-facing APIs provided by Grafana Mimir.
- backend storage components containing Grafana Mimir data.

## Installation

Refer to the [latest release](https://github.com/grafana/mimir/releases) for installation instructions.

## Configuration options

In order to interact with your Grafana Mimir, Grafana Enterprise Metrics, Prometheus, or Grafana instance, you need to
set some configuration options. These can be set via environment variables or CLI flags.

| Env Variable      | Flag        | Description                                                                                                                                                        |
| ----------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `MIMIR_ADDRESS`   | `--address` | Address of the API of the desired Grafana Mimir cluster.                                                                                                           |
| `MIMIR_API_USER`  | `--user`    | Sets the basic auth username. If empty and `MIMIR_API_KEY` is set, `MIMIR_TENANT_ID` will be used instead. If you're using Grafana Cloud this is your instance ID. |
| `MIMIR_API_KEY`   | `--key`     | Sets the basic auth password. If you're using Grafana Cloud, this is your API key.                                                                                 |
| `MIMIR_TENANT_ID` | `--id`      | The tenant ID of the Grafana Mimir instance to interact with.                                                                                                      |

## Commands

### Alertmanager

The following commands interact Grafana Mimir alertmanager configuration and alert template files.

#### Get configuration

Show the current Alertmanager configuration.

```bash
mimirtool alertmanager get
```

#### Load configuration

Load an Alertmanager configuration to the Alertmanager instance.

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

Deletes the Alertmanager configuration in the Grafana Mimir Alertmanager.

```bash
mimirtool alertmanager delete
```

### Alerts

#### Verify

Verifies whether or not alerts in an Alertmanager cluster are deduplicated; useful for verifying correct configuration
when transferring from Prometheus to Grafana Mimir alert evaluation.

```bash
mimirtool alerts verify
```

### Rules

The following commands:

- load and show Prometheus rule files.
- interact with individual rule groups in the Mimir ruler.
- manipulate local rule files.

#### List

Retrieves the names of all rule groups in the Grafana Mimir instance and prints them to the terminal.

```bash
mimirtool rules list
```

#### Print

Retrieves all rule groups in the Grafana Mimir instance and print them to the terminal.

```bash
mimirtool rules print
```

#### Get

Retrieves a single rule group and prints it to the terminal.

```bash
mimirtool rules get <namespace> <rule_group_name>
```

#### Delete

Deletes a rule group.

```bash
mimirtool rules delete <namespace> <rule_group_name>
```

#### Load

Loads each rule group from the files into Grafana Mimir. It overwrites all existing rule groups with the same name.

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

This command's aim is not to verify query correctness but just YAML and PromQL expression formatting within the rule file.
This command always edits in place, you can use the dry run flag (`-n`) if you'd like to perform a trial run that does
not make any changes. This command does not interact with your Grafana Mimir cluster.

```bash
mimirtool rules lint <file_path>...
```

The format of the file is the same as in [rules load](#load).

#### Prepare

This command prepares a rules file for upload to Grafana Mimir. It lints all your PromQL expressions and adds an
specific label to your PromQL query aggregations in the file. This command does not interact with your Grafana Mimir
cluster. The format of the file is the same as in [rules load](#load).

```bash
mimirtool rules prepare <file_path>...
```

##### Configuration

| Env Variable | Flag                      | Description                                                                                 |
| ------------ | ------------------------- | ------------------------------------------------------------------------------------------- |
| -            | `-i`, `--in-place`        | Edit the file in place. If unset, a new file with `.result` extension contains the results. |
| -            | `-l`, `--label="cluster"` | Specify the label for aggregations. `cluster` by default.                                   |

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

At the end of the run, the command outputs whether the operation was a success:

```console
INFO[0000] SUCCESS: 1 rules found, 0 modified expressions
```

#### Check

This command checks rules against the recommended [best practices](https://prometheus.io/docs/practices/rules/) for
rules. This command does not interact with your Grafana Mimir cluster.

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
ERRO[0000] bad recording rule name                       error="recording rule name does not match level:metric:operation format, must contain at least one colon" file=rules.yaml rule=job_http_inprogress_requests_sum ruleGroup=example
```

The format of the file is the same as in [rules load](#load).

#### Diff

This command compares rules against the rules in your Grafana Mimir cluster.

```bash
mimirtool rules diff <file_path>...
```

The format of the file is the same as in [rules load](#load).

#### Sync

This command compares rules against the rules in your Grafana Mimir cluster. It applies any differences to your Grafana
Mimir cluster.

```bash
mimirtool rules sync <file_path>...
```

The format of the file is the same as in [rules load](#load).

### Remote read

Grafana Mimir exposes a [remote read API] which allows access to the stored series. The `remote-read` subcommand
of `mimirtool` allows you to interact with its API, to find out which series are stored.

[remote read api]: https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations

#### Remote read show statistics

The `remote-read stats` command summarizes statistics of the stored series matching the selector.

##### Example

```bash
mimirtool remote-read stats --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
```

The output is the following:

```console
INFO[0000] Create remote read client using endpoint 'http://demo.robustperception.io:9090/api/v1/read'
INFO[0000] Querying time from=2020-12-30T14:00:00Z to=2020-12-30T15:00:00Z with selector={job="node"}
INFO[0000] MIN TIME                           MAX TIME                           DURATION     NUM SAMPLES  NUM SERIES   NUM STALE NAN VALUES  NUM NAN VALUES
INFO[0000] 2020-12-30 14:00:00.629 +0000 UTC  2020-12-30 14:59:59.629 +0000 UTC  59m59s       159480       425          0                     0
```

#### Dump series

The `remote-read dump` command prints all series and samples matching the selector.

##### Example

```bash
mimirtool remote-read dump --selector 'up{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
```

The output is the following:

```console
{__name__="up", instance="demo.robustperception.io:9100", job="node"} 1 1609336914711
{__name__="up", instance="demo.robustperception.io:9100", job="node"} NaN 1609336924709 # StaleNaN
...
```

#### Export series into local TSDB

The `remote-read export` command exports all series and samples matching the selector into a local TSDB. This TSDB can
then be further analyzed with local tooling like `prometheus` and [`promtool`](https://github.com/prometheus/prometheus/tree/main/cmd/promtool).

```bash
# Use Remote Read API to download all metrics with label job=name into local tsdb
mimirtool remote-read export --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read --tsdb-path ./local-tsdb
```

The output is the following:

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

##### Examples for using local TSDB

Analyzing contents using promtool

```bash
promtool tsdb analyze ./local-tsdb
```

Dump all values of the TSDB

```bash
promtool tsdb dump ./local-tsdb
```

Run a local prometheus

```bash
prometheus --storage.tsdb.path ./local-tsdb --config.file=<(echo "")
```

### Generate ACL Headers

This lets you generate the header which can then be used to enforce access control rules in GEM or Grafana Cloud only. Grafana Mimir doesn't support ACL.

```bash
mimirtool acl generate-header --id=<tenant_id> --rule=<promql_selector>
```

#### Example

```bash
mimirtool acl generate-header --id=1234 --rule='{namespace="A"}'
```

Output:

```console
The header to set:
X-Prom-Label-Policy: 1234:%7Bnamespace=%22A%22%7D
```

### Analyze

Run analysis against your Grafana or Hosted Grafana instance to see which metrics are being used and exported. Can also
extract metrics from dashboard JSON and rules YAML files.

#### Grafana

This command will run against your Grafana instance and will download its dashboards and then extract the Prometheus
metrics used in its queries. The output is a JSON file. You can use this file
with `analyze prometheus --grafana-metrics-file`.

```bash
mimirtool analyze grafana --address=<url>
```

##### Configuration

| Env Variable      | Flag        | Description                                                                                                                                                    |
| ----------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GRAFANA_ADDRESS` | `--address` | Address of the Grafana instance.                                                                                                                               |
| `GRAFANA_API_KEY` | `--key`     | The API Key for the Grafana instance. Create a key following the instructions at [Authentication API](https://grafana.com/docs/grafana/latest/http_api/auth/). |
| -                 | `--output`  | The output file path. `metrics-in-grafana.json` by default.                                                                                                    |

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

This command will run against your Grafana Mimir, Grafana Enterprise Metrics, or Grafana Cloud Prometheus instance. It
will fetch its rule groups and extract the Prometheus metrics used in the rule queries. The output is a JSON file. You
can use this file with `analyze prometheus --ruler-metrics-file`.

```bash
mimirtool analyze ruler --address=<url> --id=<tenant_id>
```

##### Configuration

| Env Variable      | Flag        | Description                                                                           |
| ----------------- | ----------- | ------------------------------------------------------------------------------------- |
| `MIMIR_ADDRESS`   | `--address` | Address of the Prometheus instance.                                                   |
| `MIMIR_TENANT_ID` | `--user`    | Sets the basic auth username. If you're using Grafana Cloud this is your instance ID. |
| `MIMIR_API_KEY`   | `--key`     | Sets the basic auth password. If you're using Grafana Cloud, this is your API key.    |
| -                 | `--output`  | The output file path. `metrics-in-ruler.json` by default.                             |

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

This command accepts Grafana dashboard JSON files as input and extracts Prometheus metrics used in the queries. The
output is a JSON file. You can use this file with `analyze prometheus --grafana-metrics-file`.

```bash
mimirtool analyze dashboard <file>...
```

##### Configuration

| Env Variable | Flag       | Description                                                 |
| ------------ | ---------- | ----------------------------------------------------------- |
| -            | `--output` | The output file path. `prometheus-metrics.json` by default. |

#### Rule file

This command accepts Prometheus rule YAML files as input and extracts Prometheus metrics used in the queries. The output
is a JSON file. You can use this file with `analyze prometheus --ruler-metrics-file`.

```bash
mimirtool analyze rule-file <file>
```

##### Configuration

| Env Variable | Flag       | Description                                                 |
| ------------ | ---------- | ----------------------------------------------------------- |
| -            | `--output` | The output file path. `prometheus-metrics.json` by default. |

#### Prometheus

This command will run against your Grafana Mimir, Grafana Metrics Enterprise, Prometheus, or Cloud Prometheus instance.
It will use the output from a previous run of `analyze grafana`, `analyze dashboard`, `analyze ruler`
or `analyze rule-file` to show how many series in the Prometheus instance are actually being used in dashboards and/or
rules. Also, it will show which metrics exist in Grafana Cloud that are **not** in dashboards or rules. The output is a
JSON file.

> **Note:** The command will make a request for every active series in the Prometheus instance.
> This may take some time for Prometheis with a lot of active series.

```bash
mimirtool analyze prometheus --address=<url> --id=<tenant_id>
```

##### Configuration

| Env Variable      | Flag                     | Description                                                                                                     |
| ----------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `MIMIR_ADDRESS`   | `--address`              | Address of the Prometheus instance.                                                                             |
| `MIMIR_TENANT_ID` | `--user`                 | Sets the basic auth username. If you're using Grafana Cloud this is your instance ID.                           |
| `MIMIR_API_KEY`   | `--key`                  | Sets the basic auth password. If you're using Grafana Cloud, this is your API key.                              |
| -                 | `--grafana-metrics-file` | `mimirtool analyze grafana` or `mimirtool analyze dashboard` output file. `metrics-in-grafana.json` by default. |
| -                 | `--ruler-metrics-file`   | `mimirtool analyze ruler` or `mimirtool analyze rule-file` output file. `metrics-in-ruler.json` by default.     |
| -                 | `--output`               | The output file path. `prometheus-metrics.json` by default.                                                     |

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

Validate that object store bucket works correctly.

```bash
mimirtool bucket-validation
```

| Env Variable | Flag                   | Description                                                       |
| ------------ | ---------------------- | ----------------------------------------------------------------- |
| -            | `--object-count`       | Number of objects to create & delete. 2000 by default             |
| -            | `--report-every`       | Every X operations a progress report gets printed. 100 by default |
| -            | `--test-runs`          | Number of times we want to run the whole test. 1 by default       |
| -            | `--prefix`             | Path prefix to use for test objects in object store.              |
| -            | `--retries-on-error`   | Number of times we want to retry if object store returns error.   |
| -            | `--bucket-config`      | The CLI args to configure a storage bucket.                       |
| -            | `--bucket-config-help` | Help text explaining how to use the -bucket-config parameter.     |

## License

Licensed AGPLv3, see [LICENSE](../../LICENSE).
