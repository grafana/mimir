# Cortex Tools

This repo contains tools used for interacting with [Cortex](https://github.com/cortexproject/cortex).

- [benchtool](docs/benchtool.md): A powerful YAML driven tool for benchmarking
  Cortex write and query API.
- [cortextool](#cortextool): Interacts with user-facing Cortex APIs and backend storage components
- [chunktool](#chunktool): Interacts with chunks stored and indexed in Cortex storage backends.
- [logtool](#logtool): Tool which parses Cortex query-frontend logs and formats them for easy analysis.
- [e2ealerting](docs/e2ealerting.md): Tool that helps measure how long an alerts takes from scrape of sample to Alertmanager notifcation delivery.

# Installation

The various binaries are available for macOS, Windows, and Linux.

## macOS

> TODO https://github.com/grafana/mimir/issues/837

`mimirtool` is available on macOS via [Homebrew](https://brew.sh/):

```bash
$ brew install grafana/grafana/mimirtool
```

## Linux, Docker and Windows

Refer to the [latest release](https://github.com/grafana/cortex-tools/releases) for installation intructions on these.

## mimirtool

This tool is designed to interact with the various user-facing APIs provided by Cortex, as well as, interact with various backend storage components containing Cortex data.

### Config Commands

Config commands interact with the Cortex api and read/create/update/delete user configs from Cortex. Specifically a users alertmanager and rule configs can be composed and updated using these commands.

#### Configuration

| Env Variables   | Flag      | Description                                                                                                                                                                       |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MIMIR_ADDRESS   | `address` | Address of the API of the desired Mimir cluster.                                                                                                                                  |
| MIMIR_API_USER  | `user`    | In cases where the Mimir API is set behind a basic auth gateway, a user can be set as a basic auth user. If empty and MIMIR_API_KEY is set, MIMIR_TENANT_ID will be used instead. |
| MIMIR_API_KEY   | `key`     | In cases where the Mimir API is set behind a basic auth gateway, a key can be set as a basic auth password.                                                                       |
| MIMIR_TENANT_ID | `id`      | The tenant ID of the Mimir instance to interact with.                                                                                                                             |

#### Alertmanager

The following commands are used by users to interact with their Cortex alertmanager configuration, as well as their alert template files.

##### Alertmanager Get

    mimirtool alertmanager get

##### Alertmanager Load

    mimirtool alertmanager load ./example_alertmanager_config.yaml

    mimirtool alertmanager load ./example_alertmanager_config.yaml template_file1.tmpl template_file2.tmpl

#### Rules

The following commands are used by users to interact with their Cortex ruler configuration. They can load prometheus rule files, as well as interact with individual rule groups.

##### Rules List

This command will retrieve all of the rule groups stored in the specified Cortex instance and print each one by rule group name and namespace to the terminal.

    mimirtool rules list

##### Rules Print

This command will retrieve all of the rule groups stored in the specified Cortex instance and print them to the terminal.

    mimirtool rules print

##### Rules Get

This command will retrieve the specified rule group from Cortex and print it to the terminal.

    mimirtool rules get example_namespace example_rule_group

##### Rules Delete

This command will delete the specified rule group from the specified namespace.

    mimirtool rules delete example_namespace example_rule_group

##### Rules Load

This command will load each rule group in the specified files and load them into Cortex. If a rule already exists in Cortex it will be overwritten if a diff is found.

    mimirtool rules load ./example_rules_one.yaml ./example_rules_two.yaml  ...

Example file:

```yaml
namespace: my_namespace
groups:
  - name: example
    interval: 5m
    source_tenants: [team-engineering, team-finance]
    rules:
      - record: job:http_inprogress_requests:sum
        expr: sum by (job) (http_inprogress_requests)
```

##### Rules Lint

This command lints a rules file. The linter's aim is not to verify correctness but just YAML and PromQL expression formatting within the rule file. This command always edits in place, you can use the dry run flag (`-n`) if you'd like to perform a trial run that does not make any changes. This command does not interact with your Cortex cluster.

    mimirtool rules lint -n ./example_rules_one.yaml ./example_rules_two.yaml ...

The format of the file is the same as in [Rules Diff](#rules-diff).

##### Rules Prepare

This command prepares a rules file for upload to Cortex. It lints all your PromQL expressions and adds an specific label to your PromQL query aggregations in the file. This command does not interact with your Cortex cluster.

    mimirtool rules prepare -i ./example_rules_one.yaml ./example_rules_two.yaml ...

The format of the file is the same as in [Rules Diff](#rules-diff).

There are two flags of note for this command:

- `-i` which allows you to edit in place, otherwise a a new file with a `.output` extension is created with the results of the run.
- `-l` which allows you specify the label you want you add for your aggregations, it is `cluster` by default.

At the end of the run, the command tells you whenever the operation was a success in the form of

    INFO[0000] SUCESS: 194 rules found, 0 modified expressions

It is important to note that a modification can be a PromQL expression lint or a label add to your aggregation.

##### Rules Check

This command checks rules against the recommended [best practices](https://prometheus.io/docs/practices/rules/) for rules. This command does not interact with your Mimir cluster.

    mimirtool rules check ./example_rules_one.yaml

The format of the file is the same as in [Rules Diff](#rules-diff).

##### Rules Diff

This command compares rules against the rules in your Mimir cluster.

    mimirtool rules diff ./example_rules_one.yaml

The format of the file is the same as in [Rules Diff](#rules-diff).

##### Rules Sync

This command compares rules against the rules in your Mimir cluster. It applies any differences to your Mimir cluster.

    mimirtool rules sync ./example_rules_one.yaml

The format of the file is the same as in [Rules Diff](#rules-diff).

#### Remote Read

Cortex exposes a [Remote Read API] which allows access to the stored series. The `remote-read` subcommand of `cortextool` allows to interact with its API, to find out which series are stored.

[remote read api]: https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations

##### Remote Read show statistics

The `remote-read stats` command summarizes statistics of the stored series matching the selector.

```sh
mimirtool remote-read stats --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
INFO[0000] Create remote read client using endpoint 'http://demo.robustperception.io:9090/api/v1/read'
INFO[0000] Querying time from=2020-12-30T14:00:00Z to=2020-12-30T15:00:00Z with selector={job="node"}
INFO[0000] MIN TIME                           MAX TIME                           DURATION     NUM SAMPLES  NUM SERIES   NUM STALE NAN VALUES  NUM NAN VALUES
INFO[0000] 2020-12-30 14:00:00.629 +0000 UTC  2020-12-30 14:59:59.629 +0000 UTC  59m59s       159480       425          0                     0
```

##### Remote Read dump series

The `remote-read dump` command prints all series and samples matching the selector.

```sh
mimirtool remote-read dump --selector 'up{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read
{__name__="up", instance="demo.robustperception.io:9100", job="node"} 1 1609336914711
{__name__="up", instance="demo.robustperception.io:9100", job="node"} NaN 1609336924709 # StaleNaN
[...]
```

##### Remote Read export series into local TSDB

The `remote-read export` command exports all series and samples matching the selector into a local TSDB. This TSDB can then be further analysed with local tooling like `prometheus` and `promtool`.

```shell
# Use Remote Read API to download all metrics with label job=name into local tsdb
mimirtool remote-read export --selector '{job="node"}' --address http://demo.robustperception.io:9090 --remote-read-path /api/v1/read --tsdb-path ./local-tsdb
INFO[0000] Create remote read client using endpoint 'http://demo.robustperception.io:9090/api/v1/read'
INFO[0000] Created TSDB in path './local-tsdb'
INFO[0000] Using existing TSDB in path './local-tsdb'
INFO[0000] Querying time from=2020-12-30T13:53:59Z to=2020-12-30T14:53:59Z with selector={job="node"}
INFO[0001] Store TSDB blocks in './local-tsdb'
INFO[0001] BLOCK ULID                  MIN TIME                       MAX TIME                       DURATION     NUM SAMPLES  NUM CHUNKS   NUM SERIES   SIZE
INFO[0001] 01ETT28D6B8948J87NZXY8VYD9  2020-12-30 13:53:59 +0000 UTC  2020-12-30 13:59:59 +0000 UTC  6m0.001s     15950        429          425          105KiB867B
INFO[0001] 01ETT28D91Z9SVRYF3DY0KNV41  2020-12-30 14:00:00 +0000 UTC  2020-12-30 14:53:58 +0000 UTC  53m58.001s   143530       1325         425          509KiB679B

# Examples for using local TSDB
## Analyzing contents using promtool
promtool tsdb analyze ./local-tsdb

## Dump all values of the TSDB
promtool tsdb dump ./local-tsdb

## Run a local prometheus
prometheus --storage.tsdb.path ./local-tsdb --config.file=<(echo "")
```

#### Overrides Exporter

The Overrides Exporter allows to continuously export [per tenant configuration overrides][runtime-config] as metrics. Optionally it can also export a presets file (cf. example [override config file] and [presets file]).

    mimirtool overrides-exporter --overrides-file overrides.yaml --presets-file presets.yaml

[override config file]: ./pkg/commands/testdata/overrides.yaml
[presets file]: ./pkg/commands/testdata/presets.yaml
[runtime-config]: https://cortexmetrics.io/docs/configuration/arguments/#runtime-configuration-file

#### Generate ACL Headers

This lets you generate the header which can then be used to enforce access control rules in GME / GrafanaCloud.

```shell
./mimirtool acl generate-header --id=1234 --rule='{namespace="A"}'
```

#### Analyse

Run analysis against your Prometheus, Grafana and Cortex to see which metrics being used and exported. Can also extract metrics
from dashboard JSON and rules YAML files.

##### `analyse grafana`

This command will run against your Grafana instance and will download its dashboards and then extract the Prometheus metrics used in its queries. The output is a JSON file.

###### Configuration

| Env Variables   | Flag      | Description                                                                                                                                 |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| GRAFANA_ADDRESS | `address` | Address of the Grafana instance.                                                                                                            |
| GRAFANA_API_KEY | `key`     | The API Key for the Grafana instance. Create a key using the following instructions: https://grafana.com/docs/grafana/latest/http_api/auth/ |
| \_\_            | `output`  | The output file path. metrics-in-grafana.json by default.                                                                                   |

###### Running the command

```shell
mimirtool analyse grafana --address=<grafana-address> --key=<API-Key>
```

###### Sample output

```json
{
  "metricsUsed": [
    "apiserver_request:availability30d",
    "workqueue_depth",
    "workqueue_queue_duration_seconds_bucket",
    ...
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
        "workqueue_queue_duration_seconds_bucket",
        ...
      ],
      "parse_errors": null
    }
  ]
}
```

##### `analyse ruler`

This command will run against your Grafana Cloud Prometheus instance and will fetch its rule groups. It will then extract the Prometheus metrics used in the rule queries. The output is a JSON file.

###### Configuration

| Env Variables   | Flag      | Description                                             |
| --------------- | --------- | ------------------------------------------------------- |
| MIMIR_ADDRESS   | `address` | Address of the Prometheus instance.                     |
| MIMIR_TENANT_ID | `id`      | If you're using Grafana Cloud this is your instance ID. |
| MIMIR_API_KEY   | `key`     | If you're using Grafana Cloud this is your API Key.     |
| \_\_            | `output`  | The output file path. metrics-in-ruler.json by default. |

###### Running the command

```shell
mimirtool analyse ruler --address=https://prometheus-blocks-prod-us-central1.grafana.net --id=<1234> --key=<API-Key>
```

###### Sample output

```json
{
  "metricsUsed": [
    "apiserver_request_duration_seconds_bucket",
    "container_cpu_usage_seconds_total",
    "scheduler_scheduling_algorithm_duration_seconds_bucket"
    ...
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
    },
    ...
}
```

##### `analyse prometheus`

This command will run against your Prometheus / Cloud Prometheus instance. It will then use the output from `analyse grafana` and `analyse ruler` to show you how many series in the Prometheus server are actually being used in dashboards and rules. Also, it'll show which metrics exist in Grafana Cloud that are **not** in dashboards or rules. The output is a JSON file.

###### Configuration

| Env Variables   | Flag                   | Description                                                                  |
| --------------- | ---------------------- | ---------------------------------------------------------------------------- |
| MIMIR_ADDRESS   | `address`              | Address of the Prometheus instance.                                          |
| MIMIR_TENANT_ID | `id`                   | If you're using Grafana Cloud this is your instance ID.                      |
| MIMIR_API_KEY   | `key`                  | If you're using Grafana Cloud this is your API Key.                          |
| \_\_            | `grafana-metrics-file` | The dashboard metrics input file path. `metrics-in-grafana.json` by default. |
| \_\_            | `ruler-metrics-file`   | The rules metrics input file path. `metrics-in-ruler.json` by default.       |
| \_\_            | `output`               | The output file path. `prometheus-metrics.json` by default.                  |

###### Running the command

```shell
mimirtool analyse prometheus --address=https://prometheus-blocks-prod-us-central1.grafana.net --id=<1234> --key=<API-Key> --log.level=debug
```

###### Sample output

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
    },
    ...
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
    },
    ...
```

##### `analyse dashboard`

This command accepts Grafana dashboard JSON files as input and extracts Prometheus metrics used in the queries. The output is a JSON file compatible with `analyse prometheus`.

###### Running the command

```shell
mimirtool analyse dashboard ./dashboard_one.json ./dashboard_two.json ...
```

##### `analyse rule-file`

This command accepts Prometheus rule YAML files as input and extracts Prometheus metrics used in the queries. The output is a JSON file compatible with `analyse prometheus`.

###### Running the command

```shell
mimirtool analyse rule-file ./rule_file_one.yaml ./rule_file_two.yaml ...
```

## chunktool

This repo also contains the `chunktool`. A client meant to interact with chunks stored and indexed in cortex backends.

##### Chunk Delete

The delete command currently cleans all index entries pointing to chunks in the specified index. Only bigtable and the v10 schema are currently fully supported. This will not delete the entire index entry, only the corresponding chunk entries within the index row.

##### Chunk Migrate

The migrate command helps with migrating chunks across cortex clusters. It also takes care of setting right index in the new cluster as per the specified schema config.

As of now it only supports `Bigtable` or `GCS` as a source to read chunks from for migration while for writing it supports all the storages that Cortex supports.
More details about it [here](./pkg/chunk/migrate/README.md)

##### Chunk Validate/Clean-Index

The `chunk validate-index` and `chunk clean-index` command allows users to scan their index and chunk backends for invalid entries. The `validate-index` command will find invalid entries and ouput them to a CSV file. The `clean-index` command will take that CSV file as input and delete the invalid entries.

## logtool

A CLI tool to parse Cortex query-frontend logs and formats them for easy analysis.

```
Options:
  -dur duration
        only show queries which took longer than this duration, e.g. -dur 10s
  -query
        show the query
  -utc
        show timestamp in UTC time
```

Feed logs into it using [`logcli`](https://github.com/grafana/loki/blob/master/docs/getting-started/logcli.md) from Loki, [`kubectl`](https://kubernetes.io/docs/reference/kubectl/overview/) for Kubernetes, `cat` from a file, or any other way to get raw logs:

Loki `logcli` example:

```
$ logcli query '{cluster="us-central1", name="query-frontend", namespace="dev"}' --limit=5000 --since=3h --forward -o raw | ./logtool -dur 5s
https://logs-dev-ops-tools1.grafana.net/loki/api/v1/query_range?direction=FORWARD&end=1591119479093405000&limit=5000&query=%7Bcluster%3D%22us-central1%22%2C+name%3D%22query-frontend%22%2C+namespace%3D%22dev%22%7D&start=1591108679093405000
Common labels: {cluster="us-central1", container_name="query-frontend", job="dev/query-frontend", level="debug", name="query-frontend", namespace="dev", pod_template_hash="7cd4bf469d", stream="stderr"}

Timestamp                                TraceID           Length    Duration       Status  Path
2020-06-02 10:38:40.34205349 -0400 EDT   1f2533b40f7711d3  12h0m0s   21.92465802s   (200)   /api/prom/api/v1/query_range
2020-06-02 10:40:25.171649132 -0400 EDT  2ac59421db0000d8  168h0m0s  16.378698276s  (200)   /api/prom/api/v1/query_range
2020-06-02 10:40:29.698167258 -0400 EDT  3fd088d900160ba8  168h0m0s  20.912864541s  (200)   /api/prom/api/v1/query_range
```

```
$ cat query-frontend-logs.log | ./logtool -dur 5s
Timestamp                                TraceID           Length    Duration       Status  Path
2020-05-26 13:51:15.0577354 -0400 EDT    76b9939fd5c78b8f  6h0m0s    10.249149614s  (200)   /api/prom/api/v1/query_range
2020-05-26 13:52:15.771988849 -0400 EDT  2e7473ab10160630  10h33m0s  7.472855362s   (200)   /api/prom/api/v1/query_range
2020-05-26 13:53:46.712563497 -0400 EDT  761f3221dcdd85de  10h33m0s  11.874296689s  (200)   /api/prom/api/v1/query_range
```

## benchtool

A tool for benchmarking a Prometheus remote-write backend and PromQL compatible
API. It allows for metrics to be generated using a [workload
file](docs/benchtool.md).

### License

Licensed Apache 2.0, see [LICENSE](LICENSE).
