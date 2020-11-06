# Cortex Tools

This repo contains tools used for interacting with [Cortex](https://github.com/cortexproject/cortex).

* [cortextool](#cortextool): Interacts with user-facing Cortex APIs and backend storage components
* [chunktool](#chunktool): Interacts with chunks stored and indexed in Cortex storage backends.
* [logtool](#logtool): Tool which parses Cortex query-frontend logs and formats them for easy analysis.
* [e2ealerting](docs/e2ealerting.md): Tool that helps measure how long an alerts takes from scrape of sample to Alertmanager notifcation delivery.


# Installation

The various binaries are available for macOS, Windows, and Linux.

## macOS

`cortextool` is available on macOS via [Homebrew](https://brew.sh/):

```bash
$ brew install grafana/grafana/cortextool
```

## Linux, Docker and Windows

Refer to the [latest release](https://github.com/grafana/cortex-tools/releases) for installation intructions on these.

## cortextool

This tool is designed to interact with the various user-facing APIs provided by Cortex, as well as, interact with various backend storage components containing Cortex data.

### Config Commands

Config commands interact with the Cortex api and read/create/update/delete user configs from Cortex. Specifically a users alertmanager and rule configs can be composed and updated using these commands.

#### Configuration

| Env Variables     | Flag      | Description                                                                                                   |
| ----------------- | --------- | ------------------------------------------------------------------------------------------------------------- |
| CORTEX_ADDRESS    | `address` | Addess of the API of the desired Cortex cluster.                                                              |
| CORTEX_API_KEY    | `key`     | In cases where the Cortex API is set behind a basic auth gateway, an key can be set as a basic auth password. |
| CORTEX_TENANT_ID | `id`      | The tenant ID of the Cortex instance to interact with.                                                        |

#### Alertmanager

The following commands are used by users to interact with their Cortex alertmanager configuration, as well as their alert template files.

##### Alertmanager Get

    cortextool alertmanager get

##### Alertmanager Load

    cortextool alertmanager load ./example_alertmanager_config.yaml

#### Rules

The following commands are used by users to interact with their Cortex ruler configuration. They can load prometheus rule files, as well as interact with individual rule groups.

##### Rules List

This command will retrieve all of the rule groups stored in the specified Cortex instance and print each one by rule group name and namespace to the terminal.

    cortextool rules list

##### Rules Print

This command will retrieve all of the rule groups stored in the specified Cortex instance and print them to the terminal.

    cortextool rules print

##### Rules Get

This command will retrieve the specified rule group from Cortex and print it to the terminal.

    cortextool rules get example_namespace example_rule_group

##### Rules Delete

This command will retrieve the specified rule group from Cortex and print it to the terminal.

    cortextool rules delete example_namespace example_rule_group

##### Rules Load

This command will load each rule group in the specified files and load them into Cortex. If a rule already exists in Cortex it will be overwritten if a diff is found.

    cortextool rules load ./example_rules_one.yaml ./example_rules_two.yaml  ...

#### Rules Lint

This command lints a rules file. The linter's aim is not to verify correctness but just YAML and PromQL expression formatting within the rule file. This command always edits in place, you can use the dry run flag (`-n`) if you'd like to perform a trial run that does not make any changes.

    cortextool rules lint -n ./example_rules_one.yaml ./example_rules_two.yaml ...

#### Rules Prepare

This command prepares a rules file for upload to Cortex. It lints all your PromQL expressions and adds an specific label to your PromQL query aggregations in the file. Unlike, the previous command this one does not interact with your Cortex cluster.

    cortextool rules prepare -i ./example_rules_one.yaml ./example_rules_two.yaml ...

There are two flags of note for this command:
- `-i` which allows you to edit in place, otherwise a a new file with a `.output` extension is created with the results of the run.
- `-l` which allows you specify the label you want you add for your aggregations, it is `cluster` by default.

At the end of the run, the command tells you whenever the operation was a success in the form of

    INFO[0000] SUCESS: 194 rules found, 0 modified expressions

It is important to note that a modification can be a PromQL expression lint or a label add to your aggregation.

#### Rules Check

This commands checks rules against the recommended [best practices](https://prometheus.io/docs/practices/rules/) for rules.

    cortextool rules check ./example_rules_one.yaml


#### Overrides Exporter

The Overrides Exporter allows to continuously export [per tenant configuration overrides][runtime-config] as metrics. Optionally it can also export a presets file (cf. example [override config file] and [presets file]).

    cortextool overrides-exporter --overrides-file overrides.yaml --presets-file presets.yaml

[override config file]:./pkg/commands/testdata/overrides.yaml
[presets file]:./pkg/commands/testdata/presets.yaml
[runtime-config]:https://cortexmetrics.io/docs/configuration/arguments/#runtime-configuration-file

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


### License

Licensed Apache 2.0, see [LICENSE](LICENSE).
