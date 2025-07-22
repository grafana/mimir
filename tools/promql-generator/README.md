# PromQL Generator

This is a utility to assist in generating random PromQL queries.

It is based on the demo available in the [https://github.com/cortexproject/promqlsmith](https://github.com/cortexproject/promqlsmith) project.

This utility can either:

- read a list of label sets from a json file
- read a list of label sets from a Prometheus server endpoint, restricted to a given matcher

A sample json file is provided in this directory.

```
Usage:

promql-generator -series.source=file -labels.file=<labels_file.js> [-promql.count=<int>]

or

promql-generator -series.source=prometheus -prometheus.address=<url> -prometheus.matcher=<matcher> [-promql.count=<int>]
```

The flag variables are;

- labels.file=<labels_file.js> (required for -series.source=file, default=sample_series.js) - a file which contains a json representation of 1 or more label sets. An example file is provided.
- prometheus.address=<url> (required for -series.source=prometheus, default=http://127.0.0.1:9009/prometheus) - a fully qualified URL to a Prometheus server.
- prometheus.matcher=<matcher> (required for -series.source=prometheus, default={job="prometheus"}) - a matcher to limit the label sets discovered from the Prometheus server.
- promql.count=<int> (optional default=100) - the number of queries generated. 0 < count < 10000

For the given label sets the tool will randomly generate PromQL queries and output to stdout.
