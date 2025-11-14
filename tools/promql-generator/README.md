# PromQL Generator

This is a utility to assist in generating random PromQL queries.

It is based on the demo available in the [https://github.com/cortexproject/promqlsmith](https://github.com/cortexproject/promqlsmith) project.

This utility reads a list of label sets from a json file and generates a set of random PromQL queries to stdout.

A sample json file is provided in this directory.

```
Usage:

promql-generator -labels.file=<labels_file.json> [-promql.count=<int>]

promql-generator --help

```
