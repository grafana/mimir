# PromQL Generator

This is a utility to assist in generating random PromQL queries.

It is based on the demo available in the [https://github.com/cortexproject/promqlsmith](https://github.com/cortexproject/promqlsmith) project.

This utility can either;

* read a list of label sets from a json file
* read a list of label sets from a Prometheus server endpoint, restricted to a given matcher

A sample json file is provided in this directory.

The command line options for this tool include;

* `-series.source=file|prometheus` - where we will read label sets from
* `-promql.count=<int>` - the number of queries will be output
* `-labels.file=<filename>` - the json file we will be reading label sets from
* `-prometheus.address=<http://endpoint:port/prometheus>` - the Prometheus server endpoint we will read label sets from
* `-prometheus.matcher=<matcher>` - the matcher we use to limit the label sets we read from Prometheus

For the given label sets the tool will randomly generate PromQL queries and output to stdout. 
