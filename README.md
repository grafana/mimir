# Grafana Mimir

<p align="center"><img src="images/logo.png" alt="Grafana Mimir logo"></p>

Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for [Prometheus](https://prometheus.io).

- **Horizontally scalable:** Grafana Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Mimir cluster and run "globally aggregated" queries across all data in a single place.
- **Highly available:** When run in a cluster, Grafana Mimir can replicate data between machines. This allows you to survive machine failure without gaps in your graphs.
- **Multi-tenant:** Grafana Mimir can isolate data and queries from multiple different independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long term storage:** Grafana Mimir supports S3, GCS, Swift and Microsoft Azure for long term storage of metric data. This allows you to durably store data for longer than the lifetime of any single machine, and use this data for long term capacity planning.

## Getting started

If you’re new to Grafana Mimir, read the [Getting started guide]({{< relref "./docs/sources/getting-started/_index.md" >}}).

Before deploying Grafana Mimir with a permanent storage backend, read:

1. [An overview of Grafana Mimir’s architecture]({{< relref "./docs/sources/architecture/_index.md" >}})
1. [Configuring Grafana Mimir]({{< relref "./docs/sources/configuring/_index.md" >}})

## Contributing

To contribute to Grafana Mimir, see [Contributing to Grafana Mimir](./CONTRIBUTING.md).

## Hosted Grafana Mimir (Prometheus as a service)

Grafana Mimir is used in [Grafana Cloud](https://grafana.com/cloud), and is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus via a Prometheus-compatible query API.

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Loki](https://grafana.com/oss/loki/), and [Tempo](https://grafana.com/oss/tempo/), Grafana Labs offers you the most comprehensive Observability-as-a-Service stack available.





+++++++++++++++++++++++++++++++++++++++++++++
# Loki: like Prometheus, but for logs.


## Getting started

* [Installing Loki](https://grafana.com/docs/loki/latest/installation/)
* [Installing Promtail](https://grafana.com/docs/loki/latest/clients/promtail/installation/)
* [Getting Started](https://grafana.com/docs/loki/latest/getting-started/)

## Upgrading

* [Upgrading Loki](https://grafana.com/docs/loki/latest/upgrading/)

## Documentation

* [Latest release](https://grafana.com/docs/loki/latest/)
* [Upcoming release](https://grafana.com/docs/loki/next/), at the tip of the main branch

Commonly used sections:

- [API documentation](https://grafana.com/docs/loki/latest/api/) for getting logs into Loki.
- [Labels](https://grafana.com/docs/loki/latest/getting-started/labels/)
- [Operations](https://grafana.com/docs/loki/latest/operations/)
- [Promtail](https://grafana.com/docs/loki/latest/clients/promtail/) is an agent which tails log files and pushes them to Loki.
- [Pipelines](https://grafana.com/docs/loki/latest/clients/promtail/pipelines/) details the log processing pipeline.
- [Docker Driver Client](https://grafana.com/docs/loki/latest/clients/docker-driver/) is a Docker plugin to send logs directly to Loki from Docker containers.
- [LogCLI](https://grafana.com/docs/loki/latest/getting-started/logcli/) provides a command-line interface for querying logs.
- [Loki Canary](https://grafana.com/docs/loki/latest/operations/loki-canary/) monitors your Loki installation for missing logs.
- [Troubleshooting](https://grafana.com/docs/loki/latest/getting-started/troubleshooting/) presents help dealing with error messages.
- [Loki in Grafana](https://grafana.com/docs/loki/latest/getting-started/grafana/) describes how to set up a Loki datasource in Grafana.

## Getting Help

If you have any questions or feedback regarding Loki:

- Search existing thread in the Grafana Labs community forum for Loki: [https://community.grafana.com](https://community.grafana.com/c/grafana-loki/)
- Ask a question on the Loki Slack channel. To invite yourself to the Grafana Slack, visit [https://slack.grafana.com/](https://slack.grafana.com/) and join the #loki channel.
- [File an issue](https://github.com/grafana/loki/issues/new) for bugs, issues and feature suggestions.
- Send an email to [lokiproject@googlegroups.com](mailto:lokiproject@googlegroups.com), or use the [web interface](https://groups.google.com/forum/#!forum/lokiproject).
- UI issues should be filed directly in [Grafana](https://github.com/grafana/grafana/issues/new).

Your feedback is always welcome.

## Further Reading

- The original [design doc](https://docs.google.com/document/d/11tjK_lvp1-SVsFZjgOTr1vV3-q6vBAsZYIQ5ZeYBkyM/view) for Loki is a good source for discussion of the motivation and design decisions.
- Callum Styan's March 2019 DevOpsDays Vancouver talk "[Grafana Loki: Log Aggregation for Incident Investigations][devopsdays19-talk]".
- Grafana Labs blog post "[How We Designed Loki to Work Easily Both as Microservices and as Monoliths][architecture-blog]".
- Tom Wilkie's early-2019 CNCF Paris/FOSDEM talk "[Grafana Loki: like Prometheus, but for logs][fosdem19-talk]" ([slides][fosdem19-slides], [video][fosdem19-video]).
- David Kaltschmidt's KubeCon 2018 talk "[On the OSS Path to Full Observability with Grafana][kccna18-event]" ([slides][kccna18-slides], [video][kccna18-video]) on how Loki fits into a cloud-native environment.
- Goutham Veeramachaneni's blog post "[Loki: Prometheus-inspired, open source logging for cloud natives](https://grafana.com/blog/2018/12/12/loki-prometheus-inspired-open-source-logging-for-cloud-natives/)" on details of the Loki architecture.
- David Kaltschmidt's blog post "[Closer look at Grafana's user interface for Loki](https://grafana.com/blog/2019/01/02/closer-look-at-grafanas-user-interface-for-loki/)" on the ideas that went into the logging user interface.

[devopsdays19-talk]: https://grafana.com/blog/2019/05/06/how-loki-correlates-metrics-and-logs-and-saves-you-money/
[architecture-blog]: https://grafana.com/blog/2019/04/15/how-we-designed-loki-to-work-easily-both-as-microservices-and-as-monoliths/
[fosdem19-talk]: https://fosdem.org/2019/schedule/event/loki_prometheus_for_logs/
[fosdem19-slides]: https://speakerdeck.com/grafana/grafana-loki-like-prometheus-but-for-logs
[fosdem19-video]: https://mirror.as35701.net/video.fosdem.org/2019/UB2.252A/loki_prometheus_for_logs.mp4
[kccna18-event]: https://kccna18.sched.com/event/GrXC/on-the-oss-path-to-full-observability-with-grafana-david-kaltschmidt-grafana-labs
[kccna18-slides]: https://speakerdeck.com/davkal/on-the-path-to-full-observability-with-oss-and-launch-of-loki
[kccna18-video]: https://www.youtube.com/watch?v=U7C5SpRtK74&list=PLj6h78yzYM2PZf9eA7bhWnIh_mK1vyOfU&index=346

## Contributing

Refer to [CONTRIBUTING.md](CONTRIBUTING.md)

### Building from source

Loki can be run in a single host, no-dependencies mode using the following commands.

You need `go`, we recommend using the version found in [our build Dockerfile](https://github.com/grafana/loki/blob/master/loki-build-image/Dockerfile)

```bash

$ go get github.com/grafana/loki
$ cd $GOPATH/src/github.com/grafana/loki # GOPATH is $HOME/go by default.

$ go build ./cmd/loki
$ ./loki -config.file=./cmd/loki/loki-local-config.yaml
...
```

To build Promtail on non-Linux platforms, use the following command:

```bash
$ go build ./clients/cmd/promtail
```

On Linux, Promtail requires the systemd headers to be installed for
Journal support.

With Journal support on Ubuntu, run with the following commands:

```bash
$ sudo apt install -y libsystemd-dev
$ go build ./clients/cmd/promtail
```

With Journal support on CentOS, run with the following commands:

```bash
$ sudo yum install -y systemd-devel
$ go build ./clients/cmd/promtail
```

Otherwise, to build Promtail without Journal support, run `go build`
with CGO disabled:

```bash
$ CGO_ENABLED=0 go build ./clients/cmd/promtail
```
## Adopters
Please see [ADOPTERS.md](ADOPTERS.md) for some of the organizations using Loki today.
If you would like to add your organization to the list, please open a PR to add it to the list.

## License

Grafana Loki is distributed under [AGPL-3.0-only](LICENSE). For Apache-2.0 exceptions, see [LICENSING.md](LICENSING.md).