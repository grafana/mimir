# Grafana Mimir

<p align="center"><img src="images/logo.png" alt="Grafana Mimir logo"></p>

Grafana Mimir is an open source software project that provides a scalable long-term storage for [Prometheus](https://prometheus.io). Some of the core strengths of Grafana Mimir include:

- **Massive scalability:** You can run Grafana Mimir's horizontally-scalable architecture across multiple machines, resulting in the ability to process orders of magnitude more time series than a single Prometheus instance. Internal testing shows that Grafana Mimir handles up to 1 billion active time series.
- **Global view of metrics:** Grafana Mimir enables you to run queries that aggregate series from multiple Prometheus instances, giving you a global view of your systems. Its query engine extensively parallelizes query execution, so that even the highest-cardinality queries complete with blazing speed.
- **Cheap, durable metric storage:** Grafana Mimir uses object storage for long-term data storage, allowing it to take advantage of this ubiquitous, cost-effective, high-durability technology. It is compatible with multiple object store implementations, including AWS S3, Google Cloud Storage, Azure Blob Storage, OpenStack Swift, as well as any S3-compatible object storage.
- **High availability:** Grafana Mimir replicates incoming metrics, ensuring that no data is lost in the event of machine failure. Its horizontally scalable architecture also means that it can be restarted, upgraded, or downgraded with zero downtime, which means no interruptions to metrics ingestion or querying.
- **Natively multi-tenant:** Grafana Mimir’s multi-tenant architecture enables you to isolate data and queries from independent teams or business units, making it possible for these groups to share the same cluster. Advanced limits and quality-of-service controls ensure that capacity is shared fairly among tenants.

## Migrating to Grafana Mimir

If you're migrating to Grafana Mimir, refer to the following documents:
- [Migrating from Thanos or Prometheus to Grafana Mimir](https://grafana.com/docs/mimir/next/migration-guide/migrating-from-thanos-or-prometheus/).
- [Migrating from Cortex to Grafana Mimir](link TBD)

## Deploying Grafana Mimir

For information about how to deploy Grafana Mimir, refer to [Deploying Grafana Mimir](link TBD).

## Getting started

If you’re new to Grafana Mimir, read the [Getting started guide](https://grafana.com/docs/mimir/latest/operators-guide/getting-started/).

Before deploying Grafana Mimir in production, read:

1. [An overview of Grafana Mimir’s architecture](https://grafana.com/docs/mimir/latest/operators-guide/architecture/)
1. [Configuring Grafana Mimir](https://grafana.com/docs/mimir/latest/operators-guide/configuring/)

## Documentation

* [Latest release](https://grafana.com/docs/mimir/latest/)
* [Upcoming release](https://grafana.com/docs/mimir/next/), at the tip of the main branch

Commonly used sections:

We can add links here to specific topics that we anticipate will be most useful to readers.

- [Topic name](link)
- [Topic name](link)
- [Topic name](link)

## Contributing

To contribute to Grafana Mimir, refer to [Contributing to Grafana Mimir](https://github.com/grafana/mimir/tree/main/docs/internal/contributing).

## Hosted Grafana Mimir (Prometheus as a service)

Grafana Mimir is used in [Grafana Cloud](https://grafana.com/cloud), and is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus via a Prometheus-compatible query API.

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Loki](https://grafana.com/oss/loki/), and [Tempo](https://grafana.com/oss/tempo/), Grafana Labs offers you the most comprehensive Observability-as-a-Service stack available.

## Getting Help

If you have any questions or feedback regarding Grafana Mimir:

- Search existing thread in the Grafana Labs community forum for Loki: [https://community.grafana.com](https://community.grafana.com/c/grafana-loki/)
- Ask a question on the Loki Slack channel. To invite yourself to the Grafana Slack, visit [https://slack.grafana.com/](https://slack.grafana.com/) and join the #loki channel.
- [File an issue](https://github.com/grafana/mimir/issues/new) for bugs, issues and feature suggestions.
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

## Building from source

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

## License

Grafana Mimir is distributed under [AGPL-3.0-only](LICENSE). For Apache-2.0 exceptions, see [LICENSING.md](LICENSING.md).
