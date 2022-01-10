---
title: "Mimir technical documentation"
linkTitle: "Documentation"
weight: 1
menu:
  main:
    weight: 1
---

<!-- [![CI](https://github.com/cortexproject/cortex/workflows/ci/badge.svg)](https://github.com/cortexproject/cortex/actions)
[![GoDoc](https://godoc.org/github.com/cortexproject/cortex?status.svg)](https://godoc.org/github.com/cortexproject/cortex)
<a href="https://goreportcard.com/report/github.com/cortexproject/cortex"><img src="https://goreportcard.com/badge/github.com/cortexproject/cortex" alt="Go Report Card" /></a>
<a href="https://cloud-native.slack.com/messages/cortex/"><img src="https://img.shields.io/badge/join%20slack-%23cortex-brightgreen.svg" alt="Slack" /></a>
-->

Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for
[Prometheus](https://prometheus.io).

- **Horizontally scalable:** Mimir can run across multiple machines in a cluster, exceeding the throughput and storage of a single machine. This enables you to send the metrics from multiple Prometheus servers to a single Mimir cluster and run "globally aggregated" queries across all data in a single place.
- **Highly available:** When run in a cluster, Mimir can replicate data between machines. This allows you to survive machine failure without gaps in your graphs.
- **Multi-tenant:** Mimir can isolate data and queries from multiple different independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Long term storage:** Mimir supports S3, GCS, Swift and Microsoft Azure for long term storage of metric data. This allows you to durably store data for longer than the lifetime of any single machine, and use this data for long term capacity planning.

Mimir is a [CNCF](https://cncf.io) incubation project used in several production systems including [Weave Cloud](https://cloud.weave.works) and [Grafana Cloud](https://grafana.com/cloud).
Mimir is primarily used as a [remote write](https://prometheus.io/docs/operating/configuration/#remote_write) destination for Prometheus, exposing a Prometheus-compatible query API.

## Documentation

If you’re new to Mimir, read the [Getting started guide](getting-started/_index.md).

Before deploying Mimir with a permanent storage backend, read:

1. [An overview of Mimir’s architecture](architecture.md)
1. [Getting started with Mimir](getting-started/_index.md)
1. [Configuring Mimir](configuration/_index.md)

There are also individual [guides](guides/_index.md) to many tasks.
Before deploying, review the important [security advice](guides/security.md).

## Contributing

To contribute to Mimir, see the [contributor guidelines](contributing/).

## <a name="help"></a>Getting help

If you have any questions about Mimir:

- Ask a question on the [Mimir Slack channel](https://cloud-native.slack.com/messages/mimir/). To invite yourself to the CNCF Slack, visit http://slack.cncf.io/.
- <a href="https://github.com/cortexproject/cortex/issues/new">File an issue.</a>
- Send an email to <a href="mailto:cortex-users@lists.cncf.io">cortex-users@lists.cncf.io</a>

Your feedback is always welcome.

## Hosted Mimir (Prometheus as a service)

There are several commercial services where you can use Mimir on-demand:

### Weave Cloud

[Weave Cloud](https://cloud.weave.works) from
[Weaveworks](https://weave.works) lets you deploy, manage, and monitor
container-based applications. Sign up at https://cloud.weave.works
and follow the instructions there. Additional help can also be found
in the [Weave Cloud documentation](https://www.weave.works/docs/cloud/latest/overview/).

[Instrumenting Your App: Best Practices](https://www.weave.works/docs/cloud/latest/tasks/monitor/best-instrumenting/)

### Grafana Cloud

As the creators of [Grafana](https://grafana.com/oss/grafana/), [Loki](https://grafana.com/oss/loki/), and [Tempo](https://grafana.com/oss/tempo/), Grafana Labs can offer you the most wholistic Observability-as-a-Service stack out there.

For more information, see Grafana Cloud [documentation](https://grafana.com/docs/grafana-cloud/), [tutorials](https://grafana.com/tutorials/), [webinars](https://grafana.com/videos/), and [KubeCon talks](https://grafana.com/categories/cortex/). Get started today and [sign up here](https://grafana.com/products/cloud/).

### Amazon Managed Service for Prometheus (AMP)

[Amazon Managed Service for Prometheus (AMP)](https://aws.amazon.com/prometheus/) is a Prometheus-compatible monitoring service that makes it easy to monitor containerized applications at scale. It is a highly available, secure, and managed monitoring for your containers. Get started [here](https://console.aws.amazon.com/prometheus/home). To learn more about the AMP, reference our [documentation](https://docs.aws.amazon.com/prometheus/latest/userguide/what-is-Amazon-Managed-Service-Prometheus.html) and [Getting Started with AMP blog](https://aws.amazon.com/blogs/mt/getting-started-amazon-managed-service-for-prometheus/).
