---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
no_section_index_title: true
slug: "getting-started"
---

Mimir can be run as a single binary or as multiple independent microservices.
The single-binary mode is easier to deploy and is aimed mainly at users wanting to try out Mimir or develop on it.
The microservices mode is intended for production usage, as it allows you to independently scale different services and isolate failures.

This document will focus on single-binary Mimir with the blocks storage.
See [the architecture doc](../architecture.md) for more information about the microservices and [blocks operation](../blocks-storage/_index.md)
for more information about the blocks storage.

Separately from single-binary vs microservices decision, Mimir can be configured to use local storage or cloud storage (S3, GCS and Azure).
Mimir can also make use of external Memcacheds and Redis for caching.

## Single instance running in single-binary mode

For simplicity and to get started, we'll run it as a [single process](../configuration/single-process-config-blocks.yaml) with no dependencies.
You can reconfigure the config to use GCS, Azure storage or local storage as shown in the file's comments.

> **Note:** The `filesystem` backend only works for a single instance running in
> single-binary mode. Highly available deployments (both single-binary and
> microservices modes) must use an external object store.

```sh
$ go build ./cmd/mimir
$ ./mimir -config.file=./docs/sources/configuration/single-process-config-blocks.yaml
```

Unless reconfigured this starts a single Mimir node storing blocks to S3 in bucket `mimir`.
It is not intended for production use.

Clone and build prometheus

```sh
$ git clone https://github.com/prometheus/prometheus
$ cd prometheus
$ go build ./cmd/prometheus
```

Add the following to your Prometheus config (documentation/examples/prometheus.yml in Prometheus repo):

```yaml
remote_write:
  - url: http://localhost:9009/api/v1/push
```

And start Prometheus with that config file:

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

Your Prometheus instance will now start pushing data to Mimir. To query that data, start a Grafana instance:

```sh
$ docker run --rm -d --name=grafana -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Mimir (`http://host.docker.internal:9009/prometheus`).

**To clean up:** press CTRL-C in both terminals (for Mimir and Prometheus).

## Horizontally scale out

Next we're going to show how you can run a scale out Mimir cluster using Docker. We'll need:

- A built Mimir image.
- A Docker network to put these containers on so they can resolve each other by name.
- A single node Consul instance to coordinate the Mimir cluster.

> **Note:** In order to horizontally scale mimir, you must use an external
> object store. See the [blocks storage documentation]({{< relref "../blocks-storage/_index.md" >}})
> for information on supported backends.

```sh
$ make ./cmd/mimir/.uptodate
$ docker network create mimir
$ docker run -d --name=consul --network=mimir -e CONSUL_BIND_INTERFACE=eth0 consul
```

Next we'll run a couple of Mimir instances pointed at that Consul. You'll note the Mimir configuration can be specified in either a config file or overridden on the command line. See [the arguments documentation](../configuration/arguments.md) for more information about Mimir configuration options.

```sh
$ docker run -d --name=mimir1 --network=mimir \
    -v $(pwd)/docs/sources/configuration/single-process-config-blocks.yaml:/etc/single-process-config-blocks.yaml \
    -p 9001:9009 \
    us.gcr.io/kubernetes-dev/mimir \
    -config.file=/etc/single-process-config-blocks.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500
$ docker run -d --name=mimir2 --network=mimir \
    -v $(pwd)/docs/sources/configuration/single-process-config-blocks.yaml:/etc/single-process-config-blocks.yaml \
    -p 9002:9009 \
    us.gcr.io/kubernetes-dev/mimir \
    -config.file=/etc/single-process-config-blocks.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500
```

If you go to http://localhost:9001/ring (or http://localhost:9002/ring) you should see both Mimir nodes join the ring.

To demonstrate the correct operation of Mimir clustering, we'll send samples
to one of the instances and queries to another. In production, you'd want to
load balance both pushes and queries evenly among all the nodes.

Point Prometheus at the first:

```yaml
remote_write:
  - url: http://localhost:9001/api/v1/push
```

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

And Grafana at the second:

```sh
$ docker run -d --name=grafana --network=mimir -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Mimir (`http://mimir2:9009/prometheus`).

**To clean up:** CTRL-C the Prometheus process and run:

```
$ docker rm -f mimir1 mimir2 consul grafana
$ docker network remove mimir
```

## High availability with replication

In this last demo we'll show how Mimir can replicate data among three nodes,
and demonstrate Mimir can tolerate a node failure without affecting reads and writes.

First, create a network and run a new Consul and Grafana:

```sh
$ docker network create mimir
$ docker run -d --name=consul --network=mimir -e CONSUL_BIND_INTERFACE=eth0 consul
$ docker run -d --name=grafana --network=mimir -p 3000:3000 grafana/grafana
```

Then, launch 3 Mimir nodes with replication factor 3:

```sh
$ docker run -d --name=mimir1 --network=mimir \
    -v $(pwd)/docs/sources/configuration/single-process-config-blocks.yaml:/etc/single-process-config-blocks.yaml \
    -p 9001:9009 \
    us.gcr.io/kubernetes-dev/mimir \
    -config.file=/etc/single-process-config-blocks.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
$ docker run -d --name=mimir2 --network=mimir \
    -v $(pwd)/docs/sources/configuration/single-process-config-blocks.yaml:/etc/single-process-config-blocks.yaml \
    -p 9002:9009 \
    us.gcr.io/kubernetes-dev/mimir \
    -config.file=/etc/single-process-config-blocks.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
$ docker run -d --name=mimir3 --network=mimir \
    -v $(pwd)/docs/sources/configuration/single-process-config-blocks.yaml:/etc/single-process-config-blocks.yaml \
    -p 9003:9009 \
    us.gcr.io/kubernetes-dev/mimir \
    -config.file=/etc/single-process-config-blocks.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
```

Configure Prometheus to send data to the first replica:

```yaml
remote_write:
  - url: http://localhost:9001/api/v1/push
```

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

In Grafana, add a datasource for the 3rd Mimir replica (`http://mimir3:9009/prometheus`)
and verify the same data appears in both Prometheus and Mimir.

To show that Mimir can tolerate a node failure, hard kill one of the Mimir replicas:

```
$ docker rm -f mimir2
```

You should see writes and queries continue to work without error.

**To clean up:** CTRL-C the Prometheus process and run:

```
$ docker rm -f mimir1 mimir2 mimir3 consul grafana
$ docker network remove mimir
```
