---
title: "Distributor"
description: "Overview of the distributor microservice."
weight: 10
---

# Distributor

The distributor is a stateless component that receives time-series data from Prometheus.
The distributor validates the data for correctness to ensure that it is within the configured limits for a given tenant.
The distributor then divides the data into batches and sends it to multiple [ingesters]({{< relref "./ingester.md" >}}) in parallel, shards the series among ingesters, and replicates each series by the configured replication factor. By default, the configured replication factor is three.

## Validation

The distributor validates received data before writing it to ingesters.
Because a single request can contain valid and invalid data, metrics, samples, metadata, and exemplars, the distributor only passes valid data to the ingesters. The distributor does not include invalid data in its requests to the ingesters.
If the request contains invalid data, the distributor returns a 400 HTTP status code and the details appear in the response body.
The details about the first invalid data are typically logged by the sender, for example, Prometheus or a Grafana Agent.

The distributor validation includes the following checks:

- The metric metadata and labels conform to the [Prometheus exposition format](https://prometheus.io/docs/concepts/data_model/).
- The metric metadata (name, help, and unit) is not longer than `-validation.max-metadata-length`.
- The number of labels of each metric is not higher than `-validation.max-label-names-per-series`.
- Each metric label name is not longer than `-validation.max-length-label-name`.
- Each metric label value is not longer than `-validation.max-length-label-value`.
- Each sample timestamp is not newer than `-validation.create-grace-period`.
- Each exemplar has a timestamp and at least one non-empty label name and value pair.
- Each exemplar has no more than 128 labels.

_For each tenant, you can override the validation checks by modifying the overrides section of the runtime configuration._

## Rate limiting

The distributor includes a built-in rate limiter that it applies to each tenant.
The rate limit is the maximum ingestion rate for each tenant across the Grafana Mimir cluster.
If the rate exceeds the maximum number of samples per second, the distributor drops the request and displays an HTTP 429 error.
To configure the local rate limiter for each distributor, use `ingestion rate limit / N`, where `N` is the number of healthy distributor replicas.
The distributor automatically adjusts the ingester rate limit if the number of replicas change.
Because the rate limit automatically adjusts, the ingestion rate limit requires that write requests are [evenly distributed across the pool of distributors](#load-balancing-across-distributors).

Use the following flags to configure the rate limit:

- `-distributor.ingestion-rate-limit`: Per tenant ingestion rate limit in samples per second.
- `-distributor.ingestion-burst-size`: Per tenant allowed ingestion burst size (in number of samples).

_You can override rate limiting on a per tenant basis by setting `ingestion_rate` and `ingestion_burst_size` in the overrides section of the runtime configuration._

_By default, Prometheus remote write doesn't retry requests on 429 HTTP response status code. You can modify this behavior by setting `retry_on_http_429: true` in the Prometheus [`remote_write` config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)._

### Configuration

The distributors form a [hash ring]({{<relref "./about-the-hash-ring.md">}}) (called the distributors’ ring) to discover each other and enforce limits correctly.

The default configuration uses `memberlist` as the backend for the distributors’ ring.
To configure a different backend, such as Consul or etcd, the following CLI flags (and their respective YAML configuration options) configure the key-value store of the distributors’ ring:

- `-distributor.ring.store`: The backend storage to use.
- `-distributor.ring.consul.*`: The Consul client configuration. Only use this if you have defined `consul` as your backend storage.
- `-distributor.ring.etcd.*`: The etcd client configuration. Only use this if you have defined `etcd` as your backend storage.

## High-availability tracker

Remote write senders, such as Prometheus, can be configured in pairs, which means that metrics continue to be scraped and written to Grafana Mimir even when one of the remote write senders is down for maintenance or is unavailable due to a failure.
We refer to this configuration as high-availability (HA) pairs.

The distributor includes an HA Tracker.
When the HA Tracker is enabled, the distributor deduplicates incoming series from Prometheus HA pairs.
This enables you to have multiple HA replicas of the same Prometheus servers that write the same series to Mimir and then deduplicates the series in the Mimir distributor.

For more information about HA deduplication and how to configure it, refer to [configure HA deduplication]({{< relref "../operating-grafana-mimir/configure-ha-deduplication.md" >}}).

## Sharding and replication

The distributor shards and replicates incoming series across ingesters.
You can configure the number of ingester replicas that each series is written to via the `-ingester.ring.replication-factor` flag, which is `3` by default.
Distributors use consistent hashing, in conjunction with a configurable replication factor, to determine which ingesters receive a given series.

Sharding and replication uses the ingesters' hash ring.
For each incoming series, the distributor computes a hash using the metric name, labels, and tenant ID.
The computed hash is called a _token_.
The distributor looks up the token in the hash ring to determine which ingesters to write a series to.

For more information, see [hash ring]({{<relref "./about-the-hash-ring.md">}}).

#### Quorum consistency

Because distributors share access to the same hash ring, write requests can be sent to any distributor. You can also set up a stateless load balancer in front of it.

To ensure consistent query results, Mimir uses [Dynamo-style](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) quorum consistency on reads and writes. This means that the distributor waits for a positive response from at least one-half plus one of the ingesters to send the sample to before successfully responding to the Prometheus write request.

## Load balancing across distributors

We recommend randomly load balancing write requests across distributor instances.
If you're running Grafana Mimir in a Kubernetes cluster, you can define a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/) as ingress for the distributors.

> **Note**: Be aware that when you enable HTTP keep-alive, a Kubernetes Service balances TCP connections, and not the HTTP requests within a single TCP connection.
> Because a Prometheus server establishes a TCP connection for each remote write shard, and if distributors traffic is not evenly balanced, consider increasing `min_shards` in the Prometheus [remote write config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

## Configuration

The distributors must form a hash ring (also called the _distributors ring_) so that they can discover each other and correctly enforce limits.

The default configuration uses `memberlist` as backend for the distributors ring.
If you want to configure a different backend, for example, `consul` or `etcd`, you can use the following CLI flags (and their respective YAML configuration options) to configure the distributors ring KV store:

- `-distributor.ring.store`: The backend storage to use.
- `-distributor.ring.consul.*`: The Consul client configuration. Set this flag only if `consul` is the configured backend storage.
- `-distributor.ring.etcd.*`: The etcd client configuration. Set this flag only if `etcd` is the configured backend storage.
