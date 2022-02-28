---
title: "Distributor"
description: "Overview of the distributor microservice."
weight: 10
---

# Distributor

The **distributor** component is responsible for receiving incoming series from Prometheus.
Incoming data is validated for correctness and to ensure that it is within the configured limits for the given tenant.
Valid data is then split into batches and sent to multiple [ingesters]({{<relref "./ingester.md">}}) in parallel, sharding series among ingesters and replicating each series by the configured replication factor (three by default).

The distributor is **stateless**.

## Validation

The distributor validates received data before writing it to ingesters.
Given a single request can contain both valid and invalid data, invalid metrics, samples, metadata and exemplars are skipped, while valid data is passed on to ingesters.
If the request contains any invalid data, distributor will return a 400 HTTP status code and the response body will include details.
These details about the first invalid data are typically logged by the sender, for example Prometheus or Grafana Agent.
The valid data in the request is passed on to ingesters, while only invalid data is skipped.

The distributor validation includes the following checks:

- The metric metadata and labels conform to the [Prometheus exposition format](https://prometheus.io/docs/concepts/data_model/).
- The metric metadata (name, help and unit) is not longer than `-validation.max-metadata-length`.
- The number of labels of each metric is not higher than `-validation.max-label-names-per-series`.
- Each metric label name is not longer than `-validation.max-length-label-name`.
- Each metric label value is not longer than `-validation.max-length-label-value`.
- Each sample timestamp is not newer than `-validation.create-grace-period`.
- Each exemplar has a timestamp and at least one non empty label name and value pair.
- Each exemplar has no more than 128 labels.

_The limits can be overridden on a per-tenant basis in the overrides section of the runtime configuration._

## Rate limiting

The distributor includes a built-in rate limiter, which will drop whole requests and send back an HTTP 429 error if the rate goes over a maximum number of samples per second.
The rate limit applies on a per-tenant basis.

The configured limit is the tenant's max ingestion rate across the whole Grafana Mimir cluster.
Internally, the limit is implemented configuring a per-distributor local rate limiter set as `ingestion rate limit / N`, where `N` is the number of healthy distributor replicas, and it's automatically adjusted if the number of replicas change.
For this reason, the ingestion rate limit requires that write requests are [evenly distributed across the pool of distributors](#load-balancing-across-distributors).

The rate limit can be configured via the following flags:

- `-distributor.ingestion-rate-limit`: Per-tenant ingestion rate limit in samples per second.
- `-distributor.ingestion-burst-size`: Per-tenant allowed ingestion burst size (in number of samples).

_The rate limiting can be overridden on a per-tenant basis by setting `ingestion_rate` and `ingestion_burst_size` in the overrides section of the runtime configuration._

_Prometheus remote write doesn't retry requests on 429 HTTP response status code by default. This behaviour can be modified setting `retry_on_http_429: true` in the Prometheus [`remote_write` config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)._

### Configuration

The distributors form a [hash ring]({{<relref "./about-the-hash-ring.md">}}) (called the distributors’ ring) to discover each other and enforce limits correctly.

The default configuration uses `memberlist` as the backend for the distributors’ ring.
To configure a different backend, such as Consul or etcd, the following CLI flags (and their respective YAML configuration options) configure the key-value store of the distributors’ ring:

- `-distributor.ring.store`: The backend storage to use.
- `-distributor.ring.consul.*`: The Consul client configuration. Only use this if you have defined `consul` as your backend storage.
- `-distributor.ring.etcd.*`: The etcd client configuration. Only use this if you have defined `etcd` as your backend storage.

## High-availability tracker

Remote write senders, such as Prometheus, may be configured in pairs, so that metrics are still scraped and written to Grafana Mimir when one of them is shut down for maintenance or unavailable due to a failure.
We refer to this configuration has high-availability (HA) pairs.

The distributor includes a High Availability (HA) Tracker.
When enabled, the distributor deduplicates incoming series from Prometheus HA pairs.
This allows you to have multiple HA replicas of the same Prometheus servers, writing the same series to Mimir and then deduplicate these series in the Mimir distributor.

For further information on how it works and how to configure it, see the [configure HA deduplication]({{<relref "../operating-grafana-mimir/configure-ha-deduplication.md">}}) guide.

## Sharding and replication

The distributor shards and replicates incoming series among ingesters.
The number of ingester replicas each series is written to can be configured via `-ingester.ring.replication-factor` (three by default).

Sharding and replication is built on top of ingesters hash ring.
For each incoming series, the distributor computes an hash using the metric name, labels and tenant ID.
The resulting hashing value, called _token_, is looked up in the hash ring to find out to which ingesters it should be written to.

Grafana Mimir uses [Dynamo-style](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) quorum consistency on reads and writes.
The distributor will wait for a positive response from at least one half plus one of the ingesters before successfully responding to the Prometheus write request.

For more information, see [hash ring]({{<relref "./about-the-hash-ring.md">}}).

## Load balancing across distributors

We recommend randomly load balancing write requests across distributor instances.

If you're running Grafana Mimir in a Kubernetes cluster, you could define a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/) as ingress for the distributors.
Be aware that a Kubernetes Service balances TCP connections, and not the HTTP requests within a single TCP connection when HTTP keep-alive is enabled.
Since a Prometheus server establishes a TCP connection for each remote write shard, you should consider increasing `min_shards` in the Prometheus [remote write config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) if distributors traffic is not evenly balanced.
