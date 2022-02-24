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

## High Availability Tracker

Remote write senders, such as Prometheus, may be configured in pairs, so that metrics are still scraped and written to Grafana Mimir when one of them is shut down for maintenance or unavailable due to a failure.
We refer to this configuration has high-availability (HA) pairs.

The distributor includes a High Availability (HA) Tracker.
When enabled, the distributor deduplicates incoming series from Prometheus HA pairs.
This allows you to have multiple HA replicas of the same Prometheus servers, writing the same series to Mimir and then deduplicate these series in the Mimir distributor.

For further information on how it works and how to configure it, see the [configure HA deduplication]({{<relref "../operating-grafana-mimir/configure-ha-deduplication.md">}}) guide.

## Hashing

Distributors use consistent hashing, in conjunction with a configurable replication factor, to determine which ingesters should receive a given series.

The hash is calculated using the metric name, labels and tenant ID.

### The ingesters hash ring

A hash ring (stored in a key-value store) is used to achieve consistent hashing for the series sharding and replication across the ingesters. All [ingesters]({{<relref "./ingester.md">}}) register themselves into the hash ring with a set of tokens they own; each token is a random unsigned 32-bit integer. Each incoming series is [hashed](#hashing) in the distributor and then pushed to the ingester which owns the token range for the series hash number plus N-1 subsequent ingesters in the ring, where N is the replication factor.

To do the hash lookup, distributors find the smallest appropriate token whose value is larger than the [hash of the series](#hashing). When the replication factor is larger than 1, the subsequent tokens (clockwise in the ring) that belong to different ingesters will also be included in the result.

The effect of this hash set up is that each token that an ingester owns is responsible for a range of hashes. If there are three tokens with values 0, 25, and 50, then a hash of 3 would be given to the ingester that owns the token 25; the ingester owning token 25 is responsible for the hash range of 1-25.

The supported KV stores for the hash ring are:

- [Consul](https://www.consul.io)
- [Etcd](https://etcd.io)
- Gossip [memberlist](https://github.com/hashicorp/memberlist)

#### Quorum consistency

Since all distributors share access to the same hash ring, write requests can be sent to any distributor and you can setup a stateless load balancer in front of it.

To ensure consistent query results, Mimir uses [Dynamo-style](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) quorum consistency on reads and writes. This means that the distributor will wait for a positive response of at least one half plus one of the ingesters to send the sample to before successfully responding to the Prometheus write request.

## Load balancing across distributors

We recommend randomly load balancing write requests across distributor instances.

If you're running Grafana Mimir in a Kubernetes cluster, you could define a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/) as ingress for the distributors.
Be aware that a Kubernetes Service balances TCP connections, and not the HTTP requests within a single TCP connection when HTTP keep-alive is enabled.
Since a Prometheus server establishes a TCP connection for each remote write shard, you should consider increasing `min_shards` in the Prometheus [remote write config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) if distributors traffic is not evenly balanced.

## Configuration

The distributors need to form an hash ring (called the distributors ring) in order to discover each other and enforce limits correctly.

The default configuration uses `memberlist` as backend for the distributors ring.
In case you want to configure a different backend (eg. `consul` or `etcd`), the following CLI flags (and their respective YAML config options) are available to configure the distributors ring KV store:

- `-distributor.ring.store`: The backend storage to use.
- `-distributor.ring.consul.*`: The Consul client configuration (should be used only if `consul` is the configured backend storage).
- `-distributor.ring.etcd.*`: The etcd client configuration (should be used only if `etcd` is the configured backend storage).
