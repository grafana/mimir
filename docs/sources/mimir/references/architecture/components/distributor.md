---
aliases:
  - ../../../operators-guide/architecture/components/distributor/
description: The distributor validates time-series data and sends the data to ingesters.
menuTitle: Distributor
title: Grafana Mimir distributor
weight: 20
---

# Grafana Mimir distributor

The distributor is a stateless component that acts as the entry point for the Grafana Mimir write path.
It receives incoming write requests containing time series data, validates the data for correctness, enforces tenant-specific limits, and then ingests the data into Mimir.

To scale beyond the limits of a single node, distributors shard incoming series across a pool of partitions or ingesters.
The details of how sharding, replication, and ingestion work differ between the ingest storage and classic architectures.
For more information, refer to:

- [Series sharding in ingest storage architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/hash-ring/#series-sharding-in-ingest-storage-architecture)
- [Series sharding in classic architecture](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/hash-ring/#series-sharding-in-classic-architecture)

## Supported protocols

The distributor supports the following metrics ingestion protocols:

- Prometheus remote write v1
- Prometheus remote write v2
- OpenTelemetry Protocol (OTLP)
- Influx

## Validation

The distributor cleans and validates incoming data before ingesting it into Mimir.

A single request can include both valid and invalid metrics, samples, metadata, or exemplars.
The distributor ingests only the valid data and rejects any invalid entries.

If invalid data is detected:

- When using Prometheus remote write or Influx, the distributor returns an HTTP 400 status code.
- When using OTLP, it returns an HTTP 200 status code, following the OTLP specification for partial ingestion.

In both cases, the response body contains details about the first invalid item encountered.
The returned error is typically logged by the agent sending metrics to Mimir, such as Prometheus, Grafana Alloy, or the OpenTelemetry Collector.

The distributor data cleanup includes the following transformation:

- The metric metadata `help` is truncated to fit in the length defined via the `-validation.max-metadata-length` flag.

The distributor validation includes the following checks:

- The metric metadata and labels conform to the [Prometheus exposition format](https://prometheus.io/docs/concepts/data_model/).
- The metric metadata (`name` and `unit`) are not longer than what is defined via the `-validation.max-metadata-length` flag.
- The number of labels of each metric is not higher than `-validation.max-label-names-per-series`.
- Each metric label name is not longer than `-validation.max-length-label-name`.
- Each metric label value is not longer than `-validation.max-length-label-value`.
- Each sample timestamp is not newer than `-validation.create-grace-period`.
- Each exemplar has a timestamp and at least one non-empty label name and value pair.
- Each exemplar has no more than 128 labels.

{{< admonition type="note" >}}
For each tenant, you can override the validation checks by modifying the overrides section of the [runtime configuration](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-runtime-configuration/).
{{< /admonition >}}

## Rate limiting

The distributor includes two different types of rate limiters that apply to each tenant.

- **Request rate**<br />
  The maximum number of requests per second that can be served across Grafana Mimir cluster for each tenant.

- **Ingestion rate**<br />
  The maximum samples per second that can be ingested across Grafana Mimir cluster for each tenant.

If any of these rates is exceeded, the distributor drops the request and returns an HTTP 429 response code.

Internally, these limits are implemented using a per-distributor local rate limiter.
The local rate limiter for each distributor is configured with a limit of `limit / N`, where `N` is the number of healthy distributor replicas.
The distributor automatically adjusts the request and ingestion rate limits if the number of distributor replicas change.

This design uses a per-distributor local rate limiter and requires that write requests be [evenly distributed across the pool of distributors](#load-balancing-across-distributors).

### Configure rate limits

Use the following flags to configure the rate limits:

- `-distributor.request-rate-limit`: Request rate limit, which is per tenant, and which is in requests per second
- `-distributor.request-burst-size`: Request burst size (in number of requests) allowed, which is per tenant
- `-distributor.ingestion-rate-limit`: Ingestion rate limit, which is per tenant, and which is in samples per second
- `-distributor.ingestion-burst-size`: Ingestion burst size (in number of samples) allowed, which is per tenant

{{< admonition type="note" >}}
You can override rate limiting on a per-tenant basis by setting `request_rate`, `ingestion_rate`, `request_burst_size` and `ingestion_burst_size` in the overrides section of the runtime configuration.
{{< /admonition >}}

{{< admonition type="note" >}}
By default, Prometheus remote write doesn't retry requests on 429 HTTP response status code. To modify this behavior, use `retry_on_http_429: true` in the Prometheus [`remote_write` configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).
{{< /admonition >}}

### Distributor ring and rate limiting

Since distributor rate limits are implemented locally, each distributor must know how many healthy distributors are running in the Mimir cluster.
To achieve this, each distributor joins a hash ring used for service discovery, which tracks the number of healthy distributor instances.

By default, the distributors’ ring uses `memberlist` as its backend.
If you want to configure a different backend, for example, `consul` or `etcd`, you can use the following CLI flags (and their respective YAML configuration options) to configure the distributors ring KV store:

- `-distributor.ring.store`: The backend storage to use.
- `-distributor.ring.consul.*`: The Consul client configuration. Set this flag only if `consul` is the configured backend storage.
- `-distributor.ring.etcd.*`: The etcd client configuration. Set this flag only if `etcd` is the configured backend storage.

For more information, refer to [Configure hash rings](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-hash-rings/).

## Load balancing across distributors

As a best practice, uniformly distribute write requests across all distributor instances by placing a load balancer in front of them.
The preferred approach is a Layer 7 load balancer, which balances individual HTTP requests across distributors.

{{< admonition type="note" >}}
If you run Grafana Mimir in a Kubernetes cluster and use a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/) as the ingress for distributors, a Kubernetes Service balances TCP connections across endpoints but doesn't balance HTTP requests within a single TCP connection.

If you enable HTTP persistent connections, also known as HTTP keep-alive, Prometheus reuses the same TCP connection for each remote-write HTTP request of a remote-write shard.
This can cause distributors to receive an uneven distribution of remote-write HTTP requests.

To improve the balancing of requests between distributors, consider increasing `min_shards` in the Prometheus [`remote_write` configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) block.
{{< /admonition >}}

## High-availability tracker

You can configure remote write agents, such as Prometheus or Grafana Alloy, in pairs, which means that metrics continue to be scraped and written to Grafana Mimir even when one of the remote write agents is down for maintenance or is unavailable due to a failure.
We refer to this configuration as high-availability (HA) pairs.

The distributor includes an HA tracker.
When the HA tracker is enabled, the distributor deduplicates incoming series from Prometheus HA pairs.
This enables you to have multiple Prometheus HA replicas of the same server writing identical series to Mimir, which the distributor then deduplicates.

For more information about HA deduplication and how to configure it, refer to [Configure high-availability deduplication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-high-availability-deduplication/).
