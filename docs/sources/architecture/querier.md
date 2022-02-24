---
title: "Querier"
description: "Overview of the querier microservice."
weight: 10
---

# Querier

The querier component handles queries using the [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/) to fetch time series and labels on the read path.

The querier uses the [store-gateway]({{< relref "./store-gateway.md" >}}) component to query the [long-term storage]({{< relref "./_index.md#long-term-storage" >}}) and the [ingester]({{< relref "./ingester.md" >}}) component to query recently written data.

The querier is **stateless**.

## How it works

The querier needs to have an almost up-to-date view over the bucket in long-term storage, in order to find the right blocks to lookup at query time. The querier can keep the bucket view updated in two different ways:

1. Periodically downloading the [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}) (default)
2. Periodically scanning the bucket

Queriers do not need any content from blocks except their metadata including the minimum and maximum timestamp of samples within the block.

### Bucket index enabled (default)

At startup, queriers lazily download the bucket index upon the first query received for a given tenant, cache it in memory and periodically keep it up to date. The bucket index contains the list of blocks and block deletion marks of a tenant, which is later used during the query execution to find the set of blocks that need to be queried for the given query.

Running with the bucket index enabled reduces querier startup time and reduces the volume of API calls to object storage.

### Bucket index disabled

When [bucket index]({{< relref "../operating-grafana-mimir/blocks-storage/bucket-index.md" >}}) is disabled, **queriers** iterate over the entire storage bucket to discover blocks for all tenants, and download the `meta.json` for each block. During this initial bucket scanning phase, a querier is not ready to handle incoming queries yet and its `/ready` readiness probe endpoint will fail.

While running, queriers periodically iterate over the storage bucket to discover new tenants and recently uploaded blocks.

### Anatomy of a query request

When a querier receives a query range request, it contains the following parameters:

- `query`: the PromQL query expression itself (e.g. `rate(node_cpu_seconds_total[1m])`)
- `start`: the start time
- `end`: the end time
- `step`: the query resolution (e.g. `30` to have 1 resulting data point every 30s)

Given a query, the querier analyzes the `start` and `end` time range to compute a list of all known blocks containing at least 1 sample within this time range. Given the list of blocks, the querier then computes a list of store-gateway instances holding these blocks and sends a request to each matching store-gateway instance asking to fetch all the samples for the series matching the `query` within the `start` and `end` time range.

The request sent to each store-gateway contains the list of block IDs that are expected to be queried, and the response sent back by the store-gateway to the querier contains the list of block IDs that were actually queried. This list may be a subset of the requested blocks, for example due to recent blocks resharding event (ie. last few seconds).
The querier runs a consistency check on responses received from the store-gateways to ensure all expected blocks have been queried; if not, the querier retries fetching samples from missing blocks from different store-gateways up to `-store-gateway.sharding-ring.replication-factor` (defaults to 3) times or maximum 3 times, whichever is lower. If the consistency check fails after all retries, the query execution fails as well. This way the correctness of query result is guaranteed.

If the query time range covers a period within `-querier.query-ingesters-within` duration, the querier also sends the request to all ingesters by default, in order to fetch samples that have not been uploaded to the long-term storage yet.

Once all samples have been fetched from both store-gateways and ingesters, the querier proceeds with running the PromQL engine to execute the query and send back the result to the client.

### How queriers connect to store-gateway

Queriers discover the address of store-gateways by accessing the store-gateways hash ring and thus queriers must be configured with the same `-store-gateway.sharding-ring.*` flags (or their respective YAML configuration parameters) that store-gateways have been configured.

### How queriers connect to ingester

Queriers discover the address of ingesters by accessing the ingester hash ring and thus queriers must be configured with the same `-ingester.ring.*` flags (or their respective YAML configuration parameters) that ingesters have been configured.

## Caching

The querier supports the following caches:

- [Metadata cache](#metadata-cache)

Caching is optional, but **highly recommended** in a production environment. Please also check out the [production tips]({{< relref "../operating-grafana-mimir/blocks-storage/production-tips.md#caching" >}}) for more information about configuring the cache.

### Metadata cache

[Store-gateway]({{< relref "./store-gateway.md" >}}) and querier can use memcached for caching bucket metadata:

- List of tenants
- List of blocks per tenant
- Block's `meta.json` existence and content
- Block's `deletion-mark.json` existence and content
- Tenant's `bucket-index.json.gz` content

Using the metadata cache can significantly reduce the number of API calls to long-term storage and stops the number of these API calls scaling linearly with the number of querier and store-gateway replicas.

To enable metadata cache, please set `-blocks-storage.bucket-store.metadata-cache.backend`. Only `memcached` backend is supported currently. Memcached client has additional configuration available via flags with `-blocks-storage.bucket-store.metadata-cache.memcached.*` prefix.

Additional flags for configuring metadata cache have `-blocks-storage.bucket-store.metadata-cache.*` prefix. By configuring TTL to zero or negative value, caching of given item type is disabled.

_The same memcached backend cluster should be shared between store-gateways and queriers._

## Querier configuration

Refer to the [querier]({{< relref "../configuration/reference-configuration-parameters.md#querier" >}}) block section for details of querier-related configuration.
