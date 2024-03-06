---
aliases:
  - ../../../operators-guide/architecture/components/querier/
description: The querier evaluates PromQL expressions.
menuTitle: Querier
title: Grafana Mimir querier
weight: 50
---

# Grafana Mimir querier

The querier is a stateless component that evaluates [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/)
expressions by fetching time series and labels on the read path.

The querier uses the [store-gateway]({{< relref "./store-gateway" >}}) component to query the [long-term storage]({{< relref "../../../get-started/about-grafana-mimir-architecture#long-term-storage" >}}) and the [ingester]({{< relref "./ingester" >}}) component to query recently written data.

## How it works

To find the correct blocks to look up at query time, queriers lazily download the bucket index when they receive the first query for a given tenant. The querier caches the bucket index in memory and periodically keeps it up-to-date.

The bucket index contains a list of blocks and block deletion marks of a tenant. The querier later uses the list of blocks and block deletion marks to locate the set of blocks that need to be queried for the given query.

### Anatomy of a query request

When a querier receives a query range request, the request contains the following parameters:

- `query`: the PromQL query expression (for example, `rate(node_cpu_seconds_total[1m])`)
- `start`: the start time
- `end`: the end time
- `step`: the query resolution (for example, `30` yields one data point every 30 seconds)

For each query, the querier analyzes the `start` and `end` time range to compute a list of all known blocks containing at least one sample within the time range.
For each list of blocks per query, the querier computes a list of store-gateway instances holding the blocks. The querier sends a request to each matching store-gateway instance to fetch all samples for the series matching the `query` within the `start` and `end` time range.

The request sent to each store-gateway contains the list of block IDs that are expected to be queried, and the response sent back by the store-gateway to the querier contains the list of block IDs that were queried.
This list of block IDs might be a subset of the requested blocks, for example, when a recent blocks-resharding event occurs within the last few seconds.

The querier runs a consistency check on responses received from the store-gateways to ensure all expected blocks have been queried.
If the expected blocks have not been queried, the querier retries fetching samples from missing blocks from different store-gateways up to `-store-gateway.sharding-ring.replication-factor` (defaults to 3) times or maximum 3 times, whichever is lower.

If the consistency check fails after all retry attempts, the query execution fails.
Query failure due to the querier not querying all blocks ensures the correctness of query results.

If the query time range overlaps with the `-querier.query-ingesters-within` duration, the querier also sends the request to ingesters.
The request to the ingesters fetches samples that have not yet been uploaded to the long-term storage or are not yet available for querying through the store-gateway.

The configured period for `-querier.query-ingesters-within` should be greater than both:

- `-querier.query-store-after`
- the estimated minimum amount of time for the oldest samples stored in a block uploaded by ingester to be discovered and available for querying.
  When running Grafana Mimir with the default configuration, the estimated minimum amount of time for the oldest sample in an uploaded block to be available for querying is `3h`.

After all samples have been fetched from both the store-gateways and the ingesters, the querier runs the PromQL engine to execute the query and sends back the result to the client.

### Connecting to store-gateways

You must configure the queriers with the same `-store-gateway.sharding-ring.*` flags (or their respective YAML configuration parameters) that you use to configure the store-gateways so that the querier can access the store-gateway hash ring and discover the addresses of the store-gateways.

### Connecting to ingesters

You must configure the querier with the same `-ingester.ring.*` flags (or their respective YAML configuration parameters) that you use to configure the ingesters so that the querier can access the ingester hash ring and discover the addresses of the ingesters.

## Caching

The querier supports the following cache:

- [Metadata cache](#metadata-cache)

Caching is optional, but highly recommended in a production environment.

### Metadata cache

[Store-gateways]({{< relref "./store-gateway" >}}) and queriers can use Memcached to cache the following bucket metadata:

- List of tenants
- List of blocks per tenant
- Block `meta.json` existence and content
- Block `deletion-mark.json` existence and content
- Tenant `bucket-index.json.gz` content

Using the metadata cache reduces the number of API calls to long-term storage and stops the number of the API calls that scale linearly with the number of querier and store-gateway replicas.

To enable the metadata cache, set `-blocks-storage.bucket-store.metadata-cache.backend`.

{{< admonition type="note" >}}
Currently, Mimir supports caching with the `memcached` backend.

The Memcached client includes additional configuration available via flags that begin with the prefix `-blocks-storage.bucket-store.metadata-cache.memcached.*`.
{{< /admonition >}}

Additional flags for configuring the metadata cache begin with the prefix `-blocks-storage.bucket-store.metadata-cache.*`. By configuring the TTL to zero or a negative value, caching of given item type is disabled.

{{< admonition type="note" >}}
You should use the same Memcached backend cluster for both the store-gateways and queriers.
{{< /admonition >}}

## Querier configuration

For details about querier configuration, refer to [querier]({{< relref "../../../configure/configuration-parameters#querier" >}}).
